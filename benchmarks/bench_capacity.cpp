// Capacity and failure-path measurements: the seven questions in
// docs/latency_benchmark.md § Capacity program. Not Google Benchmark — each
// scenario needs its own offered-rate sweep, per-client percentiles, or
// multi-hour samples.
//
// Release builds only. Debug/sanitizer numbers are not representative.
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <future>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <variant>
#include <vector>

#if defined(__APPLE__)
#include <mach/mach.h>
#else
#include <fstream>
#endif

#include "book/book_manager.hpp"
#include "common/sequence_validator.hpp"
#include "exchange/core/events.hpp"
#include "exchange/gateway/order_entry_gateway.hpp"
#include "exchange/latency/latency_tracer.hpp"
#include "exchange/ledger/ledger.hpp"
#include "exchange/matching/matching_engine.hpp"
#include "exchange/market_data/market_data_router.hpp"
#include "exchange/risk/risk_gated_engine.hpp"
#include "exchange/sequencing/command_sequencer.hpp"
#include "exchange/sequencing/matching_pipeline.hpp"
#include "exchange/testing/hr_timer.hpp"
#include "exchange/testing/matching_scenarios.hpp"
#include "exchange/testing/matching_workload.hpp"
#include "net/packet.hpp"
#include "net/tcp_socket.hpp"
#include "net/udp_listener.hpp"
#include "net/udp_socket.hpp"
#include "protocol/order_entry/encoder.hpp"
#include "replay/snapshot.hpp"
#include "trader/market_data/feed_subscriber.hpp"
#include "trader/oms/order_entry_client.hpp"
#include "trader/strategies/strategy_runtime.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::gateway;
using namespace mdh::exchange::testing;
using namespace mdh::protocol::order_entry;
using namespace mdh::trader::oms;
using namespace std::chrono_literals;

namespace {

// Existing benchmark funding, retained only for the labeled one-account
// "before" run so the methodology correction has a reproducible baseline.
constexpr ledger::Balance kLegacyFundCash = 1'000'000'000'000'000LL;
constexpr Quantity kLegacyFundPosition = 1'000'000'000;

// Corrected matching-capacity funding. For the default 1M-command stream,
// these are several orders of magnitude above even assigning every maximum
// sized command to one account. The benchmark still enforces zero risk
// rejections rather than trusting that arithmetic as its pass condition.
constexpr ledger::Balance kMatchingFundCash = 1'000'000'000'000'000'000LL;
constexpr Quantity kMatchingFundPosition = 1'000'000'000'000ULL;
constexpr std::uint32_t kMatchingAccounts = 128;
constexpr std::size_t kProductionIngest = 1024;
constexpr std::size_t kProductionOutbound = 1024;
constexpr std::size_t kProductionMdQueue = 8192;

struct Args {
    std::string scenario = "help";
    bool quick = false;
    std::size_t operations = 1'000'000;
    std::size_t e2e_samples = 4'000;
    double e2e_start_rate = 5'000.0;
    double knee_track = 0.95;
    double knee_p999_mult = 10.0;
    std::vector<int> connections{16, 100, 500, 2000, 10'000};
    double per_connection_rate = 20.0;
    double connection_seconds = 3.0;
    int flooders = 16;
    double polite_rate = 1'000.0;
    double soak_hours = 4.0;
    double soak_seconds = 0.0;
    double soak_offered = 5'000.0;
    std::uint32_t matching_accounts = kMatchingAccounts;
    std::optional<unsigned> matching_cpu;
};

void print_usage() {
    std::printf(
        "bench_capacity — capacity and failure-path measurements (Release only)\n"
        "\n"
        "  --scenario matching-thread|e2e-knee|connections|queue-drops|fairness|recovery|soak|all\n"
        "  --quick                         smaller ops / skip 2k+ connections / 2s soak\n"
        "  --operations N                 mixed-stream length (default 1000000)\n"
        "  --e2e-samples N                  samples per offered rate (default 4000)\n"
        "  --connections 16,100,500,...\n"
        "  --per-connection-rate R          orders/s per live session (default 20)\n"
        "  --connection-seconds S\n"
        "  --flooders N                    fairness flooders (default 16)\n"
        "  --soak-hours H                  default 4; --soak-seconds overrides\n"
        "  --matching-accounts N           matching-thread accounts (default 128; use 1 for legacy before)\n"
        "  --matching-cpu N                pin matching thread (Linux)\n"
        "\n"
        "  all = matching-thread, e2e-knee, queue-drops, fairness, recovery\n"
        "        (not connections or soak)\n");
}

[[nodiscard]] std::vector<int> parse_int_list(const char* text) {
    std::vector<int> out;
    const char* p = text;
    while (*p != '\0') {
        char* end = nullptr;
        const long v = std::strtol(p, &end, 10);
        if (end == p) {
            break;
        }
        out.push_back(static_cast<int>(v));
        p = (*end == ',') ? end + 1 : end;
    }
    return out;
}

[[nodiscard]] Args parse_args(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        const std::string flag = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) {
                return nullptr;
            }
            return argv[++i];
        };
        if (flag == "--help" || flag == "-h") {
            args.scenario = "help";
        } else if (flag == "--scenario") {
            if (const char* v = next()) {
                args.scenario = v;
            }
        } else if (flag == "--quick") {
            args.quick = true;
        } else if (flag == "--operations") {
            if (const char* v = next()) {
                args.operations = static_cast<std::size_t>(std::atoll(v));
            }
        } else if (flag == "--e2e-samples") {
            if (const char* v = next()) {
                args.e2e_samples = static_cast<std::size_t>(std::atoll(v));
            }
        } else if (flag == "--connections") {
            if (const char* v = next()) {
                auto list = parse_int_list(v);
                if (!list.empty()) {
                    args.connections = std::move(list);
                }
            }
        } else if (flag == "--per-connection-rate") {
            if (const char* v = next()) {
                args.per_connection_rate = std::atof(v);
            }
        } else if (flag == "--connection-seconds") {
            if (const char* v = next()) {
                args.connection_seconds = std::atof(v);
            }
        } else if (flag == "--flooders") {
            if (const char* v = next()) {
                args.flooders = std::max(1, std::atoi(v));
            }
        } else if (flag == "--soak-hours") {
            if (const char* v = next()) {
                args.soak_hours = std::atof(v);
            }
        } else if (flag == "--soak-seconds") {
            if (const char* v = next()) {
                args.soak_seconds = std::atof(v);
            }
        } else if (flag == "--soak-offered") {
            if (const char* v = next()) {
                args.soak_offered = std::atof(v);
            }
        } else if (flag == "--matching-accounts") {
            if (const char* v = next()) {
                args.matching_accounts = static_cast<std::uint32_t>(std::max(1, std::atoi(v)));
            }
        } else if (flag == "--matching-cpu") {
            if (const char* v = next()) {
                args.matching_cpu = static_cast<unsigned>(std::atoi(v));
            }
        }
    }
    if (args.quick) {
        args.operations = std::min(args.operations, static_cast<std::size_t>(100'000));
        args.e2e_samples = std::min(args.e2e_samples, static_cast<std::size_t>(800));
        args.connections = {16, 32};
        args.connection_seconds = std::min(args.connection_seconds, 1.0);
        args.flooders = std::min(args.flooders, 4);
        if (args.soak_seconds <= 0.0) {
            args.soak_seconds = 2.0;
        }
    }
    return args;
}

[[nodiscard]] std::size_t rss_bytes() {
#if defined(__APPLE__)
    task_vm_info_data_t info{};
    mach_msg_type_number_t count = TASK_VM_INFO_COUNT;
    if (task_info(mach_task_self(), TASK_VM_INFO, reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS) {
        return 0;
    }
    return static_cast<std::size_t>(info.resident_size);
#else
    std::ifstream in("/proc/self/statm");
    std::size_t size_pages = 0;
    std::size_t resident_pages = 0;
    if (!(in >> size_pages >> resident_pages)) {
        return 0;
    }
    const long page = sysconf(_SC_PAGESIZE);
    return resident_pages * static_cast<std::size_t>(page > 0 ? page : 4096);
#endif
}

[[nodiscard]] WorkloadConfig mixed_config(std::size_t operations, std::uint32_t accounts = 1) {
    WorkloadConfig config;
    config.operation_count = operations;
    config.account_count = accounts;
    config.instrument_count = 1;
    config.initial_orders_per_side = 1'000;
    config.price_band_ticks = 64;
    return config;
}

void fund_ledger(ledger::Ledger& ledger, const WorkloadConfig& config,
                 ledger::Balance cash = kLegacyFundCash, Quantity position = kLegacyFundPosition) {
    for (AccountId account = 1; account <= config.account_count; ++account) {
        ledger.deposit_cash(account, cash);
        for (InstrumentId instrument : config.instruments()) {
            ledger.deposit_position(account, instrument, position);
        }
    }
}

void fund_gateway(OrderEntryGateway& gateway, AccountId first, int count, std::span<const InstrumentId> instruments) {
    for (int i = 0; i < count; ++i) {
        const AccountId account = first + static_cast<AccountId>(i);
        gateway.deposit_cash(account, kLegacyFundCash);
        for (InstrumentId instrument : instruments) {
            gateway.deposit_position(account, instrument, kLegacyFundPosition);
        }
    }
}

[[nodiscard]] Message to_wire(const ExchangeCommand& command) {
    return std::visit(
        [](const auto& cmd) -> Message {
            using T = std::decay_t<decltype(cmd)>;
            if constexpr (std::is_same_v<T, NewOrderCommand>) {
                return Message{NewOrder{
                    .account_id = cmd.account_id,
                    .client_order_id = cmd.client_order_id,
                    .instrument_id = cmd.instrument_id,
                    .side = cmd.side,
                    .price = cmd.price,
                    .quantity = cmd.quantity,
                    .order_type = cmd.order_type,
                    .time_in_force = cmd.time_in_force,
                }};
            } else if constexpr (std::is_same_v<T, CancelOrderCommand>) {
                return Message{CancelOrder{
                    .account_id = cmd.account_id,
                    .client_order_id = cmd.client_order_id,
                    .instrument_id = cmd.instrument_id,
                }};
            } else {
                return Message{ReplaceOrder{
                    .account_id = cmd.account_id,
                    .original_client_order_id = cmd.original_client_order_id,
                    .new_client_order_id = cmd.new_client_order_id,
                    .instrument_id = cmd.instrument_id,
                    .new_price = cmd.new_price,
                    .new_quantity = cmd.new_quantity,
                }};
            }
        },
        command);
}

void inbound_ids(const ExchangeCommand& command, AccountId& account, ClientOrderId& client_order_id) {
    std::visit(
        [&](const auto& cmd) {
            using T = std::decay_t<decltype(cmd)>;
            account = cmd.account_id;
            if constexpr (std::is_same_v<T, ReplaceOrderCommand>) {
                client_order_id = cmd.original_client_order_id;
            } else {
                client_order_id = cmd.client_order_id;
            }
        },
        command);
}

bool wait_processed(const sequencing::MatchingPipeline& pipeline, std::size_t target,
                     std::chrono::seconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (pipeline.commands_processed() < target) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

void print_host() {
    std::printf("rss=%zu bytes  pid=%d\n", rss_bytes(), static_cast<int>(getpid()));
}

void print_counts(const Workload& workload) {
    const auto& c = workload.counts;
    std::printf("mix  rest=%zu  cross=%zu  cancel=%zu  replace=%zu/%zu  ioc=%zu  fok=%zu  downgraded=%zu\n",
                c.new_resting, c.crossing, c.cancel, c.replace_priority_preserving, c.replace_priority_losing, c.ioc,
                c.fok, c.downgraded_to_resting);
}

void drain_submit(sequencing::MatchingPipeline& pipeline, const std::vector<ExchangeCommand>& commands) {
    for (const auto& command : commands) {
        while (!pipeline.submit(command)) {
            std::this_thread::yield();
        }
    }
}

struct SweepTiming {
    std::size_t levels = 0;
    std::size_t operations = 0;
    double matching_ns_per_op = 0.0;
    double full_path_ns_per_op = 0.0;
    std::size_t rejected_events = 0;
};

[[nodiscard]] std::size_t sweep_operations(std::size_t levels) {
    constexpr std::size_t kTargetOperations = 65'536;
    constexpr std::size_t kMaxRestingOrders = 262'144;
    return std::clamp(kMaxRestingOrders / std::max<std::size_t>(levels, 1), std::size_t{256},
                      kTargetOperations);
}

[[nodiscard]] SweepTiming run_full_path_sweep(std::size_t levels) {
    constexpr InstrumentId kInstrument = 1;
    constexpr AccountId kMaker = 1;
    constexpr AccountId kTaker = 2;
    constexpr Price kBase = 10'000'000;
    constexpr Quantity kPerLevel = 10;

    const std::size_t cases = sweep_operations(levels);
    SequentialIds ids;
    std::vector<ExchangeCommand> seed;
    std::vector<ExchangeCommand> operations;
    seed.reserve(cases * levels);
    operations.reserve(cases);
    for (std::size_t i = 0; i < cases * levels; ++i) {
        seed.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kMaker, ids.take_client_order_id(),
                                                 kInstrument, Side::Sell, kBase + static_cast<Price>(i),
                                                 kPerLevel)});
    }
    for (std::size_t i = 0; i < cases; ++i) {
        const Price worst = kBase + static_cast<Price>((i + 1) * levels - 1);
        operations.push_back(ExchangeCommand{
            new_order(ids.take_command_sequence(), kTaker, ids.take_client_order_id(), kInstrument, Side::Buy,
                      worst, kPerLevel * static_cast<Quantity>(levels), TimeInForce::IOC)});
    }

    MatchingEngine matching_only{kInstrument};
    for (const auto& command : seed) {
        matching_only.process(command, discard_events());
    }
    const auto matching_start = std::chrono::steady_clock::now();
    for (const auto& command : operations) {
        matching_only.process(command, discard_events());
    }
    const double matching_elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - matching_start).count();

    MatchingEngine full_engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_cash(kMaker, kMatchingFundCash);
    ledger.deposit_cash(kTaker, kMatchingFundCash);
    ledger.deposit_position(kMaker, kInstrument, kMatchingFundPosition);
    ledger.deposit_position(kTaker, kInstrument, kMatchingFundPosition);
    risk::RiskGatedEngine gated(full_engine, ledger);
    sequencing::CommandSequencer sequencer;
    std::size_t rejected_events = 0;
    const EventSink sink = [&](const ExchangeEvent& event) {
        if (std::holds_alternative<OrderRejected>(event)) {
            ++rejected_events;
        }
    };
    for (const auto& command : seed) {
        gated.process(sequencer.sequence(command), sink);
    }
    const auto full_start = std::chrono::steady_clock::now();
    for (const auto& command : operations) {
        gated.process(sequencer.sequence(command), sink);
    }
    const double full_elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - full_start).count();

    return SweepTiming{
        .levels = levels,
        .operations = cases,
        .matching_ns_per_op = matching_elapsed * 1e9 / static_cast<double>(cases),
        .full_path_ns_per_op = full_elapsed * 1e9 / static_cast<double>(cases),
        .rejected_events = rejected_events,
    };
}

void print_full_path_sweeps() {
    std::printf("\n== MultiLevelSweep through RiskGatedEngine + CommandSequencer ==\n");
    std::printf("| levels | operations | matching-only ns/op | full-path ns/op | ratio | rejects |\n");
    std::printf("| ------:| ----------:| ------------------:| ----------------:| ------:| -------:|\n");
    for (const std::size_t levels : {std::size_t{1}, std::size_t{4}, std::size_t{16}, std::size_t{64},
                                     std::size_t{256}}) {
        const auto result = run_full_path_sweep(levels);
        const double ratio = result.matching_ns_per_op > 0.0
                                 ? result.full_path_ns_per_op / result.matching_ns_per_op
                                 : 0.0;
        std::printf("| %7zu | %10zu | %19.1f | %15.1f | %6.2fx | %7zu |\n", result.levels,
                    result.operations, result.matching_ns_per_op, result.full_path_ns_per_op, ratio,
                    result.rejected_events);
    }
}

int run_matching_thread(const Args& args) {
    const auto config = mixed_config(args.operations, args.matching_accounts);
    const auto workload = generate_workload(config);
    const bool legacy_before = config.account_count == 1;
    std::printf("methodology=%s  configured_accounts=%u\n",
                legacy_before ? "before-single-account-legacy-funding" : "after-multi-account-fully-funded",
                config.account_count);
    print_counts(workload);

    MatchingEngine engine_only(config.instruments(), 200'000);
    {
        for (const auto& command : workload.seed) {
            engine_only.process(command, discard_events());
        }
        const auto start = std::chrono::steady_clock::now();
        for (const auto& command : workload.operations) {
            engine_only.process(command, discard_events());
        }
        const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        const double ops = elapsed > 0.0 ? static_cast<double>(workload.operations.size()) / elapsed : 0.0;
        std::printf("footnote  MatchingEngine::process only  %.0f commands/s  (%.1f ns/op)  resting=%zu\n", ops,
                    ops > 0.0 ? 1e9 / ops : 0.0, engine_only.resting_order_count());
    }

    MatchingEngine engine(config.instruments(), 200'000);
    ledger::Ledger ledger;
    fund_ledger(ledger, config, legacy_before ? kLegacyFundCash : kMatchingFundCash,
                legacy_before ? kLegacyFundPosition : kMatchingFundPosition);
    risk::RiskGatedEngine gated(engine, ledger);
    std::atomic<std::size_t> rejected_events{0};
    std::array<std::atomic<std::size_t>, static_cast<std::size_t>(RejectReason::AccountMismatch) + 1>
        rejects_by_reason{};
    const EventSink sink = [&](const ExchangeEvent& event) {
        if (const auto* rejected = std::get_if<OrderRejected>(&event); rejected != nullptr) {
            rejected_events.fetch_add(1, std::memory_order_relaxed);
            rejects_by_reason[static_cast<std::size_t>(rejected->reason)].fetch_add(1, std::memory_order_relaxed);
        }
    };

    sequencing::MatchingPipelineOptions pipe_opts{
        .queue_capacity = kProductionIngest,
        .instruments = {},
        .expected_resting_orders = 200'000,
        .matching_delay = {},
        .matching_cpu = args.matching_cpu,
    };
    sequencing::MatchingPipeline pipeline(
        sink, pipe_opts,
        [&](const ExchangeCommand& command, const EventSink& event_sink) { gated.process(command, event_sink); });

    drain_submit(pipeline, workload.seed);
    if (!wait_processed(pipeline, workload.seed.size(), 60s)) {
        std::fprintf(stderr, "matching-thread: seed did not drain\n");
        return EXIT_FAILURE;
    }

    const std::size_t baseline = pipeline.commands_processed();
    const std::size_t rejects_before = pipeline.commands_rejected();
    const auto& ops = workload.operations;
    const auto start = std::chrono::steady_clock::now();
    // The generated stream contains dependent cancel/replace operations and
    // is valid only in vector order. Striped producers race those dependencies
    // in the MPSC admission order and turn the benchmark into an
    // UnknownOrderId test. One producer preserves the generated workload;
    // producer scalability is measured separately by queue-drops/fairness.
    for (const auto& command : ops) {
        while (!pipeline.submit(command)) {
            std::this_thread::yield();
        }
    }
    if (!wait_processed(pipeline, baseline + ops.size(), 120s)) {
        std::fprintf(stderr, "matching-thread: operations did not drain (%zu/%zu)\n", pipeline.commands_processed(),
                     baseline + ops.size());
    }
    const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    pipeline.stop();

    const std::size_t processed = pipeline.commands_processed() - baseline;
    const double commands_per_sec = elapsed > 0.0 ? static_cast<double>(processed) / elapsed : 0.0;
    const auto mem = engine.book_memory_stats();
    const std::size_t insufficient_funds =
        rejects_by_reason[static_cast<std::size_t>(RejectReason::InsufficientFunds)].load();
    const std::size_t insufficient_position =
        rejects_by_reason[static_cast<std::size_t>(RejectReason::InsufficientPosition)].load();
    const std::size_t too_large = rejects_by_reason[static_cast<std::size_t>(RejectReason::OrderTooLarge)].load();
    const std::size_t risk_rejects = insufficient_funds + insufficient_position + too_large;
    std::printf("\n== matching-thread ceiling (sequencer + risk + ledger + MatchingEngine, no sockets) ==\n");
    const std::size_t order_rejects = rejected_events.load();
    if (legacy_before) {
        std::printf("BEFORE_FLAWED  %.0f commands/s\n", commands_per_sec);
    } else if (risk_rejects == 0) {
        std::printf("HEADLINE_CORRECTED  %.0f commands/s\n", commands_per_sec);
    } else {
        std::printf("INVALID_CORRECTED_RUN  %.0f commands/s (funding gate failed)\n", commands_per_sec);
    }
    std::printf("processed=%zu  wall=%.4fs  ns/op=%.1f  queue_hwm=%zu/%zu  submit_rejects=%zu  "
                "order_rejected_events=%zu  resting=%zu  slab_live=%zu  slab_cap=%zu  ledger_accounts=%zu  "
                "holds=%zu\n",
                processed, elapsed, commands_per_sec > 0.0 ? 1e9 / commands_per_sec : 0.0,
                pipeline.queue_high_water_mark(), kProductionIngest, pipeline.commands_rejected() - rejects_before,
                order_rejects, engine.resting_order_count(), mem.live_orders, mem.slab_capacity,
                ledger.account_count(), ledger.hold_count());
    std::printf("risk_rejected_events=%zu  insufficient_funds=%zu  insufficient_position=%zu  "
                "order_too_large=%zu\n",
                risk_rejects, insufficient_funds, insufficient_position, too_large);
    for (std::size_t reason = 1; reason < rejects_by_reason.size(); ++reason) {
        const std::size_t count = rejects_by_reason[reason].load(std::memory_order_relaxed);
        if (count != 0) {
            std::printf("reject_reason[%.*s]=%zu\n",
                        static_cast<int>(to_string(static_cast<RejectReason>(reason)).size()),
                        to_string(static_cast<RejectReason>(reason)).data(), count);
        }
    }
    print_full_path_sweeps();
    return !legacy_before && risk_rejects != 0 ? EXIT_FAILURE : EXIT_SUCCESS;
}

class WireClient {
public:
    explicit WireClient(AccountId account)
        : account_(account),
          client_([this](const Message& message) {
              latency::tracer().stamp_client_decoded(message);
              received_.fetch_add(1, std::memory_order_release);
          }) {}

    [[nodiscard]] bool connect(std::uint16_t port) { return client_.connect("127.0.0.1", port); }

    [[nodiscard]] bool send_command(const ExchangeCommand& command) {
        const Message message = to_wire(command);
        latency::tracer().stamp_client_submit(message);
        return client_.send(message);
    }

    [[nodiscard]] bool send_message(const Message& message) {
        latency::tracer().stamp_client_submit(message);
        return client_.send(message);
    }

    [[nodiscard]] AccountId account() const { return account_; }
    [[nodiscard]] std::size_t received() const { return received_.load(std::memory_order_acquire); }

    bool wait_received(std::size_t count, std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (received_.load(std::memory_order_acquire) < count) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(50us);
        }
        return true;
    }

    void disconnect() { client_.disconnect(); }

private:
    AccountId account_;
    std::atomic<std::size_t> received_{0};
    OrderEntryClient client_;
};

struct PaceResult {
    double offered = 0.0;
    double achieved = 0.0;
    std::size_t sent = 0;
    std::size_t harvested = 0;
    double p50_us = 0.0;
    double p99_us = 0.0;
    double p999_us = 0.0;
    std::size_t queue_hwm = 0;
    std::uint64_t outbound_drops = 0;
    std::size_t commands_rejected = 0;
    bool tracked = false;
    bool p999_blew = false;
};

[[nodiscard]] PaceResult run_paced_mixed(OrderEntryGateway& gateway, WireClient& client,
                                           const std::vector<ExchangeCommand>& commands, double offered,
                                           std::size_t samples, double ticks_per_second) {
    PaceResult result;
    result.offered = offered;
    samples = std::min(samples, commands.size());
    std::vector<std::pair<AccountId, ClientOrderId>> keys;
    keys.reserve(samples);

    const auto interval = std::chrono::duration<double>(offered > 0.0 ? 1.0 / offered : 0.0);
    auto next = std::chrono::steady_clock::now();
    const auto start = next;
    const auto io0 = gateway.io_metrics();
    const std::size_t rejected0 = gateway.commands_rejected();

    for (std::size_t i = 0; i < samples; ++i) {
        const auto now = std::chrono::steady_clock::now();
        if (now < next) {
            while (std::chrono::steady_clock::now() < next) {
            }
        } else {
            next = now;
        }
        if (!client.send_command(commands[i])) {
            break;
        }
        AccountId account = 0;
        ClientOrderId id = 0;
        inbound_ids(commands[i], account, id);
        keys.emplace_back(account, id);
        ++result.sent;
        next += std::chrono::duration_cast<std::chrono::steady_clock::duration>(interval);
    }

    const auto send_done = std::chrono::steady_clock::now();
    (void)client.wait_received(result.sent, 5000ms);
    const double elapsed = std::chrono::duration<double>(send_done - start).count();
    result.achieved = elapsed > 0.0 ? static_cast<double>(result.sent) / elapsed : 0.0;

    std::vector<std::uint64_t> e2e;
    e2e.reserve(keys.size());
    for (const auto& [account, id] : keys) {
        const auto snap = latency::tracer().snapshot(account, id);
        if (!snap || snap->t0_client_submit == 0 || snap->t5_client_first == 0 ||
            snap->t5_client_first < snap->t0_client_submit) {
            continue;
        }
        e2e.push_back(snap->t5_client_first - snap->t0_client_submit);
    }
    result.harvested = e2e.size();
    const auto summary = summarise_latency(e2e, ticks_per_second);
    result.p50_us = summary.p50_ns / 1000.0;
    result.p99_us = summary.p99_ns / 1000.0;
    result.p999_us = summary.p999_ns / 1000.0;
    result.queue_hwm = gateway.matching_queue_high_water_mark();
    result.outbound_drops = gateway.io_metrics().outbound_drops - io0.outbound_drops;
    result.commands_rejected = gateway.commands_rejected() - rejected0;
    return result;
}

} // namespace

namespace {

void apply_knee_flags(PaceResult& row, const Args& args, double baseline_p999_us) {
    row.tracked = row.offered <= 0.0 || row.achieved >= args.knee_track * row.offered;
    row.p999_blew = baseline_p999_us > 0.0 && row.p999_us > baseline_p999_us * args.knee_p999_mult;
}

[[nodiscard]] std::unique_ptr<OrderEntryGateway> make_capacity_gateway(
    const Args& args, std::span<const InstrumentId> instruments, std::size_t ingest, std::size_t outbound,
    EventSink extra = {}, int accept_backlog = 128) {
    OrderEntryGatewayOptions options{
        .risk_limits = {},
        .instruments = {instruments.begin(), instruments.end()},
        .matching_queue_capacity = ingest,
        .expected_resting_orders = 200'000,
        .outbound_queue_capacity = outbound,
        .accept_backlog = accept_backlog,
        .pending_report_capacity = 1024,
        .extra_event_sink = extra ? extra : EventSink{},
        .writer_batch = 4,
        .enable_io_metrics = true,
        .matching_cpu = args.matching_cpu,
    };
    return std::make_unique<OrderEntryGateway>(0, options);
}

int run_e2e_knee(const Args& args, double ticks_per_second) {
    const auto config = mixed_config(std::max(args.operations, args.e2e_samples + 4'000));
    const auto workload = generate_workload(config);
    print_counts(workload);

    latency::ScopedEnable tracing(1 << 22);
    auto gateway = make_capacity_gateway(args, config.instruments(), kProductionIngest, kProductionOutbound);
    if (!gateway->start()) {
        std::fprintf(stderr, "e2e-knee: listen failed\n");
        return EXIT_FAILURE;
    }
    fund_gateway(*gateway, 1, static_cast<int>(config.account_count), config.instruments());
    WireClient client(1);
    if (!client.connect(*gateway->local_port())) {
        std::fprintf(stderr, "e2e-knee: connect failed\n");
        return EXIT_FAILURE;
    }
    for (const auto& command : workload.seed) {
        (void)client.send_command(command);
    }
    (void)client.wait_received(workload.seed.size(), 30s);

    std::printf("\n== e2e knee (mixed GTC over TCP, production queues %zu/%zu) ==\n", kProductionIngest,
                kProductionOutbound);
    std::printf("| offered/s | achieved/s | sent | traces | p50 us | p99 us | p99.9 us | q_hwm | out_drops | "
                "ingest_reject |\n");

    std::vector<PaceResult> rows;
    double baseline_p999 = 0.0;
    double rate = args.e2e_start_rate;
    std::size_t offset = 0;
    const std::size_t max_steps = args.quick ? 4 : 10;
    for (std::size_t step = 0; step < max_steps; ++step) {
        if (offset + args.e2e_samples > workload.operations.size()) {
            break;
        }
        std::vector<ExchangeCommand> slice(workload.operations.begin() + static_cast<std::ptrdiff_t>(offset),
                                            workload.operations.begin() +
                                                static_cast<std::ptrdiff_t>(offset + args.e2e_samples));
        offset += args.e2e_samples;
        auto row = run_paced_mixed(*gateway, client, slice, rate, args.e2e_samples, ticks_per_second);
        if (rows.empty()) {
            baseline_p999 = row.p999_us;
        }
        apply_knee_flags(row, args, baseline_p999);
        std::printf("| %9.0f | %10.0f | %4zu | %6zu | %6.1f | %6.1f | %8.1f | %5zu | %9llu | %13zu |\n", row.offered,
                    row.achieved, row.sent, row.harvested, row.p50_us, row.p99_us, row.p999_us, row.queue_hwm,
                    static_cast<unsigned long long>(row.outbound_drops), row.commands_rejected);
        rows.push_back(row);
        if (!row.tracked || row.p999_blew) {
            break;
        }
        rate *= 2.0;
    }

    std::optional<PaceResult> last_ok;
    std::optional<PaceResult> first_fail;
    for (const auto& row : rows) {
        if (row.tracked && !row.p999_blew) {
            last_ok = row;
        } else if (!first_fail) {
            first_fail = row;
        }
    }
    if (last_ok) {
        std::printf("last rate that still tracks offered: %.0f/s (achieved %.0f/s, p99.9 %.1f us)\n", last_ok->offered,
                    last_ok->achieved, last_ok->p999_us);
    }
    if (first_fail) {
        std::printf("first rate that does not: %.0f/s (achieved %.0f/s, p99.9 %.1f us)  plateau≈%.0f/s\n",
                    first_fail->offered, first_fail->achieved, first_fail->p999_us, first_fail->achieved);
    } else {
        std::printf("no knee found in this sweep; raise --e2e-samples or keep doubling.\n");
    }
    std::printf("matching_queue_hwm=%zu  commands_rejected=%zu  outbound_drops=%llu\n",
                gateway->matching_queue_high_water_mark(), gateway->commands_rejected(),
                static_cast<unsigned long long>(gateway->io_metrics().outbound_drops));
    gateway->stop();
    return EXIT_SUCCESS;
}

int run_connections(const Args& args, double ticks_per_second) {
    std::printf("\n== connection scaling (low per-connection rate, IoPoller I/O thread) ==\n");
    std::printf("expected server threads = 2 (I/O + matching); this process also has N client reader threads.\n");

    latency::ScopedEnable tracing(1 << 20);
    for (int n : args.connections) {
        const double aggregate = args.per_connection_rate * static_cast<double>(n);
        std::printf("\n-- N=%d  per-conn=%.0f/s  aggregate_offered=%.0f/s  rss_before=%zu --\n", n,
                    args.per_connection_rate, aggregate, rss_bytes());
        auto gateway =
            make_capacity_gateway(args, std::vector<InstrumentId>{1}, kProductionIngest, kProductionOutbound, {},
                                   std::max(128, n));
        if (!gateway->start()) {
            std::printf("RESULT  N=%d  listen failed\n", n);
            return EXIT_SUCCESS;
        }
        fund_gateway(*gateway, 1, n, std::vector<InstrumentId>{1});

        std::vector<std::unique_ptr<WireClient>> clients;
        clients.reserve(static_cast<std::size_t>(n));
        int connected = 0;
        try {
            for (int i = 0; i < n; ++i) {
                auto client = std::make_unique<WireClient>(static_cast<AccountId>(1 + i));
                if (!client->connect(*gateway->local_port())) {
                    std::printf("RESULT  N=%d  connect failed at client %d  connected=%d  rss=%zu\n", n, i, connected,
                                rss_bytes());
                    gateway->stop();
                    return EXIT_SUCCESS;
                }
                clients.push_back(std::move(client));
                ++connected;
            }
        } catch (const std::exception& ex) {
            std::printf("RESULT  N=%d  thread/create failed after %d connects: %s  rss=%zu\n", n, connected, ex.what(),
                        rss_bytes());
            gateway->stop();
            return EXIT_SUCCESS;
        }

        const auto interval = std::chrono::duration<double>(args.per_connection_rate > 0.0
                                                                   ? 1.0 / args.per_connection_rate
                                                                   : 0.0);
        const auto deadline = std::chrono::steady_clock::now() +
                               std::chrono::duration<double>(args.connection_seconds);
        std::atomic<std::size_t> sent{0};
        std::vector<std::thread> senders;
        std::atomic<bool> failed{false};
        for (int i = 0; i < n; ++i) {
            senders.emplace_back([&, i] {
                ClientOrderId id = 1;
                auto next = std::chrono::steady_clock::now();
                while (std::chrono::steady_clock::now() < deadline && !failed.load(std::memory_order_relaxed)) {
                    while (std::chrono::steady_clock::now() < next) {
                        std::this_thread::sleep_for(50us);
                    }
                    const Message message{NewOrder{
                        .account_id = static_cast<AccountId>(1 + i),
                        .client_order_id = id++,
                        .instrument_id = 1,
                        .side = Side::Buy,
                        .price = 1,
                        .quantity = 1,
                        .order_type = OrderType::Limit,
                        .time_in_force = TimeInForce::IOC,
                    }};
                    if (!clients[static_cast<std::size_t>(i)]->send_message(message)) {
                        failed.store(true, std::memory_order_relaxed);
                        return;
                    }
                    sent.fetch_add(1, std::memory_order_relaxed);
                    next += std::chrono::duration_cast<std::chrono::steady_clock::duration>(interval);
                }
            });
        }
        for (auto& sender : senders) {
            sender.join();
        }

        std::vector<std::uint64_t> polite_e2e;
        AccountId probe = 1;
        for (ClientOrderId id = 1; id < 10'000; ++id) {
            const auto snap = latency::tracer().snapshot(probe, id);
            if (!snap || snap->t5_client_first == 0 || snap->t0_client_submit == 0) {
                continue;
            }
            if (snap->t5_client_first >= snap->t0_client_submit) {
                polite_e2e.push_back(snap->t5_client_first - snap->t0_client_submit);
            }
        }
        const auto summary = summarise_latency(polite_e2e, ticks_per_second);
        std::printf("RESULT  N=%d  connected=%d  sent=%zu  probe_traces=%zu  p50=%.1f us  p99=%.1f us  "
                    "p99.9=%.1f us  rss=%zu  q_hwm=%zu  failed=%d\n",
                    n, connected, sent.load(), summary.count, summary.p50_ns / 1000.0, summary.p99_ns / 1000.0,
                    summary.p999_ns / 1000.0, rss_bytes(), gateway->matching_queue_high_water_mark(),
                    static_cast<int>(failed.load()));
        gateway->stop();
        if (failed) {
            std::printf("stopping the connection sweep after a send failure\n");
            break;
        }
    }
    return EXIT_SUCCESS;
}

class MuteTcp {
public:
    [[nodiscard]] bool connect(std::uint16_t port) { return socket_.connect("127.0.0.1", port); }

    void send(const Message& message) {
        std::vector<std::byte> buf;
        encode_message(message, buf);
        std::size_t written = 0;
        while (written < buf.size()) {
            if (auto n = socket_.write(std::span<const std::byte>(buf).subspan(written)); n.ok()) {
                written += n.n;
            } else {
                break;
            }
        }
    }

private:
    net::TcpSocket socket_;
};

int run_queue_drops(const Args& args) {
    std::printf("\n== queue drop points (production capacities) ==\n");

    {
        std::printf("\n-- ingest MPSC (TCP, cap %zu) --\n", kProductionIngest);
        const auto config = mixed_config(std::max(args.e2e_samples, static_cast<std::size_t>(8'000)));
        auto gateway = make_capacity_gateway(args, config.instruments(), kProductionIngest, kProductionOutbound);
        if (!gateway->start()) {
            return EXIT_FAILURE;
        }
        fund_gateway(*gateway, 1, 1, config.instruments());
        WireClient client(1);
        if (!client.connect(*gateway->local_port())) {
            return EXIT_FAILURE;
        }
        const auto workload = generate_workload(config);
        for (const auto& command : workload.seed) {
            (void)client.send_command(command);
        }
        (void)client.wait_received(workload.seed.size(), 20s);
        const auto start = std::chrono::steady_clock::now();
        std::size_t sent = 0;
        for (const auto& command : workload.operations) {
            if (!client.send_command(command)) {
                break;
            }
            ++sent;
        }
        const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        std::printf("TCP flood  sent=%zu  %.0f/s  q_hwm=%zu/%zu  commands_rejected=%zu\n", sent,
                    elapsed > 0.0 ? static_cast<double>(sent) / elapsed : 0.0, gateway->matching_queue_high_water_mark(),
                    kProductionIngest, gateway->commands_rejected());
        std::printf("client experience on ingest full: silence (gateway ignores submit() false; no Rejected).\n");
        if (gateway->matching_queue_high_water_mark() < kProductionIngest / 2) {
            std::printf("TCP cannot fill the ingest ring on this machine; matcher is faster than ingest.\n");
        }
        gateway->stop();
    }

    {
        std::printf("\n-- ingest MPSC (in-process flood, same path as matching-thread) --\n");
        const auto config = mixed_config(args.operations);
        MatchingEngine engine(config.instruments(), 200'000);
        ledger::Ledger ledger;
        fund_ledger(ledger, config);
        risk::RiskGatedEngine gated(engine, ledger);
        sequencing::MatchingPipeline pipeline(
            discard_events(),
            sequencing::MatchingPipelineOptions{.queue_capacity = kProductionIngest,
                                                .instruments = {},
                                                .expected_resting_orders = 200'000,
                                                .matching_delay = {},
                                                .matching_cpu = args.matching_cpu},
            [&](const ExchangeCommand& command, const EventSink& sink) { gated.process(command, sink); });
        const auto workload = generate_workload(config);
        drain_submit(pipeline, workload.seed);
        (void)wait_processed(pipeline, workload.seed.size(), 60s);
        const std::size_t before = pipeline.commands_rejected();
        std::size_t attempts = 0;
        const auto start = std::chrono::steady_clock::now();
        for (const auto& command : workload.operations) {
            ++attempts;
            (void)pipeline.submit(command); // do not retry: count true rejects
        }
        const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        (void)wait_processed(pipeline, pipeline.commands_processed(), 30s);
        pipeline.stop();
        std::printf("in-process  attempts=%zu  %.0f tries/s  rejected=%zu  q_hwm=%zu  processed=%zu\n", attempts,
                    elapsed > 0.0 ? static_cast<double>(attempts) / elapsed : 0.0,
                    pipeline.commands_rejected() - before, pipeline.queue_high_water_mark(),
                    pipeline.commands_processed());
        std::printf("this reject rate is matching-thread backpressure, not a TCP offered rate.\n");
    }

    {
        std::printf("\n-- outbound SPSC (slow non-reading client) --\n");
        std::printf("production outbound cap is %zu; this row uses 64 because loopback TCP\n"
                    "buffering absorbed a 1024-slot queue without a drop in earlier runs.\n",
                    kProductionOutbound);
        auto gateway = make_capacity_gateway(args, std::vector<InstrumentId>{1}, kProductionIngest, 64);
        if (!gateway->start()) {
            return EXIT_FAILURE;
        }
        fund_gateway(*gateway, 1, 2, std::vector<InstrumentId>{1});
        MuteTcp slow;
        if (!slow.connect(*gateway->local_port())) {
            return EXIT_FAILURE;
        }
        const std::size_t mute_orders = args.quick ? 16'000 : 50'000;
        for (std::size_t i = 0; i < mute_orders; ++i) {
            slow.send(Message{NewOrder{
                .account_id = 1,
                .client_order_id = static_cast<ClientOrderId>(i + 1),
                .instrument_id = 1,
                .side = Side::Buy,
                .price = 100,
                .quantity = 1,
                .order_type = OrderType::Limit,
                .time_in_force = TimeInForce::GTC,
            }});
        }
        std::this_thread::sleep_for(500ms);

        WireClient fast(2);
        if (!fast.connect(*gateway->local_port())) {
            return EXIT_FAILURE;
        }
        std::size_t sent = 0;
        for (std::size_t i = 0; i < (args.quick ? 2'000 : 20'000); ++i) {
            if (!fast.send_message(Message{NewOrder{
                    .account_id = 2,
                    .client_order_id = static_cast<ClientOrderId>(i + 1),
                    .instrument_id = 1,
                    .side = Side::Buy,
                    .price = 1,
                    .quantity = 1,
                    .order_type = OrderType::Limit,
                    .time_in_force = TimeInForce::GTC,
                }})) {
                break;
            }
            ++sent;
        }
        (void)fast.wait_received(sent / 2, 10s);
        std::printf("fast sent=%zu  received=%zu  outbound_drops=%llu  q_hwm=%zu\n", sent, fast.received(),
                    static_cast<unsigned long long>(gateway->io_metrics().outbound_drops),
                    gateway->matching_queue_high_water_mark());
        std::printf("client experience on outbound full: engine committed; slow client misses private reports.\n");
        std::printf("fast client still received reports (isolation): %s\n",
                    fast.received() > 0 ? "yes" : "no");
        gateway->stop();
    }

    {
        std::printf("\n-- MarketDataRouter DroppingQueue (cap %zu) --\n", kProductionMdQueue);
        trader::strategies::StrategyRuntime runtime;
        trader::market_data::FeedSubscriberOptions sub_opts;
        sub_opts.replay_options.stop_on_sequence_error = false;
        trader::market_data::FeedSubscriber subscriber(0, runtime, sub_opts);
        if (!subscriber.start()) {
            std::printf("subscriber bind failed\n");
            return EXIT_FAILURE;
        }
        const std::uint16_t md_port = *subscriber.local_port();

        class UdpSink {
        public:
            explicit UdpSink(std::uint16_t port) : port_(port) {}
            void send(const protocol::Event& event) {
                const std::array<protocol::Event, 1> frames{event};
                auto datagram = net::pack_frames(packet_++, std::span<const protocol::Event>(frames));
                (void)socket_.send_to(datagram, "127.0.0.1", port_);
            }

        private:
            net::UdpSocket socket_;
            std::uint16_t port_;
            std::uint64_t packet_{1};
        };
        UdpSink udp(md_port);
        exchange::market_data::MarketDataRouter router(
            [&](const protocol::Event& event) { udp.send(event); },
            exchange::market_data::MarketDataRouterOptions{.queue_capacity = kProductionMdQueue, .idle_timeout = 1ms});
        router.start();

        const auto config = mixed_config(args.operations);
        MatchingEngine engine(config.instruments(), 200'000);
        ledger::Ledger ledger;
        fund_ledger(ledger, config);
        risk::RiskGatedEngine gated(engine, ledger);
        sequencing::MatchingPipeline pipeline(
            [&](const ExchangeEvent& event) { router.publish(event); },
            sequencing::MatchingPipelineOptions{.queue_capacity = kProductionIngest,
                                                .instruments = {},
                                                .expected_resting_orders = 200'000,
                                                .matching_delay = {},
                                                .matching_cpu = args.matching_cpu},
            [&](const ExchangeCommand& command, const EventSink& sink) { gated.process(command, sink); });
        const auto workload = generate_workload(config);
        drain_submit(pipeline, workload.seed);
        (void)wait_processed(pipeline, workload.seed.size(), 60s);
        drain_submit(pipeline, workload.operations);
        (void)wait_processed(pipeline, workload.seed.size() + workload.operations.size(), 120s);
        pipeline.stop();
        std::this_thread::sleep_for(200ms);
        router.stop();
        subscriber.stop();
        std::printf("router dropped=%zu  hwm=%zu/%zu  routed=%llu  subscriber sequence_failures=%llu  recoveries=%llu\n",
                    router.dropped_count(), router.queue_high_water_mark(), kProductionMdQueue,
                    static_cast<unsigned long long>(router.routed_count()),
                    static_cast<unsigned long long>(subscriber.stats().sequence_failures),
                    static_cast<unsigned long long>(subscriber.stats().recoveries));
        std::printf("client experience on MD drop: feed sequence gap (seq assigned before enqueue), not a silent stale book.\n");
        if (router.dropped_count() == 0) {
            std::printf("router never dropped at matching-thread mixed rate on this machine with cap %zu.\n",
                        kProductionMdQueue);
        }
    }
    return EXIT_SUCCESS;
}

int run_fairness(const Args& args, double ticks_per_second) {
    std::printf("\n== fairness (one polite client vs flooders) ==\n");
    latency::ScopedEnable tracing(1 << 22);
    const int n = 1 + args.flooders;
    auto gateway = make_capacity_gateway(args, std::vector<InstrumentId>{1}, kProductionIngest, kProductionOutbound, {},
                                          std::max(32, n));
    if (!gateway->start()) {
        return EXIT_FAILURE;
    }
    fund_gateway(*gateway, 1, n, std::vector<InstrumentId>{1});

    auto isolation_client = std::make_unique<WireClient>(1);
    if (!isolation_client->connect(*gateway->local_port())) {
        return EXIT_FAILURE;
    }
    const auto polite_interval = std::chrono::duration<double>(1.0 / args.polite_rate);
    const std::size_t polite_n = args.quick ? 400 : 2'000;
    auto run_polite = [&](WireClient& client, const char* label) {
        auto next = std::chrono::steady_clock::now();
        const auto start = next;
        for (std::size_t i = 0; i < polite_n; ++i) {
            while (std::chrono::steady_clock::now() < next) {
            }
            const Message message{NewOrder{
                .account_id = 1,
                .client_order_id = static_cast<ClientOrderId>(100'000 + i),
                .instrument_id = 1,
                .side = Side::Buy,
                .price = 1,
                .quantity = 1,
                .order_type = OrderType::Limit,
                .time_in_force = TimeInForce::IOC,
            }};
            if (!client.send_message(message)) {
                break;
            }
            next += std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                std::chrono::duration<double>(polite_interval));
        }
        (void)client.wait_received(polite_n, 10s);
        (void)start;
        std::vector<std::uint64_t> e2e;
        for (std::size_t i = 0; i < polite_n; ++i) {
            const auto snap = latency::tracer().snapshot(1, static_cast<ClientOrderId>(100'000 + i));
            if (!snap || snap->t5_client_first == 0 || snap->t0_client_submit == 0 ||
                snap->t5_client_first < snap->t0_client_submit) {
                continue;
            }
            e2e.push_back(snap->t5_client_first - snap->t0_client_submit);
        }
        const auto summary = summarise_latency(e2e, ticks_per_second);
        std::printf("%s  traces=%zu  p50=%.1f us  p99=%.1f us  p99.9=%.1f us  q_hwm=%zu  rejected=%zu\n", label,
                    summary.count, summary.p50_ns / 1000.0, summary.p99_ns / 1000.0, summary.p999_ns / 1000.0,
                    gateway->matching_queue_high_water_mark(), gateway->commands_rejected());
        return summary.p99_ns / 1000.0;
    };

    const double iso_p99 = run_polite(*isolation_client, "isolation");
    isolation_client->disconnect();
    isolation_client.reset();

    WireClient polite(1);
    if (!polite.connect(*gateway->local_port())) {
        return EXIT_FAILURE;
    }
    std::vector<std::unique_ptr<WireClient>> flooders;
    std::atomic<bool> flood{true};
    std::vector<std::thread> flood_threads;
    for (int i = 0; i < args.flooders; ++i) {
        auto client = std::make_unique<WireClient>(static_cast<AccountId>(2 + i));
        if (!client->connect(*gateway->local_port())) {
            break;
        }
        flooders.push_back(std::move(client));
        flood_threads.emplace_back([&, i] {
            ClientOrderId id = 1;
            while (flood.load(std::memory_order_relaxed)) {
                const Message message{NewOrder{
                    .account_id = static_cast<AccountId>(2 + i),
                    .client_order_id = id++,
                    .instrument_id = 1,
                    .side = Side::Buy,
                    .price = 1,
                    .quantity = 1,
                    .order_type = OrderType::Limit,
                    .time_in_force = TimeInForce::IOC,
                }};
                if (!flooders[static_cast<std::size_t>(i)]->send_message(message)) {
                    return;
                }
            }
        });
    }

    const double contended_p99 = run_polite(polite, "contended");
    flood.store(false, std::memory_order_relaxed);
    for (auto& t : flood_threads) {
        t.join();
    }
    std::printf("polite p99 isolation=%.1f us  contended=%.1f us  ratio=%.2f  ingest_rejects=%zu\n", iso_p99,
                contended_p99, iso_p99 > 0.0 ? contended_p99 / iso_p99 : 0.0, gateway->commands_rejected());
    gateway->stop();
    return EXIT_SUCCESS;
}

int run_recovery(const Args& args) {
    std::printf("\n== snapshot recovery wall-clock ==\n");
    std::printf("There is no snapshot server and no retransmission; recovery rebases the book from a file.\n");
    const std::vector<std::size_t> depths = args.quick ? std::vector<std::size_t>{2'000, 8'000}
                                                       : std::vector<std::size_t>{2'000, 20'000, 80'000};
    const auto dir = std::filesystem::temp_directory_path();
    double last_load_ms = 0.0;
    for (std::size_t depth : depths) {
        book::BookManager books;
        for (std::size_t i = 0; i < depth; ++i) {
            const OrderId id = static_cast<OrderId>(i + 1);
            const Price price = 10'000 + static_cast<Price>(i % 70);
            const Side side = (i % 2 == 0) ? Side::Buy : Side::Sell;
            (void)books.book_for(1).add_order(id, price, 1, side);
        }
        const auto path = dir / ("mdh_capacity_snap_" + std::to_string(depth) + ".bin");
        if (!replay::write_snapshot(path.string(), 1, books)) {
            std::fprintf(stderr, "write_snapshot failed\n");
            return EXIT_FAILURE;
        }
        const auto t0 = std::chrono::steady_clock::now();
        auto loaded = replay::read_snapshot(path.string());
        const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
        last_load_ms = ms;
        std::printf("depth=%zu  load+rebuild=%.2f ms  ok=%d\n", depth, ms, loaded.has_value() ? 1 : 0);
        std::filesystem::remove(path);
        const double knee = 40'000.0;
        std::printf("  30s at %.0f events/s would lose ~%.0f sequences; those are NOT replayed.\n", 40'000.0,
                    30.0 * knee);
    }

    {
        std::printf("\n-- UDP listen drain during delayed recovery --\n");
        book::BookManager snap_books;
        (void)snap_books.book_for(1).add_order(999, 70, 3, Side::Buy);
        const auto path = dir / "mdh_capacity_udp_snap.bin";
        (void)replay::write_snapshot(path.string(), 100, snap_books);
        replay::ReplayOptions options;
        options.recovery_snapshot_path = path.string();
        const auto delay = std::chrono::microseconds(static_cast<int>(last_load_ms * 1000.0));
        constexpr std::uint16_t kPort = 58'901;
        auto future = std::async(std::launch::async, [&] {
            return net::run_udp_listen(kPort, options,
                                        net::UdpListenOptions{.idle_timeout = 800ms,
                                                              .queue_capacity = 1024,
                                                              .consumer_delay = delay});
        });
        std::this_thread::sleep_for(50ms);
        net::UdpSocket sender;
        const auto t0 = std::chrono::steady_clock::now();
        (void)sender.send_to(net::pack_frames(1, std::vector<protocol::Event>{protocol::Event{protocol::AddOrder{
                                        .sequence_number = 1,
                                        .timestamp_ns = 1,
                                        .order_id = 1,
                                        .instrument_id = 1,
                                        .price = 50,
                                        .quantity = 1,
                                        .side = Side::Buy,
                                    }}}),
                             "127.0.0.1", kPort);
        (void)sender.send_to(net::pack_frames(2, std::vector<protocol::Event>{protocol::Event{protocol::AddOrder{
                                        .sequence_number = 5,
                                        .timestamp_ns = 2,
                                        .order_id = 2,
                                        .instrument_id = 1,
                                        .price = 60,
                                        .quantity = 1,
                                        .side = Side::Sell,
                                    }}}),
                             "127.0.0.1", kPort);
        for (int seq = 6; seq <= 40; ++seq) {
            (void)sender.send_to(
                net::pack_frames(static_cast<std::uint64_t>(seq),
                                 std::vector<protocol::Event>{protocol::Event{protocol::AddOrder{
                                     .sequence_number = static_cast<Sequence>(seq),
                                     .timestamp_ns = static_cast<Timestamp>(seq),
                                     .order_id = static_cast<OrderId>(seq),
                                     .instrument_id = 1,
                                     .price = 60 + seq,
                                     .quantity = 1,
                                     .side = Side::Sell,
                                 }}}),
                "127.0.0.1", kPort);
        }
        auto result = future.get();
        const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
        std::printf("listen wall=%.1f ms  recoveries=%llu  processed=%llu  queue_drops=%zu  hwm=%zu\n", ms,
                    static_cast<unsigned long long>(result.outcome.stats.recoveries),
                    static_cast<unsigned long long>(result.outcome.stats.messages_processed), result.queue_dropped_count,
                    result.queue_high_water_mark);
        if (result.queue_dropped_count > 0) {
            std::printf("catch-up produced a second gap (listen DroppingQueue overflow during recovery).\n");
        }
        std::filesystem::remove(path);
    }
    return EXIT_SUCCESS;
}

int run_soak(const Args& args) {
    double seconds = args.soak_seconds > 0.0 ? args.soak_seconds : args.soak_hours * 3600.0;
    if (seconds <= 0.0) {
        seconds = 2.0;
    }
    std::printf("\n== soak  %.1fs  offered=%.0f/s mixed GTC (latency tracing OFF) ==\n", seconds, args.soak_offered);
    const auto config = mixed_config(std::max(args.operations, static_cast<std::size_t>(50'000)));
    const auto workload = generate_workload(config);
    auto gateway = make_capacity_gateway(args, config.instruments(), kProductionIngest, kProductionOutbound);
    if (!gateway->start()) {
        return EXIT_FAILURE;
    }
    fund_gateway(*gateway, 1, 1, config.instruments());
    WireClient client(1);
    if (!client.connect(*gateway->local_port())) {
        return EXIT_FAILURE;
    }
    for (const auto& command : workload.seed) {
        (void)client.send_command(command);
    }
    (void)client.wait_received(workload.seed.size(), 30s);

    const auto interval = std::chrono::duration<double>(1.0 / args.soak_offered);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::duration<double>(seconds);
    auto next = std::chrono::steady_clock::now();
    auto sample_at = std::chrono::steady_clock::now();
    std::size_t op = 0;
    std::size_t sent = 0;
    std::printf("t_s rss_kb resting holds accounts q_size q_hwm out_drops\n");
    const auto t0 = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() < deadline) {
        while (std::chrono::steady_clock::now() < next) {
            std::this_thread::sleep_for(50us);
        }
        const ExchangeCommand& command = workload.operations[op % workload.operations.size()];
        if (!client.send_command(command)) {
            break;
        }
        ++sent;
        ++op;
        next += std::chrono::duration_cast<std::chrono::steady_clock::duration>(interval);
        if (next < std::chrono::steady_clock::now()) {
            next = std::chrono::steady_clock::now();
        }
        if (std::chrono::steady_clock::now() >= sample_at) {
            const double t = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
            std::printf("%.1f %zu %zu %zu %zu %zu %zu %llu\n", t, rss_bytes() / 1024, gateway->resting_order_count(),
                        gateway->ledger_hold_count(), gateway->ledger_account_count(), gateway->matching_queue_size(),
                        gateway->matching_queue_high_water_mark(),
                        static_cast<unsigned long long>(gateway->io_metrics().outbound_drops));
            sample_at += 30s;
            if (args.quick) {
                sample_at = std::chrono::steady_clock::now() + 500ms;
            }
        }
    }
    std::printf("sent=%zu  final rss=%zu  resting=%zu  holds=%zu  q_hwm=%zu  out_drops=%llu\n", sent, rss_bytes(),
                gateway->resting_order_count(), gateway->ledger_hold_count(), gateway->matching_queue_high_water_mark(),
                static_cast<unsigned long long>(gateway->io_metrics().outbound_drops));
    gateway->stop();
    return EXIT_SUCCESS;
}

} // namespace

int main(int argc, char** argv) {
    const Args args = parse_args(argc, argv);
    if (args.scenario == "help") {
        print_usage();
        return argc == 1 ? EXIT_FAILURE : EXIT_SUCCESS;
    }

    const auto cal = calibrate_timer();
    std::printf("mdh bench_capacity  scenario=%s  quick=%d\n", args.scenario.c_str(), args.quick ? 1 : 0);
    std::printf("timer %s  %.3f MHz  resolution %.1f ns\n", cal.source_name, cal.measured_ticks_per_second / 1e6,
                cal.effective_resolution_ns);
    print_host();

    auto run_one = [&](const std::string& name) -> int {
        if (name == "matching-thread") {
            return run_matching_thread(args);
        }
        if (name == "e2e-knee") {
            return run_e2e_knee(args, cal.measured_ticks_per_second);
        }
        if (name == "connections") {
            return run_connections(args, cal.measured_ticks_per_second);
        }
        if (name == "queue-drops") {
            return run_queue_drops(args);
        }
        if (name == "fairness") {
            return run_fairness(args, cal.measured_ticks_per_second);
        }
        if (name == "recovery") {
            return run_recovery(args);
        }
        if (name == "soak") {
            return run_soak(args);
        }
        std::fprintf(stderr, "unknown scenario: %s\n", name.c_str());
        print_usage();
        return EXIT_FAILURE;
    };

    if (args.scenario == "all") {
        const char* names[] = {"matching-thread", "e2e-knee", "queue-drops", "fairness", "recovery"};
        for (const char* name : names) {
            const int rc = run_one(name);
            if (rc != EXIT_SUCCESS) {
                return rc;
            }
        }
        return EXIT_SUCCESS;
    }
    return run_one(args.scenario);
}
