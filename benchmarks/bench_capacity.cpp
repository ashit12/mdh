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
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <sys/utsname.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <variant>
#include <vector>

#if defined(__APPLE__)
#include <mach/mach.h>
#include <sys/sysctl.h>
#else
// RSS, the CPU model and the thread count all come out of /proc on Linux.
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
#include "exchange/testing/thread_cpu_sampler.hpp"
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

    // Repeated-run discipline, shared by every scenario added for the
    // post-epoll report: no single run is reported as final.
    std::size_t repeats = 5;

    // fok-latency
    std::size_t fok_operations = 20'000;
    std::size_t fok_levels = 4;

    // md-cpu. One rate, held long enough that a per-thread CPU sampler has
    // something to average over -- 4000 samples at 20000/s is 0.2 s, which
    // is fewer than a handful of sampler ticks.
    double md_cpu_rate = 20'000.0;
    double md_cpu_seconds = 5.0;
    std::size_t md_cpu_sample_ms = 20;
    // Which arms to run, by letter. Narrowing to one arm is what makes an
    // external per-thread CPU capture (`ps -M`, `sample`, Instruments)
    // possible: the whole sweep runs its arms back to back in one process, so
    // a capture aimed at the process cannot be aimed at an arm.
    std::string md_cpu_arms = "ABCDE";

    // price-drift
    std::size_t drift_operations = 1'000'000;
    std::size_t drift_window = 100'000;
    // How far the reference mid travels over the whole run, in ticks. The
    // default is three times MatchingBook::kMaxBandTicks, so the run ends
    // with the touch far outside the band the ladder anchored on.
    std::size_t drift_ticks = 3 * 8192;
    std::size_t drift_noise_ticks = 8;
};

void print_usage() {
    std::printf(
        "bench_capacity — capacity and failure-path measurements (Release only)\n"
        "\n"
        "  --scenario matching-thread|e2e-knee|connections|queue-drops|fairness|recovery|soak|\n"
        "             fok-latency|md-realistic|md-cpu|price-drift|all\n"
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
        "  --repeats N                     repeated runs for the scenarios that take a median (default 5)\n"
        "  --fok-operations N              FOK commands in fok-latency (default 20000, half of each outcome)\n"
        "  --fok-levels N                  price levels each FOK reaches (default 4)\n"
        "  --md-cpu-rate R                 md-cpu offered rate (default 20000, the rate that regressed)\n"
        "  --md-cpu-seconds S              md-cpu hold time per arm (default 5)\n"
        "  --md-cpu-sample-ms N            md-cpu per-thread CPU sampling interval (default 20)\n"
        "  --md-cpu-arms ABCDE             md-cpu arms to run (default all; use one for an external capture)\n"
        "  --drift-operations N            price-drift stream length (default 1000000)\n"
        "  --drift-window N                price-drift sampling window (default 100000)\n"
        "  --drift-ticks N                 total upward drift over the run (default 24576 = 3 x band)\n"
        "\n"
        "  all = matching-thread, e2e-knee, queue-drops, fairness, recovery\n"
        "        (not connections, soak, fok-latency, md-realistic, md-cpu or price-drift)\n");
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
        } else if (flag == "--repeats") {
            if (const char* v = next()) {
                args.repeats = static_cast<std::size_t>(std::max(1, std::atoi(v)));
            }
        } else if (flag == "--fok-operations") {
            if (const char* v = next()) {
                args.fok_operations = static_cast<std::size_t>(std::max(2LL, std::atoll(v)));
            }
        } else if (flag == "--fok-levels") {
            if (const char* v = next()) {
                args.fok_levels = static_cast<std::size_t>(std::max(1, std::atoi(v)));
            }
        } else if (flag == "--md-cpu-rate") {
            if (const char* v = next()) {
                args.md_cpu_rate = std::atof(v);
            }
        } else if (flag == "--md-cpu-seconds") {
            if (const char* v = next()) {
                args.md_cpu_seconds = std::atof(v);
            }
        } else if (flag == "--md-cpu-sample-ms") {
            if (const char* v = next()) {
                args.md_cpu_sample_ms = static_cast<std::size_t>(std::max(1, std::atoi(v)));
            }
        } else if (flag == "--md-cpu-arms") {
            if (const char* v = next()) {
                args.md_cpu_arms = v;
            }
        } else if (flag == "--drift-operations") {
            if (const char* v = next()) {
                args.drift_operations = static_cast<std::size_t>(std::max(1LL, std::atoll(v)));
            }
        } else if (flag == "--drift-window") {
            if (const char* v = next()) {
                args.drift_window = static_cast<std::size_t>(std::max(1LL, std::atoll(v)));
            }
        } else if (flag == "--drift-ticks") {
            // Zero is allowed and is the control: the same generator, the
            // same buy-heavy flow, a book that grows the same way, but a mid
            // that never moves -- so the ladder is never left behind. It is
            // what separates "the book got bigger" from "the book left its
            // band" in the windowed numbers.
            if (const char* v = next()) {
                args.drift_ticks = static_cast<std::size_t>(std::max(0LL, std::atoll(v)));
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
        args.repeats = std::min(args.repeats, static_cast<std::size_t>(2));
        args.fok_operations = std::min(args.fok_operations, static_cast<std::size_t>(2'000));
        args.md_cpu_seconds = std::min(args.md_cpu_seconds, 1.0);
        args.drift_operations = std::min(args.drift_operations, static_cast<std::size_t>(100'000));
        args.drift_window = std::min(args.drift_window, static_cast<std::size_t>(10'000));
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

// ── Reproducibility header: machine, build, exact command ─────────────────
//
// Every scenario that writes a bench-results file prints this, because a
// capacity number without the machine under it is not comparable to
// anything. The command line is echoed from argv rather than described in
// prose so that re-running is copy-paste rather than reconstruction.

std::string g_command_line;

[[nodiscard]] std::string sysctl_string(const char* name) {
#if defined(__APPLE__)
    std::size_t len = 0;
    if (sysctlbyname(name, nullptr, &len, nullptr, 0) != 0 || len == 0) {
        return {};
    }
    std::string value(len, '\0');
    if (sysctlbyname(name, value.data(), &len, nullptr, 0) != 0) {
        return {};
    }
    value.resize(len > 0 ? len - 1 : 0);
    return value;
#else
    (void)name;
    return {};
#endif
}

[[nodiscard]] std::string cpu_model() {
#if defined(__APPLE__)
    return sysctl_string("machdep.cpu.brand_string");
#else
    std::ifstream in("/proc/cpuinfo");
    std::string line;
    while (std::getline(in, line)) {
        const auto colon = line.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        const std::string key = line.substr(0, line.find_first_of(" \t"));
        if (key == "model" || key == "Model") {
            if (line.find("model name") != std::string::npos || line.find("model") == 0) {
                return line.substr(colon + 2);
            }
        }
        if (line.rfind("model name", 0) == 0 || line.rfind("Model", 0) == 0) {
            return line.substr(colon + 2);
        }
    }
    return {};
#endif
}

[[nodiscard]] std::string os_description() {
    struct utsname info {};
    if (uname(&info) != 0) {
        return "unknown";
    }
    std::string text = std::string(info.sysname) + " " + info.release + " " + info.machine;
#if defined(__APPLE__)
    const std::string product = sysctl_string("kern.osproductversion");
    if (!product.empty()) {
        text += " (macOS " + product + ")";
    }
#endif
    return text;
}

[[nodiscard]] std::size_t total_memory_bytes() {
#if defined(__APPLE__)
    std::uint64_t bytes = 0;
    std::size_t len = sizeof(bytes);
    if (sysctlbyname("hw.memsize", &bytes, &len, nullptr, 0) != 0) {
        return 0;
    }
    return static_cast<std::size_t>(bytes);
#else
    const long pages = sysconf(_SC_PHYS_PAGES);
    const long page = sysconf(_SC_PAGESIZE);
    return pages > 0 && page > 0 ? static_cast<std::size_t>(pages) * static_cast<std::size_t>(page) : 0;
#endif
}

// Threads this process actually has, according to the OS rather than
// according to what the architecture diagram claims. Native APIs, because a
// benchmark asserting "fewer threads now" has to measure the number, and
// because the equivalent shell command (printed by
// thread_count_cross_check() so a reader can verify by hand) cannot be run
// from inside a timed region.
[[nodiscard]] std::size_t os_thread_count() {
#if defined(__APPLE__)
    thread_act_array_t threads = nullptr;
    mach_msg_type_number_t count = 0;
    if (task_threads(mach_task_self(), &threads, &count) != KERN_SUCCESS) {
        return 0;
    }
    for (mach_msg_type_number_t i = 0; i < count; ++i) {
        (void)mach_port_deallocate(mach_task_self(), threads[i]);
    }
    (void)vm_deallocate(mach_task_self(), reinterpret_cast<vm_address_t>(threads),
                        count * sizeof(thread_t));
    return static_cast<std::size_t>(count);
#else
    std::ifstream in("/proc/self/status");
    std::string key;
    while (in >> key) {
        if (key == "Threads:") {
            std::size_t value = 0;
            if (in >> value) {
                return value;
            }
            return 0;
        }
        std::string rest;
        std::getline(in, rest);
    }
    return 0;
#endif
}

// The same count, taken the way the request asked for it: by asking the
// system tools about this pid from outside the process. Reported next to the
// native figure so the two can be seen to agree.
[[nodiscard]] std::string thread_count_cross_check_command() {
#if defined(__APPLE__)
    return "ps -M " + std::to_string(static_cast<int>(getpid())) + " | tail -n +2 | wc -l";
#else
    return "grep '^Threads:' /proc/" + std::to_string(static_cast<int>(getpid())) + "/status";
#endif
}

// Returns nullopt rather than a number when the tools could not be asked.
// That happens for real at the top of the sweep: popen() has to fork, and
// the whole reason the highest N fails is that the process can no longer
// create threads, so this is exactly where a "0" would be mistaken for a
// measurement. Every own-stdio buffer is flushed first so the child's output
// cannot land ahead of ours in a redirected log.
[[nodiscard]] std::optional<std::size_t> external_thread_count() {
    const std::string command =
#if defined(__APPLE__)
        "ps -M " + std::to_string(static_cast<int>(getpid())) + " 2>/dev/null | tail -n +2 | wc -l 2>/dev/null";
#else
        "ps -L -p " + std::to_string(static_cast<int>(getpid())) + " --no-headers 2>/dev/null | wc -l 2>/dev/null";
#endif
    std::fflush(stdout);
    std::fflush(stderr);
    FILE* pipe = ::popen(command.c_str(), "r");
    if (pipe == nullptr) {
        return std::nullopt;
    }
    char line[64] = {};
    const bool read_ok = std::fgets(line, sizeof(line), pipe) != nullptr;
    const int status = ::pclose(pipe);
    if (!read_ok || status != 0) {
        return std::nullopt;
    }
    char* end = nullptr;
    const unsigned long long value = std::strtoull(line, &end, 10);
    if (end == line || value == 0) {
        return std::nullopt;
    }
    return static_cast<std::size_t>(value);
}

void print_run_header(const char* scenario, std::size_t repeats, const char* statistic) {
    std::printf("=== reproducibility ===\n");
    std::printf("scenario=%s\n", scenario);
    std::printf("command=%s\n", g_command_line.c_str());
    std::printf("build=%s\n",
#if defined(NDEBUG)
                "Release (-O3 -DNDEBUG)"
#else
                "NOT-RELEASE (numbers are not representative)"
#endif
    );
    std::printf("os=%s\n", os_description().c_str());
    std::printf("cpu=%s  logical_cpus=%ld  memory=%.1f GiB\n", cpu_model().c_str(),
                sysconf(_SC_NPROCESSORS_ONLN), static_cast<double>(total_memory_bytes()) / (1024.0 * 1024 * 1024));
    std::printf("repeated_runs=%zu  reported_statistic=%s\n", repeats, statistic);
    std::printf("thread_count_cross_check=%s\n", thread_count_cross_check_command().c_str());
    std::printf("=======================\n");
}

// ── Repeated runs ─────────────────────────────────────────────────────────
//
// Same discipline as the corrected matching-thread ceiling: no single run is
// reported as final. Each scenario below runs its measurement `repeats`
// times and reports the median of each statistic across runs, with the
// per-run values printed above it so a reader can see the spread rather than
// take the median on trust.

[[nodiscard]] double median_of(std::vector<double> values) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const std::size_t mid = values.size() / 2;
    if (values.size() % 2 == 1) {
        return values[mid];
    }
    return (values[mid - 1] + values[mid]) / 2.0;
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

// The order path split at the tracer's stamps, so an end-to-end number that
// moved can be attributed to the stage that moved it. Harvested from the same
// snapshots the end-to-end percentiles come from, since walking them twice
// would be walking a table the matching thread is still writing.
//
// ingest_queue_wait is the one worth naming: t1 is stamped when the gateway
// I/O thread finishes decoding, t2 when the matching thread picks the command
// up, so the gap between them is time spent sitting in the matching ingest
// queue. That is where a matching thread that has become too slow for the
// offered rate shows up, and it is invisible in the total.
struct StageIntervals {
    std::vector<std::uint64_t> client_to_server;    // t0 -> t1
    std::vector<std::uint64_t> ingest_queue_wait;   // t1 -> t2
    std::vector<std::uint64_t> exchange_processing; // t2 -> t3 end
    // Based on t3_first_event, not t3_exchange_end: the first report is
    // produced during processing and can reach the writer before the
    // processor returns, so measuring from exchange_end discards almost every
    // sample as a negative interval.
    std::vector<std::uint64_t> writer_handoff;      // t3 first event -> t4 queued
    std::vector<std::uint64_t> socket_write;        // t4 queued -> t4 written
    std::vector<std::uint64_t> server_to_client;    // t4 written -> t5
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
    StageIntervals stages;
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
    const auto add_stage = [](std::vector<std::uint64_t>& out, std::uint64_t start, std::uint64_t end) {
        if (start == 0 || end == 0 || end < start) {
            return; // a stamp that never landed says nothing about its stage
        }
        out.push_back(end - start);
    };
    for (const auto& [account, id] : keys) {
        const auto snap = latency::tracer().snapshot(account, id);
        if (!snap || snap->t0_client_submit == 0 || snap->t5_client_first == 0 ||
            snap->t5_client_first < snap->t0_client_submit) {
            continue;
        }
        e2e.push_back(snap->t5_client_first - snap->t0_client_submit);
        add_stage(result.stages.client_to_server, snap->t0_client_submit, snap->t1_server_decoded);
        add_stage(result.stages.ingest_queue_wait, snap->t1_server_decoded, snap->t2_exchange_begin);
        add_stage(result.stages.exchange_processing, snap->t2_exchange_begin, snap->t3_exchange_end);
        add_stage(result.stages.writer_handoff, snap->t3_first_event, snap->t4_writer_queued);
        add_stage(result.stages.socket_write, snap->t4_writer_queued, snap->t4_socket_written);
        add_stage(result.stages.server_to_client, snap->t4_socket_written, snap->t5_client_first);
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

// Where the process's threads came from at one N. The gateway runs in this
// same process as the load generator, so "server threads" cannot be read off
// a single total -- it is the delta across gateway construction and start(),
// with the client reader and sender threads accounted separately. Measuring
// it this way is what lets the post-epoll figure be compared against the
// pre-epoll 2N+2 model rather than asserted against it.
struct ThreadPhases {
    std::size_t before_gateway = 0;
    std::size_t after_gateway = 0;
    std::size_t after_connect = 0;
    std::optional<std::size_t> external_after_connect;
};

// `senders` is how many sender threads were actually created, which is not
// always N: at the top of the sweep the spawn loop is the thing that fails.
void print_thread_report(int n, const ThreadPhases& phases, std::size_t peak, int senders) {
    const std::size_t server = phases.after_gateway >= phases.before_gateway
                                    ? phases.after_gateway - phases.before_gateway
                                    : 0;
    const std::size_t readers = phases.after_connect >= phases.after_gateway
                                     ? phases.after_connect - phases.after_gateway
                                     : 0;
    std::printf("THREADS N=%d  server_threads=%zu  pre_epoll_model_2N+2=%d  client_reader_threads=%zu  "
                "sender_threads=%d\n",
                n, server, 2 * n + 2, readers, senders);
    std::printf("THREADS N=%d  process_total before_gateway=%zu  after_gateway=%zu  after_connect=%zu  peak=%zu  ",
                n, phases.before_gateway, phases.after_gateway, phases.after_connect, peak);
    if (phases.external_after_connect.has_value()) {
        std::printf("ps_after_connect=%zu\n", *phases.external_after_connect);
    } else {
        std::printf("ps_after_connect=unavailable (could not fork the cross-check at this N)\n");
    }
}

int run_connections(const Args& args, double ticks_per_second) {
    print_run_header("connections", 1,
                     "single sweep per N (one process per N); thread counts are exact OS counts, not estimates");
    std::printf("\n== connection scaling (low per-connection rate, IoPoller I/O thread) ==\n");
    std::printf("expected server threads = 2 (I/O + matching); this process also has N client reader threads.\n");
    std::printf("server_threads below is measured as the process thread-count delta across gateway "
                "construction+start,\n"
                "because the gateway and the load generator share this process. Cross-check command is in the "
                "header above.\n");

    latency::ScopedEnable tracing(1 << 20);
    for (int n : args.connections) {
        const double aggregate = args.per_connection_rate * static_cast<double>(n);
        std::printf("\n-- N=%d  per-conn=%.0f/s  aggregate_offered=%.0f/s  rss_before=%zu --\n", n,
                    args.per_connection_rate, aggregate, rss_bytes());

        ThreadPhases phases;
        std::size_t peak_threads = 0;
        phases.before_gateway = os_thread_count();
        peak_threads = phases.before_gateway;

        auto gateway =
            make_capacity_gateway(args, std::vector<InstrumentId>{1}, kProductionIngest, kProductionOutbound, {},
                                   std::max(128, n));
        if (!gateway->start()) {
            std::printf("RESULT  N=%d  listen failed\n", n);
            return EXIT_SUCCESS;
        }
        phases.after_gateway = os_thread_count();
        peak_threads = std::max(peak_threads, phases.after_gateway);
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
                    phases.after_connect = os_thread_count();
                    phases.external_after_connect = external_thread_count();
                    print_thread_report(n, phases, std::max(peak_threads, phases.after_connect), 0);
                    gateway->stop();
                    return EXIT_SUCCESS;
                }
                clients.push_back(std::move(client));
                ++connected;
            }
        } catch (const std::exception& ex) {
            std::printf("RESULT  N=%d  thread/create failed after %d connects: %s  rss=%zu\n", n, connected, ex.what(),
                        rss_bytes());
            phases.after_connect = os_thread_count();
            phases.external_after_connect = external_thread_count();
            print_thread_report(n, phases, std::max(peak_threads, phases.after_connect), 0);
            // Worth stating plainly, because the obvious reading of this row
            // is the wrong one: what ran out of threads is this process's own
            // client population -- one reader thread per WireClient -- not
            // the gateway, which is still sitting on the two threads reported
            // above and had accepted every one of those connections.
            std::printf("NOTE    N=%d  the thread that could not be created is a *client* reader thread in this "
                        "harness.\n"
                        "NOTE    N=%d  the gateway accepted %d connections on %zu threads and did not fail; the "
                        "limit reached is the load generator's.\n",
                        n, n, connected,
                        phases.after_gateway >= phases.before_gateway
                            ? phases.after_gateway - phases.before_gateway
                            : 0);
            gateway->stop();
            return EXIT_SUCCESS;
        }
        phases.after_connect = os_thread_count();
        phases.external_after_connect = external_thread_count();
        peak_threads = std::max(peak_threads, phases.after_connect);

        const auto interval = std::chrono::duration<double>(args.per_connection_rate > 0.0
                                                                   ? 1.0 / args.per_connection_rate
                                                                   : 0.0);
        const auto deadline = std::chrono::steady_clock::now() +
                               std::chrono::duration<double>(args.connection_seconds);
        std::atomic<std::size_t> sent{0};
        std::vector<std::thread> senders;
        std::atomic<bool> failed{false};
        // Spawning is guarded for the same reason the connect loop above is:
        // at high N the thread that fails to start is a std::system_error out
        // of the std::thread constructor, and letting it escape aborts the
        // whole process -- taking with it every N still to be measured, and
        // leaving the already-spawned senders unjoined while they hold
        // references to these locals. So the catch stops the senders already
        // running, joins them, and reports this N as a clean failure row.
        int spawned = 0;
        std::string spawn_error;
        try {
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
                ++spawned;
            }
        } catch (const std::exception& ex) {
            spawn_error = ex.what();
            failed.store(true, std::memory_order_relaxed);
        }
        peak_threads = std::max(peak_threads, os_thread_count());
        for (auto& sender : senders) {
            sender.join();
        }
        if (!spawn_error.empty()) {
            std::printf("RESULT  N=%d  sender thread/create failed after %d of %d spawns: %s  connected=%d  "
                        "sent=%zu  rss=%zu\n",
                        n, spawned, n, spawn_error.c_str(), connected, sent.load(), rss_bytes());
            print_thread_report(n, phases, peak_threads, spawned);
            gateway->stop();
            std::printf("stopping the connection sweep after a sender spawn failure\n");
            return EXIT_SUCCESS;
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
        print_thread_report(n, phases, peak_threads, n);
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

// ── FOK isolated latency ──────────────────────────────────────────────────
//
// The mixed 40/25/20/10/5 stream puts FOK at 5% and splits even that between
// filling and rejecting, so FOK's cost is buried twice over: once in the
// mix, once in the two outcomes averaging into one figure. FOK is the only
// order type that walks the book *before* it decides anything
// (MatchingBook::crossable_quantity, the all-or-nothing preflight), which
// makes its two outcomes structurally different operations:
//
//   fill    the preflight walks K levels and succeeds, then the matching
//           walk consumes exactly those levels -- a trade, a book removal
//           and a ledger settlement for every resting order on them.
//   reject  the preflight walks the same K levels, comes up one unit short,
//           and nothing is matched, mutated or settled.
//
// So this scenario sizes every FOK against the book's real depth, which is
// what makes each outcome guaranteed rather than hoped for, and keeps the
// two buckets apart. Both go through the full RiskGatedEngine path --
// sequencer, risk, ledger, engine -- on funded, distinct accounts, with zero
// risk rejections as the pass condition: the same methodology as the
// corrected matching-thread ceiling.

// One side's price-time queue, maintained from the engine's own public event
// stream. The same technique generate_workload uses, and for the same
// reason: sizing a FOK needs current depth, and MatchingEngine::snapshot()
// copies the whole book, so it cannot be consulted per operation.
class LevelQueueModel {
public:
    struct Resting {
        ExchangeOrderId exchange_order_id;
        Quantity quantity;
    };

    explicit LevelQueueModel(Side side) : side_(side) {}

    void add(Price price, ExchangeOrderId exchange_order_id, Quantity quantity) {
        levels_[price].push_back(Resting{exchange_order_id, quantity});
    }

    void reduce(Price price, ExchangeOrderId exchange_order_id, Quantity remaining) {
        auto level = levels_.find(price);
        if (level == levels_.end()) {
            return;
        }
        for (auto& resting : level->second) {
            if (resting.exchange_order_id == exchange_order_id) {
                resting.quantity = remaining;
                return;
            }
        }
    }

    void remove(Price price, ExchangeOrderId exchange_order_id) {
        auto level = levels_.find(price);
        if (level == levels_.end()) {
            return;
        }
        auto& queue = level->second;
        for (auto it = queue.begin(); it != queue.end(); ++it) {
            if (it->exchange_order_id == exchange_order_id) {
                queue.erase(it);
                break;
            }
        }
        if (queue.empty()) {
            levels_.erase(level);
        }
    }

    // The best `count` prices in this side's own priority order, best first.
    // Bids read the map backwards; asks forwards.
    [[nodiscard]] std::vector<Price> best_prices(std::size_t count) const {
        std::vector<Price> prices;
        prices.reserve(count);
        if (side_ == Side::Buy) {
            for (auto it = levels_.rbegin(); it != levels_.rend() && prices.size() < count; ++it) {
                prices.push_back(it->first);
            }
        } else {
            for (auto it = levels_.begin(); it != levels_.end() && prices.size() < count; ++it) {
                prices.push_back(it->first);
            }
        }
        return prices;
    }

    [[nodiscard]] const std::vector<Resting>& at(Price price) const { return levels_.find(price)->second; }
    void erase_level(Price price) { levels_.erase(price); }
    [[nodiscard]] std::size_t level_count() const { return levels_.size(); }

private:
    Side side_;
    std::map<Price, std::vector<Resting>> levels_;
};

struct FokRunResult {
    LatencySummary fill;
    LatencySummary reject;
    std::size_t liquidity_rejects = 0;
    std::size_t risk_rejects = 0;
    std::size_t other_rejects = 0;
    std::size_t skipped_thin_book = 0;
    double mean_orders_per_fill = 0.0;
    std::size_t levels_per_fok = 0;
    std::size_t resting_after_seed = 0;
    std::size_t resting_at_end = 0;
    bool outcomes_confirmed = false;
};

[[nodiscard]] FokRunResult run_fok_once(const Args& args, double ticks_per_second) {
    FokRunResult out;
    out.levels_per_fok = args.fok_levels;

    // operation_count 0: this takes the mixed generator's *seed* phase only,
    // which is the whole point -- the same 1000 orders per side over the same
    // 64-tick band the mixed workload starts from, and then nothing but FOK.
    const WorkloadConfig config = mixed_config(0, args.matching_accounts);
    const auto workload = generate_workload(config);
    constexpr InstrumentId kInstrument = 1;

    MatchingEngine engine(config.instruments(), 200'000);
    ledger::Ledger ledger;
    fund_ledger(ledger, config, kMatchingFundCash, kMatchingFundPosition);
    risk::RiskGatedEngine gated(engine, ledger);
    sequencing::CommandSequencer sequencer;

    std::array<std::size_t, static_cast<std::size_t>(RejectReason::AccountMismatch) + 1> rejects{};
    LevelQueueModel bids(Side::Buy);
    LevelQueueModel asks(Side::Sell);
    const auto side_model = [&](Side side) -> LevelQueueModel& { return side == Side::Buy ? bids : asks; };

    // Untimed sink. The seed and replenishment phases run through this so the
    // model learns the exchange order ids and quantities that the measured
    // FOKs are then sized against.
    const EventSink learn = [&](const ExchangeEvent& event) {
        std::visit(
            [&](const auto& ev) {
                using T = std::decay_t<decltype(ev)>;
                if constexpr (std::is_same_v<T, BookOrderAdded>) {
                    side_model(ev.side).add(ev.price, ev.exchange_order_id, ev.quantity);
                } else if constexpr (std::is_same_v<T, BookOrderReduced>) {
                    side_model(ev.side).reduce(ev.price, ev.exchange_order_id, ev.new_remaining_quantity);
                } else if constexpr (std::is_same_v<T, BookOrderRemoved>) {
                    side_model(ev.side).remove(ev.price, ev.exchange_order_id);
                } else if constexpr (std::is_same_v<T, OrderRejected>) {
                    ++rejects[static_cast<std::size_t>(ev.reason)];
                }
            },
            event);
    };

    // Timed sink. Deliberately the same shape as the one the corrected
    // matching-thread ceiling times through -- one get_if per event, no
    // bookkeeping -- so these ns/op are comparable with that figure. The
    // book model is brought up to date afterwards, outside the timed region,
    // from what a *guaranteed* outcome implies: a fill consumes exactly the
    // levels it was sized to consume, and a reject changes nothing at all.
    const EventSink measured = [&](const ExchangeEvent& event) {
        if (const auto* rejected = std::get_if<OrderRejected>(&event); rejected != nullptr) {
            ++rejects[static_cast<std::size_t>(rejected->reason)];
        }
    };

    for (const auto& command : workload.seed) {
        gated.process(sequencer.sequence(command), learn);
    }
    out.resting_after_seed = engine.resting_order_count();

    // Clear of every id the seed used, so nothing here is a duplicate.
    SequentialIds ids;
    ids.next_client_order_id = 1'000'000;

    std::vector<std::uint64_t> fill_samples;
    std::vector<std::uint64_t> reject_samples;
    fill_samples.reserve(args.fok_operations / 2 + 1);
    reject_samples.reserve(args.fok_operations / 2 + 1);
    std::size_t orders_consumed = 0;
    std::vector<std::pair<Price, Quantity>> consumed;

    for (std::size_t op = 0; op < args.fok_operations; ++op) {
        // Exactly half of each outcome, and the taker side alternates every
        // pair so both sides of the book see the same treatment.
        const bool want_fill = (op % 2) == 0;
        const Side taker_side = ((op / 2) % 2) == 0 ? Side::Buy : Side::Sell;
        LevelQueueModel& contra = side_model(taker_side == Side::Buy ? Side::Sell : Side::Buy);

        const auto prices = contra.best_prices(args.fok_levels);
        if (prices.size() < args.fok_levels) {
            ++out.skipped_thin_book;
            continue;
        }

        // Everything reachable at the K-th best price, which is exactly what
        // the preflight will find: the levels past it are worse than the
        // limit, so they break its walk.
        Quantity reachable = 0;
        consumed.clear();
        for (const Price price : prices) {
            for (const auto& resting : contra.at(price)) {
                reachable += resting.quantity;
                consumed.emplace_back(price, resting.quantity);
            }
        }
        if (reachable == 0) {
            ++out.skipped_thin_book;
            continue;
        }

        const Price limit = prices.back();
        const Quantity quantity = want_fill ? reachable : reachable + 1;
        const AccountId taker = static_cast<AccountId>(1 + (op % config.account_count));
        const ExchangeCommand command = sequencer.sequence(ExchangeCommand{
            new_order(0, taker, ids.take_client_order_id(), kInstrument, taker_side, limit, quantity,
                      TimeInForce::FOK)});

        const std::uint64_t t0 = timer_ticks();
        gated.process(command, measured);
        const std::uint64_t t1 = timer_ticks();

        if (!want_fill) {
            reject_samples.push_back(t1 - t0);
            continue;
        }
        fill_samples.push_back(t1 - t0);
        orders_consumed += consumed.size();
        for (const Price price : prices) {
            contra.erase_level(price);
        }
        // Put the same multiset of (price, quantity) back, untimed, so the
        // next measured FOK meets the book shape this one did rather than a
        // progressively thinner one. Ids and queue positions differ; depth
        // per level does not, which is what the measurement depends on.
        const Side maker_side = taker_side == Side::Buy ? Side::Sell : Side::Buy;
        for (const auto& [price, replenish_quantity] : consumed) {
            const AccountId maker = static_cast<AccountId>(1 + (ids.next_client_order_id % config.account_count));
            gated.process(sequencer.sequence(ExchangeCommand{new_order(0, maker, ids.take_client_order_id(),
                                                                        kInstrument, maker_side, price,
                                                                        replenish_quantity)}),
                          learn);
        }
    }

    out.resting_at_end = engine.resting_order_count();
    out.mean_orders_per_fill =
        fill_samples.empty() ? 0.0 : static_cast<double>(orders_consumed) / static_cast<double>(fill_samples.size());
    out.liquidity_rejects = rejects[static_cast<std::size_t>(RejectReason::InsufficientLiquidity)];
    out.risk_rejects = rejects[static_cast<std::size_t>(RejectReason::InsufficientFunds)] +
                        rejects[static_cast<std::size_t>(RejectReason::InsufficientPosition)] +
                        rejects[static_cast<std::size_t>(RejectReason::OrderTooLarge)];
    for (std::size_t reason = 1; reason < rejects.size(); ++reason) {
        if (reason != static_cast<std::size_t>(RejectReason::InsufficientLiquidity) &&
            reason != static_cast<std::size_t>(RejectReason::InsufficientFunds) &&
            reason != static_cast<std::size_t>(RejectReason::InsufficientPosition) &&
            reason != static_cast<std::size_t>(RejectReason::OrderTooLarge)) {
            out.other_rejects += rejects[reason];
        }
    }
    // Every reject-bucket FOK must have produced exactly one
    // InsufficientLiquidity, and no fill-bucket FOK may have produced any.
    out.outcomes_confirmed = out.liquidity_rejects == reject_samples.size() && out.risk_rejects == 0 &&
                              out.other_rejects == 0 && !fill_samples.empty();
    out.fill = summarise_latency(fill_samples, ticks_per_second);
    out.reject = summarise_latency(reject_samples, ticks_per_second);
    return out;
}

int run_fok_latency(const Args& args, const TimerCalibration& cal) {
    print_run_header("fok-latency", args.repeats, "median across runs of each per-bucket statistic");
    std::printf("\n== FOK isolated latency (full RiskGatedEngine path, no sockets) ==\n");
    std::printf("book seeded at the mixed generator's own depth: %zu orders/side, %lld-tick band, "
                "instrument 1\n",
                static_cast<std::size_t>(1'000), static_cast<long long>(64));
    std::printf("each FOK reaches K=%zu price levels; fill is sized to exactly the reachable quantity, "
                "reject to that plus one\n",
                args.fok_levels);
    std::printf("fok_operations=%zu (50%% fill / 50%% InsufficientLiquidity, alternating; taker side alternates "
                "every pair)\n",
                args.fok_operations);
    std::printf("accounts=%u distinct and funded; pass condition is risk_rejected_events == 0\n",
                args.matching_accounts);
    std::printf("timer: effective resolution %.1f ns, zero-work interval p50 %.1f ns -- every per-operation "
                "sample below carries that floor\n",
                cal.effective_resolution_ns, cal.empty_interval_p50_ns);

    std::vector<FokRunResult> runs;
    runs.reserve(args.repeats);
    std::printf("\n| run | bucket | samples | p50 ns | p90 ns | p99 ns | p99.9 ns | mean ns/op | "
                "orders/fill | liq_rejects | risk_rejects | confirmed |\n");
    for (std::size_t r = 0; r < args.repeats; ++r) {
        auto result = run_fok_once(args, cal.measured_ticks_per_second);
        for (int bucket = 0; bucket < 2; ++bucket) {
            const LatencySummary& s = bucket == 0 ? result.fill : result.reject;
            std::printf("| %3zu | %-6s | %7zu | %6.0f | %6.0f | %6.0f | %8.0f | %10.0f | %11.1f | %11zu | %12zu | "
                        "%9d |\n",
                        r + 1, bucket == 0 ? "fill" : "reject", s.count, s.p50_ns, s.p90_ns, s.p99_ns, s.p999_ns,
                        s.sampled_mean_ns, bucket == 0 ? result.mean_orders_per_fill : 0.0, result.liquidity_rejects,
                        result.risk_rejects, static_cast<int>(result.outcomes_confirmed));
        }
        runs.push_back(std::move(result));
    }

    const auto pick = [&](bool fill, double LatencySummary::* field) {
        std::vector<double> values;
        values.reserve(runs.size());
        for (const auto& run : runs) {
            values.push_back((fill ? run.fill : run.reject).*field);
        }
        return median_of(std::move(values));
    };

    std::printf("\n-- median of %zu runs --\n", args.repeats);
    std::printf("| bucket | p50 ns | p90 ns | p99 ns | p99.9 ns | mean ns/op |\n");
    for (int bucket = 0; bucket < 2; ++bucket) {
        const bool fill = bucket == 0;
        std::printf("| %-6s | %6.0f | %6.0f | %6.0f | %8.0f | %10.0f |\n", fill ? "fill" : "reject",
                    pick(fill, &LatencySummary::p50_ns), pick(fill, &LatencySummary::p90_ns),
                    pick(fill, &LatencySummary::p99_ns), pick(fill, &LatencySummary::p999_ns),
                    pick(fill, &LatencySummary::sampled_mean_ns));
    }

    const double fill_median = pick(true, &LatencySummary::p50_ns);
    const double reject_median = pick(false, &LatencySummary::p50_ns);
    std::printf("\nFOK_FILL_MEDIAN_P50 %.0f ns  FOK_REJECT_MEDIAN_P50 %.0f ns  ratio %.2fx\n", fill_median,
                reject_median, reject_median > 0.0 ? fill_median / reject_median : 0.0);
    std::printf("orders consumed per fill (median run): %.1f  -- that is the trade/removal/settlement work the\n"
                "reject bucket does not do, and it is what the gap between the two buckets buys.\n",
                runs.empty() ? 0.0 : runs[runs.size() / 2].mean_orders_per_fill);

    bool all_confirmed = true;
    for (const auto& run : runs) {
        all_confirmed = all_confirmed && run.outcomes_confirmed;
    }
    std::printf("outcomes_confirmed_every_run=%d  resting_after_seed=%zu  resting_at_end=%zu  "
                "skipped_thin_book=%zu\n",
                static_cast<int>(all_confirmed), runs.empty() ? 0 : runs.front().resting_after_seed,
                runs.empty() ? 0 : runs.front().resting_at_end, runs.empty() ? 0 : runs.front().skipped_thin_book);
    if (!all_confirmed) {
        std::printf("INVALID_RUN: an outcome was not what it was sized to be, or a risk rejection fired.\n");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

// ── Market-data drop rate at a realistic offered rate ─────────────────────
//
// The existing MarketDataRouter row in queue-drops is driven by the
// matching-thread flood: commands pushed in as fast as one thread can push
// them, which is several times any rate a client population actually
// offers. It answers "what happens when the feed is overrun", and it is
// worth keeping, but it says nothing about whether the feed keeps up under
// normal load -- and those are different questions with, it turns out,
// different answers.
//
// So this runs the router behind the *e2e-knee* traffic instead: the same
// mixed GTC stream, over real TCP, paced at the same offered rates, with the
// router wired in where production wires it (extra_event_sink) and a real
// UDP subscriber on the other end.

class UdpFrameSink {
public:
    explicit UdpFrameSink(std::uint16_t port) : port_(port) {}

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

struct MdRateRow {
    double offered = 0.0;
    double achieved = 0.0;
    std::size_t dropped = 0;
    std::size_t hwm = 0;
    std::uint64_t routed = 0;
    double p50_us = 0.0;
    double p99_us = 0.0;
    double p999_us = 0.0;
    std::uint64_t sequence_failures = 0;
    bool tracked = false;
};

[[nodiscard]] std::vector<MdRateRow> run_md_realistic_once(const Args& args, double ticks_per_second) {
    std::vector<MdRateRow> rows;
    const auto config = mixed_config(std::max(args.operations, args.e2e_samples * 8 + 4'000));
    const auto workload = generate_workload(config);

    latency::ScopedEnable tracing(1 << 22);

    trader::strategies::StrategyRuntime runtime;
    trader::market_data::FeedSubscriberOptions sub_opts;
    sub_opts.replay_options.stop_on_sequence_error = false;
    trader::market_data::FeedSubscriber subscriber(0, runtime, sub_opts);
    if (!subscriber.start()) {
        return rows;
    }
    UdpFrameSink udp(*subscriber.local_port());
    exchange::market_data::MarketDataRouter router(
        [&](const protocol::Event& event) { udp.send(event); },
        exchange::market_data::MarketDataRouterOptions{.queue_capacity = kProductionMdQueue, .idle_timeout = 1ms});
    router.start();

    auto gateway = make_capacity_gateway(args, config.instruments(), kProductionIngest, kProductionOutbound,
                                          router.sink());
    if (!gateway->start()) {
        router.stop();
        subscriber.stop();
        return rows;
    }
    fund_gateway(*gateway, 1, static_cast<int>(config.account_count), config.instruments());
    WireClient client(1);
    if (!client.connect(*gateway->local_port())) {
        gateway->stop();
        router.stop();
        subscriber.stop();
        return rows;
    }
    for (const auto& command : workload.seed) {
        (void)client.send_command(command);
    }
    (void)client.wait_received(workload.seed.size(), 30s);

    // The same rate ladder e2e-knee walks, so these rows sit directly
    // alongside its own.
    double rate = args.e2e_start_rate;
    std::size_t offset = 0;
    const std::size_t steps = args.quick ? 2 : 4;
    for (std::size_t step = 0; step < steps; ++step) {
        if (offset + args.e2e_samples > workload.operations.size()) {
            break;
        }
        std::vector<ExchangeCommand> slice(workload.operations.begin() + static_cast<std::ptrdiff_t>(offset),
                                            workload.operations.begin() +
                                                static_cast<std::ptrdiff_t>(offset + args.e2e_samples));
        offset += args.e2e_samples;

        const std::size_t dropped_before = router.dropped_count();
        const std::uint64_t routed_before = router.routed_count();
        const std::uint64_t failures_before = subscriber.stats().sequence_failures;
        const auto pace = run_paced_mixed(*gateway, client, slice, rate, args.e2e_samples, ticks_per_second);
        // The routing thread is asynchronous, so give it the chance to drain
        // what the matching thread queued before reading its counters.
        std::this_thread::sleep_for(200ms);

        MdRateRow row;
        row.offered = pace.offered;
        row.achieved = pace.achieved;
        row.p50_us = pace.p50_us;
        row.p99_us = pace.p99_us;
        row.p999_us = pace.p999_us;
        row.dropped = router.dropped_count() - dropped_before;
        row.hwm = router.queue_high_water_mark();
        row.routed = router.routed_count() - routed_before;
        row.sequence_failures = subscriber.stats().sequence_failures - failures_before;
        row.tracked = pace.achieved >= args.knee_track * pace.offered;
        rows.push_back(row);
        rate *= 2.0;
    }

    gateway->stop();
    std::this_thread::sleep_for(200ms);
    router.stop();
    subscriber.stop();
    return rows;
}

int run_md_realistic(const Args& args, double ticks_per_second) {
    print_run_header("md-realistic", args.repeats, "median across runs, per offered rate");
    std::printf("\n== MarketDataRouter drop rate at REALISTIC offered rates (e2e-knee traffic) ==\n");
    std::printf("mixed GTC over TCP, production queues ingest=%zu outbound=%zu, router cap=%zu, real UDP "
                "subscriber\n",
                kProductionIngest, kProductionOutbound, kProductionMdQueue);
    std::printf("THIS IS NOT THE ESCALATED FLOOD. The 'MarketDataRouter DroppingQueue' row in --scenario "
                "queue-drops\n"
                "drives the router from an in-process matching-thread flood; that measures overrun behaviour.\n"
                "These rows are the client-paced rates e2e-knee sweeps, which is what normal load looks like.\n");

    std::vector<std::vector<MdRateRow>> runs;
    runs.reserve(args.repeats);
    std::printf("\n| run | offered/s | achieved/s | tracks | md_dropped | md_hwm | md_routed | feed_seq_fail | "
                "p50 us | p99 us | p99.9 us |\n");
    for (std::size_t r = 0; r < args.repeats; ++r) {
        auto rows = run_md_realistic_once(args, ticks_per_second);
        if (rows.empty()) {
            std::fprintf(stderr, "md-realistic: setup failed on run %zu\n", r + 1);
            return EXIT_FAILURE;
        }
        for (const auto& row : rows) {
            std::printf("| %3zu | %9.0f | %10.0f | %6d | %10zu | %6zu | %9llu | %13llu | %6.1f | %6.1f | %8.1f |\n",
                        r + 1, row.offered, row.achieved, static_cast<int>(row.tracked), row.dropped, row.hwm,
                        static_cast<unsigned long long>(row.routed),
                        static_cast<unsigned long long>(row.sequence_failures), row.p50_us, row.p99_us,
                        row.p999_us);
        }
        runs.push_back(std::move(rows));
    }

    std::size_t rate_count = 0;
    for (const auto& rows : runs) {
        rate_count = std::max(rate_count, rows.size());
    }
    std::printf("\n-- median of %zu runs --\n", args.repeats);
    std::printf("| offered/s | achieved/s | md_dropped | md_hwm | md_routed | drop_pct_of_routed | feed_seq_fail |\n");
    std::size_t total_dropped_median = 0;
    for (std::size_t i = 0; i < rate_count; ++i) {
        std::vector<double> offered;
        std::vector<double> achieved;
        std::vector<double> dropped;
        std::vector<double> hwm;
        std::vector<double> routed;
        std::vector<double> failures;
        for (const auto& rows : runs) {
            if (i >= rows.size()) {
                continue;
            }
            offered.push_back(rows[i].offered);
            achieved.push_back(rows[i].achieved);
            dropped.push_back(static_cast<double>(rows[i].dropped));
            hwm.push_back(static_cast<double>(rows[i].hwm));
            routed.push_back(static_cast<double>(rows[i].routed));
            failures.push_back(static_cast<double>(rows[i].sequence_failures));
        }
        const double dropped_median = median_of(dropped);
        const double routed_median = median_of(routed);
        total_dropped_median += static_cast<std::size_t>(dropped_median);
        std::printf("| %9.0f | %10.0f | %10.0f | %6.0f | %9.0f | %17.3f%% | %13.0f |\n", median_of(offered),
                    median_of(achieved), dropped_median, median_of(hwm), routed_median,
                    routed_median > 0.0 ? 100.0 * dropped_median / routed_median : 0.0, median_of(failures));
    }

    std::printf("\nVERDICT  ");
    if (total_dropped_median == 0) {
        std::printf("drops are ZERO at every realistic offered rate in this sweep. The feed keeps up under\n"
                    "normal load; the large drop count in queue-drops is an overrun-only behaviour and should "
                    "not be\n"
                    "read as a production feed-loss rate.\n");
    } else {
        std::printf("drops are NON-TRIVIAL at realistic offered rates (median total %zu across the sweep).\n"
                    "This is a real problem, separate from the known extreme-flood behaviour: a client at a "
                    "normal\n"
                    "rate is already losing feed events and seeing sequence gaps.\n",
                    total_dropped_median);
    }
    return EXIT_SUCCESS;
}

// ── Attributing the router's latency cost ─────────────────────────────────
//
// md-realistic shows the p50 at 20000/s jumping from tens of microseconds to
// milliseconds once the router is attached, but it cannot say why, for two
// reasons. It never runs the same rate without the router in the same
// process, so the comparison crosses a process boundary; and it reports only
// end-to-end latency, which is the sum of every possible cause.
//
// This scenario holds one offered rate long enough to sample, and runs it
// five ways back to back:
//
//   A  no router           -- the baseline, same binary, same process
//   B  router + UDP        -- the regression, uninstrumented so it is honest
//   C  B instrumented      -- publish() timed step by step
//   D  router, null sink   -- the router's own machinery with no UDP hop
//   E  D instrumented      -- publish() timed with an idle consumer
//
// B is the number to quote. C exists because the interesting hypotheses are
// about steps inside publish(), and timing them costs the matching thread
// two tick reads per step -- so the attribution and the reproduction cannot
// come from the same run.
//
// D is what makes the result causal rather than correlational. Attaching the
// router adds three things at once: a translation and a queue push on the
// matching thread, a routing thread, and one loopback UDP datagram per event
// sent by that thread and received by a subscriber. B cannot tell those
// apart. D keeps the first two and drops the third, so whatever B has that D
// does not is the UDP hop -- which shares the loopback stack with the TCP
// order path being measured.
//
// E is the control for the push cost itself. C and E push into the same queue
// from the same thread with the same code; the only difference is what the
// consumer is doing on the other core -- hammering the loopback stack in C,
// nearly idle in E. Since the producer must read the consumer's `tail_` on
// every push, that is exactly the variable that decides whether a slow push
// is the queue waiting (it cannot) or the memory system stalling (it can).
//
// Per-thread CPU is sampled across all five, and the order path is split at
// the tracer's stamps, because "the matching thread got slower" and "the
// machine ran out of cores" and "the wire got slower" are three different
// answers and the end-to-end number is the sum of all of them.

struct MdCpuArm {
    const char* label = "";
    bool router = false;
    bool instrumented = false;
    bool udp = false;

    double offered = 0.0;
    double achieved = 0.0;
    double p50_us = 0.0;
    double p99_us = 0.0;
    double p999_us = 0.0;
    std::size_t harvested = 0;
    std::size_t matching_queue_hwm = 0;
    std::uint64_t outbound_drops = 0;
    StageIntervals stages;

    // Router-only.
    std::size_t md_dropped = 0;
    std::size_t md_hwm = 0;
    std::uint64_t md_routed = 0;
    std::uint64_t cv_waits = 0;
    exchange::market_data::PublishCostStats cost;

    testing::ThreadCpuReport cpu;
};

// One arm. `with_router` decides whether a MarketDataRouter is wired into
// extra_event_sink at all; `with_udp` decides whether its downstream sink
// actually sends a datagram or discards the event.
[[nodiscard]] std::optional<MdCpuArm> run_md_cpu_arm(const Args& args, double ticks_per_second, const char* label,
                                                       bool with_router, bool with_udp, bool instrumented,
                                                       double rate, std::size_t samples) {
    MdCpuArm arm;
    arm.label = label;
    arm.router = with_router;
    arm.instrumented = instrumented;
    arm.udp = with_udp;

    const auto config = mixed_config(samples + 8'000);
    const auto workload = generate_workload(config);
    if (workload.operations.size() < samples) {
        return std::nullopt;
    }

    latency::ScopedEnable tracing(1 << 22);

    // The subscriber and the UDP hop exist in both arms' *intent* but only
    // the router arm has anything to send them; they are constructed only
    // when used, so arm A carries no threads the production no-router
    // configuration would not have.
    trader::strategies::StrategyRuntime runtime;
    trader::market_data::FeedSubscriberOptions sub_opts;
    sub_opts.replay_options.stop_on_sequence_error = false;
    std::unique_ptr<trader::market_data::FeedSubscriber> subscriber;
    std::unique_ptr<UdpFrameSink> udp;
    std::unique_ptr<exchange::market_data::MarketDataRouter> router;

    if (with_router) {
        exchange::market_data::MarketDataSink downstream = [](const protocol::Event&) {};
        if (with_udp) {
            subscriber = std::make_unique<trader::market_data::FeedSubscriber>(0, runtime, sub_opts);
            if (!subscriber->start()) {
                return std::nullopt;
            }
            udp = std::make_unique<UdpFrameSink>(*subscriber->local_port());
            downstream = [&](const protocol::Event& event) { udp->send(event); };
        }
        router = std::make_unique<exchange::market_data::MarketDataRouter>(
            std::move(downstream), exchange::market_data::MarketDataRouterOptions{
                                        .queue_capacity = kProductionMdQueue,
                                        .idle_timeout = 1ms,
                                        .measure_publish_cost = instrumented,
                                    });
        router->start();
    }

    auto gateway = make_capacity_gateway(args, config.instruments(), kProductionIngest, kProductionOutbound,
                                          router ? router->sink() : EventSink{});
    const auto teardown = [&] {
        gateway->stop();
        std::this_thread::sleep_for(200ms);
        if (router) {
            router->stop();
        }
        if (subscriber) {
            subscriber->stop();
        }
    };
    if (!gateway->start()) {
        if (router) {
            router->stop();
        }
        if (subscriber) {
            subscriber->stop();
        }
        return std::nullopt;
    }
    fund_gateway(*gateway, 1, static_cast<int>(config.account_count), config.instruments());
    WireClient client(1);
    if (!client.connect(*gateway->local_port())) {
        teardown();
        return std::nullopt;
    }
    for (const auto& command : workload.seed) {
        (void)client.send_command(command);
    }
    (void)client.wait_received(workload.seed.size(), 30s);

    std::vector<ExchangeCommand> slice(workload.operations.begin(),
                                        workload.operations.begin() + static_cast<std::ptrdiff_t>(samples));

    // Sampling starts after the seed and after the connect, so the window
    // holds the paced load and nothing else.
    testing::ThreadCpuSampler sampler(std::chrono::milliseconds(args.md_cpu_sample_ms));
    sampler.start();
    const auto pace = run_paced_mixed(*gateway, client, slice, rate, samples, ticks_per_second);
    arm.cpu = sampler.stop();

    arm.offered = pace.offered;
    arm.achieved = pace.achieved;
    arm.p50_us = pace.p50_us;
    arm.p99_us = pace.p99_us;
    arm.p999_us = pace.p999_us;
    arm.harvested = pace.harvested;
    arm.matching_queue_hwm = pace.queue_hwm;
    arm.outbound_drops = pace.outbound_drops;
    arm.stages = std::move(pace.stages);

    if (router) {
        std::this_thread::sleep_for(200ms); // let the routing thread drain before reading its counters
        arm.md_dropped = router->dropped_count();
        arm.md_hwm = router->queue_high_water_mark();
        arm.md_routed = router->routed_count();
        arm.cv_waits = router->cv_wait_count();
        arm.cost = router->publish_cost();
    }

    teardown();
    return arm;
}

void print_cpu_table(const testing::ThreadCpuReport& report, long physical_cores) {
    if (!report.supported) {
        std::printf("  per-thread CPU unavailable: %s\n", report.unsupported_reason.c_str());
        return;
    }
    // The window covers the paced send plus the drain that follows it, so
    // "mean core" is diluted by however long the drain took -- which is
    // itself longer in the arms that are slower. "peak core" is the number to
    // read for saturation; the mean is for total work done.
    std::printf("  window %.3f s, %llu samples\n", static_cast<double>(report.window_ns) / 1e9,
                static_cast<unsigned long long>(report.ticks));
    std::printf("  %-18s | %10s | %9s | %9s | %8s | %s\n", "thread", "cpu ms", "mean core", "peak core", "threads",
                "runnable ticks");
    for (const auto& thread : report.threads) {
        if (thread.cpu_ns < 1'000'000 && thread.name != "(unnamed)") {
            continue; // under a millisecond over the whole window: not a participant
        }
        std::printf("  %-18s | %10.1f | %8.2f%% | %8.2f%% | %8llu | %llu/%llu\n", thread.name.c_str(),
                    static_cast<double>(thread.cpu_ns) / 1e6, thread.mean_core_fraction * 100.0,
                    thread.peak_core_fraction * 100.0, static_cast<unsigned long long>(thread.distinct_threads),
                    static_cast<unsigned long long>(thread.runnable_observations),
                    static_cast<unsigned long long>(thread.observations));
    }
    std::printf("  runnable threads: peak %llu, mean %.2f, against %ld physical cores -> %s\n",
                static_cast<unsigned long long>(report.peak_runnable_threads), report.mean_runnable_threads,
                physical_cores,
                static_cast<long>(report.peak_runnable_threads) > physical_cores ? "OVERSUBSCRIBED at peak"
                                                                                  : "within core count");
}

void print_stage_table(const StageIntervals& stages, double ticks_per_second) {
    std::printf("  %-20s | %8s | %9s | %9s | %9s\n", "stage", "n", "p50 us", "p99 us", "max us");
    const auto row = [&](const char* name, std::vector<std::uint64_t> samples) { // summarise_latency sorts in place
        const auto summary = summarise_latency(samples, ticks_per_second);
        if (summary.count == 0) {
            std::printf("  %-20s | %8s | %9s | %9s | %9s\n", name, "-", "-", "-", "-");
            return;
        }
        std::printf("  %-20s | %8zu | %9.1f | %9.1f | %9.1f\n", name, summary.count, summary.p50_ns / 1000.0,
                    summary.p99_ns / 1000.0, summary.max_ns / 1000.0);
    };
    row("t0->t1 client->srv", stages.client_to_server);
    row("t1->t2 ingest queue", stages.ingest_queue_wait);
    row("t2->t3 matching", stages.exchange_processing);
    row("t3a->t4a writer hand", stages.writer_handoff);
    row("t4a->t4 socket write", stages.socket_write);
    row("t4->t5 srv->client", stages.server_to_client);
}

void print_publish_cost(const exchange::market_data::PublishCostStats& cost, double ticks_per_second) {
    const double ns_per_tick = ticks_per_second > 0.0 ? 1e9 / ticks_per_second : 0.0;
    const auto mean_ns = [&](std::uint64_t total, std::uint64_t count) {
        return count == 0 ? 0.0 : static_cast<double>(total) * ns_per_tick / static_cast<double>(count);
    };

    std::printf("  publish() on the matching thread: %llu events in, %llu became wire events\n",
                static_cast<unsigned long long>(cost.events_seen), static_cast<unsigned long long>(cost.wire_events));
    std::printf("  %-22s | %12s | %12s | %14s\n", "step", "mean ns", "max ns", "calls");
    std::printf("  %-22s | %12.1f | %12.1f | %14llu\n", "translate to wire",
                mean_ns(cost.translate_ticks_total, cost.events_seen),
                static_cast<double>(cost.translate_ticks_max) * ns_per_tick,
                static_cast<unsigned long long>(cost.events_seen));
    std::printf("  %-22s | %12.1f | %12.1f | %14llu\n", "DroppingQueue::push",
                mean_ns(cost.push_ticks_total, cost.wire_events),
                static_cast<double>(cost.push_ticks_max) * ns_per_tick,
                static_cast<unsigned long long>(cost.wire_events));
    std::printf("  %-22s | %12.1f | %12.1f | %14llu\n", "wake_cv_.notify_one",
                mean_ns(cost.notify_ticks_total, cost.notify_calls),
                static_cast<double>(cost.notify_ticks_max) * ns_per_tick,
                static_cast<unsigned long long>(cost.notify_calls));
    std::printf("  %-22s | %12.1f | %12.1f | %14llu\n", "(control: empty pair)",
                mean_ns(cost.control_ticks_total, cost.events_seen),
                static_cast<double>(cost.control_ticks_max) * ns_per_tick,
                static_cast<unsigned long long>(cost.events_seen));
    std::printf("  pushes accepted %llu, refused (queue full, event dropped) %llu\n",
                static_cast<unsigned long long>(cost.push_ok), static_cast<unsigned long long>(cost.push_full));
    std::printf("  the control row is two adjacent tick reads with no work between them, on the same thread:\n"
                "  its maximum is what a preemption costs, and it bounds what any single maximum above proves.\n");

    std::printf("\n  push cost distribution (tick source resolution %.1f ns, so bucket 0 is a sub-tick push):\n",
                ns_per_tick);
    std::uint64_t at_or_over_micro = 0;
    for (std::size_t bucket = 0; bucket < exchange::market_data::kPushBucketCount; ++bucket) {
        if (cost.push_buckets[bucket] == 0) {
            continue;
        }
        const double lower_ns =
            static_cast<double>(exchange::market_data::push_bucket_lower_edge_ticks(bucket)) * ns_per_tick;
        const bool last = bucket + 1 == exchange::market_data::kPushBucketCount;
        const double upper_ns =
            static_cast<double>(exchange::market_data::push_bucket_lower_edge_ticks(bucket + 1)) * ns_per_tick;
        const double share =
            cost.wire_events == 0
                ? 0.0
                : 100.0 * static_cast<double>(cost.push_buckets[bucket]) / static_cast<double>(cost.wire_events);
        if (last) {
            std::printf("    >= %9.0f ns           | %12llu | %7.4f%%\n", lower_ns,
                        static_cast<unsigned long long>(cost.push_buckets[bucket]), share);
        } else {
            std::printf("    %9.0f .. %9.0f ns | %12llu | %7.4f%%\n", lower_ns, upper_ns,
                        static_cast<unsigned long long>(cost.push_buckets[bucket]), share);
        }
        if (lower_ns >= 1000.0) {
            at_or_over_micro += cost.push_buckets[bucket];
        }
    }
    std::printf("  pushes landing in a bucket whose floor is >= 1 us: %llu of %llu\n",
                static_cast<unsigned long long>(at_or_over_micro),
                static_cast<unsigned long long>(cost.wire_events));
}

// Outliers in the push distribution, next to outliers in the control
// distribution, both normalised by how long each step left the thread exposed
// to being descheduled.
//
// The raw counts are not comparable on their own. A push costs ~80 ns and a
// control interval ~12 ns, so the push is exposed to preemption for several
// times longer per call and will collect several times as many outliers with
// no wait involved anywhere. Dividing each count by that step's total
// occupancy removes the difference, and what is left is the question worth
// asking: does the push suffer outliers at a materially higher rate than a
// step that provably cannot block?
struct OutlierComparison {
    double threshold_ns = 0.0;
    std::uint64_t push_outliers = 0;
    std::uint64_t control_outliers = 0;
    double push_exposure_ms = 0.0;
    double control_exposure_ms = 0.0;
    double push_rate_per_ms = 0.0;    // outliers per millisecond spent in the step
    double control_rate_per_ms = 0.0;
};

[[nodiscard]] OutlierComparison compare_outliers(const exchange::market_data::PublishCostStats& cost,
                                                   double ticks_per_second) {
    const double ns_per_tick = ticks_per_second > 0.0 ? 1e9 / ticks_per_second : 0.0;
    OutlierComparison out;
    for (std::size_t bucket = 0; bucket < exchange::market_data::kPushBucketCount; ++bucket) {
        const double lower_ns =
            static_cast<double>(exchange::market_data::push_bucket_lower_edge_ticks(bucket)) * ns_per_tick;
        if (lower_ns < 1000.0) {
            continue;
        }
        if (out.threshold_ns == 0.0) {
            out.threshold_ns = lower_ns;
        }
        out.push_outliers += cost.push_buckets[bucket];
        out.control_outliers += cost.control_buckets[bucket];
    }
    out.push_exposure_ms = static_cast<double>(cost.push_ticks_total) * ns_per_tick / 1e6;
    out.control_exposure_ms = static_cast<double>(cost.control_ticks_total) * ns_per_tick / 1e6;
    out.push_rate_per_ms =
        out.push_exposure_ms > 0.0 ? static_cast<double>(out.push_outliers) / out.push_exposure_ms : 0.0;
    out.control_rate_per_ms =
        out.control_exposure_ms > 0.0 ? static_cast<double>(out.control_outliers) / out.control_exposure_ms : 0.0;
    return out;
}

void print_outlier_table(const OutlierComparison& outliers) {
    std::printf("\n  outliers >= %.0f ns, next to the step that cannot block:\n", outliers.threshold_ns);
    std::printf("  %-22s | %10s | %12s | %14s\n", "step", "outliers", "exposure ms", "per exposure ms");
    std::printf("  %-22s | %10llu | %12.2f | %14.2f\n", "DroppingQueue::push",
                static_cast<unsigned long long>(outliers.push_outliers), outliers.push_exposure_ms,
                outliers.push_rate_per_ms);
    std::printf("  %-22s | %10llu | %12.2f | %14.2f\n", "(control: empty pair)",
                static_cast<unsigned long long>(outliers.control_outliers), outliers.control_exposure_ms,
                outliers.control_rate_per_ms);
}

int run_md_cpu(const Args& args, double ticks_per_second) {
    const double rate = args.md_cpu_rate;
    const auto samples =
        static_cast<std::size_t>(std::max(1.0, rate * args.md_cpu_seconds));

    print_run_header("md-cpu", args.repeats, "median across runs, per arm");
    std::printf("\n== Where the MarketDataRouter's latency cost goes, at %.0f/s ==\n", rate);
    std::printf("mixed GTC over TCP, production queues ingest=%zu outbound=%zu, router cap=%zu, real UDP "
                "subscriber\n",
                kProductionIngest, kProductionOutbound, kProductionMdQueue);
    std::printf("one rate held for %.1f s (%zu samples) so per-thread CPU has a window to sample; "
                "sampling every %zu ms\n",
                args.md_cpu_seconds, samples, args.md_cpu_sample_ms);
    std::printf("arms: A no router | B router attached | C router with publish() instrumented\n");
    std::printf("Quote B for the regression. C's own tick reads land on the matching thread, so its latency is\n"
                "not the reproduction -- its counters are.\n");

    long physical_cores = sysconf(_SC_NPROCESSORS_ONLN);
#if defined(__APPLE__)
    {
        int value = 0;
        std::size_t len = sizeof(value);
        if (sysctlbyname("hw.physicalcpu", &value, &len, nullptr, 0) == 0 && value > 0) {
            physical_cores = value;
        }
    }
#endif
    std::printf("\nphysical cores %ld, logical cpus %ld", physical_cores, sysconf(_SC_NPROCESSORS_ONLN));
#if defined(__APPLE__)
    {
        int performance = 0;
        int efficiency = 0;
        std::size_t len = sizeof(performance);
        if (sysctlbyname("hw.perflevel0.physicalcpu", &performance, &len, nullptr, 0) == 0) {
            len = sizeof(efficiency);
            if (sysctlbyname("hw.perflevel1.physicalcpu", &efficiency, &len, nullptr, 0) == 0) {
                std::printf(" (%d performance + %d efficiency)", performance, efficiency);
            }
        }
    }
#endif
    std::printf("\n");

    struct ArmSpec {
        const char* label;
        bool router;
        bool udp;
        bool instrumented;
    };
    static constexpr std::size_t kArmCount = 5;
    const std::array<ArmSpec, kArmCount> specs{
        ArmSpec{"A no-router", false, false, false},
        ArmSpec{"B router+udp", true, true, false},
        ArmSpec{"C B+instrumented", true, true, true},
        ArmSpec{"D router,null", true, false, false},
        ArmSpec{"E D+instrumented", true, false, true},
    };

    const auto arm_enabled = [&](std::size_t index) {
        return args.md_cpu_arms.find(static_cast<char>('A' + index)) != std::string::npos;
    };

    std::array<std::vector<MdCpuArm>, kArmCount> results;
    std::printf("\n| run | arm              | offered/s | achieved/s | p50 us | p99 us | p99.9 us | harvested | "
                "ingest_hwm | out_drops | md_dropped | md_hwm |\n");
    for (std::size_t r = 0; r < args.repeats; ++r) {
        for (std::size_t a = 0; a < specs.size(); ++a) {
            if (!arm_enabled(a)) {
                continue;
            }
            auto arm = run_md_cpu_arm(args, ticks_per_second, specs[a].label, specs[a].router, specs[a].udp,
                                        specs[a].instrumented, rate, samples);
            if (!arm) {
                std::fprintf(stderr, "md-cpu: setup failed on run %zu arm %s\n", r + 1, specs[a].label);
                return EXIT_FAILURE;
            }
            std::printf("| %3zu | %-16s | %9.0f | %10.0f | %6.1f | %6.1f | %8.1f | %9zu | %10zu | %9llu | %10zu | "
                        "%6zu |\n",
                        r + 1, arm->label, arm->offered, arm->achieved, arm->p50_us, arm->p99_us, arm->p999_us,
                        arm->harvested, arm->matching_queue_hwm,
                        static_cast<unsigned long long>(arm->outbound_drops), arm->md_dropped, arm->md_hwm);
            std::fflush(stdout);
            results[a].push_back(std::move(*arm));
        }
    }

    std::printf("\n-- median of %zu runs --\n", args.repeats);
    std::printf("| arm              | offered/s | achieved/s | p50 us | p99 us | p99.9 us | md_dropped |\n");
    std::array<double, kArmCount> p50_medians{};
    for (std::size_t a = 0; a < specs.size(); ++a) {
        if (results[a].empty()) {
            continue;
        }
        std::vector<double> offered;
        std::vector<double> achieved;
        std::vector<double> p50;
        std::vector<double> p99;
        std::vector<double> p999;
        std::vector<double> dropped;
        for (const auto& arm : results[a]) {
            offered.push_back(arm.offered);
            achieved.push_back(arm.achieved);
            p50.push_back(arm.p50_us);
            p99.push_back(arm.p99_us);
            p999.push_back(arm.p999_us);
            dropped.push_back(static_cast<double>(arm.md_dropped));
        }
        p50_medians[a] = median_of(p50);
        std::printf("| %-16s | %9.0f | %10.0f | %6.1f | %6.1f | %8.1f | %10.0f |\n", specs[a].label,
                    median_of(offered), median_of(achieved), p50_medians[a], median_of(p99), median_of(p999),
                    median_of(dropped));
    }
    if (p50_medians[0] > 0.0 && p50_medians[1] > 0.0 && p50_medians[3] > 0.0) {
        std::printf("\nB/A = %.1fx  (router with the UDP hop, against no router)\n", p50_medians[1] / p50_medians[0]);
        std::printf("D/A = %.1fx  (router machinery only: translate, push, notify, routing thread)\n",
                    p50_medians[3] / p50_medians[0]);
        std::printf("B/D = %.1fx  (what the loopback UDP hop itself costs)\n", p50_medians[1] / p50_medians[3]);
    }

    // The CPU table and the publish counters are per-run facts, not
    // medianable ones -- a median of a distribution is not a distribution.
    // The last run is printed in full, which is the run whose numbers a
    // reader can line up against a concurrent `ps -M` capture.
    for (std::size_t a = 0; a < specs.size(); ++a) {
        if (results[a].empty()) {
            continue;
        }
        const auto& arm = results[a].back();
        std::printf("\n-- arm %s, order path by stage (run %zu) --\n", specs[a].label, args.repeats);
        print_stage_table(arm.stages, ticks_per_second);
        std::printf("\n-- arm %s, per-thread CPU over the paced window (run %zu) --\n", specs[a].label, args.repeats);
        print_cpu_table(arm.cpu, physical_cores);
        if (arm.router) {
            std::printf("  routing thread slept on the condition variable %llu times, and routed %llu events:\n"
                        "  a notify_one() per event with the consumer asleep is a kernel wakeup, not a no-op.\n",
                        static_cast<unsigned long long>(arm.cv_waits),
                        static_cast<unsigned long long>(arm.md_routed));
        }
    }

    const double ns_per_tick = ticks_per_second > 0.0 ? 1e9 / ticks_per_second : 0.0;
    for (const std::size_t a : {std::size_t{2}, std::size_t{4}}) {
        if (results[a].empty()) {
            continue;
        }
        std::printf("\n-- arm %s, publish() cost breakdown (run %zu) --\n", specs[a].label, args.repeats);
        print_publish_cost(results[a].back().cost, ticks_per_second);
        print_outlier_table(compare_outliers(results[a].back().cost, ticks_per_second));
    }

    if (!results[2].empty()) {
        const auto& busy = results[2].back();  // C: consumer hammering the loopback stack
        const auto& cost = busy.cost;
        const auto outliers = compare_outliers(cost, ticks_per_second);

        // Claim 1 is structural and does not depend on any timing at all.
        // try_push has exactly one conditional -- the full check -- and a
        // queue that never filled took its false branch on every call. So
        // every push in the run ran the same straight-line instructions, and
        // none of them can have waited for a slot: there is no code path on
        // which they could.
        std::printf("\nVERDICT on DroppingQueue::try_push  ");
        if (cost.push_full != 0) {
            std::printf("INCONCLUSIVE for this arm.\n"
                        "The queue refused %llu pushes, so it did reach capacity. try_push still never waits --\n"
                        "it returns false -- but the drop policy was live, so this is not a clean comparison.\n",
                        static_cast<unsigned long long>(cost.push_full));
            return EXIT_SUCCESS;
        }

        std::printf("NOT blocking and NOT spinning under this load.\n");
        std::printf("  1. Structural, independent of any timing: %llu pushes, %llu refusals, queue high-water\n"
                    "     mark %zu of %zu. try_push's only conditional is the full check, and it was false on\n"
                    "     every call -- so all %llu pushes ran identical straight-line code, and there is no\n"
                    "     path on which any of them could have waited for a slot or retried.\n",
                    static_cast<unsigned long long>(cost.push_ok), static_cast<unsigned long long>(cost.push_full),
                    busy.md_hwm, kProductionMdQueue, static_cast<unsigned long long>(cost.wire_events));
        std::printf("  2. Typical cost is %.1f ns, against %.1f ns to translate the event and %.1f ns to wake\n"
                    "     the routing thread. The push is the cheapest of the three steps.\n",
                    cost.wire_events == 0 ? 0.0
                                          : static_cast<double>(cost.push_ticks_total) * ns_per_tick /
                                                static_cast<double>(cost.wire_events),
                    cost.events_seen == 0 ? 0.0
                                          : static_cast<double>(cost.translate_ticks_total) * ns_per_tick /
                                                static_cast<double>(cost.events_seen),
                    cost.notify_calls == 0 ? 0.0
                                           : static_cast<double>(cost.notify_ticks_total) * ns_per_tick /
                                                 static_cast<double>(cost.notify_calls));

        // The tail is real and deserves its own sentence rather than being
        // rounded away by the verdict. What it is *not* is a wait, per claim
        // 1; arm E is what says what it is instead.
        std::printf("  3. It does have a tail: %llu of %llu pushes (%.2f%%) took >= %.0f ns, worst %.1f us.\n",
                    static_cast<unsigned long long>(outliers.push_outliers),
                    static_cast<unsigned long long>(cost.wire_events),
                    cost.wire_events == 0 ? 0.0
                                          : 100.0 * static_cast<double>(outliers.push_outliers) /
                                                static_cast<double>(cost.wire_events),
                    outliers.threshold_ns, static_cast<double>(cost.push_ticks_max) * ns_per_tick / 1000.0);

        if (!results[4].empty()) {
            const auto& idle = results[4].back(); // E: same code, consumer nearly idle
            const auto idle_outliers = compare_outliers(idle.cost, ticks_per_second);
            const double idle_share =
                idle.cost.wire_events == 0 ? 0.0
                                           : 100.0 * static_cast<double>(idle_outliers.push_outliers) /
                                                 static_cast<double>(idle.cost.wire_events);
            const double busy_share =
                cost.wire_events == 0 ? 0.0
                                      : 100.0 * static_cast<double>(outliers.push_outliers) /
                                            static_cast<double>(cost.wire_events);
            std::printf("     Arm E pushes into the same queue with the same code, differing only in what the\n"
                        "     consumer does on the other core: %.2f%% of its pushes reach that threshold,\n"
                        "     against %.2f%% here, and its worst is %.1f us against %.1f us.\n",
                        idle_share, busy_share,
                        static_cast<double>(idle.cost.push_ticks_max) * ns_per_tick / 1000.0,
                        static_cast<double>(cost.push_ticks_max) * ns_per_tick / 1000.0);
            if (busy_share > idle_share * 1.5) {
                std::printf("     The tail therefore tracks the consumer's activity, not the queue's state. Every\n"
                            "     push must read the consumer-owned `tail_` cache line, so a consumer busy in the\n"
                            "     loopback stack on another core turns that read into a coherence miss -- a\n"
                            "     memory stall, which is not the same thing as the queue waiting, and which\n"
                            "     claim 1 has already ruled out as a wait.\n");
            } else {
                std::printf("     The tail is comparable with an idle consumer, so it is not consumer-induced\n"
                            "     contention: on this evidence it is preemption of the matching thread, whose\n"
                            "     own control interval reached %.1f us.\n",
                            static_cast<double>(cost.control_ticks_max) * ns_per_tick / 1000.0);
            }
        }
    }
    return EXIT_SUCCESS;
}

// ── Price drift / ladder overflow ─────────────────────────────────────────
//
// Every price index in this book is anchored once, at the first price its
// side ever sees, and covers a band of kMaxBandTicks around it
// (MatchingBook::SideIndex). A side only re-anchors when it empties, and a
// side carrying resting orders never does. So a market that trends in one
// direction for long enough walks out of its own ladder and into the
// std::pmr::map fallback, one level at a time, with no mechanism to follow
// it.
//
// generate_workload cannot show this: its prices are drawn around a fixed
// reference mid, so its book is roughly stationary and stays in band for as
// long as you care to run it. This generator drifts the mid instead, far
// enough past the band that the run ends with most of the touch outside it,
// and the replay is timed in windows rather than as one aggregate -- an
// average over a run that starts in-band and ends out of it is precisely the
// number that would hide the effect.

// Share of operations that cancel a live order, and the buy share of the
// rest. See the pricing comment in generate_drift_workload for why the flow
// is skewed rather than even.
constexpr unsigned kDriftCancelPct = 15;
constexpr unsigned kDriftBuyPct = 70;

struct DriftWorkload {
    std::vector<ExchangeCommand> seed;
    std::vector<ExchangeCommand> operations;
    std::size_t resting_after_seed = 0;
    std::size_t restings = 0;
    std::size_t crossings = 0;
    std::size_t cancels = 0;
    Price first_mid = 0;
    Price last_mid = 0;
    // The reference mid at the end of each window, for the table below.
    std::vector<Price> window_mid;
};

[[nodiscard]] DriftWorkload generate_drift_workload(const Args& args) {
    DriftWorkload out;
    const WorkloadConfig config = mixed_config(0, args.matching_accounts);
    constexpr InstrumentId kInstrument = 1;

    // The starting book is the ordinary one: same seed phase as every other
    // mixed-workload scenario, so what follows is the only difference.
    const auto seeded = generate_workload(config);
    out.seed = seeded.seed;
    out.resting_after_seed = seeded.resting_orders_after_seed;

    // Own engine, for the reason generate_workload has one: a cancel has to
    // target an order that genuinely rests, and classifying an order as
    // passive or aggressive needs the current touch.
    MatchingEngine engine(config.instruments(), 500'000);
    SplitMix64 rng(config.seed ^ 0x5D1F7ULL);
    SequentialIds ids;
    ids.next_client_order_id = 1'000'000;
    LiveOrderIndex live;
    DepthTracker depth(config.instrument_count);

    std::vector<ExchangeEvent> events;
    const EventSink sink = [&events](const ExchangeEvent& event) { events.push_back(event); };

    AccountId owner_account = 0;
    ClientOrderId owner_client_order_id = 0;
    const auto apply = [&](const ExchangeCommand& command) {
        events.clear();
        engine.process(command, sink);
        for (const auto& event : events) {
            std::visit(
                [&](const auto& ev) {
                    using T = std::decay_t<decltype(ev)>;
                    if constexpr (std::is_same_v<T, BookOrderAdded>) {
                        depth.on_added(ev.instrument_id, ev.side, ev.price);
                        live.insert(LiveOrderRecord{
                            .exchange_order_id = ev.exchange_order_id,
                            .client_order_id = owner_client_order_id,
                            .account_id = owner_account,
                            .instrument_id = ev.instrument_id,
                            .side = ev.side,
                            .price = ev.price,
                            .remaining_quantity = ev.quantity,
                        });
                    } else if constexpr (std::is_same_v<T, BookOrderReduced>) {
                        live.set_remaining_quantity(ev.exchange_order_id, ev.new_remaining_quantity);
                    } else if constexpr (std::is_same_v<T, BookOrderRemoved>) {
                        depth.on_removed(ev.instrument_id, ev.side, ev.price);
                        live.erase(ev.exchange_order_id);
                    }
                },
                event);
        }
    };

    for (const auto& command : out.seed) {
        std::visit(
            [&](const auto& cmd) {
                using T = std::decay_t<decltype(cmd)>;
                owner_account = cmd.account_id;
                if constexpr (std::is_same_v<T, NewOrderCommand>) {
                    owner_client_order_id = cmd.client_order_id;
                }
            },
            command);
        apply(command);
    }

    // A mid that climbs `drift_ticks` over the run, with small symmetric
    // noise so consecutive orders are not all at one price.
    //
    // Each order is priced passive relative to the mid *at its own moment*:
    // bids strictly below it, asks strictly above, so nothing crosses within
    // a single operation. Crossing comes from the drift alone -- an ask
    // placed above the mid ten thousand operations ago is below it now, and
    // the bid that reaches up to the current mid takes it. That is how a
    // trending market consumes the liquidity it left behind, and it is why
    // this generator does *not* clamp prices away from the opposing touch
    // the way generate_workload does: that clamp would peg the bid under the
    // stalest resting ask and the mid could never move at all.
    //
    // Order flow is deliberately buy-heavy (kDriftBuyPct of the new-order
    // bucket), because that is the thing that *causes* a sustained upward
    // drift rather than merely accompanying it -- and because a balanced
    // stream does not accumulate a book at all. With one bid per ask, each
    // new bid reaches down and takes exactly one stale ask as the mid passes
    // it, the two sides consume each other one for one, and the book stays a
    // few hundred orders deep no matter how far the price travels. Skewing
    // the flow leaves the surplus bids resting, so the book grows across the
    // whole drift range and the share of it living outside the band climbs
    // steadily instead of arriving all at once.
    const double per_op = static_cast<double>(args.drift_ticks) /
                           static_cast<double>(std::max<std::size_t>(args.drift_operations, 1));
    const auto mid_at = [&](std::size_t op) {
        return config.base_price + static_cast<Price>(per_op * static_cast<double>(op));
    };
    out.first_mid = mid_at(0);
    out.last_mid = mid_at(args.drift_operations);

    out.operations.reserve(args.drift_operations);
    for (std::size_t op = 0; op < args.drift_operations; ++op) {
        const Price mid = mid_at(op);
        const auto roll = static_cast<unsigned>(rng.below(100));

        if (roll >= 100 - kDriftCancelPct && !live.empty()) {
            const auto& record = live.at(rng.below(live.size()));
            ExchangeCommand command{cancel_order(ids.take_command_sequence(), record.account_id,
                                                  record.client_order_id, record.instrument_id)};
            owner_account = record.account_id;
            owner_client_order_id = record.client_order_id;
            out.operations.push_back(command);
            apply(command);
            ++out.cancels;
        } else {
            const Side side = rng.below(100) < kDriftBuyPct ? Side::Buy : Side::Sell;
            const auto noise =
                static_cast<Price>(rng.below(static_cast<std::uint64_t>(args.drift_noise_ticks) * 2 + 1)) -
                static_cast<Price>(args.drift_noise_ticks);
            const Price local_mid = mid + noise;
            const auto offset = static_cast<Price>(rng.below(static_cast<std::uint64_t>(config.price_band_ticks)));
            Price price = side == Side::Buy ? local_mid - 1 - offset : local_mid + 1 + offset;
            if (price <= 0) {
                price = 1;
            }
            const auto contra = depth.best(kInstrument, opposite_of(side));
            const bool crosses = contra.has_value() && (side == Side::Buy ? price >= *contra : price <= *contra);
            const Quantity quantity = rng.between(config.min_quantity, config.max_quantity);
            const AccountId account = static_cast<AccountId>(1 + rng.below(config.account_count));
            const ClientOrderId client_order_id = ids.take_client_order_id();
            ExchangeCommand command{new_order(ids.take_command_sequence(), account, client_order_id, kInstrument,
                                               side, price, quantity)};
            owner_account = account;
            owner_client_order_id = client_order_id;
            out.operations.push_back(command);
            apply(command);
            if (crosses) {
                ++out.crossings;
            } else {
                ++out.restings;
            }
        }

        if ((op + 1) % args.drift_window == 0) {
            out.window_mid.push_back(mid);
        }
    }
    return out;
}

struct DriftWindowSample {
    std::size_t index = 0;
    std::size_t operations = 0;
    double ns_per_op = 0.0;
    std::size_t resting = 0;
    std::size_t oob_levels = 0;
    std::size_t oob_orders = 0;
};

struct DriftRunResult {
    std::vector<DriftWindowSample> windows;
    std::size_t risk_rejects = 0;
    std::size_t order_rejects = 0;
    std::uint32_t band_ticks = 0;
};

// `full_path` selects what the window is timed through: the whole
// RiskGatedEngine path (what every other capacity number in this file
// measures) or MatchingEngine::process alone (which isolates the book's own
// structure, and so the ladder-versus-map question, from risk and ledger
// work).
[[nodiscard]] DriftRunResult run_drift_once(const Args& args, const DriftWorkload& workload, bool full_path) {
    DriftRunResult out;
    const WorkloadConfig config = mixed_config(0, args.matching_accounts);
    MatchingEngine engine(config.instruments(), 500'000);
    out.band_ticks = engine.ladder_band_ticks();

    ledger::Ledger ledger;
    fund_ledger(ledger, config, kMatchingFundCash, kMatchingFundPosition);
    risk::RiskGatedEngine gated(engine, ledger);
    sequencing::CommandSequencer sequencer;

    std::array<std::size_t, static_cast<std::size_t>(RejectReason::AccountMismatch) + 1> rejects{};
    const EventSink sink = [&](const ExchangeEvent& event) {
        if (const auto* rejected = std::get_if<OrderRejected>(&event); rejected != nullptr) {
            ++rejects[static_cast<std::size_t>(rejected->reason)];
        }
    };

    const auto run_one = [&](const ExchangeCommand& command) {
        if (full_path) {
            gated.process(sequencer.sequence(command), sink);
        } else {
            engine.process(command, sink);
        }
    };

    for (const auto& command : workload.seed) {
        run_one(command);
    }

    const std::size_t total = workload.operations.size();
    for (std::size_t first = 0; first < total; first += args.drift_window) {
        const std::size_t last = std::min(first + args.drift_window, total);
        const auto start = std::chrono::steady_clock::now();
        for (std::size_t i = first; i < last; ++i) {
            run_one(workload.operations[i]);
        }
        const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        const std::size_t count = last - first;
        out.windows.push_back(DriftWindowSample{
            .index = out.windows.size(),
            .operations = count,
            .ns_per_op = count > 0 ? elapsed * 1e9 / static_cast<double>(count) : 0.0,
            .resting = engine.resting_order_count(),
            .oob_levels = engine.out_of_band_levels(),
            .oob_orders = engine.out_of_band_orders(),
        });
    }

    out.risk_rejects = rejects[static_cast<std::size_t>(RejectReason::InsufficientFunds)] +
                        rejects[static_cast<std::size_t>(RejectReason::InsufficientPosition)] +
                        rejects[static_cast<std::size_t>(RejectReason::OrderTooLarge)];
    for (std::size_t reason = 1; reason < rejects.size(); ++reason) {
        out.order_rejects += rejects[reason];
    }
    return out;
}

int run_price_drift(const Args& args) {
    print_run_header("price-drift", args.repeats, "median ns/op per window across runs");
    std::printf("\n== price drift / ladder overflow (sustained one-directional price movement) ==\n");
    std::printf("There is no re-anchoring mechanism: SideIndex anchors on the first price its side sees and only\n"
                "re-anchors if that side empties, which a side holding resting orders never does. This measures\n"
                "what that costs when the market walks out of the band.\n");

    const auto workload = generate_drift_workload(args);
    std::printf("\nseed: %zu commands, %zu resting after seed (1000 orders/side, 64-tick band, base %lld)\n",
                workload.seed.size(), workload.resting_after_seed, static_cast<long long>(100'000));
    std::printf("stream: %zu operations  resting=%zu crossing=%zu cancel=%zu\n", workload.operations.size(),
                workload.restings, workload.crossings, workload.cancels);
    std::printf("drift: mid %lld -> %lld (%zu ticks up, noise +/-%zu), window=%zu operations\n",
                static_cast<long long>(workload.first_mid), static_cast<long long>(workload.last_mid),
                args.drift_ticks, args.drift_noise_ticks, args.drift_window);

    for (int path = 0; path < 2; ++path) {
        const bool full_path = path == 0;
        std::vector<DriftRunResult> runs;
        runs.reserve(args.repeats);
        for (std::size_t r = 0; r < args.repeats; ++r) {
            runs.push_back(run_drift_once(args, workload, full_path));
        }
        const auto& structure = runs.back();

        std::printf("\n-- %s: ns/op per window, median of %zu runs (ladder band = %u ticks) --\n",
                    full_path ? "full RiskGatedEngine path" : "MatchingEngine::process only", args.repeats,
                    structure.band_ticks);
        std::printf("| window |  ops range  | mid at end | ns/op median |");
        for (std::size_t r = 0; r < args.repeats; ++r) {
            std::printf(" run%zu |", r + 1);
        }
        std::printf(" resting | oob_levels | oob_orders | oob_share |\n");

        double first_median = 0.0;
        double last_median = 0.0;
        for (std::size_t w = 0; w < structure.windows.size(); ++w) {
            std::vector<double> per_run;
            per_run.reserve(runs.size());
            for (const auto& run : runs) {
                if (w < run.windows.size()) {
                    per_run.push_back(run.windows[w].ns_per_op);
                }
            }
            const double median = median_of(per_run);
            if (w == 0) {
                first_median = median;
            }
            last_median = median;
            const auto& s = structure.windows[w];
            const double share =
                s.resting > 0 ? 100.0 * static_cast<double>(s.oob_orders) / static_cast<double>(s.resting) : 0.0;
            std::printf("| %6zu | %5zu-%5zu | %10lld | %12.1f |", s.index, w * args.drift_window,
                        w * args.drift_window + s.operations - 1,
                        static_cast<long long>(w < workload.window_mid.size() ? workload.window_mid[w] : 0),
                        median);
            for (const auto& run : runs) {
                std::printf(" %5.0f |", w < run.windows.size() ? run.windows[w].ns_per_op : 0.0);
            }
            std::printf(" %7zu | %10zu | %10zu | %8.1f%% |\n", s.resting, s.oob_levels, s.oob_orders, share);
        }

        const auto& final_window = structure.windows.empty() ? DriftWindowSample{} : structure.windows.back();
        std::printf("%s  first window %.1f ns/op -> last window %.1f ns/op  = %.2fx\n",
                    full_path ? "DRIFT_FULL_PATH" : "DRIFT_MATCHING_ONLY", first_median, last_median,
                    first_median > 0.0 ? last_median / first_median : 0.0);
        std::printf("end of run: overflow map holds %zu levels and %zu resting orders out of %zu resting "
                    "(%.1f%%)\n",
                    final_window.oob_levels, final_window.oob_orders, final_window.resting,
                    final_window.resting > 0
                        ? 100.0 * static_cast<double>(final_window.oob_orders) / static_cast<double>(final_window.resting)
                        : 0.0);
        std::printf("risk_rejected_events=%zu  order_rejected_events=%zu\n", structure.risk_rejects,
                    structure.order_rejects);
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
    // So the load generator shows up as itself in md-cpu's per-thread CPU
    // table rather than as an anonymous row.
    set_calling_thread_name("mdh-bench-main");
    for (int i = 0; i < argc; ++i) {
        g_command_line += (i == 0 ? "" : " ");
        g_command_line += argv[i];
    }
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
        if (name == "fok-latency") {
            return run_fok_latency(args, cal);
        }
        if (name == "md-realistic") {
            return run_md_realistic(args, cal.measured_ticks_per_second);
        }
        if (name == "md-cpu") {
            return run_md_cpu(args, cal.measured_ticks_per_second);
        }
        if (name == "price-drift") {
            return run_price_drift(args);
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
