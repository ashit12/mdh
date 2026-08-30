// End-to-end order-path latency: a real OrderEntryClient talks to a real
// OrderEntryGateway over loopback TCP -- the same path trading_server uses
// for order entry. MatchingEngine is also timed in-process as a separate
// ceiling so TCP numbers are not confused with matcher throughput.
//
// Workloads: sequential, sustained, multi-client flood, idle-session
// population, burst. Matching-core-only always prints first unless disabled.
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
#include <memory>
#include <span>
#include <string>
#include <sys/resource.h>
#include <thread>
#include <vector>

#include "exchange/gateway/order_entry_gateway.hpp"
#include "exchange/latency/latency_tracer.hpp"
#include "exchange/matching/matching_engine.hpp"
#include "exchange/market_data/market_data_router.hpp"
#include "exchange/testing/hr_timer.hpp"
#include "exchange/testing/matching_scenarios.hpp"
#include "net/packet.hpp"
#include "net/udp_socket.hpp"
#include "trader/oms/order_entry_client.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::gateway;
using namespace mdh::exchange::testing;
using namespace mdh::protocol::order_entry;
using namespace mdh::trader::oms;
using namespace std::chrono_literals;

namespace {

constexpr InstrumentId kInstrument = 1;

struct Args {
    std::size_t warmup = 200;
    std::size_t sequential_samples = 8'000;
    std::size_t sustained_samples = 8'000;
    std::size_t multi_samples_per_client = 2'000;
    std::size_t burst_rounds = 40;
    std::size_t burst_size = 100;
    int burst_idle_ms = 10;
    int max_clients = 16;
    int idle_clients = 256;
    std::size_t writer_batch = 4;
    std::size_t soak_orders = 1'000'000;
    double soak_seconds = 60.0;
    std::size_t core_samples = 200'000;
    bool market_data = false;
    bool run_sequential = true;
    bool run_sustained = true;
    bool run_multi = true;
    bool run_burst = true;
    bool run_soak = false;
    bool run_idle = false;
    bool run_core = true;
};

[[nodiscard]] Args parse_args(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        const std::string flag = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) return nullptr;
            return argv[++i];
        };
        if (flag == "--warmup") {
            if (const char* v = next()) args.warmup = static_cast<std::size_t>(std::atoll(v));
        } else if (flag == "--sequential-samples") {
            if (const char* v = next()) args.sequential_samples = static_cast<std::size_t>(std::atoll(v));
        } else if (flag == "--sustained-samples") {
            if (const char* v = next()) args.sustained_samples = static_cast<std::size_t>(std::atoll(v));
        } else if (flag == "--max-clients") {
            if (const char* v = next()) args.max_clients = std::max(1, std::atoi(v));
        } else if (flag == "--idle-clients") {
            if (const char* v = next()) args.idle_clients = std::max(1, std::atoi(v));
        } else if (flag == "--multi-samples") {
            if (const char* v = next()) {
                const auto n = static_cast<std::size_t>(std::atoll(v));
                args.multi_samples_per_client = n == 0 ? 1 : n;
            }
        } else if (flag == "--idle-samples") {
            if (const char* v = next()) {
                const auto n = static_cast<std::size_t>(std::atoll(v));
                args.sequential_samples = n == 0 ? 1 : n;
            }
        } else if (flag == "--core-samples") {
            if (const char* v = next()) {
                const auto n = static_cast<std::size_t>(std::atoll(v));
                args.core_samples = n == 0 ? 1 : n;
            }
        } else if (flag == "--writer-batch") {
            if (const char* v = next()) {
                const auto n = static_cast<std::size_t>(std::atoll(v));
                args.writer_batch = n == 0 ? 1 : n;
            }
        } else if (flag == "--soak-orders") {
            if (const char* v = next()) {
                const auto n = static_cast<std::size_t>(std::atoll(v));
                args.soak_orders = n == 0 ? 1 : n;
            }
        } else if (flag == "--soak-seconds") {
            if (const char* v = next()) {
                const double s = std::atof(v);
                args.soak_seconds = s <= 0.0 ? 60.0 : s;
            }
        } else if (flag == "--market-data") {
            args.market_data = true;
        } else if (flag == "--workload") {
            const char* v = next();
            if (v == nullptr) continue;
            const std::string w = v;
            args.run_sequential = args.run_sustained = args.run_multi = args.run_burst = false;
            args.run_soak = false;
            args.run_idle = false;
            args.run_core = true;
            if (w == "sequential") args.run_sequential = true;
            else if (w == "sustained") args.run_sustained = true;
            else if (w == "multi") args.run_multi = true;
            else if (w == "burst") args.run_burst = true;
            else if (w == "soak") args.run_soak = true;
            else if (w == "idle") args.run_idle = true;
            else if (w == "core") {
                args.run_core = true;
            } else if (w == "all") {
                args.run_sequential = args.run_sustained = args.run_multi = args.run_burst = true;
            }
        }
    }
    return args;
}

struct IntervalSet {
    std::vector<std::uint64_t> end_to_end;
    std::vector<std::uint64_t> client_to_server;
    std::vector<std::uint64_t> server_pre_match;
    std::vector<std::uint64_t> exchange_processing;
    std::vector<std::uint64_t> writer_handoff;
    std::vector<std::uint64_t> server_to_client;
};

void harvest(IntervalSet& set, AccountId account_id, ClientOrderId client_order_id) {
    const auto snap = latency::tracer().snapshot(account_id, client_order_id);
    if (!snap || snap->t0_client_submit == 0 || snap->t5_client_first == 0 ||
        snap->t5_client_first < snap->t0_client_submit) {
        return;
    }
    const auto add = [](std::vector<std::uint64_t>& out, std::uint64_t start, std::uint64_t end) {
        if (start == 0 || end == 0 || end < start) {
            return;
        }
        out.push_back(end - start);
    };
    add(set.end_to_end, snap->t0_client_submit, snap->t5_client_first);
    add(set.client_to_server, snap->t0_client_submit, snap->t1_server_decoded);
    add(set.server_pre_match, snap->t1_server_decoded, snap->t2_exchange_begin);
    add(set.exchange_processing, snap->t2_exchange_begin, snap->t3_exchange_end);
    add(set.writer_handoff, snap->t4_writer_queued, snap->t4_socket_written);
    add(set.server_to_client, snap->t4_socket_written, snap->t5_client_first);
}

void print_summary(const char* name, std::vector<std::uint64_t> samples, double ticks_per_second) {
    const auto summary = summarise_latency(samples, ticks_per_second);
    if (summary.count == 0) {
        std::printf("  %-22s  (no samples)\n", name);
        return;
    }
    const auto us = [](double ns) { return ns / 1000.0; };
    std::printf("  %-22s  n=%zu  mean=%7.2f  p50=%7.2f  p90=%7.2f  p99=%7.2f  p99.9=%7.2f  max=%7.2f us\n", name,
                summary.count, us(summary.sampled_mean_ns), us(summary.p50_ns), us(summary.p90_ns), us(summary.p99_ns),
                us(summary.p999_ns), us(summary.max_ns));
}

struct OccupancySummary {
    std::size_t samples = 0;
    std::size_t p50 = 0;
    std::size_t p99 = 0;
    std::size_t max = 0;
};

struct RusageDelta {
    long nvcsw = 0;
    long nivcsw = 0;
};

[[nodiscard]] OrderEntryIoMetrics subtract_metrics(const OrderEntryIoMetrics& end,
                                                   const OrderEntryIoMetrics& start) {
    return OrderEntryIoMetrics{
        .read_syscalls = end.read_syscalls - start.read_syscalls,
        .bytes_read = end.bytes_read - start.bytes_read,
        .frames_decoded = end.frames_decoded - start.frames_decoded,
        .write_syscalls = end.write_syscalls - start.write_syscalls,
        .bytes_written = end.bytes_written - start.bytes_written,
        .reports_enqueued = end.reports_enqueued - start.reports_enqueued,
        .reports_written = end.reports_written - start.reports_written,
        .write_errors = end.write_errors - start.write_errors,
        .outbound_drops = end.outbound_drops - start.outbound_drops,
    };
}

[[nodiscard]] OccupancySummary summarise_occupancy(std::vector<std::size_t> samples) {
    OccupancySummary out;
    if (samples.empty()) {
        return out;
    }
    std::sort(samples.begin(), samples.end());
    out.samples = samples.size();
    out.p50 = samples[samples.size() / 2];
    out.p99 = samples[(samples.size() * 99) / 100];
    out.max = samples.back();
    return out;
}

class RusageScope {
public:
    RusageScope() { getrusage(RUSAGE_SELF, &start_); }

    RusageDelta stop() {
        rusage end{};
        getrusage(RUSAGE_SELF, &end);
        RusageDelta delta;
        delta.nvcsw = end.ru_nvcsw - start_.ru_nvcsw;
        delta.nivcsw = end.ru_nivcsw - start_.ru_nivcsw;
        return delta;
    }

private:
    rusage start_{};
};

struct WorkloadStats {
    std::string name;
    int clients = 1;
    double offered_per_sec = 0.0;
    double achieved_per_sec = 0.0;
    std::size_t sent = 0;
    std::size_t received = 0;
    std::size_t harvested = 0;
    double elapsed_s = 0.0;
    IntervalSet intervals;
    OccupancySummary occupancy;
    RusageDelta rusage;
    OrderEntryIoMetrics io;
};

void print_workload(const WorkloadStats& stats, double ticks_per_second) {
    std::printf("\n== %s  clients=%d  offered=%.0f/s  achieved=%.0f/s  sent=%zu  received=%zu  traces=%zu  wall=%.2fs ==\n",
                stats.name.c_str(), stats.clients, stats.offered_per_sec, stats.achieved_per_sec, stats.sent,
                stats.received, stats.harvested, stats.elapsed_s);
    print_summary("end_to_end", stats.intervals.end_to_end, ticks_per_second);
    print_summary("client_to_server", stats.intervals.client_to_server, ticks_per_second);
    print_summary("server_pre_match", stats.intervals.server_pre_match, ticks_per_second);
    print_summary("exchange_processing", stats.intervals.exchange_processing, ticks_per_second);
    print_summary("writer_handoff", stats.intervals.writer_handoff, ticks_per_second);
    print_summary("server_to_client", stats.intervals.server_to_client, ticks_per_second);
    std::printf("  matching_queue         samples=%zu  p50=%zu  p99=%zu  max=%zu\n", stats.occupancy.samples,
                stats.occupancy.p50, stats.occupancy.p99, stats.occupancy.max);
    const double per_order =
        stats.sent > 0 ? static_cast<double>(stats.rusage.nvcsw + stats.rusage.nivcsw) / static_cast<double>(stats.sent)
                       : 0.0;
    std::printf("  context_switches       nvcsw=%ld  nivcsw=%ld  switches_per_order=%.2f  (process-wide, not syscalls)\n",
                stats.rusage.nvcsw, stats.rusage.nivcsw, per_order);
    const double reports_per_write =
        stats.io.write_syscalls == 0
            ? 0.0
            : static_cast<double>(stats.io.reports_written) / static_cast<double>(stats.io.write_syscalls);
    const double frames_per_read =
        stats.io.read_syscalls == 0
            ? 0.0
            : static_cast<double>(stats.io.frames_decoded) / static_cast<double>(stats.io.read_syscalls);
    std::printf("  server_io              reads=%llu  bytes_read=%llu  frames=%llu  frames/read=%.2f\n",
                static_cast<unsigned long long>(stats.io.read_syscalls),
                static_cast<unsigned long long>(stats.io.bytes_read),
                static_cast<unsigned long long>(stats.io.frames_decoded), frames_per_read);
    std::printf("                         writes=%llu  bytes_written=%llu  enqueued=%llu  reports=%llu"
                "  reports/write=%.2f"
                "  errors=%llu  drops=%llu\n",
                static_cast<unsigned long long>(stats.io.write_syscalls),
                static_cast<unsigned long long>(stats.io.bytes_written),
                static_cast<unsigned long long>(stats.io.reports_enqueued),
                static_cast<unsigned long long>(stats.io.reports_written), reports_per_write,
                static_cast<unsigned long long>(stats.io.write_errors),
                static_cast<unsigned long long>(stats.io.outbound_drops));
}

void print_table_row(const WorkloadStats& stats, double ticks_per_second) {
    auto e2e = stats.intervals.end_to_end;
    const auto summary = summarise_latency(e2e, ticks_per_second);
    const auto us = [](double ns) { return ns / 1000.0; };
    std::printf("| %-18s | %7d | %18.0f | %8.2f | %8.2f | %8.2f | %9.2f | %8.2f |\n", stats.name.c_str(), stats.clients,
                stats.achieved_per_sec, us(summary.p50_ns), us(summary.p90_ns), us(summary.p99_ns), us(summary.p999_ns),
                us(summary.max_ns));
}

class BenchClient {
public:
    explicit BenchClient(AccountId account_id, bool market_data)
        : account_id_(account_id), time_in_force_(market_data ? TimeInForce::GTC : TimeInForce::IOC),
          client_([this](const Message&) { received_.fetch_add(1, std::memory_order_release); }) {}

    [[nodiscard]] bool connect(std::uint16_t port) { return client_.connect("127.0.0.1", port); }

    [[nodiscard]] bool send_ioc(ClientOrderId id) {
        return client_.send(Message{NewOrder{
            .account_id = account_id_,
            .client_order_id = id,
            .instrument_id = kInstrument,
            .side = Side::Buy,
            .price = 1,
            .quantity = 1,
            .order_type = OrderType::Limit,
            .time_in_force = time_in_force_,
        }});
    }

    [[nodiscard]] AccountId account_id() const { return account_id_; }
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

private:
    AccountId account_id_;
    TimeInForce time_in_force_;
    std::atomic<std::size_t> received_{0};
    OrderEntryClient client_;
};

class MarketDataBenchSink {
public:
    void send(const protocol::Event& wire_event) {
        const std::array<protocol::Event, 1> frames{wire_event};
        auto datagram = net::pack_frames(next_packet_sequence_++, std::span<const protocol::Event>(frames));
        if (socket_.send_to(datagram, "127.0.0.1", 39'999)) {
            published_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] std::uint64_t published() const { return published_.load(std::memory_order_relaxed); }

private:
    net::UdpSocket socket_;
    std::uint64_t next_packet_sequence_ = 1;
    std::atomic<std::uint64_t> published_{0};
};

[[nodiscard]] std::unique_ptr<OrderEntryGateway> make_gateway(
    const Args& args, exchange::market_data::MarketDataRouter& market_data_router) {
    OrderEntryGatewayOptions options{
        .instruments = {kInstrument},
        .matching_queue_capacity = 8192,
        .outbound_queue_capacity = 8192,
        .accept_backlog = std::max({16, args.max_clients, args.idle_clients}),
        .writer_batch = args.writer_batch,
        .enable_io_metrics = true,
    };
    if (args.market_data) {
        options.extra_event_sink = market_data_router.sink();
    }
    return std::make_unique<OrderEntryGateway>(0, options);
}

[[nodiscard]] std::vector<int> client_counts_up_to(int max_clients) {
    std::vector<int> counts;
    for (int n = 1; n < max_clients; n *= 2) {
        counts.push_back(n);
    }
    if (max_clients >= 1) {
        counts.push_back(max_clients);
    }
    return counts;
}

struct MatchingCoreStats {
    std::size_t operations = 0;
    double elapsed_s = 0.0;
    double ops_per_sec = 0.0;
    double ns_per_op = 0.0;
};

[[nodiscard]] MatchingCoreStats run_matching_core(std::size_t samples) {
    MatchingEngine engine(std::span<const InstrumentId>(&kInstrument, 1));
    const EventSink& sink = discard_events();
    auto issue = [&](ClientOrderId id) {
        engine.process(ExchangeCommand{NewOrderCommand{
                           .command_sequence = static_cast<CommandSequence>(id),
                           .account_id = 1000,
                           .client_order_id = id,
                           .instrument_id = kInstrument,
                           .side = Side::Buy,
                           .price = 1,
                           .quantity = 1,
                           .order_type = OrderType::Limit,
                           .time_in_force = TimeInForce::IOC,
                       }},
                       sink);
    };
    constexpr std::size_t warmup = 10'000;
    for (std::size_t i = 0; i < warmup; ++i) {
        issue(static_cast<ClientOrderId>(i + 1));
    }
    const auto start = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < samples; ++i) {
        issue(static_cast<ClientOrderId>(warmup + i + 1));
    }
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    MatchingCoreStats stats;
    stats.operations = samples;
    stats.elapsed_s = elapsed;
    stats.ops_per_sec = elapsed > 0.0 ? static_cast<double>(samples) / elapsed : 0.0;
    stats.ns_per_op = elapsed > 0.0 ? (elapsed * 1e9) / static_cast<double>(samples) : 0.0;
    return stats;
}

void print_matching_core(const MatchingCoreStats& core) {
    std::printf("\n== Matching-core only  MatchingEngine::process IOC empty-book  ops=%zu  wall=%.4fs ==\n",
                core.operations, core.elapsed_s);
    std::printf("  achieved=%.0f/s  ns/op=%.1f\n", core.ops_per_sec, core.ns_per_op);
    std::printf("  This is the matcher ceiling on this machine for this order shape. TCP numbers below\n");
    std::printf("  are a different ceiling (sockets, 2N+2 threads, risk, ledger, routing).\n");
}

void seed_accounts(OrderEntryGateway& gateway, int clients) {
    for (int i = 0; i < clients; ++i) {
        gateway.deposit_cash(static_cast<AccountId>(1000 + i), 1'000'000'000'000LL);
    }
}

void warmup(BenchClient& client, std::size_t n, ClientOrderId base) {
    for (std::size_t i = 0; i < n; ++i) {
        (void)client.send_ioc(base + static_cast<ClientOrderId>(i));
    }
    (void)client.wait_received(n, 5000ms);
}

WorkloadStats run_sequential(OrderEntryGateway& gateway, const Args& args, ClientOrderId id_base) {
    WorkloadStats stats;
    stats.name = "Sequential";
    stats.clients = 1;
    BenchClient client(1000, args.market_data);
    if (!client.connect(*gateway.local_port())) {
        std::fprintf(stderr, "sequential: connect failed\n");
        return stats;
    }
    warmup(client, args.warmup, id_base);
    const auto io_start = gateway.io_metrics();
    RusageScope rusage;
    std::vector<std::size_t> occupancy;
    occupancy.reserve(args.sequential_samples);
    const auto start = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < args.sequential_samples; ++i) {
        const ClientOrderId id = id_base + args.warmup + static_cast<ClientOrderId>(i);
        if (!client.send_ioc(id)) {
            break;
        }
        occupancy.push_back(gateway.matching_queue_size());
        ++stats.sent;
        if (!client.wait_received(args.warmup + i + 1, 2000ms)) {
            std::fprintf(stderr, "sequential: timeout at %zu\n", i);
            break;
        }
        harvest(stats.intervals, client.account_id(), id);
    }
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    stats.elapsed_s = elapsed;
    stats.received = client.received();
    stats.rusage = rusage.stop();
    stats.io = subtract_metrics(gateway.io_metrics(), io_start);
    stats.occupancy = summarise_occupancy(std::move(occupancy));
    stats.harvested = stats.intervals.end_to_end.size();
    stats.achieved_per_sec = elapsed > 0.0 ? static_cast<double>(stats.sent) / elapsed : 0.0;
    stats.offered_per_sec = stats.achieved_per_sec;
    return stats;
}

enum class Pace { Spin, Sleep };

WorkloadStats run_paced(OrderEntryGateway& gateway, const Args& args, double offered, std::size_t samples,
                        ClientOrderId id_base, Pace pace, std::string name,
                        std::chrono::milliseconds drain_timeout) {
    WorkloadStats stats;
    stats.clients = 1;
    stats.offered_per_sec = offered;
    stats.name = std::move(name);

    BenchClient client(1000, args.market_data);
    if (!client.connect(*gateway.local_port())) {
        std::fprintf(stderr, "%s: connect failed\n", stats.name.c_str());
        return stats;
    }
    warmup(client, args.warmup, id_base);

    const auto io_start = gateway.io_metrics();
    RusageScope rusage;
    std::vector<std::size_t> occupancy;
    occupancy.reserve(samples);
    const auto interval = std::chrono::duration<double>(offered > 0.0 ? 1.0 / offered : 0.0);
    const auto start = std::chrono::steady_clock::now();
    auto next = start;
    for (std::size_t i = 0; i < samples; ++i) {
        const ClientOrderId id = id_base + args.warmup + static_cast<ClientOrderId>(i);
        if (!client.send_ioc(id)) {
            std::fprintf(stderr, "%s: send failed at %zu\n", stats.name.c_str(), i);
            break;
        }
        occupancy.push_back(gateway.matching_queue_size());
        ++stats.sent;
        const auto now = std::chrono::steady_clock::now();
        if (now < next) {
            if (pace == Pace::Sleep) {
                std::this_thread::sleep_until(next);
            } else {
                while (std::chrono::steady_clock::now() < next) {
                }
            }
        } else {
            next = now; // do not accumulate send-time debt into a catch-up burst
        }
        next += std::chrono::duration_cast<std::chrono::steady_clock::duration>(
            std::chrono::duration<double>(interval));
    }
    if (!client.wait_received(args.warmup + stats.sent, drain_timeout)) {
        std::fprintf(stderr, "%s: drain timeout, received=%zu expected=%zu\n", stats.name.c_str(), client.received(),
                     args.warmup + stats.sent);
    }
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    stats.elapsed_s = elapsed;
    stats.received = client.received();
    stats.rusage = rusage.stop();
    stats.io = subtract_metrics(gateway.io_metrics(), io_start);
    stats.occupancy = summarise_occupancy(std::move(occupancy));
    stats.intervals.end_to_end.reserve(stats.sent);
    for (std::size_t i = 0; i < stats.sent; ++i) {
        harvest(stats.intervals, client.account_id(), id_base + args.warmup + static_cast<ClientOrderId>(i));
    }
    stats.harvested = stats.intervals.end_to_end.size();
    stats.achieved_per_sec = elapsed > 0.0 ? static_cast<double>(stats.sent) / elapsed : 0.0;
    return stats;
}

WorkloadStats run_sustained(OrderEntryGateway& gateway, const Args& args, double offered, ClientOrderId id_base) {
    return run_paced(gateway, args, offered, args.sustained_samples, id_base, Pace::Spin,
                     "Sustained " + std::to_string(static_cast<int>(offered)) + "/s", 10s);
}

WorkloadStats run_soak(OrderEntryGateway& gateway, const Args& args, ClientOrderId id_base) {
    const double offered = static_cast<double>(args.soak_orders) / args.soak_seconds;
    std::string name = "Soak " + std::to_string(args.soak_orders) + "/" +
                       std::to_string(static_cast<int>(args.soak_seconds)) + "s";
    // Sleep-paced: a minute of the sustained spin-wait would pin a core and
    // perturb the thing we are measuring. Same no-debt rule if a send is late.
    return run_paced(gateway, args, offered, args.soak_orders, id_base, Pace::Sleep, std::move(name),
                     std::chrono::milliseconds{120'000});
}

WorkloadStats run_multi(OrderEntryGateway& gateway, const Args& args, int clients, ClientOrderId id_base) {
    WorkloadStats stats;
    stats.clients = clients;
    stats.name = "Multi " + std::to_string(clients) + " clients";

    std::vector<std::unique_ptr<BenchClient>> workers;
    workers.reserve(static_cast<std::size_t>(clients));
    for (int i = 0; i < clients; ++i) {
        auto worker = std::make_unique<BenchClient>(static_cast<AccountId>(1000 + i), args.market_data);
        if (!worker->connect(*gateway.local_port())) {
            std::fprintf(stderr, "multi: connect failed for client %d\n", i);
            return stats;
        }
        workers.push_back(std::move(worker));
    }
    for (int i = 0; i < clients; ++i) {
        warmup(*workers[static_cast<std::size_t>(i)], args.warmup,
               id_base + static_cast<ClientOrderId>(i) * 1'000'000);
    }

    const auto io_start = gateway.io_metrics();
    RusageScope rusage;
    std::vector<std::vector<std::size_t>> occupancy(static_cast<std::size_t>(clients));
    std::atomic<std::size_t> sent{0};
    std::vector<std::thread> threads;
    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < clients; ++i) {
        threads.emplace_back([&, i] {
            BenchClient& client = *workers[static_cast<std::size_t>(i)];
            auto& occ = occupancy[static_cast<std::size_t>(i)];
            occ.reserve(args.multi_samples_per_client);
            const ClientOrderId base = id_base + static_cast<ClientOrderId>(i) * 1'000'000 + args.warmup;
            for (std::size_t n = 0; n < args.multi_samples_per_client; ++n) {
                if (!client.send_ioc(base + static_cast<ClientOrderId>(n))) {
                    break;
                }
                occ.push_back(gateway.matching_queue_size());
                sent.fetch_add(1, std::memory_order_relaxed);
            }
            (void)client.wait_received(args.warmup + args.multi_samples_per_client, 15000ms);
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    stats.elapsed_s = elapsed;
    stats.rusage = rusage.stop();
    stats.io = subtract_metrics(gateway.io_metrics(), io_start);
    std::vector<std::size_t> merged;
    for (auto& bucket : occupancy) {
        merged.insert(merged.end(), bucket.begin(), bucket.end());
    }
    stats.occupancy = summarise_occupancy(std::move(merged));
    stats.sent = sent.load();
    for (int i = 0; i < clients; ++i) {
        stats.received += workers[static_cast<std::size_t>(i)]->received();
        const ClientOrderId base = id_base + static_cast<ClientOrderId>(i) * 1'000'000 + args.warmup;
        for (std::size_t n = 0; n < args.multi_samples_per_client; ++n) {
            harvest(stats.intervals, workers[static_cast<std::size_t>(i)]->account_id(),
                    base + static_cast<ClientOrderId>(n));
        }
    }
    stats.harvested = stats.intervals.end_to_end.size();
    stats.achieved_per_sec = elapsed > 0.0 ? static_cast<double>(stats.sent) / elapsed : 0.0;
    stats.offered_per_sec = stats.achieved_per_sec;
    return stats;
}

WorkloadStats run_burst(OrderEntryGateway& gateway, const Args& args, int clients, ClientOrderId id_base) {
    WorkloadStats stats;
    stats.clients = clients;
    stats.name = "Burst " + std::to_string(clients) + " clients";

    std::vector<std::unique_ptr<BenchClient>> workers;
    for (int i = 0; i < clients; ++i) {
        auto worker = std::make_unique<BenchClient>(static_cast<AccountId>(1000 + i), args.market_data);
        if (!worker->connect(*gateway.local_port())) {
            return stats;
        }
        workers.push_back(std::move(worker));
    }
    for (int i = 0; i < clients; ++i) {
        warmup(*workers[static_cast<std::size_t>(i)], args.warmup,
               id_base + static_cast<ClientOrderId>(i) * 1'000'000);
    }

    const auto io_start = gateway.io_metrics();
    RusageScope rusage;
    std::vector<std::vector<std::size_t>> occupancy(static_cast<std::size_t>(clients));
    std::atomic<std::size_t> sent{0};
    std::vector<std::thread> threads;
    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < clients; ++i) {
        threads.emplace_back([&, i] {
            BenchClient& client = *workers[static_cast<std::size_t>(i)];
            auto& occ = occupancy[static_cast<std::size_t>(i)];
            occ.reserve(args.burst_rounds * args.burst_size);
            ClientOrderId next_id = id_base + static_cast<ClientOrderId>(i) * 1'000'000 + args.warmup;
            std::size_t local_sent = 0;
            for (std::size_t round = 0; round < args.burst_rounds; ++round) {
                for (std::size_t n = 0; n < args.burst_size; ++n) {
                    if (!client.send_ioc(next_id++)) {
                        sent.fetch_add(local_sent, std::memory_order_relaxed);
                        return;
                    }
                    occ.push_back(gateway.matching_queue_size());
                    ++local_sent;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(args.burst_idle_ms));
            }
            sent.fetch_add(local_sent, std::memory_order_relaxed);
            (void)client.wait_received(args.warmup + local_sent, 15000ms);
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    stats.elapsed_s = elapsed;
    stats.rusage = rusage.stop();
    stats.io = subtract_metrics(gateway.io_metrics(), io_start);
    std::vector<std::size_t> merged;
    for (auto& bucket : occupancy) {
        merged.insert(merged.end(), bucket.begin(), bucket.end());
    }
    stats.occupancy = summarise_occupancy(std::move(merged));
    stats.sent = sent.load();
    for (int i = 0; i < clients; ++i) {
        stats.received += workers[static_cast<std::size_t>(i)]->received();
        const ClientOrderId base = id_base + static_cast<ClientOrderId>(i) * 1'000'000 + args.warmup;
        const std::size_t count = args.burst_rounds * args.burst_size;
        for (std::size_t n = 0; n < count; ++n) {
            harvest(stats.intervals, workers[static_cast<std::size_t>(i)]->account_id(),
                    base + static_cast<ClientOrderId>(n));
        }
    }
    stats.harvested = stats.intervals.end_to_end.size();
    stats.achieved_per_sec = elapsed > 0.0 ? static_cast<double>(stats.sent) / elapsed : 0.0;
    return stats;
}

WorkloadStats run_idle_population(OrderEntryGateway& gateway, const Args& args, int sessions,
                                  ClientOrderId id_base) {
    WorkloadStats stats;
    stats.clients = sessions;
    stats.name = "Idle pop=" + std::to_string(sessions);

    std::vector<std::unique_ptr<BenchClient>> sessions_held;
    sessions_held.reserve(static_cast<std::size_t>(sessions));
    for (int i = 0; i < sessions; ++i) {
        auto session = std::make_unique<BenchClient>(static_cast<AccountId>(1000 + i), args.market_data);
        if (!session->connect(*gateway.local_port())) {
            std::fprintf(stderr, "idle: connect failed at session %d / %d\n", i, sessions);
            return stats;
        }
        sessions_held.push_back(std::move(session));
    }

    BenchClient& active = *sessions_held.front();
    warmup(active, args.warmup, id_base);

    const auto io_start = gateway.io_metrics();
    RusageScope rusage;
    std::vector<std::size_t> occupancy;
    occupancy.reserve(args.sequential_samples);
    const auto start = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < args.sequential_samples; ++i) {
        const ClientOrderId id = id_base + args.warmup + static_cast<ClientOrderId>(i);
        if (!active.send_ioc(id)) {
            std::fprintf(stderr, "idle: send failed at %zu with %d sessions\n", i, sessions);
            break;
        }
        occupancy.push_back(gateway.matching_queue_size());
        ++stats.sent;
        if (!active.wait_received(args.warmup + i + 1, 2000ms)) {
            std::fprintf(stderr, "idle: timeout at %zu with %d sessions\n", i, sessions);
            break;
        }
        harvest(stats.intervals, active.account_id(), id);
    }
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    stats.elapsed_s = elapsed;
    stats.received = active.received();
    stats.rusage = rusage.stop();
    stats.io = subtract_metrics(gateway.io_metrics(), io_start);
    stats.occupancy = summarise_occupancy(std::move(occupancy));
    stats.harvested = stats.intervals.end_to_end.size();
    stats.achieved_per_sec = elapsed > 0.0 ? static_cast<double>(stats.sent) / elapsed : 0.0;
    stats.offered_per_sec = stats.achieved_per_sec;
    return stats;
}

} // namespace

int main(int argc, char** argv) {
    const Args args = parse_args(argc, argv);
    const TimerCalibration cal = calibrate_timer();
    print_timer_calibration(cal);

    MatchingCoreStats core{};
    if (args.run_core) {
        core = run_matching_core(args.core_samples);
        print_matching_core(core);
    }

    const bool run_tcp = args.run_sequential || args.run_sustained || args.run_multi || args.run_burst ||
                         args.run_soak || args.run_idle;
    if (!run_tcp) {
        return EXIT_SUCCESS;
    }

    // Soak stamps 1M keys; 2^22 slots keeps direct-map collisions tolerable.
    // Smaller tables overwrite in-flight traces. Default workloads stay on 2^20.
    latency::ScopedEnable tracing(args.run_soak ? (1 << 22) : (1 << 20));
    MarketDataBenchSink market_data;
    exchange::market_data::MarketDataRouter market_data_router{
        [&market_data](const protocol::Event& event) { market_data.send(event); },
    };
    if (args.market_data) {
        market_data_router.start();
    }
    auto gateway = make_gateway(args, market_data_router);
    if (!gateway->start()) {
        std::fprintf(stderr, "failed to start gateway\n");
        return EXIT_FAILURE;
    }
    seed_accounts(*gateway, std::max({1, args.max_clients, args.idle_clients}));

    std::printf("\nmdh order-path latency  (OrderEntryClient -> TCP -> OrderEntryGateway -> TCP -> client)\n");
    std::printf("%s NewOrder; one Accepted per command%s.\n", args.market_data ? "GTC" : "IOC",
                args.market_data ? " plus one public BookOrderAdded event" : " against an empty book");
    std::printf("writer_batch=%zu  writers=per-connection (sleeping)\n", args.writer_batch);
    std::printf("thread model: 2N+2 server threads (N readers, N writers, accept, matcher).\n");
    if (args.run_core) {
        std::printf("matching-core ceiling (same process, same order shape): %.0f/s (%.1f ns/op)\n",
                    core.ops_per_sec, core.ns_per_op);
    }

    std::vector<WorkloadStats> rows;
    ClientOrderId id_space = 1;

    if (args.run_sequential) {
        auto stats = run_sequential(*gateway, args, id_space);
        print_workload(stats, cal.measured_ticks_per_second);
        rows.push_back(std::move(stats));
        id_space += 2'000'000;
    }
    if (args.run_sustained) {
        for (const double rate : {1'000.0, 10'000.0, 50'000.0, 100'000.0}) {
            auto stats = run_sustained(*gateway, args, rate, id_space);
            print_workload(stats, cal.measured_ticks_per_second);
            rows.push_back(std::move(stats));
            id_space += 2'000'000;
        }
    }
    if (args.run_multi) {
        for (const int n : client_counts_up_to(args.max_clients)) {
            auto stats = run_multi(*gateway, args, n, id_space);
            print_workload(stats, cal.measured_ticks_per_second);
            rows.push_back(std::move(stats));
            id_space += static_cast<ClientOrderId>(n) * 1'000'000ULL + 2'000'000ULL;
        }
    }
    if (args.run_idle) {
        for (const int n : client_counts_up_to(args.idle_clients)) {
            auto stats = run_idle_population(*gateway, args, n, id_space);
            print_workload(stats, cal.measured_ticks_per_second);
            rows.push_back(std::move(stats));
            id_space += 2'000'000;
        }
    }
    if (args.run_soak) {
        auto stats = run_soak(*gateway, args, id_space);
        print_workload(stats, cal.measured_ticks_per_second);
        rows.push_back(std::move(stats));
    }
    if (args.run_burst) {
        const int n = std::min(args.max_clients, 8);
        auto stats = run_burst(*gateway, args, n, id_space);
        print_workload(stats, cal.measured_ticks_per_second);
        rows.push_back(std::move(stats));
    }

    std::printf("\nTwo ceilings (do not mix them):\n");
    if (args.run_core) {
        std::printf("| Surface                         | Achieved /s | ns/op or e2e p50 |\n");
        std::printf("| ------------------------------- | ----------: | ---------------: |\n");
        std::printf("| Matching-core only (no TCP)     | %11.0f | %10.1f ns |\n", core.ops_per_sec, core.ns_per_op);
    }
    std::printf("\nSummary (end-to-end over real TCP, microseconds)\n");
    std::printf("| Workload           | Clients | Achieved throughput |      p50 |      p90 |      p99 |     p99.9 |      max |\n");
    std::printf("| ------------------ | ------: | ------------------: | -------: | -------: | -------: | --------: | -------: |\n");
    for (const auto& row : rows) {
        print_table_row(row, cal.measured_ticks_per_second);
    }

    gateway->stop();
    market_data_router.stop();
    if (args.market_data) {
        std::printf("market_data_datagrams=%llu  queue_drops=%zu  queue_high_water=%zu\n",
                    static_cast<unsigned long long>(market_data.published()), market_data_router.dropped_count(),
                    market_data_router.queue_high_water_mark());
    }
    return EXIT_SUCCESS;
}
