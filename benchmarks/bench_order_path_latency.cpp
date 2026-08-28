// End-to-end order-path latency: a real OrderEntryClient talks to a real
// OrderEntryGateway over loopback TCP -- the same path trading_server uses
// for order entry. MatchingEngine is never called directly.
//
// Workloads: sequential, sustained (offered rates), multi-client, burst.
// Raw samples are stored and percentiles computed after the timed region.
//
// Release builds only. Debug/sanitizer numbers are not representative.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <sys/resource.h>
#include <thread>
#include <vector>

#include "exchange/gateway/order_entry_gateway.hpp"
#include "exchange/latency/latency_tracer.hpp"
#include "exchange/testing/hr_timer.hpp"
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
    std::size_t writer_batch = 4;
    bool run_sequential = true;
    bool run_sustained = true;
    bool run_multi = true;
    bool run_burst = true;
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
            if (const char* v = next()) args.max_clients = std::atoi(v);
        } else if (flag == "--writer-batch") {
            if (const char* v = next()) {
                const auto n = static_cast<std::size_t>(std::atoll(v));
                args.writer_batch = n == 0 ? 1 : n;
            }
        } else if (flag == "--workload") {
            const char* v = next();
            if (v == nullptr) continue;
            const std::string w = v;
            args.run_sequential = args.run_sustained = args.run_multi = args.run_burst = false;
            if (w == "sequential") args.run_sequential = true;
            else if (w == "sustained") args.run_sustained = true;
            else if (w == "multi") args.run_multi = true;
            else if (w == "burst") args.run_burst = true;
            else if (w == "all") args.run_sequential = args.run_sustained = args.run_multi = args.run_burst = true;
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
    std::size_t harvested = 0;
    IntervalSet intervals;
    OccupancySummary occupancy;
    RusageDelta rusage;
    OrderEntryIoMetrics io;
};

void print_workload(const WorkloadStats& stats, double ticks_per_second) {
    std::printf("\n== %s  clients=%d  offered=%.0f/s  achieved=%.0f/s  sent=%zu  traces=%zu ==\n", stats.name.c_str(),
                stats.clients, stats.offered_per_sec, stats.achieved_per_sec, stats.sent, stats.harvested);
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
    explicit BenchClient(AccountId account_id) : account_id_(account_id), client_([this](const Message&) {
                                                     received_.fetch_add(1, std::memory_order_release);
                                                 }) {}

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
            .time_in_force = TimeInForce::IOC,
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
    std::atomic<std::size_t> received_{0};
    OrderEntryClient client_;
};

[[nodiscard]] std::unique_ptr<OrderEntryGateway> make_gateway(const Args& args) {
    return std::make_unique<OrderEntryGateway>(0, OrderEntryGatewayOptions{
                                                      .instruments = {kInstrument},
                                                      .matching_queue_capacity = 8192,
                                                      .outbound_queue_capacity = 8192,
                                                      .writer_batch = args.writer_batch,
                                                      .enable_io_metrics = true,
                                                  });
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
    BenchClient client(1000);
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
    stats.rusage = rusage.stop();
    stats.io = subtract_metrics(gateway.io_metrics(), io_start);
    stats.occupancy = summarise_occupancy(std::move(occupancy));
    stats.harvested = stats.intervals.end_to_end.size();
    stats.achieved_per_sec = elapsed > 0.0 ? static_cast<double>(stats.sent) / elapsed : 0.0;
    stats.offered_per_sec = stats.achieved_per_sec;
    return stats;
}

WorkloadStats run_sustained(OrderEntryGateway& gateway, const Args& args, double offered, ClientOrderId id_base) {
    WorkloadStats stats;
    stats.clients = 1;
    stats.offered_per_sec = offered;
    stats.name = "Sustained " + std::to_string(static_cast<int>(offered)) + "/s";

    BenchClient client(1000);
    if (!client.connect(*gateway.local_port())) {
        return stats;
    }
    warmup(client, args.warmup, id_base);

    const auto io_start = gateway.io_metrics();
    RusageScope rusage;
    std::vector<std::size_t> occupancy;
    occupancy.reserve(args.sustained_samples);
    const auto interval = std::chrono::duration<double>(offered > 0.0 ? 1.0 / offered : 0.0);
    const auto start = std::chrono::steady_clock::now();
    auto next = start;
    for (std::size_t i = 0; i < args.sustained_samples; ++i) {
        const ClientOrderId id = id_base + args.warmup + static_cast<ClientOrderId>(i);
        if (!client.send_ioc(id)) {
            break;
        }
        occupancy.push_back(gateway.matching_queue_size());
        ++stats.sent;
        const auto now = std::chrono::steady_clock::now();
        if (now < next) {
            while (std::chrono::steady_clock::now() < next) {
            }
        } else {
            next = now; // do not accumulate send-time debt into a catch-up burst
        }
        next += std::chrono::duration_cast<std::chrono::steady_clock::duration>(
            std::chrono::duration<double>(interval));
    }
    (void)client.wait_received(args.warmup + stats.sent, 10000ms);
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    stats.rusage = rusage.stop();
    stats.io = subtract_metrics(gateway.io_metrics(), io_start);
    stats.occupancy = summarise_occupancy(std::move(occupancy));
    for (std::size_t i = 0; i < stats.sent; ++i) {
        harvest(stats.intervals, client.account_id(), id_base + args.warmup + static_cast<ClientOrderId>(i));
    }
    stats.harvested = stats.intervals.end_to_end.size();
    stats.achieved_per_sec = elapsed > 0.0 ? static_cast<double>(stats.sent) / elapsed : 0.0;
    return stats;
}

WorkloadStats run_multi(OrderEntryGateway& gateway, const Args& args, int clients, ClientOrderId id_base) {
    WorkloadStats stats;
    stats.clients = clients;
    stats.name = "Multi " + std::to_string(clients) + " clients";

    std::vector<std::unique_ptr<BenchClient>> workers;
    workers.reserve(static_cast<std::size_t>(clients));
    for (int i = 0; i < clients; ++i) {
        auto worker = std::make_unique<BenchClient>(static_cast<AccountId>(1000 + i));
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
    stats.rusage = rusage.stop();
    stats.io = subtract_metrics(gateway.io_metrics(), io_start);
    std::vector<std::size_t> merged;
    for (auto& bucket : occupancy) {
        merged.insert(merged.end(), bucket.begin(), bucket.end());
    }
    stats.occupancy = summarise_occupancy(std::move(merged));
    stats.sent = sent.load();
    for (int i = 0; i < clients; ++i) {
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
        auto worker = std::make_unique<BenchClient>(static_cast<AccountId>(1000 + i));
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
    stats.rusage = rusage.stop();
    stats.io = subtract_metrics(gateway.io_metrics(), io_start);
    std::vector<std::size_t> merged;
    for (auto& bucket : occupancy) {
        merged.insert(merged.end(), bucket.begin(), bucket.end());
    }
    stats.occupancy = summarise_occupancy(std::move(merged));
    stats.sent = sent.load();
    for (int i = 0; i < clients; ++i) {
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

} // namespace

int main(int argc, char** argv) {
    const Args args = parse_args(argc, argv);
    const TimerCalibration cal = calibrate_timer();
    print_timer_calibration(cal);

    latency::ScopedEnable tracing(1 << 20);
    auto gateway = make_gateway(args);
    if (!gateway->start()) {
        std::fprintf(stderr, "failed to start gateway\n");
        return EXIT_FAILURE;
    }
    seed_accounts(*gateway, std::max(args.max_clients, 1));

    std::printf("\nmdh order-path latency  (OrderEntryClient -> TCP -> OrderEntryGateway -> TCP -> client)\n");
    std::printf("IOC NewOrder against an empty book; one Accepted per command.\n");
    std::printf("writer_batch=%zu  writers=per-connection (sleeping)\n", args.writer_batch);

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
        for (const int n : {1, 2, 4, 8, 16}) {
            if (n > args.max_clients) {
                break;
            }
            auto stats = run_multi(*gateway, args, n, id_space);
            print_workload(stats, cal.measured_ticks_per_second);
            rows.push_back(std::move(stats));
            id_space += 20'000'000;
        }
    }
    if (args.run_burst) {
        const int n = std::min(args.max_clients, 8);
        auto stats = run_burst(*gateway, args, n, id_space);
        print_workload(stats, cal.measured_ticks_per_second);
        rows.push_back(std::move(stats));
    }

    std::printf("\nSummary (end-to-end, microseconds)\n");
    std::printf("| Workload           | Clients | Achieved throughput |      p50 |      p90 |      p99 |     p99.9 |      max |\n");
    std::printf("| ------------------ | ------: | ------------------: | -------: | -------: | -------: | --------: | -------: |\n");
    for (const auto& row : rows) {
        print_table_row(row, cal.measured_ticks_per_second);
    }

    gateway->stop();
    return EXIT_SUCCESS;
}
