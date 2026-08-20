// Matching-engine latency distribution, sustained throughput, and scaling
// behaviour under a deterministic mixed order stream.
//
// Not a Google Benchmark binary, for the same reason
// bench_end_to_end_latency.cpp is not one: Google Benchmark times a loop and
// reports aggregates over it, but a p99.9 needs the individual sample
// values, and a scaling curve needs one figure per configuration rather than
// one figure per loop. Both are easier to produce honestly with an explicit
// sample loop than by contorting an API that was not built to expose
// per-iteration timings.
//
// ── How each number here is produced ───────────────────────────────────────
// Two different measurements are reported for the same workload, on purpose,
// because neither alone is trustworthy for sub-microsecond operations:
//
//   * Batch timing. One timer read before a run of N operations and one
//     after. Timer cost and counter quantisation are divided by N, so this
//     is the figure to trust for a mean and for operations/second. It says
//     nothing about the shape of the distribution.
//   * Per-operation sampling. Two timer reads around every single
//     process() call. This is the only way to get percentiles, but each
//     sample carries two timer reads and is quantised to the counter's
//     effective resolution -- on this project's development machine that is
//     ~42 ns against operations costing a few hundred, which is not
//     negligible. The calibration block printed at the top of every run
//     states the exact figures so a reader can judge the percentiles rather
//     than take them at face value.
//
// The two are deliberately not reconciled by subtracting one from the other.
// A measured overhead is a caveat to state, not a correction to apply.
//
// Run from a Release build only.
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "exchange/matching/matching_engine.hpp"
#include "exchange/testing/hr_timer.hpp"
#include "exchange/testing/matching_scenarios.hpp"
#include "exchange/testing/matching_workload.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::testing;

namespace {

constexpr InstrumentId kInstrument = 1;
constexpr AccountId kMaker = 1;
constexpr AccountId kTaker = 2;
constexpr Price kBase = 10'000'000;

double g_ticks_per_second = 1e9;

[[nodiscard]] double to_ns(std::uint64_t ticks) {
    return ticks_to_ns(static_cast<double>(ticks), g_ticks_per_second);
}

void rule(const char* title) {
    std::printf("\n=== %s %s\n", title, std::string(std::max<std::size_t>(4, 74 - std::strlen(title)), '=').c_str());
}

// ── Measurement primitives ─────────────────────────────────────────────────

struct BatchResult {
    std::size_t operations = 0;
    double total_ns = 0.0;
    double ns_per_op = 0.0;
    double ops_per_second = 0.0;
};

[[nodiscard]] BatchResult time_batch(MatchingEngine& engine, const std::vector<ExchangeCommand>& commands) {
    const EventSink& sink = discard_events();
    const std::uint64_t start = timer_ticks();
    for (const auto& command : commands) {
        engine.process(command, sink);
    }
    const std::uint64_t end = timer_ticks();

    BatchResult result;
    result.operations = commands.size();
    result.total_ns = to_ns(end - start);
    result.ns_per_op = commands.empty() ? 0.0 : result.total_ns / static_cast<double>(commands.size());
    result.ops_per_second = result.ns_per_op > 0.0 ? 1e9 / result.ns_per_op : 0.0;
    return result;
}

// Returns one raw tick delta per command, in command order. Kept unsorted and
// unsummarised so the caller can slice it (by operation kind, say) before
// reducing it to percentiles.
[[nodiscard]] std::vector<std::uint64_t> collect_samples(MatchingEngine& engine,
                                                          const std::vector<ExchangeCommand>& commands) {
    const EventSink& sink = discard_events();
    std::vector<std::uint64_t> samples;
    samples.reserve(commands.size());
    for (const auto& command : commands) {
        const std::uint64_t start = timer_ticks();
        engine.process(command, sink);
        const std::uint64_t end = timer_ticks();
        samples.push_back(end - start);
    }
    return samples;
}

[[nodiscard]] LatencySummary time_sampled(MatchingEngine& engine, const std::vector<ExchangeCommand>& commands) {
    std::vector<std::uint64_t> samples = collect_samples(engine, commands);
    return summarise_latency(samples, g_ticks_per_second);
}

// Replays a workload's seed phase into a fresh engine. Never timed: this is
// the "given a book that already looks like this" precondition, not part of
// what is being measured.
void apply_seed(MatchingEngine& engine, const Workload& workload) {
    replay(engine, workload.seed, discard_events());
}

// ── Stream builders for the single-shape throughput cases ──────────────────

// N passive GTC orders landing on a book that already has `levels` price
// levels, cycling across them so no new levels are created.
[[nodiscard]] std::vector<ExchangeCommand> build_resting_stream(MatchingEngine& engine, SequentialIds& ids,
                                                                 std::size_t levels, std::size_t operations) {
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, levels, 1, 100);
    std::vector<ExchangeCommand> commands;
    commands.reserve(operations);
    for (std::size_t i = 0; i < operations; ++i) {
        const Price price = kBase - static_cast<Price>(i % levels);
        commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kMaker,
                                                      ids.take_client_order_id(), kInstrument, Side::Buy, price, 100)});
    }
    return commands;
}

// N aggressive IOC orders, each fully consuming exactly one resting order at
// the front of a single deep price level. Deliberately the simplest possible
// crossing shape -- one fill, one removal, no level churn -- so the figure
// is a clean upper bound on crossing throughput rather than a blend of
// matching cost and price-level bookkeeping.
[[nodiscard]] std::vector<ExchangeCommand> build_crossing_stream(MatchingEngine& engine, SequentialIds& ids,
                                                                  std::size_t operations) {
    constexpr Quantity kPerOrder = 10;
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Sell, kBase, 1, 1, operations, kPerOrder);
    std::vector<ExchangeCommand> commands;
    commands.reserve(operations);
    for (std::size_t i = 0; i < operations; ++i) {
        commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kTaker, ids.take_client_order_id(),
                                                      kInstrument, Side::Buy, kBase, kPerOrder, TimeInForce::IOC)});
    }
    return commands;
}

// ── Report sections ────────────────────────────────────────────────────────

void report_environment() {
    rule("Environment");
#if defined(__clang__)
    std::printf("Compiler:                   clang %d.%d.%d\n", __clang_major__, __clang_minor__,
                __clang_patchlevel__);
#elif defined(__GNUC__)
    std::printf("Compiler:                   gcc %d.%d.%d\n", __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__);
#else
    std::printf("Compiler:                   unknown\n");
#endif
    std::printf("C++ standard:               %ld\n", static_cast<long>(__cplusplus));
#if defined(NDEBUG)
    std::printf("NDEBUG:                     defined (optimised build)\n");
#else
    std::printf("NDEBUG:                     NOT defined -- this is a debug build, numbers are meaningless\n");
#endif
    std::printf("sizeof(ExchangeRestingOrder): %zu bytes\n", sizeof(ExchangeRestingOrder));
    std::printf("sizeof(ExchangeCommand):      %zu bytes\n", sizeof(ExchangeCommand));
    std::printf("sizeof(ExchangeEvent):        %zu bytes\n", sizeof(ExchangeEvent));
}

void report_mixed_workload(const std::vector<std::size_t>& operation_counts, std::uint64_t seed) {
    rule("Mixed workload: latency distribution and throughput");
    std::printf("Mix: 40%% resting / 25%% crossing / 20%% cancel / 10%% replace / 5%% IOC-FOK,\n"
                "one instrument, 1000 resting orders per side before the measured stream begins.\n\n");

    std::printf("%10s %10s %10s %10s %10s %10s %10s | %12s %14s\n", "ops", "p50 ns", "p90 ns", "p99 ns", "p99.9 ns",
                "max ns", "smpl mean", "batch ns/op", "ops/sec");
    std::printf("%s\n", std::string(120, '-').c_str());

    for (const std::size_t operations : operation_counts) {
        WorkloadConfig config;
        config.seed = seed;
        config.operation_count = operations;
        config.instrument_count = 1;
        config.initial_orders_per_side = 1'000;
        const Workload workload = generate_workload(config);

        // Every engine below is told how many orders its scenario will
        // leave resting, the way a production engine would be configured
        // from expected book depth. Left at the default, these figures
        // measure a directory growing into itself instead -- at a million
        // resting orders that is the difference between 81 and 1814 ns for
        // the same insert, which would swamp everything else on the page.
        MatchingEngine sampled_engine(config.instruments(), workload.resting_orders_at_end);
        apply_seed(sampled_engine, workload);
        std::vector<std::uint64_t> raw = collect_samples(sampled_engine, workload.operations);

        MatchingEngine batch_engine(config.instruments(), workload.resting_orders_at_end);
        apply_seed(batch_engine, workload);
        const BatchResult batch = time_batch(batch_engine, workload.operations);

        // Slice before summarising: summarise_latency sorts in place, which
        // would destroy the correspondence between sample and command.
        const bool detailed = operations == operation_counts.back();
        std::vector<std::vector<std::uint64_t>> by_kind;
        if (detailed) {
            by_kind.resize(kOpKindCount);
            for (std::size_t i = 0; i < raw.size(); ++i) {
                by_kind[static_cast<std::size_t>(workload.kinds[i])].push_back(raw[i]);
            }
        }

        const LatencySummary sampled = summarise_latency(raw, g_ticks_per_second);
        std::printf("%10zu %10.0f %10.0f %10.0f %10.0f %10.0f %10.0f | %12.1f %14.0f\n", operations, sampled.p50_ns,
                    sampled.p90_ns, sampled.p99_ns, sampled.p999_ns, sampled.max_ns, sampled.sampled_mean_ns,
                    batch.ns_per_op, batch.ops_per_second);

        if (detailed) {
            std::printf("\nSame %zu samples, split by what the operation was. This is what the aggregate\n"
                        "tail above is actually made of.\n\n",
                        operations);
            std::printf("%-26s %10s %10s %10s %10s %10s %10s\n", "operation", "count", "p50 ns", "p90 ns", "p99 ns",
                        "p99.9 ns", "max ns");
            std::printf("%s\n", std::string(92, '-').c_str());
            for (std::size_t kind = 0; kind < kOpKindCount; ++kind) {
                if (by_kind[kind].empty()) {
                    continue;
                }
                const LatencySummary summary = summarise_latency(by_kind[kind], g_ticks_per_second);
                std::printf("%-26s %10zu %10.0f %10.0f %10.0f %10.0f %10.0f\n",
                            to_string(static_cast<OpKind>(kind)), summary.count, summary.p50_ns, summary.p90_ns,
                            summary.p99_ns, summary.p999_ns, summary.max_ns);
            }
        }

        if (operations == operation_counts.back()) {
            const auto& counts = workload.counts;
            std::printf("\nComposition of the %zu-operation stream (as actually emitted):\n", operations);
            std::printf("  resting                     %10zu\n", counts.new_resting);
            std::printf("  crossing                    %10zu\n", counts.crossing);
            std::printf("  cancel                      %10zu\n", counts.cancel);
            std::printf("  replace, priority preserved %10zu\n", counts.replace_priority_preserving);
            std::printf("  replace, priority lost      %10zu\n", counts.replace_priority_losing);
            std::printf("  IOC                         %10zu\n", counts.ioc);
            std::printf("  FOK                         %10zu\n", counts.fok);
            std::printf("  downgraded to resting       %10zu  (wanted liquidity or a live order, found none)\n",
                        counts.downgraded_to_resting);
            std::printf("  resting orders after seed   %10zu\n", workload.resting_orders_after_seed);
            std::printf("  resting orders at end       %10zu\n", workload.resting_orders_at_end);
            std::printf("  price levels at end         %10zu bid / %zu ask\n", workload.bid_levels_at_end,
                        workload.ask_levels_at_end);
        }
    }
}

void report_throughput(std::size_t operations, std::uint64_t seed) {
    rule("Sustained throughput");
    std::printf("%-46s %12s %14s\n", "stream", "ns/op", "ops/sec");
    std::printf("%s\n", std::string(74, '-').c_str());

    {
        MatchingEngine engine({kInstrument}, operations + 1'024); // nothing is ever removed
        SequentialIds ids;
        const auto commands = build_resting_stream(engine, ids, 1'024, operations);
        const BatchResult result = time_batch(engine, commands);
        std::printf("%-46s %12.1f %14.0f\n", "resting (GTC, existing level, 1024 levels)", result.ns_per_op,
                    result.ops_per_second);
    }
    {
        MatchingEngine engine({kInstrument}, operations); // one resting order seeded per fill
        SequentialIds ids;
        const auto commands = build_crossing_stream(engine, ids, operations);
        const BatchResult result = time_batch(engine, commands);
        std::printf("%-46s %12.1f %14.0f\n", "crossing (IOC, one fill each, one level)", result.ns_per_op,
                    result.ops_per_second);
    }
    {
        WorkloadConfig config;
        config.seed = seed;
        config.operation_count = operations;
        config.initial_orders_per_side = 1'000;
        const Workload workload = generate_workload(config);
        MatchingEngine engine(config.instruments(), workload.resting_orders_at_end);
        apply_seed(engine, workload);
        const BatchResult result = time_batch(engine, workload.operations);
        std::printf("%-46s %12.1f %14.0f\n", "mixed (realistic 40/25/20/10/5)", result.ns_per_op,
                    result.ops_per_second);
    }
}

// Scaling: does cost track the number of resting orders, or the number of
// distinct price levels? The two are separated by holding one fixed while
// the other grows.
void report_scaling_book_size(std::size_t measured_ops) {
    rule("Scaling: resting orders vs. price levels");
    std::printf("Both columns time the same operation -- a GTC order onto an already-existing price\n"
                "level -- against books built two different ways, so the difference isolates which\n"
                "dimension the cost actually follows.\n\n");
    std::printf("%14s | %12s %12s | %12s %12s\n", "resting orders", "1024 levels", "(ns/op)", "1 order/level",
                "(ns/op)");
    std::printf("%s\n", std::string(72, '-').c_str());

    for (const std::size_t total : {std::size_t{1'024}, std::size_t{10'240}, std::size_t{102'400},
                                     std::size_t{1'024'000}}) {
        // Fixed level count, deepening FIFO queues.
        double deep_ns = 0.0;
        {
            MatchingEngine engine({kInstrument}, total + measured_ops);
            SequentialIds ids;
            constexpr std::size_t kLevels = 1'024;
            seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, kLevels, total / kLevels, 100);
            std::vector<ExchangeCommand> commands;
            commands.reserve(measured_ops);
            for (std::size_t i = 0; i < measured_ops; ++i) {
                commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kMaker,
                                                              ids.take_client_order_id(), kInstrument, Side::Buy,
                                                              kBase - static_cast<Price>(i % kLevels), 100)});
            }
            deep_ns = time_batch(engine, commands).ns_per_op;
        }

        // One order per level, so the level count *is* the order count.
        double wide_ns = 0.0;
        {
            MatchingEngine engine({kInstrument}, total + measured_ops);
            SequentialIds ids;
            seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, total, 1, 100);
            std::vector<ExchangeCommand> commands;
            commands.reserve(measured_ops);
            for (std::size_t i = 0; i < measured_ops; ++i) {
                commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kMaker,
                                                              ids.take_client_order_id(), kInstrument, Side::Buy,
                                                              kBase - static_cast<Price>(i % total), 100)});
            }
            wide_ns = time_batch(engine, commands).ns_per_op;
        }

        std::printf("%14zu | %12s %12.1f | %12s %12.1f\n", total, "", deep_ns, "", wide_ns);
    }
}

// What a tick-indexed price ladder would have to cover. A ladder is an array
// indexed by tick, so its size follows the *span* between the lowest and
// highest occupied price on a side, while its usefulness follows how many of
// those ticks are actually occupied. Both are reported here because the gap
// between them is the part of a ladder that would be empty, and because the
// span is what decides whether a fixed band can hold a book at all.
void report_level_distribution() {
    rule("Price-level distribution (what a tick ladder must cover)");
    std::printf("span is (highest - lowest occupied price + 1) on that side, in ticks; levels is how\n"
                "many of those ticks hold an order. \"in band\" is the share of resting orders lying\n"
                "within a band of that width anchored at the touch and extending away from it.\n"
                "Worst side of any instrument is reported for the multi-instrument rows.\n\n");
    std::printf("%-40s %8s %9s %10s %9s %9s %6s %8s\n", "scenario", "orders", "levels", "span", "in 1024", "in 8192",
                "band", "spilled");
    std::printf("%s\n", std::string(106, '-').c_str());

    struct SideStats {
        std::size_t orders = 0;
        std::size_t levels = 0;
        std::size_t span = 0;
        double in_1024 = 0.0;
        double in_8192 = 0.0;
    };

    // Orders arrive sorted by price priority, so the first is at the touch
    // and distance from it is the ladder offset an order would occupy.
    const auto measure_side = [](const std::vector<ExchangeRestingOrder>& orders) {
        SideStats stats;
        if (orders.empty()) {
            return stats;
        }
        Price low = orders.front().price;
        Price high = orders.front().price;
        const Price touch = orders.front().price;
        std::size_t distinct = 0;
        Price previous = 0;
        std::size_t within_1024 = 0;
        std::size_t within_8192 = 0;
        for (const auto& order : orders) {
            low = std::min(low, order.price);
            high = std::max(high, order.price);
            if (distinct == 0 || order.price != previous) {
                ++distinct;
                previous = order.price;
            }
            const Price distance = order.price > touch ? order.price - touch : touch - order.price;
            within_1024 += distance < 1'024 ? 1 : 0;
            within_8192 += distance < 8'192 ? 1 : 0;
        }
        const auto count = static_cast<double>(orders.size());
        stats.orders = orders.size();
        stats.levels = distinct;
        stats.span = static_cast<std::size_t>(high - low) + 1;
        stats.in_1024 = 100.0 * static_cast<double>(within_1024) / count;
        stats.in_8192 = 100.0 * static_cast<double>(within_8192) / count;
        return stats;
    };

    // The widest-spanning side across every instrument, which is what a
    // uniform band would have to be sized for, alongside what the engine
    // actually chose and how much of the book missed it.
    const auto report = [&](const char* name, const MatchingEngine& engine) {
        SideStats worst;
        const EngineStateSnapshot snapshot = engine.snapshot();
        for (const auto& instrument : snapshot.instruments) {
            for (const auto* side : {&instrument.bids, &instrument.asks}) {
                const SideStats stats = measure_side(*side);
                if (stats.span > worst.span) {
                    worst = stats;
                }
            }
        }
        std::printf("%-40s %8zu %9zu %10zu %8.1f%% %8.1f%% %6u %8zu\n", name, worst.orders, worst.levels, worst.span,
                    worst.in_1024, worst.in_8192, engine.ladder_band_ticks(), engine.out_of_band_levels());
    };

    for (const std::uint32_t instruments : {1U, 16U, 64U}) {
        WorkloadConfig config;
        config.operation_count = 200'000;
        config.instrument_count = instruments;
        config.initial_orders_per_side = 1'000;
        const Workload workload = generate_workload(config);
        MatchingEngine engine(config.instruments(), workload.resting_orders_at_end);
        apply_seed(engine, workload);
        replay(engine, workload.operations, discard_events());
        char label[64];
        std::snprintf(label, sizeof(label), "realistic workload, %u instrument%s", instruments,
                      instruments == 1 ? "" : "s");
        report(label, engine);
    }

    // The shapes this benchmark's own scaling rows build. These are not
    // markets -- they exist to push the price index until it breaks -- but
    // they are what the rest of this file measures, so a ladder's coverage of
    // them decides which rows it can move.
    for (const std::size_t levels : {std::size_t{1'024}, std::size_t{4'096}, std::size_t{65'536},
                                      std::size_t{262'144}, std::size_t{1'000'000}}) {
        MatchingEngine engine({kInstrument}, levels);
        SequentialIds ids;
        seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, levels, 1, 100);
        char label[64];
        std::snprintf(label, sizeof(label), "benchmark seed, %zu levels", levels);
        report(label, engine);
    }
}

void report_scaling_cancel(std::size_t measured_ops) {
    rule("Scaling: cancel vs. price-level count");
    std::printf("%14s %14s | %12s\n", "price levels", "orders/level", "ns/op");
    std::printf("%s\n", std::string(46, '-').c_str());

    for (const std::size_t levels : {std::size_t{1}, std::size_t{64}, std::size_t{4'096}, std::size_t{262'144}}) {
        const std::size_t per_level = std::max<std::size_t>(2, measured_ops / levels + 1);
        MatchingEngine engine({kInstrument}, levels * per_level);
        SequentialIds ids;
        std::vector<ClientOrderId> seeded;
        seeded.reserve(levels * per_level);
        seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, levels, per_level, 100, &seeded);

        // Interleave across levels, and never take a level's last order, so
        // the level count is the same at the end of the run as at the start.
        std::vector<ExchangeCommand> commands;
        commands.reserve(measured_ops);
        for (std::size_t round = 0; round + 1 < per_level && commands.size() < measured_ops; ++round) {
            for (std::size_t level = 0; level < levels && commands.size() < measured_ops; ++level) {
                commands.push_back(ExchangeCommand{cancel_order(ids.take_command_sequence(), kMaker,
                                                                 seeded[level * per_level + round], kInstrument)});
            }
        }
        const BatchResult result = time_batch(engine, commands);
        std::printf("%14zu %14zu | %12.1f\n", levels, per_level, result.ns_per_op);
    }
}

void report_scaling_sweep() {
    rule("Scaling: depth swept by one aggressive order");
    std::printf("%10s %10s | %12s %14s\n", "levels", "sweeps", "ns/op", "ns per level");
    std::printf("%s\n", std::string(52, '-').c_str());

    constexpr std::size_t kMaxResting = 262'144;
    constexpr Quantity kPerLevel = 10;
    for (const std::size_t levels : {std::size_t{1}, std::size_t{4}, std::size_t{16}, std::size_t{64},
                                      std::size_t{256}, std::size_t{1'024}}) {
        const std::size_t cases = std::max<std::size_t>(32, kMaxResting / levels);
        MatchingEngine engine({kInstrument}, cases * levels);
        SequentialIds ids;
        seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Sell, kBase, 1, cases * levels, 1, kPerLevel);

        std::vector<ExchangeCommand> commands;
        commands.reserve(cases);
        for (std::size_t i = 0; i < cases; ++i) {
            const Price worst = kBase + static_cast<Price>((i + 1) * levels - 1);
            commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kTaker,
                                                          ids.take_client_order_id(), kInstrument, Side::Buy, worst,
                                                          kPerLevel * levels, TimeInForce::IOC)});
        }
        const BatchResult result = time_batch(engine, commands);
        std::printf("%10zu %10zu | %12.1f %14.2f\n", levels, cases, result.ns_per_op,
                    result.ns_per_op / static_cast<double>(levels));
    }
}

void report_scaling_instruments(std::size_t operations, std::uint64_t seed) {
    rule("Scaling: instrument count");
    std::printf("Total resting depth is held at ~4096 orders per side across all instruments, so\n"
                "what varies is how that depth is partitioned, not how much of it there is.\n\n");
    std::printf("%12s %20s | %12s %14s %12s\n", "instruments", "orders/side/instr", "ns/op", "ops/sec", "p99 ns");
    std::printf("%s\n", std::string(78, '-').c_str());

    for (const std::uint32_t instruments : {1U, 4U, 16U, 64U}) {
        WorkloadConfig config;
        config.seed = seed;
        config.operation_count = operations;
        config.instrument_count = instruments;
        config.initial_orders_per_side = std::max<std::size_t>(1, 4'096 / instruments);
        const Workload workload = generate_workload(config);

        MatchingEngine batch_engine(config.instruments(), workload.resting_orders_at_end);
        apply_seed(batch_engine, workload);
        const BatchResult batch = time_batch(batch_engine, workload.operations);

        MatchingEngine sampled_engine(config.instruments(), workload.resting_orders_at_end);
        apply_seed(sampled_engine, workload);
        const LatencySummary sampled = time_sampled(sampled_engine, workload.operations);

        std::printf("%12u %20zu | %12.1f %14.0f %12.0f\n", instruments, config.initial_orders_per_side,
                    batch.ns_per_op, batch.ops_per_second, sampled.p99_ns);
    }
}

} // namespace

int main(int argc, char** argv) {
    bool quick = false;
    std::uint64_t seed = 0xC0FFEE'12345678ULL;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--quick") {
            quick = true;
        } else if (arg.rfind("--seed=", 0) == 0) {
            seed = std::strtoull(arg.c_str() + 7, nullptr, 0);
        } else {
            std::fprintf(stderr, "usage: %s [--quick] [--seed=N]\n", argv[0]);
            return EXIT_FAILURE;
        }
    }

    std::printf("mdh matching-engine baseline: latency distribution, throughput, scaling\n");
    std::printf("workload seed: 0x%llX%s\n", static_cast<unsigned long long>(seed), quick ? "  (--quick)" : "");

    report_environment();

    rule("Timer");
    const TimerCalibration calibration = calibrate_timer();
    g_ticks_per_second = calibration.measured_ticks_per_second;
    print_timer_calibration(calibration);
    std::printf("\nEvery per-operation percentile below is quantised to the effective resolution and\n"
                "includes the cost of the two reads that delimit it. Batch ns/op figures divide both\n"
                "of those across the whole run and are the ones to trust for means and throughput.\n");

    const std::vector<std::size_t> mixed_sizes =
        quick ? std::vector<std::size_t>{10'000, 100'000} : std::vector<std::size_t>{10'000, 100'000, 1'000'000};
    const std::size_t throughput_ops = quick ? 200'000 : 1'000'000;
    const std::size_t scaling_ops = quick ? 20'000 : 100'000;

    report_mixed_workload(mixed_sizes, seed);
    report_throughput(throughput_ops, seed);
    report_scaling_book_size(scaling_ops);
    report_level_distribution();
    report_scaling_cancel(scaling_ops);
    report_scaling_sweep();
    report_scaling_instruments(quick ? 50'000 : 200'000, seed);

    std::printf("\n");
    return EXIT_SUCCESS;
}
