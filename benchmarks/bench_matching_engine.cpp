// Milestone 15: matching-engine operation latency, measured without
// per-iteration timer manipulation.
//
// ── What changed from Milestone 13, and why ────────────────────────────────
// The original version of this file bracketed its per-iteration setup in
// state.PauseTiming()/state.ResumeTiming(). Google Benchmark's own
// documentation warns those calls are relatively heavyweight, and the
// numbers recorded in docs/benchmarks.md §4 showed exactly that: every case
// carrying one pause/resume pair landed at ~700-730 ns while the one case
// without any landed at 169 ns, i.e. roughly 500-560 ns of each "~700 ns"
// figure was the measurement apparatus. For operations whose real cost is a
// few hundred nanoseconds that is not a small correction, it is most of the
// number, and the honest response is to remove the apparatus rather than
// subtract an estimate of it.
//
// Every benchmark below therefore does *all* of its setup before the
// `for (auto _ : state)` loop, which Google Benchmark does not time, using
// one of three designs:
//
//   1. Steady state. The operation leaves the book in the same shape it
//      found it, so it can simply be repeated: a FOK that fails its
//      preflight check mutates nothing at all; a quantity-decrease replace
//      keeps the order in place; a reprice-replace that ping-pongs an order
//      between two otherwise-empty prices keeps the level count fixed.
//      These need no per-iteration setup of any kind.
//   2. Pre-generated independent cases. The book is seeded with N disjoint
//      targets and the timed loop consumes exactly one per iteration
//      (`->Iterations(N)` pins the two together). Cancels, sweeps and
//      single-level fills work this way.
//   3. Pre-built command vectors. Where the command stream itself takes work
//      to compute (chaining replace ids, for instance), the whole vector is
//      materialised up front and the timed loop only walks it.
//
// The residual per-iteration cost is a `std::vector` index and a call
// through an already-constructed EventSink, both of which are also present
// in the real gateway path.
//
// ── The one artefact that remains, stated rather than hidden ───────────────
// Designs 2 and 3 consume the book as they go, so total resting depth is not
// constant across a run and each reported figure is a mean over the range
// the run traversed. Where that range matters the benchmark takes depth as
// an explicit argument so the effect is visible as a trend instead of hiding
// inside one number, and docs/matching_engine_baseline.md records the range
// each case covered.
//
// Run from a Release build only.
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include "exchange/matching/matching_engine.hpp"
#include "exchange/testing/matching_scenarios.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::testing;

namespace {

constexpr InstrumentId kInstrument = 1;
constexpr AccountId kMaker = 1;
constexpr AccountId kTaker = 2;

// High enough that bids never reach 0 after any amount of walking down.
constexpr Price kBase = 10'000'000;

// Roughly how many timed operations each case aims for. Large enough that
// per-run fixed costs vanish, small enough that a whole `bench_matching_engine`
// run stays in the tens of seconds.
constexpr std::int64_t kTargetOps = 65'536;

// Caps how much resting depth a pre-seeded scenario may build, so a
// high-`levels` argument cannot turn into a multi-gigabyte book.
constexpr std::int64_t kMaxRestingOrders = 262'144;

[[nodiscard]] std::int64_t clamp_ops(std::int64_t ops) { return std::clamp<std::int64_t>(ops, 256, kTargetOps); }

// Registers one Benchmark object per argument value.
//
// Not `BENCHMARK(fn)->Apply(...)`: Iterations() is a property of the
// Benchmark object, not of an individual argument, so calling
// `bench->Arg(a)->Iterations(n)` in a loop leaves every argument running the
// *last* iteration count set. Cases here need genuinely different iteration
// counts -- a benchmark that seeds a 256-level cluster per operation cannot
// run as many operations as one that seeds a single order -- so each
// argument gets its own registration instead.
using IterationPolicy = std::int64_t (*)(std::int64_t);

void register_per_argument(const char* name, void (*fn)(benchmark::State&),
                            std::initializer_list<std::int64_t> arguments, IterationPolicy iterations_for,
                            bool with_complexity = false) {
    for (const std::int64_t argument : arguments) {
        auto* registered = benchmark::RegisterBenchmark(name, fn)->Arg(argument)->Iterations(iterations_for(argument));
        if (with_complexity) {
            registered->Complexity();
        }
    }
}

[[nodiscard]] std::int64_t fixed_target_ops(std::int64_t) { return kTargetOps; }

// FOK preflight re-reads the whole contra side on every call (see
// BM_Fok_Rejected_BackgroundDepth's comment), so a case with deep contra
// liquidity has to run fewer iterations or the benchmark itself becomes
// quadratic. This keeps the total scanned-order count per case roughly
// constant instead, which is what makes the per-op figures comparable
// across arguments.
constexpr std::int64_t kScanBudget = 64'000'000;
[[nodiscard]] std::int64_t ops_for_scan(std::int64_t contra_depth) {
    return clamp_ops(kScanBudget / std::max<std::int64_t>(contra_depth, 1));
}

// The background-depth case always has one crossable level on top of its
// argument, so a zero argument still scans one order.
[[nodiscard]] std::int64_t ops_for_scan_with_background(std::int64_t background) {
    return ops_for_scan(background + 1);
}

// One timed operation seeds `levels` price levels of its own, so the
// iteration count has to fall as the levels argument rises to keep the
// pre-seeded book bounded.
[[nodiscard]] std::int64_t ops_per_seeded_level(std::int64_t levels) {
    return clamp_ops(kMaxRestingOrders / std::max<std::int64_t>(levels, 1));
}

// Total preflight scanning across an executed-FOK run is about
// cases^2 * levels / 2 (each of `cases` orders re-reads a contra side that
// starts at cases*levels orders and drains to zero), so the iteration count
// falls with sqrt(levels) to keep every argument inside the same scan budget
// rather than degenerating into a quadratic blow-up at small `levels`.
[[nodiscard]] std::int64_t ops_for_executed_fok(std::int64_t levels) {
    return clamp_ops(static_cast<std::int64_t>(
        std::sqrt(static_cast<double>(kScanBudget) / static_cast<double>(std::max<std::int64_t>(levels, 1)))));
}

constexpr std::initializer_list<std::int64_t> kDepthArgs = {1, 16, 256, 4'096, 65'536};
constexpr std::initializer_list<std::int64_t> kLevelArgs = {1, 4, 16, 64, 256, 1'024};

} // namespace

// ── 1. Resting (non-crossing) orders ───────────────────────────────────────

// A GTC order that lands on a price level that already exists: no new
// std::map node, just a push_back onto that level's FIFO list plus an index
// insert. The book is pre-seeded with `range(0)` levels and the timed
// inserts cycle across them, so the level count is constant for the whole
// run and only the per-level queues grow.
static void BM_Rest_ExistingPriceLevel(benchmark::State& state) {
    const auto levels = static_cast<std::size_t>(state.range(0));
    MatchingEngine engine{kInstrument};
    SequentialIds ids;
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, levels, 1, 100);

    std::vector<ExchangeCommand> commands;
    commands.reserve(static_cast<std::size_t>(state.max_iterations));
    for (std::int64_t i = 0; i < state.max_iterations; ++i) {
        const Price price = kBase - static_cast<Price>(static_cast<std::size_t>(i) % levels);
        commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kMaker,
                                                      ids.take_client_order_id(), kInstrument, Side::Buy, price, 100)});
    }

    const EventSink& sink = discard_events();
    std::size_t next = 0;
    for (auto _ : state) {
        engine.process(commands[next++], sink);
    }
}

// A GTC order that opens a brand-new price level: the std::map insert case.
// Seeded levels sit on even prices and the timed inserts land on the odd
// prices between them, so each one is a genuine tree insertion into the
// middle of an existing map rather than a cheap append at one end.
static void BM_Rest_NewPriceLevel(benchmark::State& state) {
    const auto levels = static_cast<std::size_t>(state.range(0));
    MatchingEngine engine{kInstrument};
    SequentialIds ids;
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -2, levels, 1, 100);

    std::vector<ExchangeCommand> commands;
    commands.reserve(static_cast<std::size_t>(state.max_iterations));
    for (std::int64_t i = 0; i < state.max_iterations; ++i) {
        const Price price = kBase - 2 * static_cast<Price>(i) - 1;
        commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kMaker,
                                                      ids.take_client_order_id(), kInstrument, Side::Buy, price, 100)});
    }

    const EventSink& sink = discard_events();
    std::size_t next = 0;
    for (auto _ : state) {
        engine.process(commands[next++], sink);
    }
}

// ── 2. Single-level match ──────────────────────────────────────────────────

// An IOC buy that fully consumes exactly one ask level holding `range(0)`
// resting orders. Each iteration gets its own private level (levels are
// consumed in ascending price order, which is the order the matcher visits
// them in), so no iteration depends on any other's leftovers.
//
// `Iterations` falls as the per-level order count rises to keep the seeded
// book bounded; ItemsProcessed reports total fills so per-fill cost is
// directly readable.
static void BM_Match_SingleLevel(benchmark::State& state) {
    const auto orders_at_level = static_cast<std::size_t>(state.range(0));
    const auto cases = static_cast<std::size_t>(state.max_iterations);
    constexpr Quantity kPerOrder = 10;

    MatchingEngine engine{kInstrument};
    SequentialIds ids;
    // Ask levels, ascending: case i owns price kBase + i.
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Sell, kBase, 1, cases, orders_at_level, kPerOrder);

    std::vector<ExchangeCommand> commands;
    commands.reserve(cases);
    for (std::size_t i = 0; i < cases; ++i) {
        commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kTaker, ids.take_client_order_id(),
                                                      kInstrument, Side::Buy, kBase + static_cast<Price>(i),
                                                      kPerOrder * orders_at_level, TimeInForce::IOC)});
    }

    const EventSink& sink = discard_events();
    std::size_t next = 0;
    for (auto _ : state) {
        engine.process(commands[next++], sink);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(orders_at_level));
}

// ── 3. Multi-level sweep ───────────────────────────────────────────────────

// An IOC buy that walks `range(0)` distinct ask levels in one process()
// call. Case i owns a contiguous block of `levels` prices immediately above
// case i-1's block, and the blocks are consumed lowest-first, so each timed
// operation sweeps exactly its own block and stops.
static void BM_Sweep_Levels(benchmark::State& state) {
    const auto levels = static_cast<std::size_t>(state.range(0));
    const auto cases = static_cast<std::size_t>(state.max_iterations);
    constexpr Quantity kPerLevel = 10;

    MatchingEngine engine{kInstrument};
    SequentialIds ids;
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Sell, kBase, 1, cases * levels, 1, kPerLevel);

    std::vector<ExchangeCommand> commands;
    commands.reserve(cases);
    for (std::size_t i = 0; i < cases; ++i) {
        const Price worst = kBase + static_cast<Price>((i + 1) * levels - 1);
        commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kTaker, ids.take_client_order_id(),
                                                      kInstrument, Side::Buy, worst, kPerLevel * levels,
                                                      TimeInForce::IOC)});
    }

    const EventSink& sink = discard_events();
    std::size_t next = 0;
    for (auto _ : state) {
        engine.process(commands[next++], sink);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(levels));
    state.SetComplexityN(static_cast<std::int64_t>(levels));
}

// ── 4. Cancel ──────────────────────────────────────────────────────────────

// Cancel of a live resting order in a book holding `range(0)` distinct price
// levels. Each level is seeded with one more order than will ever be
// cancelled from it, so no level empties and the level count -- the thing
// the O(log P) map lookup in MatchingBook::erase_at() actually depends on --
// stays fixed for the whole run. Cancels are interleaved across levels
// rather than draining one level at a time, so the access pattern is not
// artificially cache-friendly.
static void BM_Cancel(benchmark::State& state) {
    const auto levels = static_cast<std::size_t>(state.range(0));
    const auto total_cancels = static_cast<std::size_t>(state.max_iterations);
    const std::size_t per_level = std::max<std::size_t>(1, total_cancels / levels);

    MatchingEngine engine{kInstrument};
    SequentialIds ids;
    std::vector<ClientOrderId> seeded;
    seeded.reserve(levels * (per_level + 1));
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, levels, per_level + 1, 100, &seeded);

    std::vector<ExchangeCommand> commands;
    commands.reserve(total_cancels);
    for (std::size_t round = 0; round < per_level && commands.size() < total_cancels; ++round) {
        for (std::size_t level = 0; level < levels && commands.size() < total_cancels; ++level) {
            const ClientOrderId victim = seeded[level * (per_level + 1) + round];
            commands.push_back(
                ExchangeCommand{cancel_order(ids.take_command_sequence(), kMaker, victim, kInstrument)});
        }
    }
    // A degenerate levels/iterations ratio can leave the vector short; top it
    // up with the still-untouched last order of each level so the timed loop
    // never runs off the end.
    for (std::size_t level = 0; commands.size() < total_cancels; ++level) {
        const ClientOrderId victim = seeded[(level % levels) * (per_level + 1) + per_level];
        commands.push_back(ExchangeCommand{cancel_order(ids.take_command_sequence(), kMaker, victim, kInstrument)});
    }

    const EventSink& sink = discard_events();
    std::size_t next = 0;
    for (auto _ : state) {
        engine.process(commands[next++], sink);
    }
}

// ── 5. Replace ─────────────────────────────────────────────────────────────

// Priority-preserving replace: same price, reduced quantity. MatchingEngine
// mutates the resting order in place, so the book's shape never changes and
// the same order can be shrunk over and over -- a true steady state with no
// per-iteration setup. Targets rotate across `range(0)` levels so the run is
// not repeatedly touching one hot cache line.
static void BM_Replace_PriorityPreserving(benchmark::State& state) {
    const auto levels = static_cast<std::size_t>(state.range(0));
    const auto total = static_cast<std::size_t>(state.max_iterations);
    // Every replace shaves one unit off the target, so the seeded quantity
    // has to outlast the whole run.
    const Quantity seed_quantity = static_cast<Quantity>(total / levels) + 16;

    MatchingEngine engine{kInstrument};
    SequentialIds ids;
    std::vector<ClientOrderId> current;
    current.reserve(levels);
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, levels, 1, seed_quantity, &current);

    std::vector<Quantity> remaining(levels, seed_quantity);
    std::vector<ExchangeCommand> commands;
    commands.reserve(total);
    for (std::size_t i = 0; i < total; ++i) {
        const std::size_t target = i % levels;
        const ClientOrderId fresh = ids.take_client_order_id();
        --remaining[target];
        commands.push_back(ExchangeCommand{replace_order(ids.take_command_sequence(), kMaker, current[target], fresh,
                                                          kInstrument, kBase - static_cast<Price>(target),
                                                          remaining[target])});
        current[target] = fresh;
    }

    const EventSink& sink = discard_events();
    std::size_t next = 0;
    for (auto _ : state) {
        engine.process(commands[next++], sink);
    }
}

// Priority-losing replace, quantity increase at the same price: the
// cancel-plus-new path. The order is removed and re-added at the price it
// already occupied, so the level count is unchanged and this too is a
// steady state -- only the resting quantity grows, by one unit per
// iteration.
static void BM_Replace_PriorityLosing_QuantityIncrease(benchmark::State& state) {
    const auto levels = static_cast<std::size_t>(state.range(0));
    const auto total = static_cast<std::size_t>(state.max_iterations);

    MatchingEngine engine{kInstrument};
    SequentialIds ids;
    std::vector<ClientOrderId> current;
    current.reserve(levels);
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -1, levels, 1, 1, &current);

    std::vector<Quantity> remaining(levels, 1);
    std::vector<ExchangeCommand> commands;
    commands.reserve(total);
    for (std::size_t i = 0; i < total; ++i) {
        const std::size_t target = i % levels;
        const ClientOrderId fresh = ids.take_client_order_id();
        ++remaining[target];
        commands.push_back(ExchangeCommand{replace_order(ids.take_command_sequence(), kMaker, current[target], fresh,
                                                          kInstrument, kBase - static_cast<Price>(target),
                                                          remaining[target])});
        current[target] = fresh;
    }

    const EventSink& sink = discard_events();
    std::size_t next = 0;
    for (auto _ : state) {
        engine.process(commands[next++], sink);
    }
}

// Priority-losing replace, price change: also cancel-plus-new, but the
// re-added order lands on a different price than the one it left, so it
// additionally destroys one std::map node and creates another. Each target
// ping-pongs between two adjacent, otherwise-unoccupied prices, which keeps
// the total level count constant across the run.
static void BM_Replace_PriorityLosing_PriceChange(benchmark::State& state) {
    const auto levels = static_cast<std::size_t>(state.range(0));
    const auto total = static_cast<std::size_t>(state.max_iterations);

    MatchingEngine engine{kInstrument};
    SequentialIds ids;
    std::vector<ClientOrderId> current;
    current.reserve(levels);
    // Stride 2 leaves an unoccupied price next to every seeded level for
    // that level's order to move onto and back from.
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Buy, kBase, -2, levels, 1, 100, &current);

    std::vector<bool> shifted(levels, false);
    std::vector<ExchangeCommand> commands;
    commands.reserve(total);
    for (std::size_t i = 0; i < total; ++i) {
        const std::size_t target = i % levels;
        const ClientOrderId fresh = ids.take_client_order_id();
        shifted[target] = !shifted[target];
        const Price home = kBase - 2 * static_cast<Price>(target);
        commands.push_back(ExchangeCommand{replace_order(ids.take_command_sequence(), kMaker, current[target], fresh,
                                                          kInstrument, shifted[target] ? home - 1 : home, 100)});
        current[target] = fresh;
    }

    const EventSink& sink = discard_events();
    std::size_t next = 0;
    for (auto _ : state) {
        engine.process(commands[next++], sink);
    }
}

// ── 6. Fill-or-kill ────────────────────────────────────────────────────────

// FOK rejected in preflight: the order asks for one unit more than the whole
// crossable side holds, so MatchingEngine::crossable_quantity() runs, finds
// the book short, and emits OrderRejected without touching a single resting
// order. Nothing mutates, so this is exactly repeatable -- the cleanest
// steady state in this file, and therefore the cleanest measurement of the
// preflight path on its own.
//
// `range(0)` is the number of crossable levels the preflight has to add up.
static void BM_Fok_Rejected_CrossableLevels(benchmark::State& state) {
    const auto levels = static_cast<std::size_t>(state.range(0));
    constexpr Quantity kPerLevel = 10;

    MatchingEngine engine{kInstrument};
    SequentialIds ids;
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Sell, kBase, 1, levels, 1, kPerLevel);

    const Price worst = kBase + static_cast<Price>(levels) - 1;
    const Quantity unfillable = kPerLevel * levels + 1;
    std::vector<ExchangeCommand> commands;
    commands.reserve(static_cast<std::size_t>(state.max_iterations));
    for (std::int64_t i = 0; i < state.max_iterations; ++i) {
        commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kTaker, ids.take_client_order_id(),
                                                      kInstrument, Side::Buy, worst, unfillable, TimeInForce::FOK)});
    }

    const EventSink& sink = discard_events();
    std::size_t next = 0;
    for (auto _ : state) {
        engine.process(commands[next++], sink);
    }
}

// The same rejected FOK, but with the crossable liquidity pinned at exactly
// one level and `range(0)` *non-crossable* levels parked far above it.
//
// This exists to answer a specific structural question about
// crossable_quantity(): it obtains the contra side via
// MatchingBook::all_asks(), which materialises a std::vector copy of every
// resting order on that side before the loop that stops at the first
// non-crossing level ever runs. If preflight cost were a function of levels
// actually crossed, this benchmark would be flat in `range(0)`. If it grows
// with `range(0)`, the cost is a function of total contra-side depth
// instead. Also a perfect steady state -- a rejected FOK mutates nothing.
static void BM_Fok_Rejected_BackgroundDepth(benchmark::State& state) {
    const auto background = static_cast<std::size_t>(state.range(0));
    constexpr Quantity kPerLevel = 10;

    MatchingEngine engine{kInstrument};
    SequentialIds ids;
    // One crossable level at the touch...
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Sell, kBase, 1, 1, 1, kPerLevel);
    // ...and a wall of depth the FOK's limit price can never reach.
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Sell, kBase + 1'000'000, 1, background, 1, kPerLevel);

    std::vector<ExchangeCommand> commands;
    commands.reserve(static_cast<std::size_t>(state.max_iterations));
    for (std::int64_t i = 0; i < state.max_iterations; ++i) {
        commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kTaker, ids.take_client_order_id(),
                                                      kInstrument, Side::Buy, kBase, kPerLevel + 1,
                                                      TimeInForce::FOK)});
    }

    const EventSink& sink = discard_events();
    std::size_t next = 0;
    for (auto _ : state) {
        engine.process(commands[next++], sink);
    }
    state.SetComplexityN(static_cast<std::int64_t>(background));
}

// FOK that passes preflight and executes in full across `range(0)` levels:
// preflight walk plus the same matching walk BM_Sweep_Levels measures, so
// the gap between the two at equal `levels` is what the all-or-none
// guarantee costs.
//
// Unlike the rejected cases this one consumes the book, so the contra side
// shrinks from `Iterations * levels` orders down to zero over the run and
// the reported figure is a mean across that range.
static void BM_Fok_Executed(benchmark::State& state) {
    const auto levels = static_cast<std::size_t>(state.range(0));
    const auto cases = static_cast<std::size_t>(state.max_iterations);
    constexpr Quantity kPerLevel = 10;

    MatchingEngine engine{kInstrument};
    SequentialIds ids;
    seed_resting_orders(engine, ids, kMaker, kInstrument, Side::Sell, kBase, 1, cases * levels, 1, kPerLevel);

    std::vector<ExchangeCommand> commands;
    commands.reserve(cases);
    for (std::size_t i = 0; i < cases; ++i) {
        const Price worst = kBase + static_cast<Price>((i + 1) * levels - 1);
        commands.push_back(ExchangeCommand{new_order(ids.take_command_sequence(), kTaker, ids.take_client_order_id(),
                                                      kInstrument, Side::Buy, worst, kPerLevel * levels,
                                                      TimeInForce::FOK)});
    }

    const EventSink& sink = discard_events();
    std::size_t next = 0;
    for (auto _ : state) {
        engine.process(commands[next++], sink);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(levels));
}

// Registration happens here rather than through the BENCHMARK macro so that
// each argument can carry its own iteration count (see
// register_per_argument's comment), which in turn is why this file supplies
// its own main() instead of BENCHMARK_MAIN().
int main(int argc, char** argv) {
    register_per_argument("BM_Rest_ExistingPriceLevel", BM_Rest_ExistingPriceLevel, kDepthArgs, fixed_target_ops);
    register_per_argument("BM_Rest_NewPriceLevel", BM_Rest_NewPriceLevel, kDepthArgs, fixed_target_ops);
    register_per_argument("BM_Match_SingleLevel", BM_Match_SingleLevel, {1, 4, 16, 64, 256}, ops_per_seeded_level);
    register_per_argument("BM_Sweep_Levels", BM_Sweep_Levels, kLevelArgs, ops_per_seeded_level,
                          /*with_complexity=*/true);
    register_per_argument("BM_Cancel", BM_Cancel, kDepthArgs, fixed_target_ops);
    register_per_argument("BM_Replace_PriorityPreserving", BM_Replace_PriorityPreserving, kDepthArgs,
                          fixed_target_ops);
    register_per_argument("BM_Replace_PriorityLosing_QuantityIncrease", BM_Replace_PriorityLosing_QuantityIncrease,
                          kDepthArgs, fixed_target_ops);
    register_per_argument("BM_Replace_PriorityLosing_PriceChange", BM_Replace_PriorityLosing_PriceChange, kDepthArgs,
                          fixed_target_ops);
    register_per_argument("BM_Fok_Rejected_CrossableLevels", BM_Fok_Rejected_CrossableLevels, kLevelArgs,
                          ops_for_scan);
    register_per_argument("BM_Fok_Rejected_BackgroundDepth", BM_Fok_Rejected_BackgroundDepth,
                          {0, 16, 256, 4'096, 65'536}, ops_for_scan_with_background);
    register_per_argument("BM_Fok_Executed", BM_Fok_Executed, kLevelArgs, ops_for_executed_fok);

    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return EXIT_FAILURE;
    }
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return EXIT_SUCCESS;
}
