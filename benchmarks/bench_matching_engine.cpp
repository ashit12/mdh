// Milestone 13: throughput of MatchingEngine::process() itself -- the
// single biggest gap docs/current_system_assessment.md identified this
// project as missing before Milestone 2 built it. Measures the actual
// matching core in isolation (no gateway, no TCP, no risk/ledger), the same
// "measure the thing itself, not the whole pipeline" instinct
// docs/current_system_assessment.md §9 flagged the old (pre-exchange)
// codebase as never having done for its own book-reconstruction code.
//
// Every case uses a fresh MatchingEngine per iteration where book depth
// must stay constant across iterations (state.PauseTiming()/ResumeTiming()
// bracket that untimed setup) -- otherwise a "rest one order" benchmark
// would silently turn into an unbounded-book-growth benchmark by iteration
// 10,000.
#include <benchmark/benchmark.h>

#include "exchange/matching/matching_engine.hpp"

using namespace mdh;
using namespace mdh::exchange;

namespace {

constexpr InstrumentId kInstrument = 1;

NewOrderCommand new_order(CommandSequence seq, AccountId account, ClientOrderId client_id, Side side, Price price,
                           Quantity qty, TimeInForce tif = TimeInForce::GTC) {
    return NewOrderCommand{.command_sequence = seq,
                            .account_id = account,
                            .client_order_id = client_id,
                            .instrument_id = kInstrument,
                            .side = side,
                            .price = price,
                            .quantity = qty,
                            .order_type = OrderType::Limit,
                            .time_in_force = tif};
}

void noop_sink(const ExchangeEvent&) {}

} // namespace

// A GTC order resting on an empty (or same-side-only) book: the pure
// "insert, no crossing" path -- allocates a new resting order and a new
// price level, never walks the opposite side at all.
static void BM_MatchingEngine_NewOrderRestsNoCrossing(benchmark::State& state) {
    MatchingEngine engine;
    ClientOrderId next_id = 1;
    for (auto _ : state) {
        // Alternate price so the book doesn't collapse onto one level
        // (irrelevant to a std::unordered_map-keyed book, but keeps this
        // benchmark representative of a book with many distinct price
        // levels rather than one degenerately deep one).
        const Price price = 100 + static_cast<Price>(next_id % 1000);
        engine.process(ExchangeCommand{new_order(next_id, 1, next_id, Side::Buy, price, 10)}, noop_sink);
        ++next_id;
    }
}
BENCHMARK(BM_MatchingEngine_NewOrderRestsNoCrossing);

// A fully-crossing IOC order against one resting order at the best price:
// the single-fill matching path (match_and_rest() with exactly one
// iteration), re-seeded with a fresh resting order every iteration since
// the incoming order always fully consumes it.
static void BM_MatchingEngine_SingleLevelFullFill(benchmark::State& state) {
    MatchingEngine engine;
    ClientOrderId next_id = 1;
    for (auto _ : state) {
        state.PauseTiming();
        engine.process(ExchangeCommand{new_order(next_id, 1, next_id, Side::Sell, 100, 10)}, noop_sink);
        ++next_id;
        state.ResumeTiming();

        engine.process(ExchangeCommand{new_order(next_id, 2, next_id, Side::Buy, 100, 10, TimeInForce::IOC)},
                        noop_sink);
        ++next_id;
    }
}
BENCHMARK(BM_MatchingEngine_SingleLevelFullFill);

// An aggressive order that walks and fully consumes `levels` distinct
// resting price levels in one process() call -- the multi-level matching
// path this project's own roadmap named as a benchmark target
// (docs/current_system_assessment.md §12 group 1). Re-seeds all `levels`
// resting orders before each timed call.
static void BM_MatchingEngine_MultiLevelSweep(benchmark::State& state) {
    const auto levels = static_cast<int>(state.range(0));
    MatchingEngine engine;
    ClientOrderId next_id = 1;
    Quantity total_qty = 0;
    for (auto _ : state) {
        state.PauseTiming();
        total_qty = 0;
        for (int level = 0; level < levels; ++level) {
            const Price price = 100 + static_cast<Price>(level);
            engine.process(ExchangeCommand{new_order(next_id, 1, next_id, Side::Sell, price, 10)}, noop_sink);
            ++next_id;
            total_qty += 10;
        }
        state.ResumeTiming();

        // Priced to cross every seeded level; IOC so any (unexpected)
        // remainder never rests and inflates a later iteration's book.
        engine.process(
            ExchangeCommand{new_order(next_id, 2, next_id, Side::Buy, 100 + levels, total_qty, TimeInForce::IOC)},
            noop_sink);
        ++next_id;
    }
    state.SetComplexityN(levels);
}
BENCHMARK(BM_MatchingEngine_MultiLevelSweep)->Arg(1)->Arg(4)->Arg(16)->Arg(64)->Arg(256);

// Cancel of a live, resting order -- O(1) hash lookup + book removal, the
// same operation shape as book::OrderBook::cancel_order() but through the
// authoritative matching book instead of the reconstructed one.
static void BM_MatchingEngine_Cancel(benchmark::State& state) {
    MatchingEngine engine;
    ClientOrderId next_id = 1;
    for (auto _ : state) {
        state.PauseTiming();
        const Price price = 100 + static_cast<Price>(next_id % 1000);
        engine.process(ExchangeCommand{new_order(next_id, 1, next_id, Side::Buy, price, 10)}, noop_sink);
        const ClientOrderId to_cancel = next_id;
        ++next_id;
        state.ResumeTiming();

        engine.process(
            ExchangeCommand{CancelOrderCommand{
                .command_sequence = next_id, .account_id = 1, .client_order_id = to_cancel, .instrument_id = kInstrument}},
            noop_sink);
        ++next_id;
    }
}
BENCHMARK(BM_MatchingEngine_Cancel);

// Quantity-decrease replace at the same price -- the in-place, priority-
// preserving path (see matching_engine.hpp's own "Replace policy" comment),
// as opposed to the cancel-plus-new path a repricing replace would take.
static void BM_MatchingEngine_ReplaceQuantityDecreasePreservesPriority(benchmark::State& state) {
    MatchingEngine engine;
    ClientOrderId next_id = 1;
    for (auto _ : state) {
        state.PauseTiming();
        const Price price = 100 + static_cast<Price>(next_id % 1000);
        engine.process(ExchangeCommand{new_order(next_id, 1, next_id, Side::Buy, price, 20)}, noop_sink);
        const ClientOrderId original_id = next_id;
        ++next_id;
        state.ResumeTiming();

        engine.process(ExchangeCommand{ReplaceOrderCommand{.command_sequence = next_id,
                                                             .account_id = 1,
                                                             .original_client_order_id = original_id,
                                                             .new_client_order_id = next_id,
                                                             .instrument_id = kInstrument,
                                                             .new_price = price,
                                                             .new_quantity = 10}},
                        noop_sink);
        ++next_id;
    }
}
BENCHMARK(BM_MatchingEngine_ReplaceQuantityDecreasePreservesPriority);

BENCHMARK_MAIN();
