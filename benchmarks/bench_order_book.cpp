// Throughput of the trader-side reconstructed book
// (book::OrderBook) -- add/cancel/modify, plus top_bids()/top_asks() query
// cost at varying depth. A comparison of alternative book representations
// would mean building a second representation from scratch, a large design
// exercise unto itself; instead this measures how the one representation
// that exists (std::map-per-side + std::list-per-
// level + unordered_map index, order_book.hpp's own documented complexity
// analysis) behaves as book depth grows, which is the input any future
// alternative-representation comparison would need as its baseline.
#include <benchmark/benchmark.h>

#include <cstdint>

#include "book/order_book.hpp"

using namespace mdh;
using namespace mdh::book;

namespace {

// Seeds `count` resting bids at distinct, increasing prices (so each lands
// on its own price level -- the worst case for std::map's O(log P) lookup,
// and the case that actually exercises "how does this scale with distinct
// price levels" rather than "how does this scale with orders piled onto
// one level's FIFO list").
void seed_bids(OrderBook& book, OrderId first_id, std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) {
        (void)book.add_order(first_id + i, static_cast<Price>(100 + i), 10, Side::Buy);
    }
}

} // namespace

static void BM_OrderBook_AddOrderNewPriceLevel(benchmark::State& state) {
    OrderBook book;
    OrderId next_id = 1;
    for (auto _ : state) {
        (void)book.add_order(next_id, static_cast<Price>(100 + next_id), 10, Side::Buy);
        ++next_id;
    }
}
BENCHMARK(BM_OrderBook_AddOrderNewPriceLevel);

static void BM_OrderBook_AddOrderSamePriceLevel(benchmark::State& state) {
    OrderBook book;
    OrderId next_id = 1;
    for (auto _ : state) {
        (void)book.add_order(next_id, 100, 10, Side::Buy);
        ++next_id;
    }
}
BENCHMARK(BM_OrderBook_AddOrderSamePriceLevel);

// Cancel cost as book depth (distinct price levels) grows -- the O(log P)
// map lookup order_book.hpp's own class comment documents as the dominant
// cost, isolated from add_order()'s cost by pre-seeding outside the timed
// region.
static void BM_OrderBook_CancelOrder(benchmark::State& state) {
    const auto depth = static_cast<std::size_t>(state.range(0));
    OrderBook book;
    OrderId next_id = 1;
    for (auto _ : state) {
        state.PauseTiming();
        seed_bids(book, next_id, depth);
        const OrderId to_cancel = next_id; // first-inserted, deepest-to-find-nothing-special: any id works, O(1) hash lookup either way
        next_id += static_cast<OrderId>(depth);
        state.ResumeTiming();

        (void)book.cancel_order(to_cancel);

        state.PauseTiming();
        book.clear(); // reset for the next iteration's fresh seed
        state.ResumeTiming();
    }
    state.SetComplexityN(static_cast<std::int64_t>(depth));
}
BENCHMARK(BM_OrderBook_CancelOrder)->Arg(1)->Arg(16)->Arg(64)->Arg(256)->Arg(1024);

static void BM_OrderBook_ModifyOrderRepriceLosesPriority(benchmark::State& state) {
    OrderBook book;
    OrderId next_id = 1;
    for (auto _ : state) {
        state.PauseTiming();
        (void)book.add_order(next_id, 100, 10, Side::Buy);
        const OrderId to_modify = next_id;
        ++next_id;
        state.ResumeTiming();

        (void)book.modify_order(to_modify, 101, 8);

        state.PauseTiming();
        book.clear();
        state.ResumeTiming();
    }
}
BENCHMARK(BM_OrderBook_ModifyOrderRepriceLosesPriority);

// Query cost (best_bid()/top_bids()) as a function of requested depth `n`
// against a book with 1024 distinct bid levels -- the read path a UI
// gateway's GET /api/book/:id and every SSE "book" event
// actually exercise live.
static void BM_OrderBook_TopBids(benchmark::State& state) {
    const auto n = static_cast<std::size_t>(state.range(0));
    OrderBook book;
    seed_bids(book, 1, 1024);
    for (auto _ : state) {
        auto levels = book.top_bids(n);
        benchmark::DoNotOptimize(levels.data());
    }
}
BENCHMARK(BM_OrderBook_TopBids)->Arg(1)->Arg(10)->Arg(100)->Arg(1024);

BENCHMARK_MAIN();
