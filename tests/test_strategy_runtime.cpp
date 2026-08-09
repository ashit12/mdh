#include <gtest/gtest.h>

#include <vector>

#include "trader/strategies/strategy_runtime.hpp"

// Pure-logic unit tests for StrategyRuntime (Milestone 10) -- fed synthetic
// protocol::Event values directly and a fresh book::BookManager, exactly
// the same style replay's own tests use synthetic Events (see
// tests/test_sequence_recovery.cpp), with no file/socket/OMS involved.
using namespace mdh;
using namespace mdh::protocol;
using namespace mdh::book;
using namespace mdh::trader::strategies;

namespace {
constexpr InstrumentId kInstrumentA = 1;
constexpr InstrumentId kInstrumentB = 2;
} // namespace

TEST(StrategyRuntime, DispatchesToTheSinkSubscribedForTheEventsInstrument) {
    BookManager books;
    StrategyRuntime runtime;

    std::vector<InstrumentId> seen;
    runtime.subscribe(kInstrumentA, [&](InstrumentId id, const OrderBook&) { seen.push_back(id); });

    runtime.on_event(Event{AddOrder{.sequence_number = 1,
                                     .timestamp_ns = 1,
                                     .order_id = 1,
                                     .instrument_id = kInstrumentA,
                                     .price = 100,
                                     .quantity = 10,
                                     .side = Side::Buy}},
                      books);

    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0], kInstrumentA);
}

TEST(StrategyRuntime, DoesNotDispatchToASinkSubscribedForADifferentInstrument) {
    BookManager books;
    StrategyRuntime runtime;

    bool called = false;
    runtime.subscribe(kInstrumentB, [&](InstrumentId, const OrderBook&) { called = true; });

    runtime.on_event(Event{AddOrder{.sequence_number = 1,
                                     .timestamp_ns = 1,
                                     .order_id = 1,
                                     .instrument_id = kInstrumentA,
                                     .price = 100,
                                     .quantity = 10,
                                     .side = Side::Buy}},
                      books);

    EXPECT_FALSE(called);
}

TEST(StrategyRuntime, MultipleSinksForTheSameInstrumentAreAllCalled) {
    BookManager books;
    StrategyRuntime runtime;

    int first_calls = 0;
    int second_calls = 0;
    runtime.subscribe(kInstrumentA, [&](InstrumentId, const OrderBook&) { ++first_calls; });
    runtime.subscribe(kInstrumentA, [&](InstrumentId, const OrderBook&) { ++second_calls; });

    runtime.on_event(Event{AddOrder{.sequence_number = 1,
                                     .timestamp_ns = 1,
                                     .order_id = 1,
                                     .instrument_id = kInstrumentA,
                                     .price = 100,
                                     .quantity = 10,
                                     .side = Side::Buy}},
                      books);

    EXPECT_EQ(first_calls, 1);
    EXPECT_EQ(second_calls, 1);
}

TEST(StrategyRuntime, SinkObservesTheBookAlreadyUpdatedByTheEvent) {
    BookManager books;
    StrategyRuntime runtime;

    std::optional<PriceLevelView> observed_best_bid;
    runtime.subscribe(kInstrumentA, [&](InstrumentId, const OrderBook& book) { observed_best_bid = book.best_bid(); });

    // on_event() dispatches only -- see its own doc comment on why it does
    // not itself mutate the book -- so, matching how a real call site would
    // use this (immediately after replay::apply_frame_result() already
    // applied the same event to the same BookManager), the test applies the
    // event to the book directly first.
    ASSERT_FALSE(books.book_for(kInstrumentA).add_order(/*id=*/1, /*price=*/100, /*qty=*/10, Side::Buy).has_value());

    runtime.on_event(Event{AddOrder{.sequence_number = 1,
                                     .timestamp_ns = 1,
                                     .order_id = 1,
                                     .instrument_id = kInstrumentA,
                                     .price = 100,
                                     .quantity = 10,
                                     .side = Side::Buy}},
                      books);

    ASSERT_TRUE(observed_best_bid.has_value());
    EXPECT_EQ(observed_best_bid->price, 100);
    EXPECT_EQ(observed_best_bid->aggregate_quantity, 10u);
}

TEST(StrategyRuntime, ExtractsInstrumentIdCorrectlyForEveryEventType) {
    BookManager books;
    StrategyRuntime runtime;

    std::vector<InstrumentId> seen;
    auto sink = [&](InstrumentId id, const OrderBook&) { seen.push_back(id); };
    runtime.subscribe(kInstrumentA, sink);

    runtime.on_event(Event{AddOrder{.sequence_number = 1,
                                     .timestamp_ns = 1,
                                     .order_id = 1,
                                     .instrument_id = kInstrumentA,
                                     .price = 100,
                                     .quantity = 10,
                                     .side = Side::Buy}},
                      books);
    runtime.on_event(
        Event{ModifyOrder{
            .sequence_number = 2, .timestamp_ns = 2, .order_id = 1, .instrument_id = kInstrumentA, .new_price = 100, .new_quantity = 5}},
        books);
    runtime.on_event(Event{Trade{.sequence_number = 3,
                                  .timestamp_ns = 3,
                                  .instrument_id = kInstrumentA,
                                  .price = 100,
                                  .quantity = 1,
                                  .aggressor_side = Side::Sell}},
                      books);
    runtime.on_event(Event{CancelOrder{.sequence_number = 4, .timestamp_ns = 4, .order_id = 1, .instrument_id = kInstrumentA}},
                      books);
    runtime.on_event(Event{ClearBook{.sequence_number = 5, .timestamp_ns = 5, .instrument_id = kInstrumentA}}, books);

    EXPECT_EQ(seen.size(), 5u);
    for (const auto id : seen) {
        EXPECT_EQ(id, kInstrumentA);
    }
}
