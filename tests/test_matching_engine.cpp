#include <gtest/gtest.h>

#include <variant>
#include <vector>

#include "exchange/matching/matching_engine.hpp"

namespace mdh::exchange {
namespace {

constexpr InstrumentId kInstrument = 1;

NewOrderCommand new_order(CommandSequence seq, AccountId account, ClientOrderId client_id, Side side, Price price,
                           Quantity qty, TimeInForce tif = TimeInForce::GTC) {
    return NewOrderCommand{
        .command_sequence = seq,
        .account_id = account,
        .client_order_id = client_id,
        .instrument_id = kInstrument,
        .side = side,
        .price = price,
        .quantity = qty,
        .order_type = OrderType::Limit,
        .time_in_force = tif,
    };
}

CancelOrderCommand cancel_order(CommandSequence seq, AccountId account, ClientOrderId client_id) {
    return CancelOrderCommand{
        .command_sequence = seq, .account_id = account, .client_order_id = client_id, .instrument_id = kInstrument};
}

ReplaceOrderCommand replace_order(CommandSequence seq, AccountId account, ClientOrderId original_id,
                                   ClientOrderId new_id, Price new_price, Quantity new_qty) {
    return ReplaceOrderCommand{
        .command_sequence = seq,
        .account_id = account,
        .original_client_order_id = original_id,
        .new_client_order_id = new_id,
        .instrument_id = kInstrument,
        .new_price = new_price,
        .new_quantity = new_qty,
    };
}

// Collects every event emitted across any number of process() calls, in
// order, so tests can assert on the exact event sequence a scenario
// produces -- not just aggregate counts.
class CollectingSink {
public:
    EventSink sink() {
        return [this](const ExchangeEvent& ev) { events.push_back(ev); };
    }

    template <class T>
    [[nodiscard]] const T& at(std::size_t index) const {
        return std::get<T>(events.at(index));
    }

    std::vector<ExchangeEvent> events;
};

template <class T>
bool holds(const ExchangeEvent& ev) {
    return std::holds_alternative<T>(ev);
}

TEST(MatchingEngine, NonCrossingOrderRestsOnEmptyBook) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(new_order(1, 100, 1, Side::Buy, 100, 10), out.sink());

    ASSERT_EQ(out.events.size(), 2u);
    EXPECT_TRUE(holds<OrderAccepted>(out.events[0]));
    ASSERT_TRUE(holds<BookOrderAdded>(out.events[1]));
    const auto& added = out.at<BookOrderAdded>(1);
    EXPECT_EQ(added.price, 100);
    EXPECT_EQ(added.quantity, 10u);
    EXPECT_EQ(added.side, Side::Buy);
}

TEST(MatchingEngine, CrossingBuyTradesAgainstRestingSell) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 10), out.sink());
    out.events.clear();

    engine.process(new_order(2, 200, 2, Side::Buy, 100, 10), out.sink());

    ASSERT_EQ(out.events.size(), 3u);
    EXPECT_TRUE(holds<OrderAccepted>(out.events[0]));
    ASSERT_TRUE(holds<TradeExecuted>(out.events[1]));
    const auto& trade = out.at<TradeExecuted>(1);
    EXPECT_EQ(trade.price, 100);
    EXPECT_EQ(trade.quantity, 10u);
    EXPECT_EQ(trade.aggressor_side, Side::Buy);
    EXPECT_EQ(trade.buyer.account_id, 200u);
    EXPECT_EQ(trade.seller.account_id, 100u);
    ASSERT_TRUE(holds<BookOrderRemoved>(out.events[2])); // resting sell fully consumed
}

TEST(MatchingEngine, CrossingSellTradesAgainstRestingBuy) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(new_order(1, 100, 1, Side::Buy, 100, 10), out.sink());
    out.events.clear();

    engine.process(new_order(2, 200, 2, Side::Sell, 100, 10), out.sink());

    ASSERT_EQ(out.events.size(), 3u);
    ASSERT_TRUE(holds<TradeExecuted>(out.events[1]));
    const auto& trade = out.at<TradeExecuted>(1);
    EXPECT_EQ(trade.aggressor_side, Side::Sell);
    EXPECT_EQ(trade.buyer.account_id, 100u);
    EXPECT_EQ(trade.seller.account_id, 200u);
}

TEST(MatchingEngine, IncomingBuyMatchesBestAskFirst) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(new_order(1, 100, 1, Side::Sell, 110, 5), out.sink());
    engine.process(new_order(2, 100, 2, Side::Sell, 105, 5), out.sink());
    out.events.clear();

    engine.process(new_order(3, 200, 3, Side::Buy, 110, 5), out.sink());

    ASSERT_TRUE(holds<TradeExecuted>(out.events[1]));
    EXPECT_EQ(out.at<TradeExecuted>(1).price, 105); // best (lowest) ask, not the first one added
}

TEST(MatchingEngine, FifoAtOnePriceGivesEarliestOrderPriority) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 5), out.sink());
    engine.process(new_order(2, 100, 2, Side::Sell, 100, 5), out.sink());
    out.events.clear();

    engine.process(new_order(3, 200, 3, Side::Buy, 100, 5), out.sink());

    ASSERT_TRUE(holds<TradeExecuted>(out.events[1]));
    EXPECT_EQ(out.at<TradeExecuted>(1).seller.client_order_id, 1u); // first-added sell wins priority
    ASSERT_TRUE(holds<BookOrderRemoved>(out.events[2]));
    EXPECT_EQ(out.at<BookOrderRemoved>(2).exchange_order_id, 1u);
}

TEST(MatchingEngine, PartialFillOfRestingOrderLeavesItOnBookReduced) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 10), out.sink());
    out.events.clear();

    engine.process(new_order(2, 200, 2, Side::Buy, 100, 4), out.sink());

    // Accepted, Trade, BookOrderReduced(resting sell, 10 -> 6) -- no
    // BookOrderAdded since the incoming order was fully filled, not rested.
    ASSERT_EQ(out.events.size(), 3u);
    ASSERT_TRUE(holds<TradeExecuted>(out.events[1]));
    EXPECT_EQ(out.at<TradeExecuted>(1).quantity, 4u);
    ASSERT_TRUE(holds<BookOrderReduced>(out.events[2]));
    EXPECT_EQ(out.at<BookOrderReduced>(2).new_remaining_quantity, 6u);
}

TEST(MatchingEngine, PartialFillOfIncomingOrderRestsRemainderAsGtc) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 4), out.sink());
    out.events.clear();

    engine.process(new_order(2, 200, 2, Side::Buy, 100, 10), out.sink());

    ASSERT_EQ(out.events.size(), 4u); // Accepted, Trade, BookOrderRemoved(sell), BookOrderAdded(buy remainder)
    ASSERT_TRUE(holds<BookOrderRemoved>(out.events[2]));
    ASSERT_TRUE(holds<BookOrderAdded>(out.events[3]));
    EXPECT_EQ(out.at<BookOrderAdded>(3).quantity, 6u);
}

TEST(MatchingEngine, OneIncomingOrderMatchesMultipleRestingOrdersAtSamePrice) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 3), out.sink());
    engine.process(new_order(2, 100, 2, Side::Sell, 100, 4), out.sink());
    engine.process(new_order(3, 100, 3, Side::Sell, 100, 5), out.sink());
    out.events.clear();

    engine.process(new_order(4, 200, 4, Side::Buy, 100, 9), out.sink());

    // Accepted, Trade1, Removed1, Trade2, Removed2, Trade3, Reduced3
    ASSERT_EQ(out.events.size(), 7u);
    EXPECT_EQ(out.at<TradeExecuted>(1).quantity, 3u);
    EXPECT_EQ(out.at<TradeExecuted>(3).quantity, 4u);
    EXPECT_EQ(out.at<TradeExecuted>(5).quantity, 2u);
    ASSERT_TRUE(holds<BookOrderReduced>(out.events[6]));
    EXPECT_EQ(out.at<BookOrderReduced>(6).new_remaining_quantity, 3u);
}

TEST(MatchingEngine, MultiLevelMatchingWalksThroughSeveralPriceLevels) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 5), out.sink());
    engine.process(new_order(2, 100, 2, Side::Sell, 101, 5), out.sink());
    engine.process(new_order(3, 100, 3, Side::Sell, 102, 5), out.sink());
    out.events.clear();

    engine.process(new_order(4, 200, 4, Side::Buy, 102, 12), out.sink());

    // Fills 5@100, 5@101, 2@102 -- three trades, remainder 0.
    ASSERT_TRUE(holds<TradeExecuted>(out.events[1]));
    EXPECT_EQ(out.at<TradeExecuted>(1).price, 100);
    ASSERT_TRUE(holds<TradeExecuted>(out.events[3]));
    EXPECT_EQ(out.at<TradeExecuted>(3).price, 101);
    ASSERT_TRUE(holds<TradeExecuted>(out.events[5]));
    EXPECT_EQ(out.at<TradeExecuted>(5).price, 102);
    EXPECT_EQ(out.at<TradeExecuted>(5).quantity, 2u);
    // Incoming fully filled: no trailing BookOrderAdded.
    EXPECT_EQ(out.events.size(), 7u);
}

TEST(MatchingEngine, IocRemainderIsDiscardedNotRested) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 4), out.sink());
    out.events.clear();

    engine.process(new_order(2, 200, 2, Side::Buy, 100, 10, TimeInForce::IOC), out.sink());

    ASSERT_EQ(out.events.size(), 3u); // Accepted, Trade, BookOrderRemoved(sell) -- no BookOrderAdded
    for (const auto& ev : out.events) {
        EXPECT_FALSE(holds<BookOrderAdded>(ev));
    }
    // Cancelling the (never-live) IOC order confirms it never rested.
    CollectingSink cancel_out;
    engine.process(cancel_order(3, 200, 2), cancel_out.sink());
    ASSERT_EQ(cancel_out.events.size(), 1u);
    ASSERT_TRUE(holds<OrderRejected>(cancel_out.events[0]));
    EXPECT_EQ(cancel_out.at<OrderRejected>(0).reason, RejectReason::UnknownOrderId);
}

TEST(MatchingEngine, FokSucceedsWhenFullQuantityIsImmediatelyAvailable) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 6), out.sink());
    engine.process(new_order(2, 100, 2, Side::Sell, 101, 6), out.sink());
    out.events.clear();

    engine.process(new_order(3, 200, 3, Side::Buy, 101, 10, TimeInForce::FOK), out.sink());

    ASSERT_TRUE(holds<OrderAccepted>(out.events[0]));
    ASSERT_TRUE(holds<TradeExecuted>(out.events[1]));
    EXPECT_EQ(out.at<TradeExecuted>(1).quantity, 6u);
    ASSERT_TRUE(holds<TradeExecuted>(out.events[3]));
    EXPECT_EQ(out.at<TradeExecuted>(3).quantity, 4u);
}

TEST(MatchingEngine, FokRejectionLeavesBookCompletelyUntouched) {
    MatchingEngine engine{kInstrument};
    CollectingSink setup;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 5), setup.sink());

    CollectingSink out;
    engine.process(new_order(2, 200, 2, Side::Buy, 100, 10, TimeInForce::FOK), out.sink());

    ASSERT_EQ(out.events.size(), 1u);
    ASSERT_TRUE(holds<OrderRejected>(out.events[0]));
    EXPECT_EQ(out.at<OrderRejected>(0).reason, RejectReason::InsufficientLiquidity);

    // The resting sell order must be completely unaffected.
    CollectingSink verify;
    engine.process(new_order(3, 300, 3, Side::Buy, 100, 5), verify.sink());
    ASSERT_TRUE(holds<TradeExecuted>(verify.events[1]));
    EXPECT_EQ(verify.at<TradeExecuted>(1).quantity, 5u);
    EXPECT_EQ(verify.at<TradeExecuted>(1).seller.exchange_order_id, 1u); // original resting order, untouched
}

TEST(MatchingEngine, DuplicateLiveClientOrderIdIsRejected) {
    MatchingEngine engine{kInstrument};
    CollectingSink first;
    engine.process(new_order(1, 100, 1, Side::Buy, 100, 5), first.sink());

    CollectingSink second;
    engine.process(new_order(2, 100, 1, Side::Buy, 101, 5), second.sink());

    ASSERT_EQ(second.events.size(), 1u);
    ASSERT_TRUE(holds<OrderRejected>(second.events[0]));
    EXPECT_EQ(second.at<OrderRejected>(0).reason, RejectReason::DuplicateOrderId);
}

TEST(MatchingEngine, SameClientOrderIdIsFineAcrossDifferentAccounts) {
    MatchingEngine engine{kInstrument};
    CollectingSink a;
    engine.process(new_order(1, 100, 1, Side::Buy, 100, 5), a.sink());
    CollectingSink b;
    engine.process(new_order(2, 200, 1, Side::Buy, 101, 5), b.sink());

    ASSERT_TRUE(holds<OrderAccepted>(a.events[0]));
    ASSERT_TRUE(holds<OrderAccepted>(b.events[0]));
}

TEST(MatchingEngine, ReusingClientOrderIdAfterCancelIsAllowed) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(new_order(1, 100, 1, Side::Buy, 100, 5), out.sink());
    engine.process(cancel_order(2, 100, 1), out.sink());
    out.events.clear();

    engine.process(new_order(3, 100, 1, Side::Buy, 100, 5), out.sink());
    ASSERT_TRUE(holds<OrderAccepted>(out.events[0]));
}

TEST(MatchingEngine, UnknownCancelIsRejected) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(cancel_order(1, 100, 999), out.sink());

    ASSERT_EQ(out.events.size(), 1u);
    ASSERT_TRUE(holds<OrderRejected>(out.events[0]));
    EXPECT_EQ(out.at<OrderRejected>(0).reason, RejectReason::UnknownOrderId);
}

TEST(MatchingEngine, CancelOfLiveOrderRemovesItFromTheBook) {
    MatchingEngine engine{kInstrument};
    CollectingSink setup;
    engine.process(new_order(1, 100, 1, Side::Buy, 100, 5), setup.sink());

    CollectingSink out;
    engine.process(cancel_order(2, 100, 1), out.sink());

    ASSERT_EQ(out.events.size(), 2u);
    ASSERT_TRUE(holds<OrderCancelled>(out.events[0]));
    ASSERT_TRUE(holds<BookOrderRemoved>(out.events[1]));

    // Confirms it's really gone: a crossing sell now has nothing to trade against.
    CollectingSink verify;
    engine.process(new_order(3, 200, 2, Side::Sell, 100, 5), verify.sink());
    ASSERT_EQ(verify.events.size(), 2u); // Accepted + BookOrderAdded, no trade
    EXPECT_TRUE(holds<BookOrderAdded>(verify.events[1]));
}

TEST(MatchingEngine, EmptyLevelIsCleanedUpAfterFullFill) {
    MatchingEngine engine{kInstrument};
    CollectingSink setup;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 5), setup.sink());
    engine.process(new_order(2, 200, 2, Side::Buy, 100, 5), setup.sink());

    // The 100 level should be completely gone -- a new sell at a worse price
    // becomes the best ask, proving no empty level lingers at 100.
    CollectingSink out;
    engine.process(new_order(3, 300, 3, Side::Sell, 100, 3), out.sink());
    ASSERT_TRUE(holds<BookOrderAdded>(out.events[1]));
    EXPECT_EQ(out.at<BookOrderAdded>(1).price, 100);
}

TEST(MatchingEngine, ReplaceQuantityDecreaseAtSamePricePreservesPriority) {
    MatchingEngine engine{kInstrument};
    CollectingSink setup;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 10), setup.sink());
    engine.process(new_order(2, 100, 2, Side::Sell, 100, 5), setup.sink());

    CollectingSink out;
    engine.process(replace_order(3, 100, 1, 10, 100, 4), out.sink());

    ASSERT_EQ(out.events.size(), 2u);
    ASSERT_TRUE(holds<OrderReplaced>(out.events[0]));
    EXPECT_EQ(out.at<OrderReplaced>(0).exchange_order_id, 1u); // same exchange_order_id: priority preserved
    ASSERT_TRUE(holds<BookOrderReduced>(out.events[1]));
    EXPECT_EQ(out.at<BookOrderReduced>(1).new_remaining_quantity, 4u);

    // Order 1 (now qty 4) must still be ahead of order 2 in FIFO despite the mutation.
    CollectingSink trade_out;
    engine.process(new_order(4, 300, 4, Side::Buy, 100, 4), trade_out.sink());
    ASSERT_TRUE(holds<TradeExecuted>(trade_out.events[1]));
    EXPECT_EQ(trade_out.at<TradeExecuted>(1).seller.exchange_order_id, 1u);
    // The trade must be attributed to the replacement's new client_order_id
    // (10), not the retired original (1) -- the resting order's identity
    // fully migrates even though it kept its place in the FIFO queue.
    EXPECT_EQ(trade_out.at<TradeExecuted>(1).seller.client_order_id, 10u);
}

TEST(MatchingEngine, ReplacePriceChangeLosesPriorityAndCanCrossImmediately) {
    MatchingEngine engine{kInstrument};
    CollectingSink setup;
    engine.process(new_order(1, 100, 1, Side::Buy, 100, 10), setup.sink());   // resting buy to replace
    engine.process(new_order(2, 200, 2, Side::Sell, 104, 10), setup.sink()); // resting ask, would not have crossed at 100

    CollectingSink out;
    // Reprice the resting buy up to 104 -- now crosses the resting ask.
    engine.process(replace_order(3, 100, 1, 11, 104, 10), out.sink());

    ASSERT_TRUE(holds<OrderReplaced>(out.events[0]));
    const ExchangeOrderId replaced_id = out.at<OrderReplaced>(0).exchange_order_id;
    EXPECT_NE(replaced_id, 1u); // priority lost: fresh exchange_order_id assigned
    ASSERT_TRUE(holds<BookOrderRemoved>(out.events[1]));
    EXPECT_EQ(out.at<BookOrderRemoved>(1).exchange_order_id, 1u); // the old resting buy is gone
    ASSERT_TRUE(holds<TradeExecuted>(out.events[2]));
    EXPECT_EQ(out.at<TradeExecuted>(2).price, 104);
    EXPECT_EQ(out.at<TradeExecuted>(2).quantity, 10u);
}

TEST(MatchingEngine, ReplaceQuantityIncreaseAlsoLosesPriority) {
    MatchingEngine engine{kInstrument};
    CollectingSink setup;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 5), setup.sink());
    engine.process(new_order(2, 100, 2, Side::Sell, 100, 5), setup.sink());

    CollectingSink out;
    engine.process(replace_order(3, 100, 1, 10, 100, 8), out.sink()); // same price, quantity increased

    ASSERT_TRUE(holds<OrderReplaced>(out.events[0]));
    EXPECT_NE(out.at<OrderReplaced>(0).exchange_order_id, 1u);

    // Order 2 now has priority over the replaced (larger) order 1.
    CollectingSink trade_out;
    engine.process(new_order(4, 300, 4, Side::Buy, 100, 5), trade_out.sink());
    ASSERT_TRUE(holds<TradeExecuted>(trade_out.events[1]));
    EXPECT_EQ(trade_out.at<TradeExecuted>(1).seller.exchange_order_id, 2u);
}

TEST(MatchingEngine, ReplaceOfUnknownOrderIsRejected) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(replace_order(1, 100, 999, 1000, 100, 5), out.sink());

    ASSERT_EQ(out.events.size(), 1u);
    ASSERT_TRUE(holds<OrderRejected>(out.events[0]));
    EXPECT_EQ(out.at<OrderRejected>(0).reason, RejectReason::UnknownOrderId);
}

TEST(MatchingEngine, ReplaceWithInvalidPriceOrQuantityIsRejected) {
    MatchingEngine engine{kInstrument};
    CollectingSink setup;
    engine.process(new_order(1, 100, 1, Side::Buy, 100, 5), setup.sink());

    CollectingSink out;
    engine.process(replace_order(2, 100, 1, 2, 0, 5), out.sink());
    ASSERT_TRUE(holds<OrderRejected>(out.events[0]));
    EXPECT_EQ(out.at<OrderRejected>(0).reason, RejectReason::InvalidReplacement);

    CollectingSink out2;
    engine.process(replace_order(3, 100, 1, 3, 100, 0), out2.sink());
    ASSERT_TRUE(holds<OrderRejected>(out2.events[0]));
    EXPECT_EQ(out2.at<OrderRejected>(0).reason, RejectReason::InvalidReplacement);

    // Original order must still be live and untouched after both rejections.
    CollectingSink verify;
    engine.process(new_order(4, 200, 4, Side::Sell, 100, 5), verify.sink());
    ASSERT_TRUE(holds<TradeExecuted>(verify.events[1]));
    EXPECT_EQ(verify.at<TradeExecuted>(1).buyer.exchange_order_id, 1u);
}

TEST(MatchingEngine, SameAccountOrdersCrossNormally) {
    MatchingEngine engine{kInstrument};
    CollectingSink setup;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 5), setup.sink());

    CollectingSink out;
    engine.process(new_order(2, 100, 2, Side::Buy, 100, 5), out.sink()); // same account (100) on both sides

    ASSERT_TRUE(holds<TradeExecuted>(out.events[1]));
    const auto& trade = out.at<TradeExecuted>(1);
    EXPECT_EQ(trade.buyer.account_id, 100u);
    EXPECT_EQ(trade.seller.account_id, 100u); // no self-trade prevention in this milestone
}

TEST(MatchingEngine, EventSequenceNumbersAreStrictlyIncreasingAcrossCommands) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 5), out.sink());
    engine.process(new_order(2, 200, 2, Side::Buy, 100, 5), out.sink());
    engine.process(new_order(3, 300, 3, Side::Buy, 99, 5), out.sink());

    EventSequence previous = 0;
    for (const auto& ev : out.events) {
        const EventSequence current = std::visit([](const auto& e) { return e.event_sequence; }, ev);
        EXPECT_GT(current, previous);
        previous = current;
    }
}

TEST(MatchingEngine, DeterministicEventOrderForMultiFillScenario) {
    MatchingEngine engine{kInstrument};
    CollectingSink setup;
    engine.process(new_order(1, 100, 1, Side::Sell, 100, 3), setup.sink());
    engine.process(new_order(2, 100, 2, Side::Sell, 101, 3), setup.sink());

    CollectingSink out;
    engine.process(new_order(3, 200, 3, Side::Buy, 101, 10, TimeInForce::GTC), out.sink());

    // Expected exact sequence: Accepted, Trade(@100), Removed(order1),
    // Trade(@101), Removed(order2), BookOrderAdded(remainder=4).
    ASSERT_EQ(out.events.size(), 6u);
    EXPECT_TRUE(holds<OrderAccepted>(out.events[0]));
    EXPECT_TRUE(holds<TradeExecuted>(out.events[1]));
    EXPECT_TRUE(holds<BookOrderRemoved>(out.events[2]));
    EXPECT_TRUE(holds<TradeExecuted>(out.events[3]));
    EXPECT_TRUE(holds<BookOrderRemoved>(out.events[4]));
    EXPECT_TRUE(holds<BookOrderAdded>(out.events[5]));
    EXPECT_EQ(out.at<BookOrderAdded>(5).quantity, 4u);
}

TEST(MatchingEngine, DeterministicFinalBookStateAfterMixedSequence) {
    MatchingEngine engine1{kInstrument};
    MatchingEngine engine2{kInstrument};
    CollectingSink out1;
    CollectingSink out2;

    auto run = [&](MatchingEngine& engine, const EventSink& sink) {
        engine.process(new_order(1, 100, 1, Side::Buy, 99, 5), sink);
        engine.process(new_order(2, 100, 2, Side::Buy, 100, 5), sink);
        engine.process(new_order(3, 200, 3, Side::Sell, 105, 5), sink);
        engine.process(cancel_order(4, 100, 1), sink);
        engine.process(new_order(5, 300, 4, Side::Sell, 100, 3), sink); // partially fills order 2
        engine.process(replace_order(6, 100, 2, 5, 100, 1), sink);      // decrease remaining 2 -> 1, same price
    };
    run(engine1, out1.sink());
    run(engine2, out2.sink());

    // Same command sequence run twice on independent engine instances
    // produces byte-identical event streams.
    ASSERT_EQ(out1.events.size(), out2.events.size());
    for (std::size_t i = 0; i < out1.events.size(); ++i) {
        EXPECT_EQ(out1.events[i].index(), out2.events[i].index());
    }

    CollectingSink verify1;
    CollectingSink verify2;
    engine1.process(new_order(7, 400, 5, Side::Sell, 99, 100), verify1.sink());
    engine2.process(new_order(7, 400, 5, Side::Sell, 99, 100), verify2.sink());
    ASSERT_EQ(verify1.events.size(), verify2.events.size());
    for (std::size_t i = 0; i < verify1.events.size(); ++i) {
        EXPECT_EQ(verify1.events[i].index(), verify2.events[i].index());
    }
    // Final resting buy (order 2, remaining 1 after the partial fill and
    // replace) is the only thing left to trade against.
    ASSERT_TRUE(holds<TradeExecuted>(verify1.events[1]));
    EXPECT_EQ(verify1.at<TradeExecuted>(1).quantity, 1u);
}

// ── The instrument registry ────────────────────────────────────────────────

TEST(MatchingEngine, OrderOnAnUnregisteredInstrumentIsRejected) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;

    auto order = new_order(1, 100, 1, Side::Buy, 100, 5);
    order.instrument_id = kInstrument + 1;
    engine.process(order, out.sink());

    ASSERT_EQ(out.events.size(), 1u);
    ASSERT_TRUE(holds<OrderRejected>(out.events[0]));
    EXPECT_EQ(out.at<OrderRejected>(0).reason, RejectReason::InvalidInstrument);
    EXPECT_TRUE(engine.snapshot().instruments.empty());
}

TEST(MatchingEngine, UnknownInstrumentIsReportedAheadOfAnyOtherProblemWithTheOrder) {
    // Both the instrument and the price are invalid. The instrument is the
    // more useful answer: the order was sent to the wrong venue, and fixing
    // the price would not help.
    MatchingEngine engine{kInstrument};
    CollectingSink out;

    auto order = new_order(1, 100, 1, Side::Buy, /*price=*/0, 5);
    order.instrument_id = 999;
    engine.process(order, out.sink());

    ASSERT_EQ(out.events.size(), 1u);
    EXPECT_EQ(out.at<OrderRejected>(0).reason, RejectReason::InvalidInstrument);
}

TEST(MatchingEngine, CancelOnAnUnregisteredInstrumentReportsTheInstrumentNotTheOrderId) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;

    auto cancel = cancel_order(1, 100, 1);
    cancel.instrument_id = 999;
    engine.process(cancel, out.sink());

    ASSERT_EQ(out.events.size(), 1u);
    EXPECT_EQ(out.at<OrderRejected>(0).reason, RejectReason::InvalidInstrument);
}

TEST(MatchingEngine, ReplaceOnAnUnregisteredInstrumentIsRejected) {
    MatchingEngine engine{kInstrument};
    CollectingSink out;

    auto replace = replace_order(1, 100, 1, 2, 100, 5);
    replace.instrument_id = 999;
    engine.process(replace, out.sink());

    ASSERT_EQ(out.events.size(), 1u);
    const auto& rejected = out.at<OrderRejected>(0);
    EXPECT_EQ(rejected.reason, RejectReason::InvalidInstrument);
    // Reported under the original id, like every other replace rejection.
    EXPECT_EQ(rejected.client_order_id, 1u);
}

TEST(MatchingEngine, SnapshotOrdersInstrumentsByIdWhateverOrderTheyWereRegisteredIn) {
    MatchingEngine engine{5, 1, 3};
    CollectingSink out;

    CommandSequence sequence = 1;
    for (const InstrumentId instrument_id : {InstrumentId{3}, InstrumentId{5}, InstrumentId{1}}) {
        auto order = new_order(sequence, 100, /*client_order_id=*/sequence, Side::Buy, 100, 5);
        order.instrument_id = instrument_id;
        engine.process(order, out.sink());
        ++sequence;
    }

    const auto snapshot = engine.snapshot();
    ASSERT_EQ(snapshot.instruments.size(), 3u);
    EXPECT_EQ(snapshot.instruments[0].instrument_id, 1u);
    EXPECT_EQ(snapshot.instruments[1].instrument_id, 3u);
    EXPECT_EQ(snapshot.instruments[2].instrument_id, 5u);
}

TEST(MatchingEngine, RegisteringAnInstrumentIsIdempotentAndBoundedById) {
    MatchingEngine engine{kInstrument};

    EXPECT_FALSE(engine.register_instrument(kInstrument)) << "already registered";
    EXPECT_TRUE(engine.register_instrument(2));
    EXPECT_EQ(engine.instrument_count(), 2u);

    // The cap is what stops a single careless id from sizing the
    // direct-mapped table to whatever number was in the command.
    EXPECT_FALSE(engine.register_instrument(MatchingEngine::kMaxInstrumentId + 1));
    EXPECT_FALSE(engine.knows_instrument(MatchingEngine::kMaxInstrumentId + 1));
    EXPECT_EQ(engine.instrument_count(), 2u);
}

// Registering after construction grows the vector of books, which moves
// every book already in it. Handles into those books are held by the
// engine's own order directory and must survive the move -- they do because
// each book's pool sits behind a unique_ptr, so its address does not change,
// and moving a pmr container with an equal allocator relinks nodes rather
// than copying them. This test is what keeps that argument honest.
TEST(MatchingEngine, RegisteringAnInstrumentKeepsRestingOrdersReachable) {
    MatchingEngine engine{kInstrument};
    CollectingSink setup;
    engine.process(new_order(1, 100, 1, Side::Buy, 100, 5), setup.sink());
    engine.process(new_order(2, 100, 2, Side::Sell, 200, 7), setup.sink());

    // Well past the one slot reserved at construction, so the books really
    // do get reallocated (repeatedly) underneath the resting orders.
    for (InstrumentId instrument_id = 2; instrument_id <= 64; ++instrument_id) {
        ASSERT_TRUE(engine.register_instrument(instrument_id));
    }

    CollectingSink out;
    engine.process(cancel_order(3, 100, 1), out.sink());
    ASSERT_EQ(out.events.size(), 2u);
    ASSERT_TRUE(holds<OrderCancelled>(out.events[0]));
    EXPECT_EQ(out.at<BookOrderRemoved>(1).price, 100);

    // The order that stayed is still matchable, and still has its original
    // quantity -- the move preserved contents, not just addressability.
    CollectingSink fill;
    engine.process(new_order(4, 200, 3, Side::Buy, 200, 7), fill.sink());
    ASSERT_EQ(fill.events.size(), 3u);
    EXPECT_TRUE(holds<TradeExecuted>(fill.events[1]));
    EXPECT_EQ(fill.at<TradeExecuted>(1).quantity, 7u);
    EXPECT_TRUE(engine.snapshot().instruments.empty());
}

TEST(MatchingEngine, AnInstrumentRegisteredAfterTheFactAcceptsOrders) {
    MatchingEngine engine{kInstrument};
    CollectingSink rejected;

    auto order = new_order(1, 100, 1, Side::Buy, 100, 5);
    order.instrument_id = 7;
    engine.process(order, rejected.sink());
    ASSERT_EQ(rejected.events.size(), 1u);
    EXPECT_EQ(rejected.at<OrderRejected>(0).reason, RejectReason::InvalidInstrument);

    ASSERT_TRUE(engine.register_instrument(7));

    CollectingSink accepted;
    auto retry = new_order(2, 100, 2, Side::Buy, 100, 5);
    retry.instrument_id = 7;
    engine.process(retry, accepted.sink());
    ASSERT_EQ(accepted.events.size(), 2u);
    EXPECT_TRUE(holds<OrderAccepted>(accepted.events[0]));
    EXPECT_TRUE(holds<BookOrderAdded>(accepted.events[1]));
}

} // namespace
} // namespace mdh::exchange
