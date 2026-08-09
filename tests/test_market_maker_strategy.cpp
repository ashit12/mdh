#include <gtest/gtest.h>

#include <tuple>
#include <vector>

#include "book/order_book.hpp"
#include "trader/strategies/market_maker_strategy.hpp"

// Composition-level unit tests for MarketMakerStrategy (Milestone 10) -- a
// fake Sender stands in for the wire (same pattern
// test_trader_risk_gated_oms.cpp uses), a book::OrderBook is built directly
// (no BookManager/StrategyRuntime needed to exercise the strategy's own
// on_book_update() logic), and handle_message() simulates gateway
// responses to drive the order lifecycle forward.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::protocol::order_entry;
using namespace mdh::trader::oms;
using namespace mdh::trader::risk;
using namespace mdh::trader::strategies;

namespace {

constexpr AccountId kAccount = 1;
constexpr InstrumentId kInstrument = 7;

class FakeSender {
public:
    [[nodiscard]] bool operator()(const Message& message) {
        sent.push_back(message);
        return true;
    }
    std::vector<Message> sent;
};

[[nodiscard]] MarketMakerConfig make_config() {
    return MarketMakerConfig{
        .instrument_id = kInstrument, .quote_size = 10, .half_spread = 2, .max_position = 20, .requote_threshold = 3};
}

[[nodiscard]] book::OrderBook make_book(Price bid_price, Price ask_price) {
    book::OrderBook book;
    static OrderId next_id = 1;
    std::ignore = book.add_order(next_id++, bid_price, /*qty=*/50, Side::Buy);
    std::ignore = book.add_order(next_id++, ask_price, /*qty=*/50, Side::Sell);
    return book;
}

[[nodiscard]] Message accepted_for(ClientOrderId id, Side side, Price price, Quantity quantity) {
    return Message{Accepted{.account_id = kAccount,
                             .client_order_id = id,
                             .exchange_order_id = 1,
                             .instrument_id = kInstrument,
                             .side = side,
                             .price = price,
                             .quantity = quantity,
                             .order_type = OrderType::Limit,
                             .time_in_force = TimeInForce::GTC}};
}

[[nodiscard]] Message trade_report_for(ClientOrderId id, Price price, Quantity quantity, Quantity remaining) {
    return Message{TradeReport{.account_id = kAccount,
                                .client_order_id = id,
                                .exchange_order_id = 1,
                                .instrument_id = kInstrument,
                                .price = price,
                                .quantity = quantity,
                                .remaining_quantity = remaining}};
}

} // namespace

TEST(MarketMakerStrategy, NoQuotesWithoutATwoSidedMarket) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    MarketMakerStrategy mm(trading, make_config());

    book::OrderBook empty_book;
    mm.on_book_update(kInstrument, empty_book);

    EXPECT_TRUE(sender.sent.empty());
    EXPECT_FALSE(mm.bid_order_id().has_value());
    EXPECT_FALSE(mm.ask_order_id().has_value());
}

TEST(MarketMakerStrategy, IgnoresUpdatesForADifferentInstrument) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    trading.deposit_cash(1'000'000);
    trading.deposit_position(kInstrument, 15);
    MarketMakerStrategy mm(trading, make_config());

    auto book = make_book(98, 102);
    mm.on_book_update(kInstrument + 1, book);

    EXPECT_TRUE(sender.sent.empty());
}

TEST(MarketMakerStrategy, FirstBookUpdateSubmitsBidAndAsk) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    trading.deposit_cash(1'000'000);
    trading.deposit_position(kInstrument, 15); // enough to also quote the ask (>= quote_size, < max_position)
    MarketMakerStrategy mm(trading, make_config());

    auto book = make_book(98, 102); // mid = 100
    mm.on_book_update(kInstrument, book);

    ASSERT_TRUE(mm.bid_order_id().has_value());
    ASSERT_TRUE(mm.ask_order_id().has_value());
    ASSERT_EQ(sender.sent.size(), 2u);

    const auto* bid_new = std::get_if<NewOrder>(&sender.sent[0]);
    ASSERT_NE(bid_new, nullptr);
    EXPECT_EQ(bid_new->side, Side::Buy);
    EXPECT_EQ(bid_new->price, 98);
    EXPECT_EQ(bid_new->quantity, 10u);

    const auto* ask_new = std::get_if<NewOrder>(&sender.sent[1]);
    ASSERT_NE(ask_new, nullptr);
    EXPECT_EQ(ask_new->side, Side::Sell);
    EXPECT_EQ(ask_new->price, 102);
}

TEST(MarketMakerStrategy, DoesNotResendWhileFirstQuotesAreStillPendingNew) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    trading.deposit_cash(1'000'000);
    trading.deposit_position(kInstrument, 15);
    MarketMakerStrategy mm(trading, make_config());

    auto book = make_book(98, 102);
    mm.on_book_update(kInstrument, book);
    ASSERT_EQ(sender.sent.size(), 2u);

    mm.on_book_update(kInstrument, book); // same book again -- both quotes still PendingNew

    EXPECT_EQ(sender.sent.size(), 2u);
}

TEST(MarketMakerStrategy, AskWithNoInventoryIsLocallyRejectedAndRetriesWithoutDuplicating) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    trading.deposit_cash(1'000'000); // no deposit_position -- nothing to sell yet
    MarketMakerStrategy mm(trading, make_config());

    auto book = make_book(98, 102);
    mm.on_book_update(kInstrument, book);

    EXPECT_TRUE(mm.bid_order_id().has_value());
    EXPECT_FALSE(mm.ask_order_id().has_value()); // locally risk-rejected: InsufficientPosition
    ASSERT_EQ(sender.sent.size(), 1u);            // only the bid actually reached the wire

    mm.on_book_update(kInstrument, book); // retried -- still no inventory, still no crash/duplication

    EXPECT_FALSE(mm.ask_order_id().has_value());
    EXPECT_EQ(sender.sent.size(), 1u);
}

TEST(MarketMakerStrategy, ReplacesTheBidOnceTheDesiredPriceMovesPastTheThreshold) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    trading.deposit_cash(1'000'000);
    trading.deposit_position(kInstrument, 15);
    MarketMakerStrategy mm(trading, make_config());

    auto book = make_book(98, 102); // mid = 100, desired bid = 98
    mm.on_book_update(kInstrument, book);
    const auto bid_id = *mm.bid_order_id();
    trading.handle_message(accepted_for(bid_id, Side::Buy, 98, 10));

    auto moved_book = make_book(104, 108); // mid = 106, desired bid = 104 (moved 6 >= threshold 3)
    mm.on_book_update(kInstrument, moved_book);

    ASSERT_EQ(sender.sent.size(), 3u); // 2 initial NewOrders + 1 Replace
    const auto* replace = std::get_if<ReplaceOrder>(&sender.sent[2]);
    ASSERT_NE(replace, nullptr);
    EXPECT_EQ(replace->original_client_order_id, bid_id);
    EXPECT_EQ(replace->new_price, 104);
    ASSERT_TRUE(mm.bid_order_id().has_value());
    EXPECT_NE(*mm.bid_order_id(), bid_id); // now tracking the replacement's pre-registered id
}

TEST(MarketMakerStrategy, DoesNotRequoteWhenTheDesiredPriceMovesLessThanTheThreshold) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    trading.deposit_cash(1'000'000);
    trading.deposit_position(kInstrument, 15);
    MarketMakerStrategy mm(trading, make_config());

    auto book = make_book(98, 102); // mid = 100, desired bid = 98
    mm.on_book_update(kInstrument, book);
    const auto bid_id = *mm.bid_order_id();
    trading.handle_message(accepted_for(bid_id, Side::Buy, 98, 10));

    auto slightly_moved_book = make_book(99, 103); // mid = 101, desired bid = 99 (moved 1 < threshold 3)
    mm.on_book_update(kInstrument, slightly_moved_book);

    EXPECT_EQ(sender.sent.size(), 2u); // no replace sent
    EXPECT_EQ(*mm.bid_order_id(), bid_id);
}

TEST(MarketMakerStrategy, ReplacesTheFarSideFirstToAvoidMomentarilyCrossingItsOwnBook) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    trading.deposit_cash(1'000'000);
    trading.deposit_position(kInstrument, 1'000);
    MarketMakerConfig config{
        .instrument_id = kInstrument, .quote_size = 10, .half_spread = 2, .max_position = 10'000, .requote_threshold = 2};
    MarketMakerStrategy mm(trading, config);

    auto book = make_book(98, 102); // mid = 100, bid = 98, ask = 102
    mm.on_book_update(kInstrument, book);
    const auto bid_id = *mm.bid_order_id();
    const auto ask_id = *mm.ask_order_id();
    trading.handle_message(accepted_for(bid_id, Side::Buy, 98, 10));
    trading.handle_message(accepted_for(ask_id, Side::Sell, 102, 10));

    // Market jumps up sharply: new mid = 110, desired bid = 108, desired ask
    // = 112. 108 >= the still-old ask price (102) -- replacing the bid
    // first (before the ask has moved) would have this strategy's own new
    // bid immediately cross its own stale ask. The ask replace must be sent
    // first.
    auto jumped_book = make_book(108, 112);
    mm.on_book_update(kInstrument, jumped_book);

    ASSERT_EQ(sender.sent.size(), 4u); // 2 initial NewOrders + 2 Replaces
    const auto* first_replace = std::get_if<ReplaceOrder>(&sender.sent[2]);
    ASSERT_NE(first_replace, nullptr);
    EXPECT_EQ(first_replace->original_client_order_id, ask_id); // the ask (far side) replaced first
    const auto* second_replace = std::get_if<ReplaceOrder>(&sender.sent[3]);
    ASSERT_NE(second_replace, nullptr);
    EXPECT_EQ(second_replace->original_client_order_id, bid_id);
}

TEST(MarketMakerStrategy, NeverSubmitsABidWhenAlreadyAtThePositionCap) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    trading.deposit_cash(1'000'000);
    auto config = make_config(); // max_position = 20
    trading.deposit_position(kInstrument, config.max_position); // already at the cap

    MarketMakerStrategy mm(trading, config);
    auto book = make_book(98, 102);
    mm.on_book_update(kInstrument, book);

    EXPECT_FALSE(mm.bid_order_id().has_value()); // never even submitted
    ASSERT_TRUE(mm.ask_order_id().has_value());   // ask side unaffected -- plenty of inventory to sell
    ASSERT_EQ(sender.sent.size(), 1u);
    EXPECT_NE(std::get_if<NewOrder>(&sender.sent[0]), nullptr);
}

TEST(MarketMakerStrategy, WithdrawsAnAlreadyRestingBidOnceAPartialFillReachesThePositionCap) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    trading.deposit_cash(1'000'000);
    MarketMakerConfig config{
        .instrument_id = kInstrument, .quote_size = 20, .half_spread = 2, .max_position = 10, .requote_threshold = 3};
    MarketMakerStrategy mm(trading, config);

    auto book = make_book(98, 102);
    mm.on_book_update(kInstrument, book);
    const auto bid_id = *mm.bid_order_id();
    trading.handle_message(accepted_for(bid_id, Side::Buy, 98, 20));
    trading.handle_message(trade_report_for(bid_id, 98, /*quantity=*/10, /*remaining=*/10)); // partial fill: position -> 10 == cap

    ASSERT_EQ(trading.position(kInstrument), 10u);
    ASSERT_EQ(trading.order(bid_id)->state, ClientOrderState::PartiallyFilled);

    mm.on_book_update(kInstrument, book); // same book -- but now at the position cap

    ASSERT_EQ(sender.sent.size(), 2u); // original NewOrder + a CancelOrder for the still-resting remainder
    const auto* cancel = std::get_if<CancelOrder>(&sender.sent[1]);
    ASSERT_NE(cancel, nullptr);
    EXPECT_EQ(cancel->client_order_id, bid_id);
    EXPECT_EQ(mm.bid_order_id(), bid_id); // still tracked until a Cancelled response confirms it
}
