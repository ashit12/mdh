#include <gtest/gtest.h>

#include <cstddef>
#include <functional>
#include <tuple>
#include <vector>

#include "book/order_book.hpp"
#include "trader/strategies/momentum_strategy.hpp"

// Unit tests for MomentumStrategy, in the same shape as
// test_market_maker_strategy.cpp: a fake Sender for the wire, a
// book::OrderBook built directly (no BookManager or StrategyRuntime needed
// to exercise on_book_update() itself), and every decision asserted from the
// messages that actually reached the wire.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::protocol::order_entry;
using namespace mdh::trader::positions;
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

// Funded generously on both sides: these tests are about the momentum
// signal, and a local risk rejection would silently turn "did not decide to
// trade" and "could not afford to" into the same observation.
void fund(TraderRiskGatedOms& trading) {
    trading.deposit_cash(1'000'000'000);
    trading.deposit_position(kInstrument, 1'000'000);
}

[[nodiscard]] MomentumStrategyConfig make_config() {
    return MomentumStrategyConfig{.instrument_id = kInstrument,
                                   .lookback = 3,
                                   .entry_threshold = 10,
                                   .trade_size = 50,
                                   .max_position = 100,
                                   .cross_slack = 1,
                                   .cooldown_updates = 0};
}

[[nodiscard]] book::OrderBook make_book(Price bid_price, Price ask_price) {
    book::OrderBook book;
    std::ignore = book.add_order(1, bid_price, /*qty=*/500, Side::Buy);
    std::ignore = book.add_order(2, ask_price, /*qty=*/500, Side::Sell);
    return book;
}

// Feeds `count` updates at one midpoint, which is how the lookback window
// gets filled with a flat baseline before a move is applied.
void feed_flat(MomentumStrategy& strategy, Price bid_price, Price ask_price, int count) {
    for (int i = 0; i < count; ++i) {
        auto book = make_book(bid_price, ask_price);
        strategy.on_book_update(kInstrument, book);
    }
}

} // namespace

TEST(MomentumStrategy, SendsNothingUntilTheLookbackWindowHasFilled) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    fund(trading);
    MomentumStrategy strategy(trading, [] { return NetPosition{0}; }, make_config());

    // lookback 3 needs four samples before back() - front() spans it, so
    // even a large move inside the first three cannot be acted on.
    auto rising = make_book(200, 202);
    feed_flat(strategy, 99, 101, 2);
    strategy.on_book_update(kInstrument, rising);

    EXPECT_TRUE(sender.sent.empty());
    EXPECT_FALSE(strategy.signal().has_value());
}

TEST(MomentumStrategy, IgnoresAOneSidedBookBecauseItHasNoMidpoint) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    fund(trading);
    MomentumStrategy strategy(trading, [] { return NetPosition{0}; }, make_config());

    book::OrderBook bids_only;
    std::ignore = bids_only.add_order(1, 100, 500, Side::Buy);
    for (int i = 0; i < 10; ++i) {
        strategy.on_book_update(kInstrument, bids_only);
    }

    EXPECT_TRUE(sender.sent.empty());
    EXPECT_EQ(strategy.updates_seen(), 0u); // not even sampled
}

TEST(MomentumStrategy, IgnoresUpdatesForADifferentInstrument) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    fund(trading);
    MomentumStrategy strategy(trading, [] { return NetPosition{0}; }, make_config());

    auto book = make_book(99, 101);
    for (int i = 0; i < 10; ++i) {
        strategy.on_book_update(kInstrument + 1, book);
    }

    EXPECT_TRUE(sender.sent.empty());
    EXPECT_EQ(strategy.updates_seen(), 0u);
}

TEST(MomentumStrategy, BuysWhenTheMidpointHasRisenPastTheThreshold) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    fund(trading);
    MomentumStrategy strategy(trading, [] { return NetPosition{0}; }, make_config());

    feed_flat(strategy, 99, 101, 3); // baseline mid 100
    auto risen = make_book(109, 111); // mid 110, so the signal is +10
    strategy.on_book_update(kInstrument, risen);

    ASSERT_EQ(strategy.signal().value_or(0), 10);
    ASSERT_EQ(sender.sent.size(), 1u);
    const auto* order = std::get_if<NewOrder>(&sender.sent[0]);
    ASSERT_NE(order, nullptr);
    EXPECT_EQ(order->side, Side::Buy);
    EXPECT_EQ(order->quantity, 50u);
    EXPECT_EQ(order->price, 112); // best ask 111 plus one tick of slack
    // IOC, not GTC: a signal is only worth acting on against the market that
    // produced it, and this strategy has no logic to manage a resting order.
    EXPECT_EQ(order->time_in_force, TimeInForce::IOC);
}

TEST(MomentumStrategy, SellsWhenTheMidpointHasFallenPastTheThreshold) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    fund(trading);
    MomentumStrategy strategy(trading, [] { return NetPosition{0}; }, make_config());

    feed_flat(strategy, 99, 101, 3);
    auto fallen = make_book(89, 91); // mid 90, signal -10
    strategy.on_book_update(kInstrument, fallen);

    ASSERT_EQ(sender.sent.size(), 1u);
    const auto* order = std::get_if<NewOrder>(&sender.sent[0]);
    ASSERT_NE(order, nullptr);
    EXPECT_EQ(order->side, Side::Sell);
    EXPECT_EQ(order->price, 88); // best bid 89 less one tick of slack
    EXPECT_EQ(order->time_in_force, TimeInForce::IOC);
}

TEST(MomentumStrategy, SendsNothingWhenTheMoveIsBelowTheThreshold) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    fund(trading);
    MomentumStrategy strategy(trading, [] { return NetPosition{0}; }, make_config());

    feed_flat(strategy, 99, 101, 3);
    auto barely_risen = make_book(104, 106); // mid 105, signal +5 < 10
    strategy.on_book_update(kInstrument, barely_risen);

    EXPECT_EQ(strategy.signal().value_or(0), 5);
    EXPECT_TRUE(sender.sent.empty());
}

TEST(MomentumStrategy, ShrinksTheOrderToFitTheRemainingRoomBeforeTheCap) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    fund(trading);
    const auto config = make_config(); // max_position 100, trade_size 50
    MomentumStrategy strategy(trading, [] { return NetPosition{80}; }, config);

    feed_flat(strategy, 99, 101, 3);
    auto risen = make_book(109, 111);
    strategy.on_book_update(kInstrument, risen);

    ASSERT_EQ(sender.sent.size(), 1u);
    const auto* order = std::get_if<NewOrder>(&sender.sent[0]);
    ASSERT_NE(order, nullptr);
    EXPECT_EQ(order->quantity, 20u); // 100 - 80, not the full 50
}

TEST(MomentumStrategy, SendsNothingOnTheSideItIsAlreadyCappedOn) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    fund(trading);
    const auto config = make_config();
    MomentumStrategy strategy(trading, [&] { return config.max_position; }, config);

    feed_flat(strategy, 99, 101, 3);
    auto risen = make_book(109, 111);
    strategy.on_book_update(kInstrument, risen);

    EXPECT_TRUE(sender.sent.empty()); // fully long already
}

TEST(MomentumStrategy, StillSellsWhenCappedLongBecauseTheCapIsPerSide) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    fund(trading);
    const auto config = make_config();
    MomentumStrategy strategy(trading, [&] { return config.max_position; }, config);

    feed_flat(strategy, 99, 101, 3);
    auto fallen = make_book(89, 91);
    strategy.on_book_update(kInstrument, fallen);

    ASSERT_EQ(sender.sent.size(), 1u);
    const auto* order = std::get_if<NewOrder>(&sender.sent[0]);
    ASSERT_NE(order, nullptr);
    EXPECT_EQ(order->side, Side::Sell);
    EXPECT_EQ(order->quantity, 50u); // room to sell is max_position + position
}

TEST(MomentumStrategy, TheCooldownSuppressesBackToBackOrdersDuringOneSustainedMove) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    fund(trading);
    auto config = make_config();
    config.cooldown_updates = 2;
    MomentumStrategy strategy(trading, [] { return NetPosition{0}; }, config);

    feed_flat(strategy, 99, 101, 3);
    // A move that stays above the threshold for as long as it is fed, which
    // without a cooldown would fire one order per update for its duration.
    for (Price step = 0; step < 6; ++step) {
        auto book = make_book(109 + step * 10, 111 + step * 10);
        strategy.on_book_update(kInstrument, book);
    }

    // Six qualifying updates, each order followed by two suppressed ones:
    // the 3rd and 6th are the two that get through, rather than one order
    // per update for the whole duration of the move.
    EXPECT_EQ(sender.sent.size(), 2u);
}

TEST(MomentumStrategy, TheWindowSlidesSoAFinishedMoveStopsBeingASignal) {
    FakeSender sender;
    TraderRiskGatedOms trading(kAccount, std::ref(sender));
    fund(trading);
    MomentumStrategy strategy(trading, [] { return NetPosition{0}; }, make_config());

    feed_flat(strategy, 99, 101, 3);
    auto risen = make_book(109, 111);
    strategy.on_book_update(kInstrument, risen);
    ASSERT_FALSE(sender.sent.empty());

    // Hold the new level until it has displaced the old one throughout the
    // window. The signal decays to zero as the window slides, and from then
    // on the strategy is flat: momentum, not a standing bias, so a move that
    // has finished stops being a reason to keep buying.
    feed_flat(strategy, 109, 111, 3);
    ASSERT_EQ(strategy.signal().value_or(-1), 0);

    const std::size_t sent_once_decayed = sender.sent.size();
    feed_flat(strategy, 109, 111, 20);
    EXPECT_EQ(sender.sent.size(), sent_once_decayed);
}
