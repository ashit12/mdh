#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <vector>

#include "trader/strategies/ladder_market_maker.hpp"
#include "trader/strategies/reference_price_walk.hpp"

// Composition-level unit tests for LadderMarketMaker, in the same shape as
// test_market_maker_strategy.cpp: a fake Sender stands in for the wire and
// handle_message() plays the gateway, so no socket, gateway or feed is
// involved and every quote price can be asserted exactly.
//
// Most tests configure the reference-price walk with step_size 0, which
// pins the reference at its initial price -- that turns the ladder into a
// pure function of the config and lets the expected prices be written out
// literally rather than by re-deriving the walk. The one test that does
// care about movement runs a second, identically-seeded walk alongside as
// its oracle.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::protocol::order_entry;
using namespace mdh::trader::oms;
using namespace mdh::trader::positions;
using namespace mdh::trader::risk;
using namespace mdh::trader::strategies;

namespace {

constexpr AccountId kAccount = 1;
constexpr InstrumentId kInstrument = 7;
constexpr Price kReference = 1'000'000; // 100.0000

class FakeSender {
public:
    [[nodiscard]] bool operator()(const Message& message) {
        sent.push_back(message);
        return true;
    }
    std::vector<Message> sent;
};

[[nodiscard]] LadderMarketMakerConfig make_config(Price step_size = 0) {
    LadderMarketMakerConfig config;
    config.instrument_id = kInstrument;
    config.walk = ReferencePriceWalkConfig{
        .initial_price = kReference, .step_size = step_size, .min_price = 1, .max_price = 2 * kReference, .seed = 4};
    config.levels_per_side = 2;
    config.half_spread = 100;
    config.level_spacing = 100;
    config.quote_size = 50;
    config.max_position = 500;
    config.requote_threshold = 100;
    return config;
}

[[nodiscard]] Message accepted_for(ClientOrderId id, Side side, Price price, Quantity quantity) {
    return Message{Accepted{.account_id = kAccount,
                             .client_order_id = id,
                             .exchange_order_id = id,
                             .instrument_id = kInstrument,
                             .side = side,
                             .price = price,
                             .quantity = quantity,
                             .order_type = OrderType::Limit,
                             .time_in_force = TimeInForce::GTC}};
}

// Plays the gateway accepting every quote the ladder currently has
// outstanding, so the next cycle sees Live orders rather than PendingNew
// ones it must leave alone.
void accept_everything(TraderRiskGatedOms& trading, const LadderMarketMaker& mm) {
    for (const auto* side : {&mm.bids(), &mm.asks()}) {
        for (const auto& quote : *side) {
            const auto id = quote.order_id();
            if (!id) {
                continue;
            }
            const auto order = trading.order(*id);
            if (order && order->state == ClientOrderState::PendingNew) {
                trading.handle_message(accepted_for(*id, order->side, order->price, order->quantity));
            }
        }
    }
}

[[nodiscard]] std::vector<Price> new_order_prices(const std::vector<Message>& sent, Side side) {
    std::vector<Price> prices;
    for (const auto& message : sent) {
        if (const auto* order = std::get_if<NewOrder>(&message); order != nullptr && order->side == side) {
            prices.push_back(order->price);
        }
    }
    return prices;
}

// A funded trader, so neither side of the ladder is turned away by
// trader-side risk before it reaches the fake wire.
[[nodiscard]] std::unique_ptr<TraderRiskGatedOms> make_trader(FakeSender& sender) {
    auto trading = std::make_unique<TraderRiskGatedOms>(kAccount, std::ref(sender));
    trading->deposit_cash(1'000'000'000'000);
    trading->deposit_position(kInstrument, 1'000'000);
    return trading;
}

} // namespace

TEST(LadderMarketMaker, QuotesBothSidesAroundTheReferencePriceOnItsFirstCycle) {
    FakeSender sender;
    auto trading = make_trader(sender);
    LadderMarketMaker mm(*trading, [] { return NetPosition{0}; }, make_config());

    const std::size_t sent = mm.on_quote_cycle();

    EXPECT_EQ(sent, 4u); // two levels, both sides
    ASSERT_EQ(sender.sent.size(), 4u);
    // 99.99 / 99.98 bid, 100.01 / 100.02 ask -- the ladder this participant
    // exists to put on the book.
    EXPECT_EQ(new_order_prices(sender.sent, Side::Buy), (std::vector<Price>{999'900, 999'800}));
    EXPECT_EQ(new_order_prices(sender.sent, Side::Sell), (std::vector<Price>{1'000'100, 1'000'200}));
}

TEST(LadderMarketMaker, QuotesFromAnEmptyBookBecauseItNeverConsultsOne) {
    // The whole reason this class exists alongside MarketMakerStrategy: no
    // book is passed in at all, so there is no such thing as "no market to
    // quote around" and the first cycle always establishes one.
    FakeSender sender;
    auto trading = make_trader(sender);
    LadderMarketMaker mm(*trading, [] { return NetPosition{0}; }, make_config());

    EXPECT_EQ(mm.on_quote_cycle(), 4u);
}

TEST(LadderMarketMaker, LevelsPerSideIsHonoured) {
    FakeSender sender;
    auto trading = make_trader(sender);
    auto config = make_config();
    config.levels_per_side = 4;
    LadderMarketMaker mm(*trading, [] { return NetPosition{0}; }, config);

    EXPECT_EQ(mm.on_quote_cycle(), 8u);
    EXPECT_EQ(mm.bids().size(), 4u);
    EXPECT_EQ(new_order_prices(sender.sent, Side::Buy),
               (std::vector<Price>{999'900, 999'800, 999'700, 999'600}));
}

TEST(LadderMarketMaker, SendsNothingWhileItsQuotesAreStillPendingNew) {
    FakeSender sender;
    auto trading = make_trader(sender);
    LadderMarketMaker mm(*trading, [] { return NetPosition{0}; }, make_config());

    EXPECT_EQ(mm.on_quote_cycle(), 4u);
    EXPECT_EQ(mm.on_quote_cycle(), 0u); // nothing acknowledged yet -- nothing to pile on

    EXPECT_EQ(sender.sent.size(), 4u);
}

TEST(LadderMarketMaker, HoldsAnAcknowledgedLadderStillWhenTheReferenceHasNotMoved) {
    FakeSender sender;
    auto trading = make_trader(sender);
    LadderMarketMaker mm(*trading, [] { return NetPosition{0}; }, make_config());

    (void)mm.on_quote_cycle();
    accept_everything(*trading, mm);

    // step_size 0: the reference is where it was, so every level is already
    // exactly where it wants to be and keeps its queue position.
    EXPECT_EQ(mm.on_quote_cycle(), 0u);
    EXPECT_EQ(sender.sent.size(), 4u);
}

TEST(LadderMarketMaker, ReplacesTheWholeLadderOnceTheReferenceMoves) {
    FakeSender sender;
    auto trading = make_trader(sender);
    const auto config = make_config(/*step_size=*/100);
    LadderMarketMaker mm(*trading, [] { return NetPosition{0}; }, config);

    // A second walk on the same seed and config is the oracle for what the
    // strategy's own reference price must be after each cycle -- which also
    // pins that a cycle advances the walk exactly once.
    ReferencePriceWalk oracle(config.walk);

    (void)mm.on_quote_cycle();
    ASSERT_EQ(mm.reference_price(), oracle.step());
    accept_everything(*trading, mm);
    const Price quoted_around = mm.reference_price();
    sender.sent.clear();

    // Cycles where the walk happens not to move send nothing, so keep going
    // until it does. (Two thirds of steps move, so this is a handful of
    // iterations at most; the bound is a test-hang guard, not a tolerance.)
    for (int i = 0; i < 100 && mm.reference_price() == quoted_around; ++i) {
        (void)mm.on_quote_cycle();
        ASSERT_EQ(mm.reference_price(), oracle.step());
    }
    ASSERT_NE(mm.reference_price(), quoted_around);

    const Price reference = mm.reference_price();
    ASSERT_EQ(sender.sent.size(), 4u);
    for (const auto& message : sender.sent) {
        EXPECT_NE(std::get_if<ReplaceOrder>(&message), nullptr); // replaced, not cancelled and re-added
    }
    std::vector<Price> bid_prices;
    std::vector<Price> ask_prices;
    for (const auto& message : sender.sent) {
        const auto* replace = std::get_if<ReplaceOrder>(&message);
        ASSERT_NE(replace, nullptr);
        (replace->new_price < reference ? bid_prices : ask_prices).push_back(replace->new_price);
    }
    EXPECT_EQ(bid_prices, (std::vector<Price>{reference - 100, reference - 200}));
    EXPECT_EQ(ask_prices, (std::vector<Price>{reference + 100, reference + 200}));
}

TEST(LadderMarketMaker, MovesTheFarSideFirstSoItCannotCrossItsOwnStaleQuote) {
    FakeSender sender;
    auto trading = make_trader(sender);
    // A walk step far wider than the spread, so one move really would put a
    // new bid through the old ask if the sides were updated in the wrong
    // order.
    auto config = make_config(/*step_size=*/1'000);
    config.requote_threshold = 1;
    LadderMarketMaker mm(*trading, [] { return NetPosition{0}; }, config);

    (void)mm.on_quote_cycle();
    accept_everything(*trading, mm);
    Price previous = mm.reference_price();
    sender.sent.clear();

    for (int i = 0; i < 100 && mm.reference_price() == previous; ++i) {
        (void)mm.on_quote_cycle();
    }
    const bool rose = mm.reference_price() > previous;
    ASSERT_NE(mm.reference_price(), previous);
    ASSERT_EQ(sender.sent.size(), 4u);

    // Whichever side is moving away goes out first: asks on a rise, bids on
    // a fall. Classified against the new reference, not the old price -- a
    // step wider than the spread moves both sides past the old price.
    const Price reference = mm.reference_price();
    const auto* first = std::get_if<ReplaceOrder>(&sender.sent[0]);
    ASSERT_NE(first, nullptr);
    if (rose) {
        EXPECT_GT(first->new_price, reference); // an ask
    } else {
        EXPECT_LT(first->new_price, reference); // a bid
    }
}

TEST(LadderMarketMaker, WithdrawsTheBidSideAtThePositionCap) {
    FakeSender sender;
    auto trading = make_trader(sender);
    const auto config = make_config();
    LadderMarketMaker mm(*trading, [&] { return config.max_position; }, config);

    EXPECT_EQ(mm.on_quote_cycle(), 2u); // asks only
    EXPECT_TRUE(new_order_prices(sender.sent, Side::Buy).empty());
    EXPECT_EQ(new_order_prices(sender.sent, Side::Sell).size(), 2u);
}

TEST(LadderMarketMaker, WithdrawsTheAskSideWhenNetShortAtTheCap) {
    // The symmetric half of the cap, and the reason this strategy needs a
    // signed position: a participant selling against seeded inventory is net
    // short long before it runs out of anything to sell.
    FakeSender sender;
    auto trading = make_trader(sender);
    const auto config = make_config();
    LadderMarketMaker mm(*trading, [&] { return -config.max_position; }, config);

    EXPECT_EQ(mm.on_quote_cycle(), 2u); // bids only
    EXPECT_EQ(new_order_prices(sender.sent, Side::Buy).size(), 2u);
    EXPECT_TRUE(new_order_prices(sender.sent, Side::Sell).empty());
}

TEST(LadderMarketMaker, CancelsAnAlreadyRestingQuoteWhenTheCapIsReachedLater) {
    FakeSender sender;
    auto trading = make_trader(sender);
    const auto config = make_config();
    NetPosition position = 0;
    LadderMarketMaker mm(*trading, [&] { return position; }, config);

    (void)mm.on_quote_cycle();
    accept_everything(*trading, mm);
    sender.sent.clear();

    position = config.max_position; // filled up to the cap between cycles
    EXPECT_EQ(mm.on_quote_cycle(), 2u);

    ASSERT_EQ(sender.sent.size(), 2u);
    for (const auto& message : sender.sent) {
        EXPECT_NE(std::get_if<CancelOrder>(&message), nullptr); // withdrawn, not just left un-requoted
    }
}

TEST(LadderMarketMaker, ReplenishesALevelAfterItsQuoteIsFilled) {
    FakeSender sender;
    auto trading = make_trader(sender);
    LadderMarketMaker mm(*trading, [] { return NetPosition{0}; }, make_config());

    (void)mm.on_quote_cycle();
    accept_everything(*trading, mm);
    const auto filled_id = *mm.bids()[0].order_id();
    const auto filled_order = *trading->order(filled_id);
    trading->handle_message(Message{TradeReport{.account_id = kAccount,
                                                 .client_order_id = filled_id,
                                                 .exchange_order_id = filled_id,
                                                 .instrument_id = kInstrument,
                                                 .price = filled_order.price,
                                                 .quantity = filled_order.quantity,
                                                 .remaining_quantity = 0}});
    ASSERT_EQ(trading->order(filled_id)->state, ClientOrderState::Filled);
    sender.sent.clear();

    // The filled level is gone from the book, so the next cycle puts a fresh
    // order there -- this is the "replenish" half of
    // cancel/replace/replenish, and it is what keeps the book alive.
    EXPECT_EQ(mm.on_quote_cycle(), 1u);
    ASSERT_EQ(sender.sent.size(), 1u);
    const auto* replacement = std::get_if<NewOrder>(&sender.sent[0]);
    ASSERT_NE(replacement, nullptr);
    EXPECT_EQ(replacement->side, Side::Buy);
    EXPECT_EQ(replacement->price, 999'900);
    EXPECT_NE(*mm.bids()[0].order_id(), filled_id);
}

TEST(LadderMarketMaker, WithdrawAllCancelsEveryRestingQuote) {
    FakeSender sender;
    auto trading = make_trader(sender);
    LadderMarketMaker mm(*trading, [] { return NetPosition{0}; }, make_config());

    (void)mm.on_quote_cycle();
    accept_everything(*trading, mm);
    sender.sent.clear();

    EXPECT_EQ(mm.withdraw_all(), 4u);
    ASSERT_EQ(sender.sent.size(), 4u);
    for (const auto& message : sender.sent) {
        EXPECT_NE(std::get_if<CancelOrder>(&message), nullptr);
    }
    EXPECT_FALSE(mm.bids()[0].order_id().has_value());
}
