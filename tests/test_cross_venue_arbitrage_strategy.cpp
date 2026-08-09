#include <gtest/gtest.h>

#include <tuple>
#include <vector>

#include "book/order_book.hpp"
#include "trader/strategies/cross_venue_arbitrage_strategy.hpp"

// Composition-level unit tests for CrossVenueArbStrategy (Milestone 11) --
// two fake Senders stand in for two independent venues' wires (same
// FakeSender pattern test_market_maker_strategy.cpp and
// test_trader_risk_gated_oms.cpp use), and book::OrderBook instances are
// built directly per venue -- no real gateway/StrategyRuntime needed to
// exercise the strategy's own on_venue_a_update()/on_venue_b_update() logic.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::protocol::order_entry;
using namespace mdh::trader::risk;
using namespace mdh::trader::strategies;

namespace {

constexpr AccountId kAccount = 1;
constexpr InstrumentId kInstrument = 9;

class FakeSender {
public:
    [[nodiscard]] bool operator()(const Message& message) {
        sent.push_back(message);
        return true;
    }
    std::vector<Message> sent;
};

[[nodiscard]] book::OrderBook make_book(Price bid_price, Price ask_price) {
    book::OrderBook book;
    static OrderId next_id = 1;
    std::ignore = book.add_order(next_id++, bid_price, /*qty=*/50, Side::Buy);
    std::ignore = book.add_order(next_id++, ask_price, /*qty=*/50, Side::Sell);
    return book;
}

struct Fixture {
    FakeSender sender_a;
    FakeSender sender_b;
    TraderRiskGatedOms venue_a{kAccount, std::ref(sender_a)};
    TraderRiskGatedOms venue_b{kAccount, std::ref(sender_b)};

    Fixture() {
        venue_a.deposit_cash(1'000'000);
        venue_a.deposit_position(kInstrument, 1'000);
        venue_b.deposit_cash(1'000'000);
        venue_b.deposit_position(kInstrument, 1'000);
    }
};

} // namespace

TEST(CrossVenueArbStrategy, DoesNothingWhenBooksAreNotYetCrossed) {
    Fixture f;
    CrossVenueArbStrategy arb(f.venue_a, f.venue_b, CrossVenueArbConfig{.instrument_id = kInstrument, .trade_size = 10, .min_edge = 1});

    auto book_a = make_book(98, 102);
    auto book_b = make_book(99, 103); // overlapping bid/ask ranges, but ask(a)=102 > bid(b)=99 and ask(b)=103 > bid(a)=98 -- no edge
    arb.on_venue_a_update(kInstrument, book_a);
    arb.on_venue_b_update(kInstrument, book_b);

    EXPECT_TRUE(f.sender_a.sent.empty());
    EXPECT_TRUE(f.sender_b.sent.empty());
}

TEST(CrossVenueArbStrategy, DoesNotTradeWhenTheEdgeIsBelowMinEdge) {
    Fixture f;
    CrossVenueArbStrategy arb(f.venue_a, f.venue_b, CrossVenueArbConfig{.instrument_id = kInstrument, .trade_size = 10, .min_edge = 5});

    auto book_a = make_book(98, 100); // venue A ask = 100
    auto book_b = make_book(102, 104); // venue B bid = 102 -- edge = 2, below min_edge 5
    arb.on_venue_a_update(kInstrument, book_a);
    arb.on_venue_b_update(kInstrument, book_b);

    EXPECT_TRUE(f.sender_a.sent.empty());
    EXPECT_TRUE(f.sender_b.sent.empty());
}

TEST(CrossVenueArbStrategy, BuysCheapVenueAAndSellsRichVenueBWhenEdgeIsWideEnough) {
    Fixture f;
    CrossVenueArbStrategy arb(f.venue_a, f.venue_b, CrossVenueArbConfig{.instrument_id = kInstrument, .trade_size = 10, .min_edge = 5});

    auto book_a = make_book(90, 100); // venue A ask = 100
    auto book_b = make_book(110, 120); // venue B bid = 110 -- edge = 10, >= min_edge 5

    arb.on_venue_a_update(kInstrument, book_a); // only A observed so far -- nothing to compare against yet
    EXPECT_TRUE(f.sender_a.sent.empty());

    arb.on_venue_b_update(kInstrument, book_b); // now both sides known -- the edge fires

    ASSERT_EQ(f.sender_a.sent.size(), 1u);
    const auto* buy_a = std::get_if<NewOrder>(&f.sender_a.sent[0]);
    ASSERT_NE(buy_a, nullptr);
    EXPECT_EQ(buy_a->side, Side::Buy);
    EXPECT_EQ(buy_a->price, 100);
    EXPECT_EQ(buy_a->quantity, 10u);
    EXPECT_EQ(buy_a->time_in_force, TimeInForce::IOC);

    ASSERT_EQ(f.sender_b.sent.size(), 1u);
    const auto* sell_b = std::get_if<NewOrder>(&f.sender_b.sent[0]);
    ASSERT_NE(sell_b, nullptr);
    EXPECT_EQ(sell_b->side, Side::Sell);
    EXPECT_EQ(sell_b->price, 110);
    EXPECT_EQ(sell_b->quantity, 10u);
    EXPECT_EQ(sell_b->time_in_force, TimeInForce::IOC);
}

TEST(CrossVenueArbStrategy, BuysCheapVenueBAndSellsRichVenueAWhenEdgeIsWideEnough) {
    Fixture f;
    CrossVenueArbStrategy arb(f.venue_a, f.venue_b, CrossVenueArbConfig{.instrument_id = kInstrument, .trade_size = 10, .min_edge = 5});

    auto book_b = make_book(90, 100); // venue B ask = 100
    auto book_a = make_book(110, 120); // venue A bid = 110 -- edge = 10, >= min_edge 5

    arb.on_venue_b_update(kInstrument, book_b);
    arb.on_venue_a_update(kInstrument, book_a);

    ASSERT_EQ(f.sender_b.sent.size(), 1u);
    const auto* buy_b = std::get_if<NewOrder>(&f.sender_b.sent[0]);
    ASSERT_NE(buy_b, nullptr);
    EXPECT_EQ(buy_b->side, Side::Buy);
    EXPECT_EQ(buy_b->price, 100);

    ASSERT_EQ(f.sender_a.sent.size(), 1u);
    const auto* sell_a = std::get_if<NewOrder>(&f.sender_a.sent[0]);
    ASSERT_NE(sell_a, nullptr);
    EXPECT_EQ(sell_a->side, Side::Sell);
    EXPECT_EQ(sell_a->price, 110);
}

TEST(CrossVenueArbStrategy, IgnoresUpdatesForADifferentInstrument) {
    Fixture f;
    CrossVenueArbStrategy arb(f.venue_a, f.venue_b, CrossVenueArbConfig{.instrument_id = kInstrument, .trade_size = 10, .min_edge = 1});

    auto book_a = make_book(90, 100);
    auto book_b = make_book(110, 120);
    arb.on_venue_a_update(kInstrument + 1, book_a);
    arb.on_venue_b_update(kInstrument + 1, book_b);

    EXPECT_TRUE(f.sender_a.sent.empty());
    EXPECT_TRUE(f.sender_b.sent.empty());
}

TEST(CrossVenueArbStrategy, RespectsEachVenuesOwnTraderSideRiskLimits) {
    // Venue A's account has no cash deposited at all -- the buy leg on A
    // must be locally risk-rejected (never reaching sender_a), and this
    // strategy must not crash or otherwise treat that as reason to skip
    // sending the (independently risk-gated) sell leg on B.
    FakeSender sender_a;
    FakeSender sender_b;
    TraderRiskGatedOms venue_a(kAccount, std::ref(sender_a)); // no deposit_cash() at all
    TraderRiskGatedOms venue_b(kAccount, std::ref(sender_b));
    venue_b.deposit_cash(1'000'000);
    venue_b.deposit_position(kInstrument, 1'000);

    CrossVenueArbStrategy arb(venue_a, venue_b, CrossVenueArbConfig{.instrument_id = kInstrument, .trade_size = 10, .min_edge = 5});

    auto book_a = make_book(90, 100);
    auto book_b = make_book(110, 120);
    arb.on_venue_a_update(kInstrument, book_a);
    arb.on_venue_b_update(kInstrument, book_b);

    EXPECT_TRUE(sender_a.sent.empty());  // locally risk-rejected: no cash on venue A
    EXPECT_EQ(sender_b.sent.size(), 1u); // venue B's independent leg still went out
}
