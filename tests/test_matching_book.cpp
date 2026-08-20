#include <gtest/gtest.h>

#include "exchange/matching/matching_book.hpp"
#include "exchange/matching/matching_engine.hpp" // for the ladder budget the band table below is derived from

namespace mdh::exchange {
namespace {

BookOrder make_order(ExchangeOrderId id, Side side, Price price, Quantity qty, ClientOrderId client_order_id = 1,
                     AccountId account_id = 1) {
    return BookOrder{
        .exchange_order_id = id,
        .client_order_id = client_order_id,
        .account_id = account_id,
        .price = price,
        .remaining_quantity = qty,
        .side = side,
        .time_in_force = TimeInForce::GTC,
    };
}

TEST(MatchingBook, AddReturnsAHandleThatReadsBackTheOrder) {
    MatchingBook book;
    const auto handle = book.add(make_order(1, Side::Buy, 100, 10));

    EXPECT_EQ(book.at(handle).exchange_order_id, 1u);
    EXPECT_EQ(book.at(handle).price, 100);
    EXPECT_EQ(book.at(handle).remaining_quantity, 10u);
}

TEST(MatchingBook, FifoOrderWithinOneLevel) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, 100, 5));
    book.add(make_order(2, Side::Buy, 100, 7));
    book.add(make_order(3, Side::Buy, 100, 9));

    const BookOrder* front = book.front_of_best(Side::Buy);
    ASSERT_NE(front, nullptr);
    EXPECT_EQ(front->exchange_order_id, 1u); // first-added is first in FIFO

    book.remove_front(Side::Buy);
    front = book.front_of_best(Side::Buy);
    ASSERT_NE(front, nullptr);
    EXPECT_EQ(front->exchange_order_id, 2u);
}

TEST(MatchingBook, BestBidIsHighestAndBestAskIsLowest) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, 100, 1));
    book.add(make_order(2, Side::Buy, 105, 1));
    book.add(make_order(3, Side::Buy, 102, 1));
    book.add(make_order(4, Side::Sell, 110, 1));
    book.add(make_order(5, Side::Sell, 108, 1));
    book.add(make_order(6, Side::Sell, 112, 1));

    ASSERT_TRUE(book.best_bid_price().has_value());
    EXPECT_EQ(*book.best_bid_price(), 105);
    ASSERT_TRUE(book.best_ask_price().has_value());
    EXPECT_EQ(*book.best_ask_price(), 108);
}

TEST(MatchingBook, RemoveWorksFromAnyFifoPositionNotJustFront) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, 100, 5));
    const auto middle = book.add(make_order(2, Side::Buy, 100, 7));
    book.add(make_order(3, Side::Buy, 100, 9));

    const BookOrder removed = book.remove_at(middle);
    EXPECT_EQ(removed.exchange_order_id, 2u);

    // Remaining FIFO order is 1 then 3.
    auto all = book.all_bids();
    ASSERT_EQ(all.size(), 2u);
    EXPECT_EQ(all[0].exchange_order_id, 1u);
    EXPECT_EQ(all[1].exchange_order_id, 3u);
}

// The engine holds handles across arbitrary other traffic, so this is the
// invariant that makes the whole handle scheme safe: only removing the order
// a handle names may invalidate it.
TEST(MatchingBook, HandleSurvivesOtherOrdersComingAndGoing) {
    MatchingBook book;
    const auto held = book.add(make_order(1, Side::Buy, 100, 5));
    const auto neighbour = book.add(make_order(2, Side::Buy, 100, 7));
    book.add(make_order(3, Side::Buy, 105, 9));   // a new, better level
    book.add(make_order(4, Side::Sell, 110, 9));  // the other side

    book.remove_at(neighbour);
    book.remove_front(Side::Buy); // drains the 105 level entirely
    book.add(make_order(5, Side::Buy, 100, 3));

    EXPECT_EQ(book.at(held).exchange_order_id, 1u);
    EXPECT_EQ(book.at(held).remaining_quantity, 5u);
    EXPECT_EQ(book.remove_at(held).exchange_order_id, 1u);
}

// Orders live in one flat slab that grows geometrically, so an add can
// relocate every order already in the book. Handles are slab indices rather
// than addresses precisely so that this is a non-event -- but "precisely so"
// is worth an assertion, since the failure mode would be a use-after-free
// reachable from any cancel.
TEST(MatchingBook, HandleSurvivesTheSlabGrowingUnderIt) {
    MatchingBook book;
    const auto held = book.add(make_order(1, Side::Buy, 100, 5));

    for (ExchangeOrderId id = 2; id <= 10'000; ++id) {
        book.add(make_order(id, Side::Buy, 100 + static_cast<Price>(id), 1));
    }

    EXPECT_EQ(book.at(held).exchange_order_id, 1u);
    EXPECT_EQ(book.remove_at(held).remaining_quantity, 5u);
}

// A freed slot goes back on a LIFO free list, so the next order added takes
// the slot of the most recently removed one -- a slot number *behind* orders
// that are already resting. Queue position therefore cannot be inferred from
// slot number, and this asserts it is not: D arrives last and must match
// last, whatever slab entry it happens to occupy.
TEST(MatchingBook, RecycledSlotDoesNotInheritTheQueuePositionItCameFrom) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, 100, 1));
    const auto second = book.add(make_order(2, Side::Buy, 100, 1));
    book.add(make_order(3, Side::Buy, 100, 1));

    book.remove_at(second);
    book.add(make_order(4, Side::Buy, 100, 1));

    const auto all = book.all_bids();
    ASSERT_EQ(all.size(), 3u);
    EXPECT_EQ(all[0].exchange_order_id, 1u);
    EXPECT_EQ(all[1].exchange_order_id, 3u);
    EXPECT_EQ(all[2].exchange_order_id, 4u);
}

// The same recycling one level over: a slot freed from the bid side must be
// reusable by an ask without carrying anything of its old level with it.
TEST(MatchingBook, SlotFreedOnOneSideIsReusableOnTheOther) {
    MatchingBook book;
    const auto bid = book.add(make_order(1, Side::Buy, 100, 5));
    book.remove_at(bid);

    const auto ask = book.add(make_order(2, Side::Sell, 110, 7));
    EXPECT_FALSE(book.best_bid_price().has_value());
    ASSERT_TRUE(book.best_ask_price().has_value());
    EXPECT_EQ(*book.best_ask_price(), 110);
    EXPECT_EQ(book.at(ask).exchange_order_id, 2u);
    EXPECT_TRUE(book.all_bids().empty());
    ASSERT_EQ(book.all_asks().size(), 1u);
}

TEST(MatchingBook, EmptyLevelIsClearedAfterLastOrderRemoved) {
    MatchingBook book;
    const auto only = book.add(make_order(1, Side::Buy, 100, 5));
    EXPECT_TRUE(book.best_bid_price().has_value());

    book.remove_at(only);
    EXPECT_FALSE(book.best_bid_price().has_value());
}

TEST(MatchingBook, EmptyLevelIsClearedAfterRemoveFrontOfLastOrder) {
    MatchingBook book;
    book.add(make_order(1, Side::Sell, 100, 5));
    book.remove_front(Side::Sell);
    EXPECT_FALSE(book.best_ask_price().has_value());
}

TEST(MatchingBook, ReduceInPlacePreservesFifoPosition) {
    MatchingBook book;
    const auto first = book.add(make_order(1, Side::Buy, 100, 5));
    book.add(make_order(2, Side::Buy, 100, 7));

    book.reduce_at(first, 2);
    EXPECT_EQ(book.at(first).remaining_quantity, 2u);

    // Still first in FIFO despite the mutation.
    const BookOrder* front = book.front_of_best(Side::Buy);
    ASSERT_NE(front, nullptr);
    EXPECT_EQ(front->exchange_order_id, 1u);
}

TEST(MatchingBook, SetClientOrderIdUpdatesInPlaceWithoutChangingFifoPosition) {
    MatchingBook book;
    const auto first = book.add(make_order(1, Side::Buy, 100, 5, /*client_order_id=*/1));
    book.add(make_order(2, Side::Buy, 100, 7, /*client_order_id=*/2));

    book.set_client_order_id_at(first, 99);
    EXPECT_EQ(book.at(first).client_order_id, 99u);
    EXPECT_EQ(book.at(first).exchange_order_id, 1u); // exchange_order_id never changes

    // Still first in FIFO despite the mutation.
    const BookOrder* front = book.front_of_best(Side::Buy);
    ASSERT_NE(front, nullptr);
    EXPECT_EQ(front->exchange_order_id, 1u);
}

TEST(MatchingBook, AllBidsOrderedByPricePriorityThenFifo) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, 100, 1));
    book.add(make_order(2, Side::Buy, 105, 1));
    book.add(make_order(3, Side::Buy, 100, 1));
    book.add(make_order(4, Side::Buy, 105, 1));

    auto all = book.all_bids();
    ASSERT_EQ(all.size(), 4u);
    // 105 (highest bid price) first, in FIFO order, then 100.
    EXPECT_EQ(all[0].exchange_order_id, 2u);
    EXPECT_EQ(all[1].exchange_order_id, 4u);
    EXPECT_EQ(all[2].exchange_order_id, 1u);
    EXPECT_EQ(all[3].exchange_order_id, 3u);
}

TEST(MatchingBook, FrontOfBestOnEmptySideReturnsNullptr) {
    MatchingBook book;
    EXPECT_EQ(book.front_of_best(Side::Buy), nullptr);
    EXPECT_EQ(book.front_of_best(Side::Sell), nullptr);
}

// ── The ladder's band, and what happens outside it ─────────────────────────
//
// A side anchors its ladder on its first order, centred, so a book whose
// first bid is at kAnchor indexes [kAnchor - kMaxBandTicks/2, kAnchor +
// kMaxBandTicks/2). These tests work in those terms rather than in absolute
// prices, so that changing the band width does not silently stop them from
// testing what they are named for.
constexpr Price kAnchor = 1'000'000;
constexpr Price kHalfBand = MatchingBook::kMaxBandTicks / 2;
constexpr Price kBandLow = kAnchor - kHalfBand;
constexpr Price kBandHigh = kAnchor + kHalfBand - 1;

TEST(MatchingBook, PricesInsideTheBandUseTheLadderAndPricesOutsideDoNot) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, kAnchor, 1));
    EXPECT_EQ(book.out_of_band_levels(), 0u);

    // Both edges are in band; one tick past either is not.
    book.add(make_order(2, Side::Buy, kBandLow, 1));
    book.add(make_order(3, Side::Buy, kBandHigh, 1));
    EXPECT_EQ(book.out_of_band_levels(), 0u);

    book.add(make_order(4, Side::Buy, kBandLow - 1, 1));
    book.add(make_order(5, Side::Buy, kBandHigh + 1, 1));
    EXPECT_EQ(book.out_of_band_levels(), 2u);
}

TEST(MatchingBook, BestPriceIsCorrectWhenTheTouchIsOutOfBand) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, kAnchor, 1));
    book.add(make_order(2, Side::Sell, kAnchor + 10, 1));

    // A bid above the band's top and an ask below its bottom: on both sides
    // the best price now lives in the overflow map, not the ladder.
    book.add(make_order(3, Side::Buy, kBandHigh + 500, 1));
    book.add(make_order(4, Side::Sell, kAnchor - kHalfBand - 500, 1));

    EXPECT_EQ(book.best_bid_price(), kBandHigh + 500);
    EXPECT_EQ(book.best_ask_price(), kAnchor - kHalfBand - 500);

    const BookOrder* bid = book.front_of_best(Side::Buy);
    ASSERT_NE(bid, nullptr);
    EXPECT_EQ(bid->exchange_order_id, 3u);
}

TEST(MatchingBook, LevelsStraddlingTheBandWalkInOnePriorityOrder) {
    MatchingBook book;
    // Interleaved so that neither index holds a contiguous run: the walk has
    // to alternate between the ladder and the map to get this right.
    book.add(make_order(1, Side::Buy, kAnchor, 1));           // in band
    book.add(make_order(2, Side::Buy, kBandHigh + 100, 1));   // above band
    book.add(make_order(3, Side::Buy, kBandHigh, 1));         // in band, top edge
    book.add(make_order(4, Side::Buy, kBandLow - 100, 1));    // below band
    book.add(make_order(5, Side::Buy, kBandHigh + 1, 1));     // above band, just past the edge
    book.add(make_order(6, Side::Buy, kBandLow, 1));          // in band, bottom edge

    const auto all = book.all_bids();
    ASSERT_EQ(all.size(), 6u);
    EXPECT_EQ(all[0].exchange_order_id, 2u);
    EXPECT_EQ(all[1].exchange_order_id, 5u);
    EXPECT_EQ(all[2].exchange_order_id, 3u);
    EXPECT_EQ(all[3].exchange_order_id, 1u);
    EXPECT_EQ(all[4].exchange_order_id, 6u);
    EXPECT_EQ(all[5].exchange_order_id, 4u);

    // The mirrored case, so that the ascending walk is checked too.
    MatchingBook asks;
    asks.add(make_order(1, Side::Sell, kAnchor, 1));
    asks.add(make_order(2, Side::Sell, kBandLow - 100, 1));
    asks.add(make_order(3, Side::Sell, kBandLow, 1));
    asks.add(make_order(4, Side::Sell, kBandHigh + 100, 1));
    asks.add(make_order(5, Side::Sell, kBandLow - 1, 1));
    asks.add(make_order(6, Side::Sell, kBandHigh, 1));

    const auto all_asks = asks.all_asks();
    ASSERT_EQ(all_asks.size(), 6u);
    EXPECT_EQ(all_asks[0].exchange_order_id, 2u);
    EXPECT_EQ(all_asks[1].exchange_order_id, 5u);
    EXPECT_EQ(all_asks[2].exchange_order_id, 3u);
    EXPECT_EQ(all_asks[3].exchange_order_id, 1u);
    EXPECT_EQ(all_asks[4].exchange_order_id, 6u);
    EXPECT_EQ(all_asks[5].exchange_order_id, 4u);
}

TEST(MatchingBook, CrossableQuantityStopsAtThePriceBoundAcrossTheBandEdge) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, kAnchor, 10));
    book.add(make_order(2, Side::Buy, kBandHigh + 100, 10)); // out of band, best
    book.add(make_order(3, Side::Buy, kBandHigh, 10));       // in band, second
    book.add(make_order(4, Side::Buy, kBandLow - 100, 10));  // out of band, worst

    // A sell at the band's top edge can hit the two levels at or above it.
    EXPECT_EQ(book.crossable_quantity(Side::Buy, kBandHigh, 1'000), 20u);
    // One tick lower picks up the anchor level as well.
    EXPECT_EQ(book.crossable_quantity(Side::Buy, kAnchor, 1'000), 30u);
    // Below everything, and the whole side is crossable.
    EXPECT_EQ(book.crossable_quantity(Side::Buy, 1, 1'000), 40u);
    // Above everything, and none of it is.
    EXPECT_EQ(book.crossable_quantity(Side::Buy, kBandHigh + 1'000, 1'000), 0u);
}

TEST(MatchingBook, AnEmptiedSideReanchorsItsLadderSomewhereElse) {
    MatchingBook book;
    const auto first = book.add(make_order(1, Side::Buy, kAnchor, 1));

    // Far enough away that the old band cannot reach: without re-anchoring
    // this order would spend its life in the overflow map.
    const Price elsewhere = kAnchor + 10 * MatchingBook::kMaxBandTicks;
    book.add(make_order(2, Side::Buy, elsewhere, 1));
    EXPECT_EQ(book.out_of_band_levels(), 1u);

    // Emptying the ladder is not enough on its own -- the side still holds
    // the out-of-band order, so the base stays where it is.
    book.remove_at(first);
    EXPECT_EQ(book.best_bid_price(), elsewhere);
    EXPECT_EQ(book.out_of_band_levels(), 1u);

    // With nothing resting at all, the old base has nothing to protect.
    book.remove_front(Side::Buy);
    ASSERT_FALSE(book.best_bid_price().has_value());

    book.add(make_order(3, Side::Buy, elsewhere, 1));
    EXPECT_EQ(book.out_of_band_levels(), 0u);
    EXPECT_EQ(book.best_bid_price(), elsewhere);
}

TEST(MatchingBook, ABookWithNoLadderBehavesTheSameOnTheMapAlone) {
    // What a universe too large to afford a ladder gets. Every price is out
    // of band because there is no band, so this is stage 2's structure --
    // and it has to stay indistinguishable from the outside.
    MatchingBook book(/*expected_resting_orders=*/0, /*band_ticks=*/0);
    book.add(make_order(1, Side::Buy, 100, 5));
    book.add(make_order(2, Side::Buy, 105, 5));
    book.add(make_order(3, Side::Buy, 100, 7));
    book.add(make_order(4, Side::Sell, 110, 5));

    EXPECT_EQ(book.out_of_band_levels(), 3u);
    EXPECT_EQ(book.best_bid_price(), 105);
    EXPECT_EQ(book.best_ask_price(), 110);
    EXPECT_EQ(book.crossable_quantity(Side::Buy, 100, 1'000), 17u);

    const auto all = book.all_bids();
    ASSERT_EQ(all.size(), 3u);
    EXPECT_EQ(all[0].exchange_order_id, 2u);
    EXPECT_EQ(all[1].exchange_order_id, 1u);
    EXPECT_EQ(all[2].exchange_order_id, 3u);
}

TEST(MatchingBook, BandNarrowsWithTheInstrumentCountAndThenStops) {
    constexpr std::size_t kBudget = MatchingEngine::kLadderByteBudget;
    // A universe of one gets the widest band the book will index, not the
    // whole budget: past kMaxBandTicks the measurements said it stops paying.
    EXPECT_EQ(MatchingBook::band_for(1, kBudget), MatchingBook::kMaxBandTicks);
    EXPECT_EQ(MatchingBook::band_for(64, kBudget), MatchingBook::kMaxBandTicks);
    EXPECT_EQ(MatchingBook::band_for(128, kBudget), 4'096u);
    EXPECT_EQ(MatchingBook::band_for(512, kBudget), MatchingBook::kMinBandTicks);
    // Past the point where a book's share buys a useful band, there is no
    // ladder rather than a token one.
    EXPECT_EQ(MatchingBook::band_for(1'024, kBudget), 0u);
    EXPECT_EQ(MatchingBook::band_for(20'000, kBudget), 0u);
    // Always a power of two, whatever the division produces.
    EXPECT_EQ(MatchingBook::band_for(100, kBudget), 4'096u);
}

} // namespace
} // namespace mdh::exchange
