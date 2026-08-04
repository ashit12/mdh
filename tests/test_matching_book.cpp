#include <gtest/gtest.h>

#include "exchange/matching/matching_book.hpp"

namespace mdh::exchange {
namespace {

ExchangeRestingOrder make_order(ExchangeOrderId id, Side side, Price price, Quantity qty,
                                 ClientOrderId client_order_id = 1, AccountId account_id = 1) {
    return ExchangeRestingOrder{
        .exchange_order_id = id,
        .client_order_id = client_order_id,
        .account_id = account_id,
        .instrument_id = 1,
        .side = side,
        .price = price,
        .original_quantity = qty,
        .remaining_quantity = qty,
        .time_in_force = TimeInForce::GTC,
        .order_sequence = id,
    };
}

TEST(MatchingBook, AddThenFindOnEmptyBook) {
    MatchingBook book;
    EXPECT_FALSE(book.find(1).has_value());

    book.add(make_order(1, Side::Buy, 100, 10));
    auto found = book.find(1);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->price, 100);
    EXPECT_EQ(found->remaining_quantity, 10u);
}

TEST(MatchingBook, FifoOrderWithinOneLevel) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, 100, 5));
    book.add(make_order(2, Side::Buy, 100, 7));
    book.add(make_order(3, Side::Buy, 100, 9));

    auto front = book.front_of_best(Side::Buy);
    ASSERT_TRUE(front.has_value());
    EXPECT_EQ(front->exchange_order_id, 1u); // first-added is first in FIFO

    book.remove_front(Side::Buy);
    front = book.front_of_best(Side::Buy);
    ASSERT_TRUE(front.has_value());
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

TEST(MatchingBook, RemoveByIdWorksFromAnyFifoPositionNotJustFront) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, 100, 5));
    book.add(make_order(2, Side::Buy, 100, 7));
    book.add(make_order(3, Side::Buy, 100, 9));

    auto removed = book.remove(2); // middle of the FIFO queue
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(removed->exchange_order_id, 2u);
    EXPECT_FALSE(book.find(2).has_value());

    // Remaining FIFO order is 1 then 3.
    auto all = book.all_bids();
    ASSERT_EQ(all.size(), 2u);
    EXPECT_EQ(all[0].exchange_order_id, 1u);
    EXPECT_EQ(all[1].exchange_order_id, 3u);
}

TEST(MatchingBook, RemoveUnknownIdReturnsNullopt) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, 100, 5));
    EXPECT_FALSE(book.remove(999).has_value());
}

TEST(MatchingBook, EmptyLevelIsClearedAfterLastOrderRemoved) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, 100, 5));
    EXPECT_TRUE(book.best_bid_price().has_value());

    book.remove(1);
    EXPECT_FALSE(book.best_bid_price().has_value());
}

TEST(MatchingBook, EmptyLevelIsClearedAfterRemoveFrontOfLastOrder) {
    MatchingBook book;
    book.add(make_order(1, Side::Sell, 100, 5));
    book.remove_front(Side::Sell);
    EXPECT_FALSE(book.best_ask_price().has_value());
    EXPECT_FALSE(book.find(1).has_value());
}

TEST(MatchingBook, ReduceInPlacePreservesFifoPosition) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, 100, 5));
    book.add(make_order(2, Side::Buy, 100, 7));

    EXPECT_TRUE(book.reduce(1, 2));
    auto found = book.find(1);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->remaining_quantity, 2u);

    // Still first in FIFO despite the mutation.
    auto front = book.front_of_best(Side::Buy);
    ASSERT_TRUE(front.has_value());
    EXPECT_EQ(front->exchange_order_id, 1u);
}

TEST(MatchingBook, ReduceUnknownIdReturnsFalse) {
    MatchingBook book;
    EXPECT_FALSE(book.reduce(999, 1));
}

TEST(MatchingBook, SetClientOrderIdUpdatesInPlaceWithoutChangingFifoPosition) {
    MatchingBook book;
    book.add(make_order(1, Side::Buy, 100, 5, /*client_order_id=*/1));
    book.add(make_order(2, Side::Buy, 100, 7, /*client_order_id=*/2));

    EXPECT_TRUE(book.set_client_order_id(1, 99));
    auto found = book.find(1);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->client_order_id, 99u);
    EXPECT_EQ(found->exchange_order_id, 1u); // exchange_order_id never changes

    // Still first in FIFO despite the mutation.
    auto front = book.front_of_best(Side::Buy);
    ASSERT_TRUE(front.has_value());
    EXPECT_EQ(front->exchange_order_id, 1u);
}

TEST(MatchingBook, SetClientOrderIdUnknownIdReturnsFalse) {
    MatchingBook book;
    EXPECT_FALSE(book.set_client_order_id(999, 1));
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

TEST(MatchingBook, FrontOfBestOnEmptySideReturnsNullopt) {
    MatchingBook book;
    EXPECT_FALSE(book.front_of_best(Side::Buy).has_value());
    EXPECT_FALSE(book.front_of_best(Side::Sell).has_value());
}

} // namespace
} // namespace mdh::exchange
