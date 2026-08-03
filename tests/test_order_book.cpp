#include <gtest/gtest.h>

#include "book/book_manager.hpp"
#include "book/order_book.hpp"

using namespace mdh;
using namespace mdh::book;

TEST(OrderBook, BestBidAndAskAfterAdds) {
    OrderBook book;
    EXPECT_FALSE(book.add_order(1, 100, 10, Side::Buy).has_value());
    EXPECT_FALSE(book.add_order(2, 105, 5, Side::Buy).has_value());
    EXPECT_FALSE(book.add_order(3, 110, 8, Side::Sell).has_value());
    EXPECT_FALSE(book.add_order(4, 115, 2, Side::Sell).has_value());

    auto bid = book.best_bid();
    ASSERT_TRUE(bid.has_value());
    EXPECT_EQ(bid->price, 105); // highest bid wins

    auto ask = book.best_ask();
    ASSERT_TRUE(ask.has_value());
    EXPECT_EQ(ask->price, 110); // lowest ask wins
}

TEST(OrderBook, AggregatesQuantityAtSamePriceLevel) {
    OrderBook book;
    ASSERT_FALSE(book.add_order(1, 100, 10, Side::Buy).has_value());
    ASSERT_FALSE(book.add_order(2, 100, 5, Side::Buy).has_value());
    ASSERT_FALSE(book.add_order(3, 100, 3, Side::Buy).has_value());

    auto bid = book.best_bid();
    ASSERT_TRUE(bid.has_value());
    EXPECT_EQ(bid->price, 100);
    EXPECT_EQ(bid->aggregate_quantity, 18u);
    EXPECT_EQ(bid->order_count, 3u);
}

TEST(OrderBook, TopNLevelsOrderedCorrectlyForBothSides) {
    OrderBook book;
    ASSERT_FALSE(book.add_order(1, 100, 1, Side::Buy).has_value());
    ASSERT_FALSE(book.add_order(2, 102, 1, Side::Buy).has_value());
    ASSERT_FALSE(book.add_order(3, 101, 1, Side::Buy).has_value());
    ASSERT_FALSE(book.add_order(4, 200, 1, Side::Sell).has_value());
    ASSERT_FALSE(book.add_order(5, 198, 1, Side::Sell).has_value());
    ASSERT_FALSE(book.add_order(6, 199, 1, Side::Sell).has_value());

    auto top_bids = book.top_bids(2);
    ASSERT_EQ(top_bids.size(), 2u);
    EXPECT_EQ(top_bids[0].price, 102); // highest first
    EXPECT_EQ(top_bids[1].price, 101);

    auto top_asks = book.top_asks(2);
    ASSERT_EQ(top_asks.size(), 2u);
    EXPECT_EQ(top_asks[0].price, 198); // lowest first
    EXPECT_EQ(top_asks[1].price, 199);
}

TEST(OrderBook, TopNRequestBiggerThanBookReturnsWhatExists) {
    OrderBook book;
    ASSERT_FALSE(book.add_order(1, 100, 1, Side::Buy).has_value());
    auto top = book.top_bids(10);
    EXPECT_EQ(top.size(), 1u);
}

TEST(OrderBook, CancelRemovesOrderAndEmptiesLevel) {
    OrderBook book;
    ASSERT_FALSE(book.add_order(1, 100, 10, Side::Buy).has_value());
    EXPECT_FALSE(book.cancel_order(1).has_value());
    EXPECT_FALSE(book.best_bid().has_value());
    EXPECT_FALSE(book.has_order(1));
}

TEST(OrderBook, CancelOneOrderLeavesOthersAtSameLevel) {
    OrderBook book;
    ASSERT_FALSE(book.add_order(1, 100, 10, Side::Buy).has_value());
    ASSERT_FALSE(book.add_order(2, 100, 5, Side::Buy).has_value());
    EXPECT_FALSE(book.cancel_order(1).has_value());

    auto bid = book.best_bid();
    ASSERT_TRUE(bid.has_value());
    EXPECT_EQ(bid->aggregate_quantity, 5u);
    EXPECT_EQ(bid->order_count, 1u);
}

TEST(OrderBook, CancelUnknownOrderIsRejected) {
    OrderBook book;
    auto err = book.cancel_order(999);
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(*err, BookError::UnknownOrderId);
}

TEST(OrderBook, ModifyUnknownOrderIsRejected) {
    OrderBook book;
    auto err = book.modify_order(999, 100, 1);
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(*err, BookError::UnknownOrderId);
}

TEST(OrderBook, ModifyChangesPriceAndQuantity) {
    OrderBook book;
    ASSERT_FALSE(book.add_order(1, 100, 10, Side::Buy).has_value());
    EXPECT_FALSE(book.modify_order(1, 120, 3).has_value());

    auto bid = book.best_bid();
    ASSERT_TRUE(bid.has_value());
    EXPECT_EQ(bid->price, 120);
    EXPECT_EQ(bid->aggregate_quantity, 3u);
}

TEST(OrderBook, DuplicateOrderIdIsRejected) {
    OrderBook book;
    ASSERT_FALSE(book.add_order(1, 100, 10, Side::Buy).has_value());
    auto err = book.add_order(1, 200, 5, Side::Sell);
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(*err, BookError::DuplicateOrderId);
}

TEST(OrderBook, ZeroOrNegativePriceIsRejected) {
    OrderBook book;
    EXPECT_EQ(*book.add_order(1, 0, 10, Side::Buy), BookError::InvalidPrice);
    EXPECT_EQ(*book.add_order(2, -5, 10, Side::Buy), BookError::InvalidPrice);
}

TEST(OrderBook, ZeroQuantityIsRejected) {
    OrderBook book;
    EXPECT_EQ(*book.add_order(1, 100, 0, Side::Buy), BookError::InvalidQuantity);
}

TEST(OrderBook, ClearBookRemovesAllOrdersAndAllowsIdReuse) {
    OrderBook book;
    ASSERT_FALSE(book.add_order(1, 100, 10, Side::Buy).has_value());
    ASSERT_FALSE(book.add_order(2, 200, 5, Side::Sell).has_value());
    book.clear();

    EXPECT_FALSE(book.best_bid().has_value());
    EXPECT_FALSE(book.best_ask().has_value());
    EXPECT_FALSE(book.has_order(1));
    // Same id can be reused post-clear since the index was wiped too.
    EXPECT_FALSE(book.add_order(1, 150, 1, Side::Buy).has_value());
}

TEST(OrderBook, AllBidsAndAllAsksEnumerateEveryOrderNotJustTopLevels) {
    OrderBook book;
    // Two orders at the same price (FIFO within a level) plus enough
    // distinct price levels that top_bids/top_asks with a small n would
    // miss some of these -- all_bids()/all_asks() must not.
    ASSERT_FALSE(book.add_order(1, 100, 10, Side::Buy).has_value());
    ASSERT_FALSE(book.add_order(2, 100, 5, Side::Buy).has_value()); // same price as order 1
    ASSERT_FALSE(book.add_order(3, 105, 7, Side::Buy).has_value());
    ASSERT_FALSE(book.add_order(4, 200, 3, Side::Sell).has_value());
    ASSERT_FALSE(book.add_order(5, 205, 9, Side::Sell).has_value());

    auto bids = book.all_bids();
    ASSERT_EQ(bids.size(), 3u);
    // Order within a level matches FIFO add order; levels are visited in
    // the same price ordering as top_bids (highest first).
    EXPECT_EQ(bids[0].order_id, 3u);
    EXPECT_EQ(bids[0].price, 105);
    EXPECT_EQ(bids[1].order_id, 1u);
    EXPECT_EQ(bids[1].price, 100);
    EXPECT_EQ(bids[1].quantity, 10u);
    EXPECT_EQ(bids[2].order_id, 2u);
    EXPECT_EQ(bids[2].price, 100);
    EXPECT_EQ(bids[2].quantity, 5u);

    auto asks = book.all_asks();
    ASSERT_EQ(asks.size(), 2u);
    EXPECT_EQ(asks[0].order_id, 4u);
    EXPECT_EQ(asks[0].price, 200);
    EXPECT_EQ(asks[1].order_id, 5u);
    EXPECT_EQ(asks[1].price, 205);
}

TEST(OrderBook, AllBidsAndAllAsksAreEmptyForAnEmptyBook) {
    OrderBook book;
    EXPECT_TRUE(book.all_bids().empty());
    EXPECT_TRUE(book.all_asks().empty());
}

TEST(BookManagerTest, InstrumentsAreIndependent) {
    BookManager mgr;
    ASSERT_FALSE(mgr.book_for(1).add_order(1, 100, 10, Side::Buy).has_value());
    ASSERT_FALSE(mgr.book_for(2).add_order(1, 500, 1, Side::Buy).has_value()); // same order id, different instrument

    EXPECT_EQ(mgr.book_for(1).best_bid()->price, 100);
    EXPECT_EQ(mgr.book_for(2).best_bid()->price, 500);
}

TEST(BookManagerTest, TradeStatsAccumulatePerInstrument) {
    BookManager mgr;
    mgr.record_trade(1, 100, 5);
    mgr.record_trade(1, 105, 3);
    mgr.record_trade(2, 900, 1);

    const auto* s1 = mgr.trade_stats(1);
    ASSERT_NE(s1, nullptr);
    EXPECT_EQ(s1->trade_count, 2u);
    EXPECT_EQ(s1->traded_quantity, 8u);
    EXPECT_EQ(s1->last_trade_price, 105);

    const auto* s2 = mgr.trade_stats(2);
    ASSERT_NE(s2, nullptr);
    EXPECT_EQ(s2->trade_count, 1u);

    EXPECT_EQ(mgr.trade_stats(999), nullptr);
}

TEST(BookManagerTest, InstrumentsListsEverythingSeen) {
    BookManager mgr;
    (void)mgr.book_for(3);
    mgr.record_trade(7, 1, 1);

    auto ids = mgr.instruments();
    ASSERT_EQ(ids.size(), 2u);
    EXPECT_EQ(ids[0], 3u);
    EXPECT_EQ(ids[1], 7u);
}
