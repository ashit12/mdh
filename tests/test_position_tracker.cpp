#include <gtest/gtest.h>

#include "trader/positions/position_tracker.hpp"

// Pure-logic unit tests for PositionTracker (Milestone 9) -- fed synthetic
// oms::Fill values directly, no OrderManagementSystem or socket involved,
// mirroring exactly how test_ledger.cpp exercises exchange::ledger::Ledger
// with synthetic ExchangeEvents.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::trader::oms;
using namespace mdh::trader::positions;

namespace {
constexpr AccountId kAccount = 1;
constexpr InstrumentId kInstrument = 5;
} // namespace

TEST(PositionTracker, UnknownAccountReadsAsAllZero) {
    PositionTracker tracker;
    EXPECT_EQ(tracker.cash(kAccount), 0);
    EXPECT_EQ(tracker.position(kAccount, kInstrument), 0u);
    EXPECT_EQ(tracker.account(kAccount).cash, 0);
    EXPECT_TRUE(tracker.account(kAccount).holdings.empty());
}

TEST(PositionTracker, DepositsSeedInitialBalances) {
    PositionTracker tracker;
    tracker.deposit_cash(kAccount, 10'000);
    tracker.deposit_position(kAccount, kInstrument, 50);

    EXPECT_EQ(tracker.cash(kAccount), 10'000);
    EXPECT_EQ(tracker.position(kAccount, kInstrument), 50u);
}

TEST(PositionTracker, BuyFillDebitsCashAndIncreasesHoldings) {
    PositionTracker tracker;
    tracker.deposit_cash(kAccount, 10'000);

    tracker.apply(Fill{.account_id = kAccount,
                        .client_order_id = 1,
                        .instrument_id = kInstrument,
                        .side = Side::Buy,
                        .price = 100,
                        .quantity = 10});

    EXPECT_EQ(tracker.cash(kAccount), 10'000 - 1'000); // 100 * 10
    EXPECT_EQ(tracker.position(kAccount, kInstrument), 10u);
}

TEST(PositionTracker, SellFillCreditsCashAndDecreasesHoldings) {
    PositionTracker tracker;
    tracker.deposit_position(kAccount, kInstrument, 20);

    tracker.apply(Fill{.account_id = kAccount,
                        .client_order_id = 1,
                        .instrument_id = kInstrument,
                        .side = Side::Sell,
                        .price = 50,
                        .quantity = 4});

    EXPECT_EQ(tracker.cash(kAccount), 200); // 50 * 4
    EXPECT_EQ(tracker.position(kAccount, kInstrument), 16u);
}

TEST(PositionTracker, MultipleFillsAccumulate) {
    PositionTracker tracker;
    tracker.deposit_cash(kAccount, 1'000'000);

    tracker.apply(Fill{.account_id = kAccount,
                        .client_order_id = 1,
                        .instrument_id = kInstrument,
                        .side = Side::Buy,
                        .price = 100,
                        .quantity = 4});
    tracker.apply(Fill{.account_id = kAccount,
                        .client_order_id = 1,
                        .instrument_id = kInstrument,
                        .side = Side::Buy,
                        .price = 100,
                        .quantity = 6});

    EXPECT_EQ(tracker.position(kAccount, kInstrument), 10u);
    EXPECT_EQ(tracker.cash(kAccount), 1'000'000 - 1'000);
}

TEST(PositionTracker, DifferentAccountsAndInstrumentsAreIndependent) {
    PositionTracker tracker;
    constexpr AccountId kOtherAccount = 2;
    constexpr InstrumentId kOtherInstrument = 6;

    tracker.apply(Fill{.account_id = kAccount,
                        .client_order_id = 1,
                        .instrument_id = kInstrument,
                        .side = Side::Sell,
                        .price = 10,
                        .quantity = 1});
    tracker.apply(Fill{.account_id = kOtherAccount,
                        .client_order_id = 2,
                        .instrument_id = kOtherInstrument,
                        .side = Side::Sell,
                        .price = 999,
                        .quantity = 999});

    EXPECT_EQ(tracker.cash(kAccount), 10);
    EXPECT_EQ(tracker.position(kAccount, kOtherInstrument), 0u);
    EXPECT_NE(tracker.cash(kOtherAccount), 10);
}
