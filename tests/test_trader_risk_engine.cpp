#include <gtest/gtest.h>

#include "trader/risk/trader_risk_engine.hpp"

// Pure-logic unit tests for TraderRiskEngine -- mirrors
// test_risk_engine.cpp's own structure exactly, checked against a
// PositionTracker instead of an exchange::ledger::Ledger.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::trader::positions;
using namespace mdh::trader::risk;

namespace {
constexpr AccountId kAccount = 1;
constexpr InstrumentId kInstrument = 5;
} // namespace

TEST(TraderRiskEngine, AllowsAnOrderWellWithinLimitsAndFunds) {
    PositionTracker positions;
    positions.deposit_cash(kAccount, 1'000'000);
    TraderRiskEngine risk;

    EXPECT_EQ(risk.check(kAccount, kInstrument, Side::Buy, 100, 10, positions), RejectReason::None);
}

TEST(TraderRiskEngine, RejectsAnOrderLargerThanMaxOrderQuantity) {
    PositionTracker positions;
    positions.deposit_cash(kAccount, 1'000'000'000);
    TraderRiskEngine risk(TraderRiskLimits{.max_order_quantity = 100});

    EXPECT_EQ(risk.check(kAccount, kInstrument, Side::Buy, 1, 101, positions), RejectReason::OrderTooLarge);
}

TEST(TraderRiskEngine, RejectsABuyThatExceedsAvailableCash) {
    PositionTracker positions;
    positions.deposit_cash(kAccount, 500); // not enough for 10 @ 100 = 1000
    TraderRiskEngine risk;

    EXPECT_EQ(risk.check(kAccount, kInstrument, Side::Buy, 100, 10, positions), RejectReason::InsufficientFunds);
}

TEST(TraderRiskEngine, AllowsABuyThatExactlyExhaustsAvailableCash) {
    PositionTracker positions;
    positions.deposit_cash(kAccount, 1'000); // exactly 10 @ 100
    TraderRiskEngine risk;

    EXPECT_EQ(risk.check(kAccount, kInstrument, Side::Buy, 100, 10, positions), RejectReason::None);
}

TEST(TraderRiskEngine, RejectsASellThatExceedsHeldPosition) {
    PositionTracker positions;
    positions.deposit_position(kAccount, kInstrument, 5);
    TraderRiskEngine risk;

    EXPECT_EQ(risk.check(kAccount, kInstrument, Side::Sell, 100, 10, positions), RejectReason::InsufficientPosition);
}

TEST(TraderRiskEngine, AllowsASellCoveredByHeldPosition) {
    PositionTracker positions;
    positions.deposit_position(kAccount, kInstrument, 10);
    TraderRiskEngine risk;

    EXPECT_EQ(risk.check(kAccount, kInstrument, Side::Sell, 100, 10, positions), RejectReason::None);
}

TEST(TraderRiskEngine, ASellNeverChecksCashRegardlessOfBalance) {
    PositionTracker positions; // no cash deposited at all
    positions.deposit_position(kAccount, kInstrument, 10);
    TraderRiskEngine risk;

    EXPECT_EQ(risk.check(kAccount, kInstrument, Side::Sell, 100, 10, positions), RejectReason::None);
}

TEST(TraderRiskEngine, ABuyNeverChecksPositionRegardlessOfHoldings) {
    PositionTracker positions; // no position held at all
    positions.deposit_cash(kAccount, 1'000'000);
    TraderRiskEngine risk;

    EXPECT_EQ(risk.check(kAccount, kInstrument, Side::Buy, 100, 10, positions), RejectReason::None);
}
