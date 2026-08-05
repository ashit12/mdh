#include <gtest/gtest.h>

#include "exchange/ledger/ledger.hpp"
#include "exchange/risk/risk_engine.hpp"

namespace mdh::exchange::risk {
namespace {

constexpr InstrumentId kInstrument = 1;
constexpr AccountId kAccount = 100;

NewOrderCommand buy(Price price, Quantity qty) {
    return NewOrderCommand{
        .command_sequence = 1,
        .account_id = kAccount,
        .client_order_id = 1,
        .instrument_id = kInstrument,
        .side = Side::Buy,
        .price = price,
        .quantity = qty,
        .order_type = OrderType::Limit,
        .time_in_force = TimeInForce::GTC,
    };
}

NewOrderCommand sell(Price price, Quantity qty) {
    return NewOrderCommand{
        .command_sequence = 1,
        .account_id = kAccount,
        .client_order_id = 1,
        .instrument_id = kInstrument,
        .side = Side::Sell,
        .price = price,
        .quantity = qty,
        .order_type = OrderType::Limit,
        .time_in_force = TimeInForce::GTC,
    };
}

} // namespace

TEST(RiskEngine, ApprovesBuyWithSufficientCash) {
    ledger::Ledger ledger;
    ledger.deposit_cash(kAccount, 10'000);
    RiskEngine risk;
    EXPECT_EQ(risk.check(buy(100, 10), ledger), RejectReason::None);
}

TEST(RiskEngine, RejectsBuyWithInsufficientCash) {
    ledger::Ledger ledger;
    ledger.deposit_cash(kAccount, 500); // needs 1,000 for 100 * 10
    RiskEngine risk;
    EXPECT_EQ(risk.check(buy(100, 10), ledger), RejectReason::InsufficientFunds);
}

TEST(RiskEngine, ExactlyEnoughCashIsApprovedNotRejected) {
    ledger::Ledger ledger;
    ledger.deposit_cash(kAccount, 1'000);
    RiskEngine risk;
    EXPECT_EQ(risk.check(buy(100, 10), ledger), RejectReason::None);
}

TEST(RiskEngine, ApprovesSellWithSufficientPosition) {
    ledger::Ledger ledger;
    ledger.deposit_position(kAccount, kInstrument, 20);
    RiskEngine risk;
    EXPECT_EQ(risk.check(sell(100, 10), ledger), RejectReason::None);
}

TEST(RiskEngine, RejectsSellWithInsufficientPosition) {
    ledger::Ledger ledger;
    ledger.deposit_position(kAccount, kInstrument, 5);
    RiskEngine risk;
    EXPECT_EQ(risk.check(sell(100, 10), ledger), RejectReason::InsufficientPosition);
}

TEST(RiskEngine, RejectsSellWithNoPositionAtAll) {
    ledger::Ledger ledger; // never deposited anything for this account/instrument
    RiskEngine risk;
    EXPECT_EQ(risk.check(sell(100, 1), ledger), RejectReason::InsufficientPosition);
}

TEST(RiskEngine, AlreadyReservedBalanceIsNotAvailableToASecondOrder) {
    ledger::Ledger ledger;
    ledger.deposit_cash(kAccount, 1'000);
    ledger.apply(OrderAccepted{.event_sequence = 1,
                                .command_sequence = 1,
                                .account_id = kAccount,
                                .client_order_id = 1,
                                .exchange_order_id = 1,
                                .instrument_id = kInstrument,
                                .side = Side::Buy,
                                .price = 100,
                                .quantity = 10, // reserves the entire 1,000
                                .order_type = OrderType::Limit,
                                .time_in_force = TimeInForce::GTC});

    RiskEngine risk;
    // A second order competing for the same, already-reserved funds must be
    // rejected -- this is the whole point of reservation semantics (see
    // ledger.hpp's class comment): two GTC orders must not each
    // independently pass a check against the same unspent cash.
    EXPECT_EQ(risk.check(buy(100, 1), ledger), RejectReason::InsufficientFunds);
}

TEST(RiskEngine, RejectsOrderExceedingMaxQuantity) {
    ledger::Ledger ledger;
    ledger.deposit_cash(kAccount, 1'000'000'000);
    RiskEngine risk(RiskLimits{.max_order_quantity = 100});
    EXPECT_EQ(risk.check(buy(1, 101), ledger), RejectReason::OrderTooLarge);
}

TEST(RiskEngine, OrderTooLargeIsCheckedBeforeBalanceSufficiency) {
    ledger::Ledger ledger; // no deposit at all: would also fail the balance check
    RiskEngine risk(RiskLimits{.max_order_quantity = 100});
    EXPECT_EQ(risk.check(buy(1, 101), ledger), RejectReason::OrderTooLarge);
}

} // namespace mdh::exchange::risk
