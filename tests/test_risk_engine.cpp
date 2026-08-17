#include <gtest/gtest.h>

#include "exchange/core/events.hpp"
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

ReplaceOrderCommand replace_buy(ClientOrderId original, ClientOrderId neu, Price new_price, Quantity new_qty) {
    return ReplaceOrderCommand{
        .command_sequence = 2,
        .account_id = kAccount,
        .original_client_order_id = original,
        .new_client_order_id = neu,
        .instrument_id = kInstrument,
        .new_price = new_price,
        .new_quantity = new_qty,
    };
}

ReplaceOrderCommand replace_sell(ClientOrderId original, ClientOrderId neu, Price new_price, Quantity new_qty) {
    return ReplaceOrderCommand{
        .command_sequence = 2,
        .account_id = kAccount,
        .original_client_order_id = original,
        .new_client_order_id = neu,
        .instrument_id = kInstrument,
        .new_price = new_price,
        .new_quantity = new_qty,
    };
}

void accept_gtc_buy(ledger::Ledger& ledger, ClientOrderId id, Price price, Quantity qty) {
    ledger.apply(OrderAccepted{.event_sequence = 1,
                                .command_sequence = 1,
                                .account_id = kAccount,
                                .client_order_id = id,
                                .exchange_order_id = 1,
                                .instrument_id = kInstrument,
                                .side = Side::Buy,
                                .price = price,
                                .quantity = qty,
                                .order_type = OrderType::Limit,
                                .time_in_force = TimeInForce::GTC});
}

void accept_gtc_sell(ledger::Ledger& ledger, ClientOrderId id, Price price, Quantity qty) {
    ledger.apply(OrderAccepted{.event_sequence = 1,
                                .command_sequence = 1,
                                .account_id = kAccount,
                                .client_order_id = id,
                                .exchange_order_id = 1,
                                .instrument_id = kInstrument,
                                .side = Side::Sell,
                                .price = price,
                                .quantity = qty,
                                .order_type = OrderType::Limit,
                                .time_in_force = TimeInForce::GTC});
}

TEST(RiskEngine, ReplaceBuyHigherPriceThatFitsAvailableCashIsApproved) {
    ledger::Ledger ledger;
    ledger.deposit_cash(kAccount, 1'500); // 1,000 reserved + 500 free
    accept_gtc_buy(ledger, /*id=*/1, /*price=*/100, /*qty=*/10);
    ASSERT_EQ(ledger.available_cash(kAccount), 500);

    RiskEngine risk;
    // 10@120 needs 1,200; extra over the existing 1,000 hold is 200 <= 500 free.
    EXPECT_EQ(risk.check(replace_buy(1, 2, 120, 10), ledger), RejectReason::None);
}

TEST(RiskEngine, ReplaceBuyHigherPriceExceedingAvailableCashIsRejected) {
    ledger::Ledger ledger;
    ledger.deposit_cash(kAccount, 1'200);
    accept_gtc_buy(ledger, 1, 100, 10); // reserves 1,000; 200 free
    ASSERT_EQ(ledger.available_cash(kAccount), 200);

    RiskEngine risk;
    // 10@150 needs 1,500; extra 500 > 200 free. Must NOT require the full
    // 1,500 as fresh available -- only the delta -- but even the delta fails.
    EXPECT_EQ(risk.check(replace_buy(1, 2, 150, 10), ledger), RejectReason::InsufficientFunds);
}

TEST(RiskEngine, ReplaceBuyLargerQuantityExceedingAvailableCashIsRejected) {
    ledger::Ledger ledger;
    ledger.deposit_cash(kAccount, 1'200);
    accept_gtc_buy(ledger, 1, 100, 10); // reserves 1,000; 200 free

    RiskEngine risk;
    // 15@100 needs 1,500; extra 500 > 200 free.
    EXPECT_EQ(risk.check(replace_buy(1, 2, 100, 15), ledger), RejectReason::InsufficientFunds);
}

TEST(RiskEngine, ReplaceBuyReducingExposurePassesWithoutExtraCash) {
    ledger::Ledger ledger;
    ledger.deposit_cash(kAccount, 1'000);
    accept_gtc_buy(ledger, 1, 100, 10); // fully reserved; 0 free
    ASSERT_EQ(ledger.available_cash(kAccount), 0);

    RiskEngine risk;
    EXPECT_EQ(risk.check(replace_buy(1, 2, 50, 10), ledger), RejectReason::None); // lower price
    EXPECT_EQ(risk.check(replace_buy(1, 2, 100, 5), ledger), RejectReason::None); // lower qty
}

TEST(RiskEngine, ReplaceSellIncreasingQuantityBeyondAvailableInventoryIsRejected) {
    ledger::Ledger ledger;
    ledger.deposit_position(kAccount, kInstrument, 20);
    accept_gtc_sell(ledger, 1, 100, 10); // 10 reserved; 10 free

    RiskEngine risk;
    // 25 needs 15 extra > 10 free.
    EXPECT_EQ(risk.check(replace_sell(1, 2, 100, 25), ledger), RejectReason::InsufficientPosition);
}

TEST(RiskEngine, ReplaceSellReducingQuantityPasses) {
    ledger::Ledger ledger;
    ledger.deposit_position(kAccount, kInstrument, 10);
    accept_gtc_sell(ledger, 1, 100, 10); // fully reserved; 0 free

    RiskEngine risk;
    EXPECT_EQ(risk.check(replace_sell(1, 2, 100, 5), ledger), RejectReason::None);
}

TEST(RiskEngine, ReplaceCreditsExistingHoldRatherThanRequiringFullNewExposure) {
    ledger::Ledger ledger;
    // Only 100 free beyond the existing 1,000 hold -- enough for a small
    // bump, nowhere near enough if the check wrongly demanded the full
    // new notional from available.
    ledger.deposit_cash(kAccount, 1'100);
    accept_gtc_buy(ledger, 1, 100, 10);
    ASSERT_EQ(ledger.available_cash(kAccount), 100);

    RiskEngine risk;
    EXPECT_EQ(risk.check(replace_buy(1, 2, 110, 10), ledger), RejectReason::None); // extra 100
    EXPECT_EQ(risk.check(replace_buy(1, 2, 111, 10), ledger), RejectReason::InsufficientFunds); // extra 110
}

TEST(RiskEngine, ReplaceWithNoExistingHoldDefersToMatchingEngine) {
    ledger::Ledger ledger;
    ledger.deposit_cash(kAccount, 10'000);
    RiskEngine risk;
    // No OrderAccepted ever fired for id 1 -- risk returns None; the engine
    // will reject with UnknownOrderId when process_replace runs.
    EXPECT_EQ(risk.check(replace_buy(1, 2, 100, 10), ledger), RejectReason::None);
}

} // namespace mdh::exchange::risk
