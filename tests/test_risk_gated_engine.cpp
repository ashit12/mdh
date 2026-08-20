#include <gtest/gtest.h>

#include <variant>
#include <vector>

#include "exchange/risk/risk_gated_engine.hpp"

namespace mdh::exchange::risk {
namespace {

constexpr InstrumentId kInstrument = 1;
constexpr AccountId kBuyer = 100;
constexpr AccountId kSeller = 200;

NewOrderCommand buy(CommandSequence seq, ClientOrderId client_id, Price price, Quantity qty,
                     TimeInForce tif = TimeInForce::GTC) {
    return NewOrderCommand{
        .command_sequence = seq,
        .account_id = kBuyer,
        .client_order_id = client_id,
        .instrument_id = kInstrument,
        .side = Side::Buy,
        .price = price,
        .quantity = qty,
        .order_type = OrderType::Limit,
        .time_in_force = tif,
    };
}

class CollectingSink {
public:
    EventSink sink() {
        return [this](const ExchangeEvent& ev) { events.push_back(ev); };
    }

    template <class T>
    [[nodiscard]] const T& at(std::size_t index) const {
        return std::get<T>(events.at(index));
    }

    std::vector<ExchangeEvent> events;
};

template <class T>
bool holds(const ExchangeEvent& ev) {
    return std::holds_alternative<T>(ev);
}

} // namespace

TEST(RiskGatedEngine, InsufficientFundsRejectsBeforeTouchingEngineOrLedger) {
    MatchingEngine engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_cash(kBuyer, 500); // needs 1,000
    RiskGatedEngine gated(engine, ledger);
    CollectingSink out;

    gated.process(buy(1, 1, 100, 10), out.sink());

    ASSERT_EQ(out.events.size(), 1u);
    ASSERT_TRUE(holds<OrderRejected>(out.events[0]));
    EXPECT_EQ(out.at<OrderRejected>(0).reason, RejectReason::InsufficientFunds);

    EXPECT_TRUE(engine.snapshot().instruments.empty()); // never reached the book
    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 0); // no OrderAccepted ever fired, nothing to reserve
}

TEST(RiskGatedEngine, ApprovedOrderBehavesExactlyLikeTheBareEngineAndUpdatesTheLedger) {
    MatchingEngine engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);
    RiskGatedEngine gated(engine, ledger);
    CollectingSink out;

    gated.process(buy(1, 1, 100, 10), out.sink());

    ASSERT_EQ(out.events.size(), 2u);
    EXPECT_TRUE(holds<OrderAccepted>(out.events[0]));
    EXPECT_TRUE(holds<BookOrderAdded>(out.events[1]));

    EXPECT_EQ(engine.snapshot().instruments.size(), 1u);
    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 1'000);
    EXPECT_EQ(ledger.available_cash(kBuyer), 9'000);
}

TEST(RiskGatedEngine, EventSequenceStaysGaplessAcrossARiskRejectionAndASubsequentApproval) {
    MatchingEngine engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);
    RiskGatedEngine gated(engine, ledger);
    CollectingSink out;

    gated.process(buy(1, 1, 100, 100'000), out.sink()); // rejected: far more than deposited
    gated.process(buy(2, 2, 100, 10), out.sink());       // approved

    ASSERT_EQ(out.events.size(), 3u);
    ASSERT_TRUE(holds<OrderRejected>(out.events[0]));
    ASSERT_TRUE(holds<OrderAccepted>(out.events[1]));
    ASSERT_TRUE(holds<BookOrderAdded>(out.events[2]));

    const auto seq0 = out.at<OrderRejected>(0).event_sequence;
    const auto seq1 = out.at<OrderAccepted>(1).event_sequence;
    const auto seq2 = out.at<BookOrderAdded>(2).event_sequence;
    EXPECT_LT(seq0, seq1);
    EXPECT_LT(seq1, seq2);
}

TEST(RiskGatedEngine, CancelBypassesRiskCheck) {
    MatchingEngine engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);
    RiskGatedEngine gated(engine, ledger);
    CollectingSink out;

    gated.process(buy(1, 1, 100, 10), out.sink());
    gated.process(CancelOrderCommand{.command_sequence = 2, .account_id = kBuyer, .client_order_id = 1, .instrument_id = kInstrument},
                  out.sink());

    ASSERT_EQ(out.events.size(), 4u); // OrderAccepted, BookOrderAdded, OrderCancelled, BookOrderRemoved
    EXPECT_TRUE(holds<OrderCancelled>(out.events[2]));
    EXPECT_TRUE(holds<BookOrderRemoved>(out.events[3]));
    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 0); // released on cancel
}

ReplaceOrderCommand replace_buy(CommandSequence seq, ClientOrderId original, ClientOrderId neu, Price new_price,
                                 Quantity new_qty) {
    return ReplaceOrderCommand{
        .command_sequence = seq,
        .account_id = kBuyer,
        .original_client_order_id = original,
        .new_client_order_id = neu,
        .instrument_id = kInstrument,
        .new_price = new_price,
        .new_quantity = new_qty,
    };
}

TEST(RiskGatedEngine, ReplaceBuyHigherPriceThatFitsAvailableCashSucceedsAndReservesNewNotional) {
    MatchingEngine engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_cash(kBuyer, 1'500);
    RiskGatedEngine gated(engine, ledger);
    CollectingSink out;

    gated.process(buy(1, 1, 100, 10), out.sink()); // reserves 1,000; 500 free
    out.events.clear();

    gated.process(replace_buy(2, 1, 2, 120, 10), out.sink()); // extra 200

    ASSERT_FALSE(out.events.empty());
    EXPECT_TRUE(holds<OrderReplaced>(out.events[0]));
    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 1'200);
    EXPECT_EQ(ledger.available_cash(kBuyer), 300);
    const auto snap = engine.snapshot();
    ASSERT_EQ(snap.instruments.size(), 1u);
    ASSERT_EQ(snap.instruments[0].bids.size(), 1u);
    EXPECT_EQ(snap.instruments[0].bids[0].price, 120);
    EXPECT_EQ(snap.instruments[0].bids[0].remaining_quantity, 10u);
}

TEST(RiskGatedEngine, ReplaceBuyHigherPriceExceedingAvailableCashIsRejectedWithoutTouchingBookOrHold) {
    MatchingEngine engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_cash(kBuyer, 1'200);
    RiskGatedEngine gated(engine, ledger);
    CollectingSink out;

    gated.process(buy(1, 1, 100, 10), out.sink());
    out.events.clear();
    const auto reserved_before = ledger.balances(kBuyer).cash_reserved;
    const auto hold_before = ledger.find_hold(kBuyer, 1);
    ASSERT_TRUE(hold_before.has_value());
    const auto snap_before = engine.snapshot();

    gated.process(replace_buy(2, 1, 2, 150, 10), out.sink()); // extra 500 > 200 free

    ASSERT_EQ(out.events.size(), 1u);
    ASSERT_TRUE(holds<OrderRejected>(out.events[0]));
    EXPECT_EQ(out.at<OrderRejected>(0).reason, RejectReason::InsufficientFunds);
    EXPECT_EQ(out.at<OrderRejected>(0).client_order_id, 1u); // original id

    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, reserved_before);
    EXPECT_EQ(ledger.available_cash(kBuyer), 200);
    const auto hold_after = ledger.find_hold(kBuyer, 1);
    ASSERT_TRUE(hold_after.has_value());
    EXPECT_EQ(hold_after->instrument_id, hold_before->instrument_id);
    EXPECT_EQ(hold_after->side, hold_before->side);
    EXPECT_EQ(hold_after->limit_price, hold_before->limit_price);
    EXPECT_EQ(hold_after->remaining, hold_before->remaining);
    EXPECT_FALSE(ledger.find_hold(kBuyer, 2).has_value());
    EXPECT_EQ(engine.snapshot(), snap_before);
    ASSERT_EQ(snap_before.instruments[0].bids.size(), 1u);
    EXPECT_EQ(snap_before.instruments[0].bids[0].price, 100);
    EXPECT_EQ(snap_before.instruments[0].bids[0].client_order_id, 1u);
}

TEST(RiskGatedEngine, ReplaceBuyLargerQuantityExceedingAvailableCashIsRejectedPreservingOriginalHold) {
    MatchingEngine engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_cash(kBuyer, 1'200);
    RiskGatedEngine gated(engine, ledger);
    CollectingSink out;

    gated.process(buy(1, 1, 100, 10), out.sink());
    out.events.clear();

    gated.process(replace_buy(2, 1, 2, 100, 15), out.sink());

    ASSERT_EQ(out.events.size(), 1u);
    EXPECT_EQ(out.at<OrderRejected>(0).reason, RejectReason::InsufficientFunds);
    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 1'000);
    ASSERT_EQ(engine.snapshot().instruments[0].bids.size(), 1u);
    EXPECT_EQ(engine.snapshot().instruments[0].bids[0].remaining_quantity, 10u);
}

TEST(RiskGatedEngine, ReplaceBuyReducingExposureSucceeds) {
    MatchingEngine engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_cash(kBuyer, 1'000);
    RiskGatedEngine gated(engine, ledger);
    CollectingSink out;

    gated.process(buy(1, 1, 100, 10), out.sink());
    out.events.clear();

    gated.process(replace_buy(2, 1, 2, 100, 5), out.sink());

    ASSERT_TRUE(holds<OrderReplaced>(out.events[0]));
    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 500);
    EXPECT_EQ(ledger.available_cash(kBuyer), 500);
}

TEST(RiskGatedEngine, ReplaceSellIncreasingQuantityBeyondAvailableInventoryIsRejected) {
    MatchingEngine engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_position(kSeller, kInstrument, 20);
    RiskGatedEngine gated(engine, ledger);
    CollectingSink out;

    gated.process(NewOrderCommand{.command_sequence = 1,
                                   .account_id = kSeller,
                                   .client_order_id = 1,
                                   .instrument_id = kInstrument,
                                   .side = Side::Sell,
                                   .price = 100,
                                   .quantity = 10,
                                   .order_type = OrderType::Limit,
                                   .time_in_force = TimeInForce::GTC},
                  out.sink());
    out.events.clear();

    gated.process(ReplaceOrderCommand{.command_sequence = 2,
                                       .account_id = kSeller,
                                       .original_client_order_id = 1,
                                       .new_client_order_id = 2,
                                       .instrument_id = kInstrument,
                                       .new_price = 100,
                                       .new_quantity = 25},
                  out.sink());

    ASSERT_EQ(out.events.size(), 1u);
    EXPECT_EQ(out.at<OrderRejected>(0).reason, RejectReason::InsufficientPosition);
    EXPECT_EQ(ledger.balances(kSeller).position_reserved.at(kInstrument), 10u);
    ASSERT_EQ(engine.snapshot().instruments[0].asks.size(), 1u);
    EXPECT_EQ(engine.snapshot().instruments[0].asks[0].remaining_quantity, 10u);
}

TEST(RiskGatedEngine, ReplaceSellReducingQuantitySucceeds) {
    MatchingEngine engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_position(kSeller, kInstrument, 10);
    RiskGatedEngine gated(engine, ledger);
    CollectingSink out;

    gated.process(NewOrderCommand{.command_sequence = 1,
                                   .account_id = kSeller,
                                   .client_order_id = 1,
                                   .instrument_id = kInstrument,
                                   .side = Side::Sell,
                                   .price = 100,
                                   .quantity = 10,
                                   .order_type = OrderType::Limit,
                                   .time_in_force = TimeInForce::GTC},
                  out.sink());
    out.events.clear();

    gated.process(ReplaceOrderCommand{.command_sequence = 2,
                                       .account_id = kSeller,
                                       .original_client_order_id = 1,
                                       .new_client_order_id = 2,
                                       .instrument_id = kInstrument,
                                       .new_price = 100,
                                       .new_quantity = 4},
                  out.sink());

    ASSERT_TRUE(holds<OrderReplaced>(out.events[0]));
    EXPECT_EQ(ledger.balances(kSeller).position_reserved.at(kInstrument), 4u);
    EXPECT_EQ(ledger.available_position(kSeller, kInstrument), 6u);
}

TEST(RiskGatedEngine, CrossingOrdersProduceCorrectTradesAndConsistentLedgerState) {
    MatchingEngine engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);
    ledger.deposit_position(kSeller, kInstrument, 50);
    RiskGatedEngine gated(engine, ledger);
    CollectingSink out;

    gated.process(NewOrderCommand{.command_sequence = 1,
                                   .account_id = kSeller,
                                   .client_order_id = 1,
                                   .instrument_id = kInstrument,
                                   .side = Side::Sell,
                                   .price = 100,
                                   .quantity = 10,
                                   .order_type = OrderType::Limit,
                                   .time_in_force = TimeInForce::GTC},
                  out.sink());
    gated.process(buy(2, 1, 100, 10), out.sink());

    bool saw_trade = false;
    for (const auto& ev : out.events) {
        if (holds<TradeExecuted>(ev)) {
            saw_trade = true;
            const auto& trade = std::get<TradeExecuted>(ev);
            EXPECT_EQ(trade.price, 100);
            EXPECT_EQ(trade.quantity, 10u);
        }
    }
    EXPECT_TRUE(saw_trade);

    EXPECT_EQ(ledger.balances(kBuyer).cash_total, 10'000 - 1'000);
    EXPECT_EQ(ledger.balances(kSeller).cash_total, 1'000);
    EXPECT_EQ(ledger.balances(kBuyer).position_total.at(kInstrument), 10u);
    EXPECT_EQ(ledger.balances(kSeller).position_total.at(kInstrument), 40u);
    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 0);
    EXPECT_EQ(ledger.balances(kSeller).position_reserved.at(kInstrument), 0u);
}

// The instrument check has to come before the risk check, not after: a sell
// on an instrument the exchange does not trade would otherwise be reported
// as InsufficientPosition, because the account genuinely holds none of it.
// That reads as a funding problem the client could fix by depositing, which
// it is not.
TEST(RiskGatedEngine, SellOnAnUnknownInstrumentReportsTheInstrumentNotThePosition) {
    MatchingEngine engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_position(kSeller, kInstrument, 50);
    RiskGatedEngine gated(engine, ledger);
    CollectingSink out;

    gated.process(NewOrderCommand{.command_sequence = 1,
                                   .account_id = kSeller,
                                   .client_order_id = 1,
                                   .instrument_id = kInstrument + 1,
                                   .side = Side::Sell,
                                   .price = 100,
                                   .quantity = 10,
                                   .order_type = OrderType::Limit,
                                   .time_in_force = TimeInForce::GTC},
                  out.sink());

    ASSERT_EQ(out.events.size(), 1u);
    EXPECT_EQ(out.at<OrderRejected>(0).reason, RejectReason::InvalidInstrument);
    EXPECT_EQ(ledger.available_position(kSeller, kInstrument), 50u); // nothing reserved
}

TEST(RiskGatedEngine, ReplaceOnAnUnknownInstrumentIsRejectedBeforeRisk) {
    MatchingEngine engine{kInstrument};
    ledger::Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);
    RiskGatedEngine gated(engine, ledger);
    CollectingSink out;

    gated.process(ReplaceOrderCommand{.command_sequence = 1,
                                       .account_id = kBuyer,
                                       .original_client_order_id = 1,
                                       .new_client_order_id = 2,
                                       .instrument_id = kInstrument + 1,
                                       .new_price = 100,
                                       .new_quantity = 5},
                  out.sink());

    ASSERT_EQ(out.events.size(), 1u);
    EXPECT_EQ(out.at<OrderRejected>(0).reason, RejectReason::InvalidInstrument);
    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 0);
}

} // namespace mdh::exchange::risk
