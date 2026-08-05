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
    MatchingEngine engine;
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
    MatchingEngine engine;
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
    MatchingEngine engine;
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

TEST(RiskGatedEngine, CancelAndReplaceBypassRiskCheckEntirely) {
    MatchingEngine engine;
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

TEST(RiskGatedEngine, CrossingOrdersProduceCorrectTradesAndConsistentLedgerState) {
    MatchingEngine engine;
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

} // namespace mdh::exchange::risk
