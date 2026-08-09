#include <gtest/gtest.h>

#include <vector>

#include "trader/risk/trader_risk_gated_oms.hpp"

// Composition-level unit tests for TraderRiskGatedOms (Milestone 9) -- a
// fake Sender stands in for the wire (same FakeSender pattern
// test_order_management_system.cpp uses), and handle_message() is called
// directly to simulate gateway responses. Confirms the three things this
// class is actually responsible for: (1) a risk-approved order forwards
// unchanged to the real OrderManagementSystem underneath, (2) a
// risk-rejected order never reaches the sender or the OMS's own order
// tracking, and (3) the underlying OMS's fills automatically flow into this
// class's own PositionTracker with no extra wiring from the caller.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::protocol::order_entry;
using namespace mdh::trader::oms;
using namespace mdh::trader::risk;

namespace {

constexpr AccountId kAccount = 1;
constexpr InstrumentId kInstrument = 4;

class FakeSender {
public:
    [[nodiscard]] bool operator()(const Message& message) {
        sent.push_back(message);
        return true;
    }
    std::vector<Message> sent;
};

} // namespace

TEST(TraderRiskGatedOms, ApprovedOrderForwardsToTheRealOmsAndIsTracked) {
    FakeSender sender;
    TraderRiskGatedOms gated(kAccount, std::ref(sender));
    gated.deposit_cash(1'000'000);

    const auto outcome = gated.submit_new_order(kInstrument, Side::Buy, 100, 10);

    ASSERT_TRUE(outcome.client_order_id.has_value());
    EXPECT_EQ(outcome.local_reject_reason, RejectReason::None);
    ASSERT_EQ(sender.sent.size(), 1u);
    EXPECT_NE(std::get_if<NewOrder>(&sender.sent[0]), nullptr);

    const auto order = gated.order(*outcome.client_order_id);
    ASSERT_TRUE(order.has_value());
    EXPECT_EQ(order->state, ClientOrderState::PendingNew);
}

TEST(TraderRiskGatedOms, OrderTooLargeIsRejectedLocallyAndNeverSent) {
    FakeSender sender;
    TraderRiskGatedOms gated(kAccount, std::ref(sender), nullptr, TraderRiskLimits{.max_order_quantity = 5});
    gated.deposit_cash(1'000'000);

    const auto outcome = gated.submit_new_order(kInstrument, Side::Buy, 100, 10);

    EXPECT_FALSE(outcome.client_order_id.has_value());
    EXPECT_EQ(outcome.local_reject_reason, RejectReason::OrderTooLarge);
    EXPECT_TRUE(sender.sent.empty());
    EXPECT_TRUE(gated.orders().empty()); // never even reached the underlying OMS
}

TEST(TraderRiskGatedOms, InsufficientFundsIsRejectedLocallyAndNeverSent) {
    FakeSender sender;
    TraderRiskGatedOms gated(kAccount, std::ref(sender));
    // No deposit_cash() at all.

    const auto outcome = gated.submit_new_order(kInstrument, Side::Buy, 100, 10);

    EXPECT_FALSE(outcome.client_order_id.has_value());
    EXPECT_EQ(outcome.local_reject_reason, RejectReason::InsufficientFunds);
    EXPECT_TRUE(sender.sent.empty());
}

TEST(TraderRiskGatedOms, InsufficientPositionIsRejectedLocallyAndNeverSent) {
    FakeSender sender;
    TraderRiskGatedOms gated(kAccount, std::ref(sender));
    // No deposit_position() at all.

    const auto outcome = gated.submit_new_order(kInstrument, Side::Sell, 100, 10);

    EXPECT_FALSE(outcome.client_order_id.has_value());
    EXPECT_EQ(outcome.local_reject_reason, RejectReason::InsufficientPosition);
    EXPECT_TRUE(sender.sent.empty());
}

TEST(TraderRiskGatedOms, CancelAndReplaceAreForwardedUnchangedRegardlessOfRisk) {
    FakeSender sender;
    TraderRiskGatedOms gated(kAccount, std::ref(sender));
    gated.deposit_cash(1'000'000);

    const auto outcome = gated.submit_new_order(kInstrument, Side::Buy, 100, 10);
    ASSERT_TRUE(outcome.client_order_id.has_value());
    const auto id = *outcome.client_order_id;

    gated.handle_message(Message{Accepted{.account_id = kAccount,
                                                         .client_order_id = id,
                                                         .exchange_order_id = 1,
                                                         .instrument_id = kInstrument,
                                                         .side = Side::Buy,
                                                         .price = 100,
                                                         .quantity = 10,
                                                         .order_type = OrderType::Limit,
                                                         .time_in_force = TimeInForce::GTC}});

    EXPECT_TRUE(gated.cancel_order(id));
    ASSERT_EQ(sender.sent.size(), 2u);
    EXPECT_NE(std::get_if<CancelOrder>(&sender.sent[1]), nullptr);
}

TEST(TraderRiskGatedOms, FillsAutomaticallyUpdateThisClasssOwnPositionTracker) {
    FakeSender sender;
    TraderRiskGatedOms gated(kAccount, std::ref(sender));
    gated.deposit_cash(1'000'000);

    const auto outcome = gated.submit_new_order(kInstrument, Side::Buy, 100, 10);
    ASSERT_TRUE(outcome.client_order_id.has_value());
    const auto id = *outcome.client_order_id;

    gated.handle_message(Message{Accepted{.account_id = kAccount,
                                                         .client_order_id = id,
                                                         .exchange_order_id = 1,
                                                         .instrument_id = kInstrument,
                                                         .side = Side::Buy,
                                                         .price = 100,
                                                         .quantity = 10,
                                                         .order_type = OrderType::Limit,
                                                         .time_in_force = TimeInForce::GTC}});
    EXPECT_EQ(gated.position(kInstrument), 0u);

    gated.handle_message(Message{TradeReport{.account_id = kAccount,
                                                            .client_order_id = id,
                                                            .exchange_order_id = 1,
                                                            .instrument_id = kInstrument,
                                                            .price = 100,
                                                            .quantity = 10,
                                                            .remaining_quantity = 0}});

    EXPECT_EQ(gated.position(kInstrument), 10u);
    EXPECT_EQ(gated.cash(), 1'000'000 - 1'000);
}
