#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <vector>

#include "trader/oms/order_management_system.hpp"

// Pure-logic unit tests for OrderManagementSystem's state machine -- no
// real socket or gateway involved. `Sender` is faked here with a
// plain vector-capturing lambda (same "std::function fake stands in for the
// real transport" pattern MatchingEngine's own unit tests use for
// EventSink), and gateway responses are simulated by calling
// handle_message() directly instead of decoding real wire bytes -- this is
// exactly the class-level comment's point: OMS logic is fully testable
// without OrderEntryClient or a real socket at all. See
// tests/test_oms_gateway_e2e.cpp for the version of these same scenarios
// proven over a real TCP connection into a real OrderEntryGateway.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::protocol::order_entry;
using namespace mdh::trader::oms;

namespace {

constexpr AccountId kAccount = 1;
constexpr InstrumentId kInstrument = 7;

// A minimal test double standing in for the real wire: captures every
// Message the OMS attempts to send, and can be told to fail the next N
// sends on demand (simulating a dropped connection).
class FakeSender {
public:
    [[nodiscard]] bool operator()(const Message& message) {
        if (fail_next_sends_ > 0) {
            --fail_next_sends_;
            return false;
        }
        sent.push_back(message);
        return true;
    }

    void fail_next_send(int count = 1) { fail_next_sends_ = count; }

    std::vector<Message> sent;

private:
    int fail_next_sends_ = 0;
};

} // namespace

TEST(OrderManagementSystem, SubmitNewOrderSendsANewOrderMessageAndTracksItAsPendingNew) {
    FakeSender sender;
    OrderManagementSystem oms(kAccount, std::ref(sender));

    const auto id = oms.submit_new_order(kInstrument, Side::Buy, 100, 10);

    ASSERT_EQ(sender.sent.size(), 1u);
    const auto* sent = std::get_if<NewOrder>(&sender.sent[0]);
    ASSERT_NE(sent, nullptr);
    EXPECT_EQ(sent->account_id, kAccount);
    EXPECT_EQ(sent->client_order_id, id);
    EXPECT_EQ(sent->instrument_id, kInstrument);
    EXPECT_EQ(sent->side, Side::Buy);
    EXPECT_EQ(sent->price, 100);
    EXPECT_EQ(sent->quantity, 10u);

    const auto order = oms.order(id);
    ASSERT_TRUE(order.has_value());
    EXPECT_EQ(order->state, ClientOrderState::PendingNew);
    EXPECT_EQ(order->remaining_quantity, 10u);
    EXPECT_FALSE(order->exchange_order_id.has_value());
}

TEST(OrderManagementSystem, TwoSubmissionsAreAssignedDistinctClientOrderIds) {
    FakeSender sender;
    OrderManagementSystem oms(kAccount, std::ref(sender));

    const auto id1 = oms.submit_new_order(kInstrument, Side::Buy, 100, 10);
    const auto id2 = oms.submit_new_order(kInstrument, Side::Sell, 105, 5);
    EXPECT_NE(id1, id2);
}

TEST(OrderManagementSystem, AcceptedTransitionsPendingNewToLiveAndRecordsExchangeOrderId) {
    FakeSender sender;
    std::vector<ClientOrder> updates;
    OrderManagementSystem oms(kAccount, std::ref(sender), [&](const ClientOrder& o) { updates.push_back(o); });

    const auto id = oms.submit_new_order(kInstrument, Side::Buy, 100, 10);
    oms.handle_message(Message{Accepted{.account_id = kAccount,
                                         .client_order_id = id,
                                         .exchange_order_id = 555,
                                         .instrument_id = kInstrument,
                                         .side = Side::Buy,
                                         .price = 100,
                                         .quantity = 10,
                                         .order_type = OrderType::Limit,
                                         .time_in_force = TimeInForce::GTC}});

    const auto order = oms.order(id);
    ASSERT_TRUE(order.has_value());
    EXPECT_EQ(order->state, ClientOrderState::Live);
    EXPECT_EQ(order->exchange_order_id, 555u);
    EXPECT_EQ(order->remaining_quantity, 10u);

    ASSERT_EQ(updates.size(), 1u);
    EXPECT_EQ(updates[0].state, ClientOrderState::Live);
}

TEST(OrderManagementSystem, RejectedOnAPendingNewOrderTransitionsItToRejected) {
    FakeSender sender;
    OrderManagementSystem oms(kAccount, std::ref(sender));

    const auto id = oms.submit_new_order(kInstrument, Side::Buy, 100, 10);
    oms.handle_message(Message{Rejected{.account_id = kAccount,
                                         .client_order_id = id,
                                         .instrument_id = kInstrument,
                                         .reason = RejectReason::InsufficientFunds}});

    const auto order = oms.order(id);
    ASSERT_TRUE(order.has_value());
    EXPECT_EQ(order->state, ClientOrderState::Rejected);
    EXPECT_EQ(order->last_reject_reason, RejectReason::InsufficientFunds);
}

TEST(OrderManagementSystem, SendFailureOnSubmitImmediatelyMarksTheOrderRejectedWithInternalError) {
    FakeSender sender;
    sender.fail_next_send();
    OrderManagementSystem oms(kAccount, std::ref(sender));

    const auto id = oms.submit_new_order(kInstrument, Side::Buy, 100, 10);

    EXPECT_TRUE(sender.sent.empty()); // the send itself failed -- nothing was actually transmitted
    const auto order = oms.order(id);
    ASSERT_TRUE(order.has_value());
    EXPECT_EQ(order->state, ClientOrderState::Rejected);
    EXPECT_EQ(order->last_reject_reason, RejectReason::InternalError);
}

TEST(OrderManagementSystem, CancelOrderOnAnUnknownIdReturnsFalseAndSendsNothing) {
    FakeSender sender;
    OrderManagementSystem oms(kAccount, std::ref(sender));

    EXPECT_FALSE(oms.cancel_order(/*client_order_id=*/999));
    EXPECT_TRUE(sender.sent.empty());
}

TEST(OrderManagementSystem, CancelOrderOnAStillPendingNewOrderReturnsFalse) {
    // Real exchanges (and this one, see matching_engine.cpp's process_cancel())
    // only know about an order once it's live -- cancelling before an
    // Accepted has even been observed has nothing to reference yet.
    FakeSender sender;
    OrderManagementSystem oms(kAccount, std::ref(sender));

    const auto id = oms.submit_new_order(kInstrument, Side::Buy, 100, 10);
    EXPECT_FALSE(oms.cancel_order(id));
}

TEST(OrderManagementSystem, SuccessfulCancelTransitionsLiveOrderToCancelled) {
    FakeSender sender;
    OrderManagementSystem oms(kAccount, std::ref(sender));

    const auto id = oms.submit_new_order(kInstrument, Side::Buy, 100, 10);
    oms.handle_message(Message{Accepted{.account_id = kAccount,
                                         .client_order_id = id,
                                         .exchange_order_id = 1,
                                         .instrument_id = kInstrument,
                                         .side = Side::Buy,
                                         .price = 100,
                                         .quantity = 10,
                                         .order_type = OrderType::Limit,
                                         .time_in_force = TimeInForce::GTC}});

    ASSERT_TRUE(oms.cancel_order(id));
    ASSERT_EQ(sender.sent.size(), 2u); // NewOrder, then CancelOrder
    EXPECT_NE(std::get_if<CancelOrder>(&sender.sent[1]), nullptr);

    oms.handle_message(
        Message{Cancelled{.account_id = kAccount, .client_order_id = id, .exchange_order_id = 1, .instrument_id = kInstrument}});

    const auto order = oms.order(id);
    ASSERT_TRUE(order.has_value());
    EXPECT_EQ(order->state, ClientOrderState::Cancelled);
    EXPECT_EQ(order->pending_action, PendingAction::None);
}

TEST(OrderManagementSystem, ASecondCancelWhileOneIsAlreadyInFlightIsRejectedLocally) {
    FakeSender sender;
    OrderManagementSystem oms(kAccount, std::ref(sender));

    const auto id = oms.submit_new_order(kInstrument, Side::Buy, 100, 10);
    oms.handle_message(Message{Accepted{.account_id = kAccount,
                                         .client_order_id = id,
                                         .exchange_order_id = 1,
                                         .instrument_id = kInstrument,
                                         .side = Side::Buy,
                                         .price = 100,
                                         .quantity = 10,
                                         .order_type = OrderType::Limit,
                                         .time_in_force = TimeInForce::GTC}});

    ASSERT_TRUE(oms.cancel_order(id));
    EXPECT_FALSE(oms.cancel_order(id)); // already has a cancel in flight
}

TEST(OrderManagementSystem, RejectedOnALiveOrderIsInterpretedAsAFailedCancelNotAnOrderRejection) {
    // The wire's Rejected message can't distinguish "your new order was
    // rejected" from "your cancel attempt on an already-live order failed"
    // -- see OrderManagementSystem's own class-level comment. This pins
    // that the OMS uses `pending_action` to tell them apart correctly: the
    // order itself must remain Live, not flip to Rejected.
    FakeSender sender;
    OrderManagementSystem oms(kAccount, std::ref(sender));

    const auto id = oms.submit_new_order(kInstrument, Side::Buy, 100, 10);
    oms.handle_message(Message{Accepted{.account_id = kAccount,
                                         .client_order_id = id,
                                         .exchange_order_id = 1,
                                         .instrument_id = kInstrument,
                                         .side = Side::Buy,
                                         .price = 100,
                                         .quantity = 10,
                                         .order_type = OrderType::Limit,
                                         .time_in_force = TimeInForce::GTC}});
    ASSERT_TRUE(oms.cancel_order(id));

    // The cancel raced a full fill at the exchange and lost -- the exchange
    // rejects the cancel (UnknownOrderId, since the order is already gone
    // from live_orders_ by the time it's processed).
    oms.handle_message(Message{Rejected{.account_id = kAccount,
                                         .client_order_id = id,
                                         .instrument_id = kInstrument,
                                         .reason = RejectReason::UnknownOrderId}});

    const auto order = oms.order(id);
    ASSERT_TRUE(order.has_value());
    EXPECT_EQ(order->state, ClientOrderState::Live); // unaffected -- still exactly as it was
    EXPECT_EQ(order->pending_action, PendingAction::None); // but the in-flight cancel is cleared
    EXPECT_EQ(order->last_reject_reason, RejectReason::UnknownOrderId);

    // And the order is cancellable again, since no cancel is in flight anymore.
    EXPECT_TRUE(oms.cancel_order(id));
}

TEST(OrderManagementSystem, ReplaceOrderOnAnUnknownIdReturnsNulloptAndSendsNothing) {
    FakeSender sender;
    OrderManagementSystem oms(kAccount, std::ref(sender));

    EXPECT_FALSE(oms.replace_order(/*client_order_id=*/999, /*new_price=*/1, /*new_quantity=*/1).has_value());
    EXPECT_TRUE(sender.sent.empty());
}

TEST(OrderManagementSystem, SuccessfulReplaceRetiresTheOldIdAndActivatesTheNewOne) {
    FakeSender sender;
    OrderManagementSystem oms(kAccount, std::ref(sender));

    const auto original_id = oms.submit_new_order(kInstrument, Side::Buy, 100, 10);
    oms.handle_message(Message{Accepted{.account_id = kAccount,
                                         .client_order_id = original_id,
                                         .exchange_order_id = 1,
                                         .instrument_id = kInstrument,
                                         .side = Side::Buy,
                                         .price = 100,
                                         .quantity = 10,
                                         .order_type = OrderType::Limit,
                                         .time_in_force = TimeInForce::GTC}});

    const auto new_id = oms.replace_order(original_id, /*new_price=*/105, /*new_quantity=*/8);
    ASSERT_TRUE(new_id.has_value());
    EXPECT_NE(*new_id, original_id);

    // The new id is already queryable, pre-registered, before any wire
    // response -- same rationale as submit_new_order()'s own synchronous id.
    auto pre_response = oms.order(*new_id);
    ASSERT_TRUE(pre_response.has_value());
    EXPECT_EQ(pre_response->state, ClientOrderState::PendingNew);

    oms.handle_message(Message{Replaced{.account_id = kAccount,
                                         .original_client_order_id = original_id,
                                         .new_client_order_id = *new_id,
                                         .exchange_order_id = 1,
                                         .instrument_id = kInstrument,
                                         .new_price = 105,
                                         .new_quantity = 8}});

    const auto old_order = oms.order(original_id);
    ASSERT_TRUE(old_order.has_value());
    EXPECT_EQ(old_order->state, ClientOrderState::Replaced);

    const auto new_order = oms.order(*new_id);
    ASSERT_TRUE(new_order.has_value());
    EXPECT_EQ(new_order->state, ClientOrderState::Live);
    EXPECT_EQ(new_order->price, 105);
    EXPECT_EQ(new_order->quantity, 8u);
    EXPECT_EQ(new_order->remaining_quantity, 8u);
    EXPECT_EQ(new_order->exchange_order_id, 1u);
}

TEST(OrderManagementSystem, TradeReportsDrivePartialThenFullFillTransitions) {
    FakeSender sender;
    OrderManagementSystem oms(kAccount, std::ref(sender));

    const auto id = oms.submit_new_order(kInstrument, Side::Buy, 100, 10);
    oms.handle_message(Message{Accepted{.account_id = kAccount,
                                         .client_order_id = id,
                                         .exchange_order_id = 1,
                                         .instrument_id = kInstrument,
                                         .side = Side::Buy,
                                         .price = 100,
                                         .quantity = 10,
                                         .order_type = OrderType::Limit,
                                         .time_in_force = TimeInForce::GTC}});

    oms.handle_message(Message{TradeReport{.account_id = kAccount,
                                            .client_order_id = id,
                                            .exchange_order_id = 1,
                                            .instrument_id = kInstrument,
                                            .price = 100,
                                            .quantity = 4,
                                            .remaining_quantity = 6}});
    EXPECT_EQ(oms.order(id)->state, ClientOrderState::PartiallyFilled);
    EXPECT_EQ(oms.order(id)->remaining_quantity, 6u);

    oms.handle_message(Message{TradeReport{.account_id = kAccount,
                                            .client_order_id = id,
                                            .exchange_order_id = 1,
                                            .instrument_id = kInstrument,
                                            .price = 100,
                                            .quantity = 6,
                                            .remaining_quantity = 0}});
    EXPECT_EQ(oms.order(id)->state, ClientOrderState::Filled);
    EXPECT_EQ(oms.order(id)->remaining_quantity, 0u);
}

TEST(OrderManagementSystem, HandleMessageForAnUntrackedClientOrderIdIsSilentlyIgnored) {
    FakeSender sender;
    std::vector<ClientOrder> updates;
    OrderManagementSystem oms(kAccount, std::ref(sender), [&](const ClientOrder& o) { updates.push_back(o); });

    oms.handle_message(Message{Cancelled{
        .account_id = kAccount, .client_order_id = 12345, .exchange_order_id = 1, .instrument_id = kInstrument}});

    EXPECT_TRUE(updates.empty());
    EXPECT_FALSE(oms.order(12345).has_value());
}

TEST(OrderManagementSystem, OrdersReturnsEveryTrackedOrder) {
    FakeSender sender;
    OrderManagementSystem oms(kAccount, std::ref(sender));

    const auto id1 = oms.submit_new_order(kInstrument, Side::Buy, 100, 10);
    const auto id2 = oms.submit_new_order(kInstrument, Side::Sell, 105, 5);

    const auto all = oms.orders();
    EXPECT_EQ(all.size(), 2u);
    EXPECT_NE(std::find_if(all.begin(), all.end(), [&](const auto& o) { return o.client_order_id == id1; }),
              all.end());
    EXPECT_NE(std::find_if(all.begin(), all.end(), [&](const auto& o) { return o.client_order_id == id2; }),
              all.end());
}
