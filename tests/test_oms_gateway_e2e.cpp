#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "exchange/gateway/order_entry_gateway.hpp"
#include "trader/oms/order_entry_client.hpp"
#include "trader/oms/order_management_system.hpp"

// The Milestone 8 loop-closing test: proves the trader-side OMS + client
// actually interoperate with a real, unmodified OrderEntryGateway
// (Milestone 7) over a real TCP loopback connection -- the trader-side
// mirror of tests/test_order_entry_gateway_e2e.cpp, which proved the
// gateway's own wire behavior using a bare hand-rolled test client. Here the
// client is the real production one: OrderManagementSystem's state machine
// is driven entirely by OrderEntryClient's background reader thread calling
// handle_message() with whatever the real gateway actually sent back, and
// every request OrderManagementSystem issues is a real wire NewOrder/
// CancelOrder/ReplaceOrder the gateway decodes and processes for real.
//
// Since delivery is asynchronous (the client's reader thread, not the test
// thread, calls handle_message()), assertions here poll for the expected
// terminal/intermediate state with a bounded timeout rather than asserting
// immediately after submit_new_order()/cancel_order()/replace_order()
// return -- those calls only guarantee the *request* was sent, never that a
// response has already arrived.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::gateway;
using namespace mdh::trader::oms;
using namespace std::chrono_literals;

namespace {

constexpr InstrumentId kInstrument = 9;

class RunningGateway {
public:
    explicit RunningGateway(const OrderEntryGatewayOptions& options = {}) : gateway_(0, options) {
        started_ = gateway_.start();
    }

    [[nodiscard]] bool started() const { return started_; }
    [[nodiscard]] std::uint16_t port() const { return *gateway_.local_port(); }
    [[nodiscard]] OrderEntryGateway& gateway() { return gateway_; }

private:
    OrderEntryGateway gateway_;
    bool started_ = false;
};

// Bundles a connected OrderEntryClient + OrderManagementSystem for one
// account -- the OMS's Sender forwards straight to the client's send(), and
// the client's MessageSink forwards straight to the OMS's handle_message(),
// exactly how a real trading application would wire the two together.
class Trader {
public:
    Trader(AccountId account_id, std::uint16_t port)
        : client_([this](const protocol::order_entry::Message& m) { oms_.handle_message(m); }),
          oms_(account_id, [this](const protocol::order_entry::Message& m) { return client_.send(m); }) {
        connected_ = client_.connect("127.0.0.1", port);
    }

    [[nodiscard]] bool connected() const { return connected_; }
    [[nodiscard]] OrderManagementSystem& oms() { return oms_; }

private:
    OrderEntryClient client_;
    OrderManagementSystem oms_;
    bool connected_ = false;
};

// Polls `predicate` (typically an oms.order(id)->state == X check) until it
// returns true or `timeout` elapses -- necessary because responses arrive
// asynchronously on the client's reader thread, not synchronously with
// whatever request triggered them. Returns whether it converged; callers
// wrap this in an ASSERT_TRUE/EXPECT_TRUE so a timeout fails loudly instead
// of silently proceeding to assert on stale state.
template <typename Predicate>
[[nodiscard]] bool wait_until(Predicate predicate, std::chrono::milliseconds timeout = 1000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(2ms);
    }
    return predicate();
}

} // namespace

TEST(OmsGatewayE2e, SubmitNewOrderReachesLiveViaARealAcceptedResponse) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(/*account_id=*/1, /*amount=*/1'000'000);

    Trader trader(/*account_id=*/1, server.port());
    ASSERT_TRUE(trader.connected());

    const auto id = trader.oms().submit_new_order(kInstrument, Side::Buy, 100, 10);
    ASSERT_TRUE(wait_until([&] {
        auto order = trader.oms().order(id);
        return order.has_value() && order->state == ClientOrderState::Live;
    }));

    const auto order = trader.oms().order(id);
    ASSERT_TRUE(order.has_value());
    EXPECT_TRUE(order->exchange_order_id.has_value());
    EXPECT_EQ(order->remaining_quantity, 10u);
}

TEST(OmsGatewayE2e, SubmitNewOrderWithoutFundsReachesRejectedViaARealRejectedResponse) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    // No deposit_cash() -- the account can't cover a buy.

    Trader trader(/*account_id=*/2, server.port());
    ASSERT_TRUE(trader.connected());

    const auto id = trader.oms().submit_new_order(kInstrument, Side::Buy, 100, 10);
    ASSERT_TRUE(wait_until([&] {
        auto order = trader.oms().order(id);
        return order.has_value() && order->state == ClientOrderState::Rejected;
    }));

    EXPECT_EQ(trader.oms().order(id)->last_reject_reason, RejectReason::InsufficientFunds);
}

TEST(OmsGatewayE2e, CancelOrderReachesCancelledViaARealCancelledResponse) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(/*account_id=*/3, /*amount=*/1'000'000);

    Trader trader(/*account_id=*/3, server.port());
    ASSERT_TRUE(trader.connected());

    const auto id = trader.oms().submit_new_order(kInstrument, Side::Buy, 50, 5);
    ASSERT_TRUE(wait_until([&] { return trader.oms().order(id)->state == ClientOrderState::Live; }));

    ASSERT_TRUE(trader.oms().cancel_order(id));
    ASSERT_TRUE(wait_until([&] { return trader.oms().order(id)->state == ClientOrderState::Cancelled; }));
}

TEST(OmsGatewayE2e, ReplaceOrderReachesLiveUnderTheNewIdViaARealReplacedResponse) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(/*account_id=*/4, /*amount=*/1'000'000);

    Trader trader(/*account_id=*/4, server.port());
    ASSERT_TRUE(trader.connected());

    const auto id = trader.oms().submit_new_order(kInstrument, Side::Buy, 50, 5);
    ASSERT_TRUE(wait_until([&] { return trader.oms().order(id)->state == ClientOrderState::Live; }));

    const auto new_id = trader.oms().replace_order(id, /*new_price=*/55, /*new_quantity=*/8);
    ASSERT_TRUE(new_id.has_value());
    ASSERT_TRUE(wait_until([&] { return trader.oms().order(*new_id)->state == ClientOrderState::Live; }));

    EXPECT_EQ(trader.oms().order(id)->state, ClientOrderState::Replaced);
    const auto replaced = trader.oms().order(*new_id);
    EXPECT_EQ(replaced->price, 55);
    EXPECT_EQ(replaced->quantity, 8u);
}

TEST(OmsGatewayE2e, TwoTradersCrossingOrdersEachSeeTheirOwnFillViaRealTradeReports) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    constexpr AccountId kBuyerAccount = 5;
    constexpr AccountId kSellerAccount = 6;
    server.gateway().deposit_cash(kBuyerAccount, 1'000'000);
    server.gateway().deposit_position(kSellerAccount, kInstrument, 100);

    Trader buyer(kBuyerAccount, server.port());
    Trader seller(kSellerAccount, server.port());
    ASSERT_TRUE(buyer.connected());
    ASSERT_TRUE(seller.connected());

    const auto buyer_id = buyer.oms().submit_new_order(kInstrument, Side::Buy, 100, 10);
    ASSERT_TRUE(wait_until([&] { return buyer.oms().order(buyer_id)->state == ClientOrderState::Live; }));

    const auto seller_id = seller.oms().submit_new_order(kInstrument, Side::Sell, 100, 10);

    ASSERT_TRUE(wait_until([&] {
        auto order = buyer.oms().order(buyer_id);
        return order.has_value() && order->state == ClientOrderState::Filled;
    }));
    ASSERT_TRUE(wait_until([&] {
        auto order = seller.oms().order(seller_id);
        return order.has_value() && order->state == ClientOrderState::Filled;
    }));

    EXPECT_EQ(buyer.oms().order(buyer_id)->remaining_quantity, 0u);
    EXPECT_EQ(seller.oms().order(seller_id)->remaining_quantity, 0u);

    // snapshot() is only safe to call once the matching thread has been
    // joined (see its own doc comment) -- both fills having already been
    // observed proves the trade was processed, but not that the matching
    // thread has gone on to fully quiesce, so stop() first rather than
    // racing it.
    server.gateway().stop();
    const auto snapshot = server.gateway().snapshot();
    EXPECT_TRUE(snapshot.instruments.empty()); // both orders fully filled -- nothing left resting
}
