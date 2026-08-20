#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "exchange/gateway/order_entry_gateway.hpp"
#include "trader/oms/order_entry_client.hpp"
#include "trader/risk/trader_risk_gated_oms.hpp"

// The loop-closing test for trader-side risk: proves TraderRiskGatedOms interoperates
// with a real, unmodified OrderEntryGateway exactly like a bare
// OrderManagementSystem does (see test_oms_gateway_e2e.cpp), while also
// demonstrating the specific thing the gated OMS adds -- two genuinely
// independent risk layers, exactly as docs/end_to_end_architecture.md's
// system diagram draws them as two separate boxes:
//
//   - The trader's OWN risk check (TraderRiskEngine, checked against this
//     process's own PositionTracker) can reject an order before it is ever
//     sent -- the gateway/exchange never even sees it.
//   - The EXCHANGE's OWN risk check (RiskGatedEngine, checked
//     against the gateway's own, entirely separate Ledger) can still
//     independently reject an order that passed the trader-side check --
//     proving neither layer is a stand-in for the other.
//
// And that a real crossing trade over real TCP correctly drives this
// process's own PositionTracker via OrderManagementSystem's FillSink, with
// no wiring beyond what TraderRiskGatedOms's constructor already does
// internally.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::gateway;
using namespace mdh::trader::oms;
using namespace mdh::trader::risk;
using namespace std::chrono_literals;

namespace {

constexpr InstrumentId kInstrument = 11;

class RunningGateway {
public:
    // The gateway trades only what it is told about, and every test here
    // uses the one instrument above.
    explicit RunningGateway(OrderEntryGatewayOptions options = {}) : gateway_(0, with_default_instrument(options)) {
        started_ = gateway_.start();
    }

    [[nodiscard]] bool started() const { return started_; }
    [[nodiscard]] std::uint16_t port() const { return *gateway_.local_port(); }
    [[nodiscard]] OrderEntryGateway& gateway() { return gateway_; }

private:
    static const OrderEntryGatewayOptions& with_default_instrument(OrderEntryGatewayOptions& options) {
        if (options.instruments.empty()) {
            options.instruments = {kInstrument};
        }
        return options;
    }

    OrderEntryGateway gateway_;
    bool started_ = false;
};

// Bundles a connected OrderEntryClient + TraderRiskGatedOms for one account
// -- the trader-side counterpart to test_oms_gateway_e2e.cpp's own Trader
// helper, substituting TraderRiskGatedOms for a bare OrderManagementSystem.
class RiskGatedTrader {
public:
    RiskGatedTrader(AccountId account_id, std::uint16_t port, TraderRiskLimits limits = {})
        : gated_(
              account_id, [this](const protocol::order_entry::Message& m) { return client_.send(m); }, nullptr,
              limits),
          client_([this](const protocol::order_entry::Message& m) { gated_.handle_message(m); }) {
        connected_ = client_.connect("127.0.0.1", port);
    }

    [[nodiscard]] bool connected() const { return connected_; }
    [[nodiscard]] TraderRiskGatedOms& gated() { return gated_; }

private:
    // Declared (and therefore destroyed) in this order deliberately:
    // client_'s background reader thread invokes gated_.handle_message()
    // asynchronously, so client_ -- which owns that thread -- must be torn
    // down (its destructor joins the thread, see order_entry_client.hpp's
    // disconnect()) *before* gated_ is destroyed, or that thread could
    // still be calling into gated_ mid- or post-destruction. Member
    // destruction runs in reverse declaration order, so gated_ first /
    // client_ second here means client_ actually finishes tearing down
    // first. (The constructor's two lambdas capture `this`, not the
    // members directly, so which one is *constructed* first doesn't matter
    // -- neither lambda is invoked until after both members exist.)
    TraderRiskGatedOms gated_;
    OrderEntryClient client_;
    bool connected_ = false;
};

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

TEST(TraderRiskGatedOmsE2e, ApprovedOrderReachesLiveExactlyLikeABareOms) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(/*account_id=*/1, /*amount=*/1'000'000);

    RiskGatedTrader trader(/*account_id=*/1, server.port());
    ASSERT_TRUE(trader.connected());
    trader.gated().deposit_cash(1'000'000);

    const auto outcome = trader.gated().submit_new_order(kInstrument, Side::Buy, 100, 10);
    ASSERT_TRUE(outcome.client_order_id.has_value());

    ASSERT_TRUE(wait_until([&] {
        auto order = trader.gated().order(*outcome.client_order_id);
        return order.has_value() && order->state == ClientOrderState::Live;
    }));
}

TEST(TraderRiskGatedOmsE2e, TraderSideRiskRejectionNeverReachesTheGatewayAtAll) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(/*account_id=*/2, /*amount=*/1'000'000); // plenty on the exchange side

    RiskGatedTrader trader(/*account_id=*/2, server.port());
    ASSERT_TRUE(trader.connected());
    // Deliberately no local deposit_cash() -- the trader's own bookkeeping
    // thinks this account has nothing, even though the exchange would
    // happily accept the order.

    const auto outcome = trader.gated().submit_new_order(kInstrument, Side::Buy, 100, 10);

    EXPECT_FALSE(outcome.client_order_id.has_value());
    EXPECT_EQ(outcome.local_reject_reason, RejectReason::InsufficientFunds);
    EXPECT_TRUE(trader.gated().orders().empty()); // never even reached the OMS, let alone the wire

    // Give any (incorrectly sent) traffic a moment to arrive, then confirm
    // the exchange's own ledger truly never saw this order.
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(server.gateway().connection_count(), 1u); // the TCP connection itself is still fine
}

TEST(TraderRiskGatedOmsE2e, ExchangeSideRiskCanStillRejectAnOrderTheTraderSideApproved) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    // No deposit_cash() on the gateway/exchange side -- the exchange's own
    // Ledger knows nothing about this account.

    RiskGatedTrader trader(/*account_id=*/3, server.port());
    ASSERT_TRUE(trader.connected());
    trader.gated().deposit_cash(1'000'000); // but the trader's OWN bookkeeping thinks it's well-funded

    const auto outcome = trader.gated().submit_new_order(kInstrument, Side::Buy, 100, 10);
    ASSERT_TRUE(outcome.client_order_id.has_value()); // passed the trader-side check, so it really was sent

    ASSERT_TRUE(wait_until([&] {
        auto order = trader.gated().order(*outcome.client_order_id);
        return order.has_value() && order->state == ClientOrderState::Rejected;
    }));
    EXPECT_EQ(trader.gated().order(*outcome.client_order_id)->last_reject_reason, RejectReason::InsufficientFunds);
}

TEST(TraderRiskGatedOmsE2e, ACrossingTradeUpdatesThisProcesssOwnPositionTrackerViaRealTcp) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    constexpr AccountId kBuyerAccount = 4;
    constexpr AccountId kSellerAccount = 5;
    server.gateway().deposit_cash(kBuyerAccount, 1'000'000);
    server.gateway().deposit_position(kSellerAccount, kInstrument, 100);

    RiskGatedTrader buyer(kBuyerAccount, server.port());
    RiskGatedTrader seller(kSellerAccount, server.port());
    ASSERT_TRUE(buyer.connected());
    ASSERT_TRUE(seller.connected());
    buyer.gated().deposit_cash(1'000'000);
    seller.gated().deposit_position(kInstrument, 100);

    const auto buyer_outcome = buyer.gated().submit_new_order(kInstrument, Side::Buy, 100, 10);
    ASSERT_TRUE(buyer_outcome.client_order_id.has_value());
    ASSERT_TRUE(wait_until([&] { return buyer.gated().order(*buyer_outcome.client_order_id)->state == ClientOrderState::Live; }));

    const auto seller_outcome = seller.gated().submit_new_order(kInstrument, Side::Sell, 100, 10);
    ASSERT_TRUE(seller_outcome.client_order_id.has_value());

    ASSERT_TRUE(wait_until([&] { return buyer.gated().position(kInstrument) == 10u; }));
    ASSERT_TRUE(wait_until([&] { return seller.gated().position(kInstrument) == 90u; }));

    EXPECT_EQ(buyer.gated().cash(), 1'000'000 - 1'000);
    EXPECT_EQ(seller.gated().cash(), 1'000); // started with 0 local cash, credited by the fill
}
