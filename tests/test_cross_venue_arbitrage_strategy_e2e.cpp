#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <thread>

#include "book/order_book.hpp"
#include "exchange/gateway/order_entry_gateway.hpp"
#include "trader/oms/order_entry_client.hpp"
#include "trader/risk/trader_risk_gated_oms.hpp"
#include "trader/strategies/cross_venue_arbitrage_strategy.hpp"

// The "two-venue simulation": TWO complete, independent exchange stacks
// (each its own OrderEntryGateway, in front of its own matching engine and
// ledger, composed exactly as the gateway normally is), running
// side by side in the same test process on two different ports, each with
// its own liquidity-provider account creating a genuine price discrepancy
// between them. A single CrossVenueArbStrategy, trading through two
// separate real TraderRiskGatedOms + OrderEntryClient pairs (one per
// venue), observes both venues' books and captures the edge with two real,
// independent IOC order round trips -- proving both that a second strategy
// composes on the market maker's plumbing untouched, and that the plumbing
// itself has no hidden single-venue assumption anywhere in it.
//
// See test_market_maker_strategy_e2e.cpp's own doc comment for why each
// venue's book is mirrored by the test itself rather than received over a
// live feed (MarketDataPublisher is not yet wired into a running gateway,
// per docs/exchange_flow.md's "Integration status" section) -- the same
// reasoning applies here, doubled, once per venue.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::gateway;
using namespace mdh::trader::oms;
using namespace mdh::trader::risk;
using namespace mdh::trader::strategies;
using namespace std::chrono_literals;

namespace {

constexpr InstrumentId kInstrument = 42;
constexpr AccountId kLpAccount = 200;
constexpr AccountId kArbAccount = 201;

class RunningGateway {
public:
    // The gateway trades only what it is told about, and both venues here
    // trade the one instrument above.
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
    // See test_trader_risk_gated_oms_e2e.cpp's own RiskGatedTrader for why
    // gated_ is declared (and therefore destroyed) before client_: client_'s
    // reader thread calls into gated_ asynchronously, so client_ must finish
    // tearing down first.
    TraderRiskGatedOms gated_;
    OrderEntryClient client_;
    bool connected_ = false;
};

// One complete, independent venue: its own gateway, an LP account that
// establishes a resting market on it, and the arbitrageur's own connection
// to it.
struct Venue {
    RunningGateway server;
    RiskGatedTrader lp{kLpAccount, server.port()};
    RiskGatedTrader arb{kArbAccount, server.port()};
    book::OrderBook local_book; // this venue's mirrored book -- see file doc comment

    Venue() {
        server.gateway().deposit_cash(kLpAccount, 10'000'000);
        server.gateway().deposit_position(kLpAccount, kInstrument, 10'000);
        server.gateway().deposit_cash(kArbAccount, 10'000'000);
        server.gateway().deposit_position(kArbAccount, kInstrument, 10'000);
        lp.gated().deposit_cash(10'000'000);
        lp.gated().deposit_position(kInstrument, 10'000);
        arb.gated().deposit_cash(10'000'000);
        arb.gated().deposit_position(kInstrument, 10'000);
    }

    // Posts an LP bid/ask pair and mirrors it into local_book, returning
    // once both are confirmed Live at the exchange.
    void establish_market(Price bid_price, Price ask_price) {
        const auto bid = lp.gated().submit_new_order(kInstrument, Side::Buy, bid_price, 50);
        const auto ask = lp.gated().submit_new_order(kInstrument, Side::Sell, ask_price, 50);
        ASSERT_TRUE(bid.client_order_id.has_value());
        ASSERT_TRUE(ask.client_order_id.has_value());
        ASSERT_TRUE(wait_until([&] { return is_live(lp.gated().order(*bid.client_order_id)); }));
        ASSERT_TRUE(wait_until([&] { return is_live(lp.gated().order(*ask.client_order_id)); }));

        static OrderId next_local_id = 1;
        ASSERT_FALSE(local_book.add_order(next_local_id++, bid_price, 50, Side::Buy).has_value());
        ASSERT_FALSE(local_book.add_order(next_local_id++, ask_price, 50, Side::Sell).has_value());
    }

    template <typename Predicate>
    [[nodiscard]] static bool wait_until(Predicate predicate, std::chrono::milliseconds timeout = 1000ms) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) {
                return true;
            }
            std::this_thread::sleep_for(2ms);
        }
        return predicate();
    }

    [[nodiscard]] static bool is_live(const std::optional<ClientOrder>& order) {
        return order.has_value() &&
               (order->state == ClientOrderState::Live || order->state == ClientOrderState::PartiallyFilled);
    }
};

} // namespace

TEST(CrossVenueArbStrategyE2e, CapturesARealEdgeAcrossTwoIndependentVenues) {
    Venue venue_a;
    Venue venue_b;
    ASSERT_TRUE(venue_a.server.started());
    ASSERT_TRUE(venue_b.server.started());
    ASSERT_TRUE(venue_a.lp.connected());
    ASSERT_TRUE(venue_a.arb.connected());
    ASSERT_TRUE(venue_b.lp.connected());
    ASSERT_TRUE(venue_b.arb.connected());

    // Venue A is cheap (ask = 100), venue B is rich (bid = 110) -- a real
    // 10-wide edge, well above the 5 min_edge configured below.
    venue_a.establish_market(/*bid=*/90, /*ask=*/100);
    venue_b.establish_market(/*bid=*/110, /*ask=*/120);

    const Quantity trade_size = 10;
    CrossVenueArbStrategy arb(venue_a.arb.gated(), venue_b.arb.gated(),
                               CrossVenueArbConfig{.instrument_id = kInstrument, .trade_size = trade_size, .min_edge = 5});

    const std::int64_t venue_a_cash_before = venue_a.arb.gated().cash();
    const std::int64_t venue_b_cash_before = venue_b.arb.gated().cash();
    const Quantity venue_a_position_before = venue_a.arb.gated().position(kInstrument);
    const Quantity venue_b_position_before = venue_b.arb.gated().position(kInstrument);

    // Feed venue A's book first (nothing to compare against yet), then
    // venue B's -- the edge fires on this second call, exactly the way a
    // live StrategyRuntime subscriber would be driven by whichever venue's
    // book happens to update.
    arb.on_venue_a_update(kInstrument, venue_a.local_book);
    arb.on_venue_b_update(kInstrument, venue_b.local_book);

    // Buy leg on the cheap venue (A): position up, cash down.
    ASSERT_TRUE(Venue::wait_until([&] { return venue_a.arb.gated().position(kInstrument) == venue_a_position_before + trade_size; }));
    EXPECT_EQ(venue_a.arb.gated().cash(), venue_a_cash_before - static_cast<std::int64_t>(100 * trade_size));

    // Sell leg on the rich venue (B): position down, cash up.
    ASSERT_TRUE(Venue::wait_until([&] { return venue_b.arb.gated().position(kInstrument) == venue_b_position_before - trade_size; }));
    EXPECT_EQ(venue_b.arb.gated().cash(), venue_b_cash_before + static_cast<std::int64_t>(110 * trade_size));

    // The edge itself: bought 10 @ 100 (spent 1000), sold 10 @ 110 (received
    // 1100) -- a real 100-unit profit split across the two venues' own
    // independent ledgers (this strategy has no unified cross-venue P&L
    // view; each venue's own cash movement is the observable proof).
    const std::int64_t total_cash_delta =
        (venue_a.arb.gated().cash() - venue_a_cash_before) + (venue_b.arb.gated().cash() - venue_b_cash_before);
    EXPECT_EQ(total_cash_delta, 100);
}
