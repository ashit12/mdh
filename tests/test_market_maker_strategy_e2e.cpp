#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "book/order_book.hpp"
#include "exchange/gateway/order_entry_gateway.hpp"
#include "trader/oms/order_entry_client.hpp"
#include "trader/risk/trader_risk_gated_oms.hpp"
#include "trader/strategies/market_maker_strategy.hpp"

// The loop-closing test for the strategy layer: a real MarketMakerStrategy,
// trading through a real TraderRiskGatedOms + OrderEntryClient, over a real
// TCP connection to a real, unmodified OrderEntryGateway -- proving the
// strategy layer composes correctly with every layer beneath it (gateway,
// OMS/client, trader-side risk/positions), the same loop-closing role
// test_trader_risk_gated_oms_e2e.cpp plays for trader-side risk against the
// bare OMS/client.
//
// ── Why the book is built by the test, not received over a live feed ─────
// Per docs/exchange_flow.md's "Integration status" section,
// MarketDataPublisher is not yet wired into a running gateway's matching
// thread -- there is no live UDP feed to listen to yet. This test instead
// mirrors what such a feed would eventually report directly into a
// book::OrderBook, using only information this test itself already knows
// (a liquidity-provider account's own confirmed order prices/quantities),
// and drives MarketMakerStrategy::on_book_update() with it exactly the way
// a live StrategyRuntime call site would once that wiring exists. The
// order flow itself (NewOrder/Accepted/ReplaceOrder/TradeReport) is fully
// real, over a real socket, to a real gateway sitting in front of a real
// matching engine -- only the "how does a book update arrive" step is
// test-simulated. Deliberately, the mirror only ever contains the LP's own
// orders, never the market maker's own -- exactly like a real market maker
// reads the *public* book (which does not distinguish "my own resting
// order" from anyone else's, but this test's simplified mirror simply never
// adds the market maker's orders to it), so mid-price is always computed
// from the *other* side of the market, not from the strategy's own quotes.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::gateway;
using namespace mdh::trader::oms;
using namespace mdh::trader::risk;
using namespace mdh::trader::strategies;
using namespace std::chrono_literals;

namespace {

constexpr InstrumentId kInstrument = 21;
constexpr AccountId kLpAccount = 100;
constexpr AccountId kMmAccount = 101;

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

// A connected TraderRiskGatedOms for one account -- same helper shape as
// test_trader_risk_gated_oms_e2e.cpp's own RiskGatedTrader.
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

[[nodiscard]] bool is_live(const std::optional<ClientOrder>& order) {
    return order.has_value() && (order->state == ClientOrderState::Live || order->state == ClientOrderState::PartiallyFilled);
}

} // namespace

TEST(MarketMakerStrategyE2e, QuotesFillsAndRequotesOverARealGateway) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(kLpAccount, 10'000'000);
    server.gateway().deposit_position(kLpAccount, kInstrument, 10'000);
    server.gateway().deposit_cash(kMmAccount, 10'000'000);
    server.gateway().deposit_position(kMmAccount, kInstrument, 10'000);

    RiskGatedTrader lp(kLpAccount, server.port());
    RiskGatedTrader mm_trader(kMmAccount, server.port());
    ASSERT_TRUE(lp.connected());
    ASSERT_TRUE(mm_trader.connected());
    lp.gated().deposit_cash(10'000'000);
    lp.gated().deposit_position(kInstrument, 10'000);
    mm_trader.gated().deposit_cash(10'000'000);
    mm_trader.gated().deposit_position(kInstrument, 500); // local bookkeeping mirrors the exchange-side deposit above

    // 1. The LP establishes a wide initial market: bid @ 90, ask @ 110 --
    // deliberately wide so the market maker's own tighter quotes (computed
    // below) always sit strictly inside it, at strictly better prices, and
    // are therefore always the ones an aggressive order matches first --
    // no ambiguity with the LP's own resting orders at the same price
    // level, and no need to ever cancel them.
    const auto lp_bid = lp.gated().submit_new_order(kInstrument, Side::Buy, 90, 50);
    const auto lp_ask = lp.gated().submit_new_order(kInstrument, Side::Sell, 110, 50);
    ASSERT_TRUE(lp_bid.client_order_id.has_value());
    ASSERT_TRUE(lp_ask.client_order_id.has_value());
    ASSERT_TRUE(wait_until([&] { return is_live(lp.gated().order(*lp_bid.client_order_id)); }));
    ASSERT_TRUE(wait_until([&] { return is_live(lp.gated().order(*lp_ask.client_order_id)); }));

    book::OrderBook local_book;
    OrderId next_local_id = 1;
    const OrderId lp_bid_local_id = next_local_id++;
    const OrderId lp_ask_local_id = next_local_id++;
    ASSERT_FALSE(local_book.add_order(lp_bid_local_id, 90, 50, Side::Buy).has_value());
    ASSERT_FALSE(local_book.add_order(lp_ask_local_id, 110, 50, Side::Sell).has_value());

    MarketMakerConfig config{
        .instrument_id = kInstrument, .quote_size = 10, .half_spread = 2, .max_position = 10'000, .requote_threshold = 2};
    MarketMakerStrategy mm(mm_trader.gated(), config);

    // 2. First look at the market: mid = (90 + 110) / 2 = 100 -- MM quotes
    // bid @ 98 / ask @ 102, strictly inside the LP's own 90/110 market.
    mm.on_book_update(kInstrument, local_book);
    ASSERT_TRUE(mm.bid_order_id().has_value());
    ASSERT_TRUE(mm.ask_order_id().has_value());
    const auto mm_bid_id = *mm.bid_order_id();
    const auto mm_ask_id = *mm.ask_order_id();
    ASSERT_TRUE(wait_until([&] { return is_live(mm_trader.gated().order(mm_bid_id)); }));
    ASSERT_TRUE(wait_until([&] { return is_live(mm_trader.gated().order(mm_ask_id)); }));
    EXPECT_EQ(mm_trader.gated().order(mm_bid_id)->price, 98);
    EXPECT_EQ(mm_trader.gated().order(mm_ask_id)->price, 102);

    // 3. The LP crosses the spread with an aggressive IOC sell at 98: the
    // best bid in the book is the market maker's own (98 beats the LP's own
    // resting 90), so this fills the market maker's bid, not the LP's own.
    const auto lp_hit = lp.gated().submit_new_order(kInstrument, Side::Sell, 98, 5, exchange::OrderType::Limit,
                                                     exchange::TimeInForce::IOC);
    ASSERT_TRUE(lp_hit.client_order_id.has_value());

    ASSERT_TRUE(wait_until([&] { return mm_trader.gated().position(kInstrument) == 505; })); // 500 seed + 5 filled
    EXPECT_EQ(mm_trader.gated().order(mm_bid_id)->state, ClientOrderState::PartiallyFilled);
    EXPECT_EQ(mm_trader.gated().order(mm_bid_id)->remaining_quantity, 5u);

    // 4. The LP widens further: cancels its ask @ 110, replaces it with one
    // @ 130. Mirror that into the local book.
    ASSERT_TRUE(lp.gated().cancel_order(*lp_ask.client_order_id));
    ASSERT_TRUE(wait_until([&] { return lp.gated().order(*lp_ask.client_order_id)->state == ClientOrderState::Cancelled; }));
    const auto lp_new_ask = lp.gated().submit_new_order(kInstrument, Side::Sell, 130, 50);
    ASSERT_TRUE(lp_new_ask.client_order_id.has_value());
    ASSERT_TRUE(wait_until([&] { return is_live(lp.gated().order(*lp_new_ask.client_order_id)); }));

    ASSERT_FALSE(local_book.cancel_order(lp_ask_local_id).has_value());
    ASSERT_FALSE(local_book.add_order(next_local_id++, 130, 50, Side::Sell).has_value());

    // 5. New mid = (90 + 130) / 2 = 110 -- desired bid 108, desired ask 112,
    // both far enough from the current 98/102 quotes (>= requote_threshold)
    // to trigger a real ReplaceOrder round trip on each side.
    mm.on_book_update(kInstrument, local_book);
    ASSERT_TRUE(mm.bid_order_id().has_value());
    ASSERT_TRUE(mm.ask_order_id().has_value());
    EXPECT_NE(*mm.bid_order_id(), mm_bid_id); // replaced -- tracking a new id now
    EXPECT_NE(*mm.ask_order_id(), mm_ask_id);

    ASSERT_TRUE(wait_until([&] { return is_live(mm_trader.gated().order(*mm.bid_order_id())); }));
    ASSERT_TRUE(wait_until([&] { return is_live(mm_trader.gated().order(*mm.ask_order_id())); }));
    EXPECT_EQ(mm_trader.gated().order(*mm.bid_order_id())->price, 108);
    EXPECT_EQ(mm_trader.gated().order(*mm.ask_order_id())->price, 112);
}
