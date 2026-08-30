#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "book/order_book.hpp"
#include "exchange/gateway/order_entry_gateway.hpp"
#include "exchange/market_data/market_data_router.hpp"
#include "net/packet.hpp"
#include "net/udp_receiver.hpp"
#include "net/udp_socket.hpp"
#include "trader/market_data/feed_subscriber.hpp"
#include "trader/oms/order_entry_client.hpp"
#include "trader/positions/pnl_tracker.hpp"
#include "trader/risk/trader_risk_gated_oms.hpp"
#include "trader/strategies/ladder_market_maker.hpp"
#include "trader/strategies/momentum_strategy.hpp"
#include "trader/strategies/strategy_runtime.hpp"
//
// The loop-closing test for the market simulator: the whole of what
// apps/market_simulator wires up, running in one process, with nothing
// simulated that the real app does for real.
//
// Concretely, every arrow in this path is exercised here:
//
//   strategy -> TraderRiskGatedOms -> OrderManagementSystem -> encoder
//     -> OrderEntryClient -> TCP -> OrderEntryGateway -> MatchingPipeline
//       -> risk + ledger -> MatchingEngine -> ExchangeEvent
//         -> execution report -> TCP -> OMS -> PnlTracker
//         -> MarketDataPublisher -> SPSC router -> UDP -> FeedSubscriber
//            -> BookManager -> StrategyRuntime -> strategy
//
// which is the point of the exercise and the reason this test is slow and
// socket-bound rather than a unit test. The participants reach the exchange
// only through those two sockets: this file constructs an OrderEntryGateway
// (as the server under test), but the participants are handed nothing but a
// host and port, so there is no path by which they could consult
// MatchingEngine, MatchingBook, RiskEngine or Ledger even accidentally.
//
// The one thing this file does that the app does not is publish market data
// itself, via OrderEntryGatewayOptions::extra_event_sink -- exactly as
// apps/trading_server does, and as tests/test_ui_gateway.cpp's own
// RunningStack does, because that wiring lives in the app rather than in the
// gateway.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::trader;
using namespace std::chrono_literals;

namespace {

constexpr InstrumentId kInstrument = 1;
constexpr AccountId kMarketMakerAccount = 9002;
constexpr AccountId kMomentumAccount = 9003;
constexpr Price kInitialPrice = 1'000'000; // 100.0000
constexpr Price kWalkStep = 100;           // 0.0100

[[nodiscard]] std::uint16_t pick_ephemeral_udp_port() {
    net::UdpReceiver probe(0);
    return *probe.local_port();
}

template <typename Predicate>
[[nodiscard]] bool wait_until(Predicate predicate, std::chrono::milliseconds timeout = 5000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(2ms);
    }
    return predicate();
}

// The trader-side stack for one simulated participant, the same composition
// apps/market_simulator's own Participant makes -- including the declaration
// order that teardown depends on, which is the part most worth mirroring
// exactly.
class Participant {
public:
    Participant(AccountId account_id, const std::string& host, std::uint16_t tcp_port)
        : account_id_(account_id),
          gated_(
              account_id,
              [this](const protocol::order_entry::Message& message) {
                  const bool sent = client_.send(message);
                  if (sent) {
                      requests_sent_.fetch_add(1, std::memory_order_relaxed);
                  }
                  return sent;
              },
              nullptr, trader::risk::TraderRiskLimits{}, pnl_.sink()),
          client_([this](const protocol::order_entry::Message& message) { gated_.handle_message(message); }) {
        connected_ = client_.connect(host, tcp_port);
    }

    [[nodiscard]] bool connected() const { return connected_; }
    [[nodiscard]] trader::risk::TraderRiskGatedOms& gated() { return gated_; }
    [[nodiscard]] const positions::PnlTracker& pnl() const { return pnl_; }
    [[nodiscard]] std::uint64_t requests_sent() const { return requests_sent_.load(std::memory_order_relaxed); }

    [[nodiscard]] positions::PnlSnapshot snapshot(std::optional<Price> mark = std::nullopt) const {
        return pnl_.snapshot(account_id_, kInstrument, mark);
    }

    [[nodiscard]] strategies::LadderMarketMaker::NetPositionSource net_position_source() {
        return [this] { return pnl_.snapshot(account_id_, kInstrument).position; };
    }

    void seed_local_mirrors() {
        gated_.deposit_cash(100'000'000'000'000);
        gated_.deposit_position(kInstrument, 1'000'000);
    }

private:
    AccountId account_id_;
    // pnl_ before gated_ (gated_ holds pnl_.sink()), gated_ before client_
    // (client_'s reader thread calls into gated_) -- see the same comment in
    // apps/market_simulator/main.cpp.
    std::atomic<std::uint64_t> requests_sent_{0};
    positions::PnlTracker pnl_;
    trader::risk::TraderRiskGatedOms gated_;
    oms::OrderEntryClient client_;
    bool connected_ = false;
};

// A real exchange publishing real market data over a real UDP socket, plus a
// FeedSubscriber listening to it -- everything on ephemeral ports so tests in
// this file can run alongside each other.
class Exchange {
public:
    Exchange() : market_data_port_(pick_ephemeral_udp_port()) {
        options_.instruments = {kInstrument};
        market_data_router_ = std::make_unique<exchange::market_data::MarketDataRouter>(
            [this](const protocol::Event& wire_event) {
                const std::array<protocol::Event, 1> frames{wire_event};
                const auto datagram = net::pack_frames(next_packet_sequence_++, std::span<const protocol::Event>(frames));
                (void)market_data_socket_.send_to(datagram, "127.0.0.1", market_data_port_);
            });
        market_data_router_->start();
        options_.extra_event_sink = market_data_router_->sink();
        gateway_ = std::make_unique<gateway::OrderEntryGateway>(0, options_);

        // Both participant accounts funded before start(), the way
        // trading_server seeds them -- a participant that cannot afford to
        // trade would make every assertion below vacuous.
        for (const AccountId account_id : {kMarketMakerAccount, kMomentumAccount}) {
            gateway_->deposit_cash(account_id, 100'000'000'000'000);
            gateway_->deposit_position(account_id, kInstrument, 1'000'000);
        }
        started_ = gateway_->start();
    }

    ~Exchange() {
        if (gateway_) {
            gateway_->stop();
        }
        if (market_data_router_) {
            market_data_router_->stop();
        }
    }

    Exchange(const Exchange&) = delete;
    Exchange& operator=(const Exchange&) = delete;

    [[nodiscard]] bool started() const { return started_; }
    [[nodiscard]] std::uint16_t tcp_port() const { return *gateway_->local_port(); }
    [[nodiscard]] std::uint16_t market_data_port() const { return market_data_port_; }

private:
    std::uint16_t market_data_port_;
    net::UdpSocket market_data_socket_;
    std::uint64_t next_packet_sequence_ = 1;
    std::unique_ptr<exchange::market_data::MarketDataRouter> market_data_router_;
    gateway::OrderEntryGatewayOptions options_;
    std::unique_ptr<gateway::OrderEntryGateway> gateway_;
    bool started_ = false;
};

[[nodiscard]] strategies::LadderMarketMakerConfig ladder_config(std::uint64_t seed) {
    strategies::LadderMarketMakerConfig config;
    config.instrument_id = kInstrument;
    config.walk = strategies::ReferencePriceWalkConfig{.initial_price = kInitialPrice,
                                                        .step_size = kWalkStep,
                                                        .min_price = kInitialPrice / 2,
                                                        .max_price = kInitialPrice * 3 / 2,
                                                        .seed = seed};
    config.levels_per_side = 2;
    config.half_spread = kWalkStep;
    config.level_spacing = kWalkStep;
    config.quote_size = 100;
    config.max_position = 2'000;
    config.requote_threshold = kWalkStep;
    return config;
}

[[nodiscard]] strategies::MomentumStrategyConfig momentum_config() {
    // A far shorter lookback and no cooldown compared to the app's defaults:
    // this test needs the strategy to commit within a handful of quote
    // cycles rather than to behave gently over minutes.
    return strategies::MomentumStrategyConfig{.instrument_id = kInstrument,
                                               .lookback = 3,
                                               .entry_threshold = kWalkStep,
                                               .trade_size = 20,
                                               .max_position = 400,
                                               .cross_slack = kWalkStep,
                                               .cooldown_updates = 0};
}

// Drives quote cycles from the test thread -- the app's own quote thread,
// unrolled, so a test can advance the market a bounded number of steps and
// then assert, instead of sleeping and hoping.
void run_quote_cycles(strategies::LadderMarketMaker& ladder, int cycles,
                       std::chrono::milliseconds settle = 15ms) {
    for (int i = 0; i < cycles; ++i) {
        (void)ladder.on_quote_cycle();
        // Long enough for the resulting orders to round-trip and for the
        // feed to deliver the events they caused, so the next cycle acts on
        // an up-to-date position and the momentum strategy gets a steady
        // stream of midpoints rather than a burst.
        std::this_thread::sleep_for(settle);
    }
}

} // namespace

// The headline test: two participants, the real exchange between them, and
// trades that could only have happened by going all the way around the loop.
TEST(MarketSimulatorE2e, TheTwoParticipantsTradeWithEachOtherThroughTheRealExchange) {
    Exchange exchange;
    ASSERT_TRUE(exchange.started());

    Participant market_maker(kMarketMakerAccount, "127.0.0.1", exchange.tcp_port());
    Participant momentum(kMomentumAccount, "127.0.0.1", exchange.tcp_port());
    ASSERT_TRUE(market_maker.connected());
    ASSERT_TRUE(momentum.connected());
    market_maker.seed_local_mirrors();
    momentum.seed_local_mirrors();

    strategies::LadderMarketMaker ladder(market_maker.gated(), market_maker.net_position_source(),
                                          ladder_config(/*seed=*/7));
    strategies::MomentumStrategy strategy(momentum.gated(), momentum.net_position_source(), momentum_config());

    strategies::StrategyRuntime runtime;
    // Only the momentum strategy subscribes: the market maker is driven by
    // its own walk, for the reason apps/market_simulator documents.
    runtime.subscribe(kInstrument, strategy.book_update_sink());

    trader::market_data::FeedSubscriber feed(exchange.market_data_port(), runtime);
    ASSERT_TRUE(feed.start());

    run_quote_cycles(ladder, /*cycles=*/60);

    // 1. The feed saw the market maker's orders, which means the events came
    // back out of the matching engine and over UDP.
    ASSERT_TRUE(wait_until([&] { return feed.stats().adds > 0; }));
    EXPECT_EQ(feed.stats().decode_failures, 0u);
    EXPECT_EQ(feed.stats().book_errors, 0u);

    // 2. The momentum strategy acted on what it read from that feed. This is
    // the half of the loop that cannot be faked: it has no other input.
    ASSERT_GT(strategy.updates_seen(), 0u);
    ASSERT_TRUE(wait_until([&] { return momentum.requests_sent() > 0; }))
        << "the momentum strategy never traded; it saw " << strategy.updates_seen() << " book updates";

    // 3. Trades actually matched, and both sides were told about their own.
    ASSERT_TRUE(wait_until([&] { return feed.snapshot(kInstrument).trade_count > 0; }));
    ASSERT_TRUE(wait_until([&] { return momentum.snapshot().fill_count > 0; }));
    ASSERT_TRUE(wait_until([&] { return market_maker.snapshot().fill_count > 0; }));

    // 4. The two participants are each other's counterparty and nobody else
    // is trading, so their signed positions must be exact mirrors and their
    // traded quantities equal.
    ASSERT_TRUE(wait_until([&] {
        return market_maker.snapshot().position == -momentum.snapshot().position &&
               market_maker.snapshot().filled_quantity == momentum.snapshot().filled_quantity;
    })) << "market maker position " << market_maker.snapshot().position << " vs momentum "
        << momentum.snapshot().position;

    // 5. The feed's own trade tally agrees with the fills reported privately
    // over TCP -- the public and private views of the same matching engine.
    const auto market = feed.snapshot(kInstrument);
    EXPECT_EQ(market.traded_quantity, momentum.snapshot().filled_quantity);

    // 6. P&L is internally consistent for both, at a mark taken from the
    // same feed.
    const auto mark = market.mark_price();
    ASSERT_TRUE(mark.has_value());
    for (const Participant* participant : {&market_maker, &momentum}) {
        const auto snap = participant->snapshot(mark);
        EXPECT_EQ(snap.total, snap.realized + snap.unrealized);
        if (snap.position == 0) {
            EXPECT_EQ(snap.unrealized, 0);
        }
    }

    (void)ladder.withdraw_all();
}

// The market maker's stated purpose, tested on its own: make an empty book
// continuously tradeable.
TEST(MarketSimulatorE2e, TheMarketMakerAloneTurnsAnEmptyBookIntoATwoSidedMarket) {
    Exchange exchange;
    ASSERT_TRUE(exchange.started());

    Participant market_maker(kMarketMakerAccount, "127.0.0.1", exchange.tcp_port());
    ASSERT_TRUE(market_maker.connected());
    market_maker.seed_local_mirrors();

    const auto config = ladder_config(/*seed=*/11);
    strategies::LadderMarketMaker ladder(market_maker.gated(), market_maker.net_position_source(), config);

    strategies::StrategyRuntime runtime; // nothing subscribed; the feed is only observed here
    trader::market_data::FeedSubscriber feed(exchange.market_data_port(), runtime);
    ASSERT_TRUE(feed.start());

    // One cycle is enough to populate both sides -- there is nothing to
    // trade against, so nothing gets filled and every level rests.
    run_quote_cycles(ladder, /*cycles=*/1);

    ASSERT_TRUE(wait_until([&] {
        const auto market = feed.snapshot(kInstrument);
        return market.best_bid.has_value() && market.best_ask.has_value();
    }));

    const auto market = feed.snapshot(kInstrument);
    const Price reference = ladder.reference_price();
    // The touch is the nearest level of the ladder on each side, and the
    // market is uncrossed by construction.
    EXPECT_EQ(market.best_bid->price, reference - config.half_spread);
    EXPECT_EQ(market.best_ask->price, reference + config.half_spread);
    EXPECT_EQ(market.best_bid->aggregate_quantity, config.quote_size);
    EXPECT_EQ(market.best_ask->aggregate_quantity, config.quote_size);
    EXPECT_LT(market.best_bid->price, market.best_ask->price);

    // Both sides of the ladder, all levels, are resting on the exchange.
    EXPECT_EQ(feed.stats().adds, 2 * config.levels_per_side);

    (void)ladder.withdraw_all();
}

TEST(MarketSimulatorE2e, TheBookKeepsEvolvingRatherThanRestingStatic) {
    Exchange exchange;
    ASSERT_TRUE(exchange.started());

    Participant market_maker(kMarketMakerAccount, "127.0.0.1", exchange.tcp_port());
    ASSERT_TRUE(market_maker.connected());
    market_maker.seed_local_mirrors();

    strategies::LadderMarketMaker ladder(market_maker.gated(), market_maker.net_position_source(),
                                          ladder_config(/*seed=*/3));
    strategies::StrategyRuntime runtime;
    trader::market_data::FeedSubscriber feed(exchange.market_data_port(), runtime);
    ASSERT_TRUE(feed.start());

    run_quote_cycles(ladder, /*cycles=*/1);
    ASSERT_TRUE(wait_until([&] { return feed.snapshot(kInstrument).best_bid.has_value(); }));
    const Price first_bid = feed.snapshot(kInstrument).best_bid->price;
    const auto initial_stats = feed.stats();

    // Over enough cycles the seeded walk is certain to have moved (a flat
    // step is possible, a run of 40 of them is not), and each move is a real
    // ReplaceOrder round trip through the exchange.
    run_quote_cycles(ladder, /*cycles=*/40);

    EXPECT_TRUE(wait_until([&] {
        const auto market = feed.snapshot(kInstrument);
        return market.best_bid.has_value() && market.best_bid->price != first_bid;
    })) << "the best bid never moved from " << first_bid;

    // A repriced quote reaches the public feed as a removal followed by an
    // add, not as a modify: a price change loses queue priority, so the
    // matching engine takes the order off the book and puts a new one on
    // (MarketDataPublisher only emits ModifyOrder for a reduction in place).
    // Asserting on those two counters is therefore asserting that requotes
    // genuinely travelled through the exchange, rather than that the ladder
    // recomputed a price locally.
    const auto stats = feed.stats();
    EXPECT_GT(stats.cancels, initial_stats.cancels);
    EXPECT_GT(stats.adds, initial_stats.adds);

    (void)ladder.withdraw_all();
}

TEST(MarketSimulatorE2e, WithdrawingTheLadderLeavesTheExchangeBookEmpty) {
    Exchange exchange;
    ASSERT_TRUE(exchange.started());

    Participant market_maker(kMarketMakerAccount, "127.0.0.1", exchange.tcp_port());
    ASSERT_TRUE(market_maker.connected());
    market_maker.seed_local_mirrors();

    const auto config = ladder_config(/*seed=*/5);
    strategies::LadderMarketMaker ladder(market_maker.gated(), market_maker.net_position_source(), config);
    strategies::StrategyRuntime runtime;
    trader::market_data::FeedSubscriber feed(exchange.market_data_port(), runtime);
    ASSERT_TRUE(feed.start());

    run_quote_cycles(ladder, /*cycles=*/3);
    ASSERT_TRUE(wait_until([&] {
        const auto market = feed.snapshot(kInstrument);
        return market.best_bid.has_value() && market.best_ask.has_value();
    }));

    // Why this matters beyond tidiness: a resting order outlives the session
    // that placed it, so a simulator that exits without withdrawing leaves
    // quotes the next run would trade against.
    EXPECT_EQ(ladder.withdraw_all(), 2 * config.levels_per_side);
    EXPECT_TRUE(wait_until([&] {
        const auto market = feed.snapshot(kInstrument);
        return !market.best_bid.has_value() && !market.best_ask.has_value();
    }));
}

// Reproducibility, at the level the simulator actually claims it: the
// reference-price path and therefore the quote prices are a function of the
// seed alone, even though which quotes fill is not.
TEST(MarketSimulatorE2e, TwoRunsAtTheSameSeedQuoteTheSamePricesOverTheRealFeed) {
    constexpr int kCycles = 12;

    // Collects the best bid the feed reports after each quote cycle of a
    // full, independent run of the stack.
    const auto run = [](std::uint64_t seed) {
        std::vector<Price> best_bids;
        Exchange exchange;
        EXPECT_TRUE(exchange.started());

        Participant market_maker(kMarketMakerAccount, "127.0.0.1", exchange.tcp_port());
        EXPECT_TRUE(market_maker.connected());
        market_maker.seed_local_mirrors();

        const auto config = ladder_config(seed);
        strategies::LadderMarketMaker ladder(market_maker.gated(), market_maker.net_position_source(), config);
        strategies::StrategyRuntime runtime;
        trader::market_data::FeedSubscriber feed(exchange.market_data_port(), runtime);
        EXPECT_TRUE(feed.start());

        for (int cycle = 0; cycle < kCycles; ++cycle) {
            (void)ladder.on_quote_cycle();
            const Price expected_bid = ladder.reference_price() - config.half_spread;
            // Waiting for the feed to catch up to the expected price, rather
            // than sampling whatever is there, is what makes this a
            // statement about the feed and not about timing.
            EXPECT_TRUE(wait_until([&] {
                const auto market = feed.snapshot(kInstrument);
                return market.best_bid.has_value() && market.best_bid->price == expected_bid;
            }));
            best_bids.push_back(feed.snapshot(kInstrument).best_bid->price);
        }

        (void)ladder.withdraw_all();
        return best_bids;
    };

    const auto first = run(/*seed=*/2024);
    const auto second = run(/*seed=*/2024);
    const auto different = run(/*seed=*/99);

    ASSERT_EQ(first.size(), static_cast<std::size_t>(kCycles));
    EXPECT_EQ(first, second);
    // And the seed genuinely determines the path, rather than every run
    // happening to look alike.
    EXPECT_NE(first, different);
}
