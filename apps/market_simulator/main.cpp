// market_simulator: simulated market participants trading a live MDH
// exchange exactly the way external traders would.
//
// Two participants run side by side, each as its own account over its own
// TCP session, both reading the same real UDP market-data feed:
//
//   LadderMarketMaker  -- quotes a two-sided ladder around a seeded random
//                         walk, so the book is continuously tradeable and
//                         keeps moving. Account 9002.
//   MomentumStrategy   -- buys when the reconstructed midpoint has risen
//                         over a short window, sells when it has fallen.
//                         Account 9003.
//
// ── The path an order actually takes ──────────────────────────────────────
// Nothing here shortcuts the exchange. This process holds no MatchingEngine,
// MatchingBook, RiskEngine or Ledger -- it cannot, since it links only the
// trader-side components and reaches the exchange through exactly two
// sockets:
//
//   strategy -> TraderRiskGatedOms -> OrderManagementSystem
//     -> protocol::order_entry encoder -> OrderEntryClient -> TCP
//       -> OrderEntryGateway -> MatchingPipeline -> risk + ledger
//         -> MatchingEngine -> ExchangeEvent
//           -> execution report -> TCP -> OrderEntryClient
//              -> OrderManagementSystem -> PositionTracker + PnlTracker
//           -> MarketDataPublisher -> SPSC router -> UDP
//              -> FeedSubscriber -> apply_frame_result -> BookManager
//                 -> StrategyRuntime -> strategy
//
// Both halves of that loop are why this app exists: the exchange is
// exercised end to end, rather than a matcher being called in a simulation
// loop.
//
// ── Threads ───────────────────────────────────────────────────────────────
//   feed thread     FeedSubscriber's own: UDP -> book -> StrategyRuntime ->
//                   MomentumStrategy. The only thread that touches the
//                   momentum strategy.
//   quote thread    a fixed-interval loop calling LadderMarketMaker::
//                   on_quote_cycle(). The only thread that touches the
//                   market maker.
//   two reader      one per OrderEntryClient: execution reports -> OMS ->
//   threads         PositionTracker + PnlTracker (both mutex-guarded).
//   main thread     status display, then the shutdown summary.
//
// Each strategy therefore has exactly one writer and needs no lock of its
// own; everything shared between threads (the OMS, the reconstructed book,
// the P&L trackers, the request counters) is already synchronized.
//
// ── Reproducibility ───────────────────────────────────────────────────────
// --seed fully determines the market maker's reference-price path: the walk
// takes exactly one draw per quote cycle and is advanced by cycle count, not
// by elapsed time, so cycle k has the same price on every run (see
// reference_price_walk.hpp). The desired quote ladder is a pure function of
// that price, and the momentum strategy's decision function is a pure
// function of the midpoints it observes.
//
// What is NOT reproducible, and cannot be while orders travel over real
// sockets, is which of those quotes actually fills -- that depends on
// TCP/UDP interleaving and thread scheduling. --steps bounds a run by quote
// cycles rather than wall-clock so that two runs at one seed process the
// same reference-price path and land close on aggregate statistics; the
// summary prints the seed and cycle count so any run can be re-invoked
// identically.
//
// ── Prerequisites ─────────────────────────────────────────────────────────
// A running trading_server publishing market data to this app's own UDP
// port, which means giving it that port in addition to the UI gateway's:
//
//   trading_server --market-data-port 7001 --market-data-port 7002
//   market_simulator --market-data-port 7002
//
// trading_server seeds accounts 9002/9003 on the exchange side before its
// gateway starts; --seed-cash/--seed-position below seed only this process's
// own local mirrors, the same documented-but-not-enforced agreement between
// two processes that apps/live_strategy_demo already relies on.
//
// Usage:
//   market_simulator [--host 127.0.0.1] [--tcp-port 7000]
//                     [--market-data-port 7002] [--instrument 1]
//                     [--seed 42] [--initial-price 1000000]
//                     [--quote-interval-ms 250] [--steps 0]
//                     [--status-interval-ms 2000]
//                     [--mm-levels 2] [--mm-half-spread 100]
//                     [--mm-level-spacing 100] [--mm-quote-size 100]
//                     [--mm-max-position 2000] [--mm-walk-step 100]
//                     [--momentum-lookback 20] [--momentum-threshold 100]
//                     [--momentum-trade-size 50] [--momentum-max-position 500]
//                     [--momentum-cooldown 20]
//                     [--seed-cash <ticks>] [--seed-position <qty>]
//
// Every price argument is in Price ticks, where 1 tick is 0.0001 (see
// common/types.hpp): 1000000 is 100.0000 and 100 is 0.0100.
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <variant>

#include "common/types.hpp"
#include "trader/market_data/feed_subscriber.hpp"
#include "trader/oms/order_entry_client.hpp"
#include "trader/positions/pnl_tracker.hpp"
#include "trader/risk/trader_risk_gated_oms.hpp"
#include "trader/strategies/ladder_market_maker.hpp"
#include "trader/strategies/momentum_strategy.hpp"
#include "trader/strategies/strategy_runtime.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::trader;

namespace {

// Must match apps/trading_server/main.cpp's own kSim* constants -- see this
// file's own note on the seeding agreement between the two processes.
constexpr AccountId kMarketMakerAccountId = 9002;
constexpr AccountId kMomentumAccountId = 9003;

// Ticks per whole currency unit, from Price's fixed 4-decimal scale.
constexpr Price kTicksPerUnit = 10'000;

struct Args {
    std::string host = "127.0.0.1";
    std::uint16_t tcp_port = 7000;
    std::uint16_t market_data_port = 7002;
    InstrumentId instrument_id = 1;

    std::uint64_t seed = 42;
    Price initial_price = 1'000'000; // 100.0000
    Price walk_step = 100;           // 0.0100

    int quote_interval_ms = 250;
    std::uint64_t steps = 0; // 0 means run until Ctrl+C
    int status_interval_ms = 2000;

    std::size_t mm_levels = 2;
    Price mm_half_spread = 100;
    Price mm_level_spacing = 100;
    Quantity mm_quote_size = 100;
    positions::NetPosition mm_max_position = 2'000;

    std::size_t momentum_lookback = 20;
    Price momentum_threshold = 100;
    Quantity momentum_trade_size = 50;
    positions::NetPosition momentum_max_position = 500;
    std::size_t momentum_cooldown = 20;

    positions::Balance seed_cash = 100'000'000'000'000;
    Quantity seed_position = 1'000'000;
};

std::optional<Args> parse_args(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        const std::string flag = argv[i];
        auto next = [&]() -> std::optional<std::string> {
            if (i + 1 >= argc) return std::nullopt;
            return std::string(argv[++i]);
        };

        try {
            if (flag == "--host") {
                auto v = next();
                if (!v) return std::nullopt;
                args.host = *v;
            } else if (flag == "--tcp-port") {
                auto v = next();
                if (!v) return std::nullopt;
                args.tcp_port = static_cast<std::uint16_t>(std::stoul(*v));
            } else if (flag == "--market-data-port") {
                auto v = next();
                if (!v) return std::nullopt;
                args.market_data_port = static_cast<std::uint16_t>(std::stoul(*v));
            } else if (flag == "--instrument") {
                auto v = next();
                if (!v) return std::nullopt;
                args.instrument_id = static_cast<InstrumentId>(std::stoul(*v));
            } else if (flag == "--seed") {
                auto v = next();
                if (!v) return std::nullopt;
                args.seed = std::stoull(*v);
            } else if (flag == "--initial-price") {
                auto v = next();
                if (!v) return std::nullopt;
                args.initial_price = static_cast<Price>(std::stoll(*v));
            } else if (flag == "--mm-walk-step") {
                auto v = next();
                if (!v) return std::nullopt;
                args.walk_step = static_cast<Price>(std::stoll(*v));
            } else if (flag == "--quote-interval-ms") {
                auto v = next();
                if (!v) return std::nullopt;
                args.quote_interval_ms = std::stoi(*v);
            } else if (flag == "--steps") {
                auto v = next();
                if (!v) return std::nullopt;
                args.steps = std::stoull(*v);
            } else if (flag == "--status-interval-ms") {
                auto v = next();
                if (!v) return std::nullopt;
                args.status_interval_ms = std::stoi(*v);
            } else if (flag == "--mm-levels") {
                auto v = next();
                if (!v) return std::nullopt;
                args.mm_levels = std::stoull(*v);
            } else if (flag == "--mm-half-spread") {
                auto v = next();
                if (!v) return std::nullopt;
                args.mm_half_spread = static_cast<Price>(std::stoll(*v));
            } else if (flag == "--mm-level-spacing") {
                auto v = next();
                if (!v) return std::nullopt;
                args.mm_level_spacing = static_cast<Price>(std::stoll(*v));
            } else if (flag == "--mm-quote-size") {
                auto v = next();
                if (!v) return std::nullopt;
                args.mm_quote_size = static_cast<Quantity>(std::stoull(*v));
            } else if (flag == "--mm-max-position") {
                auto v = next();
                if (!v) return std::nullopt;
                args.mm_max_position = static_cast<positions::NetPosition>(std::stoll(*v));
            } else if (flag == "--momentum-lookback") {
                auto v = next();
                if (!v) return std::nullopt;
                args.momentum_lookback = std::stoull(*v);
            } else if (flag == "--momentum-threshold") {
                auto v = next();
                if (!v) return std::nullopt;
                args.momentum_threshold = static_cast<Price>(std::stoll(*v));
            } else if (flag == "--momentum-trade-size") {
                auto v = next();
                if (!v) return std::nullopt;
                args.momentum_trade_size = static_cast<Quantity>(std::stoull(*v));
            } else if (flag == "--momentum-max-position") {
                auto v = next();
                if (!v) return std::nullopt;
                args.momentum_max_position = static_cast<positions::NetPosition>(std::stoll(*v));
            } else if (flag == "--momentum-cooldown") {
                auto v = next();
                if (!v) return std::nullopt;
                args.momentum_cooldown = std::stoull(*v);
            } else if (flag == "--seed-cash") {
                auto v = next();
                if (!v) return std::nullopt;
                args.seed_cash = static_cast<positions::Balance>(std::stoll(*v));
            } else if (flag == "--seed-position") {
                auto v = next();
                if (!v) return std::nullopt;
                args.seed_position = static_cast<Quantity>(std::stoull(*v));
            } else {
                std::cerr << "unrecognized argument: " << flag << "\n";
                return std::nullopt;
            }
        } catch (const std::exception&) {
            std::cerr << "invalid value for " << flag << "\n";
            return std::nullopt;
        }
    }
    if (args.mm_levels == 0 || args.quote_interval_ms <= 0 || args.momentum_lookback == 0) {
        std::cerr << "--mm-levels, --quote-interval-ms and --momentum-lookback must all be positive\n";
        return std::nullopt;
    }
    return args;
}

void print_usage() {
    std::cerr << "Usage: market_simulator [--host <ip>] [--tcp-port <port>] [--market-data-port <port>]\n"
              << "                         [--instrument <id>] [--seed <n>] [--initial-price <ticks>]\n"
              << "                         [--quote-interval-ms <ms>] [--steps <n>] [--status-interval-ms <ms>]\n"
              << "                         [--mm-levels <n>] [--mm-half-spread <ticks>]\n"
              << "                         [--mm-level-spacing <ticks>] [--mm-quote-size <qty>]\n"
              << "                         [--mm-max-position <qty>] [--mm-walk-step <ticks>]\n"
              << "                         [--momentum-lookback <n>] [--momentum-threshold <ticks>]\n"
              << "                         [--momentum-trade-size <qty>] [--momentum-max-position <qty>]\n"
              << "                         [--momentum-cooldown <n>]\n"
              << "                         [--seed-cash <ticks>] [--seed-position <qty>]\n"
              << "\nRequires a running trading_server publishing to --market-data-port; see this\n"
                 "app's own top-of-file comment.\n";
}

std::atomic<bool> g_stop_requested{false};
void on_sigint(int) { g_stop_requested.store(true, std::memory_order_relaxed); }

// ── Display formatting ────────────────────────────────────────────────────
// Prices and money are integer ticks internally (common/types.hpp) and are
// only ever converted to a decimal string here, at the display boundary --
// never carried as a double anywhere a decision is made.
[[nodiscard]] std::string format_price(Price ticks) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(2) << (static_cast<double>(ticks) / static_cast<double>(kTicksPerUnit));
    return out.str();
}

[[nodiscard]] std::string format_money(positions::Balance ticks) {
    std::ostringstream out;
    out << std::showpos << std::fixed << std::setprecision(2)
        << (static_cast<double>(ticks) / static_cast<double>(kTicksPerUnit));
    return out.str();
}

[[nodiscard]] std::string format_signed(positions::NetPosition value) {
    std::ostringstream out;
    out << std::showpos << value;
    return out.str();
}

// Thousands separators for the counters in the status block, matching the
// "12,481" in this app's intended output without imposing a locale on the
// whole stream.
[[nodiscard]] std::string format_count(std::uint64_t value) {
    std::string digits = std::to_string(value);
    std::string out;
    out.reserve(digits.size() + digits.size() / 3);
    const std::size_t leading = digits.size() % 3 == 0 ? 3 : digits.size() % 3;
    for (std::size_t i = 0; i < digits.size(); ++i) {
        if (i >= leading && (i - leading) % 3 == 0) {
            out.push_back(',');
        }
        out.push_back(digits[i]);
    }
    return out;
}

[[nodiscard]] std::string format_level(const std::optional<book::PriceLevelView>& level) {
    if (!level) {
        return "(none)";
    }
    return format_price(level->price) + " x " + std::to_string(level->aggregate_quantity);
}

// ── One simulated participant ─────────────────────────────────────────────
// The full trader-side stack for one account: a real OrderEntryClient over
// a real TCP connection, a real TraderRiskGatedOms (trader-side risk +
// position mirror + OMS) on top of it, and a PnlTracker watching the same
// fill stream through the OMS's extra fill sink.
//
// Every wire request is counted on its way out, by wrapping the sender the
// OMS was given rather than by asking the strategies to self-report: this
// counts what genuinely reached the socket, including requests a strategy
// does not know it caused (a replace is one request, not a cancel plus a
// new order).
class Participant {
public:
    Participant(std::string name, AccountId account_id, const std::string& host, std::uint16_t tcp_port)
        : name_(std::move(name)),
          account_id_(account_id),
          gated_(
              account_id,
              [this](const protocol::order_entry::Message& message) {
                  const bool sent = client_.send(message);
                  if (sent) {
                      count_request(message);
                  }
                  return sent;
              },
              nullptr, risk::TraderRiskLimits{}, pnl_.sink()),
          client_([this](const protocol::order_entry::Message& message) { gated_.handle_message(message); }) {
        connected_ = client_.connect(host, tcp_port);
    }

    [[nodiscard]] bool connected() const { return connected_; }
    [[nodiscard]] const std::string& name() const { return name_; }
    [[nodiscard]] AccountId account_id() const { return account_id_; }
    [[nodiscard]] risk::TraderRiskGatedOms& gated() { return gated_; }
    [[nodiscard]] const positions::PnlTracker& pnl() const { return pnl_; }

    void disconnect() { client_.disconnect(); }

    // The seam LadderMarketMaker and MomentumStrategy each take: this
    // participant's own signed net traded position in `instrument_id`.
    [[nodiscard]] strategies::LadderMarketMaker::NetPositionSource net_position_source(InstrumentId instrument_id) {
        return [this, instrument_id] { return pnl_.snapshot(account_id_, instrument_id).position; };
    }

    [[nodiscard]] std::uint64_t new_orders() const { return new_orders_.load(std::memory_order_relaxed); }
    [[nodiscard]] std::uint64_t replaces() const { return replaces_.load(std::memory_order_relaxed); }
    [[nodiscard]] std::uint64_t cancels() const { return cancels_.load(std::memory_order_relaxed); }
    // A replace is an order too -- it puts a new, differently-priced order
    // on the book, so counting only NewOrder would undercount a market
    // maker's activity by most of it.
    [[nodiscard]] std::uint64_t orders_sent() const { return new_orders() + replaces(); }
    [[nodiscard]] std::uint64_t requests_sent() const { return orders_sent() + cancels(); }

private:
    void count_request(const protocol::order_entry::Message& message) {
        std::visit(
            [this](const auto& request) {
                using T = std::decay_t<decltype(request)>;
                if constexpr (std::is_same_v<T, protocol::order_entry::NewOrder>) {
                    new_orders_.fetch_add(1, std::memory_order_relaxed);
                } else if constexpr (std::is_same_v<T, protocol::order_entry::ReplaceOrder>) {
                    replaces_.fetch_add(1, std::memory_order_relaxed);
                } else if constexpr (std::is_same_v<T, protocol::order_entry::CancelOrder>) {
                    cancels_.fetch_add(1, std::memory_order_relaxed);
                }
            },
            message);
    }

    std::string name_;
    AccountId account_id_;

    // Declaration order is teardown order, in reverse, and both steps of it
    // matter here:
    //   - pnl_ before gated_, because gated_ holds pnl_.sink().
    //   - gated_ before client_, because client_'s reader thread calls
    //     gated_.handle_message() asynchronously, so that thread has to be
    //     joined (which client_'s destructor does) before gated_ goes away.
    // The second is the exact use-after-free TSan caught in this codebase's
    // test helpers; see docs/end_to_end_architecture.md's own note on it.
    std::atomic<std::uint64_t> new_orders_{0};
    std::atomic<std::uint64_t> replaces_{0};
    std::atomic<std::uint64_t> cancels_{0};
    positions::PnlTracker pnl_;
    risk::TraderRiskGatedOms gated_;
    oms::OrderEntryClient client_;
    bool connected_ = false;
};

// What the display shows about the two strategies, published by whichever
// thread owns each strategy and read by the main thread.
//
// The strategies themselves are deliberately lock-free single-writer
// objects, so reading LadderMarketMaker::reference_price() or
// MomentumStrategy::signal() straight from the display thread would be a
// plain data race on state the quote and feed threads are mutating -- the
// same class of bug TSan already caught in this codebase for
// OrderEntryGateway::snapshot(). Publishing into atomics from the owning
// thread, immediately after it finishes a cycle or an update, keeps the
// strategies free of synchronization they otherwise have no need for.
struct DisplayState {
    std::atomic<Price> reference_price{0};
    std::atomic<std::uint64_t> quote_cycles{0};
    std::atomic<Price> momentum_signal{0};
    std::atomic<bool> momentum_signal_ready{false};
    std::atomic<std::uint64_t> momentum_updates{0};
};

void print_participant(const Participant& participant, InstrumentId instrument_id, std::optional<Price> mark) {
    const auto snap = participant.pnl().snapshot(participant.account_id(), instrument_id, mark);
    std::cout << participant.name() << "\n"
              << "  Position:        " << format_signed(snap.position) << "\n"
              << "  Avg entry:       " << (snap.position == 0 ? "-" : format_price(snap.average_entry_price)) << "\n"
              << "  Realized P&L:    " << format_money(snap.realized) << "\n"
              << "  Unrealized P&L:  " << format_money(snap.unrealized) << "\n"
              << "  Total P&L:       " << format_money(snap.total) << "\n"
              << "  Orders sent:     " << format_count(participant.orders_sent()) << "\n"
              << "  Fills:           " << format_count(snap.fill_count) << "\n";
}

void print_status(const market_data::FeedSubscriber& feed, const Participant& market_maker,
                   const Participant& momentum, const DisplayState& display, InstrumentId instrument_id) {
    const auto market = feed.snapshot(instrument_id);
    const auto mark = market.mark_price();
    const std::uint64_t orders_sent = market_maker.orders_sent() + momentum.orders_sent();

    std::cout << "\n===== MDH MARKET SIMULATION =====\n\n"
              << "Instrument: " << instrument_id << "\n\n"
              << "Best Bid:  " << format_level(market.best_bid) << "\n"
              << "Best Ask:  " << format_level(market.best_ask) << "\n"
              << "Last:      " << (market.trade_count == 0 ? "(none)" : format_price(market.last_trade_price)) << "\n\n"
              << "Trades:        " << format_count(market.trade_count) << "\n"
              << "Traded qty:    " << format_count(market.traded_quantity) << "\n"
              << "Orders sent:   " << format_count(orders_sent) << "\n"
              << "Feed messages: " << format_count(feed.stats().messages_processed) << "\n\n";
    print_participant(market_maker, instrument_id, mark);
    std::cout << "  Reference:       " << format_price(display.reference_price.load(std::memory_order_relaxed))
               << " (cycle " << format_count(display.quote_cycles.load(std::memory_order_relaxed)) << ")\n\n";
    print_participant(momentum, instrument_id, mark);
    std::cout << "  Signal:          "
               << (display.momentum_signal_ready.load(std::memory_order_relaxed)
                        ? format_money(display.momentum_signal.load(std::memory_order_relaxed))
                        : "(warming up)")
               << " over " << format_count(display.momentum_updates.load(std::memory_order_relaxed)) << " updates\n";
    std::cout.flush();
}

void print_summary(const market_data::FeedSubscriber& feed, const Participant& market_maker,
                    const Participant& momentum, const DisplayState& display, InstrumentId instrument_id,
                    const Args& args) {
    const auto market = feed.snapshot(instrument_id);
    const auto mark = market.mark_price();

    std::cout << "\n===== SIMULATION SUMMARY =====\n\n"
              << "Instrument:        " << instrument_id << "\n"
              << "Seed:              " << args.seed << "\n"
              << "Quote cycles:      " << format_count(display.quote_cycles.load(std::memory_order_relaxed)) << "\n"
              << "Final reference:   " << format_price(display.reference_price.load(std::memory_order_relaxed))
              << "\n"
              << "Mark price:        " << (mark ? format_price(*mark) : "(none)") << "\n\n"
              << "Market (from the UDP feed)\n"
              << "  Trades:          " << format_count(market.trade_count) << "\n"
              << "  Traded quantity: " << format_count(market.traded_quantity) << "\n"
              << "  Best bid:        " << format_level(market.best_bid) << "\n"
              << "  Best ask:        " << format_level(market.best_ask) << "\n\n"
              << "Feed health\n"
              << "  Packets:         " << format_count(feed.packets_received()) << " (" << feed.packet_errors()
              << " malformed)\n"
              << "  Messages:        " << format_count(feed.stats().messages_processed) << "\n"
              << "  Decode failures: " << feed.stats().decode_failures << "\n"
              << "  Sequence gaps:   " << feed.stats().sequence_failures << "\n"
              << "  Book errors:     " << feed.stats().book_errors << "\n\n";

    for (const Participant* participant : {&market_maker, &momentum}) {
        const auto snap = participant->pnl().snapshot(participant->account_id(), instrument_id, mark);
        std::cout << participant->name() << " (account " << participant->account_id() << ")\n"
                   << "  New orders:      " << format_count(participant->new_orders()) << "\n"
                   << "  Replaces:        " << format_count(participant->replaces()) << "\n"
                   << "  Cancels:         " << format_count(participant->cancels()) << "\n"
                   << "  Requests total:  " << format_count(participant->requests_sent()) << "\n"
                   << "  Fills:           " << format_count(snap.fill_count) << "\n"
                   << "  Traded quantity: " << format_count(snap.filled_quantity) << "\n"
                   << "  Position:        " << format_signed(snap.position) << "\n"
                   << "  Avg entry:       " << (snap.position == 0 ? "-" : format_price(snap.average_entry_price))
                   << "\n"
                   << "  Realized P&L:    " << format_money(snap.realized) << "\n"
                   << "  Unrealized P&L:  " << format_money(snap.unrealized) << "\n"
                   << "  Total P&L:       " << format_money(snap.total) << "\n\n";
    }
    std::cout.flush();
}

} // namespace

int main(int argc, char** argv) {
    auto args = parse_args(argc, argv);
    if (!args) {
        print_usage();
        return EXIT_FAILURE;
    }

    Participant market_maker("Market Maker", kMarketMakerAccountId, args->host, args->tcp_port);
    Participant momentum("Momentum Strategy", kMomentumAccountId, args->host, args->tcp_port);
    if (!market_maker.connected() || !momentum.connected()) {
        std::cerr << "failed to connect to the order-entry gateway at " << args->host << ":" << args->tcp_port
                   << " -- is trading_server running?\n";
        return EXIT_FAILURE;
    }

    // Local mirrors only. The exchange-side balances for these two accounts
    // must already have been seeded by trading_server before its gateway
    // started; see this file's own top comment.
    for (Participant* participant : {&market_maker, &momentum}) {
        participant->gated().deposit_cash(args->seed_cash);
        participant->gated().deposit_position(args->instrument_id, args->seed_position);
    }

    strategies::LadderMarketMakerConfig mm_config;
    mm_config.instrument_id = args->instrument_id;
    mm_config.walk = strategies::ReferencePriceWalkConfig{.initial_price = args->initial_price,
                                                            .step_size = args->walk_step,
                                                            .min_price = args->initial_price / 2,
                                                            .max_price = args->initial_price * 3 / 2,
                                                            .seed = args->seed};
    mm_config.levels_per_side = args->mm_levels;
    mm_config.half_spread = args->mm_half_spread;
    mm_config.level_spacing = args->mm_level_spacing;
    mm_config.quote_size = args->mm_quote_size;
    mm_config.max_position = args->mm_max_position;
    mm_config.requote_threshold = args->walk_step;
    strategies::LadderMarketMaker ladder(market_maker.gated(),
                                          market_maker.net_position_source(args->instrument_id), mm_config);

    const strategies::MomentumStrategyConfig momentum_config{.instrument_id = args->instrument_id,
                                                              .lookback = args->momentum_lookback,
                                                              .entry_threshold = args->momentum_threshold,
                                                              .trade_size = args->momentum_trade_size,
                                                              .max_position = args->momentum_max_position,
                                                              .cross_slack = args->walk_step,
                                                              .cooldown_updates = args->momentum_cooldown};
    strategies::MomentumStrategy strategy(momentum.gated(), momentum.net_position_source(args->instrument_id),
                                           momentum_config);

    DisplayState display;
    display.reference_price.store(args->initial_price, std::memory_order_relaxed);

    // The market maker is deliberately NOT subscribed here: its prices come
    // from its own reference-price walk, and quoting off a feed that
    // contains its own quotes would have it centring on itself.
    //
    // The momentum strategy's sink is wrapped so that the feed thread
    // publishes its post-update state for the display -- see DisplayState.
    strategies::StrategyRuntime runtime;
    runtime.subscribe(args->instrument_id, [&](InstrumentId id, const book::OrderBook& book) {
        strategy.on_book_update(id, book);
        const auto signal = strategy.signal();
        display.momentum_signal.store(signal.value_or(0), std::memory_order_relaxed);
        display.momentum_signal_ready.store(signal.has_value(), std::memory_order_relaxed);
        display.momentum_updates.store(strategy.updates_seen(), std::memory_order_relaxed);
    });

    market_data::FeedSubscriber feed(args->market_data_port, runtime);
    if (!feed.start()) {
        std::cerr << "failed to bind market-data UDP port " << args->market_data_port
                   << " -- already in use?\n";
        return EXIT_FAILURE;
    }

    std::cout << "market_simulator: instrument " << args->instrument_id << ", seed " << args->seed
              << ", reference " << format_price(args->initial_price) << "\n"
              << "  market maker      account " << kMarketMakerAccountId << ", " << args->mm_levels
              << " levels x " << args->mm_quote_size << " each side\n"
              << "  momentum strategy account " << kMomentumAccountId << ", lookback "
              << args->momentum_lookback << ", size " << args->momentum_trade_size << "\n"
              << "  order entry tcp:" << args->tcp_port << ", market data udp:" << args->market_data_port << "\n";
    if (args->steps > 0) {
        std::cout << "  stopping after " << args->steps << " quote cycles\n";
    } else {
        std::cout << "  press Ctrl+C to stop\n";
    }

    std::signal(SIGINT, on_sigint);

    // The quote thread. Separate from the feed thread precisely because the
    // market maker is driven by time rather than by market data -- it must
    // keep quoting through a market that has gone quiet, which is exactly
    // the situation an empty book starts in.
    std::jthread quote_thread([&](std::stop_token token) {
        while (!token.stop_requested() && !g_stop_requested.load(std::memory_order_relaxed)) {
            (void)ladder.on_quote_cycle();
            display.reference_price.store(ladder.reference_price(), std::memory_order_relaxed);
            display.quote_cycles.store(ladder.cycles(), std::memory_order_relaxed);
            if (args->steps > 0 && ladder.cycles() >= args->steps) {
                g_stop_requested.store(true, std::memory_order_relaxed);
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(args->quote_interval_ms));
        }
    });

    auto next_status = std::chrono::steady_clock::now() + std::chrono::milliseconds(args->status_interval_ms);
    while (!g_stop_requested.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (std::chrono::steady_clock::now() >= next_status) {
            print_status(feed, market_maker, momentum, display, args->instrument_id);
            next_status = std::chrono::steady_clock::now() + std::chrono::milliseconds(args->status_interval_ms);
        }
    }

    std::cout << "\nshutting down...\n";
    quote_thread.request_stop();
    quote_thread.join();

    // Withdrawing the ladder before disconnecting leaves the exchange's book
    // as this process found it. A resting order outlives the session that
    // placed it (see order_entry_gateway.hpp -- there is no
    // cancel-on-disconnect), so without this the book would keep this run's
    // quotes indefinitely, and a second run would trade against a ghost.
    (void)ladder.withdraw_all();
    // Long enough for those cancels to be acknowledged, so the summary's
    // fill counts include anything that traded on the way out. There is no
    // "drain complete" signal in the order-entry protocol to wait on
    // instead, and inventing one for a demo's shutdown is not worth it.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    feed.stop();
    print_summary(feed, market_maker, momentum, display, args->instrument_id, *args);

    momentum.disconnect();
    market_maker.disconnect();
    return EXIT_SUCCESS;
}
