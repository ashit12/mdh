// live_strategy_demo: Milestone 14's final demonstration -- a real
// MarketMakerStrategy (Milestone 10) trading through a real
// TraderRiskGatedOms + OrderEntryClient (Milestones 8-9) over a real TCP
// connection to a running apps/trading_server (Milestone 12), closing the
// exact gap trader/strategies/strategy_runtime.hpp's own class comment
// documented as "future, out-of-scope integration work" back when it was
// written: "no live UDP feed is wired up to a running gateway yet."
// Milestone 12 built that gateway; this app is what actually plugs a real
// strategy into it end to end.
//
// ── Why this polls GET /api/book/:id instead of subscribing to the UDP
//    market-data feed directly ───────────────────────────────────────────
// trading_server's MarketDataPublisher currently sends every packet to
// exactly one destination port (--market-data-port) -- see
// apps/trading_server/main.cpp's own extra_event_sink lambda. Teaching it
// to fan out to N subscriber ports so a second, independent UDP listener
// (this app reconstructing its own book::BookManager from scratch, the way
// UiGateway::market_data_loop() already does) could exist alongside
// UiGateway's own listener is real production networking work, out of
// scope for a demo binary whose actual point is exercising the strategy/
// risk/OMS/gateway stack, not re-deriving book reconstruction a third time
// (replay::apply_frame_result() and UiGateway::market_data_loop() already
// cover that). Instead, this app reuses the exact already-tested,
// already-running reconstruction UiGateway itself performs, via the same
// GET /api/book/:id REST endpoint ui/'s own dashboard polls/streams from --
// so "where does the book come from" is answered identically for a human
// watching the dashboard and for this strategy trading against it.
// MarketMakerStrategy::on_book_update() only ever reads best_bid()/
// best_ask() from whatever book::OrderBook it is handed (see
// market_maker_strategy.cpp), so a freshly-rebuilt-every-poll mirror built
// from that JSON response is a faithful, sufficient input -- it never
// needs per-order identity or history, only current top-of-book prices.
//
// ── Why this trades as its own, separate account id, never one of
//    UiGatewayOptions::demo_account_ids ───────────────────────────────────
// UiGateway::find_or_create_session() lazily opens its own real TCP
// OrderEntryClient connection for account_id the first time any HTTP
// request names it (see that method's own doc comment). Sharing an account
// with it is no longer *unsafe* -- OrderEntryGateway supports many
// sessions per account and routes each execution report back to the
// session that placed the order (see its own session doc comment), where
// it once mapped an account to exactly one connection and let a second one
// silently take over the first's reports. What sharing would still be is
// confusing: this app and a dashboard viewer would be two independent
// traders spending the same balance and appearing in each other's
// position, with neither one's view of "my account" telling the whole
// story. Trading as a dedicated account (default 9001, see
// kDefaultAccountId below) keeps the demonstration legible: this app's
// resting quotes are still fully visible to every dashboard viewer (the
// book is public, not scoped per account -- see
// UiGateway::handle_get_book()), and a human trader using the dashboard's
// own demo accounts can trade against them for real, which is exactly what
// docs/live_demo.md's walkthrough does.
//
// Usage:
//   live_strategy_demo [--host 127.0.0.1] [--tcp-port 7000] [--http-port 8080]
//                       [--account 9001] [--instrument 1] [--quote-size 10]
//                       [--half-spread 2] [--max-position 2000]
//                       [--requote-threshold 2] [--poll-interval-ms 200]
//                       [--seed-cash 10000000000] [--seed-position 1000]
//
// `--seed-cash`/`--seed-position` seed only this process's own *local*
// PositionTracker mirror (TraderRiskGatedOms::deposit_cash()/
// deposit_position(), same as every other trader-side test harness in
// this codebase) -- the actual exchange-side balance for `--account` must
// already have been deposited by trading_server itself before its
// gateway's start() was called (see that app's own main(), which seeds
// this exact account for this exact reason); these two numbers are a
// documented-but-not-runtime-enforced agreement between the two
// processes, the same convention ui_gateway.hpp's own class comment
// already accepts for market_data_udp_port_ agreeing with trading_server's
// --market-data-port.
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <thread>

#include <httplib.h>
#include <nlohmann/json.hpp>

#include "book/order_book.hpp"
#include "common/types.hpp"
#include "trader/oms/order_entry_client.hpp"
#include "trader/risk/trader_risk_gated_oms.hpp"
#include "trader/strategies/market_maker_strategy.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::trader;
using json = nlohmann::json;

namespace {

constexpr AccountId kDefaultAccountId = 9001; // see this file's own top comment on why this must never be a UiGatewayOptions::demo_account_ids member
constexpr InstrumentId kDefaultInstrumentId = 1;

struct Args {
    std::string host = "127.0.0.1";
    std::uint16_t tcp_port = 7000;
    std::uint16_t http_port = 8080;
    AccountId account_id = kDefaultAccountId;
    InstrumentId instrument_id = kDefaultInstrumentId;
    Quantity quote_size = 10;
    Price half_spread = 2;
    Quantity max_position = 2000;
    Price requote_threshold = 2;
    int poll_interval_ms = 200;
    positions::Balance seed_cash = 1'000'000'0000;
    Quantity seed_position = 1'000;
};

std::optional<Args> parse_args(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        const std::string flag = argv[i];
        auto next = [&]() -> std::optional<std::string> {
            if (i + 1 >= argc) return std::nullopt;
            return std::string(argv[++i]);
        };
        auto require = [&](std::string& target) {
            auto v = next();
            if (!v) return false;
            target = *v;
            return true;
        };

        try {
            if (flag == "--host") {
                std::string v;
                if (!require(v)) return std::nullopt;
                args.host = v;
            } else if (flag == "--tcp-port") {
                auto v = next();
                if (!v) return std::nullopt;
                args.tcp_port = static_cast<std::uint16_t>(std::stoul(*v));
            } else if (flag == "--http-port") {
                auto v = next();
                if (!v) return std::nullopt;
                args.http_port = static_cast<std::uint16_t>(std::stoul(*v));
            } else if (flag == "--account") {
                auto v = next();
                if (!v) return std::nullopt;
                args.account_id = static_cast<AccountId>(std::stoull(*v));
            } else if (flag == "--instrument") {
                auto v = next();
                if (!v) return std::nullopt;
                args.instrument_id = static_cast<InstrumentId>(std::stoul(*v));
            } else if (flag == "--quote-size") {
                auto v = next();
                if (!v) return std::nullopt;
                args.quote_size = static_cast<Quantity>(std::stoull(*v));
            } else if (flag == "--half-spread") {
                auto v = next();
                if (!v) return std::nullopt;
                args.half_spread = static_cast<Price>(std::stoll(*v));
            } else if (flag == "--max-position") {
                auto v = next();
                if (!v) return std::nullopt;
                args.max_position = static_cast<Quantity>(std::stoull(*v));
            } else if (flag == "--requote-threshold") {
                auto v = next();
                if (!v) return std::nullopt;
                args.requote_threshold = static_cast<Price>(std::stoll(*v));
            } else if (flag == "--poll-interval-ms") {
                auto v = next();
                if (!v) return std::nullopt;
                args.poll_interval_ms = std::stoi(*v);
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
    return args;
}

void print_usage() {
    std::cerr << "Usage: live_strategy_demo [--host <ip>] [--tcp-port <port>] [--http-port <port>]\n"
              << "                           [--account <id>] [--instrument <id>] [--quote-size <qty>]\n"
              << "                           [--half-spread <ticks>] [--max-position <qty>]\n"
              << "                           [--requote-threshold <ticks>] [--poll-interval-ms <ms>]\n"
              << "                           [--seed-cash <ticks>] [--seed-position <qty>]\n";
}

std::atomic<bool> g_stop_requested{false};
void on_sigint(int) { g_stop_requested.store(true, std::memory_order_relaxed); }

// A connected TraderRiskGatedOms for one account, over a real
// OrderEntryClient -- the same helper shape as
// tests/test_market_maker_strategy_e2e.cpp's own RiskGatedTrader (see
// that file's own comment on why gated_ is declared, and therefore
// destroyed, before client_).
class Trader {
public:
    Trader(AccountId account_id, const std::string& host, std::uint16_t port)
        : gated_(account_id, [this](const protocol::order_entry::Message& m) { return client_.send(m); }),
          client_([this](const protocol::order_entry::Message& m) { gated_.handle_message(m); }) {
        connected_ = client_.connect(host, port);
    }

    [[nodiscard]] bool connected() const { return connected_; }
    [[nodiscard]] risk::TraderRiskGatedOms& gated() { return gated_; }

private:
    risk::TraderRiskGatedOms gated_;
    oms::OrderEntryClient client_;
    bool connected_ = false;
};

// Rebuilds a throwaway book::OrderBook mirror from one GET /api/book/:id
// response -- see this file's own top comment on why polling that
// endpoint, rather than a second UDP listener, is this demo's book
// source. Every call gets fresh, never-reused synthetic OrderIds:
// MarketMakerStrategy::on_book_update() only reads best_bid()/best_ask()
// (see market_maker_strategy.cpp), so no cross-call order identity is
// ever needed.
[[nodiscard]] std::optional<book::OrderBook> fetch_book(httplib::Client& cli, InstrumentId instrument_id) {
    auto res = cli.Get("/api/book/" + std::to_string(instrument_id));
    if (!res || res->status != 200) return std::nullopt;

    json parsed;
    try {
        parsed = json::parse(res->body);
    } catch (const std::exception&) {
        return std::nullopt;
    }

    book::OrderBook mirror;
    OrderId next_id = 1;
    for (const auto& level : parsed.at("bids")) {
        (void)mirror.add_order(next_id++, level.at("price").get<Price>(), level.at("quantity").get<Quantity>(), Side::Buy);
    }
    for (const auto& level : parsed.at("asks")) {
        (void)mirror.add_order(next_id++, level.at("price").get<Price>(), level.at("quantity").get<Quantity>(), Side::Sell);
    }
    return mirror;
}

void print_status(const strategies::MarketMakerStrategy& mm, risk::TraderRiskGatedOms& trader, InstrumentId instrument_id) {
    std::cout << "[live_strategy_demo] position(" << instrument_id << ")=" << trader.position(instrument_id)
               << " cash=" << trader.cash();
    if (const auto id = mm.bid_order_id()) {
        if (const auto order = trader.order(*id)) {
            std::cout << " bid=" << order->price << "x" << order->remaining_quantity;
        }
    } else {
        std::cout << " bid=(none)";
    }
    if (const auto id = mm.ask_order_id()) {
        if (const auto order = trader.order(*id)) {
            std::cout << " ask=" << order->price << "x" << order->remaining_quantity;
        }
    } else {
        std::cout << " ask=(none)";
    }
    std::cout << "\n";
}

} // namespace

int main(int argc, char** argv) {
    auto args = parse_args(argc, argv);
    if (!args) {
        print_usage();
        return EXIT_FAILURE;
    }

    Trader trader(args->account_id, args->host, args->tcp_port);
    if (!trader.connected()) {
        std::cerr << "failed to connect to order-entry gateway at " << args->host << ":" << args->tcp_port << "\n";
        return EXIT_FAILURE;
    }
    // Local mirror seeding only -- see this file's own top comment on why
    // the exchange side must already have been seeded by trading_server.
    trader.gated().deposit_cash(args->seed_cash);
    trader.gated().deposit_position(args->instrument_id, args->seed_position);

    const strategies::MarketMakerConfig config{.instrument_id = args->instrument_id,
                                                .quote_size = args->quote_size,
                                                .half_spread = args->half_spread,
                                                .max_position = args->max_position,
                                                .requote_threshold = args->requote_threshold};
    strategies::MarketMakerStrategy mm(trader.gated(), config);

    httplib::Client cli(args->host, args->http_port);
    cli.set_connection_timeout(2, 0);
    cli.set_read_timeout(2, 0);

    std::cout << "live_strategy_demo: account " << args->account_id << " quoting instrument " << args->instrument_id
              << " via tcp:" << args->tcp_port << ", reading book from http:" << args->http_port << "\n"
              << "press Ctrl+C to stop\n";

    std::signal(SIGINT, on_sigint);
    int tick = 0;
    const int ticks_per_status = std::max(1, 1000 / std::max(1, args->poll_interval_ms));
    while (!g_stop_requested.load(std::memory_order_relaxed)) {
        if (auto mirror = fetch_book(cli, args->instrument_id)) {
            mm.on_book_update(args->instrument_id, *mirror);
        }
        if (++tick % ticks_per_status == 0) {
            print_status(mm, trader.gated(), args->instrument_id);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(args->poll_interval_ms));
    }

    std::cout << "\nshutting down...\n";
    return EXIT_SUCCESS;
}
