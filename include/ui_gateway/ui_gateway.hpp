#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/types.hpp"
#include "exchange/core/types.hpp"
#include "exchange/gateway/order_entry_gateway.hpp"
#include "net/udp_receiver.hpp"
#include "replay/replay_engine.hpp"
#include "trader/oms/order_entry_client.hpp"
#include "trader/positions/position_tracker.hpp"
#include "trader/risk/trader_risk_gated_oms.hpp"

// httplib.h is a single vendored header (FetchContent, see CMakeLists.txt) --
// forward-declared here instead of #included so every other translation
// unit that merely includes this header (e.g. apps/trading_server/main.cpp)
// does not have to compile against it too; only ui_gateway.cpp needs the
// full definition. Every handler below takes/returns these only by
// reference/pointer, so a forward declaration is enough.
namespace httplib {
class Server;
struct Request;
struct Response;
} // namespace httplib

// Milestone 12: the UI gateway -- a REST + Server-Sent-Events front door
// that finally lets a browser dashboard (ui/, a separate React project)
// observe and trade against a *live*, running exchange, closing two gaps
// documented as open in docs/exchange_flow.md's "Integration status":
// there was previously no long-running gateway process, and
// market_data::MarketDataPublisher was never wired into one. See
// apps/trading_server/main.cpp, the first thing in this codebase that
// actually does both.
//
// ── Why a library (cpp-httplib) instead of hand-rolling HTTP, unlike every
//    other wire protocol in this project ──────────────────────────────────
// Milestones 1-11 all hand-rolled their own wire format on purpose (see
// e.g. protocol/order_entry/messages.hpp's own class comment) because the
// *point* of those milestones was understanding and implementing exactly
// those bytes. HTTP/1.1 framing and RFC6455 WebSocket handshakes are not
// this milestone's teaching point -- exposing exchange/trader state to a
// browser is -- so a small, mature, single-header library is used instead,
// the same "don't reinvent what isn't the point" judgment call this
// project applies elsewhere (e.g. using std::function instead of a
// hand-rolled vtable for EventSink, see event_sink.hpp).
//
// ── Why Server-Sent Events, not a WebSocket, for the live push channel ───
// cpp-httplib has no WebSocket support at all, and every actively-maintained
// standalone C++ WebSocket library evaluated for this milestone was either
// far too new/unproven to depend on or dragged in a much larger dependency
// footprint (Boost.Asio, OpenSSL, etc.) than this read-mostly dashboard
// warrants. SSE needs nothing beyond what cpp-httplib already provides
// (chunked responses over plain HTTP, see UiGateway::handle_stream()'s own
// comment) and the browser's native EventSource API needs zero client-side
// dependencies either -- a better fit than pulling in a second, riskier
// library just to satisfy the letter of "WebSocket" when the actual
// requirement (server -> browser live push) is served just as well by SSE.
//
// ── Why per-account trading sessions reuse Milestones 8-9 wholesale ───────
// Each UI account is backed by exactly the same TraderRiskGatedOms +
// OrderEntryClient pair a Milestone 10/11 strategy uses -- the UI gateway
// process connects to the exchange gateway's TCP port over real loopback
// TCP, exactly like a strategy or a CLI trader would, rather than reaching
// into RiskGatedEngine/MatchingPipeline directly. Slower than an in-process
// call, immaterially so for a browser-driven dashboard, and it means this
// class adds zero new trading logic of its own -- it is purely a protocol
// adapter (HTTP/SSE <-> the existing order-entry wire protocol).
//
// ── Why a fixed, pre-seeded demo account catalog, not free-form account_id ──
// exchange::ledger::Ledger::deposit_cash()/deposit_position() are
// documented as safe only "before traffic that could race with it begins"
// (ledger.hpp's own class comment) -- Ledger has no internal
// synchronization, by design, because the matching thread is its only
// normal caller. Seeding a brand-new account the instant an HTTP request
// mentions it for the first time would call deposit_cash() concurrently
// with the matching thread possibly processing *other* accounts' live
// commands, a genuine data race on Ledger's internal accounts_ map (e.g. a
// concurrent rehash). Pre-seeding a small fixed set of demo accounts once,
// before the exchange gateway's start() is ever called (see
// apps/trading_server/main.cpp), sidesteps that race entirely without
// weakening Ledger's existing single-writer contract -- exactly the
// deliberate, narrow, documented scope choice this codebase prefers over a
// speculative concurrency fix nothing else in Milestones 1-11 needed. Each
// Session's own local PositionTracker mirror (trader/positions/) has no
// such restriction -- it is UI-gateway-process-local state, guarded by
// sessions_mutex_ below, so lazily constructing a Session's local mirror
// balances the first time a *pre-seeded* account_id is referenced is safe.
namespace mdh::ui_gateway {

struct UiGatewayOptions {
    // The complete set of account ids this UI gateway will ever serve --
    // see class-level comment on why this must be decided up front rather
    // than accepting an arbitrary account_id from an HTTP request. A
    // request naming any other account_id gets a 404 (see
    // UiGateway::find_or_create_session()).
    std::vector<exchange::AccountId> demo_account_ids{1001, 1002, 1003};

    // Instruments seeded with a starting position for every demo account
    // (see class-level comment), so a fresh account can demonstrate selling
    // immediately. This UI accepts an order on any InstrumentId, but the
    // exchange behind it does not: apps/trading_server hands this same list
    // to OrderEntryGatewayOptions::instruments, so an order on anything else
    // comes back rejected with InvalidInstrument.
    std::vector<InstrumentId> demo_instrument_ids{1, 2};

    trader::positions::Balance demo_starting_cash = 1'000'000'0000; // ticks; see common/types.hpp's Price scale
    Quantity demo_starting_position = 1'000;

    trader::risk::TraderRiskLimits per_account_risk_limits{};

    // Top-of-book depth returned by GET /api/book/:instrument_id and
    // included in every SSE book snapshot.
    std::size_t book_depth = 10;

    // If non-empty and the directory exists, served as static files at "/"
    // (cpp-httplib's set_mount_point) -- e.g. ui/dist after `npm run
    // build`, so `trading_server` alone can serve the whole dashboard with
    // no separate web server. Left empty (the default) serves no static
    // files at all -- e.g. every test in tests/test_ui_gateway.cpp, which
    // only cares about the JSON API and never runs `npm run build` first.
    std::string static_files_dir;
};

// One order-book price level, JSON-shaped identically for both
// GET /api/book/:id and the "book" SSE event -- see ui_gateway.cpp's
// to_json() helpers.
struct BookLevel {
    Price price;
    Quantity quantity;
    std::size_t order_count;
};

class UiGateway {
public:
    // `exchange_gateway` must already have every id in
    // options.demo_account_ids pre-seeded (deposit_cash()/
    // deposit_position()) and must NOT have start() called yet when this
    // constructor runs -- see class-level comment. `exchange_tcp_port` is
    // where this gateway's own per-account OrderEntryClients will connect
    // (127.0.0.1 only, same restriction as OrderEntryClient::connect());
    // `market_data_udp_port` is where this gateway's own background
    // book-reconstruction thread listens -- must match whatever port
    // `exchange_gateway`'s OrderEntryGatewayOptions::extra_event_sink was
    // wired to publish market data on (see apps/trading_server/main.cpp;
    // neither this class nor OrderEntryGateway itself enforces that
    // agreement -- it is the caller's responsibility, the same way a real
    // market-data multicast address/port is agreed out of band).
    // `http_port` of 0 lets the OS assign an ephemeral port, discoverable
    // via local_http_port() once started() -- the same convention
    // OrderEntryGateway::local_port() uses.
    UiGateway(exchange::gateway::OrderEntryGateway& exchange_gateway, std::uint16_t exchange_tcp_port,
               std::uint16_t market_data_udp_port, std::uint16_t http_port, UiGatewayOptions options = {});

    // Calls stop().
    ~UiGateway();

    UiGateway(const UiGateway&) = delete;
    UiGateway& operator=(const UiGateway&) = delete;
    UiGateway(UiGateway&&) = delete;
    UiGateway& operator=(UiGateway&&) = delete;

    // Binds the UDP market-data socket and the HTTP listener and starts
    // both background threads. Returns false, starting nothing, if either
    // bind fails (e.g. market_data_udp_port is already in use).
    [[nodiscard]] bool start();

    // Stops the HTTP server, requests-stop and joins the market-data
    // thread, and disconnects every per-account Session's OrderEntryClient
    // (which itself joins that client's reader thread -- see
    // OrderEntryClient::disconnect()). Safe to call more than once,
    // including implicitly via the destructor.
    void stop();

    [[nodiscard]] std::optional<std::uint16_t> local_http_port() const;

private:
    struct Session {
        Session(exchange::AccountId account_id, std::uint16_t exchange_tcp_port,
                 const trader::risk::TraderRiskLimits& limits,
                 trader::oms::OrderManagementSystem::OrderUpdateSink on_update);

        // Declared (and therefore destroyed) before client_ -- client_'s
        // reader thread calls into gated_ asynchronously, so client_ must
        // finish tearing down first. Same rationale, same order, as every
        // RiskGatedTrader test helper in tests/test_*_e2e.cpp.
        trader::risk::TraderRiskGatedOms gated_;
        trader::oms::OrderEntryClient client_;
        bool connected_ = false;
    };

    // Looks up an existing Session for `account_id`, or -- iff account_id
    // is one of options_.demo_account_ids -- lazily constructs and seeds
    // one (see class-level comment on why this is safe: only the
    // process-local PositionTracker mirror is touched here, never
    // exchange_gateway_'s Ledger). Returns nullptr for any account_id
    // outside the demo catalog.
    [[nodiscard]] Session* find_or_create_session(exchange::AccountId account_id);

    // The market-data background thread: a persistent (never idle-timeout
    // stopping, unlike net::run_udp_listen()) version of the
    // receive -> unpack_frames -> replay::apply_frame_result() pipeline
    // apps/market_data_replay's --listen mode already uses, reusing that
    // exact same decode/sequence/book-application logic -- only "run
    // forever until stop_source_ says otherwise" is new here. Broadcasts a
    // fresh book snapshot over SSE for every instrument touched by a batch
    // (see broadcast_book()). `receiver` is bound by start() itself (so
    // start() can fail fast and return false if the port is already taken)
    // and then moved onto this thread -- avoids the alternative of binding
    // twice (a probe bind that is closed and immediately reopened on the
    // background thread), which would be a real, if narrow, TOCTOU race.
    void market_data_loop(std::stop_token token, net::UdpReceiver receiver);

    // Pushes a "book" SSE event for `instrument_id`'s current top-of-book
    // (options_.book_depth levels per side).
    void broadcast_book(InstrumentId instrument_id);
    // Pushed as this Session's OrderUpdateSink (see Session's constructor)
    // -- an "order" SSE event carrying the updated ClientOrder.
    void broadcast_order(exchange::AccountId account_id, const trader::oms::ClientOrder& order);

    // ── REST handlers -- registered onto *server_ in the constructor,
    // defined in ui_gateway.cpp. Each takes/returns httplib types by
    // reference/value exactly as httplib::Server::Get/Post expect; kept as
    // methods (not lambdas in the constructor) purely for readability.
    void handle_health(const httplib::Request& req, httplib::Response& res);
    void handle_list_accounts(const httplib::Request& req, httplib::Response& res);
    void handle_get_account(const httplib::Request& req, httplib::Response& res);
    void handle_get_book(const httplib::Request& req, httplib::Response& res);
    void handle_submit_order(const httplib::Request& req, httplib::Response& res);
    void handle_cancel_order(const httplib::Request& req, httplib::Response& res);
    void handle_replace_order(const httplib::Request& req, httplib::Response& res);
    void handle_stream(const httplib::Request& req, httplib::Response& res);

    // Not otherwise read: kept as a member (rather than just a constructor
    // parameter this class immediately forgets) purely to document, in the
    // type system, that a UiGateway is permanently paired with one
    // specific exchange gateway instance -- exactly the "must have already
    // been pre-seeded, must not have start() called yet" precondition the
    // constructor's own doc comment states. Nothing in this milestone's
    // REST/SSE surface needs to read exchange-side state directly (every
    // account/order/book view goes through a Session's own
    // TraderRiskGatedOms or this class's own reconstructed BookManager,
    // see class-level comment on why), so [[maybe_unused]] rather than
    // manufacturing a call site that would exist only to silence a warning.
    [[maybe_unused]] exchange::gateway::OrderEntryGateway& exchange_gateway_;
    std::uint16_t exchange_tcp_port_;
    std::uint16_t market_data_udp_port_;
    std::uint16_t http_port_;
    UiGatewayOptions options_;

    std::mutex sessions_mutex_;
    std::unordered_map<exchange::AccountId, std::unique_ptr<Session>> sessions_;

    std::mutex books_mutex_;
    replay::ReplayOutcome market_data_outcome_; // .books is the live-reconstructed book::BookManager

    // Type-erased so ui_gateway.hpp never has to include httplib.h (see
    // this file's own top-of-file comment) or json.hpp.
    class SseHub;
    std::unique_ptr<SseHub> sse_hub_;

    std::unique_ptr<httplib::Server> server_;
    std::optional<std::uint16_t> bound_http_port_;
    std::stop_source stop_source_;
    std::jthread market_data_thread_;
    std::jthread http_thread_;
};

} // namespace mdh::ui_gateway
