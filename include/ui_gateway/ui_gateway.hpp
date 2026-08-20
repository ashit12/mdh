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

// httplib.h is a single vendored header, forward-declared here rather than
// included so that every translation unit including this one does not have
// to compile it too -- only ui_gateway.cpp needs the definition. Every
// handler below takes these by reference or pointer, so this is enough.
namespace httplib {
class Server;
struct Request;
struct Response;
} // namespace httplib

// A REST and Server-Sent-Events front door, so a browser dashboard (ui/, a
// separate React project) can watch and trade against a live exchange. See
// apps/trading_server, which runs this alongside the exchange gateway.
//
// ── Why a library for HTTP, when every other protocol here is hand-rolled ─
// The other wire formats in this project are hand-rolled on purpose: the
// point of writing them was understanding exactly those bytes. HTTP framing
// is not the point here -- getting exchange state into a browser is -- so a
// small, mature, single-header library does that job instead. The same
// judgement that uses std::function for EventSink rather than a hand-rolled
// vtable.
//
// ── Why Server-Sent Events rather than a WebSocket ────────────────────────
// The traffic is one-directional: the server pushes updates, and the browser
// sends orders over ordinary REST. SSE is just a long-lived chunked HTTP
// response, which the HTTP library already supports and the browser's native
// EventSource API consumes with no client-side dependency at all. Every
// standalone C++ WebSocket library considered was either too new to rely on
// or brought a far larger dependency (Boost.Asio, OpenSSL) than a
// read-mostly dashboard justifies.
//
// ── Why sessions go over real TCP instead of calling in-process ───────────
// Each account here is backed by the same trader-side OMS and client a
// strategy uses, connected to the exchange gateway's TCP port over loopback
// exactly as an external strategy would be. Slower than an in-process call,
// immaterially so for a dashboard, and it means this class holds no trading
// logic of its own -- it is purely an adapter between HTTP and the
// order-entry protocol.
//
// ── Why a fixed set of pre-seeded demo accounts ───────────────────────────
// The ledger has no internal locking, by design, because the matching thread
// is its only normal caller -- so seeding is safe only before any traffic
// that could race with it. Creating an account the moment an HTTP request
// first mentioned it would call into the ledger while the matching thread
// was processing other accounts' commands, a real data race on its account
// map. Seeding a small fixed set once, before the exchange gateway starts,
// avoids that without weakening the ledger's single-writer contract.
//
// Each session's own local position mirror has no such restriction: it is
// local to this process and guarded by sessions_mutex_, so building one
// lazily for an already-seeded account is fine.
namespace mdh::ui_gateway {

struct UiGatewayOptions {
    // Every account this gateway will ever serve -- see the class comment on
    // why this is fixed up front rather than taken from a request. Any other
    // account id gets a 404.
    std::vector<exchange::AccountId> demo_account_ids{1001, 1002, 1003};

    // Instruments seeded with a starting position for every demo account, so
    // a fresh account can sell straight away. This UI will accept an order
    // on any instrument, but the exchange behind it will not: trading_server
    // hands this same list to the exchange gateway, so anything else comes
    // back rejected with InvalidInstrument.
    std::vector<InstrumentId> demo_instrument_ids{1, 2};

    trader::positions::Balance demo_starting_cash = 1'000'000'0000; // ticks; see common/types.hpp's Price scale
    Quantity demo_starting_position = 1'000;

    trader::risk::TraderRiskLimits per_account_risk_limits{};

    // Top-of-book depth returned by GET /api/book/:instrument_id and
    // included in every SSE book snapshot.
    std::size_t book_depth = 10;

    // If set and the directory exists, served as static files at "/" -- e.g.
    // ui/dist after `npm run build`, so trading_server alone can serve the
    // whole dashboard with no separate web server. Empty serves nothing,
    // which is what the tests want: they only exercise the JSON API.
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
    // `exchange_gateway` must already have every demo account seeded, and
    // must not have been started yet -- see the class comment.
    //
    // `exchange_tcp_port` is where this gateway's own per-account clients
    // connect, on 127.0.0.1 only. `market_data_udp_port` is where its
    // book-reconstruction thread listens, and must match the port the
    // exchange gateway was wired to publish on; nothing enforces that
    // agreement, exactly as a real feed's address is agreed out of band.
    //
    // An `http_port` of 0 lets the OS pick one, readable afterwards from
    // local_http_port().
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

    // Stops the HTTP server, stops and joins the market-data thread, and
    // disconnects every session's client, which joins that client's own
    // reader thread. Safe to call more than once, including from the
    // destructor.
    void stop();

    [[nodiscard]] std::optional<std::uint16_t> local_http_port() const;

private:
    struct Session {
        Session(exchange::AccountId account_id, std::uint16_t exchange_tcp_port,
                 const trader::risk::TraderRiskLimits& limits,
                 trader::oms::OrderManagementSystem::OrderUpdateSink on_update);

        // Declared, and so destroyed, before client_: that client's reader
        // thread calls into gated_ asynchronously, so the client has to
        // finish tearing down first.
        trader::risk::TraderRiskGatedOms gated_;
        trader::oms::OrderEntryClient client_;
        bool connected_ = false;
    };

    // Finds the session for `account_id`, or builds one if it is a demo
    // account. Safe to build lazily because only this process's own position
    // mirror is touched, never the exchange's ledger. Returns nullptr for
    // any account outside the demo catalog.
    [[nodiscard]] Session* find_or_create_session(exchange::AccountId account_id);

    // The market-data thread: the same receive, unpack and apply pipeline
    // market_data_replay's listen mode uses, except that it runs until
    // stopped rather than timing out when the feed goes quiet. Broadcasts a
    // fresh book over SSE for every instrument a batch touched.
    //
    // `receiver` is bound by start(), so start() can fail fast on a port
    // already in use, and then moved onto this thread. Binding twice instead
    // -- a probe bind, closed and reopened here -- would be a narrow but
    // real race.
    void market_data_loop(std::stop_token token, net::UdpReceiver receiver);

    // Pushes a "book" event with the top book_depth levels of each side.
    void broadcast_book(InstrumentId instrument_id);
    // Installed as each session's order-update sink: pushes an "order" event
    // carrying the updated order.
    void broadcast_order(exchange::AccountId account_id, const trader::oms::ClientOrder& order);

    // ── REST handlers, registered in the constructor and defined in the
    // .cpp. Methods rather than lambdas purely for readability.
    void handle_health(const httplib::Request& req, httplib::Response& res);
    void handle_list_accounts(const httplib::Request& req, httplib::Response& res);
    void handle_get_account(const httplib::Request& req, httplib::Response& res);
    void handle_get_book(const httplib::Request& req, httplib::Response& res);
    void handle_submit_order(const httplib::Request& req, httplib::Response& res);
    void handle_cancel_order(const httplib::Request& req, httplib::Response& res);
    void handle_replace_order(const httplib::Request& req, httplib::Response& res);
    void handle_stream(const httplib::Request& req, httplib::Response& res);

    // Never actually read. Kept as a member, rather than a constructor
    // parameter this class forgets, to record in the type system that a
    // UiGateway is paired with one specific exchange gateway -- the
    // "already seeded, not yet started" precondition above. Nothing in the
    // REST or SSE surface reads exchange state directly: every view goes
    // through a session's own OMS or this class's reconstructed book.
    [[maybe_unused]] exchange::gateway::OrderEntryGateway& exchange_gateway_;
    std::uint16_t exchange_tcp_port_;
    std::uint16_t market_data_udp_port_;
    std::uint16_t http_port_;
    UiGatewayOptions options_;

    std::mutex sessions_mutex_;
    std::unordered_map<exchange::AccountId, std::unique_ptr<Session>> sessions_;

    std::mutex books_mutex_;
    replay::ReplayOutcome market_data_outcome_; // .books is the live-reconstructed book::BookManager

    // Type-erased so this header never has to include httplib.h or json.hpp.
    class SseHub;
    std::unique_ptr<SseHub> sse_hub_;

    std::unique_ptr<httplib::Server> server_;
    std::optional<std::uint16_t> bound_http_port_;
    std::stop_source stop_source_;
    std::jthread market_data_thread_;
    std::jthread http_thread_;
};

} // namespace mdh::ui_gateway
