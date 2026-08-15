#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/spsc_queue.hpp"
#include "exchange/core/commands.hpp"
#include "exchange/core/event_sink.hpp"
#include "exchange/core/events.hpp"
#include "exchange/ledger/ledger.hpp"
#include "exchange/matching/matching_engine.hpp"
#include "exchange/matching/state_snapshot.hpp"
#include "exchange/risk/risk_engine.hpp"
#include "exchange/risk/risk_gated_engine.hpp"
#include "exchange/sequencing/matching_pipeline.hpp"
#include "net/tcp_socket.hpp"
#include "protocol/order_entry/messages.hpp"

// The live, networked front door to the exchange (Milestone 7): the piece
// that finally makes everything built in Milestones 1-6 reachable by an
// actual TCP client instead of only by in-process test code. Composes, in
// order:
//
//   TcpSocket (Milestone 7) -- accepts/reads/writes raw bytes
//     -> protocol::order_entry:: encoder/decoder (Milestone 7) -- bytes <-> Message
//     -> OrderEntryGateway::to_command()/to_execution_reports() (this class)
//        -- Message <-> ExchangeCommand/ExchangeEvent
//     -> MatchingPipeline (Milestone 4), its processor wired to
//        RiskGatedEngine (Milestone 5) via the Processor seam
//        (matching_pipeline.hpp) -- the actual matching + risk + ledger
//
// Nothing upstream of this class changes: MatchingPipeline, RiskGatedEngine,
// MatchingEngine, and Ledger are used exactly as already built and tested,
// completely unaware that their caller is now a network gateway instead of
// a test fixture calling submit() directly.
//
// ── Concurrency model ──────────────────────────────────────────────────────
//
//   accept thread (1, non-blocking poll loop)
//       |
//       +--> per connection: reader thread ---> submit_command() ---> MatchingPipeline
//       |                                                                  |
//       +--> per connection: writer thread <--- outbound SpscQueue <--- route_event()
//                                                                 (runs ON the matching thread,
//                                                                  as MatchingPipeline's EventSink)
//
// - Exactly one accept thread, polling TcpSocket::accept() in a
//   non-blocking loop against a shared stop_token rather than ever
//   blocking inside accept() -- see accept_loop()'s own doc comment for
//   why (the same macOS shutdown()/accept() caveat documented in
//   net/tcp_socket.hpp).
// - Each accepted connection gets exactly one reader thread and one writer
//   thread (a Connection, see below) -- symmetric with how UdpListener-style
//   two-thread pipelines are already built in this codebase, just per
//   connection instead of once globally.
// - MatchingPipeline::submit() requires a single producer (its own class
//   comment, matching_pipeline.hpp lines ~84-89), but this gateway has N
//   reader threads (one per connection) that all need to submit commands.
//   submit_command() below serializes every caller behind submit_mutex_ --
//   simple mutual exclusion rather than restructuring MatchingPipeline
//   itself, since submit() is not the hot inner loop MatchingEngine::process()
//   is (that stays single-threaded and lock-free, unaffected).
// - route_event() is installed as the sink MatchingPipeline hands to its
//   Processor (ultimately RiskGatedEngine, then MatchingEngine), so it runs
//   ON the matching thread, synchronously, exactly like every other
//   EventSink call site in this codebase (see event_sink.hpp) -- it must
//   not block. It translates the event (via to_execution_reports()) and
//   pushes the resulting per-account messages onto each target
//   connection's own outbound SpscQueue, non-blocking (try_push()) --
//   this is why every connection gets a queue instead of route_event()
//   calling TcpSocket::write() directly, which would make the matching
//   thread's throughput hostage to one slow client's socket. route_event()
//   also invokes OrderEntryGatewayOptions::extra_event_sink (if set) with
//   the raw, untranslated event -- see its own doc comment (Milestone 12).
// - Session-to-account binding is opportunistic, not part of a handshake:
//   a connection is unrouted (absent from routes_) until its first
//   decoded client message arrives, since every client -> gateway message
//   type carries account_id (see protocol/order_entry/messages.hpp) --
//   there's nothing to wait for beyond that first message.
//
// See tests/test_order_entry_gateway_e2e.cpp for the behavioral spec this
// class is expected to satisfy end to end, over real TCP sockets.
namespace mdh::exchange::gateway {

struct OrderEntryGatewayOptions {
    risk::RiskLimits risk_limits{};

    // MatchingPipeline's own inbound command queue (matching_pipeline.hpp).
    std::size_t matching_queue_capacity = 1024;

    // Per-connection outbound queue capacity -- see Connection::outbound
    // below for why a full one is a real, documented policy decision left
    // to you (route_event()'s TODO), not an edge case to overlook.
    std::size_t outbound_queue_capacity = 1024;

    // Passed straight through to TcpSocket::listen()'s backlog parameter.
    int accept_backlog = 16;

    // Milestone 12: an optional second observer of every ExchangeEvent this
    // gateway's matching thread produces, invoked from route_event()
    // alongside (never instead of) the existing per-connection wire
    // routing -- see route_event()'s own doc comment for exactly where.
    // Unlike to_execution_reports() (which only ever looks at the four
    // account-addressed event types), this sees *every* event, including
    // the anonymous Book*/TradeExecuted ones -- e.g. wiring in
    // market_data::MarketDataPublisher::sink(), the same seam
    // exchange::ledger::Ledger::sink() and this gateway's own route_event()
    // already use, so a live UI/market-data feed can finally observe a real
    // running gateway (see apps/trading_server) instead of only ever being
    // fed by a test. Defaults to nullptr (a no-op, checked before calling)
    // -- every existing caller of this struct is completely unaffected.
    // Runs on the matching thread, synchronously, under the exact same
    // "must not block" constraint route_event() itself documents.
    EventSink extra_event_sink;
};

class OrderEntryGateway {
public:
    explicit OrderEntryGateway(std::uint16_t port, const OrderEntryGatewayOptions& options = {});

    // Calls stop() -- see its own doc comment for exactly what that
    // guarantees before this object is fully torn down.
    ~OrderEntryGateway();

    OrderEntryGateway(const OrderEntryGateway&) = delete;
    OrderEntryGateway& operator=(const OrderEntryGateway&) = delete;
    OrderEntryGateway(OrderEntryGateway&&) = delete;
    OrderEntryGateway& operator=(OrderEntryGateway&&) = delete;

    // Starts listening (TcpSocket::listen(port)) and spawns the accept
    // thread. Returns false, spawning nothing, if listen() itself fails
    // (e.g. the port is already in use) -- safe to inspect and act on
    // before assuming the gateway is actually reachable.
    [[nodiscard]] bool start();

    // Shuts everything down, in this order:
    //   1. Request-stop the shared stop_source -- observed by accept_loop()'s
    //      poll loop and by connection_writer_loop()'s wait (also woken
    //      directly, per connection, via each Connection's wake_cv, rather
    //      than left to that wait's own timeout -- see stop()'s
    //      implementation).
    //   2. Join the accept thread, so no *new* connections can appear while
    //      steps 3-4 below iterate connections_.
    //   3. Call shutdown() on every live connection's socket -- this is what
    //      actually unblocks each connection's reader thread, which is
    //      otherwise sitting in a blocking TcpSocket::read() call (see
    //      tcp_socket.hpp's own shutdown() doc comment: this is the
    //      portable case it exists for, unlike accept()).
    //   4. Join every connection's reader and writer threads.
    //   5. Stop the MatchingPipeline (drains whatever commands are already
    //      queued, per its own stop() contract).
    // Safe to call more than once, including implicitly via the destructor
    // -- a no-op on a gateway that was never start()-ed or already stopped.
    void stop();

    // The actual local port this gateway is listening on -- meaningful
    // after a successful start(), especially when constructed with port 0.
    [[nodiscard]] std::optional<std::uint16_t> local_port() const { return listener_.local_port(); }

    // Introspection for tests: how many connections have ever been
    // accepted (does not decrease when a client disconnects -- see
    // Connection's own comment on why dead connections are not proactively
    // pruned in this scaffold).
    [[nodiscard]] std::size_t connection_count() const;

    // Read-only access to the underlying matching state, for tests that
    // want to assert on the book directly instead of only via wire
    // responses -- same rationale as MatchingPipeline::snapshot(). Shares
    // that method's exact same precondition and for the exact same reason:
    // `engine_` is the live MatchingEngine the matching thread (owned by
    // pipeline_, via the Processor seam wiring risk_gated_engine_ -- see
    // this class's own constructor) mutates on every command, so this is
    // only safe to call after stop() has returned (i.e. the matching thread
    // has been joined) -- calling it while the gateway is still running is
    // a data race, not guarded against at runtime, exactly like
    // MatchingPipeline::snapshot() itself.
    [[nodiscard]] EngineStateSnapshot snapshot() const { return engine_.snapshot(); }

    // Test/admin seeding, forwarded straight to Ledger -- see its own
    // class-level comment on why this is the only way balances start out
    // non-zero. Safe to call before start() or at any point afterward;
    // Ledger's own methods are not internally synchronized against
    // concurrent NewOrderCommand processing, so in practice this should
    // only be called before traffic that could race with it begins.
    void deposit_cash(AccountId account_id, ledger::Balance amount) { ledger_.deposit_cash(account_id, amount); }
    void deposit_position(AccountId account_id, InstrumentId instrument_id, Quantity amount) {
        ledger_.deposit_position(account_id, instrument_id, amount);
    }

private:
    // One accepted TCP connection and everything that belongs to it.
    // Allocated with std::make_unique and stored in connections_ so its
    // address stays stable for the lifetime of the connection (routes_
    // below holds raw, non-owning pointers to these) even as connections_
    // itself grows.
    struct Connection {
        explicit Connection(net::TcpSocket socket_in, std::size_t outbound_capacity)
            : socket(std::move(socket_in)), outbound(outbound_capacity) {}

        net::TcpSocket socket;

        // Bytes read but not yet decoded into a complete Message -- read()
        // can return a partial message (see tcp_socket.hpp's own doc
        // comment on why TCP has no atomic-message guarantee), so
        // connection_reader_loop() must accumulate here across possibly
        // many read() calls before protocol::order_entry::decode_message()
        // has enough bytes to produce anything.
        std::vector<std::byte> read_buffer;

        // Reports destined for this connection's client, produced by
        // route_event() on the matching thread, drained by
        // connection_writer_loop() on this connection's own writer thread.
        // A full queue here is a real design decision for you
        // (route_event()'s TODO) to make explicitly, not an edge case to
        // silently ignore -- see MatchingPipeline's own class-level comment
        // on why "drop vs. reject vs. block" is a policy choice, not a
        // default.
        SpscQueue<protocol::order_entry::Message> outbound;

        // Lets connection_writer_loop() block until route_event() actually
        // pushes something for this connection instead of unconditionally
        // sleeping a fixed poll interval every time it finds outbound
        // empty (see connection_writer_loop()'s own doc comment on why
        // that sleep used to dominate this gateway's measured end-to-end
        // latency -- docs/benchmarks.md §7). wake_mutex guards nothing
        // but the wait itself (outbound stays lock-free SPSC, unaffected);
        // route_event() does not need to hold it to call notify_one() --
        // the writer always re-checks outbound.size() itself as
        // wait_for()'s predicate, so a notification that arrives just
        // before the writer starts waiting is never lost, only redundant
        // with the predicate's own re-check. kPollInterval remains as
        // wait_for()'s timeout, purely as a safety net (e.g. against a
        // spuriously missed wakeup), not as the primary wake mechanism
        // anymore.
        std::mutex wake_mutex;
        std::condition_variable wake_cv;

        // Written exactly once, only by this connection's own reader
        // thread (see accept_loop()'s / connection_reader_loop()'s
        // opportunistic-binding doc comment) -- single-writer, so no
        // atomics needed on this field itself. routes_ (guarded by its own
        // mutex) is what makes the *binding* visible to the matching
        // thread; this field is purely so the reader thread itself can
        // tell "have I already registered myself" without re-locking
        // routes_mutex_ on every single message.
        std::optional<AccountId> account_id;

        std::jthread reader_thread;
        std::jthread writer_thread;
    };

    // Serializes MatchingPipeline::submit() across every connection's
    // reader thread -- see class-level comment on why. Prefer this over
    // calling pipeline_.submit() directly from connection_reader_loop().
    [[nodiscard]] bool submit_command(ExchangeCommand command);

    // ── The gateway/protocol translation layer (see tests/test_order_entry_gateway_e2e.cpp for the spec) ──

    // Runs on accept_thread_. Polls listener_.accept() (non-blocking, see
    // class-level comment) against stop_source_.get_token(); on a
    // successful accept, constructs a Connection, appends it to
    // connections_ (under connections_mutex_), and spawns its reader and
    // writer threads.
    void accept_loop();

    // Runs on `conn`'s own reader thread. Reads bytes (blocking
    // TcpSocket::read()), accumulates them in conn.read_buffer, decodes
    // complete messages via protocol::order_entry::decode_message(),
    // translates each via to_command(), and calls submit_command(). Also
    // responsible for the opportunistic account_id -> routes_ binding
    // described in Connection's own doc comment. Exits when read() reports
    // EOF/error (including the shutdown() this class's stop() triggers). A
    // malformed header (bad type byte) or a header whose payload hasn't
    // fully arrived yet are treated identically -- wait for the next
    // read() -- since there is no way to tell them apart from a length-
    // prefixed stream alone; a malformed *payload* under an otherwise
    // well-formed header drops just that one frame and keeps the
    // connection open, since framing (and therefore synchronization with
    // the byte stream) is still intact.
    void connection_reader_loop(Connection& conn);

    // Runs on `conn`'s own writer thread. Drains conn.outbound (try_pop())
    // against `token`, encoding and writing each Message it finds via
    // protocol::order_entry::encode_message() + TcpSocket::write() --
    // remember write() can itself return a short count (tcp_socket.hpp's
    // own doc comment), so a single write() call is not guaranteed to
    // flush one whole encoded message. When outbound is empty, blocks on
    // conn.wake_cv (see Connection's own doc comment) instead of
    // unconditionally sleeping a fixed interval -- route_event() notifies
    // this directly the moment it pushes something for this connection, so
    // in the common case this thread reacts immediately rather than on its
    // next poll tick.
    void connection_writer_loop(Connection& conn, std::stop_token token);

    // Installed as the EventSink handed to pipeline_'s Processor -- runs on
    // the matching thread (see class-level comment on why it must not
    // block). Calls to_execution_reports(event), then for each
    // (account_id, message) pair, looks up that account's Connection in
    // routes_ (guarded by routes_mutex_) and pushes onto its outbound
    // queue.
    void route_event(const ExchangeEvent& event);

    // Translates one decoded client -> gateway wire message into the
    // matching engine's own command vocabulary. Returns std::nullopt for a
    // message that is well-formed wire-protocol-wise but not valid as a
    // client request (e.g. a gateway -> client type like Accepted/
    // TradeReport arriving from a client) -- connection_reader_loop()
    // silently ignores a std::nullopt rather than disconnecting the client,
    // since this protocol has no NAK/error-response type to report it with.
    [[nodiscard]] static std::optional<ExchangeCommand> to_command(const protocol::order_entry::Message& message);

    // Translates one ExchangeEvent into zero or more (account_id, message)
    // pairs to deliver over the wire. Zero for every Book* event (see
    // events.hpp's own comment on why those are deliberately anonymous --
    // they belong to market data, Milestone 6, not order entry). One for
    // OrderAccepted/OrderRejected/OrderCancelled/OrderReplaced. Up to two
    // for TradeExecuted, since its buyer and seller (each already carrying
    // their own account_id, see events.hpp's TradeCounterparty) can belong
    // to two different connections -- see this file's class-level comment
    // and the plan this scaffold was built from for why TradeReport is
    // deliberately one-sided rather than mirroring TradeExecuted's shape.
    [[nodiscard]] static std::vector<std::pair<AccountId, protocol::order_entry::Message>> to_execution_reports(
        const ExchangeEvent& event);

    std::uint16_t port_;
    OrderEntryGatewayOptions options_;

    net::TcpSocket listener_;
    std::stop_source stop_source_; // shared by accept_loop() and every connection_writer_loop()
    std::jthread accept_thread_;

    // Guarded by connections_mutex_: accept_loop() appends on the accept
    // thread, connection_count() and stop() read it from whatever thread
    // calls them. Connection objects are allocated with make_unique
    // specifically so a pointer into one (see routes_ below) stays valid
    // even while this vector itself grows and reallocates.
    mutable std::mutex connections_mutex_;
    std::vector<std::unique_ptr<Connection>> connections_;

    std::mutex routes_mutex_;
    std::unordered_map<AccountId, Connection*> routes_; // non-owning; see Connection's own doc comment

    std::mutex submit_mutex_; // see submit_command()'s doc comment

    MatchingEngine engine_;
    ledger::Ledger ledger_;
    risk::RiskGatedEngine risk_gated_engine_; // composes engine_ + ledger_ + risk::RiskEngine
    sequencing::MatchingPipeline pipeline_;    // its Processor is wired to risk_gated_engine_.process()
};

} // namespace mdh::exchange::gateway
