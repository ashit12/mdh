#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
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
//   pushes the resulting messages onto each target connection's own
//   outbound SpscQueue, non-blocking (try_push()) -- this is why every
//   connection gets a queue instead of route_event() calling
//   TcpSocket::write() directly, which would make the matching thread's
//   throughput hostage to one slow client's socket. Which connection is
//   the target is the session model's job, immediately below.
//   route_event() also invokes OrderEntryGatewayOptions::extra_event_sink
//   (if set) with the raw, untranslated event -- see its own doc comment
//   (Milestone 12).
//
// ── Sessions, and how a private report finds its way back ─────────────────
//
// One connection is one *session*. Session-to-account binding is
// opportunistic, not part of a handshake: a connection is unbound until
// its first valid client request arrives, since every client -> gateway
// message type carries account_id (see protocol/order_entry/messages.hpp)
// -- there's nothing to wait for beyond that first message. Two rules
// follow from that, and they are what keep one client's execution stream
// out of another's socket:
//
//   1. The binding is *immutable*. Every later message on the connection
//      must carry the same account_id; one that doesn't is answered with
//      a Rejected{AccountMismatch} and never reaches the pipeline, so a
//      session can never trade (or spend the balance of) an account other
//      than the one it bound to.
//   2. An account may have *many* sessions. account_sessions_ maps one
//      account to every connection currently bound to it, so a second
//      connection for the same account joins the account instead of
//      silently displacing whoever was there first.
//
// Many sessions per account then raises the question rule 2 exists to
// answer properly: which of them should a given execution report go to?
// Not all of them -- these are private, account-addressed reports, and a
// session that placed nothing should not be told about an order it never
// sent. The gateway therefore correlates each report back to the session
// that *originated* it, using the one key the engine already guarantees
// is unique among live orders: (account_id, client_order_id) -- see
// MatchingEngine::LiveKey. order_owner_ records that mapping when a
// connection's reader thread submits a command, before the command can
// possibly produce an event, and route_event() resolves it in three
// steps: the owning session if it's still connected, otherwise one
// other live session of that account (a resting order outlives the
// session that placed it -- there is no cancel-on-disconnect here), and
// otherwise pending_reports_, which retains the report so a reconnecting
// session can reconcile. That last queue is bounded and process-local, not
// durable offline delivery: reports can be dropped on overflow and are lost
// on restart. Deliberately, none of this reaches into
// ExchangeCommand/ExchangeEvent: the exchange core stays a
// transport-independent, deterministic, replayable thing that has never
// heard of a socket (see commands.hpp's own comment), and the entire
// session model lives here in the gateway.
//
// See tests/test_order_entry_gateway_e2e.cpp for the behavioral spec this
// class is expected to satisfy end to end, over real TCP sockets.
namespace mdh::exchange::gateway {

// Identifies one connection for the lifetime of the gateway process.
// Purely for diagnostics and for tests that need to talk about "the same
// session" without holding a Connection* -- routing itself keys on
// (account_id, client_order_id), never on this.
using SessionId = std::uint64_t;

struct OrderEntryGatewayOptions {
    risk::RiskLimits risk_limits{};

    // MatchingPipeline's own inbound command queue (matching_pipeline.hpp).
    std::size_t matching_queue_capacity = 1024;

    // Passed straight to the MatchingEngine behind this gateway -- see
    // MatchingEngine::kDefaultExpectedRestingOrders. A gateway carrying real
    // order flow is exactly the caller that should raise it.
    std::size_t expected_resting_orders = MatchingEngine::kDefaultExpectedRestingOrders;

    // Per-connection outbound queue capacity -- see Connection::outbound
    // below for why a full one is a real, documented policy decision left
    // to you (route_event()'s TODO), not an edge case to overlook.
    std::size_t outbound_queue_capacity = 1024;

    // Passed straight through to TcpSocket::listen()'s backlog parameter.
    int accept_backlog = 16;

    // How many execution reports are retained per account while that
    // account has no live session at all (see this class's own session
    // doc comment): an order can rest, and later fill, long after the
    // session that placed it disconnected, and dropping that fill on the
    // floor would leave a reconnecting client unable to reconcile its own
    // position. Once this many are queued for one account the oldest is
    // dropped to make room -- the newest reports are the ones a
    // reconciling client most needs, and retaining without bound would
    // let a never-returning account grow this map forever. Set to 0 to
    // disable retention entirely (reports for an account with no live
    // session are simply dropped).
    std::size_t pending_report_capacity = 1024;

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
    // Identifies one order for as long as the gateway needs to know which
    // session it came from. Same shape, and for exactly the same reason,
    // as MatchingEngine::LiveKey: a client order id is only unique
    // per-account, so nothing keyed on client_order_id alone would be
    // unambiguous across two accounts that both chose id 1.
    struct OrderKey {
        AccountId account_id;
        ClientOrderId client_order_id;

        bool operator==(const OrderKey&) const = default;
    };

    struct OrderKeyHash {
        [[nodiscard]] std::size_t operator()(const OrderKey& key) const {
            const std::size_t a = std::hash<AccountId>{}(key.account_id);
            const std::size_t b = std::hash<ClientOrderId>{}(key.client_order_id);
            return a ^ (b + 0x9e3779b97f4a7c15ULL + (a << 6) + (a >> 2));
        }
    };

    // One accepted TCP connection -- one session -- and everything that
    // belongs to it. Allocated with std::make_unique and stored in
    // connections_ so its address stays stable for the lifetime of the
    // connection (account_sessions_ and order_owner_ below hold raw,
    // non-owning pointers to these) even as connections_ itself grows.
    struct Connection {
        explicit Connection(SessionId id, net::TcpSocket socket_in, std::size_t outbound_capacity)
            : session_id(id), socket(std::move(socket_in)), outbound(outbound_capacity),
              session_outbound(outbound_capacity) {}

        SessionId session_id;

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

        // The gateway's *own* replies to this connection -- currently just
        // the Rejected{AccountMismatch} a bound session gets for claiming
        // somebody else's account. A second queue rather than a second
        // producer on `outbound`: SpscQueue is single-producer by contract
        // (see spsc_queue.hpp -- its whole lock-free argument rests on
        // head_ having exactly one writer), and `outbound`'s producer is
        // the matching thread, whereas these are produced by this
        // connection's own reader thread. One consumer, this connection's
        // writer thread, drains both.
        SpscQueue<protocol::order_entry::Message> session_outbound;

        // Reports retained for this session's account while it had no live
        // session at all (pending_reports_ below), handed over at bind time
        // so a reconnecting client can reconcile. Drained by the writer
        // thread ahead of either queue above, which is what keeps a
        // retained report from arriving after a live one: the handover
        // happens while sessions_mutex_ is held, before this session is
        // visible in account_sessions_, so route_event() cannot have
        // pushed anything to `outbound` yet.
        std::mutex replay_mutex;
        std::vector<protocol::order_entry::Message> replay_backlog;

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

        // The account this session bound to on its first valid request,
        // and never anything else afterwards (see this class's own session
        // doc comment on why the binding is immutable). Written exactly
        // once, only by this connection's own reader thread -- single
        // writer, so no atomic needed on the field itself. It's
        // account_sessions_ (guarded by sessions_mutex_) that makes the
        // binding visible to the matching thread; this field is what lets
        // the reader thread check "am I already bound, and to whom" on
        // every subsequent message without touching that mutex.
        std::optional<AccountId> account_id;

        // Set by this connection's reader thread as it exits (peer EOF,
        // read error, or stop()'s shutdown()) and read by the matching
        // thread in route_event(). Between that store and the unbinding
        // that follows it, this is what stops a report being pushed onto
        // a queue whose writer thread is on its way out; after unbinding,
        // the connection is unreachable from routes at all. Also the
        // writer thread's own exit condition, so a disconnected client's
        // writer doesn't linger until stop().
        std::atomic<bool> closed{false};

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
    // responsible for this session's account binding and for claiming
    // ownership of every order id it submits, both described in this
    // class's own session doc comment, and for unbinding the session again
    // once it exits. Exits when read() reports
    // EOF/error (including the shutdown() this class's stop() triggers). A
    // malformed header (bad type byte) or a header whose payload hasn't
    // fully arrived yet are treated identically -- wait for the next
    // read() -- since there is no way to tell them apart from a length-
    // prefixed stream alone; a malformed *payload* under an otherwise
    // well-formed header drops just that one frame and keeps the
    // connection open, since framing (and therefore synchronization with
    // the byte stream) is still intact.
    void connection_reader_loop(Connection& conn);

    // Runs on `conn`'s own writer thread. Drains, in priority order,
    // conn.replay_backlog (retained reports handed over at bind time),
    // conn.session_outbound (the gateway's own replies) and conn.outbound
    // (execution reports from the matching thread), encoding and writing
    // each Message it finds via protocol::order_entry::encode_message() +
    // TcpSocket::write() -- remember write() can itself return a short
    // count (tcp_socket.hpp's own doc comment), so a single write() call
    // is not guaranteed to flush one whole encoded message. When all three
    // are empty, blocks on conn.wake_cv (see Connection's own doc comment)
    // instead of unconditionally sleeping a fixed interval -- every
    // producer notifies this directly the moment it pushes, so in the
    // common case this thread reacts immediately rather than on its next
    // poll tick. Exits on `token`, or as soon as its own connection is
    // marked closed.
    void connection_writer_loop(Connection& conn, std::stop_token token);

    // Binds this session to `account_id` (its first valid request) and
    // hands it whatever pending_reports_ accumulated for that account
    // while it had no live session. Runs on `conn`'s reader thread.
    void bind_session(Connection& conn, AccountId account_id);

    // Removes this session from account_sessions_ and drops every
    // order_owner_ entry pointing at it, so nothing routes to a connection
    // that is on its way out. Runs on `conn`'s reader thread as it exits.
    void unbind_session(Connection& conn);

    // Answers a request whose account_id isn't the one this session bound
    // to with a Rejected{AccountMismatch}, without `command` ever reaching
    // the pipeline. The connection stays open and stays bound to its
    // original account. Runs on `conn`'s reader thread.
    void reject_account_mismatch(Connection& conn, const ExchangeCommand& command);

    // Records `conn` as the owner of every order id `message` submits
    // under, so route_event() can send the resulting reports back to this
    // session specifically. Must be called *before* submit_command(),
    // otherwise the matching thread can emit an event for an id that has
    // no owner yet. Runs on `conn`'s reader thread.
    void claim_order_ownership(Connection& conn, AccountId account_id,
                                const protocol::order_entry::Message& message);

    // Installed as the EventSink handed to pipeline_'s Processor -- runs on
    // the matching thread (see class-level comment on why it must not
    // block). Calls to_execution_reports(event), then resolves each report
    // to a session under sessions_mutex_ -- owning session, else one
    // other live session of the account, else retained in pending_reports_
    // -- and finally updates order ownership for the event.
    void route_event(const ExchangeEvent& event);

    // Pushes one report onto `conn`'s outbound queue and wakes its writer.
    // Called by route_event() with sessions_mutex_ held; a full queue
    // drops, exactly as before (that connection's writer has fallen
    // behind, and the matching thread must not block on it).
    void deliver(Connection& conn, protocol::order_entry::Message message);

    // Keeps order_owner_ in step with the engine's own live-order
    // lifecycle as events go by: carries ownership from the original to
    // the new id on a replace, and drops keys whose order is provably gone
    // (cancelled, fully filled, or rejected in a way that leaves nothing
    // live behind). Called by route_event() with sessions_mutex_ held.
    void update_order_ownership(const ExchangeEvent& event);

    // Translates one decoded client -> gateway wire message into the
    // matching engine's own command vocabulary. Returns std::nullopt for a
    // message that is well-formed wire-protocol-wise but not valid as a
    // client request (e.g. a gateway -> client type like Accepted/
    // TradeReport arriving from a client) -- connection_reader_loop()
    // silently ignores a std::nullopt rather than disconnecting the client,
    // since this protocol has no NAK/error-response type to report it with.
    [[nodiscard]] static std::optional<ExchangeCommand> to_command(const protocol::order_entry::Message& message);

    // Translates one ExchangeEvent into zero or more (order key, message)
    // pairs to deliver over the wire. Zero for every Book* event (see
    // events.hpp's own comment on why those are deliberately anonymous --
    // they belong to market data, Milestone 6, not order entry). One for
    // OrderAccepted/OrderRejected/OrderCancelled/OrderReplaced. Up to two
    // for TradeExecuted, since its buyer and seller (each already carrying
    // their own account_id, see events.hpp's TradeCounterparty) can belong
    // to two different connections -- see this file's class-level comment
    // and the plan this scaffold was built from for why TradeReport is
    // deliberately one-sided rather than mirroring TradeExecuted's shape.
    // The key is what route_event() resolves to a session: the account
    // alone would only narrow a report down to "some connection of this
    // account", which is exactly the ambiguity the session model exists to
    // remove.
    [[nodiscard]] static std::vector<std::pair<OrderKey, protocol::order_entry::Message>> to_execution_reports(
        const ExchangeEvent& event);

    std::uint16_t port_;
    OrderEntryGatewayOptions options_;

    net::TcpSocket listener_;
    std::stop_source stop_source_; // shared by accept_loop() and every connection_writer_loop()
    std::jthread accept_thread_;

    // Guarded by connections_mutex_: accept_loop() appends on the accept
    // thread, connection_count() and stop() read it from whatever thread
    // calls them. Connection objects are allocated with make_unique
    // specifically so a pointer into one (see the routing state below)
    // stays valid even while this vector itself grows and reallocates,
    // and they are never erased before stop() -- a disconnected session
    // unbinds itself from every map below, which is what makes it
    // unreachable, rather than being destroyed out from under a pointer
    // the matching thread might still be holding.
    mutable std::mutex connections_mutex_;
    std::vector<std::unique_ptr<Connection>> connections_;

    SessionId next_session_id_ = 1; // accept thread only

    // ── Routing state: all three guarded by sessions_mutex_ ───────────────
    // Written by connection reader threads (binding, unbinding, claiming
    // order ids), read and written by the matching thread inside
    // route_event(). All pointers are non-owning -- connections_ above owns
    // the Connection objects themselves.
    std::mutex sessions_mutex_;

    // Every session currently bound to an account, in bind order (oldest
    // first). An account with no live session has no entry at all.
    std::unordered_map<AccountId, std::vector<Connection*>> account_sessions_;

    // Which session an order belongs to, i.e. where its private reports
    // should go. Populated when a reader thread submits a command, kept in
    // step with the engine's live orders by update_order_ownership().
    std::unordered_map<OrderKey, Connection*, OrderKeyHash> order_owner_;

    // Reports for an account that currently has no live session at all,
    // replayed to the next session that binds to it -- see
    // OrderEntryGatewayOptions::pending_report_capacity for the bound on
    // how many are kept.
    std::unordered_map<AccountId, std::deque<protocol::order_entry::Message>> pending_reports_;

    std::mutex submit_mutex_; // see submit_command()'s doc comment

    MatchingEngine engine_;
    ledger::Ledger ledger_;
    risk::RiskGatedEngine risk_gated_engine_; // composes engine_ + ledger_ + risk::RiskEngine
    sequencing::MatchingPipeline pipeline_;    // its Processor is wired to risk_gated_engine_.process()
};

} // namespace mdh::exchange::gateway
