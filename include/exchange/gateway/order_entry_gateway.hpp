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

// The exchange's networked front door: the piece that makes the matching
// engine reachable by a real TCP client rather than only by in-process test
// code. It stacks four layers:
//
//   TcpSocket                      raw bytes
//     -> order_entry en/decoder    bytes <-> Message
//     -> this class                Message <-> ExchangeCommand/ExchangeEvent
//     -> MatchingPipeline          risk, ledger and matching behind a queue
//
// Nothing below this class knows it exists. The pipeline, the risk-gated
// engine, the matching engine and the ledger are used exactly as built and
// tested, unaware their caller is now a socket rather than a test.
//
// ── Threads ────────────────────────────────────────────────────────────────
//
//   accept thread (one, polling)
//       |
//       +--> per connection: reader thread --> pipeline.submit()  (MPSC)
//       |                                              |
//       |                                    matching thread (one)
//       |                                              |
//       +--> bounded outbound SPSC <-- route_event()
//                    |
//                    +--> per-connection writer (sleeps on wake_cv)
//
// One accept thread polls accept() in a non-blocking loop against a shared
// stop token rather than blocking inside it -- see accept_loop() for why.
// Each accepted connection gets a blocking reader and a blocking writer.
// The writer owns that socket's outbound direction.
//
// MatchingPipeline::submit() is MPSC: every reader may call it concurrently.
// There is no submit_mutex_. Sequencing is not "whoever grabbed the mutex":
//
//   * One session: the reader is a single thread, so decode order is
//     admission order is matching order (per-session FIFO).
//   * Two sessions: matching order is the lock-free admission race into the
//     pipeline queue. command_sequence is stamped on the matching thread in
//     that order. TCP arrival time is not a promise.
//
// Matching itself stays single-threaded.
//
// route_event() is the sink the pipeline hands to its processor, so it runs
// on the matching thread, synchronously, and must not block. It translates
// each event into wire messages and pushes them onto the target connection's
// own outbound queue. Every connection gets a queue precisely so that
// route_event() never calls write() itself -- one slow client's socket would
// otherwise throttle matching for everyone. It also passes the raw event to
// OrderEntryGatewayOptions::extra_event_sink, if one is set.
//
// ── Sessions, and how a private report finds its way home ─────────────────
//
// One connection is one session. There is no login handshake: every client
// message already carries an account_id, so a connection binds to whichever
// account its first valid request names. Two rules follow, and together they
// are what keeps one client's fills out of another client's socket.
//
//   1. The binding never changes. A later message naming a different
//      account is answered with Rejected{AccountMismatch} and never reaches
//      the pipeline, so a session can never trade -- or spend the balance
//      of -- any account but the one it bound to.
//   2. An account may have many sessions. A second connection for the same
//      account joins it rather than displacing the first.
//
// Rule 2 raises the question of which session a given report belongs to. Not
// all of them: these are private reports, and a session that placed nothing
// should not learn about an order it never sent. So the gateway remembers
// which session submitted each order, keyed on (account_id, client_order_id)
// -- the same key the engine uses for live orders. A reader thread records
// that before it submits, so an event can never arrive for an unknown owner.
//
// route_event() then resolves a report in three steps: the owning session if
// it is still connected; failing that, any other live session of the same
// account (a resting order outlives the session that placed it -- there is
// no cancel-on-disconnect here); failing that, pending_reports_, which holds
// it for the next session to bind to that account. That last queue is
// bounded and in-memory only -- reports can be dropped on overflow and are
// lost on restart -- not durable offline delivery.
//
// None of this reaches into ExchangeCommand or ExchangeEvent. The exchange
// core stays transport-independent, deterministic and replayable, and has
// never heard of a socket; the whole session model lives here.
//
// tests/test_order_entry_gateway_e2e.cpp is the behavioural spec, over real
// TCP sockets.
namespace mdh::exchange::gateway {

// Names one connection for the life of the process. Diagnostics and tests
// only -- routing keys on (account_id, client_order_id), never on this.
using SessionId = std::uint64_t;

struct OrderEntryGatewayOptions {
    risk::RiskLimits risk_limits{};

    // Every instrument this gateway trades. Anything else is answered with
    // InvalidInstrument over the wire. Instrument ids arrive from clients,
    // so without this list every id a client invented got a book of its own.
    // Empty rejects every order, so a real deployment must set it.
    std::vector<InstrumentId> instruments;

    // The pipeline's inbound command queue.
    std::size_t matching_queue_capacity = 1024;

    // Passed to the matching engine -- see kDefaultExpectedRestingOrders. A
    // gateway carrying real order flow should raise it.
    std::size_t expected_resting_orders = MatchingEngine::kDefaultExpectedRestingOrders;

    // Per-connection outbound queue capacity. What happens when one fills is
    // a real policy decision -- see Connection::outbound below.
    std::size_t outbound_queue_capacity = 1024;

    // How many established-but-not-yet-accepted connections the kernel will
    // hold for the accept thread to collect, passed through to POSIX
    // listen() via TcpSocket::listen(). It is a queue depth, not a session
    // limit: it bounds only how many clients may sit waiting to be accepted
    // at one instant, never how many connections this gateway can serve at
    // once, and a connection stops occupying a slot the moment accept_loop()
    // picks it up.
    int accept_backlog = 16;

    // How many reports to keep for an account while it has no live session
    // at all. An order can rest and then fill long after the session that
    // placed it disconnected, and dropping that fill would leave a
    // reconnecting client unable to reconcile its position. Past this many,
    // the oldest is dropped: the newest reports are the ones a reconciling
    // client needs most, and keeping them without bound would let an account
    // that never returns grow this map forever. Zero disables retention.
    std::size_t pending_report_capacity = 1024;

    // An optional second observer of every event the matching thread
    // produces, called from route_event() alongside -- never instead of --
    // the per-connection wire routing. Unlike the wire path, which only
    // looks at account-addressed events, this sees everything, including the
    // anonymous book and trade events. Wiring a MarketDataPublisher in here
    // is what lets a live feed watch a real running gateway.
    //
    // Runs on the matching thread, synchronously, under the same "must not
    // block" rule as route_event() itself. Null by default.
    EventSink extra_event_sink;

    // How many immediately available outbound messages a writer may encode
    // into one write(). It never waits for a batch to fill, so an isolated
    // report still leaves promptly. Production is 4: that was the measured
    // syscall win under load. The latency bench may still raise this to
    // reproduce historical tables.
    std::size_t writer_batch = 4;

    // Benchmark/diagnostic counters around the actual server-side read() and
    // write() call sites. Disabled in production by default; the disabled
    // hot path is one relaxed atomic load.
    bool enable_io_metrics = false;
};

struct OrderEntryIoMetrics {
    std::uint64_t read_syscalls = 0;
    std::uint64_t bytes_read = 0;
    std::uint64_t frames_decoded = 0;
    std::uint64_t write_syscalls = 0;
    std::uint64_t bytes_written = 0;
    std::uint64_t reports_enqueued = 0;
    std::uint64_t reports_written = 0;
    std::uint64_t write_errors = 0;
    std::uint64_t outbound_drops = 0;
};

class OrderEntryGateway {
public:
    explicit OrderEntryGateway(std::uint16_t port, const OrderEntryGatewayOptions& options = {});

    // Calls stop(); see it for what is guaranteed before teardown.
    ~OrderEntryGateway();

    OrderEntryGateway(const OrderEntryGateway&) = delete;
    OrderEntryGateway& operator=(const OrderEntryGateway&) = delete;
    OrderEntryGateway(OrderEntryGateway&&) = delete;
    OrderEntryGateway& operator=(OrderEntryGateway&&) = delete;

    // Starts listening and spawns the accept thread. Returns false, having
    // spawned nothing, if listen() fails -- a port already in use, say -- so
    // a caller can check before assuming the gateway is reachable.
    [[nodiscard]] bool start();

    // Shuts everything down, in this order:
    //   1. Request stop on the shared stop source, and wake every writer.
    //   2. Join the accept thread, so no new connection can appear while
    //      steps 3 and 4 walk the connection list.
    //   3. shutdown() every live socket. This is what unblocks each reader
    //      thread, which is otherwise parked in a blocking read().
    //   4. Join every reader and writer.
    //   5. Stop the pipeline, which drains whatever is already queued.
    // Safe to call more than once, including from the destructor, and a
    // no-op on a gateway never started or already stopped.
    void stop();

    // The port actually being listened on -- meaningful after a successful
    // start(), and the only way to find out when constructed with port 0.
    [[nodiscard]] std::optional<std::uint16_t> local_port() const { return listener_.local_port(); }

    // How many connections have ever been accepted. Does not go down when a
    // client disconnects; see Connection on why dead ones are not pruned.
    [[nodiscard]] std::size_t connection_count() const;

    // Read-only access to the matching state, for tests that want to assert
    // on the book rather than only on wire responses.
    //
    // Only safe once stop() has returned. The matching thread mutates
    // `engine_` on every command, so calling this while the gateway is
    // running is a data race -- unguarded at runtime, exactly like
    // MatchingPipeline::snapshot(), which has the same precondition.
    [[nodiscard]] EngineStateSnapshot snapshot() const { return engine_.snapshot(); }

    // Best-effort inbound queue occupancy, safe from any thread.
    [[nodiscard]] std::size_t matching_queue_size() const { return pipeline_.queue_size(); }
    [[nodiscard]] std::size_t matching_queue_high_water_mark() const { return pipeline_.queue_high_water_mark(); }
    [[nodiscard]] OrderEntryIoMetrics io_metrics() const;

    // Test and admin seeding, forwarded to the ledger -- the only way
    // balances ever start out non-zero. The ledger is not synchronized
    // against the matching thread, so call these before any traffic that
    // could race with them.
    void deposit_cash(AccountId account_id, ledger::Balance amount) { ledger_.deposit_cash(account_id, amount); }
    void deposit_position(AccountId account_id, InstrumentId instrument_id, Quantity amount) {
        ledger_.deposit_position(account_id, instrument_id, amount);
    }

private:
    // Names one order for as long as the gateway needs to know which session
    // it came from. Same shape as the engine's own live-order key, for the
    // same reason: a client order id is unique only within an account, so
    // anything keyed on it alone would be ambiguous across two accounts that
    // both chose id 1.
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
    // belongs to it. Held by unique_ptr in connections_ so its address stays
    // put for the life of the connection even as that vector grows, because
    // the routing maps below hold raw pointers into these.
    struct Connection {
        explicit Connection(SessionId id, net::TcpSocket socket_in, std::size_t outbound_capacity)
            : session_id(id), socket(std::move(socket_in)), outbound(outbound_capacity),
              session_outbound(outbound_capacity) {}

        SessionId session_id;

        net::TcpSocket socket;

        // Bytes read but not yet decoded into a complete message. TCP is a
        // byte stream with no message boundaries, so one read() can return
        // half a message or three and a half, and the reader has to
        // accumulate here until there is enough to decode.
        std::vector<std::byte> read_buffer;

        // Reports for this client: pushed by route_event() on the matching
        // thread, drained by this connection's writer thread. What to do
        // when it fills is a genuine policy choice rather than an oversight
        // -- see MatchingPipeline on drop versus reject versus block.
        SpscQueue<protocol::order_entry::Message> outbound;

        // The gateway's own replies to this connection -- at present only
        // the Rejected{AccountMismatch} a bound session gets for claiming
        // somebody else's account. This is a second queue rather than a
        // second producer on `outbound` because SpscQueue permits exactly
        // one producer, and these come from the reader thread while
        // `outbound`'s come from the matching thread. The writer thread
        // drains both.
        SpscQueue<protocol::order_entry::Message> session_outbound;

        // Reports that piled up for this account while it had no live
        // session, handed over when this one binds so a reconnecting client
        // can reconcile. The writer drains this ahead of both queues above,
        // and the handover happens under sessions_mutex_ before this session
        // is visible for routing, so a retained report can never arrive
        // after a live one.
        std::mutex replay_mutex;
        std::vector<protocol::order_entry::Message> replay_backlog;

        // Lets the writer thread sleep until there is actually something to
        // send, rather than waking on a fixed interval to look. That poll
        // interval used to dominate this gateway's end-to-end latency; see
        // docs/benchmarks.md §7.
        //
        // wake_mutex guards only the wait itself -- `outbound` stays
        // lock-free. A producer does not need to hold it to notify, because
        // the writer re-checks the queue as its own wait predicate, so a
        // notification arriving just before the wait begins is redundant
        // rather than lost. The wait still has a timeout, purely as a safety
        // net against a missed wakeup.
        std::mutex wake_mutex;
        std::condition_variable wake_cv;

        // The account this session bound to on its first valid request, and
        // nothing else ever after. Written once, by this connection's reader
        // thread alone, so it needs no atomic. account_sessions_ is what
        // makes the binding visible to the matching thread; this field is
        // just how the reader checks "am I bound, and to whom" on every
        // later message without taking a mutex.
        std::optional<AccountId> account_id;

        // Set by the reader thread as it exits -- peer hung up, read failed,
        // or stop() shut the socket down -- and read by the matching thread.
        // Between that store and the unbinding that follows, this is what
        // stops a report being pushed onto a queue whose writer is on its
        // way out; after unbinding, nothing can route here at all. It is
        // also the writer's own exit condition, so a disconnected client's
        // writer does not linger until stop().
        std::atomic<bool> closed{false};

        std::jthread reader_thread;
        std::jthread writer_thread;
    };

    // Forwards to the pipeline. Any reader thread may call this; the queue
    // is MPSC and the matching thread is the sequencer.
    [[nodiscard]] bool submit_command(ExchangeCommand command);

    // ── Translation between the wire and the exchange core ────────────────
    // tests/test_order_entry_gateway_e2e.cpp is the spec for all of it.

    // Runs on the accept thread. Polls accept() against the stop token and,
    // for each new connection, builds a Connection, adds it to the list, and
    // spawns its reader and writer threads.
    void accept_loop();

    // Runs on this connection's reader thread. Reads bytes, accumulates them
    // in conn.read_buffer, decodes whole messages out of it, translates each
    // to a command and submits it. Also handles this session's account
    // binding, claims ownership of every order id it submits, and unbinds
    // the session on the way out. Exits when read() reports end of stream or
    // an error, including the shutdown() that stop() performs.
    //
    // A bad type byte and a header whose payload has not fully arrived are
    // treated identically -- wait for more bytes -- because a length-
    // prefixed stream cannot tell them apart. A malformed payload under a
    // valid header drops only that message and keeps the connection, since
    // framing is still intact.
    void connection_reader_loop(Connection& conn);

    // Per-connection writer implementation. Drains, in this order: the
    // replay backlog handed over at bind time, the gateway's own replies,
    // and reports from the matching thread -- encoding and writing each.
    // write() can return a short count, so one call is not guaranteed to
    // flush a whole message. When all three are empty it blocks on the
    // connection's condition variable rather than polling, so it reacts the
    // moment a producer pushes. Exits on the stop token, or as soon as its
    // connection is marked closed.
    void connection_writer_loop(Connection& conn, std::stop_token token);

    void notify_writer(Connection& conn);

    // Binds this session to `account_id` on its first valid request and
    // hands over whatever reports accumulated for that account while it had
    // no live session. Runs on the reader thread.
    void bind_session(Connection& conn, AccountId account_id);

    // Drops this session from the account map and removes every ownership
    // entry pointing at it, so nothing can route to a connection on its way
    // out. Runs on the reader thread as it exits.
    void unbind_session(Connection& conn);

    // Answers a request naming an account this session is not bound to with
    // a Rejected{AccountMismatch}, without the command reaching the
    // pipeline. The connection stays open and stays bound to its original
    // account. Runs on the reader thread.
    void reject_account_mismatch(Connection& conn, const ExchangeCommand& command);

    // Records this connection as the owner of every order id the message
    // submits under, so the resulting reports come back to this session.
    // Must run before submit_command(), or the matching thread could emit an
    // event for an id with no owner yet. Runs on the reader thread.
    void claim_order_ownership(Connection& conn, AccountId account_id,
                                const protocol::order_entry::Message& message);

    // The sink handed to the pipeline, so it runs on the matching thread and
    // must not block. Translates the event into reports, resolves each to a
    // session -- owning session, else another live session of the account,
    // else retained -- and then updates order ownership.
    void route_event(const ExchangeEvent& event);

    // Pushes one report onto a connection's outbound queue. The caller
    // wakes the writer after dropping sessions_mutex_: notify_one() can
    // switch to the writer thread, and holding the routing mutex across
    // that hand-off stalls every reader in claim_order_ownership().
    // Returns whether the writer should be woken.
    [[nodiscard]] bool deliver(Connection& conn, protocol::order_entry::Message message);

    // Keeps ownership in step with the engine's live orders as events go by:
    // moves ownership to the new id on a replace, and drops keys whose order
    // is provably gone -- cancelled, fully filled, or rejected leaving
    // nothing behind. Called with sessions_mutex_ held.
    void update_order_ownership(const ExchangeEvent& event);

    // Turns one decoded client message into a command. Returns nullopt for a
    // message that is valid wire protocol but not a valid client request --
    // a gateway-to-client type like Accepted arriving from a client, say.
    // The reader ignores a nullopt rather than disconnecting, since this
    // protocol has no error-response type to report it with.
    [[nodiscard]] static std::optional<ExchangeCommand> to_command(const protocol::order_entry::Message& message);

    // Turns one event into zero or more (order key, message) pairs to send.
    // Zero for every book event -- those are anonymous and belong to market
    // data, not order entry. One for accepted, rejected, cancelled and
    // replaced. Up to two for a trade, whose buyer and seller can belong to
    // different connections, which is why TradeReport is one-sided rather
    // than mirroring TradeExecuted.
    //
    // The key is what route_event() resolves to a session. The account alone
    // would only narrow a report down to "some connection of this account",
    // which is the exact ambiguity the session model exists to remove.
    [[nodiscard]] static std::vector<std::pair<OrderKey, protocol::order_entry::Message>> to_execution_reports(
        const ExchangeEvent& event);

    std::uint16_t port_;
    OrderEntryGatewayOptions options_;

    net::TcpSocket listener_;
    std::stop_source stop_source_; // shared by accept_loop() and every connection_writer_loop()
    std::jthread accept_thread_;

    // Guarded by connections_mutex_: the accept thread appends, and
    // connection_count() and stop() read from whatever thread calls them.
    // Connections are held by unique_ptr so the raw pointers in the routing
    // maps below stay valid as this vector grows, and none are erased before
    // stop() -- a disconnected session makes itself unreachable by unbinding
    // rather than by being destroyed under a pointer the matching thread may
    // still hold.
    mutable std::mutex connections_mutex_;
    std::vector<std::unique_ptr<Connection>> connections_;

    SessionId next_session_id_ = 1; // accept thread only

    // ── Routing state, all guarded by sessions_mutex_ ─────────────────────
    // Written by reader threads when they bind, unbind or claim an order id,
    // and read and written by the matching thread in route_event(). Every
    // pointer here is non-owning; connections_ above owns the objects.
    std::mutex sessions_mutex_;

    // Every session bound to an account, oldest first. An account with no
    // live session has no entry at all.
    std::unordered_map<AccountId, std::vector<Connection*>> account_sessions_;

    // Which session an order belongs to, and therefore where its private
    // reports go. Filled in when a reader thread submits a command, kept in
    // step with the engine's live orders by update_order_ownership().
    std::unordered_map<OrderKey, Connection*, OrderKeyHash> order_owner_;

    // Reports for an account with no live session, replayed to the next
    // session that binds to it. Bounded by pending_report_capacity.
    std::unordered_map<AccountId, std::deque<protocol::order_entry::Message>> pending_reports_;

    std::atomic<bool> io_metrics_enabled_{false};
    std::atomic<std::uint64_t> read_syscalls_{0};
    std::atomic<std::uint64_t> bytes_read_{0};
    std::atomic<std::uint64_t> frames_decoded_{0};
    std::atomic<std::uint64_t> write_syscalls_{0};
    std::atomic<std::uint64_t> bytes_written_{0};
    std::atomic<std::uint64_t> reports_enqueued_{0};
    std::atomic<std::uint64_t> reports_written_{0};
    std::atomic<std::uint64_t> write_errors_{0};
    std::atomic<std::uint64_t> outbound_drops_{0};

    MatchingEngine engine_;
    ledger::Ledger ledger_;
    risk::RiskGatedEngine risk_gated_engine_; // engine_ + ledger_ + RiskEngine
    sequencing::MatchingPipeline pipeline_;    // its processor calls risk_gated_engine_
};

} // namespace mdh::exchange::gateway
