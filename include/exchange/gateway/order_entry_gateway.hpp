#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
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
#include "net/io_poller.hpp"
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
//   I/O thread (one, IoPoller: epoll on Linux, kqueue on macOS)
//       accept + read + write for every connection
//       |
//       +--> pipeline.submit()  (MPSC)
//                    |
//                    v
//           matching thread (one)
//                    |
//                    +--> bounded outbound SPSC + dirty list + poller.wake()
//
// The I/O thread waits on readiness rather than blocking in read()/accept()
// or sleeping 1ms. Each connection is non-blocking. Matching still only
// enqueues; it never write()s. One slow client cannot stall the others.
//
// MatchingPipeline::submit() is MPSC: the I/O thread may call it for every
// connection. Sequencing is not "whoever grabbed the mutex":
//
//   * One session: decode order is admission order is matching order
//     (per-session FIFO).
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
// -- the same key the engine uses for live orders. The I/O thread records
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
    std::vector<InstrumentId> instruments{};

    // The pipeline's inbound command queue.
    std::size_t matching_queue_capacity = 1024;

    // Passed to the matching engine -- see kDefaultExpectedRestingOrders. A
    // gateway carrying real order flow should raise it.
    std::size_t expected_resting_orders = MatchingEngine::kDefaultExpectedRestingOrders;

    // Per-connection outbound queue capacity. What happens when one fills is
    // a real policy decision -- see Connection::outbound below.
    std::size_t outbound_queue_capacity = 1024;

    // How many established-but-not-yet-accepted connections the kernel will
    // hold for the I/O thread to collect, passed through to POSIX
    // listen() via TcpSocket::listen(). It is a queue depth, not a session
    // limit: it bounds only how many clients may sit waiting to be accepted
    // at one instant, never how many connections this gateway can serve at
    // once, and a connection stops occupying a slot the moment the I/O
    // thread picks it up.
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
    // anonymous book and trade events. Production wires a MarketDataRouter
    // here: it translates and performs one bounded SPSC push, while its
    // routing thread packs and sends UDP.
    //
    // Runs on the matching thread, synchronously, under the same "must not
    // block" rule as route_event() itself. Null by default.
    EventSink extra_event_sink{};

    // How many immediately available outbound messages the I/O thread may
    // encode into one write(). It never waits for a batch to fill, so an
    // isolated report still leaves promptly. Production is 4: that was the
    // measured syscall win under load. The latency bench may still raise
    // this to reproduce historical tables.
    std::size_t writer_batch = 4;

    // Benchmark/diagnostic counters around the actual server-side read() and
    // write() call sites. Disabled in production by default; the disabled
    // hot path is one relaxed atomic load.
    bool enable_io_metrics = false;

    // Forwarded to MatchingPipelineOptions::matching_cpu. Unset by default:
    // the matching thread is not pinned, which is what every test and the
    // trading_server demo want. The order-path bench sets this to isolate
    // matching on a dedicated CPU (1 on the current isolcpus host).
    std::optional<unsigned> matching_cpu{};
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

    // Starts listening and spawns the I/O thread. Returns false, having
    // spawned nothing, if listen() or poller setup fails -- a port already
    // in use, say -- so a caller can check before assuming the gateway is
    // reachable.
    [[nodiscard]] bool start();

    // Shuts everything down, in this order:
    //   1. Request stop and wake the poller.
    //   2. Join the I/O thread (no further accept/read/write).
    //   3. Stop the pipeline, which drains whatever is already queued.
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
    [[nodiscard]] std::size_t commands_processed() const { return pipeline_.commands_processed(); }
    [[nodiscard]] std::size_t commands_rejected() const { return pipeline_.commands_rejected(); }
    [[nodiscard]] std::size_t resting_order_count() const { return engine_.resting_order_count(); }
    [[nodiscard]] MatchingEngine::BookMemoryStats book_memory_stats() const { return engine_.book_memory_stats(); }
    [[nodiscard]] std::size_t ledger_account_count() const { return ledger_.account_count(); }
    [[nodiscard]] std::size_t ledger_hold_count() const { return ledger_.hold_count(); }
    [[nodiscard]] std::uint64_t matching_thread_id() const { return pipeline_.matching_thread_id(); }
    [[nodiscard]] bool matching_affinity_ready() const { return pipeline_.matching_affinity_ready(); }
    [[nodiscard]] bool matching_cpu_pinned() const { return pipeline_.matching_cpu_pinned(); }
    [[nodiscard]] std::string matching_affinity_error() const { return pipeline_.matching_affinity_error(); }
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
        // byte stream with no message boundaries; a header can arrive in one
        // poller wakeup and its payload in the next. decode_message is only
        // called once this buffer holds a whole frame.
        std::vector<std::byte> read_buffer;

        // Partial outbound batch. Non-blocking write can return WouldBlock
        // mid-frame; the remainder stays here until the socket is writable.
        std::vector<std::byte> write_buf;
        std::size_t write_offset = 0;
        std::vector<protocol::order_entry::Message> write_pending;

        // Reports for this client: pushed by route_event() on the matching
        // thread, drained by the I/O thread. What to do when it fills is a
        // genuine policy choice rather than an oversight -- see
        // MatchingPipeline on drop versus reject versus block.
        SpscQueue<protocol::order_entry::Message> outbound;

        // The gateway's own replies to this connection -- at present only
        // the Rejected{AccountMismatch} a bound session gets for claiming
        // somebody else's account. This is a second queue rather than a
        // second producer on `outbound` because SpscQueue permits exactly
        // one producer, and these come from the I/O thread while
        // `outbound`'s come from the matching thread. The I/O thread drains
        // both.
        SpscQueue<protocol::order_entry::Message> session_outbound;

        // Reports that piled up for this account while it had no live
        // session, handed over when this one binds so a reconnecting client
        // can reconcile. The I/O thread drains this ahead of both queues
        // above, and the handover happens under sessions_mutex_ before this
        // session is visible for routing, so a retained report can never
        // arrive after a live one.
        std::mutex replay_mutex;
        std::vector<protocol::order_entry::Message> replay_backlog;

        // The account this session bound to on its first valid request, and
        // nothing else ever after. Written once, by the I/O thread alone, so
        // it needs no atomic. account_sessions_ is what makes the binding
        // visible to the matching thread; this field is just how the I/O
        // thread checks "am I bound, and to whom" on every later message
        // without taking a mutex.
        std::optional<AccountId> account_id;

        // Set as the I/O thread tears the connection down -- peer hung up,
        // read/write failed, or stop() -- and read by the matching thread.
        // Between that store and the unbinding that follows, this is what
        // stops a report being pushed onto a queue whose socket is on its
        // way out; after unbinding, nothing can route here at all.
        std::atomic<bool> closed{false};

        // I/O thread only: whether EPOLLOUT / EVFILT_WRITE is currently
        // registered. Matching thread never touches this.
        bool write_interest = false;

        // Matching thread sets this and, on 0->1, pushes onto dirty_. I/O
        // thread clears it when enabling write interest. Coalesces wakes.
        std::atomic<bool> dirty{false};
    };

    [[nodiscard]] bool submit_command(ExchangeCommand command);

    void io_loop();
    void accept_ready();
    void pause_listen();
    void maybe_resume_listen();
    void handle_read(Connection& conn);
    void handle_write(Connection& conn);
    void close_connection(Connection& conn);
    void enable_write(Connection& conn);
    void disable_write(Connection& conn);
    void drain_dirty();
    void mark_dirty(Connection& conn);

    [[nodiscard]] std::optional<protocol::order_entry::Message> pop_outbound(Connection& conn);

    // Matching thread: enqueue pointer + wake poller. Never write()s.
    void notify_writer(Connection& conn);

    void bind_session(Connection& conn, AccountId account_id);
    void unbind_session(Connection& conn);
    void reject_account_mismatch(Connection& conn, const ExchangeCommand& command);
    void claim_order_ownership(Connection& conn, AccountId account_id,
                                const protocol::order_entry::Message& message);
    void route_event(const ExchangeEvent& event);
    [[nodiscard]] bool deliver(Connection& conn, protocol::order_entry::Message message);
    void update_order_ownership(const ExchangeEvent& event);
    [[nodiscard]] static std::optional<ExchangeCommand> to_command(const protocol::order_entry::Message& message);
    [[nodiscard]] static std::vector<std::pair<OrderKey, protocol::order_entry::Message>> to_execution_reports(
        const ExchangeEvent& event);

    std::uint16_t port_;
    OrderEntryGatewayOptions options_;

    net::TcpSocket listener_;
    net::IoPoller poller_;
    std::stop_source stop_source_;
    std::jthread io_thread_;
    bool listen_armed_ = false;

    mutable std::mutex connections_mutex_;
    std::vector<std::unique_ptr<Connection>> connections_;

    SessionId next_session_id_ = 1; // I/O thread only

    SpscQueue<Connection*> dirty_{65536};
    std::atomic<bool> dirty_overflow_{false};

    std::mutex sessions_mutex_;
    std::unordered_map<AccountId, std::vector<Connection*>> account_sessions_;
    std::unordered_map<OrderKey, Connection*, OrderKeyHash> order_owner_;
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
