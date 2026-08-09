#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "net/tcp_socket.hpp"
#include "protocol/order_entry/messages.hpp"

// The trader-side network transport for the order-entry protocol
// (Milestone 8) -- the client-side counterpart to
// exchange::gateway::OrderEntryGateway's per-connection reader/writer pair
// (Milestone 7), reusing the exact same net::TcpSocket and
// protocol::order_entry:: encoder/decoder the gateway itself uses (the wire
// format is the only contract between the two sides). Deliberately kept
// separate from OrderManagementSystem (order_management_system.hpp), which
// depends only on this class's shape (a Sender + a place to feed decoded
// Messages into) and not on this class itself -- see that header's own
// class-level comment for why.
//
// ── Simpler than the gateway's Connection, in two ways ────────────────────
// - One background reader thread only, no writer thread: send() writes
//   directly, synchronously, on the caller's own thread. The gateway needed
//   a writer thread + per-connection outbound queue specifically so one
//   slow client's socket could never hold the shared matching thread
//   hostage (route_event() must not block, see order_entry_gateway.hpp).
//   There is no equivalent shared resource here for a blocking write to
//   endanger -- this class *is* the one connection, so a caller blocking
//   briefly on its own send() blocks only itself.
// - No accept loop / connection registry: this class owns exactly one
//   connection it initiates via connect(), not a listener managing many.
namespace mdh::trader::oms {

class OrderEntryClient {
public:
    // Invoked once per decoded gateway -> client Message, synchronously, on
    // this class's own reader thread -- same "no thread-hopping baked into
    // the primitive itself" convention as EventSink (exchange/core/
    // event_sink.hpp) and the gateway's own route_event(). A caller that
    // needs delivery on a different thread (e.g. OrderManagementSystem's
    // handle_message(), if the owner wants it invoked elsewhere) must
    // arrange that itself.
    using MessageSink = std::function<void(const protocol::order_entry::Message&)>;

    explicit OrderEntryClient(MessageSink sink);

    // Calls disconnect() -- see its own doc comment.
    ~OrderEntryClient();

    OrderEntryClient(const OrderEntryClient&) = delete;
    OrderEntryClient& operator=(const OrderEntryClient&) = delete;
    OrderEntryClient(OrderEntryClient&&) = delete;
    OrderEntryClient& operator=(OrderEntryClient&&) = delete;

    // Connects to host:port (see TcpSocket::connect()'s own doc comment on
    // `host`'s IPv4-literal-only restriction) and starts the background
    // reader thread. Returns false, starting no thread, if connect() itself
    // fails.
    [[nodiscard]] bool connect(const std::string& host, std::uint16_t port);

    // Encodes `message` and writes it in full, loop over TcpSocket::write()'s
    // short-write behavior exactly like the gateway's own
    // connection_writer_loop() does. Safe to call from any thread
    // (serialized by write_mutex_) -- cheap enough not to need anything
    // fancier, and simpler to make outright safe than to document a
    // single-caller restriction every caller has to remember (same
    // rationale as the gateway's own submit_command()). Returns false on a
    // write error, e.g. the connection has already dropped.
    [[nodiscard]] bool send(const protocol::order_entry::Message& message);

    // Shuts down the socket (unblocking the reader thread's blocking
    // read(), see tcp_socket.hpp's own shutdown() doc comment) and joins
    // it. Safe to call more than once, including implicitly via the
    // destructor.
    void disconnect();

    // Best-effort: true once connect() has succeeded, false once the
    // reader thread has observed EOF/an error (including a disconnect()
    // triggered locally). Written by the reader thread, read from any
    // thread.
    [[nodiscard]] bool is_connected() const { return connected_.load(std::memory_order_relaxed); }

private:
    void reader_loop();

    net::TcpSocket socket_;
    std::mutex write_mutex_;
    MessageSink sink_;
    std::vector<std::byte> read_buffer_; // reader-thread-only
    std::jthread reader_thread_;
    std::atomic<bool> connected_{false};
};

} // namespace mdh::trader::oms
