#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>

namespace mdh::net {

// RAII wrapper over a POSIX TCP stream socket (Milestone 7) -- the
// order-entry gateway's transport primitive, the same role UdpSocket plays
// for market data. Uses the same BSD sockets API as UdpSocket (shared by
// Linux and macOS), for the same local-development reason.
//
// ── Why this is a separate class from UdpSocket, not a mode flag on it ───
// TCP is connection-oriented and stream-based; UDP is connectionless and
// datagram-based. That difference shows up in the API itself, not just
// behavior: TCP needs listen()/accept()/connect() (no such thing exists for
// UDP, which has no notion of "a connection"), and its read()/write() can
// return fewer bytes than requested (a "short" transfer is normal for a
// stream, whereas UdpSocket::send_to()/receive() are always whole,
// atomic datagrams). Bolting all of that onto UdpSocket behind a mode flag
// would make every method's contract conditional on that flag; two small,
// single-purpose classes are simpler than one class with two personalities.
//
// Not copyable (owns a single fd); movable, same as UdpSocket.
class TcpSocket {
public:
    // Opens a fresh, unconnected TCP socket.
    TcpSocket();
    ~TcpSocket();

    TcpSocket(const TcpSocket&) = delete;
    TcpSocket& operator=(const TcpSocket&) = delete;
    TcpSocket(TcpSocket&& other) noexcept;
    TcpSocket& operator=(TcpSocket&& other) noexcept;

    [[nodiscard]] bool is_open() const { return fd_ >= 0; }

    // Server side. Binds to 0.0.0.0:port (pass 0 for an OS-assigned
    // ephemeral port, retrievable via local_port() -- same convention as
    // UdpSocket::bind()) and starts listening, with `backlog` as the
    // pending-connection queue size passed to POSIX listen(). Sets
    // SO_REUSEADDR first so a test or restarted process can immediately
    // rebind a port still in TIME_WAIT from a just-closed prior socket.
    // Returns false on failure.
    [[nodiscard]] bool listen(std::uint16_t port, int backlog = 16);

    // Server side. Accepts one pending connection, if any, returning it as
    // a new, already-connected TcpSocket. Returns std::nullopt if none is
    // currently pending -- including EWOULDBLOCK/EAGAIN when this
    // (listening) socket is non-blocking -- or on any other error.
    //
    // ── Why the gateway must call this in a non-blocking poll loop ──────
    // See this class's shutdown() doc comment: unlike a blocked read() on
    // an already-connected socket, a thread blocked *inside* accept() on a
    // listening socket cannot be reliably unblocked from another thread on
    // every platform this project targets. accept() itself has no timeout
    // parameter, so the only portable way to make an accept loop stoppable
    // is to never block in accept() at all -- put the listening socket in
    // non-blocking mode (set_non_blocking()) and poll this method against a
    // stop_token instead.
    [[nodiscard]] std::optional<TcpSocket> accept();

    // Client side. Connects to host:port. `host` must be an IPv4
    // dotted-decimal literal (e.g. "127.0.0.1") -- no DNS resolution, same
    // restriction and rationale as UdpSocket::send_to(). Returns false on
    // failure (including connection refused, e.g. nothing listening on that
    // port).
    [[nodiscard]] bool connect(const std::string& host, std::uint16_t port);

    // The actual local port this socket is bound to -- meaningful after a
    // successful listen(), especially when it was called with port 0.
    [[nodiscard]] std::optional<std::uint16_t> local_port() const;

    // Reads whatever is currently available into buf, up to buf.size()
    // bytes.
    //
    // ── Why a short read is normal here but never was for UdpSocket ─────
    // TCP delivers an ordered byte stream with no preserved message
    // boundaries, so one read() can return anywhere from 1 byte to
    // buf.size() bytes regardless of how many bytes the peer's single
    // write() call sent -- there is no "atomic datagram" guarantee the way
    // there is for UdpSocket::receive(). This is exactly why
    // protocol/order_entry/ needs its own length-prefixed framing: a caller
    // here must be prepared to call read() repeatedly and accumulate bytes
    // until it has a complete frame, never assume one read() equals one
    // message.
    //
    // Returns 0 if the peer performed an orderly shutdown (end of stream);
    // returns std::nullopt on error, including EWOULDBLOCK/EAGAIN when this
    // socket is non-blocking and nothing is currently available to read.
    [[nodiscard]] std::optional<std::size_t> read(std::span<std::byte> buf);

    // Writes data, up to data.size() bytes. Same partial-transfer caveat as
    // read(): a return value smaller than data.size() is normal (e.g. the
    // kernel's send buffer is momentarily full) -- this method does not
    // loop internally to force a full write, so a caller with more data
    // than fits in one call must retry with the unwritten remainder itself.
    // Returns std::nullopt on error.
    [[nodiscard]] std::optional<std::size_t> write(std::span<const std::byte> data);

    // Puts the socket into non-blocking mode -- same semantics as
    // UdpSocket::set_non_blocking(). Applies to whichever operation this
    // socket is later used for: accept() on a listening socket, or
    // read()/write() on a connected one.
    void set_non_blocking();

    // Disables further send and receive on this socket (POSIX shutdown()
    // with SHUT_RDWR). The primary purpose here is unblocking a *different*
    // thread that is currently blocked inside read() on this same,
    // already-connected socket -- e.g. a connection's reader thread being
    // asked to stop -- which is a reliable, portable operation for a
    // connected stream socket on both Linux and macOS.
    //
    // ── Platform caveat: does NOT reliably unblock a blocked accept() ───
    // On Linux, calling shutdown() on a *listening* socket also reliably
    // unblocks a thread blocked inside accept() on it. On macOS, it does
    // not -- a thread already blocked in accept() can remain blocked
    // indefinitely even after shutdown() returns success on that socket.
    // Since this project is developed and must behave correctly on both
    // platforms, nothing here may depend on that Linux-only behavior:
    // accept() is always driven from a non-blocking poll loop instead (see
    // accept()'s own doc comment), and shutdown() is used only for its
    // portable purpose -- unblocking a blocked read() on a connected
    // socket, not a blocked accept() on a listening one.
    void shutdown();

    [[nodiscard]] int raw_fd() const { return fd_; }

private:
    explicit TcpSocket(int fd) : fd_(fd) {} // used by accept() to wrap the newly-accepted fd

    int fd_ = -1;
};

} // namespace mdh::net
