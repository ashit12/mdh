#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>

namespace mdh::net {

// Distinguishes a successful transfer from "the socket is fine but nothing
// is ready" (EAGAIN/EWOULDBLOCK on a non-blocking fd) from a real failure.
// Blocking I/O never produces WouldBlock; collapsing that into Error used
// to be safe and is no longer, once the gateway waits on epoll/kqueue.
enum class IoStatus : std::uint8_t { Ok, WouldBlock, Error };

struct IoResult {
    IoStatus status = IoStatus::Error;
    std::size_t n = 0; // bytes moved; read Ok with n==0 is peer EOF

    [[nodiscard]] bool ok() const { return status == IoStatus::Ok; }
};

// RAII wrapper over a POSIX TCP stream socket -- the
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
    // a new, already-connected TcpSocket. WouldBlock means none is currently
    // pending (including EAGAIN/EWOULDBLOCK on a non-blocking listener).
    // Error is every other failure, including EMFILE/ENFILE -- the gateway
    // must not treat those as "try accept again immediately."
    //
    // An accepted socket is blocking by default even when this listener is
    // not: on BSD/macOS, accept() inherits O_NONBLOCK, and this method
    // clears it so a freshly accepted TcpSocket matches a freshly
    // constructed one. The gateway then calls set_non_blocking() itself.
    //
    // AcceptResult holds an optional<TcpSocket>, so it cannot be defined
    // until TcpSocket is complete; it is declared here and defined just
    // below the class. A declaration may return an incomplete type as long
    // as the definition does not, which is why this compiles.
    struct AcceptResult;
    [[nodiscard]] AcceptResult accept();

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
    // Ok with n==0 is peer EOF. WouldBlock is EAGAIN/EWOULDBLOCK. EINTR is
    // retried internally and never surfaces.
    [[nodiscard]] IoResult read(std::span<std::byte> buf);

    // Writes data, up to data.size() bytes. Same partial-transfer caveat as
    // read(): a return value smaller than data.size() is normal (e.g. the
    // kernel's send buffer is momentarily full) -- this method does not
    // loop internally to force a full write, so a caller with more data
    // than fits in one call must retry with the unwritten remainder itself.
    [[nodiscard]] IoResult write(std::span<const std::byte> data);

    // Puts the socket into non-blocking mode -- same semantics as
    // UdpSocket::set_non_blocking(). Applies to whichever operation this
    // socket is later used for: accept() on a listening socket, or
    // read()/write() on a connected one. Returns false if the fd is closed
    // or fcntl fails.
    [[nodiscard]] bool set_non_blocking();

    // Disables further send and receive on this socket (POSIX shutdown()
    // with SHUT_RDWR). The primary purpose here is unblocking a *different*
    // thread that is currently blocked inside read() on this same,
    // already-connected socket -- e.g. OrderEntryClient's reader being
    // asked to stop -- which is a reliable, portable operation for a
    // connected stream socket on both Linux and macOS.
    //
    // The gateway's I/O thread no longer blocks in read() or accept(); it
    // waits on IoPoller. shutdown() is still used there to fail in-flight
    // syscalls when tearing a single connection down.
    void shutdown();

    [[nodiscard]] int raw_fd() const { return fd_; }

private:
    explicit TcpSocket(int fd) : fd_(fd) {}

    int fd_ = -1;
};

struct TcpSocket::AcceptResult {
    IoStatus status = IoStatus::Error;
    std::optional<TcpSocket> socket;
};

using AcceptResult = TcpSocket::AcceptResult;

} // namespace mdh::net
