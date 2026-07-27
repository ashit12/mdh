#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>

namespace mdh::net {

// RAII wrapper over a POSIX UDP socket. Uses the BSD sockets API
// (socket/bind/sendto/recvfrom/close), which is shared by Linux and macOS,
// so this works for local development even though the project is
// Linux-oriented overall.
//
// Deliberately minimal for milestone 2: `send_to` takes an IPv4
// dotted-decimal literal (e.g. "127.0.0.1"), not a hostname -- there is no
// DNS resolution (no getaddrinfo) here, since every use case in this
// milestone is a literal loopback or LAN address. IPv6 is out of scope.
//
// Not copyable (owns a single fd); movable.
class UdpSocket {
public:
    UdpSocket();
    ~UdpSocket();

    UdpSocket(const UdpSocket&) = delete;
    UdpSocket& operator=(const UdpSocket&) = delete;
    UdpSocket(UdpSocket&& other) noexcept;
    UdpSocket& operator=(UdpSocket&& other) noexcept;

    [[nodiscard]] bool is_open() const { return fd_ >= 0; }

    // Binds to 0.0.0.0:port for receiving. Pass 0 to let the OS assign an
    // ephemeral port (retrievable via local_port()) -- useful for tests
    // that need a collision-free port. Returns false on failure.
    [[nodiscard]] bool bind(std::uint16_t port);

    // The actual local port this socket is bound to (meaningful after a
    // successful bind(), especially when bind() was called with port 0).
    [[nodiscard]] std::optional<std::uint16_t> local_port() const;

    // Sends one datagram to host:port. Returns false if the socket isn't
    // open, `host` isn't a valid IPv4 literal, or the OS reports a short
    // write (UDP datagrams are sent atomically, so a "short write" here
    // effectively means the send failed outright).
    [[nodiscard]] bool send_to(std::span<const std::byte> data, const std::string& host, std::uint16_t port);

    // Receives one datagram into buf. Returns the number of bytes
    // received, or std::nullopt on error -- including EWOULDBLOCK/EAGAIN
    // when the socket is in non-blocking mode and nothing is pending.
    [[nodiscard]] std::optional<std::size_t> receive(std::span<std::byte> buf);

    // Puts the socket into non-blocking mode, so receive() returns
    // std::nullopt promptly instead of blocking when nothing is pending.
    // Used by UdpReceiver's batched-drain loop (see net/udp_receiver.hpp).
    void set_non_blocking();

    [[nodiscard]] int raw_fd() const { return fd_; }

private:
    int fd_ = -1;
};

} // namespace mdh::net
