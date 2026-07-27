#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "net/udp_socket.hpp"

namespace mdh::net {

// One received datagram, tagged with a local receive timestamp -- when
// *this* process saw it via recvfrom(), not any timestamp the sender
// claims. Useful for later end-to-end latency measurement (milestone 5);
// not otherwise acted on in milestone 2.
struct ReceivedDatagram {
    std::vector<std::byte> bytes;
    std::uint64_t receive_timestamp_ns;
};

// Batched UDP receive: drains up to max_batch pending datagrams per call
// instead of processing one datagram per syscall-then-decode round trip
// through the rest of the pipeline.
//
// Implementation choice: a recvfrom() loop over a non-blocking socket,
// portable to both Linux and macOS. Linux also offers recvmmsg(), which
// receives multiple datagrams in a single syscall (rather than one
// syscall per datagram, as this loop does) -- a genuine additional
// speedup on top of what's implemented here. It is deliberately NOT
// implemented in this project: this dev/test machine is macOS, recvmmsg()
// doesn't exist here, and shipping an untested syscall-level fast path
// with no way to verify its correctness (mmsghdr/iovec setup, partial
// receipt handling, etc.) would be worse than not having it -- "I designed
// for it but didn't ship code I couldn't verify" over false confidence.
// Revisit if this project ever runs in a Linux CI container.
class UdpReceiver {
public:
    explicit UdpReceiver(std::uint16_t port, std::size_t max_datagram_size = 2048);

    [[nodiscard]] bool is_open() const { return socket_.is_open(); }

    // The actual local port bound (meaningful when constructed with port 0
    // and letting the OS assign an ephemeral one -- see UdpSocket::bind).
    [[nodiscard]] std::optional<std::uint16_t> local_port() const { return socket_.local_port(); }

    // Drains up to max_batch pending datagrams without blocking once the
    // socket reports nothing pending. May return fewer than max_batch,
    // including zero.
    [[nodiscard]] std::vector<ReceivedDatagram> receive_batch(std::size_t max_batch);

private:
    UdpSocket socket_;
    std::size_t max_datagram_size_;
};

} // namespace mdh::net
