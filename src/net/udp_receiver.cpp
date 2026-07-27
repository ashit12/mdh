#include "net/udp_receiver.hpp"

#include <chrono>

namespace mdh::net {

namespace {

std::uint64_t now_ns() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}

} // namespace

UdpReceiver::UdpReceiver(std::uint16_t port, std::size_t max_datagram_size) : max_datagram_size_(max_datagram_size) {
    if (socket_.bind(port)) {
        socket_.set_non_blocking();
    }
}

std::vector<ReceivedDatagram> UdpReceiver::receive_batch(std::size_t max_batch) {
    std::vector<ReceivedDatagram> out;
    out.reserve(max_batch);

    std::vector<std::byte> buf(max_datagram_size_);
    for (std::size_t i = 0; i < max_batch; ++i) {
        auto received = socket_.receive(buf);
        if (!received) {
            break; // nothing more pending right now (non-blocking socket)
        }

        ReceivedDatagram dgram;
        dgram.bytes.assign(buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(*received));
        dgram.receive_timestamp_ns = now_ns();
        out.push_back(std::move(dgram));
    }
    return out;
}

} // namespace mdh::net
