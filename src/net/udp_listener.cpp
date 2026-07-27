#include "net/udp_listener.hpp"

#include <thread>
#include <variant>

#include "net/packet.hpp"
#include "net/udp_receiver.hpp"

namespace mdh::net {

UdpListenResult run_udp_listen(std::uint16_t port, const replay::ReplayOptions& options,
                                std::chrono::milliseconds idle_timeout) {
    UdpListenResult result;

    UdpReceiver receiver(port);
    if (!receiver.is_open()) {
        result.outcome.stopped_early = true;
        result.outcome.stop_reason = "failed to bind UDP port " + std::to_string(port);
        return result;
    }

    SequenceValidator validator;
    PacketSequenceTracker packet_tracker;
    bool have_received_any = false;
    bool should_stop = false;
    auto last_activity = std::chrono::steady_clock::now();
    const auto start = std::chrono::steady_clock::now();

    while (!should_stop) {
        auto batch = receiver.receive_batch(64);
        if (batch.empty()) {
            if (have_received_any && std::chrono::steady_clock::now() - last_activity > idle_timeout) {
                break; // no traffic for a while; assume the sender finished
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }

        have_received_any = true;
        last_activity = std::chrono::steady_clock::now();

        for (const auto& dgram : batch) {
            ++result.packets_received;
            auto unpacked = unpack_frames(dgram.bytes);
            if (std::holds_alternative<PacketError>(unpacked)) {
                ++result.packet_errors;
                continue;
            }

            const auto& packet = std::get<UnpackedPacket>(unpacked);
            packet_tracker.observe(packet.header.packet_sequence);

            for (const auto& frame_result : packet.frames) {
                if (replay::apply_frame_result(frame_result, validator, options, result.outcome)) {
                    should_stop = true;
                    break;
                }
            }
            if (should_stop) {
                break;
            }
        }
    }

    const auto end = std::chrono::steady_clock::now();
    result.outcome.stats.duration_ns =
        static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    result.packet_seq_stats = packet_tracker.stats();
    return result;
}

} // namespace mdh::net
