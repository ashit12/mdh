#include "net/udp_listener.hpp"

#include <stop_token>
#include <thread>
#include <variant>

#include "net/packet.hpp"
#include "net/udp_receiver.hpp"

namespace mdh::net {

namespace {

using FrameResult = std::variant<protocol::Event, protocol::DecodeError>;

} // namespace

UdpListenResult run_udp_listen(std::uint16_t port, const replay::ReplayOptions& options,
                                const UdpListenOptions& listen_options) {
    UdpListenResult result;

    UdpReceiver receiver(port);
    if (!receiver.is_open()) {
        result.outcome.stopped_early = true;
        result.outcome.stop_reason = "failed to bind UDP port " + std::to_string(port);
        return result;
    }

    DroppingQueue<FrameResult> queue(listen_options.queue_capacity);
    std::stop_source stop_source;
    PacketSequenceTracker packet_tracker;
    std::uint64_t packets_received = 0;
    std::uint64_t packet_errors = 0;

    const auto start = std::chrono::steady_clock::now();

    std::jthread producer([&] {
        const auto token = stop_source.get_token();
        bool have_received_any = false;
        auto last_activity = std::chrono::steady_clock::now();

        while (!token.stop_requested()) {
            auto batch = receiver.receive_batch(64);
            if (batch.empty()) {
                if (have_received_any && std::chrono::steady_clock::now() - last_activity > listen_options.idle_timeout) {
                    stop_source.request_stop(); // no traffic for a while; assume the sender finished
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                continue;
            }

            have_received_any = true;
            last_activity = std::chrono::steady_clock::now();

            for (const auto& dgram : batch) {
                if (token.stop_requested()) {
                    break; // the consumer asked us to stop; don't keep decoding/pushing into a drained queue
                }
                ++packets_received;
                auto unpacked = unpack_frames(dgram.bytes);
                if (std::holds_alternative<PacketError>(unpacked)) {
                    ++packet_errors;
                    continue;
                }

                const auto& packet = std::get<UnpackedPacket>(unpacked);
                packet_tracker.observe(packet.header.packet_sequence);

                for (const auto& frame_result : packet.frames) {
                    queue.push(frame_result); // drop-on-full: see DroppingQueue
                }
            }
        }
    });

    std::jthread consumer([&] {
        const auto token = stop_source.get_token();
        SequenceValidator validator;

        while (true) {
            auto item = queue.try_pop();
            if (!item) {
                if (token.stop_requested()) {
                    break; // stop was requested and the queue is now empty: nothing more is coming
                }
                std::this_thread::yield();
                continue;
            }
            if (listen_options.consumer_delay.count() > 0) {
                std::this_thread::sleep_for(listen_options.consumer_delay); // simulated slow consumer, see UdpListenOptions
            }
            if (replay::apply_frame_result(*item, validator, options, result.outcome)) {
                stop_source.request_stop(); // stop-worthy error: tell the producer too
                break;
            }
        }
    });

    producer.join();
    consumer.join();

    const auto end = std::chrono::steady_clock::now();
    result.outcome.stats.duration_ns =
        static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    result.packets_received = packets_received;
    result.packet_errors = packet_errors;
    result.packet_seq_stats = packet_tracker.stats();
    result.queue_dropped_count = queue.dropped_count();
    result.queue_high_water_mark = queue.high_water_mark();
    return result;
}

} // namespace mdh::net
