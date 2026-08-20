#pragma once

#include <chrono>
#include <cstdint>

#include "common/dropping_queue.hpp"
#include "net/packet_sequence_tracker.hpp"
#include "replay/replay_engine.hpp"

namespace mdh::net {

struct UdpListenOptions {
    std::chrono::milliseconds idle_timeout{1000};

    // Bounds how far the consumer (book reconstruction) can fall behind
    // the producer (UDP receive + decode) before the producer starts
    // dropping newly decoded frames -- see DroppingQueue. A dropped frame
    // shows up downstream as a sequence gap, exactly like a dropped UDP
    // packet would.
    std::size_t queue_capacity = 1024;

    // Artificial delay applied by the consumer after processing each
    // popped item -- a deterministic way to simulate a slow consumer
    // (e.g. a heavier book-reconstruction workload) without depending on
    // incidental machine/OS-scheduling timing to ever exercise backpressure.
    // Zero (the default) means no artificial delay at all.
    std::chrono::microseconds consumer_delay{0};
};

struct UdpListenResult {
    replay::ReplayOutcome outcome;
    std::uint64_t packets_received = 0;
    std::uint64_t packet_errors = 0;
    PacketSequenceStats packet_seq_stats;
    std::size_t queue_dropped_count = 0;
    std::size_t queue_high_water_mark = 0;
};

// Listens on `port` using two threads connected by a DroppingQueue:
//
//   producer: UdpReceiver::receive_batch() -> unpack_frames() -> push
//   consumer: pop -> replay::apply_frame_result() (validate + apply to book)
//
// Both still funnel through apply_frame_result(), so decode-error/
// sequence-validation/book-application behavior is identical to file
// replay and to a single-threaded listener -- only "how frames
// get from the socket to apply_frame_result()" changed. Packet-level
// bookkeeping (PacketSequenceTracker, packet_errors) stays entirely on the
// producer thread, since it's tied to receiving a datagram, not to
// anything the consumer does.
//
// Shutdown: a single std::stop_source shared by both threads (not each
// jthread's own per-object token -- that's per-object, and this needs one
// signal both sides observe). The producer requests stop on idle timeout;
// the consumer requests stop on a stop-worthy error from
// apply_frame_result(). Either way, the consumer always drains whatever is
// still sitting in the queue before it exits -- std::stop_source's
// request_stop()/stop_requested() pair has its own acquire/release
// synchronization, so once the consumer observes the stop signal it's
// guaranteed to see every push the producer made beforehand.
[[nodiscard]] UdpListenResult run_udp_listen(std::uint16_t port, const replay::ReplayOptions& options,
                                              const UdpListenOptions& listen_options = {});

} // namespace mdh::net
