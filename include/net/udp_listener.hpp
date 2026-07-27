#pragma once

#include <chrono>
#include <cstdint>

#include "net/packet_sequence_tracker.hpp"
#include "replay/replay_engine.hpp"

namespace mdh::net {

struct UdpListenResult {
    replay::ReplayOutcome outcome;
    std::uint64_t packets_received = 0;
    std::uint64_t packet_errors = 0;
    PacketSequenceStats packet_seq_stats;
};

// Listens on `port`, decoding and applying UDP-delivered events the same
// way run_replay() applies file-delivered ones -- both funnel through
// replay::apply_frame_result(), so decode-error/sequence-validation/book-
// application behavior is identical regardless of transport. Only "where
// do frames come from" differs (a UDP receive loop here vs. a file read
// loop in run_replay()).
//
// Packet-level framing failures (couldn't even locate the frames inside a
// datagram -- see net/packet.hpp's PacketError) are counted and that
// datagram is skipped; they do not stop the listener. A stop-worthy
// per-event error (per `options`) does stop it, exactly as in file replay.
//
// Lives in net/ (depending on replay/) rather than replay/ (depending on
// net/): replay_engine.hpp itself has zero knowledge of UDP or sockets;
// this file is the one place that bridges the two, so that dependency
// only exists here, not in the core replay module.
//
// Termination: stops once no packet has arrived for `idle_timeout` after
// having received at least one -- there is no signal-handling/graceful-
// shutdown mechanism (no SIGINT handler) for a true "run until told to
// stop" server in this milestone.
[[nodiscard]] UdpListenResult run_udp_listen(std::uint16_t port, const replay::ReplayOptions& options,
                                              std::chrono::milliseconds idle_timeout);

} // namespace mdh::net
