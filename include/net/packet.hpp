#pragma once

#include <cstdint>
#include <span>
#include <string_view>
#include <variant>
#include <vector>

#include "protocol/errors.hpp"
#include "protocol/messages.hpp"

namespace mdh::net {

// Wraps one or more encoded event frames (see protocol/messages.hpp) into
// a single UDP datagram payload:
//
//   PacketHeader (20 bytes) || event frame 1 || event frame 2 || ...
//
// This is a distinct header from the 20-byte event-frame header -- packet
// framing is a transport-level batching concern (how many messages fit in
// one datagram), while the event-frame header/payload is an
// application-level message format. Keeping them separate means
// decode_event()/SequenceValidator/BookManager never need to know or care
// whether an event frame arrived via a file or was unpacked from a UDP
// packet.
//
// All multi-byte fields are big-endian, same as the rest of the wire
// format (see docs/protocol.md).
inline constexpr std::uint32_t PACKET_MAGIC = 0x4D444831; // ASCII "MDH1"
inline constexpr std::uint16_t PACKET_VERSION = 1;
inline constexpr std::size_t PACKET_HEADER_SIZE = 20;

// How much of a datagram is safe to fill: a typical 1500-byte Ethernet MTU
// less the 20-byte IPv4 and 8-byte UDP headers. Past this a datagram is
// fragmented, and since losing any one fragment loses the whole datagram,
// fragmentation converts a packet-loss rate into a worse one.
inline constexpr std::size_t MAX_DATAGRAM_PAYLOAD = 1472;

// How many event frames a producer may batch into one datagram.
//
// Computed from the worst-case frame size rather than the average one, so the
// bound holds for any mix of message types -- a batch of the largest message
// type must fit just as well as a batch of the smallest. Receivers here
// allocate 2048-byte buffers by default (net/udp_receiver.hpp), which this
// also stays under; a datagram larger than the receive buffer is truncated
// rather than fragmented, which is a decode failure rather than a slow path.
inline constexpr std::size_t MAX_FRAMES_PER_DATAGRAM =
    (MAX_DATAGRAM_PAYLOAD - PACKET_HEADER_SIZE) / protocol::MAX_FRAME_SIZE;

struct PacketHeader {
    std::uint32_t magic;
    std::uint16_t version;
    std::uint16_t frame_count;
    std::uint64_t packet_sequence; // transport-level sequence, distinct from any event's sequence_number
    std::uint32_t payload_length;  // bytes of packed event frames that follow the packet header
};

enum class PacketError {
    TruncatedHeader,        // fewer than PACKET_HEADER_SIZE bytes available
    InvalidMagic,
    InvalidVersion,
    ZeroFrameCount,          // frame_count == 0 is malformed, not just empty
    PayloadLengthMismatch,   // payload_length doesn't match the bytes actually present
    InnerFrameHeaderInvalid, // couldn't determine one contained frame's length/boundary
    FrameCountMismatch,      // walked a different number of frames than frame_count claimed
};

[[nodiscard]] constexpr std::string_view to_string(PacketError e) {
    switch (e) {
        case PacketError::TruncatedHeader:        return "TruncatedHeader";
        case PacketError::InvalidMagic:           return "InvalidMagic";
        case PacketError::InvalidVersion:         return "InvalidVersion";
        case PacketError::ZeroFrameCount:         return "ZeroFrameCount";
        case PacketError::PayloadLengthMismatch:  return "PayloadLengthMismatch";
        case PacketError::InnerFrameHeaderInvalid: return "InnerFrameHeaderInvalid";
        case PacketError::FrameCountMismatch:     return "FrameCountMismatch";
    }
    return "UnknownPacketError";
}

// Encodes each event in `events` (via protocol::encode_event) and packs
// them into one packet payload with a PacketHeader, ready to send as a
// single UDP datagram. Does not itself impose an MTU-sized limit -- the
// caller (udp_sender) decides how many events to batch per call.
[[nodiscard]] std::vector<std::byte> pack_frames(std::uint64_t packet_sequence, std::span<const protocol::Event> events);

struct UnpackedPacket {
    PacketHeader header;
    // One decode result per contained frame. A per-frame DecodeError here
    // does NOT invalidate the packet as a whole -- framing (finding where
    // each frame starts/ends) succeeded; only that one frame's content was
    // invalid. This mirrors the two-tier error model used for event
    // frames themselves (header/framing errors vs. per-message content
    // errors).
    std::vector<std::variant<protocol::Event, protocol::DecodeError>> frames;
};

// Splits a received datagram into its individual event frames and decodes
// each one. A PacketError return means the packet itself couldn't be
// trusted enough to even locate its contained frames (bad magic/version,
// truncated packet header, or a frame whose own header is unreadable, so
// its length -- and therefore where the next frame starts -- is unknown).
[[nodiscard]] std::variant<UnpackedPacket, PacketError> unpack_frames(std::span<const std::byte> datagram);

} // namespace mdh::net
