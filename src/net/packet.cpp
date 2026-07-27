#include "net/packet.hpp"

#include "common/byte_io.hpp"
#include "protocol/decoder.hpp"
#include "protocol/encoder.hpp"

namespace mdh::net {

std::vector<std::byte> pack_frames(std::uint64_t packet_sequence, std::span<const protocol::Event> events) {
    std::vector<std::byte> payload;
    for (const auto& event : events) {
        protocol::encode_event(event, payload);
    }

    std::vector<std::byte> out;
    out.reserve(PACKET_HEADER_SIZE + payload.size());
    io::put_u32(out, PACKET_MAGIC);
    io::put_u16(out, PACKET_VERSION);
    io::put_u16(out, static_cast<std::uint16_t>(events.size()));
    io::put_u64(out, packet_sequence);
    io::put_u32(out, static_cast<std::uint32_t>(payload.size()));
    out.insert(out.end(), payload.begin(), payload.end());
    return out;
}

std::variant<UnpackedPacket, PacketError> unpack_frames(std::span<const std::byte> datagram) {
    if (datagram.size() < PACKET_HEADER_SIZE) {
        return PacketError::TruncatedHeader;
    }

    io::ByteReader r(datagram.first(PACKET_HEADER_SIZE));
    auto magic = r.get_u32();
    auto version = r.get_u16();
    auto frame_count = r.get_u16();
    auto packet_sequence = r.get_u64();
    auto payload_length = r.get_u32();

    if (!magic || !version || !frame_count || !packet_sequence || !payload_length) {
        return PacketError::TruncatedHeader;
    }
    if (*magic != PACKET_MAGIC) {
        return PacketError::InvalidMagic;
    }
    if (*version != PACKET_VERSION) {
        return PacketError::InvalidVersion;
    }
    if (*frame_count == 0) {
        return PacketError::ZeroFrameCount;
    }

    auto remaining = datagram.subspan(PACKET_HEADER_SIZE);
    if (remaining.size() != *payload_length) {
        return PacketError::PayloadLengthMismatch;
    }

    UnpackedPacket result;
    result.header = PacketHeader{
        .magic = *magic,
        .version = *version,
        .frame_count = *frame_count,
        .packet_sequence = *packet_sequence,
        .payload_length = *payload_length,
    };

    std::size_t offset = 0;
    for (std::uint16_t i = 0; i < *frame_count; ++i) {
        auto rest = remaining.subspan(offset);
        auto header_result = protocol::decode_header(rest);
        if (std::holds_alternative<protocol::DecodeError>(header_result)) {
            return PacketError::InnerFrameHeaderInvalid;
        }
        const auto& header = std::get<protocol::Header>(header_result);
        const std::size_t frame_len = protocol::HEADER_SIZE + header.payload_size;
        if (rest.size() < frame_len) {
            return PacketError::InnerFrameHeaderInvalid;
        }

        auto frame_bytes = rest.first(frame_len);
        auto event_result = protocol::decode_event(frame_bytes);
        if (std::holds_alternative<protocol::Event>(event_result)) {
            result.frames.emplace_back(std::get<protocol::Event>(event_result));
        } else {
            result.frames.emplace_back(std::get<protocol::DecodeError>(event_result));
        }
        offset += frame_len;
    }

    if (offset != remaining.size()) {
        return PacketError::FrameCountMismatch;
    }

    return result;
}

} // namespace mdh::net
