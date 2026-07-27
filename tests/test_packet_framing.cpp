#include <gtest/gtest.h>

#include "net/packet.hpp"

using namespace mdh;
using namespace mdh::protocol;
using namespace mdh::net;

namespace {

Event make_add(Sequence seq, OrderId id) {
    return Event{AddOrder{
        .sequence_number = seq,
        .timestamp_ns = 100 + seq,
        .order_id = id,
        .instrument_id = 1,
        .price = 1000,
        .quantity = 5,
        .side = Side::Buy,
    }};
}

} // namespace

TEST(PacketFraming, SingleFrameRoundTrip) {
    std::vector<Event> events = {make_add(1, 1)};
    auto datagram = pack_frames(42, events);

    auto result = unpack_frames(datagram);
    ASSERT_TRUE(std::holds_alternative<UnpackedPacket>(result));
    const auto& packet = std::get<UnpackedPacket>(result);

    EXPECT_EQ(packet.header.packet_sequence, 42u);
    EXPECT_EQ(packet.header.frame_count, 1u);
    ASSERT_EQ(packet.frames.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<Event>(packet.frames[0]));
    EXPECT_TRUE(std::holds_alternative<AddOrder>(std::get<Event>(packet.frames[0])));
}

TEST(PacketFraming, MultiFrameBatchedRoundTrip) {
    std::vector<Event> events = {
        make_add(1, 1),
        Event{CancelOrder{.sequence_number = 2, .timestamp_ns = 200, .order_id = 1, .instrument_id = 1}},
        Event{Trade{.sequence_number = 3, .timestamp_ns = 300, .instrument_id = 1, .price = 999, .quantity = 2, .aggressor_side = Side::Sell}},
    };
    auto datagram = pack_frames(7, events);

    auto result = unpack_frames(datagram);
    ASSERT_TRUE(std::holds_alternative<UnpackedPacket>(result));
    const auto& packet = std::get<UnpackedPacket>(result);

    EXPECT_EQ(packet.header.frame_count, 3u);
    ASSERT_EQ(packet.frames.size(), 3u);
    EXPECT_TRUE(std::holds_alternative<AddOrder>(std::get<Event>(packet.frames[0])));
    EXPECT_TRUE(std::holds_alternative<CancelOrder>(std::get<Event>(packet.frames[1])));
    EXPECT_TRUE(std::holds_alternative<Trade>(std::get<Event>(packet.frames[2])));
}

TEST(PacketFraming, OneBadFrameDoesNotInvalidateTheWholePacket) {
    std::vector<Event> events = {make_add(1, 1), make_add(2, 2)};
    auto datagram = pack_frames(1, events);

    // Corrupt the *second* frame's side byte (last byte of an AddOrder
    // payload) so it decodes to InvalidSide, while frame 1 and the packet
    // header remain untouched. Framing (finding frame boundaries) doesn't
    // depend on a frame's content being valid, only on its own header
    // being readable -- so this should still unpack successfully overall.
    datagram.back() = std::byte{7}; // neither Buy(0) nor Sell(1)

    auto result = unpack_frames(datagram);
    ASSERT_TRUE(std::holds_alternative<UnpackedPacket>(result));
    const auto& packet = std::get<UnpackedPacket>(result);

    ASSERT_EQ(packet.frames.size(), 2u);
    EXPECT_TRUE(std::holds_alternative<Event>(packet.frames[0]));
    ASSERT_TRUE(std::holds_alternative<DecodeError>(packet.frames[1]));
    EXPECT_EQ(std::get<DecodeError>(packet.frames[1]), DecodeError::InvalidSide);
}

TEST(PacketFraming, TruncatedPacketHeaderIsRejected) {
    std::vector<Event> events = {make_add(1, 1)};
    auto datagram = pack_frames(1, events);
    datagram.resize(PACKET_HEADER_SIZE - 1);

    auto result = unpack_frames(datagram);
    ASSERT_TRUE(std::holds_alternative<PacketError>(result));
    EXPECT_EQ(std::get<PacketError>(result), PacketError::TruncatedHeader);
}

TEST(PacketFraming, WrongMagicIsRejected) {
    std::vector<Event> events = {make_add(1, 1)};
    auto datagram = pack_frames(1, events);
    datagram[0] = std::byte{0xFF}; // corrupt the first byte of the magic

    auto result = unpack_frames(datagram);
    ASSERT_TRUE(std::holds_alternative<PacketError>(result));
    EXPECT_EQ(std::get<PacketError>(result), PacketError::InvalidMagic);
}

TEST(PacketFraming, WrongVersionIsRejected) {
    std::vector<Event> events = {make_add(1, 1)};
    auto datagram = pack_frames(1, events);
    datagram[5] = std::byte{99}; // version is bytes [4,6), low byte at offset 5

    auto result = unpack_frames(datagram);
    ASSERT_TRUE(std::holds_alternative<PacketError>(result));
    EXPECT_EQ(std::get<PacketError>(result), PacketError::InvalidVersion);
}

TEST(PacketFraming, PayloadLengthMismatchIsRejected) {
    std::vector<Event> events = {make_add(1, 1)};
    auto datagram = pack_frames(1, events);
    datagram.push_back(std::byte{0xAB}); // trailing byte not accounted for by payload_length

    auto result = unpack_frames(datagram);
    ASSERT_TRUE(std::holds_alternative<PacketError>(result));
    EXPECT_EQ(std::get<PacketError>(result), PacketError::PayloadLengthMismatch);
}

TEST(PacketFraming, TruncatedFinalFrameIsRejected) {
    std::vector<Event> events = {make_add(1, 1), make_add(2, 2)};
    auto datagram = pack_frames(1, events);
    datagram.pop_back(); // chop the last byte off the second frame

    // payload_length in the header still claims the original (now larger
    // than actual) size, so this is caught as a payload length mismatch --
    // consistent with how the event-frame decoder treats a claimed size
    // that doesn't match what's actually present.
    auto result = unpack_frames(datagram);
    ASSERT_TRUE(std::holds_alternative<PacketError>(result));
    EXPECT_EQ(std::get<PacketError>(result), PacketError::PayloadLengthMismatch);
}
