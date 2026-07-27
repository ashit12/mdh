#include <gtest/gtest.h>

#include "protocol/decoder.hpp"
#include "protocol/encoder.hpp"

using namespace mdh;
using namespace mdh::protocol;

namespace {

Event decode_or_fail(std::span<const std::byte> bytes) {
    auto result = decode_event(bytes);
    if (std::holds_alternative<DecodeError>(result)) {
        ADD_FAILURE() << "decode failed: " << to_string(std::get<DecodeError>(result));
        return AddOrder{};
    }
    return std::get<Event>(result);
}

} // namespace

TEST(ProtocolRoundtrip, WireBytesAreBigEndian) {
    // A round-trip test alone can't catch an accidental regression back to
    // little-endian: encode+decode are symmetric, so they'd still agree
    // with each other even if both flipped. This test pins the actual byte
    // values on the wire, independent of decode_event(), against the
    // documented big-endian (network byte order) layout.
    ClearBook msg{.sequence_number = 0x0102030405060708ULL, .timestamp_ns = 0, .instrument_id = 0xAABBCCDD};

    std::vector<std::byte> bytes;
    encode_event(Event{msg}, bytes);

    // sequence_number lives at header offset 4, 8 bytes, big-endian ->
    // most-significant byte (0x01) first.
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[4]), 0x01);
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[5]), 0x02);
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[11]), 0x08);

    // ClearBook's instrument_id is the first (and only) payload field,
    // right after the 20-byte header -> offset 20, 4 bytes, big-endian.
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[20]), 0xAA);
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[21]), 0xBB);
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[22]), 0xCC);
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[23]), 0xDD);
}

TEST(ProtocolRoundtrip, AddOrder) {
    AddOrder original{
        .sequence_number = 42,
        .timestamp_ns = 123456789,
        .order_id = 7,
        .instrument_id = 100,
        .price = 250000,
        .quantity = 10,
        .side = Side::Buy,
    };

    std::vector<std::byte> bytes;
    encode_event(Event{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(MessageType::AddOrder));

    Event decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<AddOrder>(decoded));
    const auto& d = std::get<AddOrder>(decoded);
    EXPECT_EQ(d.sequence_number, original.sequence_number);
    EXPECT_EQ(d.timestamp_ns, original.timestamp_ns);
    EXPECT_EQ(d.order_id, original.order_id);
    EXPECT_EQ(d.instrument_id, original.instrument_id);
    EXPECT_EQ(d.price, original.price);
    EXPECT_EQ(d.quantity, original.quantity);
    EXPECT_EQ(d.side, original.side);
}

TEST(ProtocolRoundtrip, AddOrderNegativePrice) {
    // Encoder/decoder must round-trip the full int64_t range; validating
    // that a negative price is *nonsensical* is the book layer's job.
    AddOrder original{
        .sequence_number = 1,
        .timestamp_ns = 1,
        .order_id = 1,
        .instrument_id = 1,
        .price = -12345,
        .quantity = 1,
        .side = Side::Sell,
    };
    std::vector<std::byte> bytes;
    encode_event(Event{original}, bytes);
    Event decoded = decode_or_fail(bytes);
    EXPECT_EQ(std::get<AddOrder>(decoded).price, -12345);
}

TEST(ProtocolRoundtrip, CancelOrder) {
    CancelOrder original{.sequence_number = 5, .timestamp_ns = 99, .order_id = 3, .instrument_id = 1};
    std::vector<std::byte> bytes;
    encode_event(Event{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(MessageType::CancelOrder));

    Event decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<CancelOrder>(decoded));
    const auto& d = std::get<CancelOrder>(decoded);
    EXPECT_EQ(d.order_id, original.order_id);
    EXPECT_EQ(d.instrument_id, original.instrument_id);
    EXPECT_EQ(d.sequence_number, original.sequence_number);
    EXPECT_EQ(d.timestamp_ns, original.timestamp_ns);
}

TEST(ProtocolRoundtrip, ModifyOrder) {
    ModifyOrder original{
        .sequence_number = 6,
        .timestamp_ns = 100,
        .order_id = 3,
        .instrument_id = 1,
        .new_price = 5000,
        .new_quantity = 20,
    };
    std::vector<std::byte> bytes;
    encode_event(Event{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(MessageType::ModifyOrder));

    Event decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<ModifyOrder>(decoded));
    const auto& d = std::get<ModifyOrder>(decoded);
    EXPECT_EQ(d.new_price, original.new_price);
    EXPECT_EQ(d.new_quantity, original.new_quantity);
}

TEST(ProtocolRoundtrip, Trade) {
    Trade original{
        .sequence_number = 7,
        .timestamp_ns = 200,
        .instrument_id = 2,
        .price = 999,
        .quantity = 4,
        .aggressor_side = Side::Sell,
    };
    std::vector<std::byte> bytes;
    encode_event(Event{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(MessageType::Trade));

    Event decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<Trade>(decoded));
    const auto& d = std::get<Trade>(decoded);
    EXPECT_EQ(d.price, original.price);
    EXPECT_EQ(d.quantity, original.quantity);
    EXPECT_EQ(d.aggressor_side, original.aggressor_side);
}

TEST(ProtocolRoundtrip, ClearBook) {
    ClearBook original{.sequence_number = 8, .timestamp_ns = 300, .instrument_id = 9};
    std::vector<std::byte> bytes;
    encode_event(Event{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(MessageType::ClearBook));

    Event decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<ClearBook>(decoded));
    EXPECT_EQ(std::get<ClearBook>(decoded).instrument_id, original.instrument_id);
}

TEST(ProtocolRoundtrip, MultipleEventsConcatenateCleanly) {
    std::vector<std::byte> bytes;
    encode_event(Event{AddOrder{.sequence_number = 1, .timestamp_ns = 1, .order_id = 1, .instrument_id = 1, .price = 100, .quantity = 5, .side = Side::Buy}}, bytes);
    std::size_t first_len = bytes.size();
    encode_event(Event{CancelOrder{.sequence_number = 2, .timestamp_ns = 2, .order_id = 1, .instrument_id = 1}}, bytes);

    Event first = decode_or_fail(std::span(bytes).first(first_len));
    ASSERT_TRUE(std::holds_alternative<AddOrder>(first));

    Event second = decode_or_fail(std::span(bytes).subspan(first_len));
    ASSERT_TRUE(std::holds_alternative<CancelOrder>(second));
}
