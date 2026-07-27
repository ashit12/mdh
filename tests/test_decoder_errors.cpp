#include <gtest/gtest.h>

#include "common/byte_io.hpp"
#include "protocol/decoder.hpp"
#include "protocol/encoder.hpp"

using namespace mdh;
using namespace mdh::protocol;

namespace {

DecodeError decode_expect_error(std::span<const std::byte> bytes) {
    auto result = decode_event(bytes);
    if (std::holds_alternative<Event>(result)) {
        ADD_FAILURE() << "expected a decode error but got a valid event";
        return DecodeError::TruncatedHeader;
    }
    return std::get<DecodeError>(result);
}

std::vector<std::byte> valid_add_order_bytes() {
    std::vector<std::byte> bytes;
    encode_event(Event{AddOrder{.sequence_number = 1, .timestamp_ns = 1, .order_id = 1, .instrument_id = 1, .price = 100, .quantity = 5, .side = Side::Buy}}, bytes);
    return bytes;
}

} // namespace

TEST(DecoderErrors, EmptyBufferIsTruncatedHeader) {
    std::vector<std::byte> empty;
    EXPECT_EQ(decode_expect_error(empty), DecodeError::TruncatedHeader);
}

TEST(DecoderErrors, HeaderCutShortIsTruncatedHeader) {
    auto bytes = valid_add_order_bytes();
    bytes.resize(HEADER_SIZE - 1);
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::TruncatedHeader);
}

TEST(DecoderErrors, PayloadCutShortIsTruncatedPayload) {
    auto bytes = valid_add_order_bytes();
    bytes.resize(bytes.size() - 1);
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::TruncatedPayload);
}

TEST(DecoderErrors, UnknownMessageTypeIsRejected) {
    auto bytes = valid_add_order_bytes();
    bytes[0] = std::byte{200}; // no MessageType enumerator has this value
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::InvalidMessageType);
}

TEST(DecoderErrors, NonZeroReservedByteIsRejected) {
    auto bytes = valid_add_order_bytes();
    bytes[1] = std::byte{1}; // reserved byte must be 0
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::InvalidReserved);
}

TEST(DecoderErrors, MismatchedPayloadSizeIsRejected) {
    auto bytes = valid_add_order_bytes();
    // Header stores payload_size at offset 2 (u16 LE). Corrupt it so it no
    // longer matches AddOrder's fixed payload size.
    bytes[2] = std::byte{0xFF};
    bytes[3] = std::byte{0xFF};
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::InvalidMessageSize);
}

TEST(DecoderErrors, InvalidSideByteIsRejected) {
    auto bytes = valid_add_order_bytes();
    // AddOrder payload layout: order_id(8) instrument_id(4) price(8) quantity(8) side(1)
    // Side byte is the last byte of the frame.
    bytes.back() = std::byte{7}; // neither Buy(0) nor Sell(1)
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::InvalidSide);
}

TEST(DecoderErrors, TrailingBytesBeyondPayloadAreIgnoredNotRejected) {
    // A file reader hands decode_event() exactly HEADER_SIZE + payload_size
    // bytes in practice, but decode_event should not choke if given extra
    // trailing bytes -- it only needs to read what payload_size specifies.
    auto bytes = valid_add_order_bytes();
    bytes.push_back(std::byte{0xAB});
    auto result = decode_event(bytes);
    ASSERT_TRUE(std::holds_alternative<Event>(result));
}
