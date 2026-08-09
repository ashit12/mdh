#include <gtest/gtest.h>

#include "protocol/order_entry/decoder.hpp"
#include "protocol/order_entry/encoder.hpp"

using namespace mdh;
using namespace mdh::protocol::order_entry;
using mdh::exchange::OrderType;
using mdh::exchange::RejectReason;
using mdh::exchange::TimeInForce;

namespace {

DecodeError decode_expect_error(std::span<const std::byte> bytes) {
    auto result = decode_message(bytes);
    if (std::holds_alternative<Message>(result)) {
        ADD_FAILURE() << "expected a decode error but got a valid message";
        return DecodeError::TruncatedHeader;
    }
    return std::get<DecodeError>(result);
}

// NewOrder payload layout (offsets relative to the start of the frame,
// HEADER_SIZE == 3): account_id(8)@3 client_order_id(8)@11 instrument_id(4)@19
// side(1)@23 price(8)@24 quantity(8)@32 order_type(1)@40 time_in_force(1)@41.
// Total frame size 42.
std::vector<std::byte> valid_new_order_bytes() {
    std::vector<std::byte> bytes;
    encode_message(Message{NewOrder{.account_id = 1,
                                     .client_order_id = 1,
                                     .instrument_id = 1,
                                     .side = Side::Buy,
                                     .price = 100,
                                     .quantity = 5,
                                     .order_type = OrderType::Limit,
                                     .time_in_force = TimeInForce::GTC}},
                   bytes);
    return bytes;
}

// Rejected payload layout: account_id(8)@3 client_order_id(8)@11
// instrument_id(4)@19 reason(1)@23. Total frame size 24.
std::vector<std::byte> valid_rejected_bytes() {
    std::vector<std::byte> bytes;
    encode_message(
        Message{Rejected{.account_id = 1, .client_order_id = 1, .instrument_id = 1, .reason = RejectReason::None}},
        bytes);
    return bytes;
}

} // namespace

TEST(OrderEntryDecoderErrors, EmptyBufferIsTruncatedHeader) {
    std::vector<std::byte> empty;
    EXPECT_EQ(decode_expect_error(empty), DecodeError::TruncatedHeader);
}

TEST(OrderEntryDecoderErrors, HeaderCutShortIsTruncatedHeader) {
    auto bytes = valid_new_order_bytes();
    bytes.resize(HEADER_SIZE - 1);
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::TruncatedHeader);
}

TEST(OrderEntryDecoderErrors, PayloadCutShortIsTruncatedPayload) {
    auto bytes = valid_new_order_bytes();
    bytes.resize(bytes.size() - 1);
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::TruncatedPayload);
}

TEST(OrderEntryDecoderErrors, UnknownMessageTypeIsRejected) {
    auto bytes = valid_new_order_bytes();
    bytes[0] = std::byte{200}; // no MessageType enumerator has this value
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::InvalidMessageType);
}

TEST(OrderEntryDecoderErrors, MismatchedPayloadSizeIsRejected) {
    auto bytes = valid_new_order_bytes();
    // Header stores payload_size at offset 1 (u16 big-endian). Corrupt it
    // so it no longer matches NewOrder's fixed payload size.
    bytes[1] = std::byte{0xFF};
    bytes[2] = std::byte{0xFF};
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::InvalidMessageSize);
}

TEST(OrderEntryDecoderErrors, InvalidSideByteIsRejected) {
    auto bytes = valid_new_order_bytes();
    bytes[23] = std::byte{7}; // neither Buy(0) nor Sell(1)
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::InvalidSide);
}

TEST(OrderEntryDecoderErrors, InvalidOrderTypeByteIsRejected) {
    auto bytes = valid_new_order_bytes();
    bytes[40] = std::byte{99}; // OrderType only defines Limit(0)
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::InvalidOrderType);
}

TEST(OrderEntryDecoderErrors, InvalidTimeInForceByteIsRejected) {
    auto bytes = valid_new_order_bytes();
    bytes.back() = std::byte{99}; // time_in_force is the frame's last byte
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::InvalidTimeInForce);
}

TEST(OrderEntryDecoderErrors, InvalidRejectReasonByteIsRejected) {
    auto bytes = valid_rejected_bytes();
    bytes.back() = std::byte{200}; // reason is the frame's last byte
    EXPECT_EQ(decode_expect_error(bytes), DecodeError::InvalidRejectReason);
}

TEST(OrderEntryDecoderErrors, TrailingBytesBeyondPayloadAreIgnoredNotRejected) {
    // A connection reader hands decode_message() exactly HEADER_SIZE +
    // payload_size bytes for one message in practice, but it should not
    // choke if given extra trailing bytes (e.g. the start of the next
    // message already buffered) -- it only needs to read what
    // payload_size specifies.
    auto bytes = valid_new_order_bytes();
    bytes.push_back(std::byte{0xAB});
    auto result = decode_message(bytes);
    ASSERT_TRUE(std::holds_alternative<Message>(result));
}
