#include <gtest/gtest.h>

#include "exchange/persistence/command_decoder.hpp"
#include "exchange/persistence/command_encoder.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::persistence;

namespace {

CommandDecodeError decode_expect_error(std::span<const std::byte> bytes) {
    auto result = decode_journal_frame(bytes);
    if (!std::holds_alternative<CommandDecodeError>(result)) {
        ADD_FAILURE() << "expected a decode error but got a valid frame";
        return CommandDecodeError::TruncatedHeader;
    }
    return std::get<CommandDecodeError>(result);
}

std::vector<std::byte> valid_new_order_bytes() {
    std::vector<std::byte> bytes;
    encode_command(ExchangeCommand{NewOrderCommand{.command_sequence = 1,
                                                    .account_id = 1,
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

} // namespace

TEST(CommandDecodeErrors, EmptyBufferIsTruncatedHeader) {
    std::vector<std::byte> empty;
    EXPECT_EQ(decode_expect_error(empty), CommandDecodeError::TruncatedHeader);
}

TEST(CommandDecodeErrors, HeaderCutShortIsTruncatedHeader) {
    auto bytes = valid_new_order_bytes();
    bytes.resize(HEADER_SIZE - 1);
    EXPECT_EQ(decode_expect_error(bytes), CommandDecodeError::TruncatedHeader);
}

TEST(CommandDecodeErrors, PayloadCutShortIsTruncatedPayload) {
    auto bytes = valid_new_order_bytes();
    bytes.resize(bytes.size() - 1);
    EXPECT_EQ(decode_expect_error(bytes), CommandDecodeError::TruncatedPayload);
}

TEST(CommandDecodeErrors, UnknownMessageTypeIsRejected) {
    auto bytes = valid_new_order_bytes();
    bytes[0] = std::byte{200}; // no CommandMessageType enumerator has this value
    EXPECT_EQ(decode_expect_error(bytes), CommandDecodeError::InvalidMessageType);
}

TEST(CommandDecodeErrors, NonZeroReservedByteIsRejected) {
    auto bytes = valid_new_order_bytes();
    bytes[1] = std::byte{1}; // reserved byte must be 0
    EXPECT_EQ(decode_expect_error(bytes), CommandDecodeError::InvalidReserved);
}

TEST(CommandDecodeErrors, MismatchedPayloadSizeIsRejected) {
    auto bytes = valid_new_order_bytes();
    // Header stores payload_size at offset 2 (u16). Corrupt it so it no
    // longer matches NewOrderCommand's fixed payload size.
    bytes[2] = std::byte{0xFF};
    bytes[3] = std::byte{0xFF};
    EXPECT_EQ(decode_expect_error(bytes), CommandDecodeError::InvalidMessageSize);
}

TEST(CommandDecodeErrors, InvalidSideByteIsRejected) {
    auto bytes = valid_new_order_bytes();
    // NewOrderCommand payload layout: account_id(8) client_order_id(8)
    // instrument_id(4) side(1) price(8) quantity(8) order_type(1) tif(1).
    // side is the first byte after instrument_id -> offset HEADER_SIZE+8+8+4.
    bytes[HEADER_SIZE + 8 + 8 + 4] = std::byte{7}; // neither Buy(0) nor Sell(1)
    EXPECT_EQ(decode_expect_error(bytes), CommandDecodeError::InvalidSide);
}

TEST(CommandDecodeErrors, InvalidOrderTypeByteIsRejected) {
    auto bytes = valid_new_order_bytes();
    // order_type is the second-to-last payload byte.
    bytes[bytes.size() - 2] = std::byte{99}; // no OrderType enumerator has this value
    EXPECT_EQ(decode_expect_error(bytes), CommandDecodeError::InvalidOrderType);
}

TEST(CommandDecodeErrors, InvalidTimeInForceByteIsRejected) {
    auto bytes = valid_new_order_bytes();
    bytes.back() = std::byte{99}; // no TimeInForce enumerator has this value
    EXPECT_EQ(decode_expect_error(bytes), CommandDecodeError::InvalidTimeInForce);
}

TEST(CommandDecodeErrors, TrailingBytesBeyondPayloadAreIgnoredNotRejected) {
    auto bytes = valid_new_order_bytes();
    bytes.push_back(std::byte{0xAB});
    auto result = decode_journal_frame(bytes);
    ASSERT_TRUE(std::holds_alternative<ExchangeCommand>(result));
}

TEST(CommandDecodeErrors, RegisterInstrumentFrameDecodesToARegistrationNotACommand) {
    std::vector<std::byte> bytes;
    encode_register_instrument(4242, bytes);

    auto result = decode_journal_frame(bytes);
    ASSERT_TRUE(std::holds_alternative<RegisterInstrumentRecord>(result));
    EXPECT_EQ(std::get<RegisterInstrumentRecord>(result).instrument_id, 4242u);
}

TEST(CommandDecodeErrors, TruncatedRegisterInstrumentPayloadIsRejected) {
    std::vector<std::byte> bytes;
    encode_register_instrument(1, bytes);
    bytes.resize(bytes.size() - 1);
    EXPECT_EQ(decode_expect_error(bytes), CommandDecodeError::TruncatedPayload);
}
