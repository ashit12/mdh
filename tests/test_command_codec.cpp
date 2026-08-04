#include <gtest/gtest.h>

#include "exchange/persistence/command_decoder.hpp"
#include "exchange/persistence/command_encoder.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::persistence;

namespace {

ExchangeCommand decode_or_fail(std::span<const std::byte> bytes) {
    auto result = decode_command(bytes);
    if (std::holds_alternative<CommandDecodeError>(result)) {
        ADD_FAILURE() << "decode failed: " << to_string(std::get<CommandDecodeError>(result));
        return NewOrderCommand{};
    }
    return std::get<ExchangeCommand>(result);
}

} // namespace

TEST(CommandCodec, WireBytesAreBigEndian) {
    // Pins actual byte values against the documented big-endian layout, the
    // same way ProtocolRoundtrip.WireBytesAreBigEndian does for market-data
    // messages -- a round-trip alone can't catch an accidental
    // little-endian regression since encode/decode would still agree with
    // each other.
    CancelOrderCommand cmd{
        .command_sequence = 0x0102030405060708ULL, .account_id = 0, .client_order_id = 0, .instrument_id = 0xAABBCCDD};

    std::vector<std::byte> bytes;
    encode_command(ExchangeCommand{cmd}, bytes);

    // command_sequence lives at header offset 4, 8 bytes, big-endian.
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[4]), 0x01);
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[5]), 0x02);
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[11]), 0x08);

    // CancelOrderCommand payload: account_id(8) client_order_id(8) instrument_id(4)
    // -> instrument_id starts at offset HEADER_SIZE(12) + 8 + 8 = 28.
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[28]), 0xAA);
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[29]), 0xBB);
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[30]), 0xCC);
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[31]), 0xDD);
}

TEST(CommandCodec, NewOrderCommandRoundTrips) {
    NewOrderCommand original{
        .command_sequence = 42,
        .account_id = 100,
        .client_order_id = 7,
        .instrument_id = 1,
        .side = Side::Buy,
        .price = 10000,
        .quantity = 5,
        .order_type = OrderType::Limit,
        .time_in_force = TimeInForce::IOC,
    };

    std::vector<std::byte> bytes;
    encode_command(ExchangeCommand{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(CommandMessageType::NewOrder));

    ExchangeCommand decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<NewOrderCommand>(decoded));
    EXPECT_EQ(std::get<NewOrderCommand>(decoded), original);
}

TEST(CommandCodec, NewOrderCommandRoundTripsEveryTimeInForce) {
    for (const auto tif : {TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK}) {
        NewOrderCommand original{.command_sequence = 1,
                                  .account_id = 1,
                                  .client_order_id = 1,
                                  .instrument_id = 1,
                                  .side = Side::Sell,
                                  .price = 1,
                                  .quantity = 1,
                                  .order_type = OrderType::Limit,
                                  .time_in_force = tif};
        std::vector<std::byte> bytes;
        encode_command(ExchangeCommand{original}, bytes);
        ExchangeCommand decoded = decode_or_fail(bytes);
        ASSERT_TRUE(std::holds_alternative<NewOrderCommand>(decoded));
        EXPECT_EQ(std::get<NewOrderCommand>(decoded).time_in_force, tif);
    }
}

TEST(CommandCodec, NewOrderCommandNegativePrice) {
    // Encoder/decoder must round-trip the full int64_t range; validating
    // that a negative price is nonsensical is the matching engine's job.
    NewOrderCommand original{.command_sequence = 1,
                              .account_id = 1,
                              .client_order_id = 1,
                              .instrument_id = 1,
                              .side = Side::Sell,
                              .price = -500,
                              .quantity = 1,
                              .order_type = OrderType::Limit,
                              .time_in_force = TimeInForce::GTC};
    std::vector<std::byte> bytes;
    encode_command(ExchangeCommand{original}, bytes);
    ExchangeCommand decoded = decode_or_fail(bytes);
    EXPECT_EQ(std::get<NewOrderCommand>(decoded).price, -500);
}

TEST(CommandCodec, CancelOrderCommandRoundTrips) {
    CancelOrderCommand original{.command_sequence = 5, .account_id = 100, .client_order_id = 42, .instrument_id = 7};
    std::vector<std::byte> bytes;
    encode_command(ExchangeCommand{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(CommandMessageType::CancelOrder));

    ExchangeCommand decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<CancelOrderCommand>(decoded));
    EXPECT_EQ(std::get<CancelOrderCommand>(decoded), original);
}

TEST(CommandCodec, ReplaceOrderCommandRoundTrips) {
    ReplaceOrderCommand original{
        .command_sequence = 6,
        .account_id = 100,
        .original_client_order_id = 42,
        .new_client_order_id = 43,
        .instrument_id = 7,
        .new_price = 10500,
        .new_quantity = 3,
    };
    std::vector<std::byte> bytes;
    encode_command(ExchangeCommand{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(CommandMessageType::ReplaceOrder));

    ExchangeCommand decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<ReplaceOrderCommand>(decoded));
    EXPECT_EQ(std::get<ReplaceOrderCommand>(decoded), original);
}

TEST(CommandCodec, MultipleCommandsConcatenateCleanly) {
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
    std::size_t first_len = bytes.size();
    encode_command(
        ExchangeCommand{CancelOrderCommand{.command_sequence = 2, .account_id = 1, .client_order_id = 1, .instrument_id = 1}},
        bytes);

    ExchangeCommand first = decode_or_fail(std::span(bytes).first(first_len));
    ASSERT_TRUE(std::holds_alternative<NewOrderCommand>(first));

    ExchangeCommand second = decode_or_fail(std::span(bytes).subspan(first_len));
    ASSERT_TRUE(std::holds_alternative<CancelOrderCommand>(second));
}
