#include <gtest/gtest.h>

#include "protocol/order_entry/decoder.hpp"
#include "protocol/order_entry/encoder.hpp"

using namespace mdh;
using namespace mdh::protocol::order_entry;
using mdh::exchange::OrderType;
using mdh::exchange::RejectReason;
using mdh::exchange::TimeInForce;

namespace {

Message decode_or_fail(std::span<const std::byte> bytes) {
    auto result = decode_message(bytes);
    if (std::holds_alternative<DecodeError>(result)) {
        ADD_FAILURE() << "decode failed: " << to_string(std::get<DecodeError>(result));
        return NewOrder{};
    }
    return std::get<Message>(result);
}

} // namespace

TEST(OrderEntryCodec, WireBytesAreBigEndian) {
    // Same rationale as ProtocolRoundtrip.WireBytesAreBigEndian: a
    // round-trip alone can't catch an accidental switch to little-endian,
    // since encode+decode would still agree with each other. This pins the
    // actual bytes against the documented big-endian layout.
    NewOrder msg{
        .account_id = 0x0102030405060708ULL,
        .client_order_id = 1,
        .instrument_id = 1,
        .side = Side::Buy,
        .price = 1,
        .quantity = 1,
        .order_type = OrderType::Limit,
        .time_in_force = TimeInForce::GTC,
    };

    std::vector<std::byte> bytes;
    encode_message(Message{msg}, bytes);

    // Header: type (1 byte) then payload_size (u16 big-endian). NewOrder's
    // payload_size is 39 (0x0027).
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[0]), static_cast<std::uint8_t>(MessageType::NewOrder));
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[1]), 0x00);
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[2]), 0x27);

    // account_id is the first payload field, right after the 3-byte header
    // -> offset 3, 8 bytes, big-endian (most-significant byte first).
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[3]), 0x01);
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[4]), 0x02);
    EXPECT_EQ(std::to_integer<std::uint8_t>(bytes[10]), 0x08);
}

TEST(OrderEntryCodec, NewOrder) {
    NewOrder original{
        .account_id = 100,
        .client_order_id = 7,
        .instrument_id = 1,
        .side = Side::Buy,
        .price = 250000,
        .quantity = 10,
        .order_type = OrderType::Limit,
        .time_in_force = TimeInForce::GTC,
    };

    std::vector<std::byte> bytes;
    encode_message(Message{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(MessageType::NewOrder));

    Message decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<NewOrder>(decoded));
    EXPECT_EQ(std::get<NewOrder>(decoded), original);
}

TEST(OrderEntryCodec, NewOrderNegativePrice) {
    // Same rationale as ProtocolRoundtrip.AddOrderNegativePrice: the codec
    // must round-trip the full int64_t range; validating that a price makes
    // sense is not this layer's job.
    NewOrder original{
        .account_id = 1,
        .client_order_id = 1,
        .instrument_id = 1,
        .side = Side::Sell,
        .price = -12345,
        .quantity = 1,
        .order_type = OrderType::Limit,
        .time_in_force = TimeInForce::IOC,
    };
    std::vector<std::byte> bytes;
    encode_message(Message{original}, bytes);
    Message decoded = decode_or_fail(bytes);
    EXPECT_EQ(std::get<NewOrder>(decoded).price, -12345);
}

TEST(OrderEntryCodec, CancelOrder) {
    CancelOrder original{.account_id = 100, .client_order_id = 7, .instrument_id = 1};

    std::vector<std::byte> bytes;
    encode_message(Message{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(MessageType::CancelOrder));

    Message decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<CancelOrder>(decoded));
    EXPECT_EQ(std::get<CancelOrder>(decoded), original);
}

TEST(OrderEntryCodec, ReplaceOrder) {
    ReplaceOrder original{
        .account_id = 100,
        .original_client_order_id = 7,
        .new_client_order_id = 8,
        .instrument_id = 1,
        .new_price = 5000,
        .new_quantity = 20,
    };

    std::vector<std::byte> bytes;
    encode_message(Message{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(MessageType::ReplaceOrder));

    Message decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<ReplaceOrder>(decoded));
    EXPECT_EQ(std::get<ReplaceOrder>(decoded), original);
}

TEST(OrderEntryCodec, Accepted) {
    Accepted original{
        .account_id = 100,
        .client_order_id = 7,
        .exchange_order_id = 9001,
        .instrument_id = 1,
        .side = Side::Buy,
        .price = 250000,
        .quantity = 10,
        .order_type = OrderType::Limit,
        .time_in_force = TimeInForce::FOK,
    };

    std::vector<std::byte> bytes;
    encode_message(Message{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(MessageType::Accepted));

    Message decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<Accepted>(decoded));
    EXPECT_EQ(std::get<Accepted>(decoded), original);
}

TEST(OrderEntryCodec, Rejected) {
    Rejected original{
        .account_id = 100,
        .client_order_id = 7,
        .instrument_id = 1,
        .reason = RejectReason::InsufficientFunds,
    };

    std::vector<std::byte> bytes;
    encode_message(Message{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(MessageType::Rejected));

    Message decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<Rejected>(decoded));
    EXPECT_EQ(std::get<Rejected>(decoded), original);
}

// AccountMismatch is the one reject reason the gateway itself produces
// rather than the matching or risk engine (see OrderEntryGateway's session
// binding), and it is the highest-valued enumerator -- so this is also the
// codec's check that appending to RejectReason kept the wire round-trip
// intact.
TEST(OrderEntryCodec, RejectedWithGatewayProducedAccountMismatchReason) {
    Rejected original{
        .account_id = 100,
        .client_order_id = 7,
        .instrument_id = 1,
        .reason = RejectReason::AccountMismatch,
    };

    std::vector<std::byte> bytes;
    encode_message(Message{original}, bytes);

    Message decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<Rejected>(decoded));
    EXPECT_EQ(std::get<Rejected>(decoded), original);
}

TEST(OrderEntryCodec, Cancelled) {
    Cancelled original{.account_id = 100, .client_order_id = 7, .exchange_order_id = 9001, .instrument_id = 1};

    std::vector<std::byte> bytes;
    encode_message(Message{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(MessageType::Cancelled));

    Message decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<Cancelled>(decoded));
    EXPECT_EQ(std::get<Cancelled>(decoded), original);
}

TEST(OrderEntryCodec, Replaced) {
    Replaced original{
        .account_id = 100,
        .original_client_order_id = 7,
        .new_client_order_id = 8,
        .exchange_order_id = 9001,
        .instrument_id = 1,
        .new_price = 5000,
        .new_quantity = 20,
    };

    std::vector<std::byte> bytes;
    encode_message(Message{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(MessageType::Replaced));

    Message decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<Replaced>(decoded));
    EXPECT_EQ(std::get<Replaced>(decoded), original);
}

TEST(OrderEntryCodec, TradeReport) {
    TradeReport original{
        .account_id = 100,
        .client_order_id = 7,
        .exchange_order_id = 9001,
        .instrument_id = 1,
        .price = 250000,
        .quantity = 10,
        .remaining_quantity = 0,
    };

    std::vector<std::byte> bytes;
    encode_message(Message{original}, bytes);
    EXPECT_EQ(bytes.size(), HEADER_SIZE + payload_size_for(MessageType::TradeReport));

    Message decoded = decode_or_fail(bytes);
    ASSERT_TRUE(std::holds_alternative<TradeReport>(decoded));
    EXPECT_EQ(std::get<TradeReport>(decoded), original);
}

TEST(OrderEntryCodec, MultipleMessagesConcatenateCleanly) {
    // Simulates what a TCP reader actually sees: several encoded messages
    // back-to-back in one buffer, with no gap and no framing beyond each
    // message's own header.
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
    std::size_t first_len = bytes.size();
    encode_message(Message{CancelOrder{.account_id = 1, .client_order_id = 1, .instrument_id = 1}}, bytes);

    Message first = decode_or_fail(std::span(bytes).first(first_len));
    ASSERT_TRUE(std::holds_alternative<NewOrder>(first));

    Message second = decode_or_fail(std::span(bytes).subspan(first_len));
    ASSERT_TRUE(std::holds_alternative<CancelOrder>(second));
}
