#include "protocol/order_entry/encoder.hpp"

#include "common/byte_io.hpp"

namespace mdh::protocol::order_entry {

namespace {

void put_header(std::vector<std::byte>& out, MessageType type, std::uint16_t payload_size) {
    io::put_u8(out, static_cast<std::uint8_t>(type));
    io::put_u16(out, payload_size);
}

void put_side(std::vector<std::byte>& out, Side side) {
    io::put_u8(out, static_cast<std::uint8_t>(side));
}

void put_order_type(std::vector<std::byte>& out, exchange::OrderType order_type) {
    io::put_u8(out, static_cast<std::uint8_t>(order_type));
}

void put_time_in_force(std::vector<std::byte>& out, exchange::TimeInForce tif) {
    io::put_u8(out, static_cast<std::uint8_t>(tif));
}

void put_reject_reason(std::vector<std::byte>& out, exchange::RejectReason reason) {
    io::put_u8(out, static_cast<std::uint8_t>(reason));
}

} // namespace

void encode_message(const Message& message, std::vector<std::byte>& out) {
    std::visit(
        [&out](const auto& msg) {
            using T = std::decay_t<decltype(msg)>;

            if constexpr (std::is_same_v<T, NewOrder>) {
                put_header(out, MessageType::NewOrder,
                           static_cast<std::uint16_t>(payload_size_for(MessageType::NewOrder)));
                io::put_u64(out, msg.account_id);
                io::put_u64(out, msg.client_order_id);
                io::put_u32(out, msg.instrument_id);
                put_side(out, msg.side);
                io::put_i64(out, msg.price);
                io::put_u64(out, msg.quantity);
                put_order_type(out, msg.order_type);
                put_time_in_force(out, msg.time_in_force);
            } else if constexpr (std::is_same_v<T, CancelOrder>) {
                put_header(out, MessageType::CancelOrder,
                           static_cast<std::uint16_t>(payload_size_for(MessageType::CancelOrder)));
                io::put_u64(out, msg.account_id);
                io::put_u64(out, msg.client_order_id);
                io::put_u32(out, msg.instrument_id);
            } else if constexpr (std::is_same_v<T, ReplaceOrder>) {
                put_header(out, MessageType::ReplaceOrder,
                           static_cast<std::uint16_t>(payload_size_for(MessageType::ReplaceOrder)));
                io::put_u64(out, msg.account_id);
                io::put_u64(out, msg.original_client_order_id);
                io::put_u64(out, msg.new_client_order_id);
                io::put_u32(out, msg.instrument_id);
                io::put_i64(out, msg.new_price);
                io::put_u64(out, msg.new_quantity);
            } else if constexpr (std::is_same_v<T, Accepted>) {
                put_header(out, MessageType::Accepted,
                           static_cast<std::uint16_t>(payload_size_for(MessageType::Accepted)));
                io::put_u64(out, msg.account_id);
                io::put_u64(out, msg.client_order_id);
                io::put_u64(out, msg.exchange_order_id);
                io::put_u32(out, msg.instrument_id);
                put_side(out, msg.side);
                io::put_i64(out, msg.price);
                io::put_u64(out, msg.quantity);
                put_order_type(out, msg.order_type);
                put_time_in_force(out, msg.time_in_force);
            } else if constexpr (std::is_same_v<T, Rejected>) {
                put_header(out, MessageType::Rejected,
                           static_cast<std::uint16_t>(payload_size_for(MessageType::Rejected)));
                io::put_u64(out, msg.account_id);
                io::put_u64(out, msg.client_order_id);
                io::put_u32(out, msg.instrument_id);
                put_reject_reason(out, msg.reason);
            } else if constexpr (std::is_same_v<T, Cancelled>) {
                put_header(out, MessageType::Cancelled,
                           static_cast<std::uint16_t>(payload_size_for(MessageType::Cancelled)));
                io::put_u64(out, msg.account_id);
                io::put_u64(out, msg.client_order_id);
                io::put_u64(out, msg.exchange_order_id);
                io::put_u32(out, msg.instrument_id);
            } else if constexpr (std::is_same_v<T, Replaced>) {
                put_header(out, MessageType::Replaced,
                           static_cast<std::uint16_t>(payload_size_for(MessageType::Replaced)));
                io::put_u64(out, msg.account_id);
                io::put_u64(out, msg.original_client_order_id);
                io::put_u64(out, msg.new_client_order_id);
                io::put_u64(out, msg.exchange_order_id);
                io::put_u32(out, msg.instrument_id);
                io::put_i64(out, msg.new_price);
                io::put_u64(out, msg.new_quantity);
            } else if constexpr (std::is_same_v<T, TradeReport>) {
                put_header(out, MessageType::TradeReport,
                           static_cast<std::uint16_t>(payload_size_for(MessageType::TradeReport)));
                io::put_u64(out, msg.account_id);
                io::put_u64(out, msg.client_order_id);
                io::put_u64(out, msg.exchange_order_id);
                io::put_u32(out, msg.instrument_id);
                io::put_i64(out, msg.price);
                io::put_u64(out, msg.quantity);
                io::put_u64(out, msg.remaining_quantity);
            }
        },
        message);
}

} // namespace mdh::protocol::order_entry
