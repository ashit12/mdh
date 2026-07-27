#include "protocol/encoder.hpp"

#include "common/byte_io.hpp"

namespace mdh::protocol {

namespace {

void put_header(std::vector<std::byte>& out, MessageType type, Sequence seq,
                 Timestamp ts, std::uint16_t payload_size) {
    io::put_u8(out, static_cast<std::uint8_t>(type));
    io::put_u8(out, 0); // reserved
    io::put_u16(out, payload_size);
    io::put_u64(out, seq);
    io::put_u64(out, ts);
}

void put_side(std::vector<std::byte>& out, Side side) {
    io::put_u8(out, static_cast<std::uint8_t>(side));
}

} // namespace

void encode_event(const Event& event, std::vector<std::byte>& out) {
    std::visit(
        [&out](const auto& msg) {
            using T = std::decay_t<decltype(msg)>;

            if constexpr (std::is_same_v<T, AddOrder>) {
                put_header(out, MessageType::AddOrder, msg.sequence_number, msg.timestamp_ns,
                           static_cast<std::uint16_t>(payload_size_for(MessageType::AddOrder)));
                io::put_u64(out, msg.order_id);
                io::put_u32(out, msg.instrument_id);
                io::put_i64(out, msg.price);
                io::put_u64(out, msg.quantity);
                put_side(out, msg.side);
            } else if constexpr (std::is_same_v<T, CancelOrder>) {
                put_header(out, MessageType::CancelOrder, msg.sequence_number, msg.timestamp_ns,
                           static_cast<std::uint16_t>(payload_size_for(MessageType::CancelOrder)));
                io::put_u64(out, msg.order_id);
                io::put_u32(out, msg.instrument_id);
            } else if constexpr (std::is_same_v<T, ModifyOrder>) {
                put_header(out, MessageType::ModifyOrder, msg.sequence_number, msg.timestamp_ns,
                           static_cast<std::uint16_t>(payload_size_for(MessageType::ModifyOrder)));
                io::put_u64(out, msg.order_id);
                io::put_u32(out, msg.instrument_id);
                io::put_i64(out, msg.new_price);
                io::put_u64(out, msg.new_quantity);
            } else if constexpr (std::is_same_v<T, Trade>) {
                put_header(out, MessageType::Trade, msg.sequence_number, msg.timestamp_ns,
                           static_cast<std::uint16_t>(payload_size_for(MessageType::Trade)));
                io::put_u32(out, msg.instrument_id);
                io::put_i64(out, msg.price);
                io::put_u64(out, msg.quantity);
                put_side(out, msg.aggressor_side);
            } else if constexpr (std::is_same_v<T, ClearBook>) {
                put_header(out, MessageType::ClearBook, msg.sequence_number, msg.timestamp_ns,
                           static_cast<std::uint16_t>(payload_size_for(MessageType::ClearBook)));
                io::put_u32(out, msg.instrument_id);
            }
        },
        event);
}

} // namespace mdh::protocol
