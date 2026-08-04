#include "exchange/persistence/command_encoder.hpp"

#include <type_traits>
#include <variant>

#include "common/byte_io.hpp"
#include "exchange/persistence/command_messages.hpp"

namespace mdh::exchange::persistence {

namespace {

void put_header(std::vector<std::byte>& out, CommandMessageType type, CommandSequence command_sequence,
                 std::uint16_t payload_size) {
    io::put_u8(out, static_cast<std::uint8_t>(type));
    io::put_u8(out, 0); // reserved
    io::put_u16(out, payload_size);
    io::put_u64(out, command_sequence);
}

void put_side(std::vector<std::byte>& out, Side side) { io::put_u8(out, static_cast<std::uint8_t>(side)); }
void put_order_type(std::vector<std::byte>& out, OrderType t) { io::put_u8(out, static_cast<std::uint8_t>(t)); }
void put_time_in_force(std::vector<std::byte>& out, TimeInForce t) { io::put_u8(out, static_cast<std::uint8_t>(t)); }

} // namespace

void encode_command(const ExchangeCommand& command, std::vector<std::byte>& out) {
    std::visit(
        [&out](const auto& cmd) {
            using T = std::decay_t<decltype(cmd)>;

            if constexpr (std::is_same_v<T, NewOrderCommand>) {
                put_header(out, CommandMessageType::NewOrder, cmd.command_sequence,
                           static_cast<std::uint16_t>(payload_size_for(CommandMessageType::NewOrder)));
                io::put_u64(out, cmd.account_id);
                io::put_u64(out, cmd.client_order_id);
                io::put_u32(out, cmd.instrument_id);
                put_side(out, cmd.side);
                io::put_i64(out, cmd.price);
                io::put_u64(out, cmd.quantity);
                put_order_type(out, cmd.order_type);
                put_time_in_force(out, cmd.time_in_force);
            } else if constexpr (std::is_same_v<T, CancelOrderCommand>) {
                put_header(out, CommandMessageType::CancelOrder, cmd.command_sequence,
                           static_cast<std::uint16_t>(payload_size_for(CommandMessageType::CancelOrder)));
                io::put_u64(out, cmd.account_id);
                io::put_u64(out, cmd.client_order_id);
                io::put_u32(out, cmd.instrument_id);
            } else if constexpr (std::is_same_v<T, ReplaceOrderCommand>) {
                put_header(out, CommandMessageType::ReplaceOrder, cmd.command_sequence,
                           static_cast<std::uint16_t>(payload_size_for(CommandMessageType::ReplaceOrder)));
                io::put_u64(out, cmd.account_id);
                io::put_u64(out, cmd.original_client_order_id);
                io::put_u64(out, cmd.new_client_order_id);
                io::put_u32(out, cmd.instrument_id);
                io::put_i64(out, cmd.new_price);
                io::put_u64(out, cmd.new_quantity);
            }
        },
        command);
}

} // namespace mdh::exchange::persistence
