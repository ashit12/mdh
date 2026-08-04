#include "exchange/persistence/command_decoder.hpp"

#include "common/byte_io.hpp"

namespace mdh::exchange::persistence {

namespace {

[[nodiscard]] bool is_known_type(std::uint8_t raw) {
    switch (raw) {
        case static_cast<std::uint8_t>(CommandMessageType::NewOrder):
        case static_cast<std::uint8_t>(CommandMessageType::CancelOrder):
        case static_cast<std::uint8_t>(CommandMessageType::ReplaceOrder):
            return true;
        default:
            return false;
    }
}

[[nodiscard]] bool is_valid_side(std::uint8_t raw) {
    return raw == static_cast<std::uint8_t>(Side::Buy) || raw == static_cast<std::uint8_t>(Side::Sell);
}

[[nodiscard]] bool is_valid_order_type(std::uint8_t raw) { return raw == static_cast<std::uint8_t>(OrderType::Limit); }

[[nodiscard]] bool is_valid_time_in_force(std::uint8_t raw) {
    switch (raw) {
        case static_cast<std::uint8_t>(TimeInForce::GTC):
        case static_cast<std::uint8_t>(TimeInForce::IOC):
        case static_cast<std::uint8_t>(TimeInForce::FOK):
            return true;
        default:
            return false;
    }
}

} // namespace

std::variant<CommandHeader, CommandDecodeError> decode_command_header(std::span<const std::byte> data) {
    if (data.size() < HEADER_SIZE) {
        return CommandDecodeError::TruncatedHeader;
    }

    io::ByteReader r(data.first(HEADER_SIZE));
    auto type_raw = r.get_u8();
    auto reserved = r.get_u8();
    auto payload_size = r.get_u16();
    auto command_sequence = r.get_u64();

    if (!type_raw || !reserved || !payload_size || !command_sequence) {
        return CommandDecodeError::TruncatedHeader;
    }
    if (*reserved != 0) {
        return CommandDecodeError::InvalidReserved;
    }
    if (!is_known_type(*type_raw)) {
        return CommandDecodeError::InvalidMessageType;
    }

    return CommandHeader{
        .type = static_cast<CommandMessageType>(*type_raw),
        .payload_size = *payload_size,
        .command_sequence = *command_sequence,
    };
}

std::variant<ExchangeCommand, CommandDecodeError> decode_command(std::span<const std::byte> data) {
    auto header_result = decode_command_header(data);
    if (std::holds_alternative<CommandDecodeError>(header_result)) {
        return std::get<CommandDecodeError>(header_result);
    }
    const CommandHeader& header = std::get<CommandHeader>(header_result);

    const std::size_t expected_payload = payload_size_for(header.type);
    if (header.payload_size != expected_payload) {
        return CommandDecodeError::InvalidMessageSize;
    }
    if (data.size() < HEADER_SIZE + header.payload_size) {
        return CommandDecodeError::TruncatedPayload;
    }

    io::ByteReader r(data.subspan(HEADER_SIZE, header.payload_size));

    switch (header.type) {
        case CommandMessageType::NewOrder: {
            auto account_id = r.get_u64();
            auto client_order_id = r.get_u64();
            auto instrument_id = r.get_u32();
            auto side_raw = r.get_u8();
            auto price = r.get_i64();
            auto quantity = r.get_u64();
            auto order_type_raw = r.get_u8();
            auto tif_raw = r.get_u8();
            if (!account_id || !client_order_id || !instrument_id || !side_raw || !price || !quantity ||
                !order_type_raw || !tif_raw) {
                return CommandDecodeError::TruncatedPayload;
            }
            if (!is_valid_side(*side_raw)) {
                return CommandDecodeError::InvalidSide;
            }
            if (!is_valid_order_type(*order_type_raw)) {
                return CommandDecodeError::InvalidOrderType;
            }
            if (!is_valid_time_in_force(*tif_raw)) {
                return CommandDecodeError::InvalidTimeInForce;
            }
            return ExchangeCommand{NewOrderCommand{
                .command_sequence = header.command_sequence,
                .account_id = *account_id,
                .client_order_id = *client_order_id,
                .instrument_id = *instrument_id,
                .side = static_cast<Side>(*side_raw),
                .price = *price,
                .quantity = *quantity,
                .order_type = static_cast<OrderType>(*order_type_raw),
                .time_in_force = static_cast<TimeInForce>(*tif_raw),
            }};
        }
        case CommandMessageType::CancelOrder: {
            auto account_id = r.get_u64();
            auto client_order_id = r.get_u64();
            auto instrument_id = r.get_u32();
            if (!account_id || !client_order_id || !instrument_id) {
                return CommandDecodeError::TruncatedPayload;
            }
            return ExchangeCommand{CancelOrderCommand{
                .command_sequence = header.command_sequence,
                .account_id = *account_id,
                .client_order_id = *client_order_id,
                .instrument_id = *instrument_id,
            }};
        }
        case CommandMessageType::ReplaceOrder: {
            auto account_id = r.get_u64();
            auto original_client_order_id = r.get_u64();
            auto new_client_order_id = r.get_u64();
            auto instrument_id = r.get_u32();
            auto new_price = r.get_i64();
            auto new_quantity = r.get_u64();
            if (!account_id || !original_client_order_id || !new_client_order_id || !instrument_id || !new_price ||
                !new_quantity) {
                return CommandDecodeError::TruncatedPayload;
            }
            return ExchangeCommand{ReplaceOrderCommand{
                .command_sequence = header.command_sequence,
                .account_id = *account_id,
                .original_client_order_id = *original_client_order_id,
                .new_client_order_id = *new_client_order_id,
                .instrument_id = *instrument_id,
                .new_price = *new_price,
                .new_quantity = *new_quantity,
            }};
        }
    }

    // Unreachable: decode_command_header() only returns a CommandHeader once
    // `type` has been validated against every CommandMessageType enumerator
    // above.
    return CommandDecodeError::InvalidMessageType;
}

} // namespace mdh::exchange::persistence
