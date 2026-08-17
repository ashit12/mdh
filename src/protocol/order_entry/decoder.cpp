#include "protocol/order_entry/decoder.hpp"

#include "common/byte_io.hpp"

namespace mdh::protocol::order_entry {

namespace {

[[nodiscard]] bool is_known_type(std::uint8_t raw) {
    switch (raw) {
        case static_cast<std::uint8_t>(MessageType::NewOrder):
        case static_cast<std::uint8_t>(MessageType::CancelOrder):
        case static_cast<std::uint8_t>(MessageType::ReplaceOrder):
        case static_cast<std::uint8_t>(MessageType::Accepted):
        case static_cast<std::uint8_t>(MessageType::Rejected):
        case static_cast<std::uint8_t>(MessageType::Cancelled):
        case static_cast<std::uint8_t>(MessageType::Replaced):
        case static_cast<std::uint8_t>(MessageType::TradeReport):
            return true;
        default:
            return false;
    }
}

[[nodiscard]] bool is_valid_side(std::uint8_t raw) {
    return raw == static_cast<std::uint8_t>(Side::Buy) || raw == static_cast<std::uint8_t>(Side::Sell);
}

[[nodiscard]] bool is_valid_order_type(std::uint8_t raw) {
    switch (static_cast<exchange::OrderType>(raw)) {
        case exchange::OrderType::Limit:
            return true;
    }
    return false;
}

[[nodiscard]] bool is_valid_time_in_force(std::uint8_t raw) {
    switch (static_cast<exchange::TimeInForce>(raw)) {
        case exchange::TimeInForce::GTC:
        case exchange::TimeInForce::IOC:
        case exchange::TimeInForce::FOK:
            return true;
    }
    return false;
}

[[nodiscard]] bool is_valid_reject_reason(std::uint8_t raw) {
    switch (static_cast<exchange::RejectReason>(raw)) {
        case exchange::RejectReason::None:
        case exchange::RejectReason::InvalidPrice:
        case exchange::RejectReason::InvalidQuantity:
        case exchange::RejectReason::DuplicateOrderId:
        case exchange::RejectReason::UnknownOrderId:
        case exchange::RejectReason::InvalidInstrument:
        case exchange::RejectReason::InsufficientLiquidity:
        case exchange::RejectReason::InvalidReplacement:
        case exchange::RejectReason::InternalError:
        case exchange::RejectReason::InsufficientFunds:
        case exchange::RejectReason::InsufficientPosition:
        case exchange::RejectReason::OrderTooLarge:
        case exchange::RejectReason::AccountMismatch:
            return true;
    }
    return false;
}

} // namespace

std::variant<Header, DecodeError> decode_header(std::span<const std::byte> data) {
    if (data.size() < HEADER_SIZE) {
        return DecodeError::TruncatedHeader;
    }

    io::ByteReader r(data.first(HEADER_SIZE));
    auto type_raw = r.get_u8();
    auto payload_size = r.get_u16();

    if (!type_raw || !payload_size) {
        return DecodeError::TruncatedHeader;
    }
    if (!is_known_type(*type_raw)) {
        return DecodeError::InvalidMessageType;
    }

    return Header{
        .type = static_cast<MessageType>(*type_raw),
        .payload_size = *payload_size,
    };
}

std::variant<Message, DecodeError> decode_message(std::span<const std::byte> data) {
    auto header_result = decode_header(data);
    if (std::holds_alternative<DecodeError>(header_result)) {
        return std::get<DecodeError>(header_result);
    }
    const Header& header = std::get<Header>(header_result);

    const std::size_t expected_payload = payload_size_for(header.type);
    if (header.payload_size != expected_payload) {
        return DecodeError::InvalidMessageSize;
    }
    if (data.size() < HEADER_SIZE + header.payload_size) {
        return DecodeError::TruncatedPayload;
    }

    io::ByteReader r(data.subspan(HEADER_SIZE, header.payload_size));

    switch (header.type) {
        case MessageType::NewOrder: {
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
                return DecodeError::TruncatedPayload;
            }
            if (!is_valid_side(*side_raw)) {
                return DecodeError::InvalidSide;
            }
            if (!is_valid_order_type(*order_type_raw)) {
                return DecodeError::InvalidOrderType;
            }
            if (!is_valid_time_in_force(*tif_raw)) {
                return DecodeError::InvalidTimeInForce;
            }
            return NewOrder{
                .account_id = *account_id,
                .client_order_id = *client_order_id,
                .instrument_id = *instrument_id,
                .side = static_cast<Side>(*side_raw),
                .price = *price,
                .quantity = *quantity,
                .order_type = static_cast<exchange::OrderType>(*order_type_raw),
                .time_in_force = static_cast<exchange::TimeInForce>(*tif_raw),
            };
        }
        case MessageType::CancelOrder: {
            auto account_id = r.get_u64();
            auto client_order_id = r.get_u64();
            auto instrument_id = r.get_u32();
            if (!account_id || !client_order_id || !instrument_id) {
                return DecodeError::TruncatedPayload;
            }
            return CancelOrder{
                .account_id = *account_id,
                .client_order_id = *client_order_id,
                .instrument_id = *instrument_id,
            };
        }
        case MessageType::ReplaceOrder: {
            auto account_id = r.get_u64();
            auto original_client_order_id = r.get_u64();
            auto new_client_order_id = r.get_u64();
            auto instrument_id = r.get_u32();
            auto new_price = r.get_i64();
            auto new_quantity = r.get_u64();
            if (!account_id || !original_client_order_id || !new_client_order_id || !instrument_id ||
                !new_price || !new_quantity) {
                return DecodeError::TruncatedPayload;
            }
            return ReplaceOrder{
                .account_id = *account_id,
                .original_client_order_id = *original_client_order_id,
                .new_client_order_id = *new_client_order_id,
                .instrument_id = *instrument_id,
                .new_price = *new_price,
                .new_quantity = *new_quantity,
            };
        }
        case MessageType::Accepted: {
            auto account_id = r.get_u64();
            auto client_order_id = r.get_u64();
            auto exchange_order_id = r.get_u64();
            auto instrument_id = r.get_u32();
            auto side_raw = r.get_u8();
            auto price = r.get_i64();
            auto quantity = r.get_u64();
            auto order_type_raw = r.get_u8();
            auto tif_raw = r.get_u8();
            if (!account_id || !client_order_id || !exchange_order_id || !instrument_id || !side_raw ||
                !price || !quantity || !order_type_raw || !tif_raw) {
                return DecodeError::TruncatedPayload;
            }
            if (!is_valid_side(*side_raw)) {
                return DecodeError::InvalidSide;
            }
            if (!is_valid_order_type(*order_type_raw)) {
                return DecodeError::InvalidOrderType;
            }
            if (!is_valid_time_in_force(*tif_raw)) {
                return DecodeError::InvalidTimeInForce;
            }
            return Accepted{
                .account_id = *account_id,
                .client_order_id = *client_order_id,
                .exchange_order_id = *exchange_order_id,
                .instrument_id = *instrument_id,
                .side = static_cast<Side>(*side_raw),
                .price = *price,
                .quantity = *quantity,
                .order_type = static_cast<exchange::OrderType>(*order_type_raw),
                .time_in_force = static_cast<exchange::TimeInForce>(*tif_raw),
            };
        }
        case MessageType::Rejected: {
            auto account_id = r.get_u64();
            auto client_order_id = r.get_u64();
            auto instrument_id = r.get_u32();
            auto reason_raw = r.get_u8();
            if (!account_id || !client_order_id || !instrument_id || !reason_raw) {
                return DecodeError::TruncatedPayload;
            }
            if (!is_valid_reject_reason(*reason_raw)) {
                return DecodeError::InvalidRejectReason;
            }
            return Rejected{
                .account_id = *account_id,
                .client_order_id = *client_order_id,
                .instrument_id = *instrument_id,
                .reason = static_cast<exchange::RejectReason>(*reason_raw),
            };
        }
        case MessageType::Cancelled: {
            auto account_id = r.get_u64();
            auto client_order_id = r.get_u64();
            auto exchange_order_id = r.get_u64();
            auto instrument_id = r.get_u32();
            if (!account_id || !client_order_id || !exchange_order_id || !instrument_id) {
                return DecodeError::TruncatedPayload;
            }
            return Cancelled{
                .account_id = *account_id,
                .client_order_id = *client_order_id,
                .exchange_order_id = *exchange_order_id,
                .instrument_id = *instrument_id,
            };
        }
        case MessageType::Replaced: {
            auto account_id = r.get_u64();
            auto original_client_order_id = r.get_u64();
            auto new_client_order_id = r.get_u64();
            auto exchange_order_id = r.get_u64();
            auto instrument_id = r.get_u32();
            auto new_price = r.get_i64();
            auto new_quantity = r.get_u64();
            if (!account_id || !original_client_order_id || !new_client_order_id || !exchange_order_id ||
                !instrument_id || !new_price || !new_quantity) {
                return DecodeError::TruncatedPayload;
            }
            return Replaced{
                .account_id = *account_id,
                .original_client_order_id = *original_client_order_id,
                .new_client_order_id = *new_client_order_id,
                .exchange_order_id = *exchange_order_id,
                .instrument_id = *instrument_id,
                .new_price = *new_price,
                .new_quantity = *new_quantity,
            };
        }
        case MessageType::TradeReport: {
            auto account_id = r.get_u64();
            auto client_order_id = r.get_u64();
            auto exchange_order_id = r.get_u64();
            auto instrument_id = r.get_u32();
            auto price = r.get_i64();
            auto quantity = r.get_u64();
            auto remaining_quantity = r.get_u64();
            if (!account_id || !client_order_id || !exchange_order_id || !instrument_id || !price ||
                !quantity || !remaining_quantity) {
                return DecodeError::TruncatedPayload;
            }
            return TradeReport{
                .account_id = *account_id,
                .client_order_id = *client_order_id,
                .exchange_order_id = *exchange_order_id,
                .instrument_id = *instrument_id,
                .price = *price,
                .quantity = *quantity,
                .remaining_quantity = *remaining_quantity,
            };
        }
    }

    // Unreachable: decode_header() only returns a Header once `type` has
    // been validated against every MessageType enumerator above.
    return DecodeError::InvalidMessageType;
}

} // namespace mdh::protocol::order_entry
