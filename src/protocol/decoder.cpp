#include "protocol/decoder.hpp"

#include "common/byte_io.hpp"

namespace mdh::protocol {

namespace {

[[nodiscard]] bool is_known_type(std::uint8_t raw) {
    switch (raw) {
        case static_cast<std::uint8_t>(MessageType::AddOrder):
        case static_cast<std::uint8_t>(MessageType::CancelOrder):
        case static_cast<std::uint8_t>(MessageType::ModifyOrder):
        case static_cast<std::uint8_t>(MessageType::Trade):
        case static_cast<std::uint8_t>(MessageType::ClearBook):
            return true;
        default:
            return false;
    }
}

[[nodiscard]] bool is_valid_side(std::uint8_t raw) {
    return raw == static_cast<std::uint8_t>(Side::Buy) || raw == static_cast<std::uint8_t>(Side::Sell);
}

} // namespace

std::variant<Header, DecodeError> decode_header(std::span<const std::byte> data) {
    if (data.size() < HEADER_SIZE) {
        return DecodeError::TruncatedHeader;
    }

    io::ByteReader r(data.first(HEADER_SIZE));
    auto type_raw = r.get_u8();
    auto reserved = r.get_u8();
    auto payload_size = r.get_u16();
    auto seq = r.get_u64();
    auto ts = r.get_u64();

    if (!type_raw || !reserved || !payload_size || !seq || !ts) {
        return DecodeError::TruncatedHeader;
    }
    if (*reserved != 0) {
        return DecodeError::InvalidReserved;
    }
    if (!is_known_type(*type_raw)) {
        return DecodeError::InvalidMessageType;
    }

    return Header{
        .type = static_cast<MessageType>(*type_raw),
        .payload_size = *payload_size,
        .sequence_number = *seq,
        .timestamp_ns = *ts,
    };
}

std::variant<Event, DecodeError> decode_event(std::span<const std::byte> data) {
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
        case MessageType::AddOrder: {
            auto order_id = r.get_u64();
            auto instrument_id = r.get_u32();
            auto price = r.get_i64();
            auto quantity = r.get_u64();
            auto side_raw = r.get_u8();
            if (!order_id || !instrument_id || !price || !quantity || !side_raw) {
                return DecodeError::TruncatedPayload;
            }
            if (!is_valid_side(*side_raw)) {
                return DecodeError::InvalidSide;
            }
            return AddOrder{
                .sequence_number = header.sequence_number,
                .timestamp_ns = header.timestamp_ns,
                .order_id = *order_id,
                .instrument_id = *instrument_id,
                .price = *price,
                .quantity = *quantity,
                .side = static_cast<Side>(*side_raw),
            };
        }
        case MessageType::CancelOrder: {
            auto order_id = r.get_u64();
            auto instrument_id = r.get_u32();
            if (!order_id || !instrument_id) {
                return DecodeError::TruncatedPayload;
            }
            return CancelOrder{
                .sequence_number = header.sequence_number,
                .timestamp_ns = header.timestamp_ns,
                .order_id = *order_id,
                .instrument_id = *instrument_id,
            };
        }
        case MessageType::ModifyOrder: {
            auto order_id = r.get_u64();
            auto instrument_id = r.get_u32();
            auto new_price = r.get_i64();
            auto new_quantity = r.get_u64();
            if (!order_id || !instrument_id || !new_price || !new_quantity) {
                return DecodeError::TruncatedPayload;
            }
            return ModifyOrder{
                .sequence_number = header.sequence_number,
                .timestamp_ns = header.timestamp_ns,
                .order_id = *order_id,
                .instrument_id = *instrument_id,
                .new_price = *new_price,
                .new_quantity = *new_quantity,
            };
        }
        case MessageType::Trade: {
            auto instrument_id = r.get_u32();
            auto price = r.get_i64();
            auto quantity = r.get_u64();
            auto side_raw = r.get_u8();
            if (!instrument_id || !price || !quantity || !side_raw) {
                return DecodeError::TruncatedPayload;
            }
            if (!is_valid_side(*side_raw)) {
                return DecodeError::InvalidSide;
            }
            return Trade{
                .sequence_number = header.sequence_number,
                .timestamp_ns = header.timestamp_ns,
                .instrument_id = *instrument_id,
                .price = *price,
                .quantity = *quantity,
                .aggressor_side = static_cast<Side>(*side_raw),
            };
        }
        case MessageType::ClearBook: {
            auto instrument_id = r.get_u32();
            if (!instrument_id) {
                return DecodeError::TruncatedPayload;
            }
            return ClearBook{
                .sequence_number = header.sequence_number,
                .timestamp_ns = header.timestamp_ns,
                .instrument_id = *instrument_id,
            };
        }
    }

    // Unreachable: decode_header() only returns a Header once `type` has
    // been validated against every MessageType enumerator above.
    return DecodeError::InvalidMessageType;
}

} // namespace mdh::protocol
