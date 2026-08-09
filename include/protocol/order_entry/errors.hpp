#pragma once

#include <string_view>

namespace mdh::protocol::order_entry {

enum class DecodeError {
    TruncatedHeader,    // fewer than HEADER_SIZE bytes available
    TruncatedPayload,   // header read, but payload_size bytes unavailable
    InvalidMessageType,
    InvalidMessageSize,  // header.payload_size does not match the fixed size for msg type
    InvalidSide,
    InvalidOrderType,
    InvalidTimeInForce,
    InvalidRejectReason,
};

[[nodiscard]] constexpr std::string_view to_string(DecodeError e) {
    switch (e) {
        case DecodeError::TruncatedHeader:     return "TruncatedHeader";
        case DecodeError::TruncatedPayload:    return "TruncatedPayload";
        case DecodeError::InvalidMessageType:  return "InvalidMessageType";
        case DecodeError::InvalidMessageSize:  return "InvalidMessageSize";
        case DecodeError::InvalidSide:         return "InvalidSide";
        case DecodeError::InvalidOrderType:    return "InvalidOrderType";
        case DecodeError::InvalidTimeInForce:  return "InvalidTimeInForce";
        case DecodeError::InvalidRejectReason: return "InvalidRejectReason";
    }
    return "UnknownDecodeError";
}

} // namespace mdh::protocol::order_entry
