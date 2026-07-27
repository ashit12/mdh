#pragma once

#include <string_view>

namespace mdh::protocol {

enum class DecodeError {
    TruncatedHeader,   // fewer than HEADER_SIZE bytes available
    TruncatedPayload,  // header read, but payload_size bytes unavailable
    InvalidReserved,   // header reserved byte was non-zero
    InvalidMessageType,
    InvalidMessageSize, // header.payload_size does not match the fixed size for msg type
    InvalidSide,
};

[[nodiscard]] constexpr std::string_view to_string(DecodeError e) {
    switch (e) {
        case DecodeError::TruncatedHeader:    return "TruncatedHeader";
        case DecodeError::TruncatedPayload:   return "TruncatedPayload";
        case DecodeError::InvalidReserved:    return "InvalidReserved";
        case DecodeError::InvalidMessageType: return "InvalidMessageType";
        case DecodeError::InvalidMessageSize: return "InvalidMessageSize";
        case DecodeError::InvalidSide:        return "InvalidSide";
    }
    return "UnknownDecodeError";
}

} // namespace mdh::protocol
