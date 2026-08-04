#pragma once

#include <string_view>

namespace mdh::exchange::persistence {

enum class CommandDecodeError {
    TruncatedHeader,     // fewer than HEADER_SIZE bytes available
    TruncatedPayload,    // header read, but payload_size bytes unavailable
    InvalidReserved,     // header reserved byte was non-zero
    InvalidMessageType,
    InvalidMessageSize,  // header.payload_size does not match the fixed size for this type
    InvalidSide,
    InvalidOrderType,
    InvalidTimeInForce,
};

[[nodiscard]] constexpr std::string_view to_string(CommandDecodeError e) {
    switch (e) {
        case CommandDecodeError::TruncatedHeader:    return "TruncatedHeader";
        case CommandDecodeError::TruncatedPayload:   return "TruncatedPayload";
        case CommandDecodeError::InvalidReserved:    return "InvalidReserved";
        case CommandDecodeError::InvalidMessageType: return "InvalidMessageType";
        case CommandDecodeError::InvalidMessageSize: return "InvalidMessageSize";
        case CommandDecodeError::InvalidSide:        return "InvalidSide";
        case CommandDecodeError::InvalidOrderType:   return "InvalidOrderType";
        case CommandDecodeError::InvalidTimeInForce: return "InvalidTimeInForce";
    }
    return "UnknownCommandDecodeError";
}

} // namespace mdh::exchange::persistence
