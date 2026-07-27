#pragma once

#include <cstdint>
#include <variant>

#include "common/types.hpp"

namespace mdh::protocol {

// Wire header: 20 bytes, present on every event.
//   type            u8   offset 0
//   reserved        u8   offset 1   (must be 0; reserved for future flags)
//   payload_size    u16  offset 2   (size of the payload that follows, bytes)
//   sequence_number u64  offset 4
//   timestamp_ns    u64  offset 12
inline constexpr std::size_t HEADER_SIZE = 20;

enum class MessageType : std::uint8_t {
    AddOrder = 1,
    CancelOrder = 2,
    ModifyOrder = 3,
    Trade = 4,
    ClearBook = 5,
};

struct Header {
    MessageType type;
    std::uint16_t payload_size;
    Sequence sequence_number;
    Timestamp timestamp_ns;
};

// Decoded message structs each carry sequence_number/timestamp_ns copied
// from the header, so a decoded message is self-contained per the spec's
// field list, even though the wire format itself stores those two fields
// once (in the header) rather than duplicating them across five payloads.

struct AddOrder {
    Sequence sequence_number;
    Timestamp timestamp_ns;
    OrderId order_id;
    InstrumentId instrument_id;
    Price price;
    Quantity quantity;
    Side side;
};

struct CancelOrder {
    Sequence sequence_number;
    Timestamp timestamp_ns;
    OrderId order_id;
    InstrumentId instrument_id;
};

struct ModifyOrder {
    Sequence sequence_number;
    Timestamp timestamp_ns;
    OrderId order_id;
    InstrumentId instrument_id;
    Price new_price;
    Quantity new_quantity;
};

struct Trade {
    Sequence sequence_number;
    Timestamp timestamp_ns;
    InstrumentId instrument_id;
    Price price;
    Quantity quantity;
    Side aggressor_side;
};

struct ClearBook {
    Sequence sequence_number;
    Timestamp timestamp_ns;
    InstrumentId instrument_id;
};

using Event = std::variant<AddOrder, CancelOrder, ModifyOrder, Trade, ClearBook>;

// Fixed on-wire payload size (bytes, not counting the header) for each
// known message type. All milestone-1 message types are fixed-size; a
// variable-length type would need HEADER_SIZE + payload_size validated
// against a minimum instead of an exact match.
[[nodiscard]] constexpr std::size_t payload_size_for(MessageType type) {
    switch (type) {
        case MessageType::AddOrder:    return 8 + 4 + 8 + 8 + 1; // 29
        case MessageType::CancelOrder: return 8 + 4;             // 12
        case MessageType::ModifyOrder: return 8 + 4 + 8 + 8;     // 28
        case MessageType::Trade:       return 4 + 8 + 8 + 1;     // 21
        case MessageType::ClearBook:   return 4;                 // 4
    }
    return 0;
}

} // namespace mdh::protocol
