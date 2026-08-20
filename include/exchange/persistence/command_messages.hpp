#pragma once

#include <cstddef>
#include <cstdint>

#include "common/types.hpp"
#include "exchange/core/types.hpp"

// Wire format for journaling exchange commands. This is a
// SEPARATE format from protocol/messages.hpp -- that one is the trader-side
// market-data wire format (AddOrder/CancelOrder/ModifyOrder/Trade/ClearBook,
// unrelated events, unrelated namespace) and is not modified or reused here.
// The two formats intentionally share only a *style*: big-endian encoding
// via common/byte_io.hpp, a small fixed header followed by a type-specific
// fixed payload, and a structured decode-error enum instead of exceptions
// (see command_errors.hpp) -- the same conventions applied to a different
// vocabulary, per the working rule to reuse patterns without merging
// unrelated concepts.
namespace mdh::exchange::persistence {

// 12-byte header, present before every journaled command:
//   type             u8   offset 0
//   reserved         u8   offset 1   (must be 0; reserved for future flags)
//   payload_size     u16  offset 2
//   command_sequence u64  offset 4
//
// Unlike protocol::Header, there is no timestamp field: ExchangeCommand
// itself carries no timestamp (the matching engine's determinism rule
// forbids one being captured inside the matcher), so the journal format
// doesn't invent one that doesn't exist in the domain type it's recording.
inline constexpr std::size_t HEADER_SIZE = 12;

enum class CommandMessageType : std::uint8_t {
    NewOrder = 1,
    CancelOrder = 2,
    ReplaceOrder = 3,
    // Not a command: names an instrument the engine that wrote this journal
    // traded. A MatchingEngine rejects commands on instruments it was not
    // told about, so a journal that only listed commands would no longer be
    // enough to reproduce the run that wrote it -- a replay would need the
    // universe handed to it out of band, and would silently diverge if it
    // were given the wrong one. Written before any command, and carried in
    // the file rather than in a file header so the format stays what it has
    // always been: a bare concatenation of frames, which readers that
    // predate this type reject cleanly as InvalidMessageType instead of
    // misparsing.
    RegisterInstrument = 4,
};

struct CommandHeader {
    CommandMessageType type;
    std::uint16_t payload_size;
    CommandSequence command_sequence;
};

// Fixed on-wire payload size (bytes, not counting the header) for each
// known command type. command_sequence itself lives in the header, not the
// payload, matching protocol::Header's convention of hoisting the one field
// every message type shares.
[[nodiscard]] constexpr std::size_t payload_size_for(CommandMessageType type) {
    switch (type) {
        // account_id(8) + client_order_id(8) + instrument_id(4) + side(1) +
        // price(8) + quantity(8) + order_type(1) + time_in_force(1)
        case CommandMessageType::NewOrder:
            return 8 + 8 + 4 + 1 + 8 + 8 + 1 + 1; // 39
        // account_id(8) + client_order_id(8) + instrument_id(4)
        case CommandMessageType::CancelOrder:
            return 8 + 8 + 4; // 20
        // account_id(8) + original_client_order_id(8) + new_client_order_id(8)
        // + instrument_id(4) + new_price(8) + new_quantity(8)
        case CommandMessageType::ReplaceOrder:
            return 8 + 8 + 8 + 4 + 8 + 8; // 44
        // instrument_id(4)
        case CommandMessageType::RegisterInstrument:
            return 4;
    }
    return 0;
}

// The decoded form of a RegisterInstrument frame. Deliberately not an
// alternative of ExchangeCommand: it is not something a client can send, it
// mutates no book, and it produces no event -- keeping it out of that
// variant is what stops every switch over a command from having to pretend
// this is one. Its frame's command_sequence is 0, since it consumes no
// sequence number.
struct RegisterInstrumentRecord {
    InstrumentId instrument_id;

    bool operator==(const RegisterInstrumentRecord&) const = default;
};

} // namespace mdh::exchange::persistence
