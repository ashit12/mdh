#pragma once

#include <span>
#include <variant>

#include "exchange/core/commands.hpp"
#include "exchange/persistence/command_errors.hpp"
#include "exchange/persistence/command_messages.hpp"

namespace mdh::exchange::persistence {

// Decodes just the fixed 12-byte header. Returns
// CommandDecodeError::TruncatedHeader if fewer than HEADER_SIZE bytes are
// available, or InvalidReserved if the reserved byte is non-zero. Does NOT
// validate `type` against `payload_size` -- that needs payload_size_for(),
// which requires a type already known to be a valid CommandMessageType, so
// it happens in decode_command() instead. Mirrors protocol::decode_header.
[[nodiscard]] std::variant<CommandHeader, CommandDecodeError> decode_command_header(std::span<const std::byte> data);

// Decodes a full frame: `data` must contain exactly the header bytes
// followed by exactly the payload bytes for that header's command type
// (i.e. data.size() == HEADER_SIZE + header.payload_size). Callers that
// read frames incrementally from a stream (see command_journal_reader.hpp)
// are expected to peek the header first to learn how many payload bytes to
// read before calling this. Mirrors protocol::decode_event.
[[nodiscard]] std::variant<ExchangeCommand, CommandDecodeError> decode_command(std::span<const std::byte> data);

} // namespace mdh::exchange::persistence
