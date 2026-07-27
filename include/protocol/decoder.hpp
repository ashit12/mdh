#pragma once

#include <span>
#include <variant>

#include "protocol/errors.hpp"
#include "protocol/messages.hpp"

namespace mdh::protocol {

// Decodes just the fixed 20-byte header. Returns DecodeError::TruncatedHeader
// if fewer than HEADER_SIZE bytes are available, or InvalidReserved if the
// reserved byte is non-zero. Does NOT validate `type` or `payload_size`
// against each other -- that needs payload_size_for(), which requires a
// type that is already known to be one of the MessageType enumerators, so
// it happens in decode_event() instead.
[[nodiscard]] std::variant<Header, DecodeError> decode_header(std::span<const std::byte> data);

// Decodes a full frame: `data` must contain exactly the header bytes
// followed by exactly the payload bytes for that header's message type
// (i.e. data.size() == HEADER_SIZE + header.payload_size). Callers that
// read frames incrementally from a stream (see replay/event_file_reader)
// are expected to peek the header first to learn how many payload bytes
// to read before calling this.
[[nodiscard]] std::variant<Event, DecodeError> decode_event(std::span<const std::byte> data);

} // namespace mdh::protocol
