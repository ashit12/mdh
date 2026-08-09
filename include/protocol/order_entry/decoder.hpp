#pragma once

#include <span>
#include <variant>

#include "protocol/order_entry/errors.hpp"
#include "protocol/order_entry/messages.hpp"

namespace mdh::protocol::order_entry {

// Decodes just the fixed 3-byte header. Returns DecodeError::TruncatedHeader
// if fewer than HEADER_SIZE bytes are available, or InvalidMessageType if
// the type byte isn't one of the known MessageType enumerators. Does NOT
// validate `payload_size` against `type` -- that needs payload_size_for(),
// which requires a type already known to be valid, so it happens in
// decode_message() instead.
[[nodiscard]] std::variant<Header, DecodeError> decode_header(std::span<const std::byte> data);

// Decodes a full frame: `data` must contain exactly the header bytes
// followed by exactly the payload bytes for that header's message type
// (i.e. data.size() == HEADER_SIZE + header.payload_size). A caller reading
// incrementally off a TCP stream (e.g. the gateway's connection reader
// loop) is expected to peek the header first via decode_header() to learn
// how many payload bytes to wait for before calling this.
[[nodiscard]] std::variant<Message, DecodeError> decode_message(std::span<const std::byte> data);

} // namespace mdh::protocol::order_entry
