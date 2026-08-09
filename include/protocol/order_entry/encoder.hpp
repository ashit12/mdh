#pragma once

#include <vector>

#include "protocol/order_entry/messages.hpp"

namespace mdh::protocol::order_entry {

// Appends the wire encoding of `message` (header + payload) to `out`. `out`
// is not cleared first, so a caller reusing one buffer across many calls
// (e.g. a connection's outbound write buffer) can clear() it themselves
// between messages and avoid a fresh heap allocation per call -- same
// convention as protocol::encode_event().
void encode_message(const Message& message, std::vector<std::byte>& out);

} // namespace mdh::protocol::order_entry
