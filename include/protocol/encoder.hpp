#pragma once

#include <vector>

#include "protocol/messages.hpp"

namespace mdh::protocol {

// Appends the wire encoding of `event` (header + payload) to `out`. `out` is
// not cleared first, so callers that want to reuse one buffer across many
// calls (e.g. a file writer) can clear() it themselves between events and
// avoid a fresh heap allocation per message.
void encode_event(const Event& event, std::vector<std::byte>& out);

} // namespace mdh::protocol
