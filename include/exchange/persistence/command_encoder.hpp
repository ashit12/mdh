#pragma once

#include <cstddef>
#include <vector>

#include "exchange/core/commands.hpp"

namespace mdh::exchange::persistence {

// Appends the wire encoding of `command` (header + payload) to `out`. `out`
// is not cleared first, so callers that want to reuse one buffer across many
// calls (e.g. CommandJournalWriter) can clear() it themselves between
// commands and avoid a fresh heap allocation per command.
void encode_command(const ExchangeCommand& command, std::vector<std::byte>& out);

} // namespace mdh::exchange::persistence
