#pragma once

#include <cstdint>

#include "exchange/matching/state_snapshot.hpp"

// A single 64-bit fingerprint of an engine state snapshot --
// convenient for a one-line "did final state match" assertion or a log
// line, on top of (not instead of) directly comparing two
// EngineStateSnapshot values with ==. Deterministic for the same reason the
// snapshot itself is: the byte encoding below only ever depends on
// snapshot's own already-canonically-ordered contents, never on iteration
// order of any unordered container.
namespace mdh::exchange::persistence {

[[nodiscard]] std::uint64_t hash_state_snapshot(const EngineStateSnapshot& snapshot);

} // namespace mdh::exchange::persistence
