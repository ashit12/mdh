#pragma once

#include <vector>

#include "common/types.hpp"
#include "exchange/matching/resting_order.hpp"

// A canonical, deterministically-ordered view of a MatchingEngine's entire
// resting-order state (Milestone 3), used to prove that replaying the same
// command journal twice ends in the same authoritative state -- not just
// that it emits the same events along the way. "Canonical" specifically
// means: independent of std::unordered_map's iteration order (which is not
// guaranteed to match between two separately-constructed MatchingEngine
// instances even given identical inputs) -- instruments are sorted
// ascending by id, and each side's orders are taken from MatchingBook's own
// price-priority-then-FIFO order, which is already deterministic. An
// instrument with no resting orders on either side (e.g. every order on it
// was cancelled or fully filled) is omitted entirely, rather than reported
// as an empty pair of vectors -- an empty book carries no state worth
// comparing.
namespace mdh::exchange {

struct InstrumentBookSnapshot {
    InstrumentId instrument_id;
    std::vector<ExchangeRestingOrder> bids; // best-to-worst price, FIFO within a level
    std::vector<ExchangeRestingOrder> asks; // best-to-worst price, FIFO within a level

    bool operator==(const InstrumentBookSnapshot&) const = default;
};

struct EngineStateSnapshot {
    std::vector<InstrumentBookSnapshot> instruments; // sorted ascending by instrument_id

    bool operator==(const EngineStateSnapshot&) const = default;
};

} // namespace mdh::exchange
