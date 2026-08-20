#pragma once

#include <vector>

#include "common/types.hpp"
#include "exchange/matching/resting_order.hpp"

// A canonical view of an engine's entire resting-order state, used to prove
// that replaying the same command journal twice ends in the same state --
// not just that it emitted the same events on the way there.
//
// "Canonical" means the order never depends on hash-table iteration, which
// is not guaranteed to match between two separately-built engines even given
// identical input. Instruments are sorted by ascending id, and each side's
// orders come out in the book's own price-then-queue order, which is already
// deterministic. An instrument with nothing resting is left out entirely
// rather than reported as an empty pair of vectors.
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
