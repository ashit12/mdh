#pragma once

#include <cstdint>

#include "common/types.hpp"
#include "exchange/core/types.hpp"

// The exchange's own resting-order representation (Milestone 2) -- deliberately
// not book::RestingOrder (include/book/price_level.hpp), which is a thin
// {id, quantity} pair sufficient for a book that only ever replays already-
// decided add/cancel/modify facts. The matcher instead has to decide whether
// an order crosses, track whose order it is for private events, and know
// its own time-in-force to know whether it may rest at all -- none of which
// the trader-side reconstruction book needs. Reusing one type for both would
// couple "record of a fact" to "adjudicator of a decision" (see
// docs/end_to_end_architecture.md section 5).
namespace mdh::exchange {

struct ExchangeRestingOrder {
    ExchangeOrderId exchange_order_id;
    ClientOrderId client_order_id;
    AccountId account_id;
    InstrumentId instrument_id;
    Side side;
    Price price;
    Quantity original_quantity;
    Quantity remaining_quantity;
    TimeInForce time_in_force;
    // Assignment-order marker for observability/testing only. The order's
    // actual matching priority is its position in MatchingBook's per-level
    // FIFO list; this field never drives comparisons, it just lets a test or
    // a future journal dump confirm *when* (in engine-assigned order) a
    // still-resting order was accepted onto the book.
    std::uint64_t order_sequence;

    bool operator==(const ExchangeRestingOrder&) const = default;
};

} // namespace mdh::exchange
