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

// The complete, interchange form of a resting order: what a command turns
// into on its way to the book, what snapshots and hash_state_snapshot() are
// made of, and what MatchingBook reassembles on the paths that ask for a
// whole order. It is deliberately *not* what the book stores -- see
// BookOrder below.
//
// Fields are ordered widest-first, which is not how they group by meaning --
// it is how the struct packs into 64 bytes instead of 72. Grouped naturally
// (ids, then instrument and side, then price and quantities) the four-byte
// instrument id and the two small enums end up separated by eight-byte
// fields, and the compiler pads around each of them. Nothing here depends on
// declaration order except designated initializers, which C++20 requires to
// be written in this same order at every construction site.
struct ExchangeRestingOrder {
    ExchangeOrderId exchange_order_id;
    ClientOrderId client_order_id;
    AccountId account_id;
    Price price;
    Quantity original_quantity;
    Quantity remaining_quantity;
    // Assignment-order marker for observability/testing only. The order's
    // actual matching priority is its position in MatchingBook's per-level
    // FIFO list; this field never drives comparisons, it just lets a test or
    // a future journal dump confirm *when* (in engine-assigned order) a
    // still-resting order was accepted onto the book.
    std::uint64_t order_sequence;
    InstrumentId instrument_id;
    Side side;
    TimeInForce time_in_force;

    bool operator==(const ExchangeRestingOrder&) const = default;
};

// The form an order takes while it is resting on the book: every field the
// engine reads at runtime, and nothing else.
//
// The matching loop reads five of these per fill (the three ids, price,
// remaining quantity); cancel and replace additionally read side and
// time-in-force. The three fields of ExchangeRestingOrder missing here --
// instrument_id, original_quantity, order_sequence -- are read by nothing
// but snapshot() and hash_state_snapshot(), so MatchingBook holds them
// elsewhere (its own instrument id, and the per-order index entry it already
// keeps) and reassembles the full order on those cold paths. No runtime path
// pays a second lookup for the privilege.
//
// Staying at or under 48 bytes is the entire point of the split. A
// std::pmr::list node adds two links to whatever it holds, and 48 + 16 is
// exactly the largest node the book's pool serves out of its 64-byte blocks;
// at 49 bytes the node moves up to the 128-byte class and the book's
// per-order footprint doubles. The static_assert is therefore a real
// constraint, not a tidiness check.
struct BookOrder {
    ExchangeOrderId exchange_order_id;
    ClientOrderId client_order_id;
    AccountId account_id;
    Price price;
    Quantity remaining_quantity;
    // Both enums are redundant in principle -- side repeats which of the
    // book's two maps this order is filed under, and the matching loop
    // already knows the contra side it asked for -- but they are free: the
    // five eight-byte fields above would pad out to 48 with or without
    // them. Carrying them spares every caller from being told out-of-band
    // what the order itself can say.
    Side side;
    TimeInForce time_in_force;

    bool operator==(const BookOrder&) const = default;
};

static_assert(sizeof(BookOrder) <= 48,
              "BookOrder + two list links must fit the pool's 64-byte block; see the comment above");

} // namespace mdh::exchange
