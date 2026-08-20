#pragma once

#include <cstdint>

#include "common/types.hpp"
#include "exchange/core/types.hpp"

// The exchange's own resting-order type. It is deliberately not
// book::RestingOrder (include/book/price_level.hpp), which is a thin
// {id, quantity} pair -- enough for a book that only replays decisions
// someone else already made. The matcher has to decide whether an order
// crosses, know whose order it is so private events can be addressed, and
// know its time in force to know whether it may rest at all. One shared
// type would tie "record of a fact" to "maker of a decision"; see
// docs/end_to_end_architecture.md section 5.
namespace mdh::exchange {

// The complete form of a resting order: what a command becomes on its way to
// the book, what snapshots and state hashes are made of, and what the book
// reassembles when something asks for a whole order. It is not what the book
// stores while the order rests -- see BookOrder below.
//
// Fields are widest-first, which is not how they group by meaning. That is
// how the struct packs into 64 bytes instead of 72: grouped naturally, the
// four-byte instrument id and the two small enums end up between eight-byte
// fields and the compiler pads around each. Nothing depends on declaration
// order except designated initializers, which C++20 requires to be written
// in this same order everywhere.
struct ExchangeRestingOrder {
    ExchangeOrderId exchange_order_id;
    ClientOrderId client_order_id;
    AccountId account_id;
    Price price;
    Quantity original_quantity;
    Quantity remaining_quantity;
    // Records the order in which the engine accepted this order, for tests
    // and dumps only. Real matching priority is its position in the book's
    // per-level queue; nothing compares this field.
    std::uint64_t order_sequence;
    InstrumentId instrument_id;
    Side side;
    TimeInForce time_in_force;

    bool operator==(const ExchangeRestingOrder&) const = default;
};

// What an order looks like while it rests: every field the engine reads at
// runtime, and nothing else.
//
// The matching loop reads five of them per fill (three ids, price, remaining
// quantity); cancel and replace also read side and time in force. The three
// fields left out -- instrument_id, original_quantity, order_sequence -- are
// read by nothing but snapshots and state hashes, so they are kept elsewhere
// and put back only on those cold paths. No runtime path pays for them.
//
// Staying at or under 48 bytes is the whole point of the split, and it is a
// real constraint: a slab entry is this plus two 32-bit links, so a byte
// added here is a byte per resting order and one fewer order per cache line.
struct BookOrder {
    ExchangeOrderId exchange_order_id;
    ClientOrderId client_order_id;
    AccountId account_id;
    Price price;
    Quantity remaining_quantity;
    // Both enums are redundant in principle -- side repeats which of the
    // book's two sides this order is filed under -- but they are free: the
    // five eight-byte fields above pad out to 48 with or without them.
    // Carrying them saves every caller from being told out of band what the
    // order can say for itself.
    Side side;
    TimeInForce time_in_force;

    bool operator==(const BookOrder&) const = default;
};

static_assert(sizeof(BookOrder) <= 48,
              "BookOrder + two list links must fit the pool's 64-byte block; see the comment above");

} // namespace mdh::exchange
