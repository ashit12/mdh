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
    // Both enums look redundant, and neither is worth removing.
    //
    // side repeats which of the book's two sides this order is filed under,
    // but remove_at() reads it back to find that side's own index -- which
    // is precisely what lets a Handle be a bare slot number rather than
    // something carrying a side and a price around.
    //
    // time_in_force is always GTC on anything resting here:
    // rest_remainder_if_applicable() is the only caller of add(), and it
    // turns IOC and FOK away before reaching it. It is kept anyway because
    // dropping it would buy nothing -- the five eight-byte fields above are
    // 40 bytes and this struct aligns to 8, so one trailing byte rounds up
    // to 48 exactly as two do, leaving a slab entry at 56 either way.
    // Against that zero saving, compose() and process_replace() both read it
    // back (the latter carries it onto the replacement order) and
    // state_hash.cpp hashes it, so removing the field would trade a stored
    // fact for an assumption that replay determinism rests on -- one that
    // stops holding the day a resting time in force other than GTC exists.
    Side side;
    TimeInForce time_in_force;

    bool operator==(const BookOrder&) const = default;
};

static_assert(sizeof(BookOrder) <= 48,
              "BookOrder + two list links must fit the pool's 64-byte block; see the comment above");

} // namespace mdh::exchange
