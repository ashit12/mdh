#pragma once

#include <list>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <vector>

#include "common/types.hpp"
#include "exchange/matching/resting_order.hpp"

// The exchange's authoritative, single-instrument matching book (Milestone
// 2). Structurally similar to book::OrderBook (include/book/order_book.hpp)
// -- std::map<Price, ...> per side for ordered levels, std::list per level
// for FIFO -- but this is a deliberately separate class: it stores
// BookOrder (account, remaining quantity, TIF), not book::RestingOrder, and
// exposes the front-of-level operations MatchingEngine needs to consume
// resting liquidity while it decides whether an incoming order crosses.
//
// What this book is *not* is an order directory. It knows price-time
// priority and nothing else: it cannot find an order by id, and it does not
// know its own instrument. MatchingEngine owns the one table that maps an
// order's identity to where it rests, and hands that location back here as a
// Handle. The reason is that there used to be two such tables -- an index in
// each book keyed by exchange order id, plus a live-order map in the engine
// keyed by (account, client order id) -- so every cancel and every replace
// paid two dependent hash lookups to get to one list node, and every resting
// order carried two hash nodes that rehashed independently as the book grew.
//
// The containers are the std::pmr flavours of those types, drawing their
// nodes from a book-local pool instead of from ::operator new one node at a
// time -- a change of node provenance only, with no effect on the structure
// or the matching logic.
namespace mdh::exchange {

class MatchingBook {
public:
    MatchingBook();

    // Public only because a Handle has to name it. Treat a Level, and any
    // iterator into one, as the book's business.
    using Level = std::pmr::list<BookOrder>;

    // Where one resting order sits. Returned by add(), accepted by every
    // operation that acts on an order already on the book, and valid until
    // that order is removed -- which is the caller's own doing, since only
    // remove_at() and remove_front() destroy an order. Opaque: the fields
    // are meaningful to this class and to nobody else.
    struct Handle {
        Side side{};
        Price price{};
        Level::iterator it{};
    };

    // Inserts at the back of its price level's FIFO queue (i.e. as the
    // lowest-priority order at that price).
    Handle add(const BookOrder& order);

    // Removes the order this handle locates, from wherever it rests (any
    // level, any FIFO position), and returns it -- used by Cancel and by
    // Replace's priority-losing path, both of which need the order's fields
    // to announce its removal. The handle is dead afterwards.
    BookOrder remove_at(Handle handle);

    // In-place mutations that do not change FIFO position, used by Replace's
    // priority-preserving path: the resting order keeps its
    // exchange_order_id and its place in the queue, but becomes addressable
    // under a new client_order_id (see ReplaceOrderCommand's doc comment --
    // a successful replace always retires original_client_order_id in favor
    // of new_client_order_id, even when priority itself is preserved).
    void reduce_at(Handle handle, Quantity new_remaining_quantity);
    void set_client_order_id_at(Handle handle, ClientOrderId new_client_order_id);

    [[nodiscard]] const BookOrder& at(Handle handle) const;

    [[nodiscard]] std::optional<Price> best_bid_price() const;
    [[nodiscard]] std::optional<Price> best_ask_price() const;

    // The matching walk only ever needs to look at, reduce, or remove the
    // earliest-priority (front-of-FIFO) order at the best price level on
    // one side of the book -- book_side selects which side (Side::Buy ->
    // bids, Side::Sell -> asks), matching the resting orders' own .side.
    //
    // Returns a pointer into the book rather than a copy, because the
    // matching loop calls this once per fill and only reads a handful of
    // fields from it: handing back an order by value copied the whole thing
    // every iteration to do it. nullptr means that side is empty. The
    // pointee lives until the next mutation of that side -- in particular
    // remove_front() destroys it -- so a caller that needs any of its
    // fields after mutating must read them out first. remove_front() also
    // invalidates that order's Handle, so the caller is responsible for
    // dropping it from its own table.
    [[nodiscard]] const BookOrder* front_of_best(Side book_side) const;
    void reduce_front(Side book_side, Quantity new_remaining_quantity);
    void remove_front(Side book_side);

    // Every resting order, price-priority-then-FIFO order, for snapshots and
    // for tests asserting exact final book state. These are the orders as
    // the book holds them; putting the snapshot-only fields back is the
    // engine's job, since the engine is where they live.
    [[nodiscard]] std::vector<BookOrder> all_bids() const;
    [[nodiscard]] std::vector<BookOrder> all_asks() const;

    // FOK's all-or-none pre-check: sums resting quantity on `book_side`
    // that would immediately cross at `price` or better, stopping as soon
    // as either the first non-crossing level is reached or `quantity` is
    // already satisfied. Walks bids_/asks_ in place -- unlike all_bids()/
    // all_asks(), this never materializes a copy of the side being
    // scanned, so its cost is bounded by how many orders it actually needs
    // to look at, not by how many orders are resting on that side.
    [[nodiscard]] Quantity crossable_quantity(Side book_side, Price price, Quantity quantity) const;

private:
    using BidMap = std::pmr::map<Price, Level, std::greater<Price>>;
    using AskMap = std::pmr::map<Price, Level>;

    // Declared before the containers it serves, and therefore destroyed
    // after them: members are destroyed in reverse declaration order, so
    // this is what guarantees the pool outlives the last node returned to
    // it. Held by pointer because a pool resource is neither copyable nor
    // movable, while MatchingBook must stay default-constructible and
    // movable to live inside MatchingEngine's books_ map -- indirection
    // keeps the pool object itself at a fixed address, so the resource
    // pointers the containers hold survive a move of the book.
    std::unique_ptr<std::pmr::unsynchronized_pool_resource> pool_;
    BidMap bids_;
    AskMap asks_;
};

} // namespace mdh::exchange
