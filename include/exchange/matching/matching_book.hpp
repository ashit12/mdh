#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/types.hpp"
#include "exchange/matching/resting_order.hpp"

// The exchange's authoritative, single-instrument matching book (Milestone
// 2). Structurally similar to book::OrderBook (include/book/order_book.hpp)
// -- std::map<Price, ...> per side for ordered levels, std::list per level
// for FIFO, std::unordered_map for O(1) lookup by id -- but this is a
// deliberately separate class: it stores ExchangeRestingOrder (account,
// remaining quantity, TIF), not book::RestingOrder, and exposes the
// front-of-level operations MatchingEngine needs to consume resting
// liquidity while it decides whether an incoming order crosses.
//
// The containers are the std::pmr flavours of exactly those types, drawing
// their nodes from book-local pools (see the constructor). The baseline
// measured one ::operator new per node -- a list node, an index node, and on
// a new price level a map node, per resting order -- which the pools replace
// with occasional bulk chunk allocations they then carve up and recycle.
// That is a change of node provenance only: no intrusive lists, no
// hand-rolled slab, and not one line of matching logic differs.
//
// There are two pools rather than one because a pool hands out blocks by
// size class, and which nodes share a class decides how densely they are
// packed. Once a list node shrank to 64 bytes it landed in the same class as
// an index node, halving the density of both and measurably slowing the
// lookup-heavy paths; separating them put each back in a region of its own.
//
// What the levels hold is BookOrder, not ExchangeRestingOrder: the three
// fields only the snapshot ever reads live in the book itself
// (instrument_id) or in the index entry that already exists per order
// (original_quantity, order_sequence), which is what buys the list node its
// 64-byte block. The full order is reassembled by find(), remove() and
// all_bids()/all_asks(); nothing on a runtime path needs one.
namespace mdh::exchange {

class MatchingBook {
public:
    // One book holds one instrument, and remembers which: it is the only
    // party that still knows the instrument id of the orders resting in it,
    // since they no longer carry it themselves.
    explicit MatchingBook(InstrumentId instrument_id);

    // Inserts at the back of its price level's FIFO queue (i.e. as the
    // lowest-priority order at that price). Caller is responsible for
    // ensuring order.exchange_order_id is not already present, and that
    // order.instrument_id is this book's -- the field is not stored, so a
    // mismatch is not detected here and would surface as a wrong
    // instrument id coming back out of all_bids().
    void add(const ExchangeRestingOrder& order);

    // Removes and returns the order with this id from wherever it rests
    // (any level, any FIFO position) -- used by Cancel and by Replace's
    // priority-preserving path. Returns std::nullopt if not found.
    // Reassembles a whole ExchangeRestingOrder from the level and the index
    // entry, both of which this has in hand anyway.
    std::optional<ExchangeRestingOrder> remove(ExchangeOrderId id);

    // In-place quantity mutation that does not change FIFO position --
    // used by Replace's priority-preserving quantity-decrease path. Returns
    // false if the id is not found.
    bool reduce(ExchangeOrderId id, Quantity new_remaining_quantity);

    // In-place client_order_id mutation, used alongside reduce() by
    // Replace's priority-preserving path: the resting order keeps its
    // exchange_order_id and FIFO position, but becomes addressable under a
    // new client_order_id (see ReplaceOrderCommand's doc comment -- a
    // successful replace always retires original_client_order_id in favor
    // of new_client_order_id, even when priority itself is preserved).
    // Returns false if the id is not found.
    bool set_client_order_id(ExchangeOrderId id, ClientOrderId new_client_order_id);

    [[nodiscard]] std::optional<ExchangeRestingOrder> find(ExchangeOrderId id) const;

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
    // fields after mutating must read them out first.
    [[nodiscard]] const BookOrder* front_of_best(Side book_side) const;
    void reduce_front(Side book_side, Quantity new_remaining_quantity);
    void remove_front(Side book_side);

    // Every resting order, price-priority-then-FIFO order, for snapshots and
    // for tests asserting exact final book state. Costs one index lookup per
    // order to put the snapshot-only fields back; this is the cold path that
    // pays for the hot one, and it already copies the entire book.
    [[nodiscard]] std::vector<ExchangeRestingOrder> all_bids() const;
    [[nodiscard]] std::vector<ExchangeRestingOrder> all_asks() const;

    // FOK's all-or-none pre-check: sums remaining quantity on `book_side`
    // that would immediately cross at `price` or better, stopping as soon
    // as either the first non-crossing level is reached or `quantity` is
    // already satisfied. Walks bids_/asks_ in place -- unlike all_bids()/
    // all_asks(), this never materializes a copy of the side being
    // scanned, so its cost is bounded by how many orders it actually needs
    // to look at, not by how many orders are resting on that side.
    [[nodiscard]] Quantity crossable_quantity(Side book_side, Price price, Quantity quantity) const;

private:
    using Level = std::pmr::list<BookOrder>;
    using BidMap = std::pmr::map<Price, Level, std::greater<Price>>;
    using AskMap = std::pmr::map<Price, Level>;

    // Where an order rests, plus the two fields BookOrder gave up. Those
    // fields ride here for free: at 24 bytes this entry already sat in a
    // 48-byte hash node inside a 64-byte pool block, and 40 bytes still
    // does -- the sixteen bytes were being wasted as slack. There are seven
    // bytes of that slack left, so anything added here from now on must be
    // measured, not assumed: crossing 40 bytes moves the index node into the
    // 128-byte class and gives back everything the split won.
    struct Location {
        Price price;
        Level::iterator it;
        Quantity original_quantity;
        std::uint64_t order_sequence;
        Side side;
    };

    static_assert(sizeof(Location) <= 40, "Location + its key must fit the pool's 64-byte index-node block");

    using Index = std::pmr::unordered_map<ExchangeOrderId, Location>;

    void erase_at(const Location& loc);

    // Puts a level entry and its index entry back together into the whole
    // order the cold paths hand out.
    [[nodiscard]] ExchangeRestingOrder compose(const BookOrder& order, const Location& loc) const;

    // Declared before the containers it serves, and therefore destroyed
    // after them: members are destroyed in reverse declaration order, so
    // this is what guarantees the pool outlives the last node returned to
    // it. Held by pointer because a pool resource is neither copyable nor
    // movable, while MatchingBook must stay default-constructible and
    // movable to live inside MatchingEngine's books_ map -- indirection
    // keeps the pool object itself at a fixed address, so the resource
    // pointers the containers hold survive a move of the book.
    std::unique_ptr<std::pmr::unsynchronized_pool_resource> level_pool_;
    std::unique_ptr<std::pmr::unsynchronized_pool_resource> index_pool_;
    BidMap bids_;
    AskMap asks_;
    Index index_;

    // Stored once here instead of once per resting order, which is the
    // whole of its cost: it is a constant for the lifetime of the book.
    InstrumentId instrument_id_;
};

} // namespace mdh::exchange
