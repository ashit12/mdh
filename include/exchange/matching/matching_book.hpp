#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <vector>

#include "common/types.hpp"
#include "exchange/matching/resting_order.hpp"

// The exchange's authoritative, single-instrument matching book (Milestone
// 2). It stores BookOrder (account, remaining quantity, TIF), not
// book::RestingOrder, and exposes the front-of-level operations
// MatchingEngine needs to consume resting liquidity while it decides whether
// an incoming order crosses.
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
// Three structures, none of them the obvious one:
//   - The orders live in one flat per-book slab, and a price level is a pair
//     of indices into it, so a level costs no container of its own.
//   - Prices within a band around the book are indexed by a tick ladder: a
//     flat array plus a hierarchical occupancy bitmap, so finding the touch
//     is a few count-leading-zeros over words that stay in L1 and finding a
//     level is an array index.
//   - Prices outside that band fall back to a std::pmr::map. Exchanges do
//     band prices, but as a published risk control rather than as a side
//     effect of a data structure, so an out-of-band price is stored, not
//     rejected. See bench-results/stage3-ladder-band-decision.txt for the
//     measurements this came from.
namespace mdh::exchange {

class MatchingBook {
public:
    // The widest band a single side will index with a ladder, in ticks. At
    // 8 bytes a level this is 64 KB per side. Measured level distributions
    // put a realistic book's whole occupied range at under a hundred ticks,
    // so this is not a tight fit -- it is chosen as the point where the next
    // step up stops paying for itself.
    static constexpr std::uint32_t kMaxBandTicks = 8'192;

    // Below this a ladder is not worth its fixed cost, and the book runs on
    // the map alone. MatchingEngine hands down a band of zero when its
    // universe is too large for every book to afford one.
    static constexpr std::uint32_t kMinBandTicks = 1'024;

    // The widest band `instrument_count` books can each afford two of within
    // `byte_budget`, rounded down to a power of two, or zero if that is less
    // than kMinBandTicks. A ladder costs instruments x band x 2 sides, which
    // is why the instrument registry had to come first: an engine that could
    // not say how many instruments it traded could not size this.
    [[nodiscard]] static std::uint32_t band_for(std::size_t instrument_count, std::size_t byte_budget);

    // `expected_resting_orders` is roughly how many orders will rest on
    // *this* book at once. The slab is one flat vector, so it grows by
    // relocating -- cheap on average and measurable at the tail, exactly
    // like the engine's own directory table, and cured the same way:
    // measured on a book of a million orders across a thousand levels, an
    // insert costs 47 ns against a slab reserved up front and 104 ns against
    // one that grew into it.
    //
    // `band_ticks` is how wide this book's ladder may be, and must be zero
    // or a power of two in [kMinBandTicks, kMaxBandTicks]. Zero means no
    // ladder at all. MatchingEngine derives both figures, so those are the
    // ones to get right, not these.
    explicit MatchingBook(std::size_t expected_resting_orders = 0, std::uint32_t band_ticks = kMaxBandTicks);

    // Where one resting order sits: an index into this book's slab, and
    // nothing else. Returned by add(), accepted by every operation that acts
    // on an order already on the book, and valid until that order is removed
    // -- which is the caller's own doing, since only remove_at() and
    // remove_front() destroy an order. Opaque: the slot number is meaningful
    // to this class and to nobody else.
    //
    // A handle used after its order has been removed is undefined behaviour
    // in the ordinary C++ sense, but note the shape of it here: slots are
    // recycled, so a stale handle is far more likely to name some *other*
    // live order than to fault. That is not a new hazard -- a dangling list
    // iterator was worse -- but it is why MatchingEngine erases its
    // directory entry and removes from the book in the same breath, every
    // time, with no window in between.
    struct Handle {
        std::uint32_t slot{};
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
    // pointee lives until the next mutation of *either* side -- remove_front()
    // destroys it, and any add() may move the whole slab -- so a caller that
    // needs any of its fields afterwards must read them out first.
    // remove_front() also invalidates that order's Handle, so the caller is
    // responsible for dropping it from its own table.
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
    // already satisfied. Walks the price index in place -- unlike
    // all_bids()/all_asks(), this never materializes a copy of the side
    // being scanned, so its cost is bounded by how many orders it actually
    // needs to look at, not by how many orders are resting on that side.
    [[nodiscard]] Quantity crossable_quantity(Side book_side, Price price, Quantity quantity) const;

    // How many of this book's resting orders sit outside the ladder's band,
    // and so are indexed by the fallback map. Zero is the case the ladder
    // was built for; a book where this keeps pace with the order count is a
    // book whose band is in the wrong place, which is worth being able to
    // see from a benchmark rather than having to infer from a latency.
    [[nodiscard]] std::size_t out_of_band_levels() const;

private:
    // The end-of-chain and empty-level sentinel, and the reason a slot is a
    // std::uint32_t rather than a std::size_t: a book with four billion
    // resting orders in it has a bigger problem than its index width, and
    // halving the links is what keeps a slab entry at 56 bytes.
    static constexpr std::uint32_t kNil = ~0U;

    // One resting order plus its place in a doubly-linked FIFO chain, held
    // by index. This replaces a std::pmr::list<BookOrder> per price level,
    // which cost a 64-byte pooled node per order and a 32-byte list header
    // per level, and reached each order through a pointer chase into pool
    // memory.
    //
    // Indices rather than pointers is what makes the slab a plain vector:
    // growth relocates every order, and a handle -- being an offset, not an
    // address -- survives that untouched. So does a move of the whole book,
    // which MatchingEngine::register_instrument() performs on every book
    // when it grows its own vector.
    struct SlabOrder {
        BookOrder order;
        std::uint32_t next;
        std::uint32_t prev;
    };

    static_assert(sizeof(SlabOrder) == 56, "a slab entry is BookOrder plus two 32-bit links, and 56 bytes of it "
                                            "per resting order is the figure bench_matching_memory reports");

    // A price level, entire: the head and tail of its FIFO chain through the
    // slab. Eight bytes, which is what makes a ladder of them affordable --
    // a band of 8192 is 64 KB.
    struct LevelSlot {
        std::uint32_t head = kNil;
        std::uint32_t tail = kNil;
    };

    // One side's price index: a tick ladder over a band, plus a map for
    // anything outside it. Ordering is by price priority -- descending for
    // bids, ascending for asks -- so "first" always means the touch and
    // "next" always means one level worse, whichever side this is.
    //
    // The ladder array is deliberately left uninitialised. A level's
    // contents mean nothing unless the occupancy bitmap says the tick is
    // occupied, so there is nothing to zero, and the pages of a band that is
    // never touched are never faulted in. That is what makes a wide band
    // cheap for a book that only uses a corner of it.
    class SideIndex {
    public:
        SideIndex(Side side, std::uint32_t band_ticks, std::pmr::memory_resource* resource);

        [[nodiscard]] bool empty() const { return summary_ == 0 && overflow_.empty(); }

        // Creates the level if this price has none. The reference is stable
        // until the next insert on this side.
        [[nodiscard]] LevelSlot& level_for(Price price);
        // Null if no order rests at this price.
        [[nodiscard]] LevelSlot* find_level(Price price);
        void erase_level(Price price);

        // Precondition: !empty().
        [[nodiscard]] Price best_price() const;

        // The next occupied level in priority order, strictly past `price`.
        // Walking a side is best_price() and then this until it runs out.
        [[nodiscard]] std::optional<Price> next_price(Price price) const;

        // Precondition for both: a level rests at this price.
        [[nodiscard]] LevelSlot& level_at(Price price);
        [[nodiscard]] const LevelSlot& level_at(Price price) const;

        [[nodiscard]] std::size_t overflow_levels() const { return overflow_.size(); }

    private:
        // Words in the bottom level of the occupancy bitmap, one bit per
        // tick; words in the middle level, one bit per bottom word; and
        // summary_, one bit per middle word. Three levels index a band of up
        // to 64*64*64 ticks, comfortably past kMaxBandTicks.
        [[nodiscard]] static constexpr std::size_t words_for(std::uint32_t ticks) { return (ticks + 63U) / 64U; }

        [[nodiscard]] bool in_band(Price price) const {
            // Unsigned wrap makes one comparison do both bounds: a price
            // below the base becomes a very large offset.
            return anchored_ && static_cast<std::uint64_t>(price - base_) < band_ticks_;
        }
        [[nodiscard]] std::uint32_t tick_of(Price price) const {
            return static_cast<std::uint32_t>(price - base_);
        }
        [[nodiscard]] Price price_of(std::uint32_t tick) const { return base_ + static_cast<Price>(tick); }

        void anchor(Price price);
        void set_occupied(std::uint32_t tick);
        void clear_occupied(std::uint32_t tick);
        [[nodiscard]] bool is_occupied(std::uint32_t tick) const;
        // Both return band_ticks_ when there is none, so callers test
        // against the band rather than against a sentinel.
        [[nodiscard]] std::uint32_t highest_occupied() const;
        [[nodiscard]] std::uint32_t lowest_occupied() const;
        [[nodiscard]] std::uint32_t highest_occupied_below(std::uint32_t tick) const;
        [[nodiscard]] std::uint32_t lowest_occupied_above(std::uint32_t tick) const;

        Side side_;
        std::uint32_t band_ticks_;
        Price base_ = 0;
        bool anchored_ = false;

        std::unique_ptr<LevelSlot[]> ladder_;
        std::vector<std::uint64_t> occupancy_;
        std::vector<std::uint64_t> mid_summary_;
        std::uint64_t summary_ = 0;

        // Ascending for both sides; bids read it backwards. One type rather
        // than two comparators keeps every merge below written once.
        std::pmr::map<Price, LevelSlot> overflow_;
    };

    // Takes a slot for `order`, reusing a freed one if there is one. The
    // returned slot's links are not yet meaningful; link_back() sets them.
    [[nodiscard]] std::uint32_t acquire_slot(const BookOrder& order);
    void release_slot(std::uint32_t slot);
    void link_back(LevelSlot& level, std::uint32_t slot);
    // Leaves `level` with head == kNil if `slot` was its last order, which
    // is how every caller decides whether to erase the level.
    void unlink(LevelSlot& level, std::uint32_t slot);

    [[nodiscard]] SideIndex& side_of(Side side) { return side == Side::Buy ? bids_ : asks_; }
    [[nodiscard]] const SideIndex& side_of(Side side) const { return side == Side::Buy ? bids_ : asks_; }

    [[nodiscard]] std::vector<BookOrder> all_of(Side side) const;

    std::vector<SlabOrder> slab_;
    // Freed slots, threaded through SlabOrder::next. A book that cancels as
    // fast as it rests therefore reuses a bounded set of slots forever
    // rather than growing the slab, and reuse is LIFO, so the slot handed
    // out is the one most recently touched.
    std::uint32_t free_head_ = kNil;

    // Declared before the containers it serves, and therefore destroyed
    // after them: members are destroyed in reverse declaration order, so
    // this is what guarantees the pool outlives the last node returned to
    // it. Held by pointer because a pool resource is neither copyable nor
    // movable, while MatchingBook must stay default-constructible and
    // movable to live inside MatchingEngine's books_ vector -- indirection
    // keeps the pool object itself at a fixed address, so the resource
    // pointers the containers hold survive a move of the book.
    std::unique_ptr<std::pmr::unsynchronized_pool_resource> pool_;
    SideIndex bids_;
    SideIndex asks_;
};

} // namespace mdh::exchange
