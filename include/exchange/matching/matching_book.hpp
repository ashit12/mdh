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

// The exchange's authoritative order book for one instrument. It holds
// BookOrder (account, remaining quantity, time in force) and exposes the
// front-of-level operations the matching engine needs to consume resting
// liquidity.
//
// It is not an order directory. It knows price-time priority and nothing
// else: it cannot find an order by id, and it does not know its own
// instrument. The engine owns the single table mapping an order's identity
// to where it rests, and hands that location back here as a Handle. There
// used to be two such tables -- one per book keyed by exchange order id,
// plus the engine's own -- so every cancel and replace paid two dependent
// hash lookups to reach one list node, and every resting order carried two
// hash nodes that rehashed independently.
//
// Three structures, none of them the obvious one:
//   - Orders live in one flat per-book slab, and a price level is just a
//     pair of indices into it, so a level needs no container of its own.
//   - Prices within a band around the book are indexed by a tick ladder: a
//     flat array plus a hierarchical occupancy bitmap, so finding the best
//     price is a few count-leading-zeros over words that stay in L1, and
//     finding a level is an array index.
//   - Prices outside that band fall back to a std::pmr::map. Real exchanges
//     do band prices, but as a published risk control rather than as a side
//     effect of a data structure, so an out-of-band price is stored here,
//     not rejected. See bench-results/stage3-ladder-band-decision.txt for
//     the measurements behind the band width.
namespace mdh::exchange {

class MatchingBook {
public:
    // The widest band one side will index with a ladder, in ticks. At 8
    // bytes a level that is 64 KB per side. Measured distributions put a
    // realistic book's entire occupied range under a hundred ticks, so this
    // is not a tight fit -- it is the point where the next step up stops
    // paying for itself.
    static constexpr std::uint32_t kMaxBandTicks = 8'192;

    // Below this a ladder is not worth its fixed cost and the book runs on
    // the map alone. The engine passes a band of zero when its universe is
    // too large for every book to afford one.
    static constexpr std::uint32_t kMinBandTicks = 1'024;

    // The widest band `instrument_count` books can each afford two of within
    // `byte_budget`, rounded down to a power of two, or zero if that comes
    // out below kMinBandTicks. A ladder costs instruments x band x 2 sides,
    // which is why the instrument registry had to come first: an engine that
    // could not say how many instruments it traded could not size this.
    [[nodiscard]] static std::uint32_t band_for(std::size_t instrument_count, std::size_t byte_budget);

    // `expected_resting_orders` is roughly how many orders will rest on this
    // one book at once. The slab is a flat vector, so it grows by
    // relocating: cheap on average, expensive at the tail. Measured on a
    // book of a million orders across a thousand levels, an insert costs
    // 47 ns against a slab reserved up front and 104 ns against one growing
    // into itself.
    //
    // `band_ticks` is how wide this book's ladder may be: zero, or a power
    // of two between kMinBandTicks and kMaxBandTicks. Zero means no ladder.
    // The engine works out both numbers, so those are the ones to get right.
    explicit MatchingBook(std::size_t expected_resting_orders = 0, std::uint32_t band_ticks = kMaxBandTicks);

    // Where one resting order sits: an index into this book's slab, nothing
    // more. Returned by add(), accepted by every operation on an order
    // already resting, and valid until that order is removed -- which only
    // the caller can cause, since only remove_at() and remove_front()
    // destroy an order. The slot number means nothing outside this class.
    //
    // Using a handle after its order is gone is undefined behaviour, and
    // note the shape of it: slots are recycled, so a stale handle is far
    // more likely to name some *other* live order than to fault. That is why
    // the engine erases its directory entry and removes from the book in the
    // same breath, with no window in between.
    struct Handle {
        std::uint32_t slot{};
    };

    // Inserts at the back of its price level's FIFO queue (i.e. as the
    // lowest-priority order at that price).
    Handle add(const BookOrder& order);

    // Removes the order this handle locates, from any level and any position
    // in its queue, and returns it -- cancel and the priority-losing replace
    // path both need its fields to announce the removal. The handle is dead
    // afterwards.
    BookOrder remove_at(Handle handle);

    // Edits in place without changing queue position, for the
    // priority-preserving replace path: the order keeps its
    // exchange_order_id and its place in the queue but answers to a new
    // client_order_id afterwards. A successful replace always retires the
    // original client_order_id, even when priority survives.
    void reduce_at(Handle handle, Quantity new_remaining_quantity);
    void set_client_order_id_at(Handle handle, ClientOrderId new_client_order_id);

    [[nodiscard]] const BookOrder& at(Handle handle) const;

    [[nodiscard]] std::optional<Price> best_bid_price() const;
    [[nodiscard]] std::optional<Price> best_ask_price() const;

    // The matching walk only ever touches the first order in the queue at
    // the best price on one side. `book_side` picks the side (Buy -> bids,
    // Sell -> asks), matching the resting orders' own .side.
    //
    // Returns a pointer into the book, not a copy: the matching loop calls
    // this once per fill and reads only a few fields, so returning by value
    // copied the whole order every iteration. nullptr means that side is
    // empty. The pointee dies at the next change to *either* side --
    // remove_front() destroys it, and any add() may relocate the slab -- so
    // read out anything needed afterwards first. remove_front() also
    // invalidates that order's Handle, which the caller must drop from its
    // own table.
    [[nodiscard]] const BookOrder* front_of_best(Side book_side) const;
    void reduce_front(Side book_side, Quantity new_remaining_quantity);
    void remove_front(Side book_side);

    // Every resting order, by price priority then queue position, for
    // snapshots and for tests asserting exact book state. These are the
    // orders as the book holds them; the engine puts back the fields only it
    // knows about.
    [[nodiscard]] std::vector<BookOrder> all_bids() const;
    [[nodiscard]] std::vector<BookOrder> all_asks() const;

    // FOK's all-or-nothing pre-check: sums resting quantity on `book_side`
    // that would immediately cross at `price` or better, stopping at the
    // first non-crossing level or as soon as `quantity` is covered. Walks
    // the price index in place, so unlike all_bids()/all_asks() it copies
    // nothing and costs only what it actually looks at.
    [[nodiscard]] Quantity crossable_quantity(Side book_side, Price price, Quantity quantity) const;

    // How many of this book's price levels sit outside the ladder's band and
    // so live in the fallback map. Zero is the case the ladder was built
    // for; a book where this keeps pace with the level count has its band in
    // the wrong place, which is worth seeing in a benchmark rather than
    // inferring from a latency.
    [[nodiscard]] std::size_t out_of_band_levels() const;

    // Live resting orders on this book, and how many slab slots have ever
    // been allocated (including those on the free list). Matching thread only.
    [[nodiscard]] std::size_t live_order_count() const { return live_count_; }
    [[nodiscard]] std::size_t slab_capacity() const { return slab_capacity_; }

private:
    // End-of-chain and empty-level sentinel. Slots are 32-bit, not
    // size_t: a book with four billion resting orders has a bigger problem
    // than its index width, and halving the links keeps a slab entry at 56
    // bytes.
    static constexpr std::uint32_t kNil = ~0U;

    // One resting order plus its place in a doubly-linked queue, held by
    // index. This replaced a std::pmr::list per price level, which cost a
    // 64-byte pooled node per order plus a 32-byte list header per level and
    // reached each order by chasing a pointer into pool memory.
    //
    // Indices rather than pointers are what let the slab be a plain vector:
    // growth relocates every order, and a handle is an offset, so it
    // survives untouched. So does moving the whole book, which happens to
    // every book when the engine grows its own vector.
    struct SlabOrder {
        BookOrder order;
        std::uint32_t next;
        std::uint32_t prev;
    };

    static_assert(sizeof(SlabOrder) == 56, "a slab entry is BookOrder plus two 32-bit links, and 56 bytes of it "
                                            "per resting order is the figure bench_matching_memory reports");

    // A whole price level: the head and tail of its queue through the slab.
    // Eight bytes, which is what makes a ladder of them affordable -- a band
    // of 8192 is 64 KB.
    struct LevelSlot {
        std::uint32_t head = kNil;
        std::uint32_t tail = kNil;
    };

    // One side's price index: a tick ladder over a band, plus a map for
    // anything outside it. Ordering is by price priority -- descending for
    // bids, ascending for asks -- so "first" always means the best price and
    // "next" always means one level worse, whichever side this is.
    //
    // The ladder array is deliberately left uninitialised. A level's
    // contents mean nothing unless the occupancy bitmap says its tick is
    // occupied, so there is nothing to zero, and pages of a band nobody
    // touches are never faulted in. That is what makes a wide band cheap for
    // a book using only a corner of it.
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
        // The bitmap has three levels: one bit per tick at the bottom, one
        // bit per bottom word in the middle, and one bit per middle word in
        // summary_. That covers up to 64*64*64 ticks, comfortably past
        // kMaxBandTicks.
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

        // Ascending on both sides; bids read it backwards. One type instead
        // of two comparators means each merge below is written once.
        std::pmr::map<Price, LevelSlot> overflow_;
    };

    // Takes a slot for `order`, reusing a freed one if there is any. The
    // slot's links are not meaningful until link_back() sets them.
    [[nodiscard]] std::uint32_t acquire_slot(const BookOrder& order);
    void release_slot(std::uint32_t slot);
    void link_back(LevelSlot& level, std::uint32_t slot);
    // Leaves `level` with head == kNil if `slot` was its last order, which
    // is how callers decide whether to erase the level.
    void unlink(LevelSlot& level, std::uint32_t slot);

    [[nodiscard]] SideIndex& side_of(Side side) { return side == Side::Buy ? bids_ : asks_; }
    [[nodiscard]] const SideIndex& side_of(Side side) const { return side == Side::Buy ? bids_ : asks_; }

    [[nodiscard]] std::vector<BookOrder> all_of(Side side) const;

    std::vector<SlabOrder> slab_;
    std::size_t live_count_ = 0;
    std::size_t slab_capacity_ = 0;
    // Freed slots, threaded through SlabOrder::next. A book that cancels as
    // fast as it rests reuses a bounded set of slots forever instead of
    // growing the slab, and reuse is last-in-first-out, so the slot handed
    // out is the one most recently touched and most likely still cached.
    std::uint32_t free_head_ = kNil;

    // Declared before the containers it serves, so it is destroyed after
    // them (members go in reverse declaration order) and outlives the last
    // node returned to it. Held by pointer because a pool resource is
    // neither copyable nor movable, while a MatchingBook must be movable to
    // live in the engine's books_ vector -- the indirection keeps the pool
    // at a fixed address, so the resource pointers inside the containers
    // survive a move of the book.
    std::unique_ptr<std::pmr::unsynchronized_pool_resource> pool_;
    SideIndex bids_;
    SideIndex asks_;
};

} // namespace mdh::exchange
