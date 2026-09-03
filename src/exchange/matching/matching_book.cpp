#include "exchange/matching/matching_book.hpp"

#include <bit>
#include <cstddef>
#include <utility>

namespace mdh::exchange {
namespace {

// This book allocates one node size from its pool: 48 bytes for an overflow
// map node (a Price, a two-index LevelSlot, and the tree's own links). Orders
// live in the slab and in-band levels in the ladder, so neither reaches the
// allocator one element at a time.
//
// Sizing this bound is not as direct as "the largest node we allocate".
// pool_options is advisory, and libc++ reads it conservatively: measured on
// this toolchain, it only serves blocks up to a *quarter* of this value from
// its fixed pools, sending anything larger to an adhoc fallback list whose
// deallocate is a linear scan -- which turns a book that cancels as fast as
// it rests into quadratic time. 1024 therefore pools blocks up to 256 bytes,
// well clear of the 48-byte map node, while still leaving genuinely large
// allocations to the upstream allocator where they belong.
// bench_matching_memory is the guard: if a node ever stops being pooled,
// allocations per operation jump straight back up.
constexpr std::size_t kLargestPooledBlock = 1024;

// Chunks grow geometrically up to this many blocks, so early books stay
// small while a deep one converges on roughly one upstream allocation per
// 4096 nodes instead of one per node.
constexpr std::size_t kMaxBlocksPerChunk = 4096;

// Index of the highest and lowest set bit in a non-zero word.
[[nodiscard]] std::uint32_t highest_bit(std::uint64_t word) {
    return 63U - static_cast<std::uint32_t>(std::countl_zero(word));
}
[[nodiscard]] std::uint32_t lowest_bit(std::uint64_t word) {
    return static_cast<std::uint32_t>(std::countr_zero(word));
}

// Every bit strictly below / strictly above `bit` within one word.
[[nodiscard]] std::uint64_t mask_below(std::uint32_t bit) {
    return bit == 0 ? 0ULL : (~0ULL >> (64U - bit));
}
[[nodiscard]] std::uint64_t mask_above(std::uint32_t bit) {
    return bit >= 63U ? 0ULL : (~0ULL << (bit + 1U));
}

} // namespace

// ── SideIndex: a tick ladder over a band, plus a map for the rest ──────────

MatchingBook::SideIndex::SideIndex(Side side, std::uint32_t band_ticks, std::pmr::memory_resource* resource)
    : side_(side), band_ticks_(band_ticks), overflow_(resource) {}

void MatchingBook::SideIndex::anchor(Price price) {
    // Centred, because a book grows in both directions from wherever it
    // starts and there is no reason to guess which way it will go first.
    base_ = price - static_cast<Price>(band_ticks_ / 2);
    anchored_ = true;
    if (ladder_ == nullptr) {
        // Uninitialised on purpose: see the class comment. The bitmap is
        // what makes a level's contents meaningful, and it is zeroed.
        ladder_ = std::make_unique_for_overwrite<LevelSlot[]>(band_ticks_);
        occupancy_.assign(words_for(band_ticks_), 0);
        mid_summary_.assign(words_for(static_cast<std::uint32_t>(occupancy_.size())), 0);
    }
}

void MatchingBook::SideIndex::set_occupied(std::uint32_t tick) {
    const std::uint32_t word = tick >> 6U;
    occupancy_[word] |= 1ULL << (tick & 63U);
    mid_summary_[word >> 6U] |= 1ULL << (word & 63U);
    summary_ |= 1ULL << (word >> 6U);
}

void MatchingBook::SideIndex::clear_occupied(std::uint32_t tick) {
    const std::uint32_t word = tick >> 6U;
    occupancy_[word] &= ~(1ULL << (tick & 63U));
    if (occupancy_[word] != 0) {
        return;
    }
    // A word emptying is what makes the summary above it stale, so the
    // hierarchy is only touched on the rare clear that empties one.
    const std::uint32_t mid = word >> 6U;
    mid_summary_[mid] &= ~(1ULL << (word & 63U));
    if (mid_summary_[mid] == 0) {
        summary_ &= ~(1ULL << mid);
    }
}

bool MatchingBook::SideIndex::is_occupied(std::uint32_t tick) const {
    return (occupancy_[tick >> 6U] & (1ULL << (tick & 63U))) != 0;
}

std::uint32_t MatchingBook::SideIndex::highest_occupied() const {
    if (summary_ == 0) {
        return band_ticks_;
    }
    const std::uint32_t mid = highest_bit(summary_);
    const std::uint32_t word = (mid << 6U) | highest_bit(mid_summary_[mid]);
    return (word << 6U) | highest_bit(occupancy_[word]);
}

std::uint32_t MatchingBook::SideIndex::lowest_occupied() const {
    if (summary_ == 0) {
        return band_ticks_;
    }
    const std::uint32_t mid = lowest_bit(summary_);
    const std::uint32_t word = (mid << 6U) | lowest_bit(mid_summary_[mid]);
    return (word << 6U) | lowest_bit(occupancy_[word]);
}

std::uint32_t MatchingBook::SideIndex::highest_occupied_below(std::uint32_t tick) const {
    if (summary_ == 0 || tick == 0) {
        return band_ticks_;
    }
    // Within the starting word first: the common case when levels are dense
    // is that the answer is a few bits away and no summary is consulted.
    std::uint32_t word = tick >> 6U;
    if (const std::uint64_t rest = occupancy_[word] & mask_below(tick & 63U); rest != 0) {
        return (word << 6U) | highest_bit(rest);
    }
    // Then the next non-empty word below, found through the two summary
    // levels rather than by scanning.
    std::uint32_t mid = word >> 6U;
    if (const std::uint64_t rest = mid_summary_[mid] & mask_below(word & 63U); rest != 0) {
        word = (mid << 6U) | highest_bit(rest);
        return (word << 6U) | highest_bit(occupancy_[word]);
    }
    const std::uint64_t rest = summary_ & mask_below(mid);
    if (rest == 0) {
        return band_ticks_;
    }
    mid = highest_bit(rest);
    word = (mid << 6U) | highest_bit(mid_summary_[mid]);
    return (word << 6U) | highest_bit(occupancy_[word]);
}

std::uint32_t MatchingBook::SideIndex::lowest_occupied_above(std::uint32_t tick) const {
    if (summary_ == 0 || tick + 1U >= band_ticks_) {
        return band_ticks_;
    }
    std::uint32_t word = tick >> 6U;
    if (const std::uint64_t rest = occupancy_[word] & mask_above(tick & 63U); rest != 0) {
        return (word << 6U) | lowest_bit(rest);
    }
    std::uint32_t mid = word >> 6U;
    if (const std::uint64_t rest = mid_summary_[mid] & mask_above(word & 63U); rest != 0) {
        word = (mid << 6U) | lowest_bit(rest);
        return (word << 6U) | lowest_bit(occupancy_[word]);
    }
    const std::uint64_t rest = summary_ & mask_above(mid);
    if (rest == 0) {
        return band_ticks_;
    }
    mid = lowest_bit(rest);
    word = (mid << 6U) | lowest_bit(mid_summary_[mid]);
    return (word << 6U) | lowest_bit(occupancy_[word]);
}

MatchingBook::LevelSlot& MatchingBook::SideIndex::level_for(Price price) {
    // A side with nothing in it has no reason to keep its old base, so an
    // emptied book re-anchors here rather than spending the rest of its life
    // in the overflow map. This is not a sliding band: nothing that is
    // already resting ever moves.
    if (band_ticks_ != 0 && (!anchored_ || empty())) {
        anchor(price);
    }
    if (!in_band(price)) {
        return overflow_[price];
    }
    const std::uint32_t tick = tick_of(price);
    if (!is_occupied(tick)) {
        set_occupied(tick);
        ladder_[tick] = LevelSlot{};
    }
    return ladder_[tick];
}

MatchingBook::LevelSlot* MatchingBook::SideIndex::find_level(Price price) {
    if (in_band(price)) {
        const std::uint32_t tick = tick_of(price);
        return is_occupied(tick) ? &ladder_[tick] : nullptr;
    }
    auto it = overflow_.find(price);
    return it == overflow_.end() ? nullptr : &it->second;
}

void MatchingBook::SideIndex::erase_level(Price price) {
    if (in_band(price)) {
        clear_occupied(tick_of(price));
        return;
    }
    overflow_.erase(price);
}

Price MatchingBook::SideIndex::best_price() const {
    const std::uint32_t tick = side_ == Side::Buy ? highest_occupied() : lowest_occupied();
    if (tick == band_ticks_) {
        // Ladder empty, so the overflow map holds everything there is.
        return side_ == Side::Buy ? std::prev(overflow_.end())->first : overflow_.begin()->first;
    }
    if (overflow_.empty()) {
        return price_of(tick);
    }
    const Price laddered = price_of(tick);
    const Price spilled = side_ == Side::Buy ? std::prev(overflow_.end())->first : overflow_.begin()->first;
    if (side_ == Side::Buy) {
        return laddered > spilled ? laddered : spilled;
    }
    return laddered < spilled ? laddered : spilled;
}

MatchingBook::LevelSlot& MatchingBook::SideIndex::level_at(Price price) {
    return const_cast<LevelSlot&>(std::as_const(*this).level_at(price));
}

const MatchingBook::LevelSlot& MatchingBook::SideIndex::level_at(Price price) const {
    if (in_band(price)) {
        return ladder_[tick_of(price)];
    }
    return overflow_.find(price)->second;
}

std::uint32_t MatchingBook::band_for(std::size_t instrument_count, std::size_t byte_budget) {
    if (instrument_count == 0) {
        return kMaxBandTicks;
    }
    // Both sides of every instrument come out of the same budget, and the
    // result is rounded down to a power of two because the tick-to-word
    // arithmetic in the bitmap assumes one.
    const std::size_t ticks = byte_budget / instrument_count / (2 * sizeof(LevelSlot));
    if (ticks < kMinBandTicks) {
        return 0;
    }
    return static_cast<std::uint32_t>(std::bit_floor(std::min<std::size_t>(ticks, kMaxBandTicks)));
}

std::optional<Price> MatchingBook::SideIndex::next_price(Price price) const {
    // The two indexes are each ordered, so the next level is whichever of
    // their two candidates comes first -- an ordinary two-way merge, done
    // one step at a time because callers stop early.
    std::optional<Price> laddered;
    if (anchored_ && summary_ != 0) {
        // `price` may be outside the band -- the walk alternates between the
        // two indexes -- and then the whole band is either past it or not.
        const Price first = base_;
        const Price last = base_ + static_cast<Price>(band_ticks_) - 1;
        std::uint32_t tick = band_ticks_;
        if (side_ == Side::Buy) {
            tick = price > last ? highest_occupied()
                                : (price <= first ? band_ticks_ : highest_occupied_below(tick_of(price)));
        } else {
            tick = price < first ? lowest_occupied()
                                 : (price >= last ? band_ticks_ : lowest_occupied_above(tick_of(price)));
        }
        if (tick != band_ticks_) {
            laddered = price_of(tick);
        }
    }

    std::optional<Price> spilled;
    if (!overflow_.empty()) {
        if (side_ == Side::Buy) {
            auto it = overflow_.lower_bound(price);
            if (it != overflow_.begin()) {
                spilled = std::prev(it)->first;
            }
        } else {
            auto it = overflow_.upper_bound(price);
            if (it != overflow_.end()) {
                spilled = it->first;
            }
        }
    }

    if (!laddered.has_value()) {
        return spilled;
    }
    if (!spilled.has_value()) {
        return laddered;
    }
    if (side_ == Side::Buy) {
        return *laddered > *spilled ? laddered : spilled;
    }
    return *laddered < *spilled ? laddered : spilled;
}

// ── MatchingBook ───────────────────────────────────────────────────────────

MatchingBook::MatchingBook(std::size_t expected_resting_orders, std::uint32_t band_ticks)
    : pool_(std::make_unique<std::pmr::unsynchronized_pool_resource>(
          std::pmr::pool_options{.max_blocks_per_chunk = kMaxBlocksPerChunk,
                                 .largest_required_pool_block = kLargestPooledBlock})),
      bids_(Side::Buy, band_ticks, pool_.get()),
      asks_(Side::Sell, band_ticks, pool_.get()) {
    slab_.reserve(expected_resting_orders);
}

std::uint32_t MatchingBook::acquire_slot(const BookOrder& order) {
    if (free_head_ != kNil) {
        const std::uint32_t slot = free_head_;
        free_head_ = slab_[slot].next;
        slab_[slot].order = order;
        return slot;
    }
    slab_.push_back(SlabOrder{.order = order, .next = kNil, .prev = kNil});
    slab_capacity_ = slab_.size();
    return static_cast<std::uint32_t>(slab_.size() - 1);
}

void MatchingBook::release_slot(std::uint32_t slot) {
    slab_[slot].next = free_head_;
    free_head_ = slot;
}

void MatchingBook::link_back(LevelSlot& level, std::uint32_t slot) {
    slab_[slot].next = kNil;
    slab_[slot].prev = level.tail;
    if (level.tail == kNil) {
        level.head = slot;
    } else {
        slab_[level.tail].next = slot;
    }
    level.tail = slot;
}

void MatchingBook::unlink(LevelSlot& level, std::uint32_t slot) {
    const std::uint32_t next = slab_[slot].next;
    const std::uint32_t prev = slab_[slot].prev;
    if (prev == kNil) {
        level.head = next;
    } else {
        slab_[prev].next = next;
    }
    if (next == kNil) {
        level.tail = prev;
    } else {
        slab_[next].prev = prev;
    }
}

MatchingBook::Handle MatchingBook::add(const BookOrder& order) {
    // Before the level lookup, so that a slab growth that fails cannot leave
    // a freshly-inserted empty level behind it.
    const std::uint32_t slot = acquire_slot(order);
    link_back(side_of(order.side).level_for(order.price), slot);
    live_count_ += 1;
    return Handle{.slot = slot};
}

BookOrder MatchingBook::remove_at(Handle handle) {
    // The order knows its own side and price, so the handle no longer has to
    // carry them -- which is the whole reason it fits in four bytes.
    const BookOrder removed = slab_[handle.slot].order;
    SideIndex& side = side_of(removed.side);
    LevelSlot& level = *side.find_level(removed.price);
    unlink(level, handle.slot);
    if (level.head == kNil) {
        side.erase_level(removed.price);
    }
    release_slot(handle.slot);
    live_count_ -= 1;
    return removed;
}

void MatchingBook::reduce_at(Handle handle, Quantity new_remaining_quantity) {
    slab_[handle.slot].order.remaining_quantity = new_remaining_quantity;
}

void MatchingBook::set_client_order_id_at(Handle handle, ClientOrderId new_client_order_id) {
    slab_[handle.slot].order.client_order_id = new_client_order_id;
}

const BookOrder& MatchingBook::at(Handle handle) const { return slab_[handle.slot].order; }

std::optional<Price> MatchingBook::best_bid_price() const {
    if (bids_.empty()) {
        return std::nullopt;
    }
    return bids_.best_price();
}

std::optional<Price> MatchingBook::best_ask_price() const {
    if (asks_.empty()) {
        return std::nullopt;
    }
    return asks_.best_price();
}

const BookOrder* MatchingBook::front_of_best(Side book_side) const {
    const SideIndex& side = side_of(book_side);
    if (side.empty()) {
        return nullptr;
    }
    return &slab_[side.level_at(side.best_price()).head].order;
}

void MatchingBook::reduce_front(Side book_side, Quantity new_remaining_quantity) {
    SideIndex& side = side_of(book_side);
    slab_[side.level_at(side.best_price()).head].order.remaining_quantity = new_remaining_quantity;
}

void MatchingBook::remove_front(Side book_side) {
    SideIndex& side = side_of(book_side);
    const Price price = side.best_price();
    LevelSlot& level = side.level_at(price);
    const std::uint32_t head = level.head;
    unlink(level, head);
    release_slot(head);
    live_count_ -= 1;
    if (level.head == kNil) {
        side.erase_level(price);
    }
}

Quantity MatchingBook::crossable_quantity(Side book_side, Price price, Quantity quantity) const {
    const SideIndex& side = side_of(book_side);
    if (side.empty()) {
        return 0;
    }
    const bool bids = book_side == Side::Buy;
    Quantity total = 0;
    for (std::optional<Price> level_price = side.best_price(); level_price.has_value();
         level_price = side.next_price(*level_price)) {
        if (bids ? price > *level_price : price < *level_price) {
            break;
        }
        for (std::uint32_t slot = side.level_at(*level_price).head; slot != kNil; slot = slab_[slot].next) {
            total += slab_[slot].order.remaining_quantity;
            if (total >= quantity) {
                return total;
            }
        }
    }
    return total;
}

std::vector<BookOrder> MatchingBook::all_of(Side book_side) const {
    const SideIndex& side = side_of(book_side);
    std::vector<BookOrder> result;
    if (side.empty()) {
        return result;
    }
    for (std::optional<Price> level_price = side.best_price(); level_price.has_value();
         level_price = side.next_price(*level_price)) {
        for (std::uint32_t slot = side.level_at(*level_price).head; slot != kNil; slot = slab_[slot].next) {
            result.push_back(slab_[slot].order);
        }
    }
    return result;
}

std::vector<BookOrder> MatchingBook::all_bids() const { return all_of(Side::Buy); }

std::vector<BookOrder> MatchingBook::all_asks() const { return all_of(Side::Sell); }

std::size_t MatchingBook::out_of_band_levels() const {
    return bids_.overflow_levels() + asks_.overflow_levels();
}

std::size_t MatchingBook::out_of_band_orders() const {
    std::size_t total = 0;
    for (const SideIndex* side : {&bids_, &asks_}) {
        for (const auto& [price, level] : side->overflow()) {
            for (std::uint32_t slot = level.head; slot != kNil; slot = slab_[slot].next) {
                ++total;
            }
        }
    }
    return total;
}

} // namespace mdh::exchange
