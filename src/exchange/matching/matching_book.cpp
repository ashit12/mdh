#include "exchange/matching/matching_book.hpp"

#include <cstddef>

namespace mdh::exchange {
namespace {

// This book allocates two node sizes: 64 bytes for a list node (a 48-byte
// BookOrder plus two links) and 72 for a map node.
//
// Sizing this bound is not as direct as "the largest node we allocate".
// pool_options is advisory, and libc++ reads it conservatively: measured on
// this toolchain, it only serves blocks up to a *quarter* of this value from
// its fixed pools, sending anything larger to an adhoc fallback list whose
// deallocate is a linear scan -- which turns a book that cancels as fast as
// it rests into quadratic time. 1024 therefore pools blocks up to 256 bytes,
// well clear of the 72-byte largest node, while still leaving genuinely
// large allocations to the upstream allocator where they belong.
// bench_matching_memory is the guard: if a node ever stops being pooled,
// allocations per operation jump straight back up.
constexpr std::size_t kLargestPooledBlock = 1024;

// Chunks grow geometrically up to this many blocks, so early books stay
// small while a deep one converges on roughly one upstream allocation per
// 4096 nodes instead of one per node.
constexpr std::size_t kMaxBlocksPerChunk = 4096;

} // namespace

MatchingBook::MatchingBook()
    : pool_(std::make_unique<std::pmr::unsynchronized_pool_resource>(
          std::pmr::pool_options{.max_blocks_per_chunk = kMaxBlocksPerChunk,
                                 .largest_required_pool_block = kLargestPooledBlock})),
      bids_(pool_.get()),
      asks_(pool_.get()) {}

MatchingBook::Handle MatchingBook::add(const BookOrder& order) {
    // bids_ and asks_ differ in comparator and so in type; the lambda keeps
    // the one handle construction below shared between them.
    const Level::iterator it = [&] {
        if (order.side == Side::Buy) {
            auto [level_it, inserted] = bids_.try_emplace(order.price);
            return level_it->second.insert(level_it->second.end(), order);
        }
        auto [level_it, inserted] = asks_.try_emplace(order.price);
        return level_it->second.insert(level_it->second.end(), order);
    }();
    return Handle{.side = order.side, .price = order.price, .it = it};
}

BookOrder MatchingBook::remove_at(Handle handle) {
    const BookOrder removed = *handle.it;
    if (handle.side == Side::Buy) {
        auto level_it = bids_.find(handle.price);
        level_it->second.erase(handle.it);
        if (level_it->second.empty()) {
            bids_.erase(level_it);
        }
    } else {
        auto level_it = asks_.find(handle.price);
        level_it->second.erase(handle.it);
        if (level_it->second.empty()) {
            asks_.erase(level_it);
        }
    }
    return removed;
}

void MatchingBook::reduce_at(Handle handle, Quantity new_remaining_quantity) {
    handle.it->remaining_quantity = new_remaining_quantity;
}

void MatchingBook::set_client_order_id_at(Handle handle, ClientOrderId new_client_order_id) {
    handle.it->client_order_id = new_client_order_id;
}

const BookOrder& MatchingBook::at(Handle handle) const { return *handle.it; }

std::optional<Price> MatchingBook::best_bid_price() const {
    if (bids_.empty()) {
        return std::nullopt;
    }
    return bids_.begin()->first;
}

std::optional<Price> MatchingBook::best_ask_price() const {
    if (asks_.empty()) {
        return std::nullopt;
    }
    return asks_.begin()->first;
}

const BookOrder* MatchingBook::front_of_best(Side book_side) const {
    if (book_side == Side::Buy) {
        if (bids_.empty()) {
            return nullptr;
        }
        return &bids_.begin()->second.front();
    }
    if (asks_.empty()) {
        return nullptr;
    }
    return &asks_.begin()->second.front();
}

void MatchingBook::reduce_front(Side book_side, Quantity new_remaining_quantity) {
    if (book_side == Side::Buy) {
        bids_.begin()->second.front().remaining_quantity = new_remaining_quantity;
    } else {
        asks_.begin()->second.front().remaining_quantity = new_remaining_quantity;
    }
}

void MatchingBook::remove_front(Side book_side) {
    if (book_side == Side::Buy) {
        auto level_it = bids_.begin();
        level_it->second.pop_front();
        if (level_it->second.empty()) {
            bids_.erase(level_it);
        }
    } else {
        auto level_it = asks_.begin();
        level_it->second.pop_front();
        if (level_it->second.empty()) {
            asks_.erase(level_it);
        }
    }
}

Quantity MatchingBook::crossable_quantity(Side book_side, Price price, Quantity quantity) const {
    Quantity total = 0;
    if (book_side == Side::Buy) {
        for (const auto& [level_price, level] : bids_) {
            if (price > level_price) {
                break;
            }
            for (const auto& order : level) {
                total += order.remaining_quantity;
                if (total >= quantity) {
                    return total;
                }
            }
        }
    } else {
        for (const auto& [level_price, level] : asks_) {
            if (price < level_price) {
                break;
            }
            for (const auto& order : level) {
                total += order.remaining_quantity;
                if (total >= quantity) {
                    return total;
                }
            }
        }
    }
    return total;
}

std::vector<BookOrder> MatchingBook::all_bids() const {
    std::vector<BookOrder> result;
    for (const auto& [price, level] : bids_) {
        result.insert(result.end(), level.begin(), level.end());
    }
    return result;
}

std::vector<BookOrder> MatchingBook::all_asks() const {
    std::vector<BookOrder> result;
    for (const auto& [price, level] : asks_) {
        result.insert(result.end(), level.begin(), level.end());
    }
    return result;
}

} // namespace mdh::exchange
