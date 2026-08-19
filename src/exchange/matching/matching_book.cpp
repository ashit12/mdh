#include "exchange/matching/matching_book.hpp"

#include <cstddef>

namespace mdh::exchange {
namespace {

// This book allocates three node sizes: 64 bytes for a list node (a 48-byte
// BookOrder plus two links), 72 for a map node, 64 for an index node. Both
// pools are given the same options; they exist to keep the two 64-byte node
// types out of each other's size class, not to be tuned differently.
//
// Sizing this bound is not as direct as "the largest node we allocate".
// pool_options is advisory, and libc++ reads it conservatively: measured on
// this toolchain, it only serves blocks up to a *quarter* of this value from
// its fixed pools, sending anything larger to an adhoc fallback list whose
// deallocate is a linear scan -- which turns a book that cancels as fast as
// it rests into quadratic time. 1024 therefore pools blocks up to 256 bytes,
// well clear of the 72-byte largest node, while still leaving genuinely
// large allocations (a rehashed bucket array) to the upstream allocator
// where they belong. bench_matching_memory is the guard: if a node ever
// stops being pooled, allocations per operation jump straight back up.
constexpr std::size_t kLargestPooledBlock = 1024;

// Chunks grow geometrically up to this many blocks, so early books stay
// small while a deep one converges on roughly one upstream allocation per
// 4096 nodes instead of one per node.
constexpr std::size_t kMaxBlocksPerChunk = 4096;

// Enough buckets for a modestly deep book to fill up without a rehash. Not
// a cap: the index still grows past this, it just does not pay for the
// several rehashes it would otherwise take to get here.
constexpr std::size_t kInitialIndexBuckets = 1024;

} // namespace

MatchingBook::MatchingBook(InstrumentId instrument_id)
    : level_pool_(std::make_unique<std::pmr::unsynchronized_pool_resource>(
          std::pmr::pool_options{.max_blocks_per_chunk = kMaxBlocksPerChunk,
                                 .largest_required_pool_block = kLargestPooledBlock})),
      index_pool_(std::make_unique<std::pmr::unsynchronized_pool_resource>(
          std::pmr::pool_options{.max_blocks_per_chunk = kMaxBlocksPerChunk,
                                 .largest_required_pool_block = kLargestPooledBlock})),
      bids_(level_pool_.get()),
      asks_(level_pool_.get()),
      index_(index_pool_.get()),
      instrument_id_(instrument_id) {
    index_.reserve(kInitialIndexBuckets);
}

void MatchingBook::add(const ExchangeRestingOrder& order) {
    const BookOrder resting{
        .exchange_order_id = order.exchange_order_id,
        .client_order_id = order.client_order_id,
        .account_id = order.account_id,
        .price = order.price,
        .remaining_quantity = order.remaining_quantity,
        .side = order.side,
        .time_in_force = order.time_in_force,
    };
    // bids_ and asks_ differ in comparator and so in type; the lambda keeps
    // the one index write below shared between them.
    const Level::iterator order_it = [&] {
        if (order.side == Side::Buy) {
            auto [level_it, inserted] = bids_.try_emplace(order.price);
            return level_it->second.insert(level_it->second.end(), resting);
        }
        auto [level_it, inserted] = asks_.try_emplace(order.price);
        return level_it->second.insert(level_it->second.end(), resting);
    }();
    index_[order.exchange_order_id] = Location{
        .price = order.price,
        .it = order_it,
        .original_quantity = order.original_quantity,
        .order_sequence = order.order_sequence,
        .side = order.side,
    };
}

ExchangeRestingOrder MatchingBook::compose(const BookOrder& order, const Location& loc) const {
    return ExchangeRestingOrder{
        .exchange_order_id = order.exchange_order_id,
        .client_order_id = order.client_order_id,
        .account_id = order.account_id,
        .price = order.price,
        .original_quantity = loc.original_quantity,
        .remaining_quantity = order.remaining_quantity,
        .order_sequence = loc.order_sequence,
        .instrument_id = instrument_id_,
        .side = order.side,
        .time_in_force = order.time_in_force,
    };
}

void MatchingBook::erase_at(const Location& loc) {
    if (loc.side == Side::Buy) {
        auto level_it = bids_.find(loc.price);
        level_it->second.erase(loc.it);
        if (level_it->second.empty()) {
            bids_.erase(level_it);
        }
    } else {
        auto level_it = asks_.find(loc.price);
        level_it->second.erase(loc.it);
        if (level_it->second.empty()) {
            asks_.erase(level_it);
        }
    }
}

std::optional<ExchangeRestingOrder> MatchingBook::remove(ExchangeOrderId id) {
    auto it = index_.find(id);
    if (it == index_.end()) {
        return std::nullopt;
    }
    const Location loc = it->second;
    const ExchangeRestingOrder removed = compose(*loc.it, loc);
    erase_at(loc);
    index_.erase(it);
    return removed;
}

bool MatchingBook::reduce(ExchangeOrderId id, Quantity new_remaining_quantity) {
    auto it = index_.find(id);
    if (it == index_.end()) {
        return false;
    }
    it->second.it->remaining_quantity = new_remaining_quantity;
    return true;
}

bool MatchingBook::set_client_order_id(ExchangeOrderId id, ClientOrderId new_client_order_id) {
    auto it = index_.find(id);
    if (it == index_.end()) {
        return false;
    }
    it->second.it->client_order_id = new_client_order_id;
    return true;
}

std::optional<ExchangeRestingOrder> MatchingBook::find(ExchangeOrderId id) const {
    auto it = index_.find(id);
    if (it == index_.end()) {
        return std::nullopt;
    }
    return compose(*it->second.it, it->second);
}

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
        index_.erase(level_it->second.front().exchange_order_id);
        level_it->second.pop_front();
        if (level_it->second.empty()) {
            bids_.erase(level_it);
        }
    } else {
        auto level_it = asks_.begin();
        index_.erase(level_it->second.front().exchange_order_id);
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

std::vector<ExchangeRestingOrder> MatchingBook::all_bids() const {
    std::vector<ExchangeRestingOrder> result;
    for (const auto& [price, level] : bids_) {
        for (const auto& order : level) {
            result.push_back(compose(order, index_.at(order.exchange_order_id)));
        }
    }
    return result;
}

std::vector<ExchangeRestingOrder> MatchingBook::all_asks() const {
    std::vector<ExchangeRestingOrder> result;
    for (const auto& [price, level] : asks_) {
        for (const auto& order : level) {
            result.push_back(compose(order, index_.at(order.exchange_order_id)));
        }
    }
    return result;
}

} // namespace mdh::exchange
