#pragma once

#include <list>
#include <map>
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
// liquidity while it decides whether an incoming order crosses. Correctness
// and readability over cleverness: no object pools or intrusive lists yet,
// per the working rules (only add those once a baseline matcher is correct
// and benchmarked).
namespace mdh::exchange {

class MatchingBook {
public:
    // Inserts at the back of its price level's FIFO queue (i.e. as the
    // lowest-priority order at that price). Caller is responsible for
    // ensuring order.exchange_order_id is not already present.
    void add(const ExchangeRestingOrder& order);

    // Removes and returns the order with this id from wherever it rests
    // (any level, any FIFO position) -- used by Cancel and by Replace's
    // priority-preserving path. Returns std::nullopt if not found.
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
    [[nodiscard]] std::optional<ExchangeRestingOrder> front_of_best(Side book_side) const;
    void reduce_front(Side book_side, Quantity new_remaining_quantity);
    void remove_front(Side book_side);

    // Every resting order, price-priority-then-FIFO order, for tests
    // asserting exact final book state.
    [[nodiscard]] std::vector<ExchangeRestingOrder> all_bids() const;
    [[nodiscard]] std::vector<ExchangeRestingOrder> all_asks() const;

private:
    struct Location {
        Side side;
        Price price;
        std::list<ExchangeRestingOrder>::iterator it;
    };

    using Level = std::list<ExchangeRestingOrder>;
    using BidMap = std::map<Price, Level, std::greater<Price>>;
    using AskMap = std::map<Price, Level>;

    void erase_at(const Location& loc);

    BidMap bids_;
    AskMap asks_;
    std::unordered_map<ExchangeOrderId, Location> index_;
};

} // namespace mdh::exchange
