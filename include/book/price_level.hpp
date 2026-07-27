#pragma once

#include <list>

#include "common/types.hpp"

namespace mdh::book {

struct RestingOrder {
    OrderId id;
    Quantity quantity;
};

// All resting orders at one price, for one side of one instrument's book.
// FIFO within a level via std::list (stable iterators on insert/erase
// elsewhere in the list, which is what makes O(1) removal-by-iterator
// possible once the caller already knows *which* level and *which* list
// node to look at -- see OrderBook for how it gets there).
class PriceLevel {
public:
    using Iterator = std::list<RestingOrder>::iterator;

    explicit PriceLevel(Price price) : price_(price) {}

    Iterator add(OrderId id, Quantity qty) {
        orders_.push_back(RestingOrder{id, qty});
        aggregate_qty_ += qty;
        return std::prev(orders_.end());
    }

    void remove(Iterator it) {
        aggregate_qty_ -= it->quantity;
        orders_.erase(it);
    }

    [[nodiscard]] bool empty() const { return orders_.empty(); }
    [[nodiscard]] Price price() const { return price_; }
    [[nodiscard]] Quantity aggregate_quantity() const { return aggregate_qty_; }
    [[nodiscard]] std::size_t order_count() const { return orders_.size(); }
    [[nodiscard]] const std::list<RestingOrder>& orders() const { return orders_; }

private:
    Price price_;
    std::list<RestingOrder> orders_;
    Quantity aggregate_qty_ = 0;
};

} // namespace mdh::book
