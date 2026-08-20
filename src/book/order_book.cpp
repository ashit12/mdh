#include "book/order_book.hpp"

#include <algorithm>

namespace mdh::book {

void OrderBook::insert_at(Side side, OrderId id, Price price, Quantity qty) {
    if (side == Side::Buy) {
        auto [level_it, inserted] = bids_.try_emplace(price, price);
        auto order_it = level_it->second.add(id, qty);
        order_index_[id] = OrderLocation{side, price, order_it};
    } else {
        auto [level_it, inserted] = asks_.try_emplace(price, price);
        auto order_it = level_it->second.add(id, qty);
        order_index_[id] = OrderLocation{side, price, order_it};
    }
}

void OrderBook::erase_at(const OrderLocation& loc) {
    if (loc.side == Side::Buy) {
        auto level_it = bids_.find(loc.price);
        level_it->second.remove(loc.it);
        if (level_it->second.empty()) {
            bids_.erase(level_it);
        }
    } else {
        auto level_it = asks_.find(loc.price);
        level_it->second.remove(loc.it);
        if (level_it->second.empty()) {
            asks_.erase(level_it);
        }
    }
}

std::optional<BookError> OrderBook::add_order(OrderId id, Price price, Quantity qty, Side side) {
    if (price <= 0) {
        return BookError::InvalidPrice;
    }
    if (qty == 0) {
        return BookError::InvalidQuantity;
    }
    if (order_index_.contains(id)) {
        return BookError::DuplicateOrderId;
    }
    insert_at(side, id, price, qty);
    return std::nullopt;
}

std::optional<BookError> OrderBook::cancel_order(OrderId id) {
    auto it = order_index_.find(id);
    if (it == order_index_.end()) {
        return BookError::UnknownOrderId;
    }
    const OrderLocation loc = it->second;
    erase_at(loc);
    order_index_.erase(it);
    return std::nullopt;
}

std::optional<BookError> OrderBook::modify_order(OrderId id, Price new_price, Quantity new_qty) {
    if (new_price <= 0) {
        return BookError::InvalidPrice;
    }
    if (new_qty == 0) {
        return BookError::InvalidQuantity;
    }
    auto it = order_index_.find(id);
    if (it == order_index_.end()) {
        return BookError::UnknownOrderId;
    }
    const OrderLocation loc = it->second;
    const Side side = loc.side;
    erase_at(loc);
    order_index_.erase(it);
    // Re-added at the back of the (possibly new) level: a modify always
    // loses time priority here, even for a quantity-only decrease where a
    // real exchange would keep it. Simpler to reason about and to test.
    insert_at(side, id, new_price, new_qty);
    return std::nullopt;
}

void OrderBook::clear() {
    bids_.clear();
    asks_.clear();
    order_index_.clear();
}

std::optional<PriceLevelView> OrderBook::best_bid() const {
    if (bids_.empty()) {
        return std::nullopt;
    }
    const auto& [price, level] = *bids_.begin();
    return PriceLevelView{price, level.aggregate_quantity(), level.order_count()};
}

std::optional<PriceLevelView> OrderBook::best_ask() const {
    if (asks_.empty()) {
        return std::nullopt;
    }
    const auto& [price, level] = *asks_.begin();
    return PriceLevelView{price, level.aggregate_quantity(), level.order_count()};
}

std::vector<PriceLevelView> OrderBook::top_bids(std::size_t n) const {
    std::vector<PriceLevelView> result;
    result.reserve(std::min(n, bids_.size()));
    for (auto it = bids_.begin(); it != bids_.end() && result.size() < n; ++it) {
        result.push_back(PriceLevelView{it->first, it->second.aggregate_quantity(), it->second.order_count()});
    }
    return result;
}

std::vector<PriceLevelView> OrderBook::top_asks(std::size_t n) const {
    std::vector<PriceLevelView> result;
    result.reserve(std::min(n, asks_.size()));
    for (auto it = asks_.begin(); it != asks_.end() && result.size() < n; ++it) {
        result.push_back(PriceLevelView{it->first, it->second.aggregate_quantity(), it->second.order_count()});
    }
    return result;
}

std::vector<OrderView> OrderBook::all_bids() const {
    std::vector<OrderView> result;
    for (const auto& [price, level] : bids_) {
        for (const auto& order : level.orders()) {
            result.push_back(OrderView{order.id, price, order.quantity});
        }
    }
    return result;
}

std::vector<OrderView> OrderBook::all_asks() const {
    std::vector<OrderView> result;
    for (const auto& [price, level] : asks_) {
        for (const auto& order : level.orders()) {
            result.push_back(OrderView{order.id, price, order.quantity});
        }
    }
    return result;
}

} // namespace mdh::book
