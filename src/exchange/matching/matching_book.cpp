#include "exchange/matching/matching_book.hpp"

namespace mdh::exchange {

void MatchingBook::add(const ExchangeRestingOrder& order) {
    if (order.side == Side::Buy) {
        auto [level_it, inserted] = bids_.try_emplace(order.price);
        auto order_it = level_it->second.insert(level_it->second.end(), order);
        index_[order.exchange_order_id] = Location{order.side, order.price, order_it};
    } else {
        auto [level_it, inserted] = asks_.try_emplace(order.price);
        auto order_it = level_it->second.insert(level_it->second.end(), order);
        index_[order.exchange_order_id] = Location{order.side, order.price, order_it};
    }
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
    ExchangeRestingOrder removed = *loc.it;
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
    return *it->second.it;
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

std::optional<ExchangeRestingOrder> MatchingBook::front_of_best(Side book_side) const {
    if (book_side == Side::Buy) {
        if (bids_.empty()) {
            return std::nullopt;
        }
        return bids_.begin()->second.front();
    }
    if (asks_.empty()) {
        return std::nullopt;
    }
    return asks_.begin()->second.front();
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
        result.insert(result.end(), level.begin(), level.end());
    }
    return result;
}

std::vector<ExchangeRestingOrder> MatchingBook::all_asks() const {
    std::vector<ExchangeRestingOrder> result;
    for (const auto& [price, level] : asks_) {
        result.insert(result.end(), level.begin(), level.end());
    }
    return result;
}

} // namespace mdh::exchange
