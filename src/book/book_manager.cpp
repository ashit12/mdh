#include "book/book_manager.hpp"

#include <set>

namespace mdh::book {

const OrderBook* BookManager::find_book(InstrumentId id) const {
    auto it = books_.find(id);
    return it == books_.end() ? nullptr : &it->second;
}

void BookManager::record_trade(InstrumentId id, Price price, Quantity qty) {
    auto& s = stats_[id];
    s.trade_count += 1;
    s.traded_quantity += qty;
    s.last_trade_price = price;
}

const InstrumentStats* BookManager::trade_stats(InstrumentId id) const {
    auto it = stats_.find(id);
    return it == stats_.end() ? nullptr : &it->second;
}

std::vector<InstrumentId> BookManager::instruments() const {
    std::set<InstrumentId> ids;
    for (const auto& [id, _] : books_) {
        ids.insert(id);
    }
    for (const auto& [id, _] : stats_) {
        ids.insert(id);
    }
    return std::vector<InstrumentId>(ids.begin(), ids.end());
}

} // namespace mdh::book
