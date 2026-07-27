#pragma once

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "book/order_book.hpp"
#include "common/types.hpp"

namespace mdh::book {

// Trade messages carry no order_id in this milestone's protocol, so there
// is nothing to match a trade back to a specific resting order -- they are
// treated as informational (statistics only) and do not mutate book depth.
// AddOrder/CancelOrder/ModifyOrder are the only book mutators.
struct InstrumentStats {
    std::uint64_t trade_count = 0;
    Quantity traded_quantity = 0;
    Price last_trade_price = 0;
};

// Owns one OrderBook per instrument, created lazily on first reference, plus
// per-instrument trade statistics.
class BookManager {
public:
    [[nodiscard]] OrderBook& book_for(InstrumentId id) { return books_[id]; }
    [[nodiscard]] const OrderBook* find_book(InstrumentId id) const;

    void record_trade(InstrumentId id, Price price, Quantity qty);
    [[nodiscard]] const InstrumentStats* trade_stats(InstrumentId id) const;

    // All instrument ids seen so far (via an order or a trade), sorted ascending.
    [[nodiscard]] std::vector<InstrumentId> instruments() const;

private:
    std::unordered_map<InstrumentId, OrderBook> books_;
    std::unordered_map<InstrumentId, InstrumentStats> stats_;
};

} // namespace mdh::book
