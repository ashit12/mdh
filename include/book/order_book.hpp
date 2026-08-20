#pragma once

#include <map>
#include <optional>
#include <unordered_map>
#include <vector>

#include "book/book_errors.hpp"
#include "book/price_level.hpp"
#include "common/types.hpp"

namespace mdh::book {

struct PriceLevelView {
    Price price;
    Quantity aggregate_quantity;
    std::size_t order_count;
};

// One resting order, with enough detail to reconstruct it via add_order()
// -- unlike PriceLevelView, which aggregates a whole level for display.
// Used for snapshotting (need every order, not just the top N levels).
struct OrderView {
    OrderId order_id;
    Price price;
    Quantity quantity;
};

// A single instrument's book. Bids ordered highest-first, asks ordered
// lowest-first, each side a std::map<Price, PriceLevel> keyed for that
// ordering. An OrderId -> location index lets cancel/modify find the right
// side and price level without scanning either map.
//
// Cancel/modify cost: O(1) hash lookup in order_index_ to find the side and
// price, then an O(log P) std::map lookup (P = distinct price levels on
// that side) to reach the PriceLevel, then O(1) list erase within it (and
// an O(log P) map erase if that was the level's last order). This is NOT
// O(1) overall -- the map lookup is the dominant cost. A flat array
// indexed by (price - base_price) would make the level lookup O(1) at the
// cost of wasted space for sparse price ranges and a linear best-price
// scan when the top level empties; std::map was chosen for simplicity and
// because book depth in this project's synthetic feeds is small enough
// that the log(P) factor is negligible next to memory-allocation costs
// elsewhere in the pipeline.
class OrderBook {
public:
    [[nodiscard]] std::optional<BookError> add_order(OrderId id, Price price, Quantity qty, Side side);
    [[nodiscard]] std::optional<BookError> cancel_order(OrderId id);
    [[nodiscard]] std::optional<BookError> modify_order(OrderId id, Price new_price, Quantity new_qty);
    void clear();

    [[nodiscard]] std::optional<PriceLevelView> best_bid() const;
    [[nodiscard]] std::optional<PriceLevelView> best_ask() const;
    [[nodiscard]] std::vector<PriceLevelView> top_bids(std::size_t n) const;
    [[nodiscard]] std::vector<PriceLevelView> top_asks(std::size_t n) const;

    // Every resting order on one side, across all price levels -- for
    // snapshotting the whole book, not just the top N levels for display.
    [[nodiscard]] std::vector<OrderView> all_bids() const;
    [[nodiscard]] std::vector<OrderView> all_asks() const;

    [[nodiscard]] bool has_order(OrderId id) const { return order_index_.contains(id); }

private:
    struct OrderLocation {
        Side side;
        Price price;
        PriceLevel::Iterator it;
    };

    using BidMap = std::map<Price, PriceLevel, std::greater<Price>>;
    using AskMap = std::map<Price, PriceLevel>;

    void insert_at(Side side, OrderId id, Price price, Quantity qty);
    void erase_at(const OrderLocation& loc);

    BidMap bids_;
    AskMap asks_;
    std::unordered_map<OrderId, OrderLocation> order_index_;
};

} // namespace mdh::book
