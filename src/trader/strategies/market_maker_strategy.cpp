#include "trader/strategies/market_maker_strategy.hpp"

#include "trader/oms/client_order.hpp"

namespace mdh::trader::strategies {

namespace {

using oms::is_terminal;

[[nodiscard]] Price abs_diff(Price a, Price b) { return a > b ? a - b : b - a; }

} // namespace

MarketMakerStrategy::MarketMakerStrategy(risk::TraderRiskGatedOms& trading, MarketMakerConfig config)
    : trading_(trading), config_(config) {}

void MarketMakerStrategy::on_book_update(InstrumentId instrument_id, const book::OrderBook& book) {
    if (instrument_id != config_.instrument_id) {
        return;
    }
    const auto best_bid = book.best_bid();
    const auto best_ask = book.best_ask();
    if (!best_bid || !best_ask) {
        return; // no two-sided market yet to quote around
    }
    const Price mid = (best_bid->price + best_ask->price) / 2;

    const bool at_position_cap = trading_.position(config_.instrument_id) >= config_.max_position;
    const std::optional<Price> desired_bid = at_position_cap ? std::nullopt : std::optional<Price>(mid - config_.half_spread);
    const std::optional<Price> desired_ask = mid + config_.half_spread;

    // Order matters here: replacing the bid up to a price that meets or
    // beats the *currently still-resting* ask (before that ask has had a
    // chance to move too) would have this strategy's own new bid
    // immediately cross and self-fill against its own stale ask one
    // exchange-side command early -- and symmetrically for a sharp downward
    // move crossing the currently-resting bid. Whichever side's move would
    // create that transient cross gets sent first, clearing it out of the
    // way before the other side advances into that price -- the same
    // "widen the far side before tightening the near side" discipline a
    // real market maker's own quoting logic would need.
    const auto current_ask = ask_order_id_ ? trading_.order(*ask_order_id_) : std::nullopt;
    const auto current_bid = bid_order_id_ ? trading_.order(*bid_order_id_) : std::nullopt;
    const bool bid_would_cross_current_ask =
        desired_bid && current_ask && !is_terminal(current_ask->state) && *desired_bid >= current_ask->price;
    const bool ask_would_cross_current_bid =
        desired_ask && current_bid && !is_terminal(current_bid->state) && *desired_ask <= current_bid->price;

    if (bid_would_cross_current_ask && !ask_would_cross_current_bid) {
        update_side(Side::Sell, ask_order_id_, desired_ask);
        update_side(Side::Buy, bid_order_id_, desired_bid);
    } else if (ask_would_cross_current_bid && !bid_would_cross_current_ask) {
        update_side(Side::Buy, bid_order_id_, desired_bid);
        update_side(Side::Sell, ask_order_id_, desired_ask);
    } else {
        update_side(Side::Buy, bid_order_id_, desired_bid);
        update_side(Side::Sell, ask_order_id_, desired_ask);
    }
}

void MarketMakerStrategy::update_side(Side side, std::optional<exchange::ClientOrderId>& order_id,
                                       std::optional<Price> desired_price) {
    if (order_id) {
        const auto order = trading_.order(*order_id);
        if (!order || is_terminal(order->state)) {
            order_id.reset(); // that id is done; free to submit a fresh quote below
        } else if (order->state == oms::ClientOrderState::PendingNew || order->pending_action != oms::PendingAction::None) {
            return; // an Accept or a previous cancel/replace is still in flight -- wait for it, don't pile on another
        } else if (!desired_price) {
            // No longer want this side quoted (e.g. inventory cap reached).
            // A false return (already terminal, or a cancel/replace beat us
            // to it) is harmless to ignore here: either way order_id stays
            // set until on_book_update sees it go terminal above.
            [[maybe_unused]] const bool sent = trading_.cancel_order(*order_id);
            return;
        } else if (abs_diff(order->price, *desired_price) < config_.requote_threshold) {
            return; // already quoting close enough to the desired price
        } else {
            if (const auto new_id = trading_.replace_order(*order_id, *desired_price, config_.quote_size)) {
                order_id = new_id;
            }
            return;
        }
    }

    if (!desired_price) {
        return; // nothing resting and nothing wanted -- nothing to do
    }
    const auto outcome = trading_.submit_new_order(config_.instrument_id, side, *desired_price, config_.quote_size);
    order_id = outcome.client_order_id; // nullopt if locally risk-rejected (e.g. no inventory to sell yet); retried next tick
}

} // namespace mdh::trader::strategies
