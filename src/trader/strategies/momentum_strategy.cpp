#include "trader/strategies/momentum_strategy.hpp"

#include <algorithm>
#include <utility>

namespace mdh::trader::strategies {

MomentumStrategy::MomentumStrategy(risk::TraderRiskGatedOms& trading, NetPositionSource net_position,
                                    MomentumStrategyConfig config)
    : trading_(trading), net_position_(std::move(net_position)), config_(config) {}

std::optional<Price> MomentumStrategy::signal() const {
    if (mids_.size() <= config_.lookback) {
        return std::nullopt;
    }
    return mids_.back() - mids_.front();
}

void MomentumStrategy::on_book_update(InstrumentId instrument_id, const book::OrderBook& book) {
    if (instrument_id != config_.instrument_id) {
        return;
    }
    const auto best_bid = book.best_bid();
    const auto best_ask = book.best_ask();
    if (!best_bid || !best_ask) {
        return; // no midpoint to sample from a one-sided market
    }

    ++updates_seen_;
    mids_.push_back((best_bid->price + best_ask->price) / 2);
    while (mids_.size() > config_.lookback + 1) {
        mids_.pop_front();
    }

    const auto current_signal = signal();
    if (!current_signal) {
        return; // window still filling
    }
    if (++updates_since_order_ <= config_.cooldown_updates) {
        return;
    }

    const positions::NetPosition position = net_position_ ? net_position_() : 0;
    const auto headroom = [&](Side side) -> Quantity {
        // Room left before the cap on this side, then the order shrunk to
        // fit it -- see the class comment on why the cap is approached
        // rather than collided with.
        const positions::NetPosition room =
            side == Side::Buy ? config_.max_position - position : config_.max_position + position;
        if (room <= 0) {
            return 0;
        }
        return std::min(config_.trade_size, static_cast<Quantity>(room));
    };

    Side side{};
    Price limit_price = 0;
    if (*current_signal >= config_.entry_threshold) {
        side = Side::Buy;
        limit_price = best_ask->price + config_.cross_slack;
    } else if (*current_signal <= -config_.entry_threshold) {
        side = Side::Sell;
        limit_price = std::max(Price{1}, best_bid->price - config_.cross_slack);
    } else {
        return;
    }

    const Quantity quantity = headroom(side);
    if (quantity == 0) {
        return; // at the cap on the side the signal points to
    }

    const auto outcome = trading_.submit_new_order(config_.instrument_id, side, limit_price, quantity,
                                                    exchange::OrderType::Limit, exchange::TimeInForce::IOC);
    // A local risk rejection (nullopt) means nothing was sent. The cooldown
    // is still reset either way: retrying immediately would just re-reject
    // on the same unchanged position, once per event.
    updates_since_order_ = 0;
    if (outcome.client_order_id) {
        ++orders_sent_;
    }
}

} // namespace mdh::trader::strategies
