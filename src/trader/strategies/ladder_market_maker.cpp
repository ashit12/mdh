#include "trader/strategies/ladder_market_maker.hpp"

#include <utility>

namespace mdh::trader::strategies {

LadderMarketMaker::LadderMarketMaker(risk::TraderRiskGatedOms& trading, NetPositionSource net_position,
                                      LadderMarketMakerConfig config)
    : trading_(trading), net_position_(std::move(net_position)), config_(config), walk_(config.walk) {
    bids_.reserve(config_.levels_per_side);
    asks_.reserve(config_.levels_per_side);
    for (std::size_t level = 0; level < config_.levels_per_side; ++level) {
        bids_.emplace_back(config_.instrument_id, Side::Buy, config_.quote_size, config_.requote_threshold);
        asks_.emplace_back(config_.instrument_id, Side::Sell, config_.quote_size, config_.requote_threshold);
    }
}

Price LadderMarketMaker::bid_price_for(std::size_t level, Price reference) const {
    return reference - config_.half_spread - static_cast<Price>(level) * config_.level_spacing;
}

Price LadderMarketMaker::ask_price_for(std::size_t level, Price reference) const {
    return reference + config_.half_spread + static_cast<Price>(level) * config_.level_spacing;
}

std::size_t LadderMarketMaker::on_quote_cycle() {
    const Price previous_reference = walk_.price();
    const Price reference = walk_.step();

    const positions::NetPosition position = net_position_ ? net_position_() : 0;
    const bool withdraw_bids = position >= config_.max_position;
    const bool withdraw_asks = position <= -config_.max_position;

    // Whichever side is moving *away* goes first. A ladder is internally
    // uncrossed by construction (its two sides are 2 * half_spread apart),
    // but a reference price that jumps further than that in one step would
    // otherwise briefly put a new bid at or through this strategy's own
    // still-resting stale ask, and it would trade with itself. Widening the
    // far side before advancing the near one is the same discipline
    // MarketMakerStrategy applies to its single quote, generalized to a
    // ladder.
    const bool rising = reference >= previous_reference;

    std::size_t sent = 0;
    auto update_bids = [&] {
        for (std::size_t level = 0; level < bids_.size(); ++level) {
            const std::optional<Price> desired =
                withdraw_bids ? std::nullopt : std::optional<Price>(bid_price_for(level, reference));
            sent += bids_[level].update(trading_, desired) ? 1 : 0;
        }
    };
    auto update_asks = [&] {
        for (std::size_t level = 0; level < asks_.size(); ++level) {
            const std::optional<Price> desired =
                withdraw_asks ? std::nullopt : std::optional<Price>(ask_price_for(level, reference));
            sent += asks_[level].update(trading_, desired) ? 1 : 0;
        }
    };

    if (rising) {
        update_asks();
        update_bids();
    } else {
        update_bids();
        update_asks();
    }
    return sent;
}

std::size_t LadderMarketMaker::withdraw_all() {
    std::size_t sent = 0;
    for (auto& quote : bids_) {
        sent += quote.withdraw(trading_) ? 1 : 0;
    }
    for (auto& quote : asks_) {
        sent += quote.withdraw(trading_) ? 1 : 0;
    }
    return sent;
}

} // namespace mdh::trader::strategies
