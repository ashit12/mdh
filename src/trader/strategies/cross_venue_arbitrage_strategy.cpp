#include "trader/strategies/cross_venue_arbitrage_strategy.hpp"

#include <tuple>

namespace mdh::trader::strategies {

CrossVenueArbStrategy::CrossVenueArbStrategy(risk::TraderRiskGatedOms& venue_a, risk::TraderRiskGatedOms& venue_b,
                                              CrossVenueArbConfig config)
    : venue_a_(venue_a), venue_b_(venue_b), config_(config) {}

void CrossVenueArbStrategy::on_venue_a_update(InstrumentId instrument_id, const book::OrderBook& book) {
    if (instrument_id != config_.instrument_id) {
        return;
    }
    venue_a_best_bid_ = book.best_bid();
    venue_a_best_ask_ = book.best_ask();
    evaluate();
}

void CrossVenueArbStrategy::on_venue_b_update(InstrumentId instrument_id, const book::OrderBook& book) {
    if (instrument_id != config_.instrument_id) {
        return;
    }
    venue_b_best_bid_ = book.best_bid();
    venue_b_best_ask_ = book.best_ask();
    evaluate();
}

void CrossVenueArbStrategy::evaluate() {
    if (venue_a_best_ask_ && venue_b_best_bid_ && venue_b_best_bid_->price - venue_a_best_ask_->price >= config_.min_edge) {
        // Cheaper on A, richer on B: buy A, sell B.
        std::ignore = venue_a_.submit_new_order(config_.instrument_id, Side::Buy, venue_a_best_ask_->price, config_.trade_size,
                                                 exchange::OrderType::Limit, exchange::TimeInForce::IOC);
        std::ignore = venue_b_.submit_new_order(config_.instrument_id, Side::Sell, venue_b_best_bid_->price, config_.trade_size,
                                                 exchange::OrderType::Limit, exchange::TimeInForce::IOC);
        return;
    }
    if (venue_b_best_ask_ && venue_a_best_bid_ && venue_a_best_bid_->price - venue_b_best_ask_->price >= config_.min_edge) {
        // Cheaper on B, richer on A: buy B, sell A.
        std::ignore = venue_b_.submit_new_order(config_.instrument_id, Side::Buy, venue_b_best_ask_->price, config_.trade_size,
                                                 exchange::OrderType::Limit, exchange::TimeInForce::IOC);
        std::ignore = venue_a_.submit_new_order(config_.instrument_id, Side::Sell, venue_a_best_bid_->price, config_.trade_size,
                                                 exchange::OrderType::Limit, exchange::TimeInForce::IOC);
    }
}

} // namespace mdh::trader::strategies
