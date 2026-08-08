#include "exchange/market_data/market_data_publisher.hpp"

#include <chrono>
#include <type_traits>
#include <utility>
#include <variant>

namespace mdh::exchange::market_data {

namespace {

Timestamp wall_clock_now_ns() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<Timestamp>(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

} // namespace

MarketDataPublisher::MarketDataPublisher(MarketDataPublisherOptions options) : options_(std::move(options)) {
    if (!options_.clock) {
        options_.clock = wall_clock_now_ns;
    }
}

// Dispatch is already wired up -- you shouldn't need to touch this
// function. It mirrors MatchingEngine::process()'s own std::visit shape
// (see matching_engine.cpp): one branch per publishable alternative, and no
// branch at all for the four private alternatives (OrderAccepted,
// OrderRejected, OrderCancelled, OrderReplaced) -- std::visit is fine with
// an incomplete if-constexpr chain; the unhandled alternatives simply fall
// through and do nothing, which is exactly the "never publish this"
// behavior the class-level comment describes.
void MarketDataPublisher::publish(const ExchangeEvent& event, const MarketDataSink& sink) {
    std::visit(
        [&](const auto& e) {
            using T = std::decay_t<decltype(e)>;
            if constexpr (std::is_same_v<T, BookOrderAdded>) {
                publish_book_order_added(e, sink);
            } else if constexpr (std::is_same_v<T, BookOrderReduced>) {
                publish_book_order_reduced(e, sink);
            } else if constexpr (std::is_same_v<T, BookOrderRemoved>) {
                publish_book_order_removed(e, sink);
            } else if constexpr (std::is_same_v<T, TradeExecuted>) {
                publish_trade_executed(e, sink);
            }
        },
        event);
}

void MarketDataPublisher::publish_book_order_added(const BookOrderAdded& event, const MarketDataSink& sink) {
    sink(protocol::AddOrder{
        .sequence_number = next_sequence_number(),
        .timestamp_ns = options_.clock(),
        .order_id = event.exchange_order_id,
        .instrument_id = event.instrument_id,
        .price = event.price,
        .quantity = event.quantity,
        .side = event.side,
    });
}

void MarketDataPublisher::publish_book_order_reduced(const BookOrderReduced& event, const MarketDataSink& sink) {
    sink(protocol::ModifyOrder{
        .sequence_number = next_sequence_number(),
        .timestamp_ns = options_.clock(),
        .order_id = event.exchange_order_id,
        .instrument_id = event.instrument_id,
        .new_price = event.price, // unchanged -- BookOrderReduced never reprices
        .new_quantity = event.new_remaining_quantity,
    });
}

void MarketDataPublisher::publish_book_order_removed(const BookOrderRemoved& event, const MarketDataSink& sink) {
    sink(protocol::CancelOrder{
        .sequence_number = next_sequence_number(),
        .timestamp_ns = options_.clock(),
        .order_id = event.exchange_order_id,
        .instrument_id = event.instrument_id,
    });
}

void MarketDataPublisher::publish_trade_executed(const TradeExecuted& event, const MarketDataSink& sink) {
    sink(protocol::Trade{
        .sequence_number = next_sequence_number(),
        .timestamp_ns = options_.clock(),
        .instrument_id = event.instrument_id,
        .price = event.price,
        .quantity = event.quantity,
        .aggressor_side = event.aggressor_side,
    });
}

} // namespace mdh::exchange::market_data
