#include "exchange/market_data/market_data_router.hpp"

#include <utility>

namespace mdh::exchange::market_data {

MarketDataRouter::MarketDataRouter(MarketDataSink downstream, MarketDataRouterOptions options,
                                   MarketDataPublisherOptions publisher_options)
    : downstream_(std::move(downstream)), publisher_(std::move(publisher_options)), queue_(options.queue_capacity),
      idle_timeout_(options.idle_timeout) {}

MarketDataRouter::~MarketDataRouter() { stop(); }

void MarketDataRouter::start() {
    if (routing_thread_.joinable()) {
        return;
    }
    routing_thread_ = std::jthread([this] { routing_loop(stop_source_.get_token()); });
}

void MarketDataRouter::stop() {
    stop_source_.request_stop();
    wake_cv_.notify_all();
    if (routing_thread_.joinable()) {
        routing_thread_.join();
    }
}

void MarketDataRouter::publish(const ExchangeEvent& event) {
    publisher_.publish(event, [this](const protocol::Event& wire_event) {
        if (queue_.push(wire_event)) {
            wake_cv_.notify_one();
        }
    });
}

void MarketDataRouter::routing_loop(std::stop_token token) {
    while (true) {
        if (auto event = queue_.try_pop()) {
            downstream_(*event);
            routed_.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        if (token.stop_requested()) {
            break; // queue was observed empty after stop: drain complete
        }

        std::unique_lock<std::mutex> lock(wake_mutex_);
        wake_cv_.wait_for(lock, idle_timeout_, [&] {
            return token.stop_requested() || queue_.size() > 0;
        });
    }
}

} // namespace mdh::exchange::market_data
