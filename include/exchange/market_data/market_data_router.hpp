#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <stop_token>
#include <thread>

#include "common/dropping_queue.hpp"
#include "exchange/core/event_sink.hpp"
#include "exchange/market_data/market_data_publisher.hpp"

namespace mdh::exchange::market_data {

struct MarketDataRouterOptions {
    std::size_t queue_capacity = 8192;
    std::chrono::milliseconds idle_timeout{1};
};

// Keeps packet construction and network I/O off the matching thread.
//
// The matching thread remains the sole producer: it translates a public
// ExchangeEvent to its sequenced protocol::Event and performs one bounded
// SPSC push. The routing thread is the sole consumer and invokes the
// downstream sink, which may pack and send UDP datagrams.
//
// Translation happens before enqueue so a full-queue drop consumes a feed
// sequence number. The next delivered event therefore exposes a detectable
// sequence gap instead of silently leaving subscribers with a stale book.
class MarketDataRouter {
public:
    explicit MarketDataRouter(MarketDataSink downstream, MarketDataRouterOptions options = {},
                              MarketDataPublisherOptions publisher_options = {});
    ~MarketDataRouter();

    MarketDataRouter(const MarketDataRouter&) = delete;
    MarketDataRouter& operator=(const MarketDataRouter&) = delete;
    MarketDataRouter(MarketDataRouter&&) = delete;
    MarketDataRouter& operator=(MarketDataRouter&&) = delete;

    void start();
    void stop();

    // Called by the matching thread through OrderEntryGateway::extra_event_sink.
    // Private events produce no wire event. A full queue drops the newest
    // public event and never waits for the routing thread.
    void publish(const ExchangeEvent& event);

    [[nodiscard]] EventSink sink() {
        return [this](const ExchangeEvent& event) { publish(event); };
    }

    [[nodiscard]] std::size_t queue_size() const { return queue_.size(); }
    [[nodiscard]] std::size_t queue_high_water_mark() const { return queue_.high_water_mark(); }
    [[nodiscard]] std::size_t dropped_count() const { return queue_.dropped_count(); }
    [[nodiscard]] std::uint64_t routed_count() const { return routed_.load(std::memory_order_relaxed); }

private:
    void routing_loop(std::stop_token token);

    MarketDataSink downstream_;
    MarketDataPublisher publisher_;
    DroppingQueue<protocol::Event> queue_;
    std::chrono::milliseconds idle_timeout_;

    std::stop_source stop_source_;
    std::jthread routing_thread_;
    std::mutex wake_mutex_;
    std::condition_variable wake_cv_;
    std::atomic<std::uint64_t> routed_{0};
};

} // namespace mdh::exchange::market_data
