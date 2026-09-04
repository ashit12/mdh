#pragma once

#include <array>
#include <atomic>
#include <bit>
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

    // Times each step of publish() separately -- see PublishCostStats. Off by
    // default because the extra tick reads land on the matching thread, the
    // one path where cost is worth measuring and therefore also the one path
    // where measuring it is not free.
    bool measure_publish_cost = false;
};

// A counter written only by the matching thread and read as a best-effort
// snapshot by whoever reports it. Atomic because of that cross-thread read,
// relaxed because no other state is published through it, and never a
// read-modify-write because a single writer makes one unnecessary.
class ProducerCounter {
public:
    void add(std::uint64_t delta) noexcept {
        value_.store(value_.load(std::memory_order_relaxed) + delta, std::memory_order_relaxed);
    }

    void raise_to(std::uint64_t candidate) noexcept {
        if (candidate > value_.load(std::memory_order_relaxed)) {
            value_.store(candidate, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] std::uint64_t get() const noexcept { return value_.load(std::memory_order_relaxed); }

private:
    std::atomic<std::uint64_t> value_{0};
};

// Per-call push cost is bucketed by raw tick count, so recording it stays
// integer-only and needs no tick-to-nanosecond conversion on the matching
// thread. Whoever reports the buckets knows the tick rate and can label the
// edges in nanoseconds.
//
// Buckets are powers of two -- bucket k holds [2^(k-1), 2^k) ticks -- rather
// than a hand-picked ladder, because the tick rate is a property of the
// machine. A ladder tuned to a 24 MHz counter puts everything from 240 ns to
// 40 us in one top bucket on a nanosecond counter, which is exactly the
// range the question is about. std::bit_width gives the index directly.
inline constexpr std::size_t kPushBucketCount = 24;

// Inclusive lower edge, in ticks, of bucket `index`. Bucket 0 is zero ticks
// (below the counter's resolution); the last bucket is open-ended.
[[nodiscard]] inline constexpr std::uint64_t push_bucket_lower_edge_ticks(std::size_t index) {
    return index == 0 ? 0 : std::uint64_t{1} << (index - 1);
}

[[nodiscard]] inline std::size_t push_bucket_index(std::uint64_t ticks) {
    const auto width = static_cast<std::size_t>(std::bit_width(ticks));
    return width < kPushBucketCount ? width : kPushBucketCount - 1;
}

// What the matching thread actually spent inside publish(), split three ways:
// translating the event to wire form, the bounded SPSC push, and waking the
// routing thread. Split rather than totalled because the three have different
// remedies, and because "the push is blocking or spinning" is a claim that
// should be settled by a measured distribution rather than by reading the
// queue's source.
//
// Times are in ticks (mdh::monotonic_ticks); the tick source's resolution
// bounds what any single sample can say, so the distribution matters more
// than the maximum. Snapshot only -- fields are read one at a time while the
// matching thread keeps writing, so they are mutually consistent only once
// the matching thread has stopped.
struct PublishCostStats {
    std::uint64_t events_seen = 0; // ExchangeEvents handed to publish()
    std::uint64_t wire_events = 0; // of those, the public ones that became a wire event
    std::uint64_t push_ok = 0;
    std::uint64_t push_full = 0; // try_push refused, newest event dropped
    std::uint64_t push_ticks_total = 0;
    std::uint64_t push_ticks_max = 0;
    std::uint64_t notify_calls = 0;
    std::uint64_t notify_ticks_total = 0;
    std::uint64_t notify_ticks_max = 0;
    std::uint64_t translate_ticks_total = 0; // publish() as a whole, minus push and notify
    std::uint64_t translate_ticks_max = 0;
    std::array<std::uint64_t, kPushBucketCount> push_buckets{};

    // Control: two adjacent tick reads with nothing at all between them,
    // measured once per publish() on the same thread, and bucketed exactly
    // like the push. An empty interval has nothing to block on, so every
    // outlier in this distribution is the matching thread being descheduled
    // -- which makes it the yardstick for whether the push's own outliers
    // need any other explanation. Without it, a 40 us maximum anywhere in
    // publish() is unattributable.
    //
    // Compare the two by rate per unit of exposure, not by maximum: the
    // control's window is only as wide as two tick reads, while the push's
    // is that plus the push, so the push is exposed to preemption for longer
    // and will show a worse maximum for that reason alone.
    std::uint64_t control_ticks_total = 0;
    std::uint64_t control_ticks_max = 0;
    std::array<std::uint64_t, kPushBucketCount> control_buckets{};
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

    // How often the routing thread found the queue empty and went to sleep on
    // the condition variable. This is what decides whether the matching
    // thread's notify_one() has a waiter to wake -- a cheap no-op when the
    // consumer is still running, a kernel wakeup when it is not -- so it
    // belongs next to the publish cost it explains.
    [[nodiscard]] std::uint64_t cv_wait_count() const { return cv_waits_.load(std::memory_order_relaxed); }

    // Empty unless MarketDataRouterOptions::measure_publish_cost was set.
    [[nodiscard]] PublishCostStats publish_cost() const;

private:
    void routing_loop(std::stop_token token);
    void publish_measured(const ExchangeEvent& event);

    MarketDataSink downstream_;
    MarketDataPublisher publisher_;
    DroppingQueue<protocol::Event> queue_;
    std::chrono::milliseconds idle_timeout_;
    bool measure_publish_cost_;

    std::stop_source stop_source_;
    std::jthread routing_thread_;
    std::mutex wake_mutex_;
    std::condition_variable wake_cv_;
    std::atomic<std::uint64_t> routed_{0};
    std::atomic<std::uint64_t> cv_waits_{0};

    // All matching-thread-written, all only when measure_publish_cost_.
    ProducerCounter events_seen_;
    ProducerCounter wire_events_;
    ProducerCounter push_ok_;
    ProducerCounter push_full_;
    ProducerCounter push_ticks_total_;
    ProducerCounter push_ticks_max_;
    ProducerCounter notify_calls_;
    ProducerCounter notify_ticks_total_;
    ProducerCounter notify_ticks_max_;
    ProducerCounter translate_ticks_total_;
    ProducerCounter translate_ticks_max_;
    ProducerCounter control_ticks_total_;
    ProducerCounter control_ticks_max_;
    std::array<ProducerCounter, kPushBucketCount> push_buckets_;
    std::array<ProducerCounter, kPushBucketCount> control_buckets_;
};

} // namespace mdh::exchange::market_data
