#include "exchange/market_data/market_data_router.hpp"

#include <utility>

#include "common/monotonic_ticks.hpp"
#include "common/thread_affinity.hpp"

namespace mdh::exchange::market_data {

MarketDataRouter::MarketDataRouter(MarketDataBatchSink downstream, MarketDataRouterOptions options,
                                   MarketDataPublisherOptions publisher_options)
    : downstream_(std::move(downstream)), publisher_(std::move(publisher_options)), queue_(options.queue_capacity),
      idle_timeout_(options.idle_timeout), max_batch_(options.max_batch == 0 ? 1 : options.max_batch),
      measure_publish_cost_(options.measure_publish_cost) {
    // Reserved here rather than on the routing thread so that a drain never
    // allocates, and so a batch of max_batch_ is not a reallocation.
    batch_.reserve(max_batch_);
}

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
    if (measure_publish_cost_) {
        publish_measured(event);
        return;
    }
    publisher_.publish(event, [this](const protocol::Event& wire_event) {
        if (queue_.push(wire_event)) {
            wake_cv_.notify_one();
        }
    });
}

// The same work as publish(), with each step timed. Kept as a separate
// function rather than as branches inside publish() so the measured path can
// never cost the unmeasured one anything.
void MarketDataRouter::publish_measured(const ExchangeEvent& event) {
    std::uint64_t push_ticks = 0;
    std::uint64_t notify_ticks = 0;

    // The control interval, taken first so it is outside everything it is
    // the control for. Adjacent reads with nothing between them.
    const std::uint64_t control_begin = monotonic_ticks();
    const std::uint64_t control_ticks = monotonic_ticks() - control_begin;

    const std::uint64_t publish_begin = monotonic_ticks();
    publisher_.publish(event, [&](const protocol::Event& wire_event) {
        const std::uint64_t push_begin = monotonic_ticks();
        const bool queued = queue_.push(wire_event);
        const std::uint64_t push_end = monotonic_ticks();
        push_ticks = push_end - push_begin;

        if (queued) {
            wake_cv_.notify_one();
            notify_ticks = monotonic_ticks() - push_end;
            notify_calls_.add(1);
            push_ok_.add(1);
        } else {
            push_full_.add(1);
        }

        wire_events_.add(1);
        push_ticks_total_.add(push_ticks);
        push_ticks_max_.raise_to(push_ticks);
        push_buckets_[push_bucket_index(push_ticks)].add(1);
        notify_ticks_total_.add(notify_ticks);
        notify_ticks_max_.raise_to(notify_ticks);
    });
    const std::uint64_t publish_ticks = monotonic_ticks() - publish_begin;

    // publisher_.publish() calls the sink at most once, so subtracting what
    // the sink measured leaves exactly the translation.
    const std::uint64_t accounted = push_ticks + notify_ticks;
    const std::uint64_t translate_ticks = publish_ticks > accounted ? publish_ticks - accounted : 0;
    events_seen_.add(1);
    translate_ticks_total_.add(translate_ticks);
    translate_ticks_max_.raise_to(translate_ticks);
    control_ticks_total_.add(control_ticks);
    control_ticks_max_.raise_to(control_ticks);
    control_buckets_[push_bucket_index(control_ticks)].add(1);
}

PublishCostStats MarketDataRouter::publish_cost() const {
    PublishCostStats stats{
        .events_seen = events_seen_.get(),
        .wire_events = wire_events_.get(),
        .push_ok = push_ok_.get(),
        .push_full = push_full_.get(),
        .push_ticks_total = push_ticks_total_.get(),
        .push_ticks_max = push_ticks_max_.get(),
        .notify_calls = notify_calls_.get(),
        .notify_ticks_total = notify_ticks_total_.get(),
        .notify_ticks_max = notify_ticks_max_.get(),
        .translate_ticks_total = translate_ticks_total_.get(),
        .translate_ticks_max = translate_ticks_max_.get(),
        .control_ticks_total = control_ticks_total_.get(),
        .control_ticks_max = control_ticks_max_.get(),
    };
    for (std::size_t bucket = 0; bucket < kPushBucketCount; ++bucket) {
        stats.push_buckets[bucket] = push_buckets_[bucket].get();
        stats.control_buckets[bucket] = control_buckets_[bucket].get();
    }
    return stats;
}

void MarketDataRouter::routing_loop(std::stop_token token) {
    set_calling_thread_name("mdh-md-router");
    while (true) {
        // Take whatever is queued, up to the batch cap, and stop at the first
        // empty pop rather than waiting for more. See the class comment: this
        // is what keeps a quiet feed's latency identical to the unbatched
        // path while still collapsing a busy one into far fewer datagrams.
        batch_.clear();
        while (batch_.size() < max_batch_) {
            auto event = queue_.try_pop();
            if (!event) {
                break;
            }
            batch_.push_back(std::move(*event));
        }

        if (!batch_.empty()) {
            downstream_(batch_);
            routed_.fetch_add(batch_.size(), std::memory_order_relaxed);
            batches_.fetch_add(1, std::memory_order_relaxed);
            if (batch_.size() > max_batch_observed_.load(std::memory_order_relaxed)) {
                max_batch_observed_.store(batch_.size(), std::memory_order_relaxed);
            }
            // Straight back round without considering sleep: the send that
            // just happened took long enough that more events may well have
            // arrived, and if the batch hit the cap there is certainly more.
            continue;
        }

        if (token.stop_requested()) {
            break; // queue was observed empty after stop: drain complete
        }

        cv_waits_.fetch_add(1, std::memory_order_relaxed);
        std::unique_lock<std::mutex> lock(wake_mutex_);
        wake_cv_.wait_for(lock, idle_timeout_, [&] {
            return token.stop_requested() || queue_.size() > 0;
        });
    }
}

} // namespace mdh::exchange::market_data
