#pragma once

#include <atomic>
#include <cstddef>
#include <optional>
#include <utility>

#include "common/spsc_queue.hpp"

namespace mdh {

// Wraps SpscQueue<T> with this project's chosen backpressure policy: drop
// the newest item when the queue is full, and count how many times that
// happened. SpscQueue itself stays policy-free (see its own doc comment)
// -- this is the one place that turns a failed try_push into a decision,
// mirroring how PacketSequenceTracker wraps SequenceValidator to turn a
// sequence classification into an observational count rather than baking
// that decision into the classifier itself.
//
// Single producer, single consumer, same as the underlying SpscQueue --
// push() is producer-only. try_pop()/size()/high_water_mark()/
// dropped_count() are safe to call from the consumer or a third
// (metrics-reporting) thread, as best-effort snapshots.
template <typename T>
class DroppingQueue {
public:
    explicit DroppingQueue(std::size_t capacity) : queue_(capacity) {}

    // Producer side only. Returns false if `value` was dropped (the queue
    // was full) -- the caller has nothing further to do in that case, the
    // value is already gone, exactly like a failed SpscQueue::try_push.
    bool push(T value) {
        if (queue_.try_push(std::move(value))) {
            return true;
        }
        dropped_count_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    [[nodiscard]] std::optional<T> try_pop() { return queue_.try_pop(); }

    [[nodiscard]] std::size_t capacity() const { return queue_.capacity(); }
    [[nodiscard]] std::size_t size() const { return queue_.size(); }
    [[nodiscard]] std::size_t high_water_mark() const { return queue_.high_water_mark(); }
    [[nodiscard]] std::size_t dropped_count() const { return dropped_count_.load(std::memory_order_relaxed); }

private:
    SpscQueue<T> queue_;
    std::atomic<std::size_t> dropped_count_{0}; // written only by the producer thread, via push()
};

} // namespace mdh
