#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <utility>

namespace mdh {

// A bounded, single-producer single-consumer ring buffer.
//
// Lock-free, and does not need a CAS loop: `head_` is written only by the
// producer, `tail_` is written only by the consumer, so neither atomic
// variable ever has concurrent writers. Each side does one atomic load of
// the *other* side's index (acquire) and one atomic store of its *own*
// index (release) per operation. That release/acquire pair is what makes
// this safe: the producer's release-store of `head_` (after constructing
// the new element) happens-before the consumer's acquire-load of `head_`
// sees it, so the consumer never observes a slot before it's fully
// constructed -- and symmetrically for the consumer freeing a slot before
// the producer reuses it. This is the classic SPSC design (see e.g.
// Rigtorp's spsc_queue); it would need a CAS loop only if more than one
// thread could write the same index, which SPSC rules out by contract.
//
// `head_`/`tail_` are each padded to their own cache line (`alignas(64)`)
// so the producer's writes to `head_` don't force the consumer's cache
// line holding `tail_` to bounce back and forth between cores (false
// sharing) -- they're logically independent counters, so they shouldn't
// share a cache line just because they're declared next to each other.
//
// try_push()/try_pop() only -- neither blocks. This is deliberate: a live
// UDP receive loop must never stall waiting for a slow consumer (that
// would mean not reading off the socket, i.e. dropping packets at the OS
// level instead of here, with no visibility into it happening). What a
// full queue's producer should do about it (drop, in this project's
// choice) is a policy decision made by the caller, not by the queue --
// see DroppingQueue<T> (common/dropping_queue.hpp), which wraps this class
// to add that policy plus a counter for how often it kicked in.
//
// size()/high_water_mark() are pure introspection (how full is this queue,
// how full has it ever been) and stay here rather than in DroppingQueue --
// they're facts about the queue itself, not about any policy layered on
// top of it.
template <typename T>
class SpscQueue {
public:
    // Capacity is rounded up to the next power of two so that wraparound
    // is `& mask_` instead of `% capacity_`. Capacity 0 is treated as 1.
    explicit SpscQueue(std::size_t capacity)
        : capacity_(next_power_of_two(capacity)), mask_(capacity_ - 1), storage_(alloc_.allocate(capacity_)) {}

    ~SpscQueue() {
        for (std::size_t i = tail_.load(std::memory_order_relaxed); i != head_.load(std::memory_order_relaxed); ++i) {
            std::destroy_at(storage_ + (i & mask_));
        }
        alloc_.deallocate(storage_, capacity_);
    }

    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;
    SpscQueue(SpscQueue&&) = delete;
    SpscQueue& operator=(SpscQueue&&) = delete;

    // Producer side only. Returns false, leaving `value` unconsumed, if
    // the queue is currently full.
    [[nodiscard]] bool try_push(T value) {
        const std::size_t head = head_.load(std::memory_order_relaxed); // only the producer writes head_
        const std::size_t tail = tail_.load(std::memory_order_acquire); // syncs with consumer's destroy_at before this slot is reused
        if (head - tail >= capacity_) {
            return false; // full
        }
        std::construct_at(storage_ + (head & mask_), std::move(value));

        const std::size_t new_occupancy = head + 1 - tail;
        if (new_occupancy > high_water_mark_.load(std::memory_order_relaxed)) {
            high_water_mark_.store(new_occupancy, std::memory_order_relaxed); // only the producer writes this, so no CAS needed
        }

        head_.store(head + 1, std::memory_order_release); // publishes the constructed slot to the consumer
        return true;
    }

    // Consumer side only. Returns std::nullopt if the queue is currently
    // empty.
    [[nodiscard]] std::optional<T> try_pop() {
        const std::size_t tail = tail_.load(std::memory_order_relaxed); // only the consumer writes tail_
        const std::size_t head = head_.load(std::memory_order_acquire); // syncs with producer's construct_at before we read this slot
        if (tail == head) {
            return std::nullopt; // empty
        }
        T value = std::move(*(storage_ + (tail & mask_)));
        std::destroy_at(storage_ + (tail & mask_));
        tail_.store(tail + 1, std::memory_order_release); // publishes the freed slot to the producer
        return value;
    }

    [[nodiscard]] std::size_t capacity() const { return capacity_; }

    // Current occupancy. A best-effort snapshot, not a synchronized read
    // paired with any specific push/pop -- valid at some instant between
    // the two loads below, which may already be stale by the time the
    // caller acts on it if the other side is concurrently pushing/popping.
    // Fine for diagnostics/metrics; not meant to gate correctness.
    [[nodiscard]] std::size_t size() const {
        const std::size_t head = head_.load(std::memory_order_acquire);
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        return head - tail;
    }

    // Largest occupancy ever observed (monotonically non-decreasing, even
    // as the queue drains back down) -- "how full has this queue ever
    // gotten," for spotting a consumer that's falling behind.
    [[nodiscard]] std::size_t high_water_mark() const { return high_water_mark_.load(std::memory_order_acquire); }

private:
    static std::size_t next_power_of_two(std::size_t n) {
        std::size_t p = 1;
        while (p < n) {
            p <<= 1;
        }
        return p;
    }

    std::size_t capacity_;
    std::size_t mask_;
    std::allocator<T> alloc_;
    T* storage_;
    std::atomic<std::size_t> high_water_mark_{0}; // written only by the producer (in try_push); no false-sharing
                                                   // concern with head_ since the same thread writes both

    alignas(64) std::atomic<std::size_t> head_{0}; // next slot the producer will write
    alignas(64) std::atomic<std::size_t> tail_{0}; // next slot the consumer will read
};

} // namespace mdh
