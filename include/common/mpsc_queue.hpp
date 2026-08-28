#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <optional>
#include <utility>

namespace mdh {

// A bounded, multi-producer single-consumer ring buffer.
//
// Producers reserve a slot with a CAS on `head_` (no mutex). The consumer
// is the only thread that advances `tail_`. A producer that reserved slot
// `i` may finish after the producer that reserved `i+1`, so the consumer
// does not treat `head_` as "readable": it waits for that slot's
// `published` ticket, the same generation trick a disruptor uses.
//
// FIFO order is the order of successful reservations. One thread pushing
// twice therefore preserves its own order; two threads race on the CAS,
// and the winner is ahead in line. That race *is* the cross-producer
// total order -- there is no second mutex to disagree with it.
//
// try_push()/try_pop() only; a full queue returns false without dropping
// the caller's value. Capacity rounds up to a power of two.
template <typename T>
class MpscQueue {
public:
    explicit MpscQueue(std::size_t capacity)
        : capacity_(next_power_of_two(capacity)), mask_(capacity_ - 1), slots_(alloc_.allocate(capacity_)) {
        for (std::size_t i = 0; i < capacity_; ++i) {
            std::construct_at(&slots_[i]);
        }
    }

    ~MpscQueue() {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        while (tail != head) {
            Slot& slot = slots_[tail & mask_];
            while (slot.published.load(std::memory_order_acquire) != tail + 1) {
            }
            std::destroy_at(std::launder(reinterpret_cast<T*>(&slot.storage)));
            ++tail;
        }
        for (std::size_t i = 0; i < capacity_; ++i) {
            std::destroy_at(&slots_[i]);
        }
        alloc_.deallocate(slots_, capacity_);
    }

    MpscQueue(const MpscQueue&) = delete;
    MpscQueue& operator=(const MpscQueue&) = delete;
    MpscQueue(MpscQueue&&) = delete;
    MpscQueue& operator=(MpscQueue&&) = delete;

    // Any number of producer threads. Returns false, leaving `value`
    // unconsumed, if the queue is currently full.
    [[nodiscard]] bool try_push(T value) {
        std::size_t head = head_.load(std::memory_order_relaxed);
        for (;;) {
            const std::size_t tail = tail_.load(std::memory_order_acquire);
            if (head - tail >= capacity_) {
                return false;
            }
            if (head_.compare_exchange_weak(head, head + 1, std::memory_order_relaxed,
                                            std::memory_order_relaxed)) {
                break;
            }
        }

        Slot& slot = slots_[head & mask_];
        std::construct_at(reinterpret_cast<T*>(&slot.storage), std::move(value));
        slot.published.store(head + 1, std::memory_order_release);

        const std::size_t occupancy = head + 1 - tail_.load(std::memory_order_relaxed);
        std::size_t peak = high_water_mark_.load(std::memory_order_relaxed);
        while (occupancy > peak &&
               !high_water_mark_.compare_exchange_weak(peak, occupancy, std::memory_order_relaxed)) {
        }
        return true;
    }

    // Consumer side only.
    [[nodiscard]] std::optional<T> try_pop() {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        Slot& slot = slots_[tail & mask_];
        if (slot.published.load(std::memory_order_acquire) != tail + 1) {
            return std::nullopt;
        }
        T* ptr = std::launder(reinterpret_cast<T*>(&slot.storage));
        T value = std::move(*ptr);
        std::destroy_at(ptr);
        slot.published.store(0, std::memory_order_relaxed);
        tail_.store(tail + 1, std::memory_order_release);
        return value;
    }

    [[nodiscard]] std::size_t capacity() const { return capacity_; }

    [[nodiscard]] std::size_t size() const {
        const std::size_t head = head_.load(std::memory_order_acquire);
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        return head - tail;
    }

    [[nodiscard]] std::size_t high_water_mark() const { return high_water_mark_.load(std::memory_order_acquire); }

private:
    struct Slot {
        std::atomic<std::size_t> published{0};
        alignas(T) std::byte storage[sizeof(T)];
    };

    static std::size_t next_power_of_two(std::size_t n) {
        std::size_t p = 1;
        while (p < n) {
            p <<= 1;
        }
        return p;
    }

    std::size_t capacity_;
    std::size_t mask_;
    std::allocator<Slot> alloc_;
    Slot* slots_;
    std::atomic<std::size_t> high_water_mark_{0};

    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::size_t> tail_{0};
};

} // namespace mdh
