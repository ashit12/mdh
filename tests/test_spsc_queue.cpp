#include <gtest/gtest.h>

#include <memory>
#include <thread>
#include <vector>

#include "common/spsc_queue.hpp"

using namespace mdh;

TEST(SpscQueue, CapacityRoundsUpToPowerOfTwo) {
    EXPECT_EQ(SpscQueue<int>(1).capacity(), 1u);
    EXPECT_EQ(SpscQueue<int>(4).capacity(), 4u);
    EXPECT_EQ(SpscQueue<int>(5).capacity(), 8u);
    EXPECT_EQ(SpscQueue<int>(9).capacity(), 16u);
}

TEST(SpscQueue, TryPopReturnsNulloptWhenEmpty) {
    SpscQueue<int> q(4);
    EXPECT_FALSE(q.try_pop().has_value());
}

TEST(SpscQueue, PushThenPopReturnsSameValue) {
    SpscQueue<int> q(4);
    ASSERT_TRUE(q.try_push(42));
    auto popped = q.try_pop();
    ASSERT_TRUE(popped.has_value());
    EXPECT_EQ(*popped, 42);
}

TEST(SpscQueue, TryPushFailsWhenFullThenSucceedsAfterPop) {
    SpscQueue<int> q(4);
    EXPECT_TRUE(q.try_push(1));
    EXPECT_TRUE(q.try_push(2));
    EXPECT_TRUE(q.try_push(3));
    EXPECT_TRUE(q.try_push(4));
    EXPECT_FALSE(q.try_push(5)); // full: capacity is 4, all 4 slots occupied

    auto popped = q.try_pop();
    ASSERT_TRUE(popped.has_value());
    EXPECT_EQ(*popped, 1);

    EXPECT_TRUE(q.try_push(5)); // space freed by the pop above
}

TEST(SpscQueue, SizeTracksCurrentOccupancy) {
    SpscQueue<int> q(4);
    EXPECT_EQ(q.size(), 0u);

    ASSERT_TRUE(q.try_push(1));
    EXPECT_EQ(q.size(), 1u);
    ASSERT_TRUE(q.try_push(2));
    EXPECT_EQ(q.size(), 2u);

    ASSERT_TRUE(q.try_pop().has_value());
    EXPECT_EQ(q.size(), 1u);
    ASSERT_TRUE(q.try_pop().has_value());
    EXPECT_EQ(q.size(), 0u);
}

TEST(SpscQueue, HighWaterMarkTracksPeakAndDoesNotDecreaseOnPop) {
    SpscQueue<int> q(4);
    EXPECT_EQ(q.high_water_mark(), 0u);

    ASSERT_TRUE(q.try_push(1));
    ASSERT_TRUE(q.try_push(2));
    ASSERT_TRUE(q.try_push(3));
    EXPECT_EQ(q.high_water_mark(), 3u);

    // Draining back down must not lower the recorded peak.
    ASSERT_TRUE(q.try_pop().has_value());
    ASSERT_TRUE(q.try_pop().has_value());
    EXPECT_EQ(q.size(), 1u);
    EXPECT_EQ(q.high_water_mark(), 3u);

    // A later push that doesn't exceed the prior peak shouldn't change it.
    ASSERT_TRUE(q.try_push(4));
    EXPECT_EQ(q.high_water_mark(), 3u);
}

TEST(SpscQueue, HighWaterMarkReachesCapacityWhenFull) {
    SpscQueue<int> q(4);
    ASSERT_TRUE(q.try_push(1));
    ASSERT_TRUE(q.try_push(2));
    ASSERT_TRUE(q.try_push(3));
    ASSERT_TRUE(q.try_push(4));
    EXPECT_EQ(q.high_water_mark(), q.capacity());

    EXPECT_FALSE(q.try_push(5)); // rejected: must not count towards occupancy or the peak
    EXPECT_EQ(q.size(), 4u);
    EXPECT_EQ(q.high_water_mark(), 4u);
}

TEST(SpscQueue, FifoOrderPreservedAcrossWraparound) {
    SpscQueue<int> q(4);
    // Push/pop repeatedly past the physical buffer size (4) to exercise
    // index wraparound -- proves `& mask_` is wrapping correctly rather
    // than aliasing into a slot that's still logically occupied.
    for (int cycle = 0; cycle < 5; ++cycle) {
        ASSERT_TRUE(q.try_push(cycle * 10 + 1));
        ASSERT_TRUE(q.try_push(cycle * 10 + 2));
        ASSERT_TRUE(q.try_push(cycle * 10 + 3));

        auto a = q.try_pop();
        auto b = q.try_pop();
        auto c = q.try_pop();
        ASSERT_TRUE(a.has_value());
        ASSERT_TRUE(b.has_value());
        ASSERT_TRUE(c.has_value());
        EXPECT_EQ(*a, cycle * 10 + 1);
        EXPECT_EQ(*b, cycle * 10 + 2);
        EXPECT_EQ(*c, cycle * 10 + 3);
    }
}

TEST(SpscQueue, PoppedElementIsMovedNotCopied) {
    auto tracker = std::make_shared<int>(1);
    SpscQueue<std::shared_ptr<int>> q(4);

    ASSERT_TRUE(q.try_push(tracker));
    EXPECT_EQ(tracker.use_count(), 2); // tracker itself + the copy now inside the queue

    auto popped = q.try_pop();
    ASSERT_TRUE(popped.has_value());
    EXPECT_EQ(tracker.use_count(), 2); // ownership moved out of the queue's slot, not copied again

    popped.reset();
    EXPECT_EQ(tracker.use_count(), 1); // only `tracker` remains once the popped value is dropped
}

TEST(SpscQueue, DestructorCleansUpElementsStillInQueue) {
    auto tracker = std::make_shared<int>(42);
    {
        SpscQueue<std::shared_ptr<int>> q(4);
        ASSERT_TRUE(q.try_push(tracker));
        ASSERT_TRUE(q.try_push(tracker));
        EXPECT_EQ(tracker.use_count(), 3); // tracker + 2 copies left sitting in the queue
    } // ~SpscQueue() must destroy both remaining elements here
    EXPECT_EQ(tracker.use_count(), 1); // both copies released; no leak, no double-destroy crash
}

// ── Real concurrency (as opposed to the single-threaded tests above) ──────
//
// Everything above exercises try_push/try_pop from one thread, which never
// touches the actual reason this queue is lock-free instead of mutex-based:
// the producer's release-store of head_ / consumer's release-store of
// tail_, and the acquire-loads on the other side, only matter when two
// real threads are racing. These two tests are the ones that would catch a
// memory-ordering mistake (e.g. relaxed where acquire/release was needed)
// -- run them under ThreadSanitizer (MDH_ENABLE_TSAN) to actually check
// that, since a single-threaded run can't exercise the race at all.
//
// The producer here spin-retries on a full queue rather than dropping --
// that's deliberate: these tests validate the queue's own mechanics
// (nothing lost, nothing duplicated, FIFO order held), independent of the
// drop-on-full backpressure policy that's layered on top in a later step.

TEST(SpscQueue, ConcurrentProducerConsumerPreservesFifoOrderAndCount) {
    constexpr int kCount = 1'000'000;
    SpscQueue<int> q(64); // small on purpose: forces frequent full/empty contention and wraparound

    std::vector<int> received;
    received.reserve(kCount);

    {
        std::jthread producer([&] {
            for (int i = 0; i < kCount; ++i) {
                while (!q.try_push(i)) {
                    std::this_thread::yield();
                }
            }
        });
        std::jthread consumer([&] {
            while (received.size() < static_cast<std::size_t>(kCount)) {
                if (auto v = q.try_pop()) {
                    received.push_back(*v);
                } else {
                    std::this_thread::yield();
                }
            }
        });
    } // both jthreads joined here, before we inspect `received`

    ASSERT_EQ(received.size(), static_cast<std::size_t>(kCount));
    for (int i = 0; i < kCount; ++i) {
        ASSERT_EQ(received[static_cast<std::size_t>(i)], i) << "FIFO order violated at index " << i;
    }
}

TEST(SpscQueue, ConcurrentProducerConsumerWithMoveOnlyElements) {
    constexpr int kCount = 200'000;
    SpscQueue<std::unique_ptr<int>> q(32);

    int consumed_count = 0;
    long long checksum = 0; // only ever touched by the consumer thread

    {
        std::jthread producer([&] {
            for (int i = 0; i < kCount; ++i) {
                // A fresh unique_ptr per retry attempt, not a reused moved-from
                // one: try_push takes T by value, so std::move()-ing the same
                // ptr into a failed attempt already nulled it out at the call
                // site, regardless of whether the push actually happened. That
                // is deliberate -- it's what makes drop-on-full "free" (a
                // failed push already discarded its argument, so the caller
                // has nothing left to reinsert) -- but it does mean a caller
                // that wants retry semantics must supply a new value each try,
                // exactly as this loop does.
                while (!q.try_push(std::make_unique<int>(i))) {
                    std::this_thread::yield();
                }
            }
        });
        std::jthread consumer([&] {
            while (consumed_count < kCount) {
                if (auto v = q.try_pop()) {
                    checksum += **v;
                    ++consumed_count;
                } else {
                    std::this_thread::yield();
                }
            }
        });
    } // both jthreads joined here, before we inspect consumed_count/checksum

    EXPECT_EQ(consumed_count, kCount);
    const long long expected_checksum = static_cast<long long>(kCount - 1) * kCount / 2; // sum 0..kCount-1
    EXPECT_EQ(checksum, expected_checksum);
}
