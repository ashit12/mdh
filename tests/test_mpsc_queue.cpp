#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "common/mpsc_queue.hpp"

using namespace mdh;

TEST(MpscQueue, CapacityRoundsUpToPowerOfTwo) {
    EXPECT_EQ(MpscQueue<int>(1).capacity(), 1u);
    EXPECT_EQ(MpscQueue<int>(5).capacity(), 8u);
}

TEST(MpscQueue, PushThenPopReturnsSameValue) {
    MpscQueue<int> q(4);
    ASSERT_TRUE(q.try_push(42));
    auto popped = q.try_pop();
    ASSERT_TRUE(popped.has_value());
    EXPECT_EQ(*popped, 42);
}

TEST(MpscQueue, TryPushFailsWhenFullThenSucceedsAfterPop) {
    MpscQueue<int> q(4);
    EXPECT_TRUE(q.try_push(1));
    EXPECT_TRUE(q.try_push(2));
    EXPECT_TRUE(q.try_push(3));
    EXPECT_TRUE(q.try_push(4));
    EXPECT_FALSE(q.try_push(5));

    auto popped = q.try_pop();
    ASSERT_TRUE(popped.has_value());
    EXPECT_EQ(*popped, 1);
    EXPECT_TRUE(q.try_push(5));
}

TEST(MpscQueue, FifoOrderPreservedAcrossWraparound) {
    MpscQueue<int> q(4);
    for (int cycle = 0; cycle < 5; ++cycle) {
        ASSERT_TRUE(q.try_push(cycle * 10 + 1));
        ASSERT_TRUE(q.try_push(cycle * 10 + 2));
        ASSERT_TRUE(q.try_push(cycle * 10 + 3));
        EXPECT_EQ(*q.try_pop(), cycle * 10 + 1);
        EXPECT_EQ(*q.try_pop(), cycle * 10 + 2);
        EXPECT_EQ(*q.try_pop(), cycle * 10 + 3);
    }
}

TEST(MpscQueue, DestructorCleansUpElementsStillInQueue) {
    auto tracker = std::make_shared<int>(42);
    {
        MpscQueue<std::shared_ptr<int>> q(4);
        ASSERT_TRUE(q.try_push(tracker));
        ASSERT_TRUE(q.try_push(tracker));
        EXPECT_EQ(tracker.use_count(), 3);
    }
    EXPECT_EQ(tracker.use_count(), 1);
}

TEST(MpscQueue, TwoProducersPreservePerProducerFifo) {
    constexpr int kPerProducer = 50'000;
    MpscQueue<std::pair<int, int>> q(1024);

    std::jthread a([&] {
        for (int i = 0; i < kPerProducer; ++i) {
            while (!q.try_push({0, i})) {
                std::this_thread::yield();
            }
        }
    });
    std::jthread b([&] {
        for (int i = 0; i < kPerProducer; ++i) {
            while (!q.try_push({1, i})) {
                std::this_thread::yield();
            }
        }
    });

    int last[2] = {-1, -1};
    int seen = 0;
    while (seen < kPerProducer * 2) {
        auto item = q.try_pop();
        if (!item) {
            std::this_thread::yield();
            continue;
        }
        EXPECT_EQ(item->second, last[item->first] + 1);
        last[item->first] = item->second;
        ++seen;
    }
    EXPECT_EQ(last[0], kPerProducer - 1);
    EXPECT_EQ(last[1], kPerProducer - 1);
}

TEST(MpscQueue, TwoProducersOneConsumerNoLossOrDuplication) {
    constexpr int kPerProducer = 100'000;
    MpscQueue<int> q(256);
    std::atomic<int> produced{0};

    std::jthread a([&] {
        for (int i = 0; i < kPerProducer; ++i) {
            while (!q.try_push(i)) {
                std::this_thread::yield();
            }
            produced.fetch_add(1, std::memory_order_relaxed);
        }
    });
    std::jthread b([&] {
        for (int i = 0; i < kPerProducer; ++i) {
            while (!q.try_push(i)) {
                std::this_thread::yield();
            }
            produced.fetch_add(1, std::memory_order_relaxed);
        }
    });

    int consumed = 0;
    while (consumed < kPerProducer * 2) {
        if (q.try_pop()) {
            ++consumed;
        } else {
            std::this_thread::yield();
        }
    }
    EXPECT_EQ(produced.load(), kPerProducer * 2);
    EXPECT_EQ(consumed, kPerProducer * 2);
}
