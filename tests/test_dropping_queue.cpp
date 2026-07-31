#include <gtest/gtest.h>

#include "common/dropping_queue.hpp"

using namespace mdh;

TEST(DroppingQueue, PushSucceedsAndCountsNothingWhenNotFull) {
    DroppingQueue<int> q(4);
    EXPECT_TRUE(q.push(1));
    EXPECT_TRUE(q.push(2));
    EXPECT_EQ(q.dropped_count(), 0u);
    EXPECT_EQ(q.size(), 2u);
}

TEST(DroppingQueue, PushDropsAndCountsWhenFull) {
    DroppingQueue<int> q(4);
    ASSERT_TRUE(q.push(1));
    ASSERT_TRUE(q.push(2));
    ASSERT_TRUE(q.push(3));
    ASSERT_TRUE(q.push(4));

    EXPECT_FALSE(q.push(5)); // dropped: queue full
    EXPECT_EQ(q.dropped_count(), 1u);
    EXPECT_EQ(q.size(), 4u); // the dropped item never entered the queue

    EXPECT_FALSE(q.push(6)); // dropped again
    EXPECT_EQ(q.dropped_count(), 2u);
}

TEST(DroppingQueue, PoppingFreesSpaceSoPushSucceedsAgain) {
    DroppingQueue<int> q(4);
    ASSERT_TRUE(q.push(1));
    ASSERT_TRUE(q.push(2));
    ASSERT_TRUE(q.push(3));
    ASSERT_TRUE(q.push(4));
    EXPECT_FALSE(q.push(5));
    EXPECT_EQ(q.dropped_count(), 1u);

    auto popped = q.try_pop();
    ASSERT_TRUE(popped.has_value());
    EXPECT_EQ(*popped, 1);

    EXPECT_TRUE(q.push(5)); // space freed by the pop above
    EXPECT_EQ(q.dropped_count(), 1u); // unchanged: this push succeeded
}

TEST(DroppingQueue, HighWaterMarkAndCapacityPassThroughToUnderlyingQueue) {
    DroppingQueue<int> q(5); // rounds up to 8
    EXPECT_EQ(q.capacity(), 8u);

    ASSERT_TRUE(q.push(1));
    ASSERT_TRUE(q.push(2));
    ASSERT_TRUE(q.push(3));
    EXPECT_EQ(q.high_water_mark(), 3u);

    ASSERT_TRUE(q.try_pop().has_value());
    EXPECT_EQ(q.high_water_mark(), 3u); // draining doesn't lower the peak
}
