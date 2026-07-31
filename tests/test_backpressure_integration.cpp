#include <gtest/gtest.h>

#include <cstdint>
#include <variant>

#include "common/dropping_queue.hpp"
#include "protocol/messages.hpp"
#include "replay/replay_engine.hpp"

using namespace mdh;
using namespace mdh::protocol;
using namespace mdh::replay;

namespace {

using FrameResult = std::variant<Event, DecodeError>;

FrameResult make_add(std::uint64_t seq) {
    return FrameResult(Event{AddOrder{.sequence_number = seq,
                                       .timestamp_ns = seq,
                                       .order_id = seq,
                                       .instrument_id = 1,
                                       .price = 100,
                                       .quantity = 1,
                                       .side = Side::Buy}});
}

} // namespace

// This is the deterministic counterpart to
// UdpReplayE2E.SlowConsumerForcesDrops: rather than racing real UDP
// sockets and OS thread scheduling to reproduce "a drop shows up
// downstream as a detected sequence gap" (which turned out to be
// racy -- see that test's comment), this drives the exact same real
// components (DroppingQueue, replay::apply_frame_result,
// SequenceValidator) directly, single-threaded, with full control over
// exactly which sequence numbers get dropped and which survive.
TEST(BackpressureIntegration, DroppedFrameIsRevealedAsGapByALaterSurvivor) {
    DroppingQueue<FrameResult> queue(2); // tiny capacity: only 2 in flight at once

    ASSERT_TRUE(queue.push(make_add(1)));
    ASSERT_TRUE(queue.push(make_add(2)));
    // Queue is now full (capacity 2) -- sequences 3 and 4 are dropped,
    // exactly as a producer racing ahead of a slow consumer would drop them.
    EXPECT_FALSE(queue.push(make_add(3)));
    EXPECT_FALSE(queue.push(make_add(4)));
    EXPECT_EQ(queue.dropped_count(), 2u);

    SequenceValidator validator;
    ReplayOptions options;
    options.stop_on_sequence_error = false; // keep going so the gap is recorded, not treated as a halt condition
    ReplayOutcome outcome;

    auto item1 = queue.try_pop();
    ASSERT_TRUE(item1.has_value());
    EXPECT_FALSE(apply_frame_result(*item1, validator, options, outcome)); // sequence 1: InOrder

    auto item2 = queue.try_pop();
    ASSERT_TRUE(item2.has_value());
    EXPECT_FALSE(apply_frame_result(*item2, validator, options, outcome)); // sequence 2: InOrder

    EXPECT_FALSE(queue.try_pop().has_value()); // nothing more queued: 3 and 4 never made it in
    EXPECT_EQ(outcome.stats.sequence_failures, 0u); // no gap detected *yet* -- nothing has revealed it

    // Sequence 5 now arrives and is pushed successfully (the queue has
    // room again) -- this is the frame that actually reveals the drop: the
    // validator expected 3, got 5, and classifies that as Missing.
    ASSERT_TRUE(queue.push(make_add(5)));
    auto item5 = queue.try_pop();
    ASSERT_TRUE(item5.has_value());
    EXPECT_FALSE(apply_frame_result(*item5, validator, options, outcome));

    EXPECT_EQ(outcome.stats.sequence_failures, 1u);
    EXPECT_EQ(outcome.stats.messages_processed, 3u); // sequences 1, 2, 5 -- 3 and 4 were dropped, never processed
}

// The mirror case: if nothing ever arrives after the drop (the queue-full
// window happens to land on the very end of a burst), the gap is never
// detected -- not a bug, just an inherent limit of retrospective
// detection. This is exactly the scenario that made the real UDP test
// flaky; pinning it down here makes the limitation an explicit, tested
// fact instead of a surprise.
TEST(BackpressureIntegration, DropWithNoLaterSurvivorIsNeverDetected) {
    DroppingQueue<FrameResult> queue(2);

    ASSERT_TRUE(queue.push(make_add(1)));
    ASSERT_TRUE(queue.push(make_add(2)));
    EXPECT_FALSE(queue.push(make_add(3))); // dropped, and nothing with a higher sequence ever arrives after this

    SequenceValidator validator;
    ReplayOptions options;
    options.stop_on_sequence_error = false;
    ReplayOutcome outcome;

    auto item1 = queue.try_pop();
    ASSERT_TRUE(item1.has_value());
    EXPECT_FALSE(apply_frame_result(*item1, validator, options, outcome));
    auto item2 = queue.try_pop();
    ASSERT_TRUE(item2.has_value());
    EXPECT_FALSE(apply_frame_result(*item2, validator, options, outcome));

    EXPECT_EQ(queue.dropped_count(), 1u); // the drop genuinely happened...
    EXPECT_EQ(outcome.stats.sequence_failures, 0u); // ...but nothing ever surfaces it downstream
}
