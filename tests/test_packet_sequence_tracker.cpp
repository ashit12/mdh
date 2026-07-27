#include <gtest/gtest.h>

#include "net/packet_sequence_tracker.hpp"

using namespace mdh;
using namespace mdh::net;

TEST(PacketSequenceTracker, AllInOrderCountsOnlyInOrder) {
    PacketSequenceTracker tracker;
    (void)tracker.observe(1);
    (void)tracker.observe(2);
    (void)tracker.observe(3);

    const auto& stats = tracker.stats();
    EXPECT_EQ(stats.packets_seen, 3u);
    EXPECT_EQ(stats.in_order, 3u);
    EXPECT_EQ(stats.duplicate, 0u);
    EXPECT_EQ(stats.out_of_order, 0u);
    EXPECT_EQ(stats.missing_events, 0u);
}

TEST(PacketSequenceTracker, DuplicatePacketIsCountedSeparatelyFromInOrder) {
    PacketSequenceTracker tracker;
    (void)tracker.observe(1);
    (void)tracker.observe(1); // redundant-feed-style retransmit of the same packet

    const auto& stats = tracker.stats();
    EXPECT_EQ(stats.packets_seen, 2u);
    EXPECT_EQ(stats.in_order, 1u);
    EXPECT_EQ(stats.duplicate, 1u);
}

TEST(PacketSequenceTracker, GapIsCountedAsMissingButTrackerKeepsGoing) {
    PacketSequenceTracker tracker;
    (void)tracker.observe(1);
    (void)tracker.observe(5); // packets 2,3,4 never arrived
    auto after_gap = tracker.observe(6);

    const auto& stats = tracker.stats();
    EXPECT_EQ(stats.missing_events, 1u);
    // Observational only: the tracker doesn't stop or refuse further
    // packets after a gap -- unlike the event-level SequenceValidator
    // usage in ReplayEngine, there is no "halt" policy here.
    EXPECT_EQ(after_gap.outcome, SequenceOutcome::InOrder);
    EXPECT_EQ(stats.packets_seen, 3u);
}

TEST(PacketSequenceTracker, ObserveReturnsTheClassificationForCallerUse) {
    PacketSequenceTracker tracker;
    auto first = tracker.observe(10);
    EXPECT_EQ(first.outcome, SequenceOutcome::InOrder);

    auto reordered = tracker.observe(3);
    EXPECT_EQ(reordered.outcome, SequenceOutcome::OutOfOrder);
}
