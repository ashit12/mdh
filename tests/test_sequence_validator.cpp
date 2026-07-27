#include <gtest/gtest.h>

#include "common/sequence_validator.hpp"

using namespace mdh;

TEST(SequenceValidator, FirstSequenceIsAlwaysInOrder) {
    SequenceValidator v;
    auto r = v.check(1);
    EXPECT_EQ(r.outcome, SequenceOutcome::InOrder);
    EXPECT_EQ(r.observed, 1u);
}

TEST(SequenceValidator, ConsecutiveSequencesAreInOrder) {
    SequenceValidator v;
    (void)v.check(1);
    (void)v.check(2);
    auto r = v.check(3);
    EXPECT_EQ(r.outcome, SequenceOutcome::InOrder);
}

TEST(SequenceValidator, RepeatOfLastSequenceIsDuplicate) {
    SequenceValidator v;
    (void)v.check(1);
    (void)v.check(2);
    auto r = v.check(2);
    EXPECT_EQ(r.outcome, SequenceOutcome::Duplicate);
    EXPECT_EQ(r.observed, 2u);
}

TEST(SequenceValidator, SkippedSequenceIsMissingWithExpectedRange) {
    SequenceValidator v;
    (void)v.check(1);
    auto r = v.check(5);
    EXPECT_EQ(r.outcome, SequenceOutcome::Missing);
    EXPECT_EQ(r.expected, 2u);
    EXPECT_EQ(r.observed, 5u);
}

TEST(SequenceValidator, SequenceGoingBackwardsIsOutOfOrder) {
    SequenceValidator v;
    (void)v.check(1);
    (void)v.check(2);
    (void)v.check(3);
    auto r = v.check(2);
    EXPECT_EQ(r.outcome, SequenceOutcome::OutOfOrder);
    EXPECT_EQ(r.observed, 2u);
}

TEST(SequenceValidator, ValidatorRecoversHighWaterMarkAfterGap) {
    SequenceValidator v;
    (void)v.check(1);
    (void)v.check(5); // gap: 2,3,4 missing
    auto r = v.check(6);
    EXPECT_EQ(r.outcome, SequenceOutcome::InOrder);
}
