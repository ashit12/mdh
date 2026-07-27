#include "common/sequence_validator.hpp"

namespace mdh {

SequenceCheck SequenceValidator::check(Sequence seq) {
    if (!last_seen_) {
        // First message establishes the baseline; there is no prior
        // sequence to compare against, so we cannot detect a gap before
        // the very first observed message (would need an externally
        // supplied "expected start", which this project does not have).
        last_seen_ = seq;
        return SequenceCheck{.outcome = SequenceOutcome::InOrder, .observed = seq};
    }

    const Sequence last = *last_seen_;

    if (seq == last + 1) {
        last_seen_ = seq;
        return SequenceCheck{.outcome = SequenceOutcome::InOrder, .observed = seq};
    }
    if (seq == last) {
        return SequenceCheck{.outcome = SequenceOutcome::Duplicate, .observed = seq};
    }
    if (seq < last) {
        return SequenceCheck{.outcome = SequenceOutcome::OutOfOrder, .observed = seq};
    }

    // seq > last + 1: one or more sequence numbers were skipped. Advance
    // last_seen_ to seq anyway so later messages are checked against the
    // new high-water mark rather than re-reporting the same gap forever.
    const Sequence expected = last + 1;
    last_seen_ = seq;
    return SequenceCheck{.outcome = SequenceOutcome::Missing, .observed = seq, .expected = expected};
}

} // namespace mdh
