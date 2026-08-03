#pragma once

#include <optional>

#include "common/types.hpp"

namespace mdh {

enum class SequenceOutcome {
    InOrder,
    Duplicate,       // seq == last seen seq
    OutOfOrder,      // seq < last seen seq, and not a plain duplicate
    Missing,         // seq > last seen seq + 1: one or more sequence numbers skipped
};

struct SequenceCheck {
    SequenceOutcome outcome;
    Sequence observed;
    // Only meaningful for Missing: the inclusive range of sequence numbers
    // that were skipped, [expected, observed - 1].
    Sequence expected = 0;
};

// Tracks a sequence number and classifies each new one relative to the
// last one accepted. Deliberately stateless beyond "what was the last
// sequence I saw" -- it cannot distinguish a duplicate of an old (not
// immediately preceding) sequence from a general out-of-order arrival,
// since that would require remembering the full set of sequence numbers
// seen so far. That's a documented simplification: see docs/protocol.md.
//
// Generic enough to track two independent things in this project: the
// event-level sequence_number carried by every message (see
// replay/replay_engine.cpp, where it is the actual correctness gate for
// book reconstruction), and the transport-level packet_sequence carried by
// each UDP packet (see net/packet_sequence_tracker.hpp, where it is
// purely observational/diagnostic and does not gate anything). Two
// independent SequenceValidator instances track these two unrelated
// sequences; neither knows about the other.
//
// The validator only classifies; it does not decide what to do about a
// non-InOrder outcome. That policy decision belongs to the caller, which
// keeps this class reusable if a later milestone wants a configurable
// recovery policy (skip-and-continue, buffer-and-reorder, etc.) without
// changing this interface.
class SequenceValidator {
public:
    [[nodiscard]] SequenceCheck check(Sequence seq);

    // Declares a new baseline directly, bypassing classification -- used
    // by sequence-gap recovery (replay::apply_frame_result) after loading
    // a snapshot: the event that triggered recovery becomes the new
    // "last seen", so the *next* one is checked normally against it. This
    // is not a classification decision (there is no outcome to report);
    // it is the caller declaratively overriding the baseline, which
    // check() has no way to express since it always classifies relative
    // to prior state.
    void reset(Sequence seq) { last_seen_ = seq; }

private:
    std::optional<Sequence> last_seen_;
};

} // namespace mdh
