#pragma once

#include <cstdint>

#include "common/sequence_validator.hpp"

namespace mdh::net {

// Tracks duplicate/out-of-order/missing UDP *packets* (by PacketHeader's
// packet_sequence) as running counts. This is purely observational/
// diagnostic -- it does NOT gate book reconstruction. UDP packets can
// legitimately arrive out of order or duplicated for reasons unrelated to
// data correctness (different network paths, retransmits, redundant
// feeds), whereas the actual correctness gate remains the existing
// event-level SequenceValidator inside ReplayEngine, which validates each
// event's own sequence_number regardless of which packet carried it. Two
// independent SequenceValidator instances track these two unrelated
// sequences.
struct PacketSequenceStats {
    std::uint64_t packets_seen = 0;
    std::uint64_t in_order = 0;
    std::uint64_t duplicate = 0;
    std::uint64_t out_of_order = 0;
    std::uint64_t missing_events = 0; // count of gaps observed, not count of missing packets
};

class PacketSequenceTracker {
public:
    // Classifies packet_sequence, updates the running stats, and returns
    // the classification -- in case a caller wants to log or react to a
    // specific outcome (e.g. bump a separate metric). Nothing in this
    // project uses the return value to alter control flow; it is always
    // used purely as a stat.
    SequenceCheck observe(Sequence packet_sequence) {
        const auto check = validator_.check(packet_sequence);
        ++stats_.packets_seen;
        switch (check.outcome) {
            case SequenceOutcome::InOrder:
                ++stats_.in_order;
                break;
            case SequenceOutcome::Duplicate:
                ++stats_.duplicate;
                break;
            case SequenceOutcome::OutOfOrder:
                ++stats_.out_of_order;
                break;
            case SequenceOutcome::Missing:
                ++stats_.missing_events;
                break;
        }
        return check;
    }

    [[nodiscard]] const PacketSequenceStats& stats() const { return stats_; }

private:
    SequenceValidator validator_;
    PacketSequenceStats stats_;
};

} // namespace mdh::net
