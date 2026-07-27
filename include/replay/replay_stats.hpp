#pragma once

#include <cstdint>

namespace mdh::replay {

// Purely observational counters -- nothing here is a target/threshold, and
// nothing here is a fabricated benchmark number. duration_ns is measured by
// the caller (ReplayEngine::run) with std::chrono::steady_clock around the
// actual replay loop.
struct ReplayStats {
    std::uint64_t messages_processed = 0;
    std::uint64_t decode_failures = 0;
    std::uint64_t sequence_failures = 0;

    std::uint64_t adds = 0;
    std::uint64_t cancels = 0;
    std::uint64_t modifies = 0;
    std::uint64_t trades = 0;
    std::uint64_t clears = 0;

    std::uint64_t book_errors = 0;

    std::uint64_t duration_ns = 0;

    [[nodiscard]] double messages_per_second() const {
        if (duration_ns == 0) {
            return 0.0;
        }
        return static_cast<double>(messages_processed) / (static_cast<double>(duration_ns) / 1e9);
    }
};

} // namespace mdh::replay
