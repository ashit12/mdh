#pragma once

#include <string>
#include <variant>

#include "book/book_manager.hpp"
#include "common/sequence_validator.hpp"
#include "protocol/errors.hpp"
#include "protocol/messages.hpp"
#include "replay/replay_stats.hpp"

namespace mdh::replay {

struct ReplayOptions {
    // Milestone 1 only implements "stop on first error" (per spec); these
    // flags exist so a later milestone can add a skip-and-continue policy
    // at this call site without changing run_replay()'s signature.
    bool stop_on_sequence_error = true;
    bool stop_on_decode_error = true;
};

struct ReplayOutcome {
    ReplayStats stats;
    book::BookManager books;
    bool stopped_early = false;
    std::string stop_reason;
};

// Reads `input_path` end to end (or until the first error, per `options`),
// validating sequencing and applying every event to a fresh BookManager.
// A free function rather than a class: it holds no state across calls, so
// there is nothing an object would buy beyond the return value itself.
[[nodiscard]] ReplayOutcome run_replay(const std::string& input_path, const ReplayOptions& options = {});

// Applies one already-decoded frame result (an Event or a DecodeError) to
// `outcome`: classifies the event's sequence number via `validator`,
// updates stats, and applies the event to `outcome.books`. Returns true if
// the caller should stop processing further frames (a stop-worthy error
// was hit, per `options`).
//
// Exposed publicly (run_replay() uses it internally for the file-replay
// loop) so a different transport can reuse the exact same decode-error /
// sequence-validation / book-application logic without duplicating it --
// see apps/market_data_replay's --listen mode, which drives this same
// function from a UDP receive loop instead of a file-read loop. Only the
// "where do frames come from" part differs between the two call sites.
[[nodiscard]] bool apply_frame_result(std::variant<protocol::Event, protocol::DecodeError> frame,
                                       SequenceValidator& validator,
                                       const ReplayOptions& options,
                                       ReplayOutcome& outcome);

} // namespace mdh::replay
