#pragma once

#include <optional>
#include <string>
#include <variant>

#include "book/book_manager.hpp"
#include "common/sequence_validator.hpp"
#include "protocol/errors.hpp"
#include "protocol/messages.hpp"
#include "replay/replay_stats.hpp"

namespace mdh::replay {

struct ReplayOptions {
    // Only "stop on the first error" is implemented. These flags exist so a
    // skip-and-continue policy can be added at this call site later without
    // changing run_replay()'s signature.
    bool stop_on_sequence_error = true;
    bool stop_on_decode_error = true;

    // If set, a detected gap -- a genuine Missing, not a duplicate or a
    // reorder -- loads this snapshot and resumes instead of stopping.
    //
    // Reloaded from disk every time recovery triggers, so several gaps in
    // one run all reload the same file, which is only as fresh as whenever
    // it was written. A real system would fetch a current snapshot per
    // recovery; that needs snapshot-serving infrastructure this project does
    // not model.
    std::optional<std::string> recovery_snapshot_path{};
};

struct ReplayOutcome {
    ReplayStats stats;
    book::BookManager books;
    bool stopped_early = false;
    std::string stop_reason;

    // The sequence_number of the most recently applied event, if any --
    // "as of what point does `books` reflect reality." Used to tag a
    // snapshot written from this outcome (see market_data_replay's
    // --snapshot-out) with the sequence recovery should resume from.
    std::optional<Sequence> last_sequence_number;
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
//
// Only a genuine gap triggers snapshot recovery, and only when
// `options.recovery_snapshot_path` is set. Duplicates and reorders keep
// their ordinary stop_on_sequence_error behaviour: a duplicate is no
// evidence anything was lost, so reprocessing it would be wrong and a full
// book reset would be a pure regression, while a reordered-but-not-missing
// message calls for buffer-and-reorder in a real system rather than a reset.
//
// On a genuine gap with a snapshot configured, the snapshot is loaded,
// `outcome.books` is replaced with its state, and the event that revealed
// the gap becomes the new validator baseline -- there is no way to fill in
// what happened between the snapshot's sequence and that event without a
// retransmission service, which is out of scope. The event is then applied
// normally. If it references an order that only existed during the
// unrecoverable window, that shows up as an ordinary book error rather than
// a crash, through the same path as any operation on an unknown order.
[[nodiscard]] bool apply_frame_result(std::variant<protocol::Event, protocol::DecodeError> frame,
                                       SequenceValidator& validator,
                                       const ReplayOptions& options,
                                       ReplayOutcome& outcome);

} // namespace mdh::replay
