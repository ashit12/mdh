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
    // Milestone 1 only implements "stop on first error" (per spec); these
    // flags exist so a later milestone can add a skip-and-continue policy
    // at this call site without changing run_replay()'s signature.
    bool stop_on_sequence_error = true;
    bool stop_on_decode_error = true;

    // Milestone 4: if set, a detected sequence GAP (SequenceOutcome::Missing
    // specifically -- not a Duplicate or OutOfOrder, see apply_frame_result's
    // doc comment for why) loads this snapshot and resumes instead of
    // stopping. Reloaded from disk fresh every time recovery triggers -- if
    // multiple gaps occur in one run, each one reloads the SAME snapshot
    // file, which is only as fresh as whenever it was written. A real
    // system would fetch a current snapshot per recovery event, which needs
    // a live snapshot-serving/retransmission infrastructure this project
    // doesn't model; this is a deliberate, documented simplification.
    std::optional<std::string> recovery_snapshot_path;
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
// Sequence-gap recovery (milestone 4): only a SequenceOutcome::Missing
// classification -- a genuine gap -- triggers snapshot-based recovery, if
// `options.recovery_snapshot_path` is set. Duplicate and OutOfOrder keep
// their existing stop_on_sequence_error-governed behavior unchanged: a
// duplicate isn't evidence anything was lost (reprocessing it would be
// wrong, and a full book reset would be pure regression), and a
// genuinely-reordered-but-not-missing message calls for buffer-and-reorder
// in a real system, not a snapshot reset -- neither is what recovery is
// for. On a Missing classification with a snapshot configured: the
// snapshot is loaded, `outcome.books` is replaced with its state, and the
// event that revealed the gap becomes the new validator baseline (there is
// no way to fill in exactly what happened between the snapshot's sequence
// and this event without a real gap-fill/retransmission service, which is
// out of scope -- see ReplayOptions::recovery_snapshot_path). That event is
// then applied normally; if it references an order that only existed
// during the unrecoverable window, that surfaces as an ordinary
// BookError/book_errors count, not a crash -- the same machinery that
// already handles any "operating on an order the book doesn't know about"
// case.
[[nodiscard]] bool apply_frame_result(std::variant<protocol::Event, protocol::DecodeError> frame,
                                       SequenceValidator& validator,
                                       const ReplayOptions& options,
                                       ReplayOutcome& outcome);

} // namespace mdh::replay
