#pragma once

#include "exchange/core/commands.hpp"
#include "exchange/core/event_sink.hpp"
#include "exchange/ledger/ledger.hpp"
#include "exchange/matching/matching_engine.hpp"
#include "exchange/risk/risk_engine.hpp"

// Composes RiskEngine + Ledger + MatchingEngine behind exactly
// MatchingEngine::process()'s own signature (Milestone 5) -- a drop-in
// replacement anywhere a bare MatchingEngine is used today, e.g. in place
// of the `engine_.process(*command, sink_)` call inside
// exchange::sequencing::MatchingPipeline's matching thread (Milestone 4),
// though wiring that substitution in is left to whichever later milestone
// actually needs risk-gating live (this milestone's own tests drive
// RiskGatedEngine directly, single-threaded, which is sufficient to prove
// the composition is correct without touching Milestone 4's already-tested
// pipeline code).
//
// ── Why risk-check and ledger-update must share one thread with matching ──
// The architecture diagram (docs/end_to_end_architecture.md) places
// "exchange validation and risk" *before* the command sequencer, and
// "balance and ledger updates" *after* the matching engine -- two separate
// stages, on either side of matching. Naively splitting them the same way
// in code -- risk-checking a command on whatever thread first receives it,
// only updating the ledger later, on the matching thread, once events come
// back -- would reopen exactly the double-spend race reservations exist to
// prevent (see ledger.hpp's own class comment): two commands from the same
// account could each read the same not-yet-reserved balance before either
// one's reservation actually lands. RiskGatedEngine avoids that by keeping
// check-then-reserve entirely single-threaded and in strict command order:
// like MatchingEngine itself, it must only ever be driven from one thread.
// This is an intentional, documented departure from the diagram's box
// *ordering* in service of the diagram's own determinism requirements, not
// a misreading of it -- risk is still fully evaluated before a command is
// allowed to affect the book, it now just happens immediately before
// matching rather than immediately after sequencing.
namespace mdh::exchange::risk {

class RiskGatedEngine {
public:
    // Does not own `engine`/`ledger` -- both are expected to outlive this
    // object and to remain independently usable (e.g. a caller inspecting
    // `ledger.balances(...)` or `engine.snapshot()` directly). This class
    // is a thin composition, not a container, the same ownership shape
    // MatchingPipeline uses for the EventSink it's handed rather than owns.
    RiskGatedEngine(MatchingEngine& engine, ledger::Ledger& ledger, RiskLimits limits = {});

    // Same signature as MatchingEngine::process() -- see class-level
    // comment. Only NewOrderCommand is ever risk-checked; see
    // RiskEngine's own class comment for why Cancel/Replace are exempt.
    // Every event actually emitted by the underlying MatchingEngine is fed
    // to `ledger` before being forwarded to `sink`, so a caller observing
    // `sink` sees ledger state that is already consistent with the event
    // just received.
    void process(const ExchangeCommand& command, const EventSink& sink);

private:
    MatchingEngine& engine_;
    ledger::Ledger& ledger_;
    RiskEngine risk_;
};

} // namespace mdh::exchange::risk
