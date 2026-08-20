#pragma once

#include <cstddef>
#include <span>
#include <string>
#include <vector>

#include "exchange/core/events.hpp"
#include "exchange/matching/matching_engine.hpp"

// Deterministic exchange-command replay: reads a command
// journal end to end and feeds each decoded command into a fresh
// MatchingEngine, collecting every emitted event in order. This is a
// SEPARATE replay concept from replay::run_replay() (replay_engine.hpp) --
// that one replays market-data events into the trader-side book::BookManager;
// this one replays exchange commands into the authoritative MatchingEngine.
// Different inputs, different engines, different purposes; neither is
// modified or reused by the other, per the working rule that the two
// replay concepts must stay distinct.
namespace mdh::exchange::persistence {

struct CommandReplayOptions {
    // Only "stop on the first decode error" is implemented, mirroring
    // replay::ReplayOptions -- a skip-and-continue policy can be added at
    // this call site later without changing
    // run_command_replay()'s signature.
    bool stop_on_decode_error = true;
};

struct CommandReplayOutcome {
    // Starts with no instruments and learns them from the journal's own
    // RegisterInstrument frames as they are read, so a replay reproduces the
    // run that wrote the file without being told anything about it beyond
    // the path. A journal written before that frame type existed registers
    // nothing, so every command in it is rejected as InvalidInstrument --
    // visibly wrong rather than quietly different.
    MatchingEngine engine{std::span<const InstrumentId>{}};
    std::vector<ExchangeEvent> events; // every event emitted, across all commands, in order
    std::size_t commands_processed = 0;
    std::size_t instruments_registered = 0;
    bool stopped_early = false;
    std::string stop_reason;
};

// Reads `journal_path` end to end (or until the first decode error, per
// `options`), replaying each command into a fresh MatchingEngine. Given the
// same journal file, two calls to this function are required to produce
// events streams and final engine snapshots that compare equal -- that is
// what proves the matching engine is genuinely deterministic end-to-end,
// not just within a single process's lifetime.
[[nodiscard]] CommandReplayOutcome run_command_replay(const std::string& journal_path,
                                                       const CommandReplayOptions& options = {});

} // namespace mdh::exchange::persistence
