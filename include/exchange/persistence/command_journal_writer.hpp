#pragma once

#include <fstream>
#include <span>
#include <string>
#include <vector>

#include "common/types.hpp"
#include "exchange/core/commands.hpp"

namespace mdh::exchange::persistence {

// Writes encoded commands to a binary file, sequentially, with no framing
// beyond each command's own header -- the file is just a concatenation of
// frames. Mirrors replay::EventFileWriter exactly, applied to
// ExchangeCommand instead of protocol::Event.
class CommandJournalWriter {
public:
    // `instruments` is the universe of the engine whose commands are about
    // to be journaled; one RegisterInstrument frame per entry is written
    // immediately, before any command, so the file describes the engine that
    // can replay it (see CommandMessageType::RegisterInstrument). Required
    // rather than defaulted: a journal without it replays into an engine
    // that rejects every command in it, which is a file that looks fine and
    // reproduces nothing.
    CommandJournalWriter(const std::string& path, std::span<const InstrumentId> instruments);

    [[nodiscard]] bool is_open() const { return file_.is_open(); }

    // Encodes and appends one command. Reuses an internal buffer across
    // calls to avoid a heap allocation per command.
    void write(const ExchangeCommand& command);

private:
    std::ofstream file_;
    std::vector<std::byte> buffer_;
};

} // namespace mdh::exchange::persistence
