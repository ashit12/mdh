#pragma once

#include <fstream>
#include <string>
#include <vector>

#include "exchange/core/commands.hpp"

namespace mdh::exchange::persistence {

// Writes encoded commands to a binary file, sequentially, with no framing
// beyond each command's own header -- the file is just a concatenation of
// frames. Mirrors replay::EventFileWriter exactly, applied to
// ExchangeCommand instead of protocol::Event.
class CommandJournalWriter {
public:
    explicit CommandJournalWriter(const std::string& path);

    [[nodiscard]] bool is_open() const { return file_.is_open(); }

    // Encodes and appends one command. Reuses an internal buffer across
    // calls to avoid a heap allocation per command.
    void write(const ExchangeCommand& command);

private:
    std::ofstream file_;
    std::vector<std::byte> buffer_;
};

} // namespace mdh::exchange::persistence
