#pragma once

#include <fstream>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "exchange/core/commands.hpp"
#include "exchange/persistence/command_decoder.hpp"
#include "exchange/persistence/command_errors.hpp"

namespace mdh::exchange::persistence {

// Reads frames (header + payload) sequentially from a binary command
// journal. Each call to next() reads exactly one frame's worth of bytes,
// using the frame's own header.payload_size to know how much payload to
// read. Mirrors replay::EventFileReader exactly, applied to ExchangeCommand
// instead of protocol::Event.
class CommandJournalReader {
public:
    explicit CommandJournalReader(const std::string& path);

    [[nodiscard]] bool is_open() const { return file_.is_open(); }

    // Returns std::nullopt at a clean end-of-file (no partial frame
    // pending). Otherwise returns the decoded frame -- a command, an
    // instrument registration, or the CommandDecodeError that made this
    // frame unreadable/invalid.
    [[nodiscard]] std::optional<DecodedFrame> next();

private:
    std::ifstream file_;
    std::vector<std::byte> frame_buf_; // reused across calls; never allocated per-command once warm
};

} // namespace mdh::exchange::persistence
