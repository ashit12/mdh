#pragma once

#include <fstream>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "protocol/errors.hpp"
#include "protocol/messages.hpp"

namespace mdh::replay {

// Reads frames (header + payload) sequentially from a binary event file.
// Each call to next() reads exactly one frame's worth of bytes, using the
// frame's own header.payload_size to know how much payload to read -- there
// is no outer index or length-prefixed container.
class EventFileReader {
public:
    explicit EventFileReader(const std::string& path);

    [[nodiscard]] bool is_open() const { return file_.is_open(); }

    // Returns std::nullopt at a clean end-of-file (no partial frame
    // pending). Otherwise returns the decoded event or the DecodeError
    // that made this frame unreadable/invalid.
    [[nodiscard]] std::optional<std::variant<protocol::Event, protocol::DecodeError>> next();

private:
    std::ifstream file_;
    std::vector<std::byte> frame_buf_; // reused across calls; never allocated per-message once warm
};

} // namespace mdh::replay
