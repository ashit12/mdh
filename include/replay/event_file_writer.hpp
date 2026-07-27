#pragma once

#include <fstream>
#include <string>
#include <vector>

#include "protocol/messages.hpp"

namespace mdh::replay {

// Writes encoded events to a binary file, sequentially, with no framing
// beyond each event's own header (there is no outer file header/footer in
// milestone 1 -- the file is just a concatenation of frames).
class EventFileWriter {
public:
    explicit EventFileWriter(const std::string& path);

    [[nodiscard]] bool is_open() const { return file_.is_open(); }

    // Encodes and appends one event. Reuses an internal buffer across calls
    // to avoid a heap allocation per event.
    void write(const protocol::Event& event);

private:
    std::ofstream file_;
    std::vector<std::byte> buffer_;
};

} // namespace mdh::replay
