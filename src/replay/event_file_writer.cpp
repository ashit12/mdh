#include "replay/event_file_writer.hpp"

#include "protocol/encoder.hpp"

namespace mdh::replay {

EventFileWriter::EventFileWriter(const std::string& path)
    : file_(path, std::ios::binary | std::ios::trunc) {}

void EventFileWriter::write(const protocol::Event& event) {
    buffer_.clear();
    protocol::encode_event(event, buffer_);
    file_.write(reinterpret_cast<const char*>(buffer_.data()), static_cast<std::streamsize>(buffer_.size()));
}

} // namespace mdh::replay
