#include "replay/event_file_reader.hpp"
#include "protocol/decoder.hpp"

namespace mdh::replay {

EventFileReader::EventFileReader(const std::string& path)
    : file_(path, std::ios::binary) {}

std::optional<std::variant<protocol::Event, protocol::DecodeError>> EventFileReader::next() {
    using protocol::DecodeError;
    using protocol::Event;
    using protocol::HEADER_SIZE;

    frame_buf_.resize(HEADER_SIZE);
    file_.read(reinterpret_cast<char*>(frame_buf_.data()), static_cast<std::streamsize>(HEADER_SIZE));
    const auto header_bytes_read = static_cast<std::size_t>(file_.gcount());

    if (header_bytes_read == 0) {
        return std::nullopt; // clean EOF, nothing left to read
    }
    if (header_bytes_read < HEADER_SIZE) {
        return std::variant<Event, DecodeError>(DecodeError::TruncatedHeader);
    }

    auto header_result = protocol::decode_header(frame_buf_);
    if (std::holds_alternative<DecodeError>(header_result)) {
        return std::variant<Event, DecodeError>(std::get<DecodeError>(header_result));
    }
    const auto& header = std::get<protocol::Header>(header_result);

    frame_buf_.resize(HEADER_SIZE + header.payload_size);
    file_.read(reinterpret_cast<char*>(frame_buf_.data()) + HEADER_SIZE,
               static_cast<std::streamsize>(header.payload_size));
    const auto payload_bytes_read = static_cast<std::size_t>(file_.gcount());

    // gcount() only reports a shortfall against a genuine end-of-file. If
    // the file still has payload_size-or-more bytes ahead of us (mid-file
    // corruption, not truncation), this read succeeds with exactly the
    // requested count even though those bytes may not actually belong to
    // this frame -- e.g. a corrupted/wrong payload_size could silently
    // consume bytes that belong to the next frame's header, desyncing
    // every frame after it. Undetectable without a per-frame checksum
    // this format doesn't have; a known scope limit, not a bug here.
    if (payload_bytes_read < header.payload_size) {
        return std::variant<Event, DecodeError>(DecodeError::TruncatedPayload);
    }

    auto event_result = protocol::decode_event(frame_buf_);
    if (std::holds_alternative<DecodeError>(event_result)) {
        return std::variant<Event, DecodeError>(std::get<DecodeError>(event_result));
    }
    return std::variant<Event, DecodeError>(std::get<Event>(event_result));
}

} // namespace mdh::replay
