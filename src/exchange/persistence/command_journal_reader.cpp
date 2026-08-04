#include "exchange/persistence/command_journal_reader.hpp"

#include "exchange/persistence/command_decoder.hpp"
#include "exchange/persistence/command_messages.hpp"

namespace mdh::exchange::persistence {

CommandJournalReader::CommandJournalReader(const std::string& path) : file_(path, std::ios::binary) {}

std::optional<std::variant<ExchangeCommand, CommandDecodeError>> CommandJournalReader::next() {
    frame_buf_.resize(HEADER_SIZE);
    file_.read(reinterpret_cast<char*>(frame_buf_.data()), static_cast<std::streamsize>(HEADER_SIZE));
    const auto header_bytes_read = static_cast<std::size_t>(file_.gcount());

    if (header_bytes_read == 0) {
        return std::nullopt; // clean EOF, nothing left to read
    }
    if (header_bytes_read < HEADER_SIZE) {
        return std::variant<ExchangeCommand, CommandDecodeError>(CommandDecodeError::TruncatedHeader);
    }

    auto header_result = decode_command_header(frame_buf_);
    if (std::holds_alternative<CommandDecodeError>(header_result)) {
        return std::variant<ExchangeCommand, CommandDecodeError>(std::get<CommandDecodeError>(header_result));
    }
    const auto& header = std::get<CommandHeader>(header_result);

    frame_buf_.resize(HEADER_SIZE + header.payload_size);
    file_.read(reinterpret_cast<char*>(frame_buf_.data()) + HEADER_SIZE,
               static_cast<std::streamsize>(header.payload_size));
    const auto payload_bytes_read = static_cast<std::size_t>(file_.gcount());

    if (payload_bytes_read < header.payload_size) {
        return std::variant<ExchangeCommand, CommandDecodeError>(CommandDecodeError::TruncatedPayload);
    }

    auto command_result = decode_command(frame_buf_);
    if (std::holds_alternative<CommandDecodeError>(command_result)) {
        return std::variant<ExchangeCommand, CommandDecodeError>(std::get<CommandDecodeError>(command_result));
    }
    return std::variant<ExchangeCommand, CommandDecodeError>(std::get<ExchangeCommand>(command_result));
}

} // namespace mdh::exchange::persistence
