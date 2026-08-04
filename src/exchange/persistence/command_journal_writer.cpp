#include "exchange/persistence/command_journal_writer.hpp"

#include "exchange/persistence/command_encoder.hpp"

namespace mdh::exchange::persistence {

CommandJournalWriter::CommandJournalWriter(const std::string& path) : file_(path, std::ios::binary | std::ios::trunc) {}

void CommandJournalWriter::write(const ExchangeCommand& command) {
    buffer_.clear();
    encode_command(command, buffer_);
    file_.write(reinterpret_cast<const char*>(buffer_.data()), static_cast<std::streamsize>(buffer_.size()));
}

} // namespace mdh::exchange::persistence
