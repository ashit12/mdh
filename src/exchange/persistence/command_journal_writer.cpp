#include "exchange/persistence/command_journal_writer.hpp"

#include "exchange/persistence/command_encoder.hpp"

namespace mdh::exchange::persistence {

CommandJournalWriter::CommandJournalWriter(const std::string& path, std::span<const InstrumentId> instruments)
    : file_(path, std::ios::binary | std::ios::trunc) {
    if (!file_.is_open()) {
        return;
    }
    // One write for the whole universe rather than one per instrument: this
    // runs once, but a large universe would otherwise be a lot of tiny
    // writes for no reason.
    buffer_.clear();
    for (const InstrumentId instrument_id : instruments) {
        encode_register_instrument(instrument_id, buffer_);
    }
    file_.write(reinterpret_cast<const char*>(buffer_.data()), static_cast<std::streamsize>(buffer_.size()));
}

void CommandJournalWriter::write(const ExchangeCommand& command) {
    buffer_.clear();
    encode_command(command, buffer_);
    file_.write(reinterpret_cast<const char*>(buffer_.data()), static_cast<std::streamsize>(buffer_.size()));
}

} // namespace mdh::exchange::persistence
