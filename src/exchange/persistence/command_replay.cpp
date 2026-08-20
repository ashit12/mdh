#include "exchange/persistence/command_replay.hpp"

#include <variant>

#include "exchange/persistence/command_journal_reader.hpp"

namespace mdh::exchange::persistence {

CommandReplayOutcome run_command_replay(const std::string& journal_path, const CommandReplayOptions& options) {
    CommandReplayOutcome outcome;

    CommandJournalReader reader(journal_path);
    if (!reader.is_open()) {
        outcome.stopped_early = true;
        outcome.stop_reason = "could not open journal file: " + journal_path;
        return outcome;
    }

    const EventSink sink = [&outcome](const ExchangeEvent& ev) { outcome.events.push_back(ev); };

    while (true) {
        auto frame = reader.next();
        if (!frame.has_value()) {
            break; // clean EOF
        }
        if (std::holds_alternative<CommandDecodeError>(*frame)) {
            outcome.stop_reason =
                std::string("decode error: ") + std::string(to_string(std::get<CommandDecodeError>(*frame)));
            if (options.stop_on_decode_error) {
                outcome.stopped_early = true;
                break;
            }
            continue; // skip this frame, keep reading
        }
        if (const auto* registration = std::get_if<RegisterInstrumentRecord>(&*frame)) {
            // Ordering is the journal's, not ours: these frames are written
            // before any command, so by the time a command arrives its
            // instrument is already registered.
            if (outcome.engine.register_instrument(registration->instrument_id)) {
                ++outcome.instruments_registered;
            }
            continue;
        }

        outcome.engine.process(std::get<ExchangeCommand>(*frame), sink);
        ++outcome.commands_processed;
    }

    return outcome;
}

} // namespace mdh::exchange::persistence
