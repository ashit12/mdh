#include "exchange/sequencing/command_sequencer.hpp"

#include <variant>

namespace mdh::exchange::sequencing {

ExchangeCommand CommandSequencer::sequence(ExchangeCommand command) {
    const CommandSequence assigned = next_sequence_++;
    std::visit([assigned](auto& cmd) { cmd.command_sequence = assigned; }, command);
    return command;
}

} // namespace mdh::exchange::sequencing
