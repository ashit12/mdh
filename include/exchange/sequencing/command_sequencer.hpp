#pragma once

#include "exchange/core/commands.hpp"
#include "exchange/core/types.hpp"

// Decides the authoritative position of each command in the matching order.
//
// Every command struct already has a command_sequence field, but the engine
// and the replay driver only ever echo whatever value a command arrives
// with. This class is the one place that chooses it, so a client -- or the
// gateway acting for one -- never picks its own place in line. The engine's
// determinism rests on commands arriving in one true, gapless order, and
// letting an untrusted upstream assign that order would undermine it.
//
// There is deliberately no separate "unsequenced command" type. That would
// duplicate the three command structs into a near-identical second set for
// no benefit. A wire-level message genuinely has no sequence field, which is
// where the distinction properly lives; here, callers build an ordinary
// command with any placeholder value and sequence() overwrites it.
//
// Single writer only, like the SPSC queue: the plain non-atomic counter
// below depends on it, and the only intended caller is the pipeline's
// submit(), which is producer-thread-only for the same reason.
namespace mdh::exchange::sequencing {

class CommandSequencer {
public:
    // Overwrites `command`'s own command_sequence field (whichever
    // alternative it holds) with the next value from this sequencer's
    // monotonically increasing counter, then returns it.
    [[nodiscard]] ExchangeCommand sequence(ExchangeCommand command);

    // The value the *next* call to sequence() will assign -- introspection
    // only (e.g. for a test asserting how many commands a sequencer has
    // handed out), never itself an authoritative sequence number.
    [[nodiscard]] CommandSequence next_sequence() const { return next_sequence_; }

private:
    CommandSequence next_sequence_ = 1;
};

} // namespace mdh::exchange::sequencing
