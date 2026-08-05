#pragma once

#include "exchange/core/commands.hpp"
#include "exchange/core/types.hpp"

// Assigns the authoritative CommandSequence to an inbound ExchangeCommand
// (Milestone 4). Every NewOrderCommand/CancelOrderCommand/ReplaceOrderCommand
// struct already has a command_sequence field (Milestone 1), but nothing
// upstream of this class has ever been the thing that *chooses* that value
// -- Milestone 2's MatchingEngine and Milestone 3's replay driver both just
// echo whatever command_sequence a command already carries (e.g. one a test
// or a journal file supplied). CommandSequencer is that missing piece: the
// one place that decides what "next" means, so a client (or, later, a
// gateway) never gets to pick its own authoritative position in the
// matching order -- only the sequencer does. This matters because the
// matching engine's whole determinism guarantee rests on commands arriving
// in one true, gapless order; letting an untrusted upstream assign that
// order itself would undermine it.
//
// Deliberately does NOT define a new "unsequenced command" type (e.g. a
// NewOrderRequest without a command_sequence field) to represent "a command
// that hasn't been sequenced yet" -- that would duplicate exchange/core/
// commands.hpp's three structs into a second, near-identical hierarchy for
// no benefit at this milestone. A wire-level inbound message genuinely has
// no command_sequence field (Milestone 7, protocol/order_entry/), which is
// exactly where that distinction belongs; until then, callers construct an
// ordinary ExchangeCommand (the command_sequence field is set to whatever
// placeholder value is convenient, e.g. 0) and sequence() overwrites it
// unconditionally, discarding whatever was there.
//
// Single-writer only: like SpscQueue's head_/tail_ split (common/
// spsc_queue.hpp), this class is only safe to call from one thread. The
// plain (non-atomic) counter below relies on that exact same invariant --
// MatchingPipeline::submit() (matching_pipeline.hpp), the only intended
// caller, is itself documented as producer-thread-only for this reason.
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
