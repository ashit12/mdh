#pragma once

#include "common/types.hpp"
#include "exchange/core/commands.hpp"
#include "exchange/core/types.hpp"
#include "exchange/ledger/ledger.hpp"

// Pre-trade risk checks (Milestone 5) -- deliberately a short, named list of
// "small but credible" checks (mirroring RejectReason's own stated policy of
// growing only as far as the current milestones actually need), not an
// attempt at a real venue's full risk stack (margin, per-instrument limits,
// self-trade prevention, kill switches, etc. are all out of scope here).
//
// Only NewOrderCommand is checked. CancelOrderCommand never increases an
// account's exposure. ReplaceOrderCommand doesn't need an independent check
// either, for a reason specific to *this* engine's own documented replace
// policy (matching_engine.hpp): the priority-preserving path only ever
// keeps quantity the same or shrinks it (never a check-worthy increase in
// exposure), and the cancel-plus-new path is treated by Ledger's
// on_order_replaced as a full, fresh reservation at the new price/quantity
// regardless of size -- if that reservation would overdraw the account, it
// simply goes negative rather than being rejected, a known, documented
// simplification for this milestone (see RiskGatedEngine's own class
// comment) rather than an oversight: correctly rejecting a too-large
// replace would require the same before-you-touch-the-book check this class
// gives NewOrderCommand, applied through the *matching engine's* two
// different replace code paths instead of before them, which is
// meaningfully more invasive to Milestone 2's already-tested code than this
// milestone's scope calls for.
namespace mdh::exchange::risk {

struct RiskLimits {
    // A generous, illustrative cap -- not a real venue's actual position
    // limit -- present so "an order too large for this account/venue to
    // accept" is a checked, tested case rather than an unbounded one.
    Quantity max_order_quantity = 1'000'000;
};

class RiskEngine {
public:
    explicit RiskEngine(RiskLimits limits = {}) : limits_(limits) {}

    // Returns RejectReason::None if `command` may proceed to the matching
    // engine, otherwise the specific reason it must not. Reads `ledger` but
    // never mutates it -- reservation happens later, via Ledger::apply()
    // watching the resulting OrderAccepted (see ledger.hpp's own class
    // comment); this function only decides yes/no.
    [[nodiscard]] RejectReason check(const NewOrderCommand& command, const ledger::Ledger& ledger) const;

private:
    RiskLimits limits_;
};

} // namespace mdh::exchange::risk
