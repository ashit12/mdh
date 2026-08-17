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
// NewOrderCommand is checked against the account's currently *available*
// (unreserved) cash/position. CancelOrderCommand never increases exposure
// and is not checked. ReplaceOrderCommand *is* checked, but credits the
// reservation already held for the original order: only the *extra*
// exposure (max(0, new_required - old_required)) must fit in available
// resources. A replace that shrinks or keeps exposure therefore always
// passes the balance check (OrderTooLarge still applies to new_quantity).
// If there is no open hold for the original id, this class returns None and
// leaves UnknownOrderId / InvalidReplacement to MatchingEngine -- risk has
// nothing to evaluate without a reservation to credit.
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
    // watching the resulting OrderAccepted / OrderReplaced (see ledger.hpp);
    // this function only decides yes/no.
    [[nodiscard]] RejectReason check(const NewOrderCommand& command, const ledger::Ledger& ledger) const;
    [[nodiscard]] RejectReason check(const ReplaceOrderCommand& command, const ledger::Ledger& ledger) const;

private:
    RiskLimits limits_;
};

} // namespace mdh::exchange::risk
