#pragma once

#include "common/types.hpp"
#include "exchange/core/commands.hpp"
#include "exchange/core/types.hpp"
#include "exchange/ledger/ledger.hpp"

// Pre-trade risk checks: a short list of small but credible ones, not an
// attempt at a real venue's risk stack. Margin, per-instrument limits,
// self-trade prevention and kill switches are all out of scope.
//
// A new order is checked against the account's *available* balance -- what
// it holds minus what its resting orders have already reserved. A cancel is
// never checked, since it cannot increase exposure.
//
// A replace is checked, but credits the hold the original order already
// carries: only the extra exposure, max(0, new_required - old_required), has
// to fit. So a replace that shrinks or keeps exposure always passes the
// balance check, though the size cap still applies to the new quantity. With
// no open hold for the original id there is nothing to credit, so this class
// passes and leaves the unknown-order rejection to the matching engine.
namespace mdh::exchange::risk {

struct RiskLimits {
    // A generous, illustrative cap rather than a real venue's limit, so that
    // "too large to accept" is a tested case rather than an unbounded one.
    Quantity max_order_quantity = 1'000'000;
};

class RiskEngine {
public:
    explicit RiskEngine(RiskLimits limits = {}) : limits_(limits) {}

    // Returns RejectReason::None if the command may go on to the matching
    // engine, otherwise the reason it must not. Reads the ledger but never
    // changes it: the reservation is opened later, when the ledger sees the
    // resulting event. This only decides yes or no.
    [[nodiscard]] RejectReason check(const NewOrderCommand& command, const ledger::Ledger& ledger) const;
    [[nodiscard]] RejectReason check(const ReplaceOrderCommand& command, const ledger::Ledger& ledger) const;

private:
    RiskLimits limits_;
};

} // namespace mdh::exchange::risk
