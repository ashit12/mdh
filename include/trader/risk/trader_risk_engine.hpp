#pragma once

#include "common/types.hpp"
#include "exchange/core/types.hpp"
#include "trader/positions/position_tracker.hpp"

// Pre-trade risk checks performed on the trader's own side, before an order
// is even sent to the exchange (Milestone 9) -- the trader-side mirror of
// exchange::risk::RiskEngine, checked against trader::positions::
// PositionTracker instead of exchange::ledger::Ledger. Reuses
// exchange::RejectReason wholesale rather than inventing a parallel
// trader-side enum: "insufficient funds," "insufficient position," and
// "order too large" mean the same thing on both sides of the wire, and
// RejectReason's own doc comment already scopes it to "only the reasons the
// next few milestones actually need" -- this is exactly that.
//
// ── Why this exists at all, given the exchange already checks this ───────
// A real trading firm never trusts a venue's own risk check as its only
// line of defense -- a firm-side check catches a bad order (or a bug in a
// future Milestone 10 strategy) *before* it even leaves the building,
// without waiting on a round trip to find out it would have been rejected
// anyway, and can enforce firm-specific limits the exchange has no way to
// know about. See docs/end_to_end_architecture.md's system diagram, which
// draws "Exchange validation + pre-trade risk" (Milestone 5) and
// "Trader-side risk" (Milestone 9) as two separate boxes for exactly this
// reason.
//
// ── Why this checks *settled* exposure only, like PositionTracker itself ──
// See position_tracker.hpp's own class comment: this is a best-effort
// second screen, not the authoritative check, so it does not attempt to
// reserve against the trader's own in-flight (not-yet-filled) orders the
// way exchange::ledger::Ledger does. Concretely, this means two rapid buy
// orders that together exceed available cash can both pass this check
// before either one settles -- the exchange's own RiskEngine remains the
// backstop that actually prevents a double-spend.
namespace mdh::trader::risk {

struct TraderRiskLimits {
    // Mirrors exchange::risk::RiskLimits::max_order_quantity exactly (a
    // generous, illustrative cap, not a real firm's actual limit) -- kept
    // as a separate value, not a shared constant, since a firm's own
    // self-imposed limit and a venue's accepted-order limit are
    // independent policy decisions that happen to default to the same
    // number today.
    Quantity max_order_quantity = 1'000'000;
};

class TraderRiskEngine {
public:
    explicit TraderRiskEngine(TraderRiskLimits limits = {}) : limits_(limits) {}

    // Returns RejectReason::None if a NewOrder for `quantity` @ `price` on
    // `instrument_id`, side `side`, may be submitted for `account_id` given
    // `positions`' currently-known settled exposure -- otherwise the
    // specific reason it must not, using exactly the same three checks (and
    // the same order of evaluation) as exchange::risk::RiskEngine::check().
    // Never mutates `positions` -- this function only decides yes/no, same
    // division of responsibility as the exchange-side RiskEngine.
    [[nodiscard]] exchange::RejectReason check(exchange::AccountId account_id, InstrumentId instrument_id, Side side,
                                                Price price, Quantity quantity,
                                                const positions::PositionTracker& positions) const;

private:
    TraderRiskLimits limits_;
};

} // namespace mdh::trader::risk
