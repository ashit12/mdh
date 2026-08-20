#pragma once

#include "common/types.hpp"
#include "exchange/core/types.hpp"
#include "trader/positions/position_tracker.hpp"

// Risk checks on the trader's own side, before an order is ever sent -- the
// mirror of the exchange's risk engine, checked against this trader's own
// position tracker rather than the exchange's ledger. It reuses the same
// RejectReason enum instead of inventing a parallel one: insufficient funds,
// insufficient position and order too large mean the same thing on both
// sides of the wire.
//
// ── Why this exists when the exchange checks the same things ─────────────
// A real trading firm never treats the venue's check as its only line of
// defence. A firm-side check catches a bad order, or a bug in a strategy,
// before it leaves the building -- no round trip needed to find out it would
// have been rejected -- and can enforce firm-specific limits the exchange
// has no way to know about.
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
