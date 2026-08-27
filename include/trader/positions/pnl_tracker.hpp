#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "common/types.hpp"
#include "exchange/core/types.hpp"
#include "trader/oms/order_management_system.hpp"
#include "trader/positions/position_tracker.hpp"

// Mark-to-market trading performance, kept from the same OMS fill stream
// PositionTracker watches -- a second, independent consumer of
// OrderManagementSystem::FillSink, wired in exactly the same way (see
// sink() below, and PositionTracker::sink()'s own doc comment).
//
// ── Why this is not just more fields on PositionTracker ───────────────────
// The two answer different questions and cannot share a representation.
// PositionTracker answers "what does this account hold," which is what
// TraderRiskEngine needs to decide whether an order may be sent: a
// non-negative Quantity of an instrument, decremented by sells, seeded by
// deposit_position(). This class answers "how has this account's trading
// *done*," which needs the opposite of that:
//
//   - A *signed* position. A participant that has sold more than it bought
//     is net short by any trading measure, even though it still holds a
//     positive, non-zero balance of seeded inventory -- and Quantity is
//     unsigned, so PositionTracker::position() cannot express that at all.
//   - A position that starts at zero and ignores seeding. Seeded inventory
//     is funding (it is what lets a participant sell before it has bought),
//     not a trade, and counting it as an opening position at a fictional
//     entry price would make every P&L figure meaningless.
//
// Keeping both is the same division PositionTracker itself already draws
// against exchange::ledger::Ledger: one class per question, each fed from a
// sink, rather than one class trying to be authoritative about everything.
//
// ── Why a cost basis rather than a stored average entry price ─────────────
// Prices here are integer ticks (common/types.hpp), so an average entry
// price cannot generally be represented exactly -- 3 units bought at
// 100/101/101 average to 100.667 ticks. Storing the truncated average and
// computing realized P&L against it lets that truncation compound across
// every subsequent partial close. This class instead stores the exact
// signed notional of the still-open position (`open_notional`) and removes
// a proportional slice of it on each close, which makes a full close exact
// by construction (the slice is the whole basis) and leaves at most a
// sub-tick rounding on a partial one. The average entry price is then
// derived for display only, where truncation is harmless.
namespace mdh::trader::positions {

// Signed net traded quantity -- see the class comment on why Quantity
// (unsigned) is the wrong type here. Deliberately distinct from Quantity
// rather than a typedef of it, so the two cannot be silently interchanged.
using NetPosition = std::int64_t;

// Everything this class knows about one (account, instrument) pair, before
// any mark price is applied. Balance (position_tracker.hpp) is reused for
// every money field: these are all on the same "ticks times quantity"
// scale as PositionTracker::cash and exchange::ledger::Balance.
struct InstrumentPnl {
    NetPosition position = 0;
    // Signed notional of the currently-open position: positive for a long
    // (cash paid to open it), negative for a short (cash received). Zero
    // whenever `position` is zero.
    Balance open_notional = 0;
    Balance realized = 0;
    // Cumulative traded quantity and fill count, both sides -- turnover,
    // not exposure, so both keep growing as a position is opened and closed.
    Quantity filled_quantity = 0;
    std::uint64_t fill_count = 0;
};

// One instrument's P&L with a mark price applied. Returned by value from
// snapshot() so a display thread never holds this class's mutex while
// formatting.
struct PnlSnapshot {
    NetPosition position = 0;
    // Derived from open_notional, for display only -- see the class comment.
    // Zero when flat, since there is no open position to have entered.
    Price average_entry_price = 0;
    Balance realized = 0;
    // Zero, not "unknown", when no mark price is available: a flat position
    // has no unrealized P&L either way, and for an open one there is
    // nothing better to report than "no change since entry" until the
    // market says otherwise.
    Balance unrealized = 0;
    Balance total = 0;
    Quantity filled_quantity = 0;
    std::uint64_t fill_count = 0;
};

class PnlTracker {
public:
    // The mutation entry point, wired to an OrderManagementSystem's
    // FillSink via sink(). Mutex-guarded for the same reason
    // PositionTracker::apply() is: the real caller is a single OMS reader
    // thread, but snapshot() is read from whichever thread is displaying.
    void apply(const oms::Fill& fill);
    [[nodiscard]] oms::OrderManagementSystem::FillSink sink() {
        return [this](const oms::Fill& fill) { apply(fill); };
    }

    // `mark` is the price to value the open position at -- typically the
    // reconstructed book's midpoint, or the last trade price when the book
    // is one-sided. std::nullopt reports zero unrealized; see PnlSnapshot.
    [[nodiscard]] PnlSnapshot snapshot(exchange::AccountId account_id, InstrumentId instrument_id,
                                        std::optional<Price> mark = std::nullopt) const;

    // Turnover across every instrument, for a summary line that does not
    // want to enumerate instruments.
    [[nodiscard]] Quantity total_filled_quantity() const;
    [[nodiscard]] std::uint64_t total_fill_count() const;

private:
    // Same nested shape as PositionTracker::accounts_, for the same reason:
    // an account is the unit a participant is scoped to, and an instrument
    // is a detail within it.
    mutable std::mutex mutex_;
    std::unordered_map<exchange::AccountId, std::unordered_map<InstrumentId, InstrumentPnl>> accounts_;
};

} // namespace mdh::trader::positions
