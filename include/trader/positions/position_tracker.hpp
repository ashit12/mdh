#pragma once

#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "common/types.hpp"
#include "exchange/core/types.hpp"
#include "trader/oms/order_management_system.hpp"

// Per-account cash and position bookkeeping, kept purely by watching the
// OMS's fill events -- the trader-side mirror of the exchange's ledger,
// which keeps the authoritative equivalent by watching exchange events. Same
// "one object is the single place this state changes, fed by a sink"
// pattern; see PositionTracker::sink().
//
// ── Why this does NOT reserve against in-flight orders, unlike Ledger ────
// exchange::ledger::Ledger reserves funds/inventory the moment an order
// becomes live (OrderAccepted/OrderReplaced with TimeInForce::GTC), because
// it is the one authoritative record the exchange itself trusts to prevent
// a double-spend across two commands racing the same not-yet-settled
// balance (see ledger.hpp's own class comment for the full argument, which
// depends specifically on MatchingEngine's single-threaded, one-event-
// stream-in/one-event-stream-out determinism guarantee). PositionTracker
// has no such guarantee to lean on: it is a *second*, independent,
// best-effort screen sitting in front of the OMS (see trader::risk::
// TraderRiskGatedOms) -- the exchange's own risk-gated engine remains the
// authoritative check no matter what this class does. Reservation
// bookkeeping this class cannot make authoritative would only add
// complexity without adding safety, so it deliberately tracks
// *settled* (post-fill) balances only, exactly like Ledger's own `total`
// fields, with nothing corresponding to `reserved`.
namespace mdh::trader::positions {

// Cash is kept on the same fixed-point tick scale as Price (common/
// types.hpp) and signed for the same reason exchange::ledger::Balance is:
// well-defined arithmetic regardless, even though it is not expected to go
// negative in practice. Deliberately a distinct type from
// exchange::ledger::Balance -- see docs/end_to_end_architecture.md section
// 5's general rule that the trader side and exchange side never share a
// class just because the shape happens to match.
using Balance = std::int64_t;

struct AccountPosition {
    Balance cash = 0; // net realized cash flow from fills: buys debit, sells credit
    std::unordered_map<InstrumentId, Quantity> holdings;
};

class PositionTracker {
public:
    // Seeding only -- there is no DepositCommand/funding message modeled
    // anywhere in this codebase (exchange side or trader side), same
    // rationale as exchange::ledger::Ledger::deposit_cash()/
    // deposit_position()'s own doc comment. Without this, `cash` would
    // start at 0 for every account and never represent anything but
    // cumulative realized trading P&L -- deposit_cash() is what lets it
    // instead represent "this account's actual cash," the number
    // TraderRiskEngine's insufficient-funds check needs to be meaningful
    // for an account that hasn't traded yet.
    void deposit_cash(exchange::AccountId account_id, Balance amount);
    void deposit_position(exchange::AccountId account_id, InstrumentId instrument_id, Quantity amount);

    // The mutation entry point -- typically wired to an
    // OrderManagementSystem's FillSink at construction (via sink() below),
    // exactly like exchange::ledger::Ledger::apply()/sink() is wired to an
    // EventSink. Safe to call from any thread: a real deployment has
    // exactly one caller (the OMS's reader thread invoking the FillSink),
    // but a test or a multi-OMS setup sharing one tracker across accounts
    // might not, so this is mutex-guarded rather than documented as
    // single-threaded the way, e.g., CommandSequencer is.
    void apply(const oms::Fill& fill);
    [[nodiscard]] oms::OrderManagementSystem::FillSink sink() {
        return [this](const oms::Fill& fill) { apply(fill); };
    }

    // What TraderRiskEngine (trader/risk/) checks a prospective order
    // against. Returns 0 / a default-constructed AccountPosition for an
    // account never seen before, rather than requiring the caller to check
    // existence first -- same convention as Ledger::balances().
    [[nodiscard]] Balance cash(exchange::AccountId account_id) const;
    [[nodiscard]] Quantity position(exchange::AccountId account_id, InstrumentId instrument_id) const;
    [[nodiscard]] AccountPosition account(exchange::AccountId account_id) const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<exchange::AccountId, AccountPosition> accounts_;
};

} // namespace mdh::trader::positions
