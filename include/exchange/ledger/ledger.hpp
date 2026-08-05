#pragma once

#include <cstdint>
#include <unordered_map>

#include "common/types.hpp"
#include "exchange/core/event_sink.hpp"
#include "exchange/core/events.hpp"
#include "exchange/core/types.hpp"

// Per-account cash and instrument-position balances (Milestone 5), updated
// by watching the matching engine's own event stream -- the same "one
// function/object is the single place state changes happen, fed by an
// EventSink" pattern MatchingEngine itself uses for the book. There is no
// DepositCommand in this project's command set (out of scope for the
// current milestones; commands.hpp models trading, not funding), so
// deposit_cash()/deposit_position() are the only way balances start out
// non-zero -- test/admin seeding, not something the event stream drives.
//
// ── Reservation semantics (the actual design decision here) ───────────────
// A resting GTC order can outlive many other commands, so the funds/
// inventory behind it must be locked ("reserved") for as long as it rests --
// otherwise two GTC orders from the same account could each independently
// pass a balance check against the same unspent funds. IOC/FOK orders are
// different: MatchingEngine resolves them completely within a single
// process() call, with no other command interleaved (matching is single-
// threaded and one command's whole event sequence is delivered before the
// next command starts, see MatchingEngine's own class-level comment) -- so
// there is no window in which a concurrent command could double-spend
// against an IOC/FOK order's funds, and therefore *no reservation is needed
// for them at all*. This ledger reserves only for GTC orders (created on
// OrderAccepted/OrderReplaced when time_in_force == GTC) and settles IOC/FOK
// fills by debiting/crediting `total` balances directly, with nothing to
// release afterward. This is what avoids the alternative, genuinely hard
// problem: IOC's "any unfilled remainder is silently discarded, no event
// fires for it" behavior (see MatchingEngine::rest_remainder_if_applicable)
// would otherwise leave a permanently unreleasable reservation with no
// signal to ever clean it up. Because IOC/FOK never reserve in the first
// place, that problem never arises.
//
// A GTC order's reservation is tracked per (account_id, client_order_id) --
// the same key MatchingEngine itself uses for live_orders_ -- rather than
// per exchange_order_id, specifically so a replace (which can assign a
// brand new exchange_order_id via the cancel-plus-new path, see
// MatchingEngine's replace-policy comment) can still be resolved: an
// OrderReplaced event always carries both the *original* and *new*
// client_order_id, letting the ledger release the old hold and open a new
// one under the new key regardless of which replace path the engine took --
// the two paths are handled by the exact same code here, uniformly (release
// whatever remains of the old hold in full, open a fresh one at the new
// price/quantity), which is arithmetically equivalent to a priority-
// preserving quantity-only adjustment when the price is unchanged.
//
// A resting order's limit price, not the eventual trade price, is what gets
// reserved (the worst case the account could be on the hook for). A buy
// that later trades at a *better* price than its own limit (always possible
// for the aggressor side of a trade, since trades execute at the resting/
// passive order's price -- see MatchingEngine::match_and_rest) releases the
// full limit-price reservation for the filled slice but only actually
// debits `total` at the true trade price, so the difference reappears in
// `available` (= total - reserved) automatically, with no separate refund
// bookkeeping needed. A resting order's own price *is* the trade price by
// construction, so this refund case never applies to the passive side of a
// trade, only the aggressor side.
namespace mdh::exchange::ledger {

// Cash is kept on the same fixed-point tick scale as Price (common/
// types.hpp) -- a signed 64-bit integer rather than Quantity's unsigned
// Price*Quantity so that intermediate arithmetic (which this ledger never
// expects to go negative in practice, but does not clamp defensively away
// from either) is well-defined regardless.
using Balance = std::int64_t;

struct AccountBalances {
    Balance cash_total = 0;
    Balance cash_reserved = 0; // locked against resting GTC buy orders
    std::unordered_map<InstrumentId, Quantity> position_total;
    std::unordered_map<InstrumentId, Quantity> position_reserved; // locked against resting GTC sell orders
};

class Ledger {
public:
    // Seeding only -- see class-level comment on why this exists instead of
    // a command that flows through the matching engine.
    void deposit_cash(AccountId account_id, Balance amount);
    void deposit_position(AccountId account_id, InstrumentId instrument_id, Quantity amount);

    // What RiskEngine (exchange/risk/) checks a prospective order against:
    // funds/holdings not already locked behind another resting order.
    [[nodiscard]] Balance available_cash(AccountId account_id) const;
    [[nodiscard]] Quantity available_position(AccountId account_id, InstrumentId instrument_id) const;

    // Raw balances, for tests/introspection -- returns a default-constructed
    // (all-zero) AccountBalances for an account never seen before, rather
    // than requiring the caller to check existence first.
    [[nodiscard]] AccountBalances balances(AccountId account_id) const;

    // The ledger's only mutation entry point: an EventSink-compatible
    // callable, exactly like every other event consumer in this codebase
    // (see event_sink.hpp). Only OrderAccepted, OrderReplaced,
    // OrderCancelled, and TradeExecuted carry account information and
    // therefore matter here; OrderRejected and every Book* event
    // (deliberately anonymous, see events.hpp's own class comment) are
    // no-ops.
    void apply(const ExchangeEvent& event);
    [[nodiscard]] EventSink sink() {
        return [this](const ExchangeEvent& event) { apply(event); };
    }

private:
    struct Hold {
        InstrumentId instrument_id;
        Side side;
        Price limit_price; // meaningful for Side::Buy; irrelevant (but harmless) for Side::Sell
        Quantity remaining;
    };

    struct HoldKey {
        AccountId account_id;
        ClientOrderId client_order_id;

        bool operator==(const HoldKey& other) const {
            return account_id == other.account_id && client_order_id == other.client_order_id;
        }
    };

    struct HoldKeyHash {
        std::size_t operator()(const HoldKey& key) const noexcept {
            return std::hash<AccountId>{}(key.account_id) ^ (std::hash<ClientOrderId>{}(key.client_order_id) << 1);
        }
    };

    void open_hold(AccountId account_id, ClientOrderId client_order_id, InstrumentId instrument_id, Side side,
                   Price limit_price, Quantity quantity);
    // Releases `hold`'s entire currently-remaining reservation back to
    // `available` (does not touch `total` -- releasing a reservation is not
    // a settlement, see class-level comment).
    void release_hold_fully(AccountId account_id, const Hold& hold);
    void settle_leg(const TradeCounterparty& leg, Side side, InstrumentId instrument_id, Price trade_price,
                     Quantity trade_quantity);

    void on_order_accepted(const OrderAccepted& event);
    void on_order_replaced(const OrderReplaced& event);
    void on_order_cancelled(const OrderCancelled& event);
    void on_trade_executed(const TradeExecuted& event);

    std::unordered_map<AccountId, AccountBalances> accounts_;
    std::unordered_map<HoldKey, Hold, HoldKeyHash> holds_;
};

} // namespace mdh::exchange::ledger
