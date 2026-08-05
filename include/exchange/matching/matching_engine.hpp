#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <unordered_map>

#include "common/types.hpp"
#include "exchange/core/commands.hpp"
#include "exchange/core/event_sink.hpp"
#include "exchange/matching/matching_book.hpp"
#include "exchange/matching/resting_order.hpp"
#include "exchange/matching/state_snapshot.hpp"

// The exchange's authoritative matching engine (Milestone 2):
// ExchangeCommand -> MatchingEngine::process() -> zero or more ExchangeEvent
// delivered through an EventSink, in order, on the calling thread. Single-
// threaded and deterministic: every decision depends only on the command's
// own fields and the current book state, never on wall-clock time, thread
// scheduling, or any randomness. Does not touch sockets, files, a database,
// a logger, the trader-side book::OrderBook, or anything else outside its
// own in-memory book -- that separation is what keeps it deterministic and
// unit-testable without any of those dependencies.
namespace mdh::exchange {

// ── Replace policy (documented, not accidental) ────────────────────────────
// - Quantity decrease (or unchanged quantity) at the same price preserves
//   time priority: the resting order is mutated in place (same
//   exchange_order_id, same FIFO position), no re-matching occurs.
// - A price change, or a quantity *increase*, loses priority: implemented as
//   cancel-plus-new. The old resting order is removed and a brand new one
//   (new exchange_order_id) re-enters the same matching path a fresh
//   NewOrderCommand uses, so a replace that reprices into a crossing price
//   executes immediately, exactly like any other aggressive order.
//
// ── Self-trade policy ───────────────────────────────────────────────────────
// Deferred. Orders from the same account are allowed to match each other
// normally; TradeExecuted carries both accounts as-is with no special
// handling. A future milestone may add prevention; this one does not.
class MatchingEngine {
public:
    void process(const ExchangeCommand& command, const EventSink& sink);

    // Emits an OrderRejected for `command` using this engine's own
    // event_sequence counter, without touching any book state (Milestone
    // 5): for a pre-trade rejection decided *before* a command is even
    // handed to process() (exchange/risk/'s RiskEngine) -- so that
    // event_sequence numbering stays globally gapless and strictly
    // increasing across every event this engine has ever produced,
    // regardless of whether a given rejection happened inside or outside
    // process() itself. Does not, by itself, decide *whether* to reject --
    // that policy question belongs entirely to the caller (RiskEngine); this
    // is purely "emit the event using the right counter."
    void reject_new_order(const NewOrderCommand& command, RejectReason reason, const EventSink& sink);

    // A canonical, deterministically-ordered dump of every resting order
    // across every instrument (Milestone 3) -- see state_snapshot.hpp for
    // why this is safe to compare with == across two independently-built
    // engine instances despite books_ being an unordered_map internally.
    [[nodiscard]] EngineStateSnapshot snapshot() const;

private:
    struct LiveOrderRef {
        InstrumentId instrument_id;
        ExchangeOrderId exchange_order_id;
    };

    // A client order id is only unique per-account (two different accounts
    // may independently choose client_order_id == 1), so live-order lookup
    // is keyed on the pair, not client_order_id alone.
    struct LiveKey {
        AccountId account_id;
        ClientOrderId client_order_id;

        bool operator==(const LiveKey& other) const {
            return account_id == other.account_id && client_order_id == other.client_order_id;
        }
    };

    struct LiveKeyHash {
        std::size_t operator()(const LiveKey& key) const noexcept {
            return std::hash<AccountId>{}(key.account_id) ^ (std::hash<ClientOrderId>{}(key.client_order_id) << 1);
        }
    };

    void process_new_order(const NewOrderCommand& cmd, const EventSink& sink);
    void process_cancel(const CancelOrderCommand& cmd, const EventSink& sink);
    void process_replace(const ReplaceOrderCommand& cmd, const EventSink& sink);

    // Matches `incoming` against the opposite side of its instrument's book
    // (price-time priority, one resting order at a time via
    // MatchingBook::front_of_best), mutating the book and emitting
    // TradeExecuted + BookOrderReduced/BookOrderRemoved as it goes.
    // `incoming.remaining_quantity` is decremented in place as fills occur;
    // `incoming` itself is never added to the book by this function --
    // callers decide what happens to any remainder (see
    // rest_remainder_if_applicable).
    void match_and_rest(ExchangeRestingOrder& incoming, CommandSequence command_sequence, const EventSink& sink);

    // GTC: any remainder rests on the book (BookOrderAdded emitted, order
    // becomes live). IOC/FOK: any remainder is discarded silently -- it was
    // never accepted as resting, so there is nothing to cancel or announce.
    void rest_remainder_if_applicable(const ExchangeRestingOrder& order, const EventSink& sink);

    // Sums resting quantity that would immediately cross at `price` or
    // better, without mutating the book -- FOK's all-or-none pre-check.
    [[nodiscard]] Quantity crossable_quantity(InstrumentId instrument_id, Side incoming_side, Price price,
                                               Quantity quantity) const;

    std::unordered_map<InstrumentId, MatchingBook> books_;
    std::unordered_map<LiveKey, LiveOrderRef, LiveKeyHash> live_orders_;

    // Deterministic, engine-owned counters -- no timestamps, no randomness,
    // per the working rules on determinism in the exchange core.
    ExchangeOrderId next_exchange_order_id_ = 1;
    EventSequence next_event_sequence_ = 1;
    std::uint64_t next_priority_ = 1;
};

} // namespace mdh::exchange
