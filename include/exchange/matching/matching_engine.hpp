#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <memory_resource>
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
    // Roughly how many orders will be resting across all instruments at
    // once. The engine keeps one directory entry per resting order, and
    // sizes that table from this figure so a book that fills up does not
    // have to grow it repeatedly under load. Getting it wrong is not a
    // correctness problem -- the table grows either way -- but growth is
    // where this engine's worst latencies come from, by a wide margin:
    // measured at a million resting orders, the same insert costs 81 ns
    // against a table sized up front and 1814 ns against one that grew into
    // it, and the workload benchmark's worst single operation improves from
    // 1.3 ms to 0.12 ms. Over-estimating costs 8 bytes per unused slot, in
    // exactly one array, so it is much the cheaper direction to be wrong in.
    //
    // The default suits a test or a small book. Anything running real order
    // flow should say what it expects.
    static constexpr std::size_t kDefaultExpectedRestingOrders = 1024;

    explicit MatchingEngine(std::size_t expected_resting_orders = kDefaultExpectedRestingOrders);

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

    // Same as reject_new_order(), for a ReplaceOrderCommand rejected by
    // RiskEngine before process_replace() runs. Reports under
    // original_client_order_id (MatchingEngine's own convention for every
    // replace rejection). The resting order and its ledger hold are left
    // untouched -- the caller must not have invoked process() for this
    // command.
    void reject_replace_order(const ReplaceOrderCommand& command, RejectReason reason, const EventSink& sink);

    // A canonical, deterministically-ordered dump of every resting order
    // across every instrument (Milestone 3) -- see state_snapshot.hpp for
    // why this is safe to compare with == across two independently-built
    // engine instances despite books_ being an unordered_map internally.
    [[nodiscard]] EngineStateSnapshot snapshot() const;

private:
    // Everything the engine knows about one live resting order that the
    // book itself does not: which book it is in, whereabouts in that book,
    // and the two fields only a snapshot ever reads.
    //
    // This is the single per-order directory. It replaces a pair of tables
    // -- this one, plus an index inside each MatchingBook keyed by exchange
    // order id -- whose lookups were strictly serial: find the order's
    // exchange id here, then find its list node there. Every cancel and
    // replace paid both misses, and every resting order carried two hash
    // nodes that grew and rehashed independently.
    struct OrderRef {
        MatchingBook::Handle handle;
        Quantity original_quantity;
        std::uint64_t order_sequence;
        InstrumentId instrument_id;
    };

    // A client order id is only unique per-account (two different accounts
    // may independently choose client_order_id == 1), so live-order lookup
    // is keyed on the pair, not client_order_id alone. Keyed globally
    // rather than per-instrument, which is what makes a client order id
    // already live on *another* instrument a duplicate.
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

    MatchingBook& book_for(InstrumentId instrument_id);

    // Puts a resting order back together for the snapshot: the book holds
    // the order, orders_ holds the fields the book gave up, and the caller
    // supplies the instrument the book belongs to. One hash lookup per
    // order, on a path that already copies the entire book.
    [[nodiscard]] ExchangeRestingOrder compose(const BookOrder& order, InstrumentId instrument_id) const;

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

    // orders_ holds one node per resting order -- now the only one outside
    // the book's own list node -- so it draws from an engine-owned pool for
    // the same reason each MatchingBook's containers draw from a book-owned
    // one. books_ stays on the general heap: it holds one node per
    // *instrument*, a number that does not grow with order flow. Declared
    // before both, so it outlives them (see MatchingBook::pool_).
    std::unique_ptr<std::pmr::unsynchronized_pool_resource> pool_;
    std::unordered_map<InstrumentId, MatchingBook> books_;
    std::pmr::unordered_map<LiveKey, OrderRef, LiveKeyHash> orders_;

    // Deterministic, engine-owned counters -- no timestamps, no randomness,
    // per the working rules on determinism in the exchange core.
    ExchangeOrderId next_exchange_order_id_ = 1;
    EventSequence next_event_sequence_ = 1;
    std::uint64_t next_priority_ = 1;
};

} // namespace mdh::exchange
