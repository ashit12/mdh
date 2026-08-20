#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <memory>
#include <memory_resource>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>

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
    // once. Two structures are sized from this figure: the directory, which
    // holds one entry per resting order, and each book's order slab, which
    // takes an equal share of it. Getting it wrong is not a correctness
    // problem -- both grow either way -- but growth is where this engine's
    // worst latencies come from, by a wide margin: measured at a million
    // resting orders, the same insert costs 81 ns against a directory sized
    // up front and 1814 ns against one that grew into it, and the workload
    // benchmark's worst single operation improves from 1.3 ms to 0.12 ms.
    // The slab shows the same effect at 47 ns against 104. Over-estimating
    // costs 8 bytes per unused directory slot and 56 per unused slab entry,
    // in two flat arrays, so it is much the cheaper direction to be wrong in.
    //
    // The default suits a test or a small book. Anything running real order
    // flow should say what it expects.
    static constexpr std::size_t kDefaultExpectedRestingOrders = 1024;

    // The largest instrument id the registry will accept. The id-to-slot
    // table is direct-mapped and sized to the largest id registered, so this
    // caps that table at 16 MB no matter what is asked for. Instrument ids
    // are assigned from reference data, not chosen by clients, so a
    // deployment needing ids above this has a numbering problem rather than
    // a capacity one.
    static constexpr InstrumentId kMaxInstrumentId = (1U << 22) - 1;

    // What the whole engine may spend on tick ladders, divided evenly across
    // the universe. Each book indexes a band of prices with a flat array and
    // falls back to a map outside it, so this is the knob that decides how
    // wide that band is -- and, for a universe large enough that the share
    // stops being worth its fixed cost, whether there is a ladder at all.
    //
    // 8 MB buys the full band for 64 instruments. It is a budget rather than
    // a per-book size because the cost is instruments x band x 2 sides: the
    // one figure a book cannot work out for itself.
    static constexpr std::size_t kLadderByteBudget = 8U << 20;

    // `universe` is every instrument this engine will trade. Commands naming
    // anything else are rejected with RejectReason::InvalidInstrument rather
    // than quietly conjuring a book, which is what the engine used to do --
    // and since instrument ids arrive from clients, "quietly conjure a book"
    // meant a client could make the engine allocate one per id it invented.
    //
    // Ids need not be contiguous or start anywhere in particular; the
    // registry maps them to dense internal slots. Ids above kMaxInstrumentId,
    // and repeats, are skipped: there is no exception to throw here (this
    // codebase does not use them) and no silent corruption either, because
    // an instrument that failed to register simply stays unknown and its
    // commands are rejected in the ordinary, observable way.
    explicit MatchingEngine(std::span<const InstrumentId> universe,
                            std::size_t expected_resting_orders = kDefaultExpectedRestingOrders);
    explicit MatchingEngine(std::initializer_list<InstrumentId> universe,
                            std::size_t expected_resting_orders = kDefaultExpectedRestingOrders);

    // Adds one instrument after construction. Exists for replay, which
    // learns the universe from the journal's own RegisterInstrument frames
    // as it reads them (see command_replay.hpp) rather than being told it
    // out of band. Returns false if the id is already registered or above
    // kMaxInstrumentId.
    //
    // Safe while orders are resting: growing the book vector moves each
    // MatchingBook, and outstanding handles survive that because a book's
    // pool sits behind a unique_ptr (so its address is stable) and moving a
    // pmr container with an equal allocator steals nodes rather than
    // copying them. MatchingEngineTest.RegisteringAnInstrumentKeepsRestingOrdersReachable
    // is what keeps that true.
    bool register_instrument(InstrumentId instrument_id);

    [[nodiscard]] bool knows_instrument(InstrumentId instrument_id) const {
        return instrument_id < slot_of_id_.size() && slot_of_id_[instrument_id] != kNoSlot;
    }

    [[nodiscard]] std::size_t instrument_count() const { return books_.size(); }

    // How wide a band each book registered from here on will index with a
    // ladder, in ticks; zero when the universe is too large for one to be
    // worth its fixed cost. Reported so a benchmark can say which structure
    // it is actually measuring.
    [[nodiscard]] std::uint32_t ladder_band_ticks() const { return band_ticks_; }

    // Resting price levels that fell outside their book's band and so are
    // held by the fallback map, summed across instruments. The ladder was
    // built for this to be zero; a figure that tracks the level count means
    // the band is in the wrong place or too narrow, and that is worth being
    // able to read off a benchmark rather than infer from a latency.
    [[nodiscard]] std::size_t out_of_band_levels() const;

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
    // engine instances. Instruments are emitted in ascending id order, and
    // registered instruments with nothing resting are omitted, so two
    // engines configured with different universes still compare equal if
    // the same orders are resting on both.
    [[nodiscard]] EngineStateSnapshot snapshot() const;

private:
    static constexpr std::uint32_t kNoSlot = ~0U;

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
    //
    // Fields are ordered widest-first for the reason given on
    // ExchangeRestingOrder: written in the order they group by meaning this
    // would pad to 32 bytes. At 24 it makes a hash node of 56 -- pointer,
    // cached hash, key, value -- which is what fits the pool's 64-byte
    // block, the same constraint BookOrder is under.
    struct OrderRef {
        Quantity original_quantity;
        std::uint64_t order_sequence;
        MatchingBook::Handle handle;
        InstrumentId instrument_id;
    };

    static_assert(sizeof(OrderRef) == 24, "see the note above on what this size is protecting");

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

    // Both require the instrument to be registered -- every caller has
    // already been past knows_instrument(), which is the check that turns
    // an unregistered id into a rejection.
    MatchingBook& book_for(InstrumentId instrument_id) { return books_[slot_of_id_[instrument_id]]; }
    [[nodiscard]] const MatchingBook& book_for(InstrumentId instrument_id) const {
        return books_[slot_of_id_[instrument_id]];
    }

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
    // one. The books stay on the general heap: one per *instrument*, a
    // number that does not grow with order flow. Declared before them, so it
    // outlives them (see MatchingBook::pool_).
    std::unique_ptr<std::pmr::unsynchronized_pool_resource> pool_;

    // The registry, in three parts. slot_of_id_ is direct-mapped rather than
    // hashed: every command carries an instrument id, so this lookup is on
    // the hottest path there is, and a bounds check plus an array load beats
    // a hash, a bucket load and a node hop. The bounds check is also the
    // validity check -- an id past the end is simply not registered.
    std::vector<std::uint32_t> slot_of_id_;
    std::vector<MatchingBook> books_; // indexed by slot, dense
    // Each book's share of expected_resting_orders, fixed at construction so
    // that a book registered later is sized like the ones that came with the
    // universe.
    std::size_t expected_orders_per_book_ = 0;
    // Each book's share of kLadderByteBudget, expressed as a band width.
    // Only ever narrows: an engine told its universe up front sizes this once
    // and every book gets the same band, while one that learns its
    // instruments as it goes (replay) shrinks the band for books registered
    // later rather than going back over books already built.
    std::uint32_t band_ticks_ = MatchingBook::kMaxBandTicks;
    // Registration order is not id order, and snapshot() must emit
    // instruments by ascending id, so it walks this instead of sorting.
    std::vector<std::pair<InstrumentId, std::uint32_t>> by_id_;

    std::pmr::unordered_map<LiveKey, OrderRef, LiveKeyHash> orders_;

    // Deterministic, engine-owned counters -- no timestamps, no randomness,
    // per the working rules on determinism in the exchange core.
    ExchangeOrderId next_exchange_order_id_ = 1;
    EventSequence next_event_sequence_ = 1;
    std::uint64_t next_priority_ = 1;
};

} // namespace mdh::exchange
