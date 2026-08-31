#pragma once

#include <atomic>
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

// The exchange's authoritative matching engine: a command goes in, zero or
// more events come out through an EventSink, in order, on the calling
// thread.
//
// Single-threaded and deterministic. Every decision depends only on the
// command's own fields and the current book state -- never on the clock,
// thread scheduling, or randomness. It touches no sockets, files or logs,
// which is what makes it replayable and testable on its own.
namespace mdh::exchange {

// ── Replace policy ─────────────────────────────────────────────────────────
// A quantity decrease at the same price keeps time priority: the resting
// order is edited in place, keeping its exchange order id and its position
// in the queue. Anything else -- a price change, or a quantity increase --
// loses priority and is done as cancel-plus-new, so a replace that reprices
// into a crossing price executes immediately like any other aggressive
// order.
//
// ── Self-trade policy ──────────────────────────────────────────────────────
// Not implemented. Two orders from the same account match each other
// normally, and TradeExecuted reports both accounts as-is.
class MatchingEngine {
public:
    // Roughly how many orders will be resting across all instruments at
    // once. Two structures are sized from it: the directory, which holds one
    // entry per resting order, and each book's order slab, which takes an
    // equal share.
    //
    // Guessing wrong is not a correctness problem -- both grow either way --
    // but growth is where this engine's worst latencies come from. Measured
    // at a million resting orders, one insert costs 81 ns against a
    // directory sized up front and 1814 ns against one growing into itself;
    // the slab shows the same effect at 47 ns against 104. Guessing high
    // costs 8 bytes per unused directory slot and 56 per unused slab entry,
    // so it is much the cheaper direction to be wrong in.
    //
    // The default suits a test or a small book. Anything carrying real order
    // flow should say what it expects.
    static constexpr std::size_t kDefaultExpectedRestingOrders = 1024;

    // The largest instrument id the registry accepts. The id-to-slot table
    // is a flat array sized to the largest id registered, so this caps it at
    // 16 MB. Instrument ids come from reference data rather than from
    // clients, so needing ids above this is a numbering problem, not a
    // capacity one.
    static constexpr InstrumentId kMaxInstrumentId = (1U << 22) - 1;

    // What the whole engine may spend on tick ladders, split evenly across
    // the universe. Each book indexes a band of prices with a flat array and
    // falls back to a map outside it, so this decides how wide that band is
    // -- and, for a universe large enough that its share stops being worth
    // the fixed cost, whether there is a ladder at all.
    //
    // 8 MB buys the full band for 64 instruments. It is a budget rather than
    // a per-book size because the cost is instruments x band x 2 sides, and
    // the instrument count is the part a single book cannot know.
    static constexpr std::size_t kLadderByteBudget = 8U << 20;

    // `universe` is every instrument this engine will trade. A command
    // naming anything else is rejected with InvalidInstrument rather than
    // quietly getting a book of its own -- instrument ids arrive from
    // clients, so that older behaviour let a client allocate one book per id
    // it invented.
    //
    // Ids need not be contiguous; the registry maps them to dense internal
    // slots. Duplicates, and ids above kMaxInstrumentId, are skipped.
    // Nothing is thrown: an instrument that fails to register simply stays
    // unknown, and its commands are rejected the ordinary, visible way.
    explicit MatchingEngine(std::span<const InstrumentId> universe,
                            std::size_t expected_resting_orders = kDefaultExpectedRestingOrders);
    explicit MatchingEngine(std::initializer_list<InstrumentId> universe,
                            std::size_t expected_resting_orders = kDefaultExpectedRestingOrders);

    // Adds one instrument after construction. Replay needs this: it learns
    // the universe from the journal's own RegisterInstrument frames as it
    // reads them, rather than being told out of band. Returns false if the
    // id is already registered or above kMaxInstrumentId.
    //
    // Safe to call while orders are resting. Growing the book vector moves
    // each MatchingBook, and existing handles survive that: a book's pool
    // sits behind a unique_ptr so its address never moves, and moving a pmr
    // container with an equal allocator steals its nodes rather than
    // copying them. MatchingEngineTest.RegisteringAnInstrumentKeepsRestingOrdersReachable
    // is what keeps that true.
    bool register_instrument(InstrumentId instrument_id);

    [[nodiscard]] bool knows_instrument(InstrumentId instrument_id) const {
        return instrument_id < slot_of_id_.size() && slot_of_id_[instrument_id] != kNoSlot;
    }

    [[nodiscard]] std::size_t instrument_count() const { return books_.size(); }

    // How wide a band each book registered from here on indexes with a
    // ladder, in ticks; zero when the universe is too large for one to be
    // worth its fixed cost. Exposed so a benchmark can report which
    // structure it actually measured.
    [[nodiscard]] std::uint32_t ladder_band_ticks() const { return band_ticks_; }

    // Resting price levels that fell outside their book's band and are held
    // by the fallback map instead, summed across instruments. The ladder is
    // built for this to be zero; a number that tracks the level count means
    // the band is too narrow or in the wrong place, which is worth reading
    // off a benchmark rather than inferring from a latency.
    [[nodiscard]] std::size_t out_of_band_levels() const;

    void process(const ExchangeCommand& command, const EventSink& sink);

    // Emits an OrderRejected for `command` using this engine's own
    // event-sequence counter, touching no book state. It exists for
    // rejections decided before a command ever reaches process() -- the
    // pre-trade risk check -- so event sequence numbers stay gapless and
    // increasing across everything this engine emits, wherever the rejection
    // was decided. It does not decide *whether* to reject; that is the
    // caller's business.
    void reject_new_order(const NewOrderCommand& command, RejectReason reason, const EventSink& sink);

    // The same, for a replace rejected before process_replace() runs.
    // Reported under original_client_order_id, as every replace rejection
    // is. The resting order and its ledger hold are left alone, so the
    // caller must not also call process() for this command.
    void reject_replace_order(const ReplaceOrderCommand& command, RejectReason reason, const EventSink& sink);

    // A canonical dump of every resting order across every instrument, in a
    // fixed order, so two independently-built engines can be compared with
    // == (see state_snapshot.hpp). Instruments come out in ascending id
    // order, and registered instruments with nothing resting are left out --
    // so two engines with different universes still compare equal when the
    // same orders rest on both.
    [[nodiscard]] EngineStateSnapshot snapshot() const;

    // Directory size: how many orders currently rest. Safe from any thread.
    [[nodiscard]] std::size_t resting_order_count() const {
        return resting_orders_->load(std::memory_order_relaxed);
    }

    struct BookMemoryStats {
        std::size_t live_orders = 0;
        std::size_t slab_capacity = 0;
    };

    // Sum of per-book slab occupancy. Matching-thread or after stop() only.
    [[nodiscard]] BookMemoryStats book_memory_stats() const;

private:
    static constexpr std::uint32_t kNoSlot = ~0U;

    // Everything the engine knows about a live resting order that the book
    // does not: which book it is in, where in that book, and the two fields
    // only a snapshot reads.
    //
    // This is the one per-order directory. It replaced a pair of tables --
    // this one plus a per-book index keyed by exchange order id -- whose
    // lookups ran strictly one after the other, so every cancel and replace
    // paid both cache misses and every resting order carried two hash nodes
    // that grew and rehashed independently.
    //
    // Fields are widest-first on purpose, for the reason given on
    // ExchangeRestingOrder: grouped by meaning instead, this would pad to 32
    // bytes. At 24 it makes a 56-byte hash node -- pointer, cached hash,
    // key, value -- which fits the pool's 64-byte block, the same
    // constraint BookOrder is under.
    struct OrderRef {
        Quantity original_quantity;
        std::uint64_t order_sequence;
        MatchingBook::Handle handle;
        InstrumentId instrument_id;
    };

    static_assert(sizeof(OrderRef) == 24, "see the note above on what this size is protecting");

    // A client order id is only unique within an account -- two accounts may
    // both pick id 1 -- so live orders are keyed on the pair. The key is
    // global rather than per-instrument, which is what makes an id already
    // live on another instrument count as a duplicate.
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

    // Both assume the instrument is registered. Every caller has already
    // been through knows_instrument(), which is the check that turns an
    // unregistered id into a rejection.
    MatchingBook& book_for(InstrumentId instrument_id) { return books_[slot_of_id_[instrument_id]]; }
    [[nodiscard]] const MatchingBook& book_for(InstrumentId instrument_id) const {
        return books_[slot_of_id_[instrument_id]];
    }

    // Rebuilds a full resting order for the snapshot: the book holds the
    // order, orders_ holds the fields the book gave up, and the caller
    // supplies the instrument. One hash lookup per order, on a path that
    // already copies the whole book.
    [[nodiscard]] ExchangeRestingOrder compose(const BookOrder& order, InstrumentId instrument_id) const;

    void process_new_order(const NewOrderCommand& cmd, const EventSink& sink);
    void process_cancel(const CancelOrderCommand& cmd, const EventSink& sink);
    void process_replace(const ReplaceOrderCommand& cmd, const EventSink& sink);

    // Matches `incoming` against the other side of its book in price-time
    // priority, one resting order at a time, emitting TradeExecuted and
    // BookOrderReduced/BookOrderRemoved as it goes and decrementing
    // `incoming.remaining_quantity` in place. Never adds `incoming` to the
    // book itself -- what happens to any remainder is the caller's decision
    // (see rest_remainder_if_applicable).
    void match_and_rest(ExchangeRestingOrder& incoming, CommandSequence command_sequence, const EventSink& sink);

    // GTC: any remainder rests on the book and a BookOrderAdded is emitted.
    // IOC/FOK: any remainder is discarded silently -- it was never resting,
    // so there is nothing to cancel or announce.
    void rest_remainder_if_applicable(const ExchangeRestingOrder& order, const EventSink& sink);

    // Sums the resting quantity that would immediately cross at `price` or
    // better, without touching the book. This is FOK's all-or-nothing
    // pre-check.
    [[nodiscard]] Quantity crossable_quantity(InstrumentId instrument_id, Side incoming_side, Price price,
                                               Quantity quantity) const;

    // orders_ holds one node per resting order -- the only one left outside
    // the book's own slab -- so it draws from an engine-owned pool, for the
    // same reason each book's containers draw from a book-owned one. The
    // books themselves stay on the general heap: there is one per
    // instrument, and that count does not grow with order flow. Declared
    // first so it outlives them (see MatchingBook::pool_).
    std::unique_ptr<std::pmr::unsynchronized_pool_resource> pool_;

    // The registry, in three parts. slot_of_id_ is a flat array rather than
    // a hash map because every command carries an instrument id, making this
    // the hottest lookup in the engine, and a bounds check plus an array
    // load beats a hash, a bucket load and a node hop. The bounds check
    // doubles as the validity check: an id past the end is not registered.
    std::vector<std::uint32_t> slot_of_id_;
    std::vector<MatchingBook> books_; // indexed by slot, dense
    // Each book's share of expected_resting_orders, fixed at construction so
    // a book registered later is sized like the ones that came with the
    // universe.
    std::size_t expected_orders_per_book_ = 0;
    // Each book's share of kLadderByteBudget, as a band width. It only ever
    // narrows: an engine told its universe up front sizes this once and
    // gives every book the same band, while one learning its instruments as
    // it goes (replay) narrows the band for later books rather than
    // rebuilding the ones already made.
    std::uint32_t band_ticks_ = MatchingBook::kMaxBandTicks;
    // Registration order is not id order, and snapshot() must emit
    // instruments by ascending id, so it walks this rather than sorting.
    std::vector<std::pair<InstrumentId, std::uint32_t>> by_id_;

    std::pmr::unordered_map<LiveKey, OrderRef, LiveKeyHash> orders_;
    std::unique_ptr<std::atomic<std::size_t>> resting_orders_ = std::make_unique<std::atomic<std::size_t>>(0);

    // Engine-owned counters -- no clock, no randomness, so a replay produces
    // the same numbers.
    ExchangeOrderId next_exchange_order_id_ = 1;
    EventSequence next_event_sequence_ = 1;
    std::uint64_t next_priority_ = 1;
};

} // namespace mdh::exchange
