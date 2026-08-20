#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <unordered_map>
#include <variant>
#include <vector>

#include "common/types.hpp"
#include "exchange/core/commands.hpp"
#include "exchange/core/event_sink.hpp"
#include "exchange/core/events.hpp"
#include "exchange/matching/matching_engine.hpp"
#include "exchange/testing/matching_scenarios.hpp"

// A deterministic generator for a realistic mixed order stream, shared by
// the mixed-workload benchmarks and the stress harness.
//
// Header-only and test/benchmark-only. It drives MatchingEngine only through
// the public process() entry point and changes nothing about how the engine
// behaves.
//
// ── Why the generator runs its own engine ──────────────────────────────────
// A workload is only realistic if its cancels and replaces mostly *hit*: a
// stream where 20% of operations are cancels of order ids that were already
// filled measures the rejection path, not the cancel path. Rather than
// guessing which orders are still resting, the generator plays every command
// it emits into its own MatchingEngine instance and tracks liveness from the
// resulting event stream, so each cancel/replace it produces targets an
// order that genuinely rests at that point in the sequence.
//
// The consumer then replays the identical command sequence (seed phase
// first, then operations) into a *fresh* engine. Because MatchingEngine is
// deterministic -- every decision depends only on the command's fields and
// current book state, never on time or scheduling -- the replay engine
// passes through exactly the same states the generator saw, so the
// pre-computed command stream stays valid with zero generation cost inside
// any measured region.
//
// ── Reproducibility ────────────────────────────────────────────────────────
// The only entropy is WorkloadConfig::seed. The RNG is a self-contained
// SplitMix64 with a hand-written unbiased bounded draw rather than
// <random>'s distributions, whose output is implementation-defined and
// therefore not reproducible across standard libraries -- a seed printed in
// docs/matching_engine_baseline.md has to mean the same stream on somebody
// else's machine for a recorded number to be checkable.
namespace mdh::exchange::testing {

// ── Deterministic RNG ──────────────────────────────────────────────────────

class SplitMix64 {
public:
    explicit SplitMix64(std::uint64_t seed) : state_(seed) {}

    std::uint64_t next() {
        state_ += 0x9E3779B97F4A7C15ULL;
        std::uint64_t z = state_;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31);
    }

    // Uniform in [0, bound), rejection-sampled so the low values are not
    // over-represented the way a bare `next() % bound` would make them.
    std::uint64_t below(std::uint64_t bound) {
        if (bound <= 1) {
            return 0;
        }
        // (2^64 - bound) % bound == 2^64 % bound: the size of the partial
        // final block that has to be rejected for the draw to stay uniform.
        const std::uint64_t reject_below = (std::numeric_limits<std::uint64_t>::max() - bound + 1) % bound;
        std::uint64_t draw = next();
        while (draw < reject_below) {
            draw = next();
        }
        return draw % bound;
    }

    // Uniform in [low, high], inclusive.
    std::uint64_t between(std::uint64_t low, std::uint64_t high) {
        return low >= high ? low : low + below(high - low + 1);
    }

private:
    std::uint64_t state_;
};

// ── Liveness tracking, driven purely by the engine's own event stream ──────

struct LiveOrderRecord {
    ExchangeOrderId exchange_order_id;
    ClientOrderId client_order_id;
    AccountId account_id;
    InstrumentId instrument_id;
    Side side;
    Price price;
    Quantity remaining_quantity;
};

// Every resting order the engine currently holds, supporting O(1) insert,
// O(1) erase by exchange order id, and O(1) uniform random selection (which
// is what makes generating a cancel-heavy workload cheap).
class LiveOrderIndex {
public:
    void insert(const LiveOrderRecord& record) {
        position_[record.exchange_order_id] = records_.size();
        records_.push_back(record);
    }

    void erase(ExchangeOrderId exchange_order_id) {
        auto it = position_.find(exchange_order_id);
        if (it == position_.end()) {
            return;
        }
        const std::size_t slot = it->second;
        const std::size_t last = records_.size() - 1;
        if (slot != last) {
            records_[slot] = records_[last];
            position_[records_[slot].exchange_order_id] = slot;
        }
        records_.pop_back();
        position_.erase(it);
    }

    void set_remaining_quantity(ExchangeOrderId exchange_order_id, Quantity remaining) {
        if (auto* record = find(exchange_order_id); record != nullptr) {
            record->remaining_quantity = remaining;
        }
    }

    void set_client_order_id(ExchangeOrderId exchange_order_id, ClientOrderId client_order_id) {
        if (auto* record = find(exchange_order_id); record != nullptr) {
            record->client_order_id = client_order_id;
        }
    }

    [[nodiscard]] LiveOrderRecord* find(ExchangeOrderId exchange_order_id) {
        auto it = position_.find(exchange_order_id);
        return it == position_.end() ? nullptr : &records_[it->second];
    }

    [[nodiscard]] const LiveOrderRecord& at(std::size_t slot) const { return records_[slot]; }
    [[nodiscard]] std::size_t size() const { return records_.size(); }
    [[nodiscard]] bool empty() const { return records_.empty(); }
    [[nodiscard]] const std::vector<LiveOrderRecord>& records() const { return records_; }

private:
    std::vector<LiveOrderRecord> records_;
    std::unordered_map<ExchangeOrderId, std::size_t> position_;
};

// Per-instrument, per-side price-level occupancy, maintained from
// BookOrderAdded/BookOrderRemoved so the generator knows where the touch is
// without calling MatchingEngine::snapshot() (which copies the entire book
// and would make generation quadratic).
class DepthTracker {
public:
    explicit DepthTracker(std::uint32_t instrument_count)
        : levels_(static_cast<std::size_t>(instrument_count) * 2) {}

    void on_added(InstrumentId instrument_id, Side side, Price price) { ++levels_[slot(instrument_id, side)][price]; }

    void on_removed(InstrumentId instrument_id, Side side, Price price) {
        auto& side_levels = levels_[slot(instrument_id, side)];
        auto it = side_levels.find(price);
        if (it == side_levels.end()) {
            return;
        }
        if (--it->second == 0) {
            side_levels.erase(it);
        }
    }

    // Best bid is the highest bid price, best ask the lowest ask price.
    [[nodiscard]] std::optional<Price> best(InstrumentId instrument_id, Side side) const {
        const auto& side_levels = levels_[slot(instrument_id, side)];
        if (side_levels.empty()) {
            return std::nullopt;
        }
        return side == Side::Buy ? std::prev(side_levels.end())->first : side_levels.begin()->first;
    }

    [[nodiscard]] std::size_t level_count(InstrumentId instrument_id, Side side) const {
        return levels_[slot(instrument_id, side)].size();
    }

private:
    [[nodiscard]] std::size_t slot(InstrumentId instrument_id, Side side) const {
        return static_cast<std::size_t>(instrument_id - 1) * 2 + (side == Side::Buy ? 0 : 1);
    }

    std::vector<std::map<Price, std::size_t>> levels_;
};

[[nodiscard]] constexpr Side opposite_of(Side side) { return side == Side::Buy ? Side::Sell : Side::Buy; }

// ── Workload description ───────────────────────────────────────────────────

// Operation-kind ratios, in percent. Must sum to 100; whatever is left over
// after the first four buckets falls into the IOC/FOK bucket, so a mix that
// does not sum to 100 silently reshapes that last bucket rather than
// misbehaving.
struct WorkloadMix {
    unsigned new_resting_pct = 40;
    unsigned crossing_pct = 25;
    unsigned cancel_pct = 20;
    unsigned replace_pct = 10;
    unsigned ioc_fok_pct = 5;

    [[nodiscard]] static WorkloadMix realistic() { return WorkloadMix{}; }
    [[nodiscard]] static WorkloadMix all_resting() { return WorkloadMix{100, 0, 0, 0, 0}; }
    [[nodiscard]] static WorkloadMix all_crossing() { return WorkloadMix{0, 100, 0, 0, 0}; }
};

struct WorkloadConfig {
    std::uint64_t seed = 0xC0FFEE'12345678ULL;
    std::size_t operation_count = 100'000;
    std::uint32_t instrument_count = 1;
    // Resting orders placed per side, per instrument, before the measured
    // operations begin.
    std::size_t initial_orders_per_side = 1'000;
    std::uint32_t account_count = 8;
    Price base_price = 100'000;
    // Keeps two instruments' price bands from overlapping, purely cosmetic.
    Price instrument_price_offset = 10'000;
    // How far from the touch a passive order may be placed, in ticks. Also
    // sets roughly how many distinct price levels a side accumulates.
    Price price_band_ticks = 64;
    // How far *through* the touch an aggressive order may be priced, which
    // bounds how many levels one crossing order can sweep.
    Price aggressive_reach_ticks = 8;
    Quantity min_quantity = 1;
    Quantity max_quantity = 100;
    // Aggressive sizing is what decides whether book depth is stationary
    // over a long run, and it is set from a quantity-flow argument rather
    // than picked to look plausible. Per 100 operations the default mix adds
    // 40 passive orders and removes quantity via 20 cancels (a whole order
    // each) plus 30 aggressive orders (their own size each), so depth holds
    // roughly steady when
    //
    //     40 * mean(passive) == 20 * mean(passive) + 30 * mean(aggressive)
    //
    // i.e. when the mean aggressive order is about two thirds of the mean
    // passive one. With passive quantities uniform on [1, 100] (mean ~50.5)
    // that puts the aggressive mean near 34, hence the bound below. Without
    // this the book drains within a few tens of thousands of operations and
    // the "crossing" bucket silently degrades into more resting orders.
    Quantity aggressive_max_quantity = 67;
    // FOK is sized well past that balance point on purpose: an oversized
    // all-or-none order is the case that fails preflight, and both outcomes
    // belong in the baseline. Rejected FOKs consume nothing, so they do not
    // disturb the flow balance above.
    Quantity fok_max_quantity = 300;
    // Percent of cancels/replaces that deliberately reference an order that
    // has already been retired (filled, cancelled, or replaced away). Zero
    // for benchmarks -- a rejection is not the operation being measured --
    // and non-zero for the stress harness, where re-touching dead ids is
    // exactly the interesting case.
    unsigned stale_reference_pct = 0;
    WorkloadMix mix{};

    // The instruments this workload trades, numbered 1..instrument_count.
    // Any engine replaying the stream must be constructed with these, or
    // every command in it is rejected as InvalidInstrument.
    [[nodiscard]] std::vector<InstrumentId> instruments() const { return instrument_universe(instrument_count); }
};

struct WorkloadCounts {
    std::size_t new_resting = 0;
    std::size_t crossing = 0;
    std::size_t cancel = 0;
    std::size_t replace_priority_preserving = 0;
    std::size_t replace_priority_losing = 0;
    std::size_t ioc = 0;
    std::size_t fok = 0;
    // Operations that wanted a live order (cancel/replace) or resting
    // liquidity (crossing/IOC/FOK) and found none, so they were emitted as a
    // plain resting order instead.
    std::size_t downgraded_to_resting = 0;
    // Cancels/replaces deliberately aimed at an already-retired order id.
    std::size_t stale_references = 0;
};

// What each emitted operation was *meant* to be. Carried alongside the
// command stream so a latency distribution can be split by operation kind:
// a mixed workload whose p99 is 200x its p50 is only interesting once you
// know which kind of operation lives out in that tail.
enum class OpKind : std::uint8_t {
    Resting,
    Crossing,
    Cancel,
    ReplacePriorityPreserving,
    ReplacePriorityLosing,
    Ioc,
    Fok,
};

[[nodiscard]] constexpr const char* to_string(OpKind kind) {
    switch (kind) {
        case OpKind::Resting: return "resting";
        case OpKind::Crossing: return "crossing";
        case OpKind::Cancel: return "cancel";
        case OpKind::ReplacePriorityPreserving: return "replace (priority kept)";
        case OpKind::ReplacePriorityLosing: return "replace (priority lost)";
        case OpKind::Ioc: return "IOC";
        case OpKind::Fok: return "FOK";
    }
    return "unknown";
}

inline constexpr std::size_t kOpKindCount = 7;

struct Workload {
    WorkloadConfig config;
    // Builds the starting book. Replay this first, untimed.
    std::vector<ExchangeCommand> seed;
    // The stream under measurement.
    std::vector<ExchangeCommand> operations;
    // Parallel to `operations`, one entry each.
    std::vector<OpKind> kinds;
    WorkloadCounts counts;
    std::size_t resting_orders_after_seed = 0;
    std::size_t resting_orders_at_end = 0;
    std::size_t bid_levels_at_end = 0;
    std::size_t ask_levels_at_end = 0;
};

// ── Generation ─────────────────────────────────────────────────────────────

[[nodiscard]] inline Workload generate_workload(const WorkloadConfig& config) {
    Workload workload;
    workload.config = config;
    workload.seed.reserve(static_cast<std::size_t>(config.instrument_count) * config.initial_orders_per_side * 2);
    workload.operations.reserve(config.operation_count);
    workload.kinds.reserve(config.operation_count);

    MatchingEngine engine(config.instruments());
    SplitMix64 rng(config.seed);
    SequentialIds ids;
    LiveOrderIndex live;
    DepthTracker depth(config.instrument_count);

    std::vector<ExchangeEvent> events;
    const EventSink sink = [&events](const ExchangeEvent& event) { events.push_back(event); };

    // A short ring of recently-retired order references, so the stress
    // harness can aim commands at ids the engine has already forgotten.
    struct RetiredRef {
        AccountId account_id;
        ClientOrderId client_order_id;
        InstrumentId instrument_id;
    };
    std::vector<RetiredRef> retired;
    retired.reserve(1024);
    std::size_t retired_cursor = 0;
    const auto remember_retired = [&](const RetiredRef& ref) {
        if (retired.size() < 1024) {
            retired.push_back(ref);
        } else {
            retired[retired_cursor] = ref;
            retired_cursor = (retired_cursor + 1) % retired.size();
        }
    };

    // Plays one command into the generator's own engine and folds the
    // resulting events back into the liveness/depth model.
    const auto apply = [&](const ExchangeCommand& command) {
        events.clear();
        engine.process(command, sink);

        // Which client order id owns any BookOrderAdded this command emits.
        // BookOrderAdded carries only an exchange order id (it is a
        // depth-visible event, deliberately account-anonymous), so the
        // client-side identity has to come from the command itself.
        AccountId owner_account = 0;
        ClientOrderId owner_client_order_id = 0;
        std::visit(
            [&](const auto& cmd) {
                using T = std::decay_t<decltype(cmd)>;
                owner_account = cmd.account_id;
                if constexpr (std::is_same_v<T, NewOrderCommand>) {
                    owner_client_order_id = cmd.client_order_id;
                } else if constexpr (std::is_same_v<T, ReplaceOrderCommand>) {
                    owner_client_order_id = cmd.new_client_order_id;
                }
            },
            command);

        for (const auto& event : events) {
            std::visit(
                [&](const auto& ev) {
                    using T = std::decay_t<decltype(ev)>;
                    if constexpr (std::is_same_v<T, OrderReplaced>) {
                        // Priority-preserving replace: the order keeps its
                        // exchange order id and book position but becomes
                        // addressable under a new client order id. (On the
                        // priority-losing path this id is not live yet, so
                        // this is a no-op and the BookOrderAdded below does
                        // the real work.)
                        live.set_client_order_id(ev.exchange_order_id, ev.new_client_order_id);
                    } else if constexpr (std::is_same_v<T, BookOrderAdded>) {
                        depth.on_added(ev.instrument_id, ev.side, ev.price);
                        live.insert(LiveOrderRecord{
                            .exchange_order_id = ev.exchange_order_id,
                            .client_order_id = owner_client_order_id,
                            .account_id = owner_account,
                            .instrument_id = ev.instrument_id,
                            .side = ev.side,
                            .price = ev.price,
                            .remaining_quantity = ev.quantity,
                        });
                    } else if constexpr (std::is_same_v<T, BookOrderReduced>) {
                        live.set_remaining_quantity(ev.exchange_order_id, ev.new_remaining_quantity);
                    } else if constexpr (std::is_same_v<T, BookOrderRemoved>) {
                        depth.on_removed(ev.instrument_id, ev.side, ev.price);
                        if (const auto* record = live.find(ev.exchange_order_id); record != nullptr) {
                            remember_retired(RetiredRef{record->account_id, record->client_order_id,
                                                         record->instrument_id});
                        }
                        live.erase(ev.exchange_order_id);
                    }
                },
                event);
        }
    };

    const auto instrument_of = [&](std::uint64_t draw) {
        return static_cast<InstrumentId>(1 + draw % config.instrument_count);
    };
    const auto account_of = [&](std::uint64_t draw) {
        return static_cast<AccountId>(1 + draw % config.account_count);
    };
    const auto mid_of = [&](InstrumentId instrument_id) {
        return config.base_price + static_cast<Price>(instrument_id - 1) * config.instrument_price_offset;
    };

    // A price that cannot cross: strictly inside its own side relative to
    // both the instrument's reference mid and the current opposing touch.
    const auto passive_price = [&](InstrumentId instrument_id, Side side) {
        const Price mid = mid_of(instrument_id);
        const auto contra = depth.best(instrument_id, opposite_of(side));
        const auto offset = static_cast<Price>(rng.below(static_cast<std::uint64_t>(config.price_band_ticks)));
        if (side == Side::Buy) {
            Price ceiling = mid;
            if (contra.has_value()) {
                ceiling = std::min(ceiling, *contra - 1);
            }
            const Price price = ceiling - offset;
            return price > 0 ? price : Price{1};
        }
        Price floor_price = mid;
        if (contra.has_value()) {
            floor_price = std::max(floor_price, *contra + 1);
        }
        return floor_price + offset;
    };

    // A price that crosses the opposing touch by up to
    // aggressive_reach_ticks. std::nullopt when that side of the book is
    // empty, i.e. there is nothing to cross at all.
    const auto crossing_price = [&](InstrumentId instrument_id, Side side) -> std::optional<Price> {
        const auto contra = depth.best(instrument_id, opposite_of(side));
        if (!contra.has_value()) {
            return std::nullopt;
        }
        const auto reach = static_cast<Price>(rng.below(static_cast<std::uint64_t>(config.aggressive_reach_ticks)));
        if (side == Side::Buy) {
            return *contra + reach;
        }
        const Price price = *contra - reach;
        return price > 0 ? price : Price{1};
    };

    const auto emit_resting = [&](std::vector<ExchangeCommand>& out, InstrumentId instrument_id, Side side) {
        const Quantity quantity = rng.between(config.min_quantity, config.max_quantity);
        ExchangeCommand command{new_order(ids.take_command_sequence(), account_of(rng.next()),
                                           ids.take_client_order_id(), instrument_id, side,
                                           passive_price(instrument_id, side), quantity)};
        out.push_back(command);
        apply(command);
    };

    // ── Seed phase: build the starting book ────────────────────────────────
    for (InstrumentId instrument_id = 1; instrument_id <= config.instrument_count; ++instrument_id) {
        for (std::size_t n = 0; n < config.initial_orders_per_side; ++n) {
            emit_resting(workload.seed, instrument_id, Side::Buy);
            emit_resting(workload.seed, instrument_id, Side::Sell);
        }
    }
    workload.resting_orders_after_seed = live.size();

    // ── Operation phase ────────────────────────────────────────────────────
    const WorkloadMix& mix = config.mix;
    const unsigned resting_edge = mix.new_resting_pct;
    const unsigned crossing_edge = resting_edge + mix.crossing_pct;
    const unsigned cancel_edge = crossing_edge + mix.cancel_pct;
    const unsigned replace_edge = cancel_edge + mix.replace_pct;

    for (std::size_t op = 0; op < config.operation_count; ++op) {
        const auto roll = static_cast<unsigned>(rng.below(100));
        const InstrumentId instrument_id = instrument_of(rng.next());
        const Side side = (rng.next() & 1U) != 0 ? Side::Buy : Side::Sell;

        if (roll < resting_edge) {
            emit_resting(workload.operations, instrument_id, side);
            workload.kinds.push_back(OpKind::Resting);
            ++workload.counts.new_resting;
            continue;
        }

        if (roll < crossing_edge) {
            const auto price = crossing_price(instrument_id, side);
            if (!price.has_value()) {
                emit_resting(workload.operations, instrument_id, side);
                workload.kinds.push_back(OpKind::Resting);
                ++workload.counts.downgraded_to_resting;
                continue;
            }
            const Quantity quantity = rng.between(config.min_quantity, config.aggressive_max_quantity);
            ExchangeCommand command{new_order(ids.take_command_sequence(), account_of(rng.next()),
                                               ids.take_client_order_id(), instrument_id, side, *price, quantity)};
            workload.operations.push_back(command);
            workload.kinds.push_back(OpKind::Crossing);
            apply(command);
            ++workload.counts.crossing;
            continue;
        }

        const bool use_stale = config.stale_reference_pct > 0 && !retired.empty() &&
                                rng.below(100) < config.stale_reference_pct;

        if (roll < cancel_edge) {
            if (live.empty() && !use_stale) {
                emit_resting(workload.operations, instrument_id, side);
                workload.kinds.push_back(OpKind::Resting);
                ++workload.counts.downgraded_to_resting;
                continue;
            }
            AccountId target_account = 0;
            ClientOrderId target_client_order_id = 0;
            InstrumentId target_instrument = 0;
            if (use_stale) {
                const auto& ref = retired[rng.below(retired.size())];
                target_account = ref.account_id;
                target_client_order_id = ref.client_order_id;
                target_instrument = ref.instrument_id;
                ++workload.counts.stale_references;
            } else {
                const auto& record = live.at(rng.below(live.size()));
                target_account = record.account_id;
                target_client_order_id = record.client_order_id;
                target_instrument = record.instrument_id;
            }
            ExchangeCommand command{cancel_order(ids.take_command_sequence(), target_account, target_client_order_id,
                                                  target_instrument)};
            workload.operations.push_back(command);
            workload.kinds.push_back(OpKind::Cancel);
            apply(command);
            ++workload.counts.cancel;
            continue;
        }

        if (roll < replace_edge) {
            if (live.empty()) {
                emit_resting(workload.operations, instrument_id, side);
                workload.kinds.push_back(OpKind::Resting);
                ++workload.counts.downgraded_to_resting;
                continue;
            }
            const LiveOrderRecord record = live.at(rng.below(live.size()));
            const auto variant_roll = static_cast<unsigned>(rng.below(100));
            Price new_price = record.price;
            Quantity new_quantity = record.remaining_quantity;
            bool preserves_priority = true;
            if (variant_roll < 50) {
                // Same price, quantity reduced: the in-place path.
                new_quantity = std::max<Quantity>(1, record.remaining_quantity / 2);
            } else if (variant_roll < 75) {
                // Same price, quantity increased: cancel-plus-new.
                new_quantity = record.remaining_quantity + rng.between(1, config.max_quantity);
                preserves_priority = false;
            } else {
                // Repriced (still passive, so this measures the replace path
                // rather than turning into a disguised crossing order).
                new_price = passive_price(record.instrument_id, record.side);
                preserves_priority = new_price == record.price && new_quantity <= record.remaining_quantity;
            }
            AccountId target_account = record.account_id;
            ClientOrderId target_client_order_id = record.client_order_id;
            InstrumentId target_instrument = record.instrument_id;
            if (use_stale) {
                const auto& ref = retired[rng.below(retired.size())];
                target_account = ref.account_id;
                target_client_order_id = ref.client_order_id;
                target_instrument = ref.instrument_id;
                ++workload.counts.stale_references;
            }
            ExchangeCommand command{replace_order(ids.take_command_sequence(), target_account,
                                                   target_client_order_id, ids.take_client_order_id(),
                                                   target_instrument, new_price, new_quantity)};
            workload.operations.push_back(command);
            workload.kinds.push_back(preserves_priority ? OpKind::ReplacePriorityPreserving
                                                        : OpKind::ReplacePriorityLosing);
            apply(command);
            if (preserves_priority) {
                ++workload.counts.replace_priority_preserving;
            } else {
                ++workload.counts.replace_priority_losing;
            }
            continue;
        }

        // IOC/FOK.
        const bool fill_or_kill = (rng.next() & 1U) != 0;
        const auto price = crossing_price(instrument_id, side);
        if (!price.has_value()) {
            emit_resting(workload.operations, instrument_id, side);
            workload.kinds.push_back(OpKind::Resting);
            ++workload.counts.downgraded_to_resting;
            continue;
        }
        const Quantity ceiling = fill_or_kill ? config.fok_max_quantity : config.aggressive_max_quantity;
        const Quantity quantity = rng.between(config.min_quantity, ceiling);
        ExchangeCommand command{new_order(ids.take_command_sequence(), account_of(rng.next()),
                                           ids.take_client_order_id(), instrument_id, side, *price, quantity,
                                           fill_or_kill ? TimeInForce::FOK : TimeInForce::IOC)};
        workload.operations.push_back(command);
        workload.kinds.push_back(fill_or_kill ? OpKind::Fok : OpKind::Ioc);
        apply(command);
        if (fill_or_kill) {
            ++workload.counts.fok;
        } else {
            ++workload.counts.ioc;
        }
    }

    workload.resting_orders_at_end = live.size();
    for (InstrumentId instrument_id = 1; instrument_id <= config.instrument_count; ++instrument_id) {
        workload.bid_levels_at_end += depth.level_count(instrument_id, Side::Buy);
        workload.ask_levels_at_end += depth.level_count(instrument_id, Side::Sell);
    }
    return workload;
}

// Replays a pre-generated command vector into `engine`. Deliberately trivial
// and non-inlineable-by-accident: measured regions call this so that what is
// timed is process() plus a vector walk, nothing else.
inline void replay(MatchingEngine& engine, const std::vector<ExchangeCommand>& commands, const EventSink& sink) {
    for (const auto& command : commands) {
        engine.process(command, sink);
    }
}

} // namespace mdh::exchange::testing
