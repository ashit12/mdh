// Milestone 15: a long-running deterministic stress/correctness harness for
// the matching engine.
//
// Deliberately not a Google Benchmark case and not an ordinary unit test
// either. tests/test_matching_engine.cpp asserts specific outcomes for
// specific hand-written scenarios; this harness instead runs a large,
// randomly-mixed-but-reproducible command stream and continuously checks the
// properties that must hold no matter what the stream contains. It is what
// gives the Milestone 15 baseline numbers their credibility: measuring an
// engine that quietly corrupts its own indexes under load would be measuring
// nothing worth knowing.
//
// ── What is checked ────────────────────────────────────────────────────────
// On every command:
//   - event_sequence is gapless and strictly increasing across the whole run
//   - exchange_order_id is never reissued
//   - a trade never executes against an order that is not resting right now
//     (which is what "a cancelled order can never later execute" reduces to)
//   - a trade never takes more than the resting order has left, and the
//     remaining quantity it reports agrees with the harness's running total
//   - a trade prices at the resting order's price, never the aggressor's
//   - a resting order's remaining quantity is never zero and never exceeds
//     its original quantity
//   - FOK either rejects with no other event at all, or fills completely --
//     never partially
//   - IOC never produces a BookOrderAdded
//   - a GTC remainder rests at the right price, side and quantity
//   - a rejected command emits exactly one event and mutates nothing
//   - a successful cancel emits exactly OrderCancelled + BookOrderRemoved
//
// Periodically, against MatchingEngine::snapshot():
//   - the harness's shadow index and the engine's book agree exactly on
//     which orders rest, at what price, on what side, for how much, under
//     which client order id
//   - price levels are ordered best-to-worst on each side
//   - price-time FIFO holds: order_sequence strictly increases within a level
//
// At the end of a run:
//   - every order the harness believes is live can actually be cancelled and
//     the book is exactly empty afterwards. A cancel only succeeds when the
//     engine's private live-order index and its per-instrument book index
//     still agree, so draining the book this way is the strongest available
//     statement that the two never drifted apart.
//
// Empty-price-level removal cannot be observed through snapshot() (a level
// with no orders contributes nothing either way), so MatchingBook is stressed
// directly against a reference model at the bottom of this file, where
// best_bid_price()/best_ask_price() do expose a level that failed to be
// erased.
//
// ── Size of the run ────────────────────────────────────────────────────────
// Millions of commands in an optimised build, fewer in a Debug build where
// the same run would take tens of minutes and nobody would wait for it.
// MDH_STRESS_OPS overrides both. docs/matching_engine_baseline.md records the
// size actually used for the recorded soak.
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <optional>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "exchange/matching/matching_book.hpp"
#include "exchange/matching/matching_engine.hpp"
#include "exchange/testing/matching_scenarios.hpp"
#include "exchange/testing/matching_workload.hpp"

// `testing` is ambiguous inside mdh::exchange (gtest owns ::testing, this
// project owns mdh::exchange::testing), so the project's one gets an alias.
namespace mt = mdh::exchange::testing;

namespace mdh::exchange {
namespace {

[[nodiscard]] std::size_t stress_operations(std::size_t optimised_default, std::size_t debug_default) {
    if (const char* override_value = std::getenv("MDH_STRESS_OPS"); override_value != nullptr) {
        const auto parsed = std::strtoull(override_value, nullptr, 10);
        if (parsed > 0) {
            return static_cast<std::size_t>(parsed);
        }
    }
#if defined(NDEBUG)
    (void)debug_default;
    return optimised_default;
#else
    (void)optimised_default;
    return debug_default;
#endif
}

// ── A stable digest of an event stream ─────────────────────────────────────
//
// Replay determinism is asserted by comparing digests rather than by keeping
// two full event vectors: a multi-million-command run emits tens of millions
// of events and holding two copies of that would dominate the test's memory
// use. The digest folds each event's *semantic fields*, never its raw bytes,
// so struct padding cannot make two identical runs disagree.
class EventDigest {
public:
    void add(const ExchangeEvent& event) {
        std::visit(
            [this](const auto& ev) {
                using T = std::decay_t<decltype(ev)>;
                if constexpr (std::is_same_v<T, OrderAccepted>) {
                    mix({1, ev.event_sequence, ev.command_sequence, ev.account_id, ev.client_order_id,
                         ev.exchange_order_id, ev.instrument_id, static_cast<std::uint64_t>(ev.side),
                         static_cast<std::uint64_t>(ev.price), ev.quantity,
                         static_cast<std::uint64_t>(ev.order_type), static_cast<std::uint64_t>(ev.time_in_force)});
                } else if constexpr (std::is_same_v<T, OrderRejected>) {
                    mix({2, ev.event_sequence, ev.command_sequence, ev.account_id, ev.client_order_id,
                         ev.instrument_id, static_cast<std::uint64_t>(ev.reason)});
                } else if constexpr (std::is_same_v<T, OrderCancelled>) {
                    mix({3, ev.event_sequence, ev.command_sequence, ev.account_id, ev.client_order_id,
                         ev.exchange_order_id, ev.instrument_id});
                } else if constexpr (std::is_same_v<T, OrderReplaced>) {
                    mix({4, ev.event_sequence, ev.command_sequence, ev.account_id, ev.original_client_order_id,
                         ev.new_client_order_id, ev.exchange_order_id, ev.instrument_id,
                         static_cast<std::uint64_t>(ev.new_price), ev.new_quantity});
                } else if constexpr (std::is_same_v<T, TradeExecuted>) {
                    mix({5, ev.event_sequence, ev.command_sequence, ev.instrument_id,
                         static_cast<std::uint64_t>(ev.price), ev.quantity,
                         static_cast<std::uint64_t>(ev.aggressor_side), ev.buyer.account_id, ev.buyer.client_order_id,
                         ev.buyer.exchange_order_id, ev.buyer.remaining_quantity, ev.seller.account_id,
                         ev.seller.client_order_id, ev.seller.exchange_order_id, ev.seller.remaining_quantity});
                } else if constexpr (std::is_same_v<T, BookOrderAdded>) {
                    mix({6, ev.event_sequence, ev.instrument_id, ev.exchange_order_id,
                         static_cast<std::uint64_t>(ev.side), static_cast<std::uint64_t>(ev.price), ev.quantity});
                } else if constexpr (std::is_same_v<T, BookOrderReduced>) {
                    mix({7, ev.event_sequence, ev.instrument_id, ev.exchange_order_id,
                         static_cast<std::uint64_t>(ev.side), static_cast<std::uint64_t>(ev.price),
                         ev.new_remaining_quantity});
                } else if constexpr (std::is_same_v<T, BookOrderRemoved>) {
                    mix({8, ev.event_sequence, ev.instrument_id, ev.exchange_order_id,
                         static_cast<std::uint64_t>(ev.side), static_cast<std::uint64_t>(ev.price)});
                }
            },
            event);
    }

    [[nodiscard]] std::uint64_t value() const { return hash_; }

private:
    void mix(std::initializer_list<std::uint64_t> fields) {
        for (const std::uint64_t field : fields) {
            hash_ ^= field + 0x9E3779B97F4A7C15ULL + (hash_ << 6) + (hash_ >> 2);
        }
    }

    std::uint64_t hash_ = 0xCBF29CE484222325ULL;
};

// ── The invariant checker ──────────────────────────────────────────────────

class InvariantChecker {
public:
    struct ShadowOrder {
        ClientOrderId client_order_id;
        AccountId account_id;
        InstrumentId instrument_id;
        Side side;
        Price price;
        Quantity remaining_quantity;
    };

    void on_command(const ExchangeCommand& command, const std::vector<ExchangeEvent>& events) {
        ++commands_seen_;
        for (const auto& event : events) {
            digest_.add(event);
        }

        Outcome outcome;
        for (const auto& event : events) {
            ASSERT_EQ(sequence_of(event), next_event_sequence_)
                << "event_sequence must be gapless and strictly increasing across the entire run";
            ++next_event_sequence_;
            absorb(command, event, outcome);
        }

        std::visit([&](const auto& cmd) { check_command(cmd, events, outcome); }, command);
    }

    // Full cross-check of the harness's model against the engine's own
    // authoritative state, plus the ordering invariants only a canonical
    // dump can expose.
    void verify_snapshot(const EngineStateSnapshot& snapshot) const {
        std::size_t counted = 0;
        for (const auto& instrument : snapshot.instruments) {
            counted += check_side(instrument.instrument_id, instrument.bids, Side::Buy);
            counted += check_side(instrument.instrument_id, instrument.asks, Side::Sell);
        }
        EXPECT_EQ(counted, live_.size())
            << "the engine's book and the harness's shadow index disagree on how many orders rest";
    }

    [[nodiscard]] const std::unordered_map<ExchangeOrderId, ShadowOrder>& live() const { return live_; }
    [[nodiscard]] std::uint64_t digest() const { return digest_.value(); }
    [[nodiscard]] std::size_t commands_seen() const { return commands_seen_; }
    [[nodiscard]] std::size_t trades_seen() const { return trades_seen_; }
    [[nodiscard]] Quantity traded_quantity() const { return traded_quantity_; }

private:
    // What one command's event burst amounted to, gathered while walking it
    // so the per-command rules can be checked once at the end.
    struct Outcome {
        bool accepted = false;
        bool rejected = false;
        bool cancelled = false;
        bool replaced = false;
        bool added = false;
        bool removed = false;
        bool reduced_self = false;
        ExchangeOrderId aggressor_id = 0;
        ExchangeOrderId accepted_id = 0;
        Price added_price = 0;
        Side added_side = Side::Buy;
        Quantity added_quantity = 0;
        Price replaced_price = 0;
        Quantity replaced_quantity = 0;
        Quantity aggressor_filled = 0;
    };

    static EventSequence sequence_of(const ExchangeEvent& event) {
        return std::visit([](const auto& ev) { return ev.event_sequence; }, event);
    }

    static ClientOrderId owner_client_order_id(const ExchangeCommand& command) {
        return std::visit(
            [](const auto& cmd) -> ClientOrderId {
                using T = std::decay_t<decltype(cmd)>;
                if constexpr (std::is_same_v<T, ReplaceOrderCommand>) {
                    return cmd.new_client_order_id;
                } else {
                    return cmd.client_order_id;
                }
            },
            command);
    }

    void absorb(const ExchangeCommand& command, const ExchangeEvent& event, Outcome& outcome) {
        std::visit(
            [&](const auto& ev) {
                using T = std::decay_t<decltype(ev)>;
                if constexpr (std::is_same_v<T, OrderAccepted>) {
                    outcome.accepted = true;
                    outcome.accepted_id = ev.exchange_order_id;
                    outcome.aggressor_id = ev.exchange_order_id;
                    EXPECT_GT(ev.exchange_order_id, highest_exchange_order_id_)
                        << "exchange_order_id must never be reissued";
                    highest_exchange_order_id_ = ev.exchange_order_id;
                } else if constexpr (std::is_same_v<T, OrderRejected>) {
                    outcome.rejected = true;
                } else if constexpr (std::is_same_v<T, OrderCancelled>) {
                    outcome.cancelled = true;
                } else if constexpr (std::is_same_v<T, OrderReplaced>) {
                    outcome.replaced = true;
                    outcome.aggressor_id = ev.exchange_order_id;
                    outcome.replaced_price = ev.new_price;
                    outcome.replaced_quantity = ev.new_quantity;
                    if (auto it = live_.find(ev.exchange_order_id); it != live_.end()) {
                        // Priority preserved: same order, same book slot, new
                        // client order id.
                        it->second.client_order_id = ev.new_client_order_id;
                    } else {
                        EXPECT_GT(ev.exchange_order_id, highest_exchange_order_id_)
                            << "a replace that loses priority must issue an unused exchange_order_id";
                        highest_exchange_order_id_ = ev.exchange_order_id;
                    }
                } else if constexpr (std::is_same_v<T, TradeExecuted>) {
                    check_trade(ev, outcome);
                } else if constexpr (std::is_same_v<T, BookOrderAdded>) {
                    outcome.added = true;
                    outcome.added_price = ev.price;
                    outcome.added_side = ev.side;
                    outcome.added_quantity = ev.quantity;
                    EXPECT_GT(ev.quantity, Quantity{0}) << "an order must never rest with zero quantity";
                    EXPECT_FALSE(live_.contains(ev.exchange_order_id))
                        << "two live orders must never share an exchange_order_id";
                    live_[ev.exchange_order_id] =
                        ShadowOrder{owner_client_order_id(command),
                                    std::visit([](const auto& cmd) { return cmd.account_id; }, command),
                                    ev.instrument_id, ev.side, ev.price, ev.quantity};
                } else if constexpr (std::is_same_v<T, BookOrderReduced>) {
                    auto it = live_.find(ev.exchange_order_id);
                    if (it == live_.end()) {
                        ADD_FAILURE() << "BookOrderReduced for an order that is not resting";
                        return;
                    }
                    EXPECT_GT(ev.new_remaining_quantity, Quantity{0})
                        << "a reduction to zero must be a removal, not a reduction";
                    EXPECT_LE(ev.new_remaining_quantity, it->second.remaining_quantity)
                        << "BookOrderReduced must never increase remaining quantity";
                    it->second.remaining_quantity = ev.new_remaining_quantity;
                    if (ev.exchange_order_id == outcome.aggressor_id) {
                        outcome.reduced_self = true;
                    }
                } else if constexpr (std::is_same_v<T, BookOrderRemoved>) {
                    outcome.removed = true;
                    if (live_.erase(ev.exchange_order_id) == 0) {
                        ADD_FAILURE() << "BookOrderRemoved for an order that is not resting";
                    }
                }
            },
            event);
    }

    void check_trade(const TradeExecuted& trade, Outcome& outcome) {
        ++trades_seen_;
        traded_quantity_ += trade.quantity;
        EXPECT_GT(trade.quantity, Quantity{0}) << "a trade must move a non-zero quantity";
        EXPECT_GT(trade.price, Price{0}) << "a trade must have a positive price";

        const TradeCounterparty& aggressor = trade.aggressor_side == Side::Buy ? trade.buyer : trade.seller;
        const TradeCounterparty& resting = trade.aggressor_side == Side::Buy ? trade.seller : trade.buyer;
        EXPECT_EQ(aggressor.exchange_order_id, outcome.aggressor_id)
            << "the aggressor side of a trade must be the order this command created";
        outcome.aggressor_filled += trade.quantity;

        // The passive side has to be an order that is resting *right now*. A
        // cancelled or already-filled order failing this lookup is exactly
        // the "cancelled orders cannot later execute" invariant.
        auto it = live_.find(resting.exchange_order_id);
        if (it == live_.end()) {
            ADD_FAILURE() << "a trade executed against an order that is not on the book";
            return;
        }
        EXPECT_EQ(trade.price, it->second.price)
            << "a trade must execute at the resting order's price, never the aggressor's";
        if (it->second.remaining_quantity < trade.quantity) {
            ADD_FAILURE() << "a trade took more quantity than the resting order had left";
            return;
        }
        const Quantity after = it->second.remaining_quantity - trade.quantity;
        EXPECT_EQ(resting.remaining_quantity, after)
            << "the resting counterparty's reported remaining quantity disagrees with the running total";
        it->second.remaining_quantity = after;
        // A following BookOrderReduced re-asserts this number, or a
        // BookOrderRemoved retires the order when `after` is zero -- the
        // per-command checks below require one of the two.
    }

    void check_command(const NewOrderCommand& cmd, const std::vector<ExchangeEvent>& events, const Outcome& outcome) {
        if (outcome.rejected) {
            EXPECT_EQ(events.size(), 1U) << "a rejected new order must not mutate the book";
            EXPECT_FALSE(outcome.accepted);
            return;
        }
        EXPECT_TRUE(outcome.accepted) << "a new order must be either accepted or rejected";
        EXPECT_LE(outcome.aggressor_filled, cmd.quantity) << "an order cannot fill for more than it asked for";

        switch (cmd.time_in_force) {
            case TimeInForce::FOK:
                EXPECT_EQ(outcome.aggressor_filled, cmd.quantity) << "FOK must fill completely or not execute at all";
                EXPECT_FALSE(outcome.added) << "FOK must never rest";
                break;
            case TimeInForce::IOC:
                EXPECT_FALSE(outcome.added) << "IOC must never rest";
                break;
            case TimeInForce::GTC:
                if (outcome.aggressor_filled < cmd.quantity) {
                    EXPECT_TRUE(outcome.added) << "a GTC remainder must rest on the book";
                    EXPECT_EQ(outcome.added_quantity, cmd.quantity - outcome.aggressor_filled);
                    EXPECT_EQ(outcome.added_price, cmd.price);
                    EXPECT_EQ(outcome.added_side, cmd.side);
                } else {
                    EXPECT_FALSE(outcome.added) << "a fully filled GTC order has no remainder to rest";
                }
                break;
        }
    }

    void check_command(const CancelOrderCommand&, const std::vector<ExchangeEvent>& events, const Outcome& outcome) {
        if (outcome.rejected) {
            EXPECT_EQ(events.size(), 1U) << "a rejected cancel must not mutate the book";
            return;
        }
        EXPECT_TRUE(outcome.cancelled) << "a cancel must be either accepted or rejected";
        EXPECT_EQ(events.size(), 2U) << "a successful cancel emits exactly OrderCancelled + BookOrderRemoved";
        EXPECT_TRUE(outcome.removed);
    }

    void check_command(const ReplaceOrderCommand& cmd, const std::vector<ExchangeEvent>& events,
                       const Outcome& outcome) {
        if (outcome.rejected) {
            EXPECT_EQ(events.size(), 1U) << "a rejected replace must not mutate the book";
            return;
        }
        EXPECT_TRUE(outcome.replaced) << "a replace must be either accepted or rejected";
        EXPECT_EQ(outcome.replaced_price, cmd.new_price);
        EXPECT_EQ(outcome.replaced_quantity, cmd.new_quantity);
        if (outcome.reduced_self) {
            EXPECT_EQ(events.size(), 2U) << "an in-place replace emits exactly OrderReplaced + BookOrderReduced";
            EXPECT_FALSE(outcome.added);
        } else {
            EXPECT_TRUE(outcome.removed) << "a replace that loses priority must remove the original order";
        }
    }

    // Returns how many orders this side held, asserting price-priority
    // ordering across levels and FIFO ordering within them.
    std::size_t check_side(InstrumentId instrument_id, const std::vector<ExchangeRestingOrder>& orders,
                            Side side) const {
        std::optional<Price> previous_price;
        std::uint64_t previous_sequence = 0;
        for (const auto& order : orders) {
            EXPECT_EQ(order.side, side);
            EXPECT_EQ(order.instrument_id, instrument_id);
            EXPECT_GT(order.remaining_quantity, Quantity{0}) << "a resting order must have quantity left";
            EXPECT_LE(order.remaining_quantity, order.original_quantity)
                << "remaining quantity must never exceed the original";

            if (previous_price.has_value() && order.price != *previous_price) {
                if (side == Side::Buy) {
                    EXPECT_LT(order.price, *previous_price) << "bids must be ordered highest price first";
                } else {
                    EXPECT_GT(order.price, *previous_price) << "asks must be ordered lowest price first";
                }
                previous_sequence = 0;
            }
            if (previous_sequence != 0) {
                EXPECT_GT(order.order_sequence, previous_sequence)
                    << "price-time FIFO broken at price " << order.price;
            }
            previous_price = order.price;
            previous_sequence = order.order_sequence;

            const auto it = live_.find(order.exchange_order_id);
            if (it == live_.end()) {
                ADD_FAILURE() << "the engine holds a resting order the harness never saw rest";
                continue;
            }
            EXPECT_EQ(it->second.price, order.price);
            EXPECT_EQ(it->second.side, order.side);
            EXPECT_EQ(it->second.remaining_quantity, order.remaining_quantity);
            EXPECT_EQ(it->second.client_order_id, order.client_order_id);
            EXPECT_EQ(it->second.account_id, order.account_id);
        }
        return orders.size();
    }

    std::unordered_map<ExchangeOrderId, ShadowOrder> live_;
    EventDigest digest_;
    EventSequence next_event_sequence_ = 1;
    ExchangeOrderId highest_exchange_order_id_ = 0;
    std::size_t commands_seen_ = 0;
    std::size_t trades_seen_ = 0;
    Quantity traded_quantity_ = 0;
};

// ── The soak ───────────────────────────────────────────────────────────────

TEST(MatchingEngineStress, MixedCommandStreamPreservesEveryInvariant) {
    mt::WorkloadConfig config;
    config.seed = 0xA5A55A5A1234ABCDULL;
    config.operation_count = stress_operations(/*optimised=*/3'000'000, /*debug=*/200'000);
    config.instrument_count = 4;
    config.initial_orders_per_side = 500;
    config.account_count = 16;
    // Aim a meaningful share of cancels and replaces at ids the engine has
    // already retired: an index that drifted out of sync would show up here
    // as a spurious success rather than the expected rejection.
    config.stale_reference_pct = 20;
    const auto workload = mt::generate_workload(config);

    MatchingEngine engine(config.instruments());
    InvariantChecker checker;
    std::vector<ExchangeEvent> events;
    const EventSink sink = [&events](const ExchangeEvent& event) { events.push_back(event); };

    const auto run = [&](const std::vector<ExchangeCommand>& commands, std::size_t snapshot_every) {
        std::size_t index = 0;
        for (const auto& command : commands) {
            events.clear();
            engine.process(command, sink);
            checker.on_command(command, events);
            if (::testing::Test::HasFailure()) {
                FAIL() << "invariant violated at command " << index << " of " << commands.size();
            }
            if (++index % snapshot_every == 0) {
                checker.verify_snapshot(engine.snapshot());
                if (::testing::Test::HasFailure()) {
                    FAIL() << "snapshot cross-check failed after command " << index;
                }
            }
        }
    };

    run(workload.seed, workload.seed.size() + 1);
    ASSERT_FALSE(::testing::Test::HasFailure());
    run(workload.operations, std::max<std::size_t>(1, config.operation_count / 20));
    ASSERT_FALSE(::testing::Test::HasFailure());
    checker.verify_snapshot(engine.snapshot());
    ASSERT_FALSE(::testing::Test::HasFailure());

    struct LiveRef {
        AccountId account_id;
        ClientOrderId client_order_id;
        InstrumentId instrument_id;
    };
    std::vector<LiveRef> to_drain;
    to_drain.reserve(checker.live().size());
    for (const auto& [exchange_order_id, order] : checker.live()) {
        to_drain.push_back(LiveRef{order.account_id, order.client_order_id, order.instrument_id});
    }
    ASSERT_FALSE(to_drain.empty()) << "the run left nothing resting -- the drain check would assert nothing";

    CommandSequence sequence = 1'000'000'000;
    for (const auto& reference : to_drain) {
        events.clear();
        engine.process(ExchangeCommand{mt::cancel_order(sequence++, reference.account_id, reference.client_order_id,
                                                         reference.instrument_id)},
                        sink);
        ASSERT_EQ(events.size(), 2U) << "a live order refused to cancel: account " << reference.account_id
                                      << ", client order " << reference.client_order_id;
        ASSERT_TRUE(std::holds_alternative<OrderCancelled>(events[0]));
    }

    EXPECT_TRUE(engine.snapshot().instruments.empty())
        << "the book still holds orders after cancelling everything the harness knew about";

    std::printf("[ stress    ] %zu commands, %zu trades, %llu units traded, %zu orders drained at the end\n",
                checker.commands_seen(), checker.trades_seen(),
                static_cast<unsigned long long>(checker.traded_quantity()), to_drain.size());
}

TEST(MatchingEngineStress, ReplayingTheSameCommandsProducesIdenticalEventsAndState) {
    mt::WorkloadConfig config;
    config.seed = 0x13579BDF2468ACE0ULL;
    config.operation_count = stress_operations(/*optimised=*/500'000, /*debug=*/50'000);
    config.instrument_count = 3;
    config.initial_orders_per_side = 300;
    config.stale_reference_pct = 10;
    const auto workload = mt::generate_workload(config);

    const auto play = [&workload]() {
        MatchingEngine engine(workload.config.instruments());
        EventDigest digest;
        const EventSink sink = [&digest](const ExchangeEvent& event) { digest.add(event); };
        mt::replay(engine, workload.seed, sink);
        mt::replay(engine, workload.operations, sink);
        return std::make_pair(digest.value(), engine.snapshot());
    };

    const auto first = play();
    const auto second = play();

    EXPECT_EQ(first.first, second.first) << "the same command sequence produced a different event stream";
    EXPECT_TRUE(first.second == second.second) << "the same command sequence ended in a different book state";
    EXPECT_FALSE(first.second.instruments.empty()) << "the replay left nothing resting -- this asserts nothing";
}

// ── MatchingBook against an independent reference model ────────────────────

// Empty price levels are invisible through MatchingEngine::snapshot() but
// very visible through MatchingBook's own best_bid_price()/best_ask_price():
// a level that was not erased when its last order left would keep being
// reported as the touch. The reference model is built from plain vectors, so
// a bug in how MatchingBook drives std::list/std::map cannot be mirrored by
// the thing it is compared against.
// Differential test of MatchingBook against a std::map reference model.
// `spread` is how many ticks the random prices span, which is what decides
// whether they land in the ladder or in the out-of-band map: a spread inside
// `band_ticks` never leaves the ladder, while a wider one keeps both indexes
// non-empty and makes every best-price and every walk a merge of the two.
void check_price_level_lifecycle(Price spread, std::uint32_t band_ticks) {
    const std::size_t operations = stress_operations(/*optimised=*/400'000, /*debug=*/40'000);

    MatchingBook book(/*expected_resting_orders=*/0, band_ticks);
    std::map<Price, std::vector<BookOrder>, std::greater<Price>> reference_bids;
    std::map<Price, std::vector<BookOrder>> reference_asks;

    // O(1) uniform pick and erase over the live set. Keeping a handle
    // alongside each live id mirrors what the engine now does for real: the
    // book cannot find an order by id, so whoever put it there holds on to
    // where it went.
    std::vector<ExchangeOrderId> live_ids;
    struct Location {
        Side side;
        Price price;
        std::size_t slot;
        MatchingBook::Handle handle;
    };
    std::unordered_map<ExchangeOrderId, Location> located;

    const auto forget = [&](ExchangeOrderId id) {
        const std::size_t slot = located.at(id).slot;
        const std::size_t last = live_ids.size() - 1;
        if (slot != last) {
            live_ids[slot] = live_ids[last];
            located.at(live_ids[slot]).slot = slot;
        }
        live_ids.pop_back();
        located.erase(id);
    };
    const auto erase_from = [](auto& levels, Price price, ExchangeOrderId victim) {
        auto& level = levels.at(price);
        level.erase(std::find_if(level.begin(), level.end(), [victim](const BookOrder& candidate) {
            return candidate.exchange_order_id == victim;
        }));
        if (level.empty()) {
            levels.erase(price);
        }
    };
    const auto pop_front_of_best = [](auto& levels) {
        auto level_it = levels.begin();
        const ExchangeOrderId victim = level_it->second.front().exchange_order_id;
        level_it->second.erase(level_it->second.begin());
        if (level_it->second.empty()) {
            levels.erase(level_it);
        }
        return victim;
    };
    const auto reduce_in = [](auto& levels, Price price, ExchangeOrderId victim, Quantity quantity) {
        for (auto& order : levels.at(price)) {
            if (order.exchange_order_id == victim) {
                order.remaining_quantity = quantity;
            }
        }
    };
    const auto flatten = [](const auto& levels) {
        std::vector<BookOrder> flat;
        for (const auto& [price, level] : levels) {
            flat.insert(flat.end(), level.begin(), level.end());
        }
        return flat;
    };

    mt::SplitMix64 rng(0xFEEDFACECAFEB00BULL);
    ExchangeOrderId next_id = 1;

    for (std::size_t op = 0; op < operations; ++op) {
        const auto choice = live_ids.empty() ? 0U : static_cast<unsigned>(rng.below(100));
        if (choice < 45) {
            const Side side = (rng.next() & 1U) != 0 ? Side::Buy : Side::Sell;
            const auto price = static_cast<Price>(1'000 + rng.below(static_cast<std::uint64_t>(spread)));
            const BookOrder order{
                .exchange_order_id = next_id,
                .client_order_id = next_id,
                .account_id = 1,
                .price = price,
                .remaining_quantity = 100,
                .side = side,
                .time_in_force = TimeInForce::GTC,
            };
            const auto handle = book.add(order);
            if (side == Side::Buy) {
                reference_bids[price].push_back(order);
            } else {
                reference_asks[price].push_back(order);
            }
            located[next_id] = Location{side, price, live_ids.size(), handle};
            live_ids.push_back(next_id);
            ++next_id;
        } else if (choice < 75) {
            const ExchangeOrderId victim = live_ids[rng.below(live_ids.size())];
            const Location location = located.at(victim);
            const BookOrder removed = book.remove_at(location.handle);
            EXPECT_EQ(removed.exchange_order_id, victim) << "at operation " << op;
            if (location.side == Side::Buy) {
                erase_from(reference_bids, location.price, victim);
            } else {
                erase_from(reference_asks, location.price, victim);
            }
            forget(victim);
        } else if (choice < 90) {
            const Side side = (rng.next() & 1U) != 0 ? Side::Buy : Side::Sell;
            const bool empty = side == Side::Buy ? reference_bids.empty() : reference_asks.empty();
            if (empty) {
                continue;
            }
            book.remove_front(side);
            const ExchangeOrderId victim =
                side == Side::Buy ? pop_front_of_best(reference_bids) : pop_front_of_best(reference_asks);
            forget(victim);
        } else {
            const ExchangeOrderId victim = live_ids[rng.below(live_ids.size())];
            const Location location = located.at(victim);
            const auto new_quantity = static_cast<Quantity>(1 + rng.below(99));
            book.reduce_at(location.handle, new_quantity);
            if (location.side == Side::Buy) {
                reduce_in(reference_bids, location.price, victim, new_quantity);
            } else {
                reduce_in(reference_asks, location.price, victim, new_quantity);
            }
        }

        // Cheap every-operation checks: a level that was not erased when it
        // emptied shows up here immediately.
        const auto expected_bid = reference_bids.empty() ? std::optional<Price>{}
                                                          : std::optional<Price>(reference_bids.begin()->first);
        const auto expected_ask = reference_asks.empty() ? std::optional<Price>{}
                                                          : std::optional<Price>(reference_asks.begin()->first);
        ASSERT_EQ(book.best_bid_price(), expected_bid) << "at operation " << op;
        ASSERT_EQ(book.best_ask_price(), expected_ask) << "at operation " << op;

        // Full structural comparison periodically -- O(book) per check, so
        // not on every one of hundreds of thousands of operations.
        if (op % 512 == 0) {
            ASSERT_TRUE(book.all_bids() == flatten(reference_bids)) << "at operation " << op;
            ASSERT_TRUE(book.all_asks() == flatten(reference_asks)) << "at operation " << op;
        }
    }

    EXPECT_TRUE(book.all_bids() == flatten(reference_bids));
    EXPECT_TRUE(book.all_asks() == flatten(reference_asks));
}

TEST(MatchingBookStress, PriceLevelLifecycleMatchesReferenceModel) {
    check_price_level_lifecycle(/*spread=*/64, MatchingBook::kMaxBandTicks);
}

TEST(MatchingBookStress, PriceLevelLifecycleMatchesReferenceModelAcrossTheBandEdge) {
    // Five times the band, so roughly four out of five prices land outside
    // it and the two indexes stay interleaved for the whole run.
    check_price_level_lifecycle(/*spread=*/5 * MatchingBook::kMaxBandTicks, MatchingBook::kMaxBandTicks);
}

TEST(MatchingBookStress, PriceLevelLifecycleMatchesReferenceModelWithNoLadder) {
    // What a universe too large to afford a ladder runs on.
    check_price_level_lifecycle(/*spread=*/1'024, /*band_ticks=*/0);
}

} // namespace
} // namespace mdh::exchange
