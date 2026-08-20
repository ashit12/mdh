#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/types.hpp"
#include "exchange/core/commands.hpp"
#include "exchange/core/event_sink.hpp"
#include "exchange/matching/matching_engine.hpp"

// The small amount of scaffolding every matching-engine benchmark and the
// stress harness both need -- command builders, a shared
// discard sink, and book-seeding helpers -- in one place instead of copied
// into each benchmark file.
//
// Header-only and test/benchmark-only. Nothing under src/ or apps/ includes
// this, and it changes no production behaviour: everything here drives
// MatchingEngine through its ordinary public process() entry point, exactly
// as the gateway does.
namespace mdh::exchange::testing {

// One shared, already-constructed EventSink that discards everything.
//
// Constructed once and handed out by reference on purpose. EventSink is
// std::function, so passing a lambda or a function pointer directly to
// process() constructs a fresh std::function at every call site. That
// construction is cheap (a captureless lambda fits the small-object buffer,
// so it does not allocate) but it is not free, and it is *harness* cost, not
// matching-engine cost -- charging it to the engine would inflate every
// number in the baseline. The indirect call *through* the
// std::function is left in place, because that one is genuinely part of how
// the engine emits events today.
[[nodiscard]] inline const EventSink& discard_events() {
    static const EventSink sink = [](const ExchangeEvent&) {};
    return sink;
}

// MatchingEngine rejects commands on instruments it was not told about, so
// every test and benchmark has to state its universe. Single-instrument
// callers pass their own id directly (`MatchingEngine engine{kInstrument}`);
// this is for the ones that spread flow across `count` books numbered
// 1..count.
[[nodiscard]] inline std::vector<InstrumentId> instrument_universe(std::size_t count, InstrumentId first = 1) {
    std::vector<InstrumentId> ids(count);
    for (std::size_t i = 0; i < count; ++i) {
        ids[i] = first + static_cast<InstrumentId>(i);
    }
    return ids;
}

// Monotonic id sources threaded through a scenario so that command
// sequences and client order ids stay unique and gapless across a setup
// phase and the measured phase that follows it.
struct SequentialIds {
    CommandSequence next_command_sequence = 1;
    ClientOrderId next_client_order_id = 1;

    [[nodiscard]] CommandSequence take_command_sequence() { return next_command_sequence++; }
    [[nodiscard]] ClientOrderId take_client_order_id() { return next_client_order_id++; }
};

[[nodiscard]] inline NewOrderCommand new_order(CommandSequence command_sequence, AccountId account_id,
                                                ClientOrderId client_order_id, InstrumentId instrument_id, Side side,
                                                Price price, Quantity quantity,
                                                TimeInForce time_in_force = TimeInForce::GTC) {
    return NewOrderCommand{
        .command_sequence = command_sequence,
        .account_id = account_id,
        .client_order_id = client_order_id,
        .instrument_id = instrument_id,
        .side = side,
        .price = price,
        .quantity = quantity,
        .order_type = OrderType::Limit,
        .time_in_force = time_in_force,
    };
}

[[nodiscard]] inline CancelOrderCommand cancel_order(CommandSequence command_sequence, AccountId account_id,
                                                      ClientOrderId client_order_id, InstrumentId instrument_id) {
    return CancelOrderCommand{
        .command_sequence = command_sequence,
        .account_id = account_id,
        .client_order_id = client_order_id,
        .instrument_id = instrument_id,
    };
}

[[nodiscard]] inline ReplaceOrderCommand replace_order(CommandSequence command_sequence, AccountId account_id,
                                                        ClientOrderId original_client_order_id,
                                                        ClientOrderId new_client_order_id, InstrumentId instrument_id,
                                                        Price new_price, Quantity new_quantity) {
    return ReplaceOrderCommand{
        .command_sequence = command_sequence,
        .account_id = account_id,
        .original_client_order_id = original_client_order_id,
        .new_client_order_id = new_client_order_id,
        .instrument_id = instrument_id,
        .new_price = new_price,
        .new_quantity = new_quantity,
    };
}

// Rests `orders_per_level` GTC orders on each of `levels` distinct prices,
// walking away from the touch (`price_step` should be negative for bids,
// positive for asks, so that seeded liquidity never crosses itself).
// Appends every client order id it created to `out_client_order_ids` when
// that pointer is non-null, so a caller can later cancel or replace exactly
// the orders it seeded.
inline void seed_resting_orders(MatchingEngine& engine, SequentialIds& ids, AccountId account_id,
                                 InstrumentId instrument_id, Side side, Price first_price, Price price_step,
                                 std::size_t levels, std::size_t orders_per_level, Quantity quantity,
                                 std::vector<ClientOrderId>* out_client_order_ids = nullptr) {
    const EventSink& sink = discard_events();
    for (std::size_t level = 0; level < levels; ++level) {
        const Price price = first_price + price_step * static_cast<Price>(level);
        for (std::size_t n = 0; n < orders_per_level; ++n) {
            const ClientOrderId client_order_id = ids.take_client_order_id();
            engine.process(ExchangeCommand{new_order(ids.take_command_sequence(), account_id, client_order_id,
                                                      instrument_id, side, price, quantity)},
                            sink);
            if (out_client_order_ids != nullptr) {
                out_client_order_ids->push_back(client_order_id);
            }
        }
    }
}

} // namespace mdh::exchange::testing
