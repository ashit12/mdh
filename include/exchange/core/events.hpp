#pragma once

#include <variant>

#include "common/types.hpp"
#include "exchange/core/types.hpp"

// The exchange's outbound vocabulary: what the matching engine emits through
// an EventSink, with no knowledge of any transport. Each event carries
// enough for every consumer -- execution reports, the ledger, market data,
// the journal, tests -- to act on its own without reaching back into engine
// internals.
//
// One command usually produces several. A crossing new order emits an
// OrderAccepted, a TradeExecuted per fill, and a BookOrderReduced or
// BookOrderRemoved for each resting order it consumed. Consumers need that
// discrete sequence, not a count of how many of each kind occurred.
//
// ── Public and private events ─────────────────────────────────────────────
// BookOrderAdded, BookOrderReduced and BookOrderRemoved deliberately carry
// no account_id or client_order_id. They describe visible book depth the way
// any market observer sees it -- an exchange order id, never whose order it
// is. OrderAccepted, OrderRejected, OrderCancelled, OrderReplaced and
// TradeExecuted are private and addressed to an account.
//
// Keeping that split in the type system is what lets the market-data
// publisher forward the public events, and only those, without a filtering
// step that could leak private data by omission.
//
// Every struct defines a defaulted operator== because replaying the same
// journal twice must produce identical event streams, and the most direct
// way to assert that is to compare two vectors of them.
namespace mdh::exchange {

struct OrderAccepted {
    EventSequence event_sequence;
    CommandSequence command_sequence;
    AccountId account_id;
    ClientOrderId client_order_id;
    ExchangeOrderId exchange_order_id;
    InstrumentId instrument_id;
    Side side;
    Price price;
    Quantity quantity;
    OrderType order_type;
    TimeInForce time_in_force;

    bool operator==(const OrderAccepted&) const = default;
};

struct OrderRejected {
    EventSequence event_sequence;
    CommandSequence command_sequence;
    AccountId account_id;
    ClientOrderId client_order_id;
    InstrumentId instrument_id;
    RejectReason reason;

    bool operator==(const OrderRejected&) const = default;
};

struct OrderCancelled {
    EventSequence event_sequence;
    CommandSequence command_sequence;
    AccountId account_id;
    ClientOrderId client_order_id;
    ExchangeOrderId exchange_order_id;
    InstrumentId instrument_id;

    bool operator==(const OrderCancelled&) const = default;
};

struct OrderReplaced {
    EventSequence event_sequence;
    CommandSequence command_sequence;
    AccountId account_id;
    ClientOrderId original_client_order_id;
    ClientOrderId new_client_order_id;
    ExchangeOrderId exchange_order_id;
    InstrumentId instrument_id;
    Price new_price;
    Quantity new_quantity;

    bool operator==(const OrderReplaced&) const = default;
};

// One side of a trade. TradeExecuted carries one per side, so a single event
// describes both legs and no consumer has to correlate two events to build
// one execution report.
struct TradeCounterparty {
    AccountId account_id;
    ClientOrderId client_order_id;
    ExchangeOrderId exchange_order_id;
    // What is left on this order after the trade, zero if it is now fully
    // filled, so a consumer can tell a partial fill from a complete one
    // without re-deriving it from earlier state.
    Quantity remaining_quantity;

    bool operator==(const TradeCounterparty&) const = default;
};

struct TradeExecuted {
    EventSequence event_sequence;
    CommandSequence command_sequence;
    InstrumentId instrument_id;
    Price price;
    Quantity quantity;
    Side aggressor_side;
    TradeCounterparty buyer;
    TradeCounterparty seller;

    bool operator==(const TradeExecuted&) const = default;
};

struct BookOrderAdded {
    EventSequence event_sequence;
    InstrumentId instrument_id;
    ExchangeOrderId exchange_order_id;
    Side side;
    Price price;
    Quantity quantity;

    bool operator==(const BookOrderAdded&) const = default;
};

struct BookOrderReduced {
    EventSequence event_sequence;
    InstrumentId instrument_id;
    ExchangeOrderId exchange_order_id;
    Side side;
    Price price;
    Quantity new_remaining_quantity;

    bool operator==(const BookOrderReduced&) const = default;
};

struct BookOrderRemoved {
    EventSequence event_sequence;
    InstrumentId instrument_id;
    ExchangeOrderId exchange_order_id;
    Side side;
    Price price;

    bool operator==(const BookOrderRemoved&) const = default;
};

using ExchangeEvent = std::variant<OrderAccepted, OrderRejected, OrderCancelled, OrderReplaced, TradeExecuted,
                                    BookOrderAdded, BookOrderReduced, BookOrderRemoved>;

} // namespace mdh::exchange
