#pragma once

#include <variant>

#include "common/types.hpp"
#include "exchange/core/types.hpp"

// Transport-independent exchange events -- the exchange's outbound
// vocabulary, to be emitted by the matching engine (Milestone 2) via an
// EventSink (see event_sink.hpp). Each event carries enough information for
// every downstream consumer -- execution reports, ledger updates,
// market-data publication, journaling, tests -- to act independently,
// without reaching back into matcher-internal state. One command can, and
// often will, produce more than one of these: a single crossing
// NewOrderCommand can emit an OrderAccepted, one or more TradeExecuted, and
// one or more BookOrder{Reduced,Removed} for the resting side(s) it traded
// against. Aggregate counters are not a substitute for this -- a consumer
// needs the discrete sequence, not just how many of each kind occurred.
//
// BookOrderAdded/BookOrderReduced/BookOrderRemoved deliberately do NOT carry
// account_id/client_order_id: they describe changes to visible book depth
// the way a market observer would see them (comparable to an L3 feed, which
// shows an exchange_order_id but never whose order it is), whereas
// OrderAccepted/OrderRejected/OrderCancelled/OrderReplaced/TradeExecuted are
// private, account-addressed events. Keeping that split in the type system
// now is what lets Milestone 6's market-data publisher forward the Book*
// events (and only those) without a filtering step that could otherwise leak
// private data by omission.
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
};

struct OrderRejected {
    EventSequence event_sequence;
    CommandSequence command_sequence;
    AccountId account_id;
    ClientOrderId client_order_id;
    InstrumentId instrument_id;
    RejectReason reason;
};

struct OrderCancelled {
    EventSequence event_sequence;
    CommandSequence command_sequence;
    AccountId account_id;
    ClientOrderId client_order_id;
    ExchangeOrderId exchange_order_id;
    InstrumentId instrument_id;
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
};

// One resting-order counterparty to a trade. TradeExecuted carries one of
// these per side so a single event fully describes both legs -- no consumer
// needs to correlate two separate events to build one execution report.
struct TradeCounterparty {
    AccountId account_id;
    ClientOrderId client_order_id;
    ExchangeOrderId exchange_order_id;
    // Remaining quantity on this order after this trade (0 if now fully
    // filled) -- lets a consumer distinguish a partial from a complete fill
    // without re-deriving it from prior state.
    Quantity remaining_quantity;
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
};

struct BookOrderAdded {
    EventSequence event_sequence;
    InstrumentId instrument_id;
    ExchangeOrderId exchange_order_id;
    Side side;
    Price price;
    Quantity quantity;
};

struct BookOrderReduced {
    EventSequence event_sequence;
    InstrumentId instrument_id;
    ExchangeOrderId exchange_order_id;
    Side side;
    Price price;
    Quantity new_remaining_quantity;
};

struct BookOrderRemoved {
    EventSequence event_sequence;
    InstrumentId instrument_id;
    ExchangeOrderId exchange_order_id;
    Side side;
    Price price;
};

using ExchangeEvent = std::variant<OrderAccepted, OrderRejected, OrderCancelled, OrderReplaced, TradeExecuted,
                                    BookOrderAdded, BookOrderReduced, BookOrderRemoved>;

} // namespace mdh::exchange
