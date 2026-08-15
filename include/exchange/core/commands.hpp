#pragma once

#include <variant>

#include "common/types.hpp"
#include "exchange/core/types.hpp"

// Transport-independent exchange commands -- the exchange's inbound
// vocabulary. Deliberately carry only what deterministic processing needs:
// no socket handles, no file offsets, no JSON, and no timestamp captured
// inside the matcher (a command's effect must depend only on its own fields
// and book state at the time it's processed, never on wall-clock time
// observed during matching). Whatever eventually produces these -- a TCP
// gateway (a later milestone) or a test constructing them directly -- is
// entirely decoupled from this type's definition; nothing here has been
// decoded from wire bytes.
//
// Every struct below also defines a defaulted operator== (Milestone 3): the
// command journal's roundtrip tests need to assert a decoded command equals
// the original one that was encoded. Purely additive -- does not affect
// aggregate-ness or existing designated-initializer construction.
namespace mdh::exchange {

struct NewOrderCommand {
    CommandSequence command_sequence;
    AccountId account_id;
    ClientOrderId client_order_id;
    InstrumentId instrument_id;
    Side side;
    Price price;
    Quantity quantity;
    OrderType order_type;
    TimeInForce time_in_force;

    bool operator==(const NewOrderCommand&) const = default;
};

struct CancelOrderCommand {
    CommandSequence command_sequence;
    AccountId account_id;
    ClientOrderId client_order_id;
    InstrumentId instrument_id;

    bool operator==(const CancelOrderCommand&) const = default;
};

struct ReplaceOrderCommand {
    CommandSequence command_sequence;
    AccountId account_id;
    ClientOrderId original_client_order_id;
    ClientOrderId new_client_order_id;
    InstrumentId instrument_id;
    Price new_price;
    Quantity new_quantity;

    bool operator==(const ReplaceOrderCommand&) const = default;
};

using ExchangeCommand = std::variant<NewOrderCommand, CancelOrderCommand, ReplaceOrderCommand>;

} // namespace mdh::exchange
