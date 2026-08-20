#pragma once

#include <variant>

#include "common/types.hpp"
#include "exchange/core/types.hpp"

// The exchange's inbound vocabulary, with no knowledge of any transport.
// These carry only what deterministic processing needs: no socket handles,
// no file offsets, no JSON, and no timestamp read inside the matcher. A
// command's effect must depend only on its own fields and the book state
// when it is processed, never on the clock.
//
// Whatever produces them -- the TCP gateway, or a test building them
// directly -- is entirely decoupled from this definition. Nothing here has
// been decoded from wire bytes.
//
// Every struct defines a defaulted operator== so the journal's round-trip
// tests can assert that a decoded command equals the one encoded.
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
