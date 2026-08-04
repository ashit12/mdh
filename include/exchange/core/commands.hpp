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
};

struct CancelOrderCommand {
    CommandSequence command_sequence;
    AccountId account_id;
    // Identifies the resting order to cancel -- the same client_order_id it
    // was submitted under via NewOrderCommand. This project does not model a
    // separate cancel-request id distinct from the target order's own id
    // (some real order-entry protocols do, for ack correlation); deferred as
    // unneeded complexity for the current milestones.
    ClientOrderId client_order_id;
    InstrumentId instrument_id;
};

struct ReplaceOrderCommand {
    CommandSequence command_sequence;
    AccountId account_id;
    // The live order being replaced, identified by its original client_order_id.
    ClientOrderId original_client_order_id;
    // The id the replacement order is submitted under. A successful replace
    // makes original_client_order_id no longer live; new_client_order_id
    // becomes the id of the (possibly re-priced/re-sized) resting order.
    ClientOrderId new_client_order_id;
    InstrumentId instrument_id;
    Price new_price;
    Quantity new_quantity;
};

using ExchangeCommand = std::variant<NewOrderCommand, CancelOrderCommand, ReplaceOrderCommand>;

} // namespace mdh::exchange
