#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

#include "common/types.hpp"
#include "exchange/core/types.hpp"

// The trader-side order-entry OMS's own view of one order (Milestone 8) --
// deliberately a distinct type from every exchange-side or wire-level order
// representation (exchange::matching's resting orders, protocol::order_entry::
// NewOrder/Accepted/etc.), for exactly the same reason exchange-side and
// trader-side books are kept as separate classes (see docs/
// end_to_end_architecture.md section 5): this is a *reconstruction* of what
// the OMS believes is true from the responses it has received so far, not
// the authoritative record (that's still the exchange's MatchingEngine).
namespace mdh::trader::oms {

// A client_order_id can be terminal (nothing further will ever be observed
// for it) or live (still working at the exchange, in one of these states):
//   PendingNew -> Live -> [PartiallyFilled <-> Live via more trades] -> Filled
//              -> Rejected
//   Live/PartiallyFilled -> Cancelled
//   Live/PartiallyFilled -> Replaced (superseded by a new client_order_id --
//                                     see ClientOrder::pending_action and
//                                     OrderManagementSystem::replace_order())
enum class ClientOrderState {
    PendingNew,      // NewOrder sent; no Accepted/Rejected observed yet
    Live,            // Accepted, and either never filled or filled with more still working
    PartiallyFilled, // at least one fill observed, with quantity still remaining
    Filled,          // remaining_quantity reached zero via a TradeReport (terminal)
    Rejected,        // the original NewOrder was rejected (terminal)
    Cancelled,       // a Cancelled response was observed (terminal)
    Replaced,        // superseded by a new client_order_id (terminal for *this* id only)
};

[[nodiscard]] constexpr std::string_view to_string(ClientOrderState s) {
    switch (s) {
        case ClientOrderState::PendingNew:      return "PendingNew";
        case ClientOrderState::Live:             return "Live";
        case ClientOrderState::PartiallyFilled: return "PartiallyFilled";
        case ClientOrderState::Filled:           return "Filled";
        case ClientOrderState::Rejected:         return "Rejected";
        case ClientOrderState::Cancelled:        return "Cancelled";
        case ClientOrderState::Replaced:         return "Replaced";
    }
    return "UnknownClientOrderState";
}

// Tracks a Cancel/Replace request already sent for this order, still
// awaiting a definitive response. Needed because the wire protocol's
// Rejected message (protocol/order_entry/messages.hpp) carries no
// "what kind of request failed" field of its own -- a Rejected referencing
// an order that is *not* PendingNew must mean an in-flight cancel/replace
// attempt failed, not that the order itself was rejected (the order is
// unaffected and remains exactly as it was); see
// OrderManagementSystem::handle_message()'s own doc comment. Also used to
// reject a second concurrent cancel/replace attempt on the same order
// (cancel_order()/replace_order() both require this to be None).
enum class PendingAction {
    None,
    Cancel,
    Replace,
};

struct ClientOrder {
    exchange::AccountId account_id;
    exchange::ClientOrderId client_order_id;
    // Unset until an Accepted (or, for a replacement, a Replaced) response
    // assigns one -- never known at submission time.
    std::optional<exchange::ExchangeOrderId> exchange_order_id;
    InstrumentId instrument_id;
    Side side;
    // Current working price/quantity -- updated wholesale by a successful
    // Replaced (mirrors the exchange's own ledger reservation semantics,
    // which likewise re-opens a fresh hold at the new price/quantity rather
    // than adjusting the old one in place, see ledger.hpp's class comment).
    Price price;
    Quantity quantity;
    // Live/remaining amount still workable at the exchange; 0 once Filled.
    // Meaningless (left at whatever it last was) once the order reaches any
    // other terminal state.
    Quantity remaining_quantity;
    exchange::OrderType order_type;
    exchange::TimeInForce time_in_force;

    ClientOrderState state = ClientOrderState::PendingNew;
    PendingAction pending_action = PendingAction::None;
    // Meaningful only after at least one Rejected has been observed for
    // this client_order_id (either the order itself, or its most recent
    // failed cancel/replace attempt) -- see PendingAction's own comment.
    exchange::RejectReason last_reject_reason = exchange::RejectReason::None;

    bool operator==(const ClientOrder&) const = default;
};

} // namespace mdh::trader::oms
