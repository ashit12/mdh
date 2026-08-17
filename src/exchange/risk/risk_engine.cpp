#include "exchange/risk/risk_engine.hpp"

#include <algorithm>

namespace mdh::exchange::risk {

RejectReason RiskEngine::check(const NewOrderCommand& command, const ledger::Ledger& ledger) const {
    if (command.quantity > limits_.max_order_quantity) {
        return RejectReason::OrderTooLarge;
    }

    if (command.side == Side::Buy) {
        const auto notional = static_cast<ledger::Balance>(command.quantity) * command.price;
        if (ledger.available_cash(command.account_id) < notional) {
            return RejectReason::InsufficientFunds;
        }
    } else {
        if (ledger.available_position(command.account_id, command.instrument_id) < command.quantity) {
            return RejectReason::InsufficientPosition;
        }
    }

    return RejectReason::None;
}

RejectReason RiskEngine::check(const ReplaceOrderCommand& command, const ledger::Ledger& ledger) const {
    if (command.new_quantity > limits_.max_order_quantity) {
        return RejectReason::OrderTooLarge;
    }

    const auto hold = ledger.find_hold(command.account_id, command.original_client_order_id);
    if (!hold.has_value() || hold->instrument_id != command.instrument_id) {
        // Nothing reserved under this id (or wrong instrument) -- MatchingEngine
        // will reject with UnknownOrderId. Risk has no reservation to credit.
        return RejectReason::None;
    }

    if (hold->side == Side::Buy) {
        const auto old_required = static_cast<ledger::Balance>(hold->remaining) * hold->limit_price;
        const auto new_required = static_cast<ledger::Balance>(command.new_quantity) * command.new_price;
        const auto extra_required = std::max(ledger::Balance{0}, new_required - old_required);
        if (ledger.available_cash(command.account_id) < extra_required) {
            return RejectReason::InsufficientFunds;
        }
    } else {
        const Quantity extra_required =
            command.new_quantity > hold->remaining ? command.new_quantity - hold->remaining : 0;
        if (ledger.available_position(command.account_id, command.instrument_id) < extra_required) {
            return RejectReason::InsufficientPosition;
        }
    }

    return RejectReason::None;
}

} // namespace mdh::exchange::risk
