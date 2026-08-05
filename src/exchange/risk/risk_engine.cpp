#include "exchange/risk/risk_engine.hpp"

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

} // namespace mdh::exchange::risk
