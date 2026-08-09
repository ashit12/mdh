#include "trader/risk/trader_risk_engine.hpp"

namespace mdh::trader::risk {

exchange::RejectReason TraderRiskEngine::check(exchange::AccountId account_id, InstrumentId instrument_id, Side side,
                                                Price price, Quantity quantity,
                                                const positions::PositionTracker& positions) const {
    if (quantity > limits_.max_order_quantity) {
        return exchange::RejectReason::OrderTooLarge;
    }

    if (side == Side::Buy) {
        const auto notional = static_cast<positions::Balance>(quantity) * price;
        if (positions.cash(account_id) < notional) {
            return exchange::RejectReason::InsufficientFunds;
        }
    } else {
        if (positions.position(account_id, instrument_id) < quantity) {
            return exchange::RejectReason::InsufficientPosition;
        }
    }

    return exchange::RejectReason::None;
}

} // namespace mdh::trader::risk
