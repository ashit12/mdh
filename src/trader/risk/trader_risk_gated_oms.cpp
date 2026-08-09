#include "trader/risk/trader_risk_gated_oms.hpp"

#include <utility>

namespace mdh::trader::risk {

TraderRiskGatedOms::TraderRiskGatedOms(exchange::AccountId account_id, oms::OrderManagementSystem::Sender sender,
                                        oms::OrderManagementSystem::OrderUpdateSink update_sink,
                                        TraderRiskLimits limits)
    : account_id_(account_id),
      risk_(limits),
      positions_(),
      oms_(account_id, std::move(sender), std::move(update_sink), positions_.sink()) {}

SubmitOutcome TraderRiskGatedOms::submit_new_order(InstrumentId instrument_id, Side side, Price price,
                                                    Quantity quantity, exchange::OrderType order_type,
                                                    exchange::TimeInForce time_in_force) {
    const auto reason = risk_.check(account_id_, instrument_id, side, price, quantity, positions_);
    if (reason != exchange::RejectReason::None) {
        return SubmitOutcome{.client_order_id = std::nullopt, .local_reject_reason = reason};
    }
    return SubmitOutcome{.client_order_id = oms_.submit_new_order(instrument_id, side, price, quantity, order_type,
                                                                   time_in_force)};
}

bool TraderRiskGatedOms::cancel_order(exchange::ClientOrderId client_order_id) {
    return oms_.cancel_order(client_order_id);
}

std::optional<exchange::ClientOrderId> TraderRiskGatedOms::replace_order(exchange::ClientOrderId client_order_id,
                                                                          Price new_price, Quantity new_quantity) {
    return oms_.replace_order(client_order_id, new_price, new_quantity);
}

std::optional<oms::ClientOrder> TraderRiskGatedOms::order(exchange::ClientOrderId client_order_id) const {
    return oms_.order(client_order_id);
}

std::vector<oms::ClientOrder> TraderRiskGatedOms::orders() const { return oms_.orders(); }

void TraderRiskGatedOms::handle_message(const protocol::order_entry::Message& message) { oms_.handle_message(message); }

} // namespace mdh::trader::risk
