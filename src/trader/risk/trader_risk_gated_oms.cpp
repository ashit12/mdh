#include "trader/risk/trader_risk_gated_oms.hpp"

#include <utility>

namespace mdh::trader::risk {

namespace {

// positions_'s own sink, plus an optional second observer. Built as one
// FillSink so OrderManagementSystem still sees exactly one, and so the
// fan-out order ("positions first, then the observer") is stated in one
// place: an observer reacting to a fill can then already read the updated
// position, exactly as it would if it were reading it a moment later.
[[nodiscard]] oms::OrderManagementSystem::FillSink fan_out(oms::OrderManagementSystem::FillSink primary,
                                                            oms::OrderManagementSystem::FillSink extra) {
    if (!extra) {
        return primary;
    }
    return [primary = std::move(primary), extra = std::move(extra)](const oms::Fill& fill) {
        primary(fill);
        extra(fill);
    };
}

} // namespace

TraderRiskGatedOms::TraderRiskGatedOms(exchange::AccountId account_id, oms::OrderManagementSystem::Sender sender,
                                        oms::OrderManagementSystem::OrderUpdateSink update_sink,
                                        TraderRiskLimits limits,
                                        oms::OrderManagementSystem::FillSink extra_fill_sink)
    : account_id_(account_id),
      risk_(limits),
      positions_(),
      oms_(account_id, std::move(sender), std::move(update_sink),
            fan_out(positions_.sink(), std::move(extra_fill_sink))) {}

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
