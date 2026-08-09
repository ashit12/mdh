#include "trader/oms/order_management_system.hpp"

#include <utility>
#include <variant>

namespace mdh::trader::oms {

namespace {
[[nodiscard]] bool is_live(ClientOrderState state) {
    return state == ClientOrderState::Live || state == ClientOrderState::PartiallyFilled;
}
} // namespace

OrderManagementSystem::OrderManagementSystem(exchange::AccountId account_id, Sender sender,
                                              OrderUpdateSink update_sink, FillSink fill_sink)
    : account_id_(account_id),
      sender_(std::move(sender)),
      update_sink_(std::move(update_sink)),
      fill_sink_(std::move(fill_sink)) {}

exchange::ClientOrderId OrderManagementSystem::submit_new_order(InstrumentId instrument_id, Side side, Price price,
                                                                  Quantity quantity, exchange::OrderType order_type,
                                                                  exchange::TimeInForce time_in_force) {
    exchange::ClientOrderId id;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        id = next_client_order_id_++;
        orders_.emplace(id, ClientOrder{.account_id = account_id_,
                                         .client_order_id = id,
                                         .exchange_order_id = std::nullopt,
                                         .instrument_id = instrument_id,
                                         .side = side,
                                         .price = price,
                                         .quantity = quantity,
                                         .remaining_quantity = quantity,
                                         .order_type = order_type,
                                         .time_in_force = time_in_force,
                                         .state = ClientOrderState::PendingNew});
    }

    const protocol::order_entry::Message wire_message{protocol::order_entry::NewOrder{.account_id = account_id_,
                                                                                        .client_order_id = id,
                                                                                        .instrument_id = instrument_id,
                                                                                        .side = side,
                                                                                        .price = price,
                                                                                        .quantity = quantity,
                                                                                        .order_type = order_type,
                                                                                        .time_in_force = time_in_force}};
    if (!sender_(wire_message)) {
        mark_send_failed(id);
    }
    return id;
}

bool OrderManagementSystem::cancel_order(exchange::ClientOrderId client_order_id) {
    protocol::order_entry::Message wire_message;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = orders_.find(client_order_id);
        if (it == orders_.end() || !is_live(it->second.state) || it->second.pending_action != PendingAction::None) {
            return false;
        }
        it->second.pending_action = PendingAction::Cancel;
        wire_message = protocol::order_entry::Message{protocol::order_entry::CancelOrder{
            .account_id = account_id_, .client_order_id = client_order_id, .instrument_id = it->second.instrument_id}};
    }

    if (!sender_(wire_message)) {
        clear_pending_action(client_order_id);
        return false;
    }
    return true;
}

std::optional<exchange::ClientOrderId> OrderManagementSystem::replace_order(exchange::ClientOrderId client_order_id,
                                                                             Price new_price, Quantity new_quantity) {
    exchange::ClientOrderId new_id;
    protocol::order_entry::Message wire_message;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = orders_.find(client_order_id);
        if (it == orders_.end() || !is_live(it->second.state) || it->second.pending_action != PendingAction::None) {
            return std::nullopt;
        }
        it->second.pending_action = PendingAction::Replace;

        new_id = next_client_order_id_++;
        orders_.emplace(new_id, ClientOrder{.account_id = account_id_,
                                             .client_order_id = new_id,
                                             .exchange_order_id = std::nullopt,
                                             .instrument_id = it->second.instrument_id,
                                             .side = it->second.side,
                                             .price = new_price,
                                             .quantity = new_quantity,
                                             .remaining_quantity = new_quantity,
                                             .order_type = it->second.order_type,
                                             .time_in_force = it->second.time_in_force,
                                             .state = ClientOrderState::PendingNew});

        wire_message = protocol::order_entry::Message{
            protocol::order_entry::ReplaceOrder{.account_id = account_id_,
                                                 .original_client_order_id = client_order_id,
                                                 .new_client_order_id = new_id,
                                                 .instrument_id = it->second.instrument_id,
                                                 .new_price = new_price,
                                                 .new_quantity = new_quantity}};
    }

    if (!sender_(wire_message)) {
        std::lock_guard<std::mutex> lock(mutex_);
        orders_.erase(new_id); // the pre-registered replacement never actually went out -- it never existed
        if (auto it = orders_.find(client_order_id); it != orders_.end()) {
            it->second.pending_action = PendingAction::None;
        }
        return std::nullopt;
    }
    return new_id;
}

void OrderManagementSystem::handle_message(const protocol::order_entry::Message& message) {
    using namespace protocol::order_entry;
    std::visit(
        [this](const auto& event) {
            using T = std::decay_t<decltype(event)>;
            if constexpr (std::is_same_v<T, Accepted>) {
                on_accepted(event);
            } else if constexpr (std::is_same_v<T, Rejected>) {
                on_rejected(event);
            } else if constexpr (std::is_same_v<T, Cancelled>) {
                on_cancelled(event);
            } else if constexpr (std::is_same_v<T, Replaced>) {
                on_replaced(event);
            } else if constexpr (std::is_same_v<T, TradeReport>) {
                on_trade_report(event);
            }
            // NewOrder/CancelOrder/ReplaceOrder: client -> gateway types,
            // never valid arriving here -- see this method's own doc comment.
        },
        message);
}

void OrderManagementSystem::on_accepted(const protocol::order_entry::Accepted& event) {
    std::optional<ClientOrder> updated;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = orders_.find(event.client_order_id);
        if (it == orders_.end()) {
            return;
        }
        it->second.exchange_order_id = event.exchange_order_id;
        it->second.state = ClientOrderState::Live;
        it->second.quantity = event.quantity;
        it->second.remaining_quantity = event.quantity;
        updated = it->second;
    }
    notify(*updated);
}

void OrderManagementSystem::on_rejected(const protocol::order_entry::Rejected& event) {
    std::optional<ClientOrder> updated;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = orders_.find(event.client_order_id);
        if (it == orders_.end()) {
            return;
        }
        // Only a PendingNew order can actually *be* the thing that was
        // rejected -- any other state means this Rejected is answering an
        // in-flight cancel/replace attempt instead, which leaves the order
        // itself untouched. See class-level doc comment for why the wire
        // message alone can't distinguish these two cases.
        if (it->second.state == ClientOrderState::PendingNew) {
            it->second.state = ClientOrderState::Rejected;
        }
        it->second.pending_action = PendingAction::None;
        it->second.last_reject_reason = event.reason;
        updated = it->second;
    }
    notify(*updated);
}

void OrderManagementSystem::on_cancelled(const protocol::order_entry::Cancelled& event) {
    std::optional<ClientOrder> updated;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = orders_.find(event.client_order_id);
        if (it == orders_.end()) {
            return;
        }
        it->second.state = ClientOrderState::Cancelled;
        it->second.pending_action = PendingAction::None;
        updated = it->second;
    }
    notify(*updated);
}

void OrderManagementSystem::on_replaced(const protocol::order_entry::Replaced& event) {
    std::optional<ClientOrder> old_updated;
    std::optional<ClientOrder> new_updated;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (auto it = orders_.find(event.original_client_order_id); it != orders_.end()) {
            it->second.state = ClientOrderState::Replaced;
            it->second.pending_action = PendingAction::None;
            old_updated = it->second;
        }
        if (auto it = orders_.find(event.new_client_order_id); it != orders_.end()) {
            it->second.state = ClientOrderState::Live;
            it->second.exchange_order_id = event.exchange_order_id;
            it->second.price = event.new_price;
            it->second.quantity = event.new_quantity;
            it->second.remaining_quantity = event.new_quantity;
            new_updated = it->second;
        }
    }
    if (old_updated) {
        notify(*old_updated);
    }
    if (new_updated) {
        notify(*new_updated);
    }
}

void OrderManagementSystem::on_trade_report(const protocol::order_entry::TradeReport& event) {
    std::optional<ClientOrder> updated;
    Side side;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = orders_.find(event.client_order_id);
        if (it == orders_.end()) {
            return;
        }
        it->second.remaining_quantity = event.remaining_quantity;
        it->second.state = (event.remaining_quantity == 0) ? ClientOrderState::Filled : ClientOrderState::PartiallyFilled;
        side = it->second.side;
        updated = it->second;
    }
    notify(*updated);
    if (fill_sink_) {
        fill_sink_(Fill{.account_id = account_id_,
                         .client_order_id = event.client_order_id,
                         .instrument_id = event.instrument_id,
                         .side = side,
                         .price = event.price,
                         .quantity = event.quantity});
    }
}

void OrderManagementSystem::mark_send_failed(exchange::ClientOrderId client_order_id) {
    std::optional<ClientOrder> updated;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = orders_.find(client_order_id);
        if (it == orders_.end()) {
            return;
        }
        it->second.state = ClientOrderState::Rejected;
        it->second.pending_action = PendingAction::None;
        it->second.last_reject_reason = exchange::RejectReason::InternalError;
        updated = it->second;
    }
    notify(*updated);
}

void OrderManagementSystem::clear_pending_action(exchange::ClientOrderId client_order_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (auto it = orders_.find(client_order_id); it != orders_.end()) {
        it->second.pending_action = PendingAction::None;
    }
}

void OrderManagementSystem::notify(const ClientOrder& updated_order) {
    if (update_sink_) {
        update_sink_(updated_order);
    }
}

std::optional<ClientOrder> OrderManagementSystem::order(exchange::ClientOrderId client_order_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = orders_.find(client_order_id);
    return it == orders_.end() ? std::nullopt : std::optional<ClientOrder>(it->second);
}

std::vector<ClientOrder> OrderManagementSystem::orders() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<ClientOrder> result;
    result.reserve(orders_.size());
    for (const auto& [id, order] : orders_) {
        result.push_back(order);
    }
    return result;
}

} // namespace mdh::trader::oms
