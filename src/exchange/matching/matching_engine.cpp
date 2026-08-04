#include "exchange/matching/matching_engine.hpp"

#include <algorithm>
#include <type_traits>
#include <variant>

namespace mdh::exchange {

EngineStateSnapshot MatchingEngine::snapshot() const {
    std::vector<InstrumentId> instrument_ids;
    instrument_ids.reserve(books_.size());
    for (const auto& [instrument_id, book] : books_) {
        instrument_ids.push_back(instrument_id);
    }
    std::sort(instrument_ids.begin(), instrument_ids.end());

    EngineStateSnapshot snap;
    snap.instruments.reserve(instrument_ids.size());
    for (const auto instrument_id : instrument_ids) {
        const MatchingBook& book = books_.at(instrument_id);
        auto bids = book.all_bids();
        auto asks = book.all_asks();
        if (bids.empty() && asks.empty()) {
            // books_ never erases an instrument entry once touched, even
            // after every resting order on it is gone (Cancel/full-fill
            // don't remove the MatchingBook itself, only its contents) --
            // an instrument with nothing resting on either side carries no
            // meaningful state, so it's omitted rather than reported as an
            // empty pair of vectors.
            continue;
        }
        snap.instruments.push_back(InstrumentBookSnapshot{
            .instrument_id = instrument_id,
            .bids = std::move(bids),
            .asks = std::move(asks),
        });
    }
    return snap;
}

void MatchingEngine::process(const ExchangeCommand& command, const EventSink& sink) {
    std::visit(
        [&](const auto& cmd) {
            using T = std::decay_t<decltype(cmd)>;
            if constexpr (std::is_same_v<T, NewOrderCommand>) {
                process_new_order(cmd, sink);
            } else if constexpr (std::is_same_v<T, CancelOrderCommand>) {
                process_cancel(cmd, sink);
            } else if constexpr (std::is_same_v<T, ReplaceOrderCommand>) {
                process_replace(cmd, sink);
            }
        },
        command);
}

Quantity MatchingEngine::crossable_quantity(InstrumentId instrument_id, Side incoming_side, Price price,
                                             Quantity quantity) const {
    auto it = books_.find(instrument_id);
    if (it == books_.end()) {
        return 0;
    }
    // all_bids()/all_asks() are already ordered price-priority-then-FIFO, so
    // the first non-crossing level ends the scan -- nothing after it can
    // cross either.
    const auto contra_orders = incoming_side == Side::Buy ? it->second.all_asks() : it->second.all_bids();
    Quantity total = 0;
    for (const auto& order : contra_orders) {
        const bool crosses = incoming_side == Side::Buy ? price >= order.price : price <= order.price;
        if (!crosses) {
            break;
        }
        total += order.remaining_quantity;
        if (total >= quantity) {
            break;
        }
    }
    return total;
}

void MatchingEngine::match_and_rest(ExchangeRestingOrder& incoming, CommandSequence command_sequence,
                                     const EventSink& sink) {
    MatchingBook& book = books_[incoming.instrument_id];
    const Side contra_side = incoming.side == Side::Buy ? Side::Sell : Side::Buy;

    while (incoming.remaining_quantity > 0) {
        const auto contra = book.front_of_best(contra_side);
        if (!contra.has_value()) {
            break;
        }
        const bool crosses = incoming.side == Side::Buy ? incoming.price >= contra->price : incoming.price <= contra->price;
        if (!crosses) {
            break;
        }

        const Quantity trade_qty = std::min(incoming.remaining_quantity, contra->remaining_quantity);
        incoming.remaining_quantity -= trade_qty;
        const Quantity contra_remaining_after = contra->remaining_quantity - trade_qty;

        const TradeCounterparty aggressor_cp{
            .account_id = incoming.account_id,
            .client_order_id = incoming.client_order_id,
            .exchange_order_id = incoming.exchange_order_id,
            .remaining_quantity = incoming.remaining_quantity,
        };
        const TradeCounterparty resting_cp{
            .account_id = contra->account_id,
            .client_order_id = contra->client_order_id,
            .exchange_order_id = contra->exchange_order_id,
            .remaining_quantity = contra_remaining_after,
        };

        // Price-time priority convention: a trade executes at the resting
        // (passive) order's price, never the incoming (aggressive) order's
        // price -- the resting order is the one that already committed to a
        // price by sitting on the book.
        sink(TradeExecuted{
            .event_sequence = next_event_sequence_++,
            .command_sequence = command_sequence,
            .instrument_id = incoming.instrument_id,
            .price = contra->price,
            .quantity = trade_qty,
            .aggressor_side = incoming.side,
            .buyer = incoming.side == Side::Buy ? aggressor_cp : resting_cp,
            .seller = incoming.side == Side::Buy ? resting_cp : aggressor_cp,
        });

        if (contra_remaining_after == 0) {
            book.remove_front(contra_side);
            live_orders_.erase(LiveKey{contra->account_id, contra->client_order_id});
            sink(BookOrderRemoved{
                .event_sequence = next_event_sequence_++,
                .instrument_id = incoming.instrument_id,
                .exchange_order_id = contra->exchange_order_id,
                .side = contra->side,
                .price = contra->price,
            });
        } else {
            book.reduce_front(contra_side, contra_remaining_after);
            sink(BookOrderReduced{
                .event_sequence = next_event_sequence_++,
                .instrument_id = incoming.instrument_id,
                .exchange_order_id = contra->exchange_order_id,
                .side = contra->side,
                .price = contra->price,
                .new_remaining_quantity = contra_remaining_after,
            });
        }
    }
}

void MatchingEngine::rest_remainder_if_applicable(const ExchangeRestingOrder& order, const EventSink& sink) {
    if (order.time_in_force != TimeInForce::GTC || order.remaining_quantity == 0) {
        // IOC/FOK: any remainder is discarded silently -- never accepted as
        // resting, so there is nothing to announce.
        return;
    }
    books_[order.instrument_id].add(order);
    live_orders_[LiveKey{order.account_id, order.client_order_id}] =
        LiveOrderRef{order.instrument_id, order.exchange_order_id};
    sink(BookOrderAdded{
        .event_sequence = next_event_sequence_++,
        .instrument_id = order.instrument_id,
        .exchange_order_id = order.exchange_order_id,
        .side = order.side,
        .price = order.price,
        .quantity = order.remaining_quantity,
    });
}

void MatchingEngine::process_new_order(const NewOrderCommand& cmd, const EventSink& sink) {
    if (cmd.price <= 0) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::InvalidPrice,
        });
        return;
    }
    if (cmd.quantity == 0) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::InvalidQuantity,
        });
        return;
    }

    const LiveKey key{cmd.account_id, cmd.client_order_id};
    if (live_orders_.contains(key)) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::DuplicateOrderId,
        });
        return;
    }

    if (cmd.time_in_force == TimeInForce::FOK) {
        const Quantity available = crossable_quantity(cmd.instrument_id, cmd.side, cmd.price, cmd.quantity);
        if (available < cmd.quantity) {
            // No OrderAccepted, no book mutation: FOK either fills
            // completely or never touches the book at all.
            sink(OrderRejected{
                .event_sequence = next_event_sequence_++,
                .command_sequence = cmd.command_sequence,
                .account_id = cmd.account_id,
                .client_order_id = cmd.client_order_id,
                .instrument_id = cmd.instrument_id,
                .reason = RejectReason::InsufficientLiquidity,
            });
            return;
        }
    }

    const ExchangeOrderId exchange_order_id = next_exchange_order_id_++;
    ExchangeRestingOrder order{
        .exchange_order_id = exchange_order_id,
        .client_order_id = cmd.client_order_id,
        .account_id = cmd.account_id,
        .instrument_id = cmd.instrument_id,
        .side = cmd.side,
        .price = cmd.price,
        .original_quantity = cmd.quantity,
        .remaining_quantity = cmd.quantity,
        .time_in_force = cmd.time_in_force,
        .order_sequence = next_priority_++,
    };

    sink(OrderAccepted{
        .event_sequence = next_event_sequence_++,
        .command_sequence = cmd.command_sequence,
        .account_id = cmd.account_id,
        .client_order_id = cmd.client_order_id,
        .exchange_order_id = exchange_order_id,
        .instrument_id = cmd.instrument_id,
        .side = cmd.side,
        .price = cmd.price,
        .quantity = cmd.quantity,
        .order_type = cmd.order_type,
        .time_in_force = cmd.time_in_force,
    });

    match_and_rest(order, cmd.command_sequence, sink);
    rest_remainder_if_applicable(order, sink);
}

void MatchingEngine::process_cancel(const CancelOrderCommand& cmd, const EventSink& sink) {
    const LiveKey key{cmd.account_id, cmd.client_order_id};
    auto it = live_orders_.find(key);
    if (it == live_orders_.end() || it->second.instrument_id != cmd.instrument_id) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::UnknownOrderId,
        });
        return;
    }

    const ExchangeOrderId exchange_order_id = it->second.exchange_order_id;
    live_orders_.erase(it);
    const auto removed = books_[cmd.instrument_id].remove(exchange_order_id);
    // live_orders_ and each MatchingBook are kept in sync at every mutation
    // point in this engine, so `removed` is guaranteed to have a value here.

    sink(OrderCancelled{
        .event_sequence = next_event_sequence_++,
        .command_sequence = cmd.command_sequence,
        .account_id = cmd.account_id,
        .client_order_id = cmd.client_order_id,
        .exchange_order_id = exchange_order_id,
        .instrument_id = cmd.instrument_id,
    });
    sink(BookOrderRemoved{
        .event_sequence = next_event_sequence_++,
        .instrument_id = cmd.instrument_id,
        .exchange_order_id = exchange_order_id,
        .side = removed->side,
        .price = removed->price,
    });
}

void MatchingEngine::process_replace(const ReplaceOrderCommand& cmd, const EventSink& sink) {
    if (cmd.new_price <= 0 || cmd.new_quantity == 0) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.original_client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::InvalidReplacement,
        });
        return;
    }

    const LiveKey original_key{cmd.account_id, cmd.original_client_order_id};
    auto it = live_orders_.find(original_key);
    if (it == live_orders_.end() || it->second.instrument_id != cmd.instrument_id) {
        sink(OrderRejected{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .client_order_id = cmd.original_client_order_id,
            .instrument_id = cmd.instrument_id,
            .reason = RejectReason::UnknownOrderId,
        });
        return;
    }

    const ExchangeOrderId old_exchange_order_id = it->second.exchange_order_id;
    MatchingBook& book = books_[cmd.instrument_id];
    const auto old_order = book.find(old_exchange_order_id);
    // Guaranteed to have a value: same invariant as process_cancel.

    // Priority-preserving path: same price, quantity unchanged or decreased.
    // A price change or a quantity *increase* falls through to the
    // cancel-plus-new path below (see the class-level policy comment in
    // matching_engine.hpp).
    const bool preserves_priority = cmd.new_price == old_order->price && cmd.new_quantity <= old_order->remaining_quantity;

    if (preserves_priority) {
        book.reduce(old_exchange_order_id, cmd.new_quantity);
        book.set_client_order_id(old_exchange_order_id, cmd.new_client_order_id);
        live_orders_.erase(it);
        live_orders_[LiveKey{cmd.account_id, cmd.new_client_order_id}] =
            LiveOrderRef{cmd.instrument_id, old_exchange_order_id};

        sink(OrderReplaced{
            .event_sequence = next_event_sequence_++,
            .command_sequence = cmd.command_sequence,
            .account_id = cmd.account_id,
            .original_client_order_id = cmd.original_client_order_id,
            .new_client_order_id = cmd.new_client_order_id,
            .exchange_order_id = old_exchange_order_id,
            .instrument_id = cmd.instrument_id,
            .new_price = cmd.new_price,
            .new_quantity = cmd.new_quantity,
        });
        sink(BookOrderReduced{
            .event_sequence = next_event_sequence_++,
            .instrument_id = cmd.instrument_id,
            .exchange_order_id = old_exchange_order_id,
            .side = old_order->side,
            .price = old_order->price,
            .new_remaining_quantity = cmd.new_quantity,
        });
        return;
    }

    // Priority-losing path: cancel-plus-new. The old order vanishes from the
    // book and a freshly-id'd order re-enters the ordinary matching path, so
    // a reprice into a crossing price trades immediately just like any other
    // aggressive new order.
    book.remove(old_exchange_order_id);
    live_orders_.erase(it);

    const ExchangeOrderId new_exchange_order_id = next_exchange_order_id_++;
    sink(OrderReplaced{
        .event_sequence = next_event_sequence_++,
        .command_sequence = cmd.command_sequence,
        .account_id = cmd.account_id,
        .original_client_order_id = cmd.original_client_order_id,
        .new_client_order_id = cmd.new_client_order_id,
        .exchange_order_id = new_exchange_order_id,
        .instrument_id = cmd.instrument_id,
        .new_price = cmd.new_price,
        .new_quantity = cmd.new_quantity,
    });
    sink(BookOrderRemoved{
        .event_sequence = next_event_sequence_++,
        .instrument_id = cmd.instrument_id,
        .exchange_order_id = old_exchange_order_id,
        .side = old_order->side,
        .price = old_order->price,
    });

    ExchangeRestingOrder new_order{
        .exchange_order_id = new_exchange_order_id,
        .client_order_id = cmd.new_client_order_id,
        .account_id = cmd.account_id,
        .instrument_id = cmd.instrument_id,
        .side = old_order->side,
        .price = cmd.new_price,
        .original_quantity = cmd.new_quantity,
        .remaining_quantity = cmd.new_quantity,
        .time_in_force = old_order->time_in_force,
        .order_sequence = next_priority_++,
    };
    match_and_rest(new_order, cmd.command_sequence, sink);
    rest_remainder_if_applicable(new_order, sink);
}

} // namespace mdh::exchange
