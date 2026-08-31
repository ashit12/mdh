#include "exchange/ledger/ledger.hpp"

#include <type_traits>
#include <variant>

namespace mdh::exchange::ledger {

void Ledger::deposit_cash(AccountId account_id, Balance amount) { account_ref(account_id).cash_total += amount; }

void Ledger::deposit_position(AccountId account_id, InstrumentId instrument_id, Quantity amount) {
    account_ref(account_id).position_total[instrument_id] += amount;
}

Balance Ledger::available_cash(AccountId account_id) const {
    const auto it = accounts_.find(account_id);
    if (it == accounts_.end()) {
        return 0;
    }
    return it->second.cash_total - it->second.cash_reserved;
}

Quantity Ledger::available_position(AccountId account_id, InstrumentId instrument_id) const {
    const auto it = accounts_.find(account_id);
    if (it == accounts_.end()) {
        return 0;
    }
    const auto total_it = it->second.position_total.find(instrument_id);
    const Quantity total = total_it == it->second.position_total.end() ? 0 : total_it->second;
    const auto reserved_it = it->second.position_reserved.find(instrument_id);
    const Quantity reserved = reserved_it == it->second.position_reserved.end() ? 0 : reserved_it->second;
    return total - reserved; // MatchingEngine never lets a hold exceed what was available when opened, so total >= reserved always
}

AccountBalances Ledger::balances(AccountId account_id) const {
    const auto it = accounts_.find(account_id);
    return it == accounts_.end() ? AccountBalances{} : it->second;
}

std::optional<HoldView> Ledger::find_hold(AccountId account_id, ClientOrderId client_order_id) const {
    const auto it = holds_.find(HoldKey{account_id, client_order_id});
    if (it == holds_.end()) {
        return std::nullopt;
    }
    return HoldView{
        .instrument_id = it->second.instrument_id,
        .side = it->second.side,
        .limit_price = it->second.limit_price,
        .remaining = it->second.remaining,
    };
}

void Ledger::open_hold(AccountId account_id, ClientOrderId client_order_id, InstrumentId instrument_id, Side side,
                        Price limit_price, Quantity quantity) {
    if (quantity == 0) {
        return; // nothing to reserve (e.g. a GTC order that fully matched immediately, see on_order_replaced)
    }
    AccountBalances& balances_ref = account_ref(account_id);
    if (side == Side::Buy) {
        balances_ref.cash_reserved += static_cast<Balance>(quantity) * limit_price;
    } else {
        balances_ref.position_reserved[instrument_id] += quantity;
    }
    const HoldKey key{account_id, client_order_id};
    const auto [it, inserted] = holds_.insert_or_assign(key, Hold{
                                                               .instrument_id = instrument_id,
                                                               .side = side,
                                                               .limit_price = limit_price,
                                                               .remaining = quantity,
                                                           });
    (void)it;
    if (inserted) {
        hold_count_->fetch_add(1, std::memory_order_relaxed);
    }
}

void Ledger::release_hold_fully(AccountId account_id, const Hold& hold) {
    if (hold.remaining == 0) {
        return;
    }
    AccountBalances& balances_ref = account_ref(account_id);
    if (hold.side == Side::Buy) {
        balances_ref.cash_reserved -= static_cast<Balance>(hold.remaining) * hold.limit_price;
    } else {
        balances_ref.position_reserved[hold.instrument_id] -= hold.remaining;
    }
}

void Ledger::settle_leg(const TradeCounterparty& leg, Side side, InstrumentId instrument_id, Price trade_price,
                         Quantity trade_quantity) {
    AccountBalances& balances_ref = account_ref(leg.account_id);

    const auto hold_it = holds_.find(HoldKey{leg.account_id, leg.client_order_id});
    if (hold_it != holds_.end()) {
        // The resting (GTC) side of this trade, or a GTC order that
        // aggressed and is now trading against its own still-open hold --
        // release exactly the reservation this filled slice was holding
        // (at the order's own limit price, not the trade price: see
        // class-level comment on why any difference belongs in
        // `available`, not a separate refund step), then shrink the hold to
        // whatever the engine reports as still remaining on this order.
        Hold& hold = hold_it->second;
        if (side == Side::Buy) {
            balances_ref.cash_reserved -= static_cast<Balance>(trade_quantity) * hold.limit_price;
        } else {
            balances_ref.position_reserved[instrument_id] -= trade_quantity;
        }
        hold.remaining = leg.remaining_quantity;
        if (hold.remaining == 0) {
            erase_hold(hold_it);
        }
    }
    // Settlement (moving `total`) happens whether or not a hold existed --
    // an IOC/FOK leg never had one (see class-level comment), and simply
    // settles directly.
    if (side == Side::Buy) {
        balances_ref.cash_total -= static_cast<Balance>(trade_quantity) * trade_price;
        balances_ref.position_total[instrument_id] += trade_quantity;
    } else {
        balances_ref.cash_total += static_cast<Balance>(trade_quantity) * trade_price;
        balances_ref.position_total[instrument_id] -= trade_quantity;
    }
}

void Ledger::on_order_accepted(const OrderAccepted& event) {
    if (event.time_in_force != TimeInForce::GTC) {
        return; // IOC/FOK: never reserved, see class-level comment
    }
    open_hold(event.account_id, event.client_order_id, event.instrument_id, event.side, event.price, event.quantity);
}

void Ledger::on_order_replaced(const OrderReplaced& event) {
    const auto it = holds_.find(HoldKey{event.account_id, event.original_client_order_id});
    if (it == holds_.end()) {
        // The original order was never held (should not happen: only a
        // currently-live order can be replaced, and only GTC orders are
        // ever live long enough to be found this way -- see class-level
        // comment) -- defensively a no-op rather than fabricating state.
        return;
    }
    const Hold old_hold = it->second;
    release_hold_fully(event.account_id, old_hold);
    erase_hold(it);
    // Uniform for both of MatchingEngine's replace paths (priority-
    // preserving and cancel-plus-new): release the old hold in full, then
    // open a fresh one at the new price/quantity. When price is unchanged
    // (priority-preserving path) this is arithmetically identical to
    // adjusting the existing hold by the quantity delta.
    open_hold(event.account_id, event.new_client_order_id, event.instrument_id, old_hold.side, event.new_price,
              event.new_quantity);
}

void Ledger::on_order_cancelled(const OrderCancelled& event) {
    const auto it = holds_.find(HoldKey{event.account_id, event.client_order_id});
    if (it == holds_.end()) {
        return; // nothing was ever held for this order (should not happen; see on_order_replaced's comment)
    }
    release_hold_fully(event.account_id, it->second);
    erase_hold(it);
}

void Ledger::on_trade_executed(const TradeExecuted& event) {
    settle_leg(event.buyer, Side::Buy, event.instrument_id, event.price, event.quantity);
    settle_leg(event.seller, Side::Sell, event.instrument_id, event.price, event.quantity);
}

void Ledger::apply(const ExchangeEvent& event) {
    std::visit(
        [this](const auto& ev) {
            using T = std::decay_t<decltype(ev)>;
            if constexpr (std::is_same_v<T, OrderAccepted>) {
                on_order_accepted(ev);
            } else if constexpr (std::is_same_v<T, OrderReplaced>) {
                on_order_replaced(ev);
            } else if constexpr (std::is_same_v<T, OrderCancelled>) {
                on_order_cancelled(ev);
            } else if constexpr (std::is_same_v<T, TradeExecuted>) {
                on_trade_executed(ev);
            }
            // OrderRejected, BookOrderAdded/Reduced/Removed: no account
            // information to act on (see events.hpp's own class comment on
            // why Book* events are deliberately anonymous) -- no-ops.
        },
        event);
}

AccountBalances& Ledger::account_ref(AccountId account_id) {
    auto [it, inserted] = accounts_.try_emplace(account_id);
    if (inserted) {
        account_count_->fetch_add(1, std::memory_order_relaxed);
    }
    return it->second;
}

void Ledger::erase_hold(std::unordered_map<HoldKey, Hold, HoldKeyHash>::iterator it) {
    holds_.erase(it);
    hold_count_->fetch_sub(1, std::memory_order_relaxed);
}

} // namespace mdh::exchange::ledger
