#include "trader/positions/position_tracker.hpp"

namespace mdh::trader::positions {

void PositionTracker::deposit_cash(exchange::AccountId account_id, Balance amount) {
    std::lock_guard<std::mutex> lock(mutex_);
    accounts_[account_id].cash += amount;
}

void PositionTracker::deposit_position(exchange::AccountId account_id, InstrumentId instrument_id, Quantity amount) {
    std::lock_guard<std::mutex> lock(mutex_);
    accounts_[account_id].holdings[instrument_id] += amount;
}

void PositionTracker::apply(const oms::Fill& fill) {
    std::lock_guard<std::mutex> lock(mutex_);
    AccountPosition& account = accounts_[fill.account_id];
    const auto notional = static_cast<Balance>(fill.quantity) * fill.price;
    if (fill.side == Side::Buy) {
        account.cash -= notional;
        account.holdings[fill.instrument_id] += fill.quantity;
    } else {
        account.cash += notional;
        // Relies on the same invariant exchange::ledger::Ledger's own
        // position_total decrement does: something upstream (here,
        // TraderRiskEngine, best-effort; ultimately the exchange's own
        // authoritative RiskEngine) never lets a sell fill for more than
        // was actually held, so this never underflows Quantity's unsigned
        // range in practice.
        account.holdings[fill.instrument_id] -= fill.quantity;
    }
}

Balance PositionTracker::cash(exchange::AccountId account_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = accounts_.find(account_id);
    return it == accounts_.end() ? Balance{0} : it->second.cash;
}

Quantity PositionTracker::position(exchange::AccountId account_id, InstrumentId instrument_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto account_it = accounts_.find(account_id);
    if (account_it == accounts_.end()) {
        return 0;
    }
    const auto holding_it = account_it->second.holdings.find(instrument_id);
    return holding_it == account_it->second.holdings.end() ? 0 : holding_it->second;
}

AccountPosition PositionTracker::account(exchange::AccountId account_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = accounts_.find(account_id);
    return it == accounts_.end() ? AccountPosition{} : it->second;
}

} // namespace mdh::trader::positions
