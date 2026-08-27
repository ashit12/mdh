#include "trader/positions/pnl_tracker.hpp"

#include <algorithm>

namespace mdh::trader::positions {

namespace {

[[nodiscard]] NetPosition magnitude(NetPosition value) { return value < 0 ? -value : value; }

[[nodiscard]] Balance sign_of(NetPosition value) { return value < 0 ? Balance{-1} : Balance{1}; }

} // namespace

void PnlTracker::apply(const oms::Fill& fill) {
    const auto quantity = static_cast<NetPosition>(fill.quantity);
    const NetPosition delta = fill.side == Side::Buy ? quantity : -quantity;

    std::lock_guard<std::mutex> lock(mutex_);
    InstrumentPnl& pnl = accounts_[fill.account_id][fill.instrument_id];
    pnl.filled_quantity += fill.quantity;
    ++pnl.fill_count;

    const bool opening_or_adding = pnl.position == 0 || (pnl.position > 0) == (delta > 0);
    if (opening_or_adding) {
        pnl.position += delta;
        pnl.open_notional += static_cast<Balance>(delta) * fill.price;
        return;
    }

    // Reducing, closing, or flipping. Take the closing part first, against a
    // proportional slice of the open basis -- exact when this closes the
    // position outright, since the slice is then the whole basis.
    const NetPosition open = magnitude(pnl.position);
    const NetPosition closed = std::min(open, magnitude(delta));
    const Balance closed_notional = pnl.open_notional * static_cast<Balance>(closed) / static_cast<Balance>(open);
    const Balance direction = sign_of(pnl.position);

    // For a long, this is (proceeds - cost); for a short, closed_notional is
    // itself negative (cash received on the way in), so subtracting it adds
    // those proceeds back and `direction` flips the closing cash flow.
    pnl.realized += direction * static_cast<Balance>(closed) * fill.price - closed_notional;
    pnl.open_notional -= closed_notional;
    pnl.position -= direction * closed;

    // Whatever the fill had left over past flat opens a new position on the
    // other side, entered at this fill's own price.
    const NetPosition flipped = magnitude(delta) - closed;
    if (flipped > 0) {
        const NetPosition flipped_signed = delta > 0 ? flipped : -flipped;
        pnl.position += flipped_signed;
        pnl.open_notional += static_cast<Balance>(flipped_signed) * fill.price;
    }
    if (pnl.position == 0) {
        pnl.open_notional = 0; // guards against a sub-tick remainder outliving the position it belonged to
    }
}

PnlSnapshot PnlTracker::snapshot(exchange::AccountId account_id, InstrumentId instrument_id,
                                  std::optional<Price> mark) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto account_it = accounts_.find(account_id);
    if (account_it == accounts_.end()) {
        return PnlSnapshot{};
    }
    const auto instrument_it = account_it->second.find(instrument_id);
    if (instrument_it == account_it->second.end()) {
        return PnlSnapshot{};
    }
    const InstrumentPnl& pnl = instrument_it->second;

    PnlSnapshot snap;
    snap.position = pnl.position;
    snap.average_entry_price = pnl.position == 0 ? 0 : pnl.open_notional / static_cast<Balance>(pnl.position);
    snap.realized = pnl.realized;
    snap.unrealized = mark ? static_cast<Balance>(pnl.position) * *mark - pnl.open_notional : Balance{0};
    snap.total = snap.realized + snap.unrealized;
    snap.filled_quantity = pnl.filled_quantity;
    snap.fill_count = pnl.fill_count;
    return snap;
}

Quantity PnlTracker::total_filled_quantity() const {
    std::lock_guard<std::mutex> lock(mutex_);
    Quantity total = 0;
    for (const auto& [account_id, instruments] : accounts_) {
        for (const auto& [instrument_id, pnl] : instruments) {
            total += pnl.filled_quantity;
        }
    }
    return total;
}

std::uint64_t PnlTracker::total_fill_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::uint64_t total = 0;
    for (const auto& [account_id, instruments] : accounts_) {
        for (const auto& [instrument_id, pnl] : instruments) {
            total += pnl.fill_count;
        }
    }
    return total;
}

} // namespace mdh::trader::positions
