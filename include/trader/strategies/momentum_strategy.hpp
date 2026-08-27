#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <optional>

#include "book/order_book.hpp"
#include "common/types.hpp"
#include "trader/positions/pnl_tracker.hpp"
#include "trader/risk/trader_risk_gated_oms.hpp"
#include "trader/strategies/strategy_runtime.hpp"

// A deliberately understandable momentum strategy: if the midpoint has
// risen over a short window, buy; if it has fallen, sell.
//
//     mid(now) - mid(lookback updates ago)  >  +threshold   ->  BUY
//     mid(now) - mid(lookback updates ago)  <  -threshold   ->  SELL
//
// That is the whole signal. No trend/mean-reversion regime detection, no
// volatility scaling, no attempt to be right more often than wrong. The
// point is to demonstrate a strategy trading a live market through the real
// order-entry path, not to claim alpha -- the same explicit scoping
// MarketMakerStrategy and CrossVenueArbStrategy each state for themselves.
//
// ── Everything it decides from comes from market data ─────────────────────
// Its only input is the reconstructed book handed to on_book_update() by
// StrategyRuntime, which is fed by market_data::FeedSubscriber from the real
// UDP feed. It never reads exchange state; it cannot, since it holds nothing
// but a TraderRiskGatedOms, which reaches the exchange only over TCP.
//
// ── Why IOC, and why priced through the touch ─────────────────────────────
// Same reason CrossVenueArbStrategy uses IOC: a signal is worth acting on
// only if it can execute against the market just observed. A GTC order
// resting at the touch would sit there while the signal that justified it
// decayed, and would leave this class tracking resting orders it has no
// logic to manage. Pricing `cross_slack` through the far touch covers the
// book having moved in the microseconds between the feed event and the
// order reaching the matching engine -- the fill still happens at the
// resting order's price, so the slack costs nothing when the book has not
// moved.
//
// ── Why a cooldown and a position cap are not optional extras ────────────
// on_book_update() runs on every single feed event, which on a busy
// instrument is thousands per second, and the signal stays above its
// threshold for as long as the trend lasts. Without a cooldown this class
// would fire an order per event for the whole duration of a move -- a flood
// that says nothing about whether the strategy works and a great deal about
// how fast a loop can call send(). The position cap plays the matching role
// for size: it bounds what one sustained trend can accumulate, and orders
// are shrunk to fit it rather than rejected at it, so the cap is approached
// smoothly instead of being hit and bounced off.
//
// ── Threading ─────────────────────────────────────────────────────────────
// on_book_update() is called from the FeedSubscriber's receive thread and
// from nowhere else, so this class has a single writer and needs no lock --
// the same discipline LadderMarketMaker follows on its own quote thread.
namespace mdh::trader::strategies {

struct MomentumStrategyConfig {
    InstrumentId instrument_id = 1;

    // Window length, in book updates -- not in wall-clock time, so a run's
    // behaviour does not depend on how fast the feed happens to be.
    std::size_t lookback = 20;

    // How far the midpoint must have moved across the window before this
    // strategy acts, in Price ticks (100 ticks is 0.01).
    Price entry_threshold = 100;

    Quantity trade_size = 50;
    positions::NetPosition max_position = 500;

    // Ticks past the far touch to price an order at -- see the class comment.
    Price cross_slack = 100;

    // Book updates that must pass after an order before another may be
    // sent.
    std::size_t cooldown_updates = 20;
};

class MomentumStrategy {
public:
    // Same seam, for the same reason, as LadderMarketMaker::NetPositionSource.
    using NetPositionSource = std::function<positions::NetPosition()>;

    MomentumStrategy(risk::TraderRiskGatedOms& trading, NetPositionSource net_position, MomentumStrategyConfig config);

    // Ignores updates for any instrument other than config.instrument_id,
    // matching MarketMakerStrategy::on_book_update()'s own convention.
    void on_book_update(InstrumentId instrument_id, const book::OrderBook& book);

    [[nodiscard]] BookUpdateSink book_update_sink() {
        return [this](InstrumentId id, const book::OrderBook& book) { on_book_update(id, book); };
    }

    // Introspection for tests and for a status display. `signal` is
    // std::nullopt until the window has filled.
    [[nodiscard]] std::optional<Price> signal() const;
    [[nodiscard]] std::uint64_t orders_sent() const { return orders_sent_; }
    [[nodiscard]] std::uint64_t updates_seen() const { return updates_seen_; }

private:
    risk::TraderRiskGatedOms& trading_;
    NetPositionSource net_position_;
    MomentumStrategyConfig config_;

    // Oldest first; capped at config_.lookback + 1 entries so that
    // back() - front() spans exactly `lookback` updates.
    std::deque<Price> mids_;
    std::size_t updates_since_order_ = 0;
    std::uint64_t updates_seen_ = 0;
    std::uint64_t orders_sent_ = 0;
};

} // namespace mdh::trader::strategies
