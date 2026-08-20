#pragma once

#include <optional>

#include "book/order_book.hpp"
#include "common/types.hpp"
#include "exchange/core/types.hpp"
#include "trader/risk/trader_risk_gated_oms.hpp"
#include "trader/strategies/strategy_runtime.hpp"

// A minimal, textbook two-sided market maker: on every book
// update for its own instrument, quotes a bid and an ask centered on the
// book's current midpoint, `half_spread` ticks apart from it on either
// side, `quote_size` deep, replacing a resting quote only once the desired
// price has drifted by at least `requote_threshold` ticks. Deliberately
// does not try to be profitable in any measured sense (no adverse-selection
// modeling, no dynamic spread widening on volatility, no order-book-
// imbalance skew) -- proving the strategy runtime / trader-side risk / OMS
// / gateway plumbing all compose correctly end to end (test_market_maker_
// strategy_e2e.cpp) is the actual goal here, matching the same
// "small but credible, not a real venue's/strategy's full stack" scoping
// exchange::risk::RiskLimits and trader::risk::TraderRiskLimits already
// document for themselves. CrossVenueArbStrategy is a second,
// still-deliberately-simple strategy built on this same plumbing.
//
// ── Inventory management: why this checks position itself, not just relying on risk ──
// TraderRiskEngine already refuses a sell that exceeds held
// position, so the ask side is naturally self-limiting once inventory runs
// out -- an ask this class cannot currently afford to quote (zero
// inventory) is simply locally risk-rejected by TraderRiskGatedOms
// (SubmitOutcome::client_order_id == nullopt) and retried on the next book
// update, with no special-casing needed here. But nothing stops the *bid*
// side from accumulating an ever-growing long position if this class
// quoted it unconditionally forever -- capping the bid side at
// `max_position` (withdrawing the resting bid entirely, not just declining
// to requote it, once reached) is this strategy's own P&L/inventory-risk
// decision, independent of and in addition to what TraderRiskEngine checks
// for -- exactly the "two independent layers" relationship the trader-side
// risk headers describe between trader-side and exchange-side risk, one
// level up.
namespace mdh::trader::strategies {

struct MarketMakerConfig {
    InstrumentId instrument_id;
    Quantity quote_size = 10;
    Price half_spread = 1;       // ticks from the book's current midpoint on each side
    Quantity max_position = 100; // stop (and withdraw) the bid once held position reaches this
    Price requote_threshold = 1; // only replace a resting quote once the desired price has moved at least this many ticks
};

class MarketMakerStrategy {
public:
    MarketMakerStrategy(risk::TraderRiskGatedOms& trading, MarketMakerConfig config);

    // Ignores updates for any instrument other than config.instrument_id --
    // safe to subscribe this same instance to a StrategyRuntime for
    // multiple instruments if ever needed, though in practice it is only
    // ever wired to its own one.
    void on_book_update(InstrumentId instrument_id, const book::OrderBook& book);

    [[nodiscard]] BookUpdateSink book_update_sink() {
        return [this](InstrumentId id, const book::OrderBook& book) { on_book_update(id, book); };
    }

    // Introspection for tests -- the client_order_id currently believed to
    // be this strategy's resting quote on each side, if any.
    [[nodiscard]] std::optional<exchange::ClientOrderId> bid_order_id() const { return bid_order_id_; }
    [[nodiscard]] std::optional<exchange::ClientOrderId> ask_order_id() const { return ask_order_id_; }

private:
    // `desired_price` of std::nullopt means "withdraw and do not quote this
    // side at all right now" (used for the bid once max_position is
    // reached) rather than "quote at price 0," which would be a real,
    // meaningful (if nonsensical) price.
    void update_side(Side side, std::optional<exchange::ClientOrderId>& order_id, std::optional<Price> desired_price);

    risk::TraderRiskGatedOms& trading_;
    MarketMakerConfig config_;
    std::optional<exchange::ClientOrderId> bid_order_id_;
    std::optional<exchange::ClientOrderId> ask_order_id_;
};

} // namespace mdh::trader::strategies
