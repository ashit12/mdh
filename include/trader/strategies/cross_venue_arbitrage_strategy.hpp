#pragma once

#include <optional>

#include "book/order_book.hpp"
#include "common/types.hpp"
#include "exchange/core/types.hpp"
#include "trader/risk/trader_risk_gated_oms.hpp"
#include "trader/strategies/strategy_runtime.hpp"

// A textbook cross-venue arbitrage strategy (Milestone 11): watches the
// SAME instrument's book on two independent venues -- two entirely separate
// exchange gateway + matching engine + ledger stacks, each with its own
// trader::risk::TraderRiskGatedOms on this trader's own side -- and,
// whenever one venue's best ask sits at least `min_edge` below the other
// venue's best bid, buys on the cheap venue and sells on the expensive one,
// `trade_size` at a time. Proves two things at once: (a) a second, distinct
// strategy composes on top of the exact same StrategyRuntime/
// TraderRiskGatedOms plumbing MarketMakerStrategy (Milestone 10) uses, with
// no changes to that plumbing, and (b) that plumbing genuinely supports
// trading on more than one venue from a single process at once -- see
// test_cross_venue_arbitrage_strategy_e2e.cpp's "two-venue simulation,"
// which runs two complete, independent exchange stacks side by side.
//
// ── Why IOC, not GTC (unlike MarketMakerStrategy's resting quotes) ────────
// An arbitrage order is only worth sending if it can execute immediately
// against the edge just observed -- by the time a GTC order rested and
// waited, the edge that justified it may already be gone (or worse, only
// one leg fills, leaving a naked, un-hedged position). IOC also keeps this
// strategy's own bookkeeping simple: it fires one order per leg per
// opportunity and never has to track a resting arbitrage order the way
// MarketMakerStrategy tracks its quotes.
//
// ── A known, documented simplification: no de-duplication/cooldown ───────
// evaluate() re-checks the edge and can fire again on every single book
// update from either venue, using whatever the OTHER venue's most recently
// observed price was (which may not have caught up yet if only one venue's
// book has moved since the last evaluation). A very bursty sequence of
// updates from one venue, before the other venue's confirming update or
// this trade's own fill arrives, could therefore re-fire on an edge that a
// prior, still-in-flight order already started consuming. TraderRiskEngine
// (Milestone 9) and each venue's own exchange-side risk still bound the
// damage (insufficient funds/position simply locally- or exchange-reject
// the excess), so this is a documented inefficiency, not a safety hole --
// mirroring the same "small but credible, not exhaustive" scoping already
// established for RiskLimits/TraderRiskLimits, applied here to strategy
// logic instead of risk checks.
namespace mdh::trader::strategies {

struct CrossVenueArbConfig {
    InstrumentId instrument_id;
    Quantity trade_size = 10;
    // Minimum edge (the higher venue's best bid minus the lower venue's
    // best ask) required before trading -- a real venue's fees and
    // slippage, which this simulation does not model, would otherwise eat
    // into a razor-thin edge; this cap stands in for that margin of safety.
    Price min_edge = 1;
};

class CrossVenueArbStrategy {
public:
    CrossVenueArbStrategy(risk::TraderRiskGatedOms& venue_a, risk::TraderRiskGatedOms& venue_b, CrossVenueArbConfig config);

    // Called whenever venue A's (respectively, venue B's) book for this
    // strategy's instrument updates -- wire venue_a_sink() to venue A's own
    // StrategyRuntime::subscribe() and venue_b_sink() to venue B's.
    void on_venue_a_update(InstrumentId instrument_id, const book::OrderBook& book);
    void on_venue_b_update(InstrumentId instrument_id, const book::OrderBook& book);

    [[nodiscard]] BookUpdateSink venue_a_sink() {
        return [this](InstrumentId id, const book::OrderBook& book) { on_venue_a_update(id, book); };
    }
    [[nodiscard]] BookUpdateSink venue_b_sink() {
        return [this](InstrumentId id, const book::OrderBook& book) { on_venue_b_update(id, book); };
    }

private:
    void evaluate();

    risk::TraderRiskGatedOms& venue_a_;
    risk::TraderRiskGatedOms& venue_b_;
    CrossVenueArbConfig config_;
    std::optional<book::PriceLevelView> venue_a_best_bid_;
    std::optional<book::PriceLevelView> venue_a_best_ask_;
    std::optional<book::PriceLevelView> venue_b_best_bid_;
    std::optional<book::PriceLevelView> venue_b_best_ask_;
};

} // namespace mdh::trader::strategies
