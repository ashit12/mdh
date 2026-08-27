#pragma once

#include <cstddef>
#include <functional>
#include <vector>

#include "common/types.hpp"
#include "trader/positions/pnl_tracker.hpp"
#include "trader/risk/trader_risk_gated_oms.hpp"
#include "trader/strategies/reference_price_walk.hpp"
#include "trader/strategies/resting_quote.hpp"

// A two-sided quote ladder around an internally-driven reference price --
// the participant whose purpose is to make the market continuously
// tradeable, so that everything downstream of it has something to trade
// against.
//
// With the defaults below and a reference price of 100.0000, it quotes:
//
//     99.98  bid          100.01  ask
//     99.99  bid          100.02  ask
//
// and moves the whole ladder as the reference price walks. Deliberately not
// a good market-making algorithm: no adverse-selection modelling, no
// volatility-dependent spread, no order-book-imbalance skew. Plausible
// liquidity that exercises the exchange is the entire objective, the same
// scoping MarketMakerStrategy already documents for itself.
//
// ── How this differs from MarketMakerStrategy, and why both exist ─────────
// MarketMakerStrategy quotes one level per side around the book's own
// midpoint, reacting to book updates. That is the right model for a maker
// joining a market that already exists, and it is the wrong model for the
// participant that has to create one: with an empty book there is no
// midpoint, so it returns without quoting and nothing ever starts. This
// class inverts both halves of that -- its price comes from a
// ReferencePriceWalk rather than the book, and it is driven by a timer
// rather than by book updates -- which is also what keeps it from quoting
// around its own reflection, since the public feed it would otherwise read
// contains its own quotes. The per-quote cancel/replace/replenish state
// machine both need is shared (RestingQuote), so the duplication is the
// config and the ladder arithmetic, not the order handling.
//
// ── Threading ─────────────────────────────────────────────────────────────
// on_quote_cycle() must be called from one thread only, which is the only
// thread allowed to touch this object. It learns about its own fills by
// reading its OMS's order states (which are mutex-guarded) on each cycle,
// rather than through a callback from the OMS's reader thread -- so this
// class has exactly one writer and needs no lock of its own, the same
// single-writer discipline every other strategy here follows.
namespace mdh::trader::strategies {

struct LadderMarketMakerConfig {
    InstrumentId instrument_id = 1;
    ReferencePriceWalkConfig walk{};

    std::size_t levels_per_side = 2;

    // Distance from the reference price to the *nearest* quote on each
    // side, and between consecutive quotes beyond it. Both in Price ticks;
    // 100 ticks is 0.01 (see common/types.hpp's fixed 4-decimal scale).
    Price half_spread = 100;
    Price level_spacing = 100;

    Quantity quote_size = 100;

    // Signed inventory cap. At +max_position the bid side is withdrawn
    // entirely (not merely left un-requoted) and at -max_position the ask
    // side is -- this strategy's own inventory-risk decision, independent of
    // and in addition to what TraderRiskEngine checks, exactly the
    // "two independent layers" relationship MarketMakerStrategy documents.
    // Symmetric here, unlike that class's bid-only cap, because a
    // participant trading against seeded inventory can genuinely end up net
    // short.
    positions::NetPosition max_position = 2'000;

    // Only replace a resting quote once its desired price has moved at
    // least this far. Defaulted to one walk step, so an actual move in the
    // reference price requotes and a flat step does not.
    Price requote_threshold = 100;
};

class LadderMarketMaker {
public:
    // How this strategy learns its own signed net traded position. A
    // std::function seam rather than a positions::PnlTracker reference, for
    // the same reason OrderManagementSystem takes a Sender instead of owning
    // a client (order_management_system.hpp): the only thing this class
    // needs is the number, and taking just the number keeps it unit-testable
    // with a one-line lambda and free of any opinion about who computes it.
    using NetPositionSource = std::function<positions::NetPosition()>;

    LadderMarketMaker(risk::TraderRiskGatedOms& trading, NetPositionSource net_position,
                       LadderMarketMakerConfig config);

    // One quote cycle: advance the reference price one step, recompute the
    // desired ladder, and bring every level toward it. Returns how many wire
    // requests were sent, which is 0 in a cycle where the reference price
    // did not move far enough to matter and nothing was filled.
    std::size_t on_quote_cycle();

    // Cancels every quote this strategy currently believes is resting, for a
    // clean shutdown. Returns how many cancel requests were sent.
    std::size_t withdraw_all();

    [[nodiscard]] Price reference_price() const { return walk_.price(); }
    [[nodiscard]] std::uint64_t cycles() const { return walk_.steps_taken(); }

    // Introspection for tests and for a status display -- the ladder as this
    // strategy believes it stands, nearest level first on each side.
    [[nodiscard]] const std::vector<RestingQuote>& bids() const { return bids_; }
    [[nodiscard]] const std::vector<RestingQuote>& asks() const { return asks_; }

private:
    [[nodiscard]] Price bid_price_for(std::size_t level, Price reference) const;
    [[nodiscard]] Price ask_price_for(std::size_t level, Price reference) const;

    risk::TraderRiskGatedOms& trading_;
    NetPositionSource net_position_;
    LadderMarketMakerConfig config_;
    ReferencePriceWalk walk_;
    std::vector<RestingQuote> bids_;
    std::vector<RestingQuote> asks_;
};

} // namespace mdh::trader::strategies
