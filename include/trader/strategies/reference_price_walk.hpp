#pragma once

#include <algorithm>
#include <cstdint>
#include <random>

#include "common/types.hpp"

// A deterministic, seeded random walk over Price -- a stand-in for "what
// this instrument is currently worth" for a participant that needs an
// opinion of its own rather than one read off the book.
//
// ── Why a market maker needs this at all ──────────────────────────────────
// trader::strategies::MarketMakerStrategy quotes around the *book's* own
// midpoint, which is the right input for a maker joining an existing
// market. It is the wrong input for the participant whose job is to *be*
// the market: on an empty book there is no midpoint to quote around, so
// such a strategy never sends its first order and the book stays empty
// forever. Quoting around an internally-driven reference price solves that,
// and also removes a feedback loop -- the public feed contains the maker's
// own quotes, so a maker centring on the book mid is partly centring on
// itself.
//
// ── Why the walk is advanced by step count, never by elapsed time ─────────
// This is the whole reproducibility story for a simulation that otherwise
// runs over real sockets. std::mt19937_64 seeded once, one draw per step(),
// means step k has the same value on every run, machine and build type, for
// a given seed -- so the sequence of prices a market maker *intends* to
// quote is exactly reproducible even though which of those quotes actually
// gets filled depends on thread and network timing that nothing can pin
// down. Reading a clock here, or drawing a variable number of times per
// step, would give that up for nothing. Same seeded-mt19937_64 convention
// apps/feed_generator already uses to make its output byte-identical per
// seed.
namespace mdh::trader::strategies {

struct ReferencePriceWalkConfig {
    // 1'000'000 ticks is 100.0000 at Price's fixed 4-decimal scale (see
    // common/types.hpp) -- the "fair price, e.g. 100.00" this walk starts
    // from.
    Price initial_price = 1'000'000;

    // How far one step moves, in Price ticks. 100 ticks is 0.01, so a walk
    // reads 100.00 -> 100.01 -> 100.01 -> 100.00 -> 99.99 rather than
    // crawling in 0.0001 increments no quote ladder would notice.
    Price step_size = 100;

    // The walk is clamped to [min_price, max_price] rather than allowed to
    // wander arbitrarily far: a long run would otherwise drift somewhere a
    // fixed seeded cash/inventory allocation can no longer support, turning
    // a simulation into a study of risk rejections. Clamping is a simulation
    // convenience with no real-market meaning, which is exactly why it is
    // configurable and not hidden.
    Price min_price = 500'000;   // 50.0000
    Price max_price = 1'500'000; // 150.0000

    std::uint64_t seed = 42;
};

class ReferencePriceWalk {
public:
    explicit ReferencePriceWalk(ReferencePriceWalkConfig config)
        : config_(config), price_(config.initial_price), rng_(config.seed) {}

    // Advances one step -- down one step_size, unchanged, or up one, with
    // equal probability -- and returns the new price. Exactly one draw per
    // call; see the class comment on why that matters.
    Price step() {
        std::uniform_int_distribution<int> direction(-1, 1);
        price_ += static_cast<Price>(direction(rng_)) * config_.step_size;
        price_ = std::min(config_.max_price, std::max(config_.min_price, price_));
        ++steps_taken_;
        return price_;
    }

    [[nodiscard]] Price price() const { return price_; }
    [[nodiscard]] std::uint64_t steps_taken() const { return steps_taken_; }

private:
    ReferencePriceWalkConfig config_;
    Price price_;
    std::mt19937_64 rng_;
    std::uint64_t steps_taken_ = 0;
};

} // namespace mdh::trader::strategies
