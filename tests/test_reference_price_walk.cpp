#include <gtest/gtest.h>

#include <vector>

#include "trader/strategies/reference_price_walk.hpp"

// The reproducibility guarantee the whole simulation rests on: for a given
// seed, step k has the same value on every run. Everything else here is
// bounds checking.
using namespace mdh;
using namespace mdh::trader::strategies;

namespace {

[[nodiscard]] std::vector<Price> walk_prices(const ReferencePriceWalkConfig& config, int steps) {
    ReferencePriceWalk walk(config);
    std::vector<Price> prices;
    prices.reserve(static_cast<std::size_t>(steps));
    for (int i = 0; i < steps; ++i) {
        prices.push_back(walk.step());
    }
    return prices;
}

} // namespace

TEST(ReferencePriceWalk, StartsAtTheConfiguredInitialPriceBeforeAnyStep) {
    ReferencePriceWalk walk(ReferencePriceWalkConfig{.initial_price = 1'000'000});

    EXPECT_EQ(walk.price(), 1'000'000);
    EXPECT_EQ(walk.steps_taken(), 0u);
}

TEST(ReferencePriceWalk, TheSameSeedProducesTheSameSequence) {
    const ReferencePriceWalkConfig config{.seed = 12'345};

    EXPECT_EQ(walk_prices(config, 200), walk_prices(config, 200));
}

TEST(ReferencePriceWalk, DifferentSeedsProduceDifferentSequences) {
    auto a = walk_prices(ReferencePriceWalkConfig{.seed = 1}, 200);
    auto b = walk_prices(ReferencePriceWalkConfig{.seed = 2}, 200);

    EXPECT_NE(a, b);
}

TEST(ReferencePriceWalk, EachStepMovesByAtMostOneStepSize) {
    const ReferencePriceWalkConfig config{.step_size = 100, .seed = 7};
    ReferencePriceWalk walk(config);

    Price previous = walk.price();
    for (int i = 0; i < 500; ++i) {
        const Price next = walk.step();
        EXPECT_LE(std::abs(next - previous), config.step_size);
        previous = next;
    }
    EXPECT_EQ(walk.steps_taken(), 500u);
}

TEST(ReferencePriceWalk, NeverLeavesTheConfiguredBounds) {
    // Bounds only one step wide either side of the start, so a 2,000-step
    // walk is certain to press against both of them.
    const ReferencePriceWalkConfig config{
        .initial_price = 1'000, .step_size = 100, .min_price = 900, .max_price = 1'100, .seed = 99};
    ReferencePriceWalk walk(config);

    for (int i = 0; i < 2'000; ++i) {
        const Price price = walk.step();
        EXPECT_GE(price, config.min_price);
        EXPECT_LE(price, config.max_price);
    }
}

TEST(ReferencePriceWalk, AZeroStepSizeNeverMoves) {
    // Not a degenerate case to be guarded against but the tool the ladder
    // market maker's own unit tests use to pin exact quote prices without
    // reproducing the walk -- see test_ladder_market_maker.cpp.
    ReferencePriceWalk walk(ReferencePriceWalkConfig{.initial_price = 1'000'000, .step_size = 0});

    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(walk.step(), 1'000'000);
    }
}
