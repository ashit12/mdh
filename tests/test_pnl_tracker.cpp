#include <gtest/gtest.h>

#include "trader/positions/pnl_tracker.hpp"

// Unit tests for PnlTracker's average-cost accounting. Driven by
// synthesized oms::Fill values directly -- the same "feed the sink, not the
// socket" approach test_position_tracker.cpp uses, since a Fill is the whole
// input contract this class has.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::trader::oms;
using namespace mdh::trader::positions;

namespace {

constexpr AccountId kAccount = 1;
constexpr InstrumentId kInstrument = 7;

[[nodiscard]] Fill fill(Side side, Price price, Quantity quantity, InstrumentId instrument_id = kInstrument,
                         AccountId account_id = kAccount) {
    return Fill{.account_id = account_id,
                 .client_order_id = 1,
                 .instrument_id = instrument_id,
                 .side = side,
                 .price = price,
                 .quantity = quantity};
}

} // namespace

TEST(PnlTracker, AnAccountThatHasNeverTradedIsFlatWithNoPnl) {
    PnlTracker pnl;

    const auto snap = pnl.snapshot(kAccount, kInstrument, /*mark=*/1'000);

    EXPECT_EQ(snap.position, 0);
    EXPECT_EQ(snap.average_entry_price, 0);
    EXPECT_EQ(snap.realized, 0);
    EXPECT_EQ(snap.unrealized, 0);
    EXPECT_EQ(snap.total, 0);
    EXPECT_EQ(snap.fill_count, 0u);
}

TEST(PnlTracker, SeedingIsNotATradeBecauseThereIsNoWayToSeedThisClassAtAll) {
    // The point being pinned here is a design property, not a computation:
    // unlike PositionTracker, this class has no deposit_*() surface, so a
    // seeded inventory can never appear as an opening position at a
    // fictional entry price. Position starts at 0 and only fills move it.
    PnlTracker pnl;
    pnl.apply(fill(Side::Sell, 1'000, 10));

    // Net short, even though the account would still hold plenty of seeded
    // inventory on the PositionTracker side of the house.
    EXPECT_EQ(pnl.snapshot(kAccount, kInstrument).position, -10);
}

TEST(PnlTracker, BuysAverageTheirEntryPriceAcrossFills) {
    PnlTracker pnl;
    pnl.apply(fill(Side::Buy, 100, 10));
    pnl.apply(fill(Side::Buy, 120, 10));

    const auto snap = pnl.snapshot(kAccount, kInstrument);
    EXPECT_EQ(snap.position, 20);
    EXPECT_EQ(snap.average_entry_price, 110);
    EXPECT_EQ(snap.realized, 0); // nothing closed yet
    EXPECT_EQ(snap.fill_count, 2u);
    EXPECT_EQ(snap.filled_quantity, 20u);
}

TEST(PnlTracker, ARoundTripRealizesTheDifferenceAndLeavesNothingOpen) {
    PnlTracker pnl;
    pnl.apply(fill(Side::Buy, 100, 10));
    pnl.apply(fill(Side::Sell, 105, 10));

    const auto snap = pnl.snapshot(kAccount, kInstrument, /*mark=*/999);
    EXPECT_EQ(snap.position, 0);
    EXPECT_EQ(snap.realized, 50); // 10 * (105 - 100)
    EXPECT_EQ(snap.average_entry_price, 0);
    EXPECT_EQ(snap.unrealized, 0); // flat: the mark price is irrelevant
    EXPECT_EQ(snap.total, 50);
    // Turnover counts both legs, unlike exposure.
    EXPECT_EQ(snap.filled_quantity, 20u);
}

TEST(PnlTracker, AShortRoundTripRealizesAProfitWhenThePriceFalls) {
    PnlTracker pnl;
    pnl.apply(fill(Side::Sell, 100, 10));

    const auto opened = pnl.snapshot(kAccount, kInstrument);
    EXPECT_EQ(opened.position, -10);
    EXPECT_EQ(opened.average_entry_price, 100); // positive, for a short entered at 100

    pnl.apply(fill(Side::Buy, 90, 10));

    const auto snap = pnl.snapshot(kAccount, kInstrument);
    EXPECT_EQ(snap.position, 0);
    EXPECT_EQ(snap.realized, 100); // sold 10 at 100, bought back at 90
}

TEST(PnlTracker, APartialCloseRealizesOnlyTheClosedPartAndLeavesTheEntryPriceIntact) {
    PnlTracker pnl;
    pnl.apply(fill(Side::Buy, 100, 20));
    pnl.apply(fill(Side::Sell, 110, 5));

    const auto snap = pnl.snapshot(kAccount, kInstrument);
    EXPECT_EQ(snap.position, 15);
    EXPECT_EQ(snap.realized, 50);              // 5 * (110 - 100)
    EXPECT_EQ(snap.average_entry_price, 100); // unchanged by a close
}

TEST(PnlTracker, FlippingThroughZeroClosesTheOldPositionAndOpensTheNewAtTheFillPrice) {
    PnlTracker pnl;
    pnl.apply(fill(Side::Buy, 100, 10));
    pnl.apply(fill(Side::Sell, 110, 25)); // closes 10 long, opens 15 short

    const auto snap = pnl.snapshot(kAccount, kInstrument);
    EXPECT_EQ(snap.position, -15);
    EXPECT_EQ(snap.realized, 100);             // 10 * (110 - 100)
    EXPECT_EQ(snap.average_entry_price, 110); // the flip's own price, not the old one
}

TEST(PnlTracker, UnrealizedMarksAnOpenPositionAndIsZeroWithoutAMarkPrice) {
    PnlTracker pnl;
    pnl.apply(fill(Side::Buy, 100, 10));

    EXPECT_EQ(pnl.snapshot(kAccount, kInstrument, /*mark=*/105).unrealized, 50);
    EXPECT_EQ(pnl.snapshot(kAccount, kInstrument, /*mark=*/95).unrealized, -50);
    EXPECT_EQ(pnl.snapshot(kAccount, kInstrument, /*mark=*/std::nullopt).unrealized, 0);
}

TEST(PnlTracker, AShortIsUnrealizedProfitableWhenTheMarkFalls) {
    PnlTracker pnl;
    pnl.apply(fill(Side::Sell, 100, 10));

    EXPECT_EQ(pnl.snapshot(kAccount, kInstrument, /*mark=*/95).unrealized, 50);
    EXPECT_EQ(pnl.snapshot(kAccount, kInstrument, /*mark=*/105).unrealized, -50);
}

TEST(PnlTracker, TotalIsAlwaysRealizedPlusUnrealized) {
    PnlTracker pnl;
    pnl.apply(fill(Side::Buy, 100, 20));
    pnl.apply(fill(Side::Sell, 110, 5)); // realized 50, 15 still open at 100

    const auto snap = pnl.snapshot(kAccount, kInstrument, /*mark=*/120);
    EXPECT_EQ(snap.realized, 50);
    EXPECT_EQ(snap.unrealized, 300); // 15 * (120 - 100)
    EXPECT_EQ(snap.total, snap.realized + snap.unrealized);
}

TEST(PnlTracker, AccountsAndInstrumentsAreTrackedIndependently) {
    constexpr AccountId kOtherAccount = 2;
    constexpr InstrumentId kOtherInstrument = 8;
    PnlTracker pnl;
    pnl.apply(fill(Side::Buy, 100, 10));
    pnl.apply(fill(Side::Sell, 100, 3, kOtherInstrument));
    pnl.apply(fill(Side::Buy, 100, 7, kInstrument, kOtherAccount));

    EXPECT_EQ(pnl.snapshot(kAccount, kInstrument).position, 10);
    EXPECT_EQ(pnl.snapshot(kAccount, kOtherInstrument).position, -3);
    EXPECT_EQ(pnl.snapshot(kOtherAccount, kInstrument).position, 7);
    EXPECT_EQ(pnl.snapshot(kOtherAccount, kOtherInstrument).position, 0);

    EXPECT_EQ(pnl.total_fill_count(), 3u);
    EXPECT_EQ(pnl.total_filled_quantity(), 20u);
}

TEST(PnlTracker, TheSinkIsTheSameEntryPointAsApply) {
    PnlTracker pnl;
    auto sink = pnl.sink();
    sink(fill(Side::Buy, 100, 10));

    EXPECT_EQ(pnl.snapshot(kAccount, kInstrument).position, 10);
}
