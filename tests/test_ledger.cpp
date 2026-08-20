#include <gtest/gtest.h>

#include "exchange/ledger/ledger.hpp"
#include "exchange/matching/matching_engine.hpp"

namespace mdh::exchange::ledger {
namespace {

constexpr InstrumentId kInstrument = 1;
constexpr AccountId kBuyer = 100;
constexpr AccountId kSeller = 200;

} // namespace

TEST(Ledger, UnknownAccountHasZeroBalances) {
    Ledger ledger;
    EXPECT_EQ(ledger.available_cash(kBuyer), 0);
    EXPECT_EQ(ledger.available_position(kBuyer, kInstrument), 0u);
    const auto balances = ledger.balances(kBuyer);
    EXPECT_EQ(balances.cash_total, 0);
    EXPECT_EQ(balances.cash_reserved, 0);
}

TEST(Ledger, DepositsAreImmediatelyAvailable) {
    Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);
    ledger.deposit_position(kSeller, kInstrument, 50);

    EXPECT_EQ(ledger.available_cash(kBuyer), 10'000);
    EXPECT_EQ(ledger.available_position(kSeller, kInstrument), 50u);
}

TEST(Ledger, GtcBuyOrderAcceptedReservesCashAtLimitPrice) {
    Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);

    ledger.apply(OrderAccepted{.event_sequence = 1,
                                .command_sequence = 1,
                                .account_id = kBuyer,
                                .client_order_id = 1,
                                .exchange_order_id = 1,
                                .instrument_id = kInstrument,
                                .side = Side::Buy,
                                .price = 100,
                                .quantity = 10,
                                .order_type = OrderType::Limit,
                                .time_in_force = TimeInForce::GTC});

    EXPECT_EQ(ledger.available_cash(kBuyer), 10'000 - 1'000); // 100 * 10 reserved
    EXPECT_EQ(ledger.balances(kBuyer).cash_total, 10'000);    // total is untouched by a reservation
    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 1'000);
}

TEST(Ledger, GtcSellOrderAcceptedReservesPosition) {
    Ledger ledger;
    ledger.deposit_position(kSeller, kInstrument, 50);

    ledger.apply(OrderAccepted{.event_sequence = 1,
                                .command_sequence = 1,
                                .account_id = kSeller,
                                .client_order_id = 1,
                                .exchange_order_id = 1,
                                .instrument_id = kInstrument,
                                .side = Side::Sell,
                                .price = 100,
                                .quantity = 20,
                                .order_type = OrderType::Limit,
                                .time_in_force = TimeInForce::GTC});

    EXPECT_EQ(ledger.available_position(kSeller, kInstrument), 50u - 20u);
    EXPECT_EQ(ledger.balances(kSeller).position_total.at(kInstrument), 50u);
}

TEST(Ledger, IocAndFokOrdersAcceptedNeverReserveAnything) {
    Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);

    for (const auto tif : {TimeInForce::IOC, TimeInForce::FOK}) {
        ledger.apply(OrderAccepted{.event_sequence = 1,
                                    .command_sequence = 1,
                                    .account_id = kBuyer,
                                    .client_order_id = 1,
                                    .exchange_order_id = 1,
                                    .instrument_id = kInstrument,
                                    .side = Side::Buy,
                                    .price = 100,
                                    .quantity = 10,
                                    .order_type = OrderType::Limit,
                                    .time_in_force = tif});
        EXPECT_EQ(ledger.available_cash(kBuyer), 10'000);
        EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 0);
    }
}

TEST(Ledger, TradeSettlesRestingGtcBuyerReleasingItsOwnLimitPriceReservation) {
    Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);
    ledger.apply(OrderAccepted{.event_sequence = 1,
                                .command_sequence = 1,
                                .account_id = kBuyer,
                                .client_order_id = 1,
                                .exchange_order_id = 1,
                                .instrument_id = kInstrument,
                                .side = Side::Buy,
                                .price = 100,
                                .quantity = 10,
                                .order_type = OrderType::Limit,
                                .time_in_force = TimeInForce::GTC});

    // Resting buyer trades at its own limit price (100) -- always true for
    // the passive side, see ledger.hpp's class comment.
    ledger.apply(TradeExecuted{.event_sequence = 2,
                                .command_sequence = 2,
                                .instrument_id = kInstrument,
                                .price = 100,
                                .quantity = 10,
                                .aggressor_side = Side::Sell,
                                .buyer = TradeCounterparty{.account_id = kBuyer, .client_order_id = 1, .exchange_order_id = 1, .remaining_quantity = 0},
                                .seller = TradeCounterparty{.account_id = kSeller, .client_order_id = 9, .exchange_order_id = 9, .remaining_quantity = 0}});

    const auto buyer_balances = ledger.balances(kBuyer);
    EXPECT_EQ(buyer_balances.cash_total, 10'000 - 1'000); // fully spent at trade price
    EXPECT_EQ(buyer_balances.cash_reserved, 0);           // hold fully released once remaining hit 0
    EXPECT_EQ(buyer_balances.position_total.at(kInstrument), 10u);
    EXPECT_EQ(ledger.available_cash(kBuyer), 9'000);
}

TEST(Ledger, TradeWithPriceImprovementRefundsDifferenceToAvailableForAggressor) {
    Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);
    // Aggressor buyer's own limit is 105, but it will trade against a
    // resting ask at 100 -- trades always execute at the resting price.
    ledger.apply(OrderAccepted{.event_sequence = 1,
                                .command_sequence = 1,
                                .account_id = kBuyer,
                                .client_order_id = 1,
                                .exchange_order_id = 1,
                                .instrument_id = kInstrument,
                                .side = Side::Buy,
                                .price = 105,
                                .quantity = 10,
                                .order_type = OrderType::Limit,
                                .time_in_force = TimeInForce::GTC});
    ASSERT_EQ(ledger.available_cash(kBuyer), 10'000 - 1'050); // reserved at its own limit price

    ledger.apply(TradeExecuted{.event_sequence = 2,
                                .command_sequence = 1,
                                .instrument_id = kInstrument,
                                .price = 100, // the resting seller's (better) price
                                .quantity = 10,
                                .aggressor_side = Side::Buy,
                                .buyer = TradeCounterparty{.account_id = kBuyer, .client_order_id = 1, .exchange_order_id = 1, .remaining_quantity = 0},
                                .seller = TradeCounterparty{.account_id = kSeller, .client_order_id = 9, .exchange_order_id = 9, .remaining_quantity = 0}});

    const auto buyer_balances = ledger.balances(kBuyer);
    EXPECT_EQ(buyer_balances.cash_reserved, 0);
    EXPECT_EQ(buyer_balances.cash_total, 10'000 - 1'000); // only actually debited at the trade price (100), not the limit (105)
    EXPECT_EQ(ledger.available_cash(kBuyer), 9'000);      // the 50-per-unit improvement is back in `available`
}

TEST(Ledger, TradeForNonHeldIocFokLegSettlesDirectlyWithoutTouchingReserved) {
    Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);
    // No OrderAccepted applied at all here -- exactly what an IOC/FOK leg
    // looks like from the ledger's perspective: never held (see
    // ledger.hpp's class comment), so settlement must still work correctly
    // from a trade alone.
    ledger.apply(TradeExecuted{.event_sequence = 1,
                                .command_sequence = 1,
                                .instrument_id = kInstrument,
                                .price = 100,
                                .quantity = 10,
                                .aggressor_side = Side::Buy,
                                .buyer = TradeCounterparty{.account_id = kBuyer, .client_order_id = 1, .exchange_order_id = 1, .remaining_quantity = 0},
                                .seller = TradeCounterparty{.account_id = kSeller, .client_order_id = 9, .exchange_order_id = 9, .remaining_quantity = 0}});

    EXPECT_EQ(ledger.balances(kBuyer).cash_total, 10'000 - 1'000);
    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 0);
    EXPECT_EQ(ledger.balances(kBuyer).position_total.at(kInstrument), 10u);
}

TEST(Ledger, PartialFillLeavesTheRemainderStillHeld) {
    Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);
    ledger.apply(OrderAccepted{.event_sequence = 1,
                                .command_sequence = 1,
                                .account_id = kBuyer,
                                .client_order_id = 1,
                                .exchange_order_id = 1,
                                .instrument_id = kInstrument,
                                .side = Side::Buy,
                                .price = 100,
                                .quantity = 10,
                                .order_type = OrderType::Limit,
                                .time_in_force = TimeInForce::GTC});

    ledger.apply(TradeExecuted{.event_sequence = 2,
                                .command_sequence = 2,
                                .instrument_id = kInstrument,
                                .price = 100,
                                .quantity = 4, // only 4 of the 10 fill
                                .aggressor_side = Side::Sell,
                                .buyer = TradeCounterparty{.account_id = kBuyer, .client_order_id = 1, .exchange_order_id = 1, .remaining_quantity = 6},
                                .seller = TradeCounterparty{.account_id = kSeller, .client_order_id = 9, .exchange_order_id = 9, .remaining_quantity = 0}});

    const auto balances = ledger.balances(kBuyer);
    EXPECT_EQ(balances.cash_total, 10'000 - 400);
    EXPECT_EQ(balances.cash_reserved, 600); // still holding the unfilled 6 @ 100
    EXPECT_EQ(ledger.available_cash(kBuyer), 10'000 - 400 - 600);
}

TEST(Ledger, OrderCancelledReleasesWhateverRemainsOfTheHold) {
    Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);
    ledger.apply(OrderAccepted{.event_sequence = 1,
                                .command_sequence = 1,
                                .account_id = kBuyer,
                                .client_order_id = 1,
                                .exchange_order_id = 1,
                                .instrument_id = kInstrument,
                                .side = Side::Buy,
                                .price = 100,
                                .quantity = 10,
                                .order_type = OrderType::Limit,
                                .time_in_force = TimeInForce::GTC});
    ASSERT_EQ(ledger.balances(kBuyer).cash_reserved, 1'000);

    ledger.apply(OrderCancelled{.event_sequence = 2,
                                 .command_sequence = 2,
                                 .account_id = kBuyer,
                                 .client_order_id = 1,
                                 .exchange_order_id = 1,
                                 .instrument_id = kInstrument});

    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 0);
    EXPECT_EQ(ledger.available_cash(kBuyer), 10'000); // untouched -- nothing ever traded
}

TEST(Ledger, ReplacePriorityPreservingQuantityDecreaseShrinksTheHold) {
    Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);
    ledger.apply(OrderAccepted{.event_sequence = 1,
                                .command_sequence = 1,
                                .account_id = kBuyer,
                                .client_order_id = 1,
                                .exchange_order_id = 1,
                                .instrument_id = kInstrument,
                                .side = Side::Buy,
                                .price = 100,
                                .quantity = 10,
                                .order_type = OrderType::Limit,
                                .time_in_force = TimeInForce::GTC});

    // Priority-preserving path: same price, new client_order_id, same
    // exchange_order_id (see MatchingEngine::process_replace).
    ledger.apply(OrderReplaced{.event_sequence = 2,
                                .command_sequence = 2,
                                .account_id = kBuyer,
                                .original_client_order_id = 1,
                                .new_client_order_id = 2,
                                .exchange_order_id = 1,
                                .instrument_id = kInstrument,
                                .new_price = 100,
                                .new_quantity = 4});

    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 400);
    EXPECT_EQ(ledger.available_cash(kBuyer), 10'000 - 400);

    // The hold now lives under the *new* client_order_id -- releasing via
    // the old id must no longer find anything.
    ledger.apply(OrderCancelled{.event_sequence = 3,
                                 .command_sequence = 3,
                                 .account_id = kBuyer,
                                 .client_order_id = 1,
                                 .exchange_order_id = 1,
                                 .instrument_id = kInstrument});
    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 400); // unaffected: no hold existed under the old id anymore

    ledger.apply(OrderCancelled{.event_sequence = 4,
                                 .command_sequence = 4,
                                 .account_id = kBuyer,
                                 .client_order_id = 2,
                                 .exchange_order_id = 1,
                                 .instrument_id = kInstrument});
    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 0);
}

TEST(Ledger, ReplaceCancelPlusNewMovesTheHoldToTheNewPriceAndQuantity) {
    Ledger ledger;
    ledger.deposit_cash(kBuyer, 10'000);
    ledger.apply(OrderAccepted{.event_sequence = 1,
                                .command_sequence = 1,
                                .account_id = kBuyer,
                                .client_order_id = 1,
                                .exchange_order_id = 1,
                                .instrument_id = kInstrument,
                                .side = Side::Buy,
                                .price = 100,
                                .quantity = 10,
                                .order_type = OrderType::Limit,
                                .time_in_force = TimeInForce::GTC});
    ASSERT_EQ(ledger.balances(kBuyer).cash_reserved, 1'000);

    // Cancel-plus-new path: a brand new exchange_order_id, a repriced order.
    ledger.apply(OrderReplaced{.event_sequence = 2,
                                .command_sequence = 2,
                                .account_id = kBuyer,
                                .original_client_order_id = 1,
                                .new_client_order_id = 2,
                                .exchange_order_id = 42,
                                .instrument_id = kInstrument,
                                .new_price = 110,
                                .new_quantity = 5});

    EXPECT_EQ(ledger.balances(kBuyer).cash_reserved, 550); // 110 * 5, not the old 100 * 10
    EXPECT_EQ(ledger.available_cash(kBuyer), 10'000 - 550);
}

TEST(Ledger, EndToEndCrossingOrdersConservesValueAcrossBothAccounts) {
    MatchingEngine engine{kInstrument};
    Ledger ledger;
    ledger.deposit_cash(kBuyer, 100'000);
    ledger.deposit_position(kSeller, kInstrument, 100);

    const EventSink sink = ledger.sink();

    // Resting sell first.
    engine.process(NewOrderCommand{.command_sequence = 1,
                                    .account_id = kSeller,
                                    .client_order_id = 1,
                                    .instrument_id = kInstrument,
                                    .side = Side::Sell,
                                    .price = 100,
                                    .quantity = 10,
                                    .order_type = OrderType::Limit,
                                    .time_in_force = TimeInForce::GTC},
                   sink);
    // Crossing (aggressive) buy, priced better than it needs to be.
    engine.process(NewOrderCommand{.command_sequence = 2,
                                    .account_id = kBuyer,
                                    .client_order_id = 1,
                                    .instrument_id = kInstrument,
                                    .side = Side::Buy,
                                    .price = 110,
                                    .quantity = 10,
                                    .order_type = OrderType::Limit,
                                    .time_in_force = TimeInForce::GTC},
                   sink);

    const auto buyer_balances = ledger.balances(kBuyer);
    const auto seller_balances = ledger.balances(kSeller);

    EXPECT_EQ(buyer_balances.cash_reserved, 0);
    EXPECT_EQ(seller_balances.position_reserved.at(kInstrument), 0u);

    // Value conservation: whatever cash left the buyer's account is exactly
    // what arrived in the seller's, and vice versa for the instrument --
    // trades neither create nor destroy value.
    EXPECT_EQ(100'000 - buyer_balances.cash_total, seller_balances.cash_total);
    EXPECT_EQ(100u - seller_balances.position_total.at(kInstrument), buyer_balances.position_total.at(kInstrument));

    // Both settled at the resting seller's price (100), not the aggressor's
    // limit (110) -- the buyer's price improvement is reflected in
    // `available`, already asserted to be fully un-reserved above.
    EXPECT_EQ(buyer_balances.cash_total, 100'000 - 1'000);
    EXPECT_EQ(seller_balances.cash_total, 1'000);
}

} // namespace mdh::exchange::ledger
