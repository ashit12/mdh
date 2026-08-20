#include <gtest/gtest.h>

#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include "exchange/core/event_sink.hpp"
#include "exchange/core/events.hpp"

namespace mdh::exchange {
namespace {

TEST(ExchangeEvents, OrderAcceptedConstructs) {
    OrderAccepted ev{
        .event_sequence = 1,
        .command_sequence = 1,
        .account_id = 100,
        .client_order_id = 42,
        .exchange_order_id = 9001,
        .instrument_id = 7,
        .side = Side::Buy,
        .price = 10000,
        .quantity = 5,
        .order_type = OrderType::Limit,
        .time_in_force = TimeInForce::GTC,
    };
    EXPECT_EQ(ev.exchange_order_id, 9001u);
    EXPECT_EQ(ev.side, Side::Buy);
}

TEST(ExchangeEvents, OrderRejectedCarriesReason) {
    OrderRejected ev{.event_sequence = 1,
                      .command_sequence = 1,
                      .account_id = 100,
                      .client_order_id = 42,
                      .instrument_id = 7,
                      .reason = RejectReason::InvalidPrice};
    EXPECT_EQ(ev.reason, RejectReason::InvalidPrice);
    EXPECT_EQ(to_string(ev.reason), "InvalidPrice");
}

TEST(ExchangeEvents, OrderCancelledConstructs) {
    OrderCancelled ev{.event_sequence = 1,
                       .command_sequence = 1,
                       .account_id = 100,
                       .client_order_id = 42,
                       .exchange_order_id = 9001,
                       .instrument_id = 7};
    EXPECT_EQ(ev.exchange_order_id, 9001u);
}

TEST(ExchangeEvents, OrderReplacedConstructs) {
    OrderReplaced ev{.event_sequence = 1,
                      .command_sequence = 1,
                      .account_id = 100,
                      .original_client_order_id = 42,
                      .new_client_order_id = 43,
                      .exchange_order_id = 9001,
                      .instrument_id = 7,
                      .new_price = 10500,
                      .new_quantity = 3};
    EXPECT_EQ(ev.new_client_order_id, 43u);
    EXPECT_EQ(ev.new_price, 10500);
}

TEST(ExchangeEvents, TradeExecutedCarriesBothCounterparties) {
    TradeExecuted ev{
        .event_sequence = 1,
        .command_sequence = 1,
        .instrument_id = 7,
        .price = 10000,
        .quantity = 5,
        .aggressor_side = Side::Buy,
        .buyer = TradeCounterparty{.account_id = 100, .client_order_id = 42, .exchange_order_id = 9001, .remaining_quantity = 0},
        .seller = TradeCounterparty{.account_id = 200, .client_order_id = 43, .exchange_order_id = 9002, .remaining_quantity = 10},
    };
    EXPECT_EQ(ev.buyer.account_id, 100u);
    EXPECT_EQ(ev.buyer.remaining_quantity, 0u);
    EXPECT_EQ(ev.seller.account_id, 200u);
    EXPECT_EQ(ev.seller.remaining_quantity, 10u);
}

TEST(ExchangeEvents, BookOrderAddedReducedRemovedConstruct) {
    BookOrderAdded added{
        .event_sequence = 1, .instrument_id = 7, .exchange_order_id = 9001, .side = Side::Buy, .price = 10000, .quantity = 5};
    BookOrderReduced reduced{
        .event_sequence = 2, .instrument_id = 7, .exchange_order_id = 9001, .side = Side::Buy, .price = 10000, .new_remaining_quantity = 2};
    BookOrderRemoved removed{.event_sequence = 3, .instrument_id = 7, .exchange_order_id = 9001, .side = Side::Buy, .price = 10000};

    EXPECT_EQ(added.quantity, 5u);
    EXPECT_EQ(reduced.new_remaining_quantity, 2u);
    EXPECT_EQ(removed.exchange_order_id, 9001u);
}

TEST(ExchangeEvents, VariantHoldsEachAlternative) {
    ExchangeEvent e1 = OrderAccepted{};
    ExchangeEvent e2 = OrderRejected{};
    ExchangeEvent e3 = OrderCancelled{};
    ExchangeEvent e4 = OrderReplaced{};
    ExchangeEvent e5 = TradeExecuted{};
    ExchangeEvent e6 = BookOrderAdded{};
    ExchangeEvent e7 = BookOrderReduced{};
    ExchangeEvent e8 = BookOrderRemoved{};

    EXPECT_TRUE(std::holds_alternative<OrderAccepted>(e1));
    EXPECT_TRUE(std::holds_alternative<OrderRejected>(e2));
    EXPECT_TRUE(std::holds_alternative<OrderCancelled>(e3));
    EXPECT_TRUE(std::holds_alternative<OrderReplaced>(e4));
    EXPECT_TRUE(std::holds_alternative<TradeExecuted>(e5));
    EXPECT_TRUE(std::holds_alternative<BookOrderAdded>(e6));
    EXPECT_TRUE(std::holds_alternative<BookOrderReduced>(e7));
    EXPECT_TRUE(std::holds_alternative<BookOrderRemoved>(e8));
}

TEST(ExchangeEvents, VariantVisitationDispatchesToCorrectAlternative) {
    std::vector<ExchangeEvent> events;
    events.push_back(OrderAccepted{});
    events.push_back(TradeExecuted{});
    events.push_back(BookOrderRemoved{});

    std::vector<std::string> kinds;
    for (const auto& ev : events) {
        std::visit(
            [&](const auto& e) {
                using T = std::decay_t<decltype(e)>;
                if constexpr (std::is_same_v<T, OrderAccepted>) {
                    kinds.emplace_back("Accepted");
                } else if constexpr (std::is_same_v<T, TradeExecuted>) {
                    kinds.emplace_back("Trade");
                } else if constexpr (std::is_same_v<T, BookOrderRemoved>) {
                    kinds.emplace_back("Removed");
                } else {
                    kinds.emplace_back("Other");
                }
            },
            ev);
    }
    EXPECT_EQ(kinds, (std::vector<std::string>{"Accepted", "Trade", "Removed"}));
}

TEST(ExchangeEvents, RejectReasonToStringCoversEveryEnumerator) {
    EXPECT_EQ(to_string(RejectReason::None), "None");
    EXPECT_EQ(to_string(RejectReason::InvalidPrice), "InvalidPrice");
    EXPECT_EQ(to_string(RejectReason::InvalidQuantity), "InvalidQuantity");
    EXPECT_EQ(to_string(RejectReason::DuplicateOrderId), "DuplicateOrderId");
    EXPECT_EQ(to_string(RejectReason::UnknownOrderId), "UnknownOrderId");
    EXPECT_EQ(to_string(RejectReason::InvalidInstrument), "InvalidInstrument");
    EXPECT_EQ(to_string(RejectReason::InsufficientLiquidity), "InsufficientLiquidity");
    EXPECT_EQ(to_string(RejectReason::InvalidReplacement), "InvalidReplacement");
    EXPECT_EQ(to_string(RejectReason::InternalError), "InternalError");
    EXPECT_EQ(to_string(RejectReason::InsufficientFunds), "InsufficientFunds");
    EXPECT_EQ(to_string(RejectReason::InsufficientPosition), "InsufficientPosition");
    EXPECT_EQ(to_string(RejectReason::OrderTooLarge), "OrderTooLarge");
    EXPECT_EQ(to_string(RejectReason::AccountMismatch), "AccountMismatch");
}

TEST(ExchangeEvents, OrderTypeAndTimeInForceToString) {
    EXPECT_EQ(to_string(OrderType::Limit), "Limit");
    EXPECT_EQ(to_string(TimeInForce::GTC), "GTC");
    EXPECT_EQ(to_string(TimeInForce::IOC), "IOC");
    EXPECT_EQ(to_string(TimeInForce::FOK), "FOK");
}

TEST(EventSink, CollectsMultipleEventsEmittedForOneCommand) {
    std::vector<ExchangeEvent> collected;
    EventSink sink = [&](const ExchangeEvent& ev) { collected.push_back(ev); };

    // Deliberately does not involve a MatchingEngine. This simulates what
    // the engine does for one crossing NewOrderCommand -- emit an
    // acceptance, a trade, and a book reduction, in order -- to prove the
    // EventSink shape itself (a plain callable) is sufficient for a test to
    // collect an ordered sequence of heterogeneous events from one logical
    // operation.
    sink(OrderAccepted{.event_sequence = 1});
    sink(TradeExecuted{.event_sequence = 2});
    sink(BookOrderReduced{.event_sequence = 3});

    ASSERT_EQ(collected.size(), 3u);
    EXPECT_TRUE(std::holds_alternative<OrderAccepted>(collected[0]));
    EXPECT_TRUE(std::holds_alternative<TradeExecuted>(collected[1]));
    EXPECT_TRUE(std::holds_alternative<BookOrderReduced>(collected[2]));
}

} // namespace
} // namespace mdh::exchange
