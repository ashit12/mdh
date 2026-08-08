#include <gtest/gtest.h>

#include <vector>

#include "exchange/market_data/market_data_publisher.hpp"

// This is the spec MarketDataPublisher::publish() (and its four private
// helpers) needs to satisfy. It's written and complete -- nothing here
// needs filling in. Build and run it as you go
// (`ctest --test-dir build -R MarketDataPublisher --output-on-failure`) to
// see exactly which cases still fail while you implement the translation
// logic in src/exchange/market_data/market_data_publisher.cpp.
namespace mdh::exchange::market_data {
namespace {

constexpr InstrumentId kInstrument = 1;
constexpr AccountId kBuyer = 100;
constexpr AccountId kSeller = 200;
constexpr Timestamp kFixedTimestamp = 42'000'000'000ULL;

MarketDataPublisher make_publisher_with_fixed_clock() {
    return MarketDataPublisher(MarketDataPublisherOptions{.clock = [] { return kFixedTimestamp; }});
}

std::vector<protocol::Event> publish_all(MarketDataPublisher& publisher, const std::vector<ExchangeEvent>& events) {
    std::vector<protocol::Event> out;
    for (const auto& event : events) {
        publisher.publish(event, [&](const protocol::Event& wire_event) { out.push_back(wire_event); });
    }
    return out;
}

} // namespace

TEST(MarketDataPublisher, BookOrderAddedTranslatesToAddOrder) {
    auto publisher = make_publisher_with_fixed_clock();

    const auto wire_events = publish_all(publisher, {BookOrderAdded{
                                                          .event_sequence = 1,
                                                          .instrument_id = kInstrument,
                                                          .exchange_order_id = 901,
                                                          .side = Side::Buy,
                                                          .price = 101,
                                                          .quantity = 10,
                                                      }});

    ASSERT_EQ(wire_events.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<protocol::AddOrder>(wire_events[0]));
    const auto& add = std::get<protocol::AddOrder>(wire_events[0]);
    EXPECT_EQ(add.order_id, 901u);
    EXPECT_EQ(add.instrument_id, kInstrument);
    EXPECT_EQ(add.price, 101);
    EXPECT_EQ(add.quantity, 10u);
    EXPECT_EQ(add.side, Side::Buy);
    EXPECT_EQ(add.sequence_number, 1u);
    EXPECT_EQ(add.timestamp_ns, kFixedTimestamp);
}

TEST(MarketDataPublisher, BookOrderReducedTranslatesToModifyOrderWithUnchangedPrice) {
    auto publisher = make_publisher_with_fixed_clock();

    const auto wire_events = publish_all(publisher, {BookOrderReduced{
                                                          .event_sequence = 1,
                                                          .instrument_id = kInstrument,
                                                          .exchange_order_id = 901,
                                                          .side = Side::Sell,
                                                          .price = 105,
                                                          .new_remaining_quantity = 3,
                                                      }});

    ASSERT_EQ(wire_events.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<protocol::ModifyOrder>(wire_events[0]));
    const auto& modify = std::get<protocol::ModifyOrder>(wire_events[0]);
    EXPECT_EQ(modify.order_id, 901u);
    EXPECT_EQ(modify.instrument_id, kInstrument);
    EXPECT_EQ(modify.new_price, 105); // unchanged -- BookOrderReduced never reprices
    EXPECT_EQ(modify.new_quantity, 3u);
}

TEST(MarketDataPublisher, BookOrderRemovedTranslatesToCancelOrder) {
    auto publisher = make_publisher_with_fixed_clock();

    const auto wire_events = publish_all(publisher, {BookOrderRemoved{
                                                          .event_sequence = 1,
                                                          .instrument_id = kInstrument,
                                                          .exchange_order_id = 901,
                                                          .side = Side::Buy,
                                                          .price = 101,
                                                      }});

    ASSERT_EQ(wire_events.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<protocol::CancelOrder>(wire_events[0]));
    const auto& cancel = std::get<protocol::CancelOrder>(wire_events[0]);
    EXPECT_EQ(cancel.order_id, 901u);
    EXPECT_EQ(cancel.instrument_id, kInstrument);
}

TEST(MarketDataPublisher, TradeExecutedTranslatesToTradeAndStripsAccountInfo) {
    auto publisher = make_publisher_with_fixed_clock();

    const auto wire_events = publish_all(publisher, {TradeExecuted{
                                                          .event_sequence = 1,
                                                          .command_sequence = 1,
                                                          .instrument_id = kInstrument,
                                                          .price = 100,
                                                          .quantity = 5,
                                                          .aggressor_side = Side::Buy,
                                                          .buyer = TradeCounterparty{.account_id = kBuyer,
                                                                                      .client_order_id = 1,
                                                                                      .exchange_order_id = 901,
                                                                                      .remaining_quantity = 5},
                                                          .seller = TradeCounterparty{.account_id = kSeller,
                                                                                       .client_order_id = 2,
                                                                                       .exchange_order_id = 700,
                                                                                       .remaining_quantity = 0},
                                                      }});

    ASSERT_EQ(wire_events.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<protocol::Trade>(wire_events[0]));
    const auto& trade = std::get<protocol::Trade>(wire_events[0]);
    // protocol::Trade has no fields for account_id/client_order_id/
    // exchange_order_id at all -- the struct itself makes leaking them
    // impossible as long as only these four fields are populated from it:
    EXPECT_EQ(trade.instrument_id, kInstrument);
    EXPECT_EQ(trade.price, 100);
    EXPECT_EQ(trade.quantity, 5u);
    EXPECT_EQ(trade.aggressor_side, Side::Buy);
}

TEST(MarketDataPublisher, PrivateEventsProduceNoWireMessage) {
    auto publisher = make_publisher_with_fixed_clock();

    const std::vector<ExchangeEvent> private_events = {
        OrderAccepted{.event_sequence = 1,
                      .command_sequence = 1,
                      .account_id = kBuyer,
                      .client_order_id = 1,
                      .exchange_order_id = 901,
                      .instrument_id = kInstrument,
                      .side = Side::Buy,
                      .price = 101,
                      .quantity = 10,
                      .order_type = OrderType::Limit,
                      .time_in_force = TimeInForce::GTC},
        OrderRejected{.event_sequence = 2,
                      .command_sequence = 2,
                      .account_id = kBuyer,
                      .client_order_id = 2,
                      .instrument_id = kInstrument,
                      .reason = RejectReason::InvalidPrice},
        OrderCancelled{.event_sequence = 3,
                       .command_sequence = 3,
                       .account_id = kBuyer,
                       .client_order_id = 1,
                       .exchange_order_id = 901,
                       .instrument_id = kInstrument},
        OrderReplaced{.event_sequence = 4,
                      .command_sequence = 4,
                      .account_id = kBuyer,
                      .original_client_order_id = 1,
                      .new_client_order_id = 3,
                      .exchange_order_id = 902,
                      .instrument_id = kInstrument,
                      .new_price = 102,
                      .new_quantity = 8},
    };

    const auto wire_events = publish_all(publisher, private_events);

    EXPECT_TRUE(wire_events.empty());
}

TEST(MarketDataPublisher, SequenceNumberIsMonotonicAndSkipsPrivateEvents) {
    auto publisher = make_publisher_with_fixed_clock();

    // A private event sandwiched between two publishable ones must not
    // consume a sequence number -- the second AddOrder should still get
    // sequence_number == 2, not 3.
    const auto wire_events = publish_all(publisher, {
                                                          BookOrderAdded{.event_sequence = 1,
                                                                         .instrument_id = kInstrument,
                                                                         .exchange_order_id = 1,
                                                                         .side = Side::Buy,
                                                                         .price = 100,
                                                                         .quantity = 1},
                                                          OrderRejected{.event_sequence = 2,
                                                                        .command_sequence = 2,
                                                                        .account_id = kBuyer,
                                                                        .client_order_id = 2,
                                                                        .instrument_id = kInstrument,
                                                                        .reason = RejectReason::InvalidPrice},
                                                          BookOrderAdded{.event_sequence = 3,
                                                                         .instrument_id = kInstrument,
                                                                         .exchange_order_id = 2,
                                                                         .side = Side::Buy,
                                                                         .price = 100,
                                                                         .quantity = 1},
                                                      });

    ASSERT_EQ(wire_events.size(), 2u);
    EXPECT_EQ(std::get<protocol::AddOrder>(wire_events[0]).sequence_number, 1u);
    EXPECT_EQ(std::get<protocol::AddOrder>(wire_events[1]).sequence_number, 2u);
    EXPECT_EQ(publisher.next_sequence(), 3u);
}

TEST(MarketDataPublisher, SinkBindsDownstreamIntoAnEventSink) {
    auto publisher = make_publisher_with_fixed_clock();
    std::vector<protocol::Event> received;
    const EventSink bound = publisher.sink([&](const protocol::Event& e) { received.push_back(e); });

    bound(BookOrderRemoved{
        .event_sequence = 1, .instrument_id = kInstrument, .exchange_order_id = 5, .side = Side::Sell, .price = 100});

    ASSERT_EQ(received.size(), 1u);
    EXPECT_TRUE(std::holds_alternative<protocol::CancelOrder>(received[0]));
}

TEST(MarketDataPublisher, DefaultClockProducesAPlausibleWallClockTimestamp) {
    MarketDataPublisher publisher; // no clock override -- exercises the real wall-clock default
    std::vector<protocol::Event> wire_events;
    publisher.publish(
        BookOrderAdded{
            .event_sequence = 1, .instrument_id = kInstrument, .exchange_order_id = 1, .side = Side::Buy, .price = 100, .quantity = 1},
        [&](const protocol::Event& e) { wire_events.push_back(e); });

    ASSERT_EQ(wire_events.size(), 1u);
    // Nanoseconds since epoch for 2020-01-01: sanity-checks that this is a
    // real, current-ish wall-clock timestamp, not a zero/uninitialized one.
    constexpr Timestamp kYear2020Ns = 1'577'836'800ULL * 1'000'000'000ULL;
    EXPECT_GT(std::get<protocol::AddOrder>(wire_events[0]).timestamp_ns, kYear2020Ns);
}

} // namespace mdh::exchange::market_data
