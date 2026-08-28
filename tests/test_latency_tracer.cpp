#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "exchange/gateway/order_entry_gateway.hpp"
#include "exchange/latency/latency_tracer.hpp"
#include "protocol/order_entry/messages.hpp"
#include "trader/oms/order_entry_client.hpp"

// Instrumentation correctness for the order-path tracer: correlation,
// accepted/rejected/resting/crossing, multiple reports, and the disabled
// path. Behaviour is still the real TCP gateway + OrderEntryClient.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::gateway;
using namespace mdh::protocol::order_entry;
using namespace mdh::trader::oms;
using namespace std::chrono_literals;

namespace {

constexpr InstrumentId kInstrument = 42;

class MessageCollector {
public:
    void operator()(const Message& message) {
        std::lock_guard<std::mutex> lock(mutex_);
        messages_.push_back(message);
        cv_.notify_all();
    }

    [[nodiscard]] bool wait_for_count(std::size_t count, std::chrono::milliseconds timeout = 2000ms) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return messages_.size() >= count; });
    }

    [[nodiscard]] std::vector<Message> messages() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return messages_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<Message> messages_;
};

[[nodiscard]] bool wait_for_t5(AccountId account_id, ClientOrderId client_order_id,
                               std::chrono::milliseconds timeout = 2000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto snap = latency::tracer().snapshot(account_id, client_order_id);
        if (snap && snap->t5_client_first != 0) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    auto snap = latency::tracer().snapshot(account_id, client_order_id);
    return snap && snap->t5_client_first != 0;
}

NewOrder buy(AccountId account, ClientOrderId id, Price price, Quantity qty, TimeInForce tif) {
    return NewOrder{
        .account_id = account,
        .client_order_id = id,
        .instrument_id = kInstrument,
        .side = Side::Buy,
        .price = price,
        .quantity = qty,
        .order_type = OrderType::Limit,
        .time_in_force = tif,
    };
}

NewOrder sell(AccountId account, ClientOrderId id, Price price, Quantity qty, TimeInForce tif) {
    return NewOrder{
        .account_id = account,
        .client_order_id = id,
        .instrument_id = kInstrument,
        .side = Side::Sell,
        .price = price,
        .quantity = qty,
        .order_type = OrderType::Limit,
        .time_in_force = tif,
    };
}

} // namespace

TEST(LatencyTracer, DisabledPathLeavesNoSnapshot) {
    ASSERT_FALSE(latency::tracer().enabled());

    OrderEntryGateway gateway(0, OrderEntryGatewayOptions{.instruments = {kInstrument}});
    ASSERT_TRUE(gateway.start());
    gateway.deposit_cash(1, 1'000'000);

    MessageCollector collector;
    OrderEntryClient client([&](const Message& m) { collector(m); });
    ASSERT_TRUE(client.connect("127.0.0.1", *gateway.local_port()));
    ASSERT_TRUE(client.send(Message{buy(1, 1, 100, 1, TimeInForce::IOC)}));
    ASSERT_TRUE(collector.wait_for_count(1));

    EXPECT_FALSE(latency::tracer().snapshot(1, 1).has_value());
    gateway.stop();
}

TEST(LatencyTracer, CorrelatesSubmitWithAcceptedReport) {
    latency::ScopedEnable tracing;
    OrderEntryGateway gateway(0, OrderEntryGatewayOptions{.instruments = {kInstrument}});
    ASSERT_TRUE(gateway.start());
    gateway.deposit_cash(1, 1'000'000);

    MessageCollector collector;
    OrderEntryClient client([&](const Message& m) { collector(m); });
    ASSERT_TRUE(client.connect("127.0.0.1", *gateway.local_port()));
    ASSERT_TRUE(client.send(Message{buy(1, 7, 100, 1, TimeInForce::IOC)}));
    ASSERT_TRUE(wait_for_t5(1, 7));
    ASSERT_TRUE(collector.wait_for_count(1));

    const auto snap = latency::tracer().snapshot(1, 7);
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->client_order_id, 7u);
    EXPECT_NE(snap->t0_client_submit, 0u);
    EXPECT_NE(snap->t1_server_decoded, 0u);
    EXPECT_NE(snap->t2_exchange_begin, 0u);
    EXPECT_NE(snap->t3_first_event, 0u);
    EXPECT_NE(snap->t3_exchange_end, 0u);
    EXPECT_NE(snap->t4_writer_queued, 0u);
    EXPECT_NE(snap->t4_socket_written, 0u);
    EXPECT_NE(snap->t5_client_first, 0u);
    EXPECT_GE(snap->t1_server_decoded, snap->t0_client_submit);
    EXPECT_GE(snap->t2_exchange_begin, snap->t1_server_decoded);
    EXPECT_GE(snap->t3_exchange_end, snap->t2_exchange_begin);
    EXPECT_GE(snap->t4_socket_written, snap->t4_writer_queued);
    EXPECT_GE(snap->t5_client_first, snap->t4_socket_written);
    EXPECT_EQ(snap->first_report_type, MessageType::Accepted);
    EXPECT_EQ(snap->reports_decoded, 1u);
    EXPECT_TRUE(std::holds_alternative<Accepted>(collector.messages().front()));
}

TEST(LatencyTracer, MeasuresRejectedOrder) {
    latency::ScopedEnable tracing;
    OrderEntryGateway gateway(0, OrderEntryGatewayOptions{.instruments = {kInstrument}});
    ASSERT_TRUE(gateway.start());
    // No cash -- risk rejects the buy.

    MessageCollector collector;
    OrderEntryClient client([&](const Message& m) { collector(m); });
    ASSERT_TRUE(client.connect("127.0.0.1", *gateway.local_port()));
    ASSERT_TRUE(client.send(Message{buy(1, 3, 100, 1, TimeInForce::IOC)}));
    ASSERT_TRUE(wait_for_t5(1, 3));
    ASSERT_TRUE(collector.wait_for_count(1));

    const auto snap = latency::tracer().snapshot(1, 3);
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->first_report_type, MessageType::Rejected);
    EXPECT_TRUE(std::holds_alternative<Rejected>(collector.messages().front()));
    EXPECT_NE(snap->t5_client_first, 0u);
    EXPECT_NE(snap->t2_exchange_begin, 0u);
}

TEST(LatencyTracer, RestingOrderAcknowledgment) {
    latency::ScopedEnable tracing;
    OrderEntryGateway gateway(0, OrderEntryGatewayOptions{.instruments = {kInstrument}});
    ASSERT_TRUE(gateway.start());
    gateway.deposit_cash(1, 1'000'000);

    MessageCollector collector;
    OrderEntryClient client([&](const Message& m) { collector(m); });
    ASSERT_TRUE(client.connect("127.0.0.1", *gateway.local_port()));
    ASSERT_TRUE(client.send(Message{buy(1, 11, 100, 5, TimeInForce::GTC)}));
    ASSERT_TRUE(wait_for_t5(1, 11));

    const auto snap = latency::tracer().snapshot(1, 11);
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->first_report_type, MessageType::Accepted);
    EXPECT_EQ(snap->reports_decoded, 1u);
    EXPECT_EQ(snap->t5_client_first, snap->t5_client_last);
}

TEST(LatencyTracer, CrossingOrderProducesMultipleReportsForAggressor) {
    latency::ScopedEnable tracing;
    OrderEntryGateway gateway(0, OrderEntryGatewayOptions{.instruments = {kInstrument}});
    ASSERT_TRUE(gateway.start());
    gateway.deposit_cash(1, 1'000'000);
    gateway.deposit_position(2, kInstrument, 100);

    MessageCollector seller_col;
    OrderEntryClient seller([&](const Message& m) { seller_col(m); });
    ASSERT_TRUE(seller.connect("127.0.0.1", *gateway.local_port()));
    ASSERT_TRUE(seller.send(Message{sell(2, 1, 100, 1, TimeInForce::GTC)}));
    ASSERT_TRUE(seller_col.wait_for_count(1));

    MessageCollector buyer_col;
    OrderEntryClient buyer([&](const Message& m) { buyer_col(m); });
    ASSERT_TRUE(buyer.connect("127.0.0.1", *gateway.local_port()));
    ASSERT_TRUE(buyer.send(Message{buy(1, 50, 100, 1, TimeInForce::IOC)}));
    ASSERT_TRUE(wait_for_t5(1, 50));
    ASSERT_TRUE(buyer_col.wait_for_count(2)); // Accepted + TradeReport

    const auto snap = latency::tracer().snapshot(1, 50);
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->first_report_type, MessageType::Accepted);
    EXPECT_GE(snap->reports_generated, 2u);
    EXPECT_GE(snap->reports_decoded, 2u);
    EXPECT_NE(snap->t5_client_last, 0u);
    EXPECT_GE(snap->t5_client_last, snap->t5_client_first);

    const auto msgs = buyer_col.messages();
    EXPECT_TRUE(std::holds_alternative<Accepted>(msgs[0]));
    EXPECT_TRUE(std::holds_alternative<TradeReport>(msgs[1]));
}

TEST(LatencyTracer, DoesNotChangeAcceptedWireBehaviour) {
    OrderEntryGateway gateway(0, OrderEntryGatewayOptions{.instruments = {kInstrument}});
    ASSERT_TRUE(gateway.start());
    gateway.deposit_cash(1, 1'000'000);

    MessageCollector off;
    OrderEntryClient client_off([&](const Message& m) { off(m); });
    ASSERT_TRUE(client_off.connect("127.0.0.1", *gateway.local_port()));
    ASSERT_TRUE(client_off.send(Message{buy(1, 1, 50, 2, TimeInForce::IOC)}));
    ASSERT_TRUE(off.wait_for_count(1));
    const auto without_tracing = off.messages().front();

    latency::ScopedEnable tracing;
    MessageCollector on;
    OrderEntryClient client_on([&](const Message& m) { on(m); });
    ASSERT_TRUE(client_on.connect("127.0.0.1", *gateway.local_port()));
    ASSERT_TRUE(client_on.send(Message{buy(1, 2, 50, 2, TimeInForce::IOC)}));
    ASSERT_TRUE(on.wait_for_count(1));
    const auto with_tracing = on.messages().front();

    ASSERT_TRUE(std::holds_alternative<Accepted>(without_tracing));
    ASSERT_TRUE(std::holds_alternative<Accepted>(with_tracing));
    auto a = std::get<Accepted>(without_tracing);
    auto b = std::get<Accepted>(with_tracing);
    EXPECT_EQ(a.account_id, b.account_id);
    EXPECT_EQ(a.instrument_id, b.instrument_id);
    EXPECT_EQ(a.side, b.side);
    EXPECT_EQ(a.price, b.price);
    EXPECT_EQ(a.quantity, b.quantity);
    EXPECT_EQ(a.order_type, b.order_type);
    EXPECT_EQ(a.time_in_force, b.time_in_force);
}
