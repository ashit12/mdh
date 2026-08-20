#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <span>
#include <thread>
#include <vector>

#include "net/tcp_socket.hpp"
#include "protocol/order_entry/decoder.hpp"
#include "protocol/order_entry/encoder.hpp"
#include "trader/oms/order_entry_client.hpp"

// Transport-level unit tests for OrderEntryClient -- a raw
// net::TcpSocket stands in for the gateway here (same style as
// tests/test_tcp_socket.cpp's own loopback tests), so these pin the
// framing/threading contract in isolation from any real
// OrderEntryGateway/OrderManagementSystem. See tests/test_oms_gateway_e2e.cpp
// for the version proven against the real gateway.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::net;
using namespace mdh::protocol::order_entry;
using namespace mdh::trader::oms;
using namespace std::chrono_literals;

namespace {

constexpr InstrumentId kInstrument = 3;

// A tiny synchronizing collector for messages delivered via
// OrderEntryClient::MessageSink -- the reader thread calls it, the test
// thread waits on it, so a plain std::vector alone (no synchronization)
// would be a data race.
class MessageCollector {
public:
    void operator()(const Message& message) {
        std::lock_guard<std::mutex> lock(mutex_);
        messages_.push_back(message);
        cv_.notify_all();
    }

    [[nodiscard]] bool wait_for_count(std::size_t count, std::chrono::milliseconds timeout = 1000ms) {
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

} // namespace

TEST(OrderEntryClient, ConnectFailsWhenNothingIsListening) {
    std::uint16_t port = 0;
    {
        TcpSocket probe;
        ASSERT_TRUE(probe.listen(0));
        port = *probe.local_port();
    } // closed -- guarantees ECONNREFUSED, same trick test_tcp_socket.cpp uses

    OrderEntryClient client([](const Message&) {});
    EXPECT_FALSE(client.connect("127.0.0.1", port));
    EXPECT_FALSE(client.is_connected());
}

TEST(OrderEntryClient, SendEncodesAndWritesAWholeFrameThePeerCanDecode) {
    TcpSocket listener;
    ASSERT_TRUE(listener.listen(0));
    const auto port = *listener.local_port();

    OrderEntryClient client([](const Message&) {});
    ASSERT_TRUE(client.connect("127.0.0.1", port));
    auto peer = listener.accept();
    ASSERT_TRUE(peer.has_value());

    const Message sent{NewOrder{.account_id = 1,
                                 .client_order_id = 2,
                                 .instrument_id = kInstrument,
                                 .side = Side::Buy,
                                 .price = 100,
                                 .quantity = 10,
                                 .order_type = OrderType::Limit,
                                 .time_in_force = TimeInForce::GTC}};
    ASSERT_TRUE(client.send(sent));

    std::vector<std::byte> received;
    std::array<std::byte, 128> chunk{};
    const auto expected_size = HEADER_SIZE + payload_size_for(MessageType::NewOrder);
    const auto deadline = std::chrono::steady_clock::now() + 1000ms;
    while (received.size() < expected_size && std::chrono::steady_clock::now() < deadline) {
        auto n = peer->read(chunk);
        if (n && *n > 0) {
            received.insert(received.end(), chunk.begin(), chunk.begin() + static_cast<std::ptrdiff_t>(*n));
        }
    }
    ASSERT_EQ(received.size(), expected_size);

    auto decoded = decode_message(received);
    const auto* message = std::get_if<Message>(&decoded);
    ASSERT_NE(message, nullptr);
    EXPECT_EQ(*message, sent);
}

TEST(OrderEntryClient, MessagesWrittenByThePeerArriveViaTheSinkInOrder) {
    TcpSocket listener;
    ASSERT_TRUE(listener.listen(0));
    const auto port = *listener.local_port();

    MessageCollector collector;
    OrderEntryClient client(std::ref(collector));
    ASSERT_TRUE(client.connect("127.0.0.1", port));
    auto peer = listener.accept();
    ASSERT_TRUE(peer.has_value());

    const Message first{Accepted{.account_id = 1,
                                  .client_order_id = 2,
                                  .exchange_order_id = 3,
                                  .instrument_id = kInstrument,
                                  .side = Side::Buy,
                                  .price = 100,
                                  .quantity = 10,
                                  .order_type = OrderType::Limit,
                                  .time_in_force = TimeInForce::GTC}};
    const Message second{
        Cancelled{.account_id = 1, .client_order_id = 2, .exchange_order_id = 3, .instrument_id = kInstrument}};

    // Encoded and written back-to-back in a single write() call -- exercises
    // the reader loop's "drain every complete frame before reading again"
    // behavior (both frames arrive in one read(), same as the gateway's own
    // connection_reader_loop() must handle).
    std::vector<std::byte> buf;
    encode_message(first, buf);
    encode_message(second, buf);
    ASSERT_TRUE(peer->write(buf).has_value());

    ASSERT_TRUE(collector.wait_for_count(2));
    const auto messages = collector.messages();
    ASSERT_EQ(messages.size(), 2u);
    EXPECT_EQ(messages[0], first);
    EXPECT_EQ(messages[1], second);
}

TEST(OrderEntryClient, DisconnectUnblocksTheReaderThreadAndIsIdempotent) {
    TcpSocket listener;
    ASSERT_TRUE(listener.listen(0));
    const auto port = *listener.local_port();

    OrderEntryClient client([](const Message&) {});
    ASSERT_TRUE(client.connect("127.0.0.1", port));
    auto peer = listener.accept();
    ASSERT_TRUE(peer.has_value());

    client.disconnect();
    client.disconnect(); // safe to call again -- must not hang or crash
    EXPECT_FALSE(client.is_connected());
}

TEST(OrderEntryClient, IsConnectedBecomesFalseAfterThePeerCloses) {
    TcpSocket listener;
    ASSERT_TRUE(listener.listen(0));
    const auto port = *listener.local_port();

    OrderEntryClient client([](const Message&) {});
    ASSERT_TRUE(client.connect("127.0.0.1", port));
    ASSERT_TRUE(client.is_connected());

    {
        auto peer = listener.accept();
        ASSERT_TRUE(peer.has_value());
    } // peer's destructor closes its socket -- an orderly EOF for the client's reader thread

    const auto deadline = std::chrono::steady_clock::now() + 1000ms;
    while (client.is_connected() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(5ms);
    }
    EXPECT_FALSE(client.is_connected());
}
