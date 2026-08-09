#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <optional>
#include <span>
#include <thread>
#include <vector>

#include "exchange/gateway/order_entry_gateway.hpp"
#include "net/tcp_socket.hpp"
#include "protocol/order_entry/decoder.hpp"
#include "protocol/order_entry/encoder.hpp"

// The Milestone 7 loop-closing test: proves the gateway is actually reachable
// and correct end to end over a real TCP loopback connection -- not just that
// its pieces (codec, TcpSocket, MatchingPipeline's Processor seam) are
// individually correct in isolation, the way their own unit tests already
// show. A real client here is nothing more than a TcpSocket plus the same,
// completely unmodified protocol::order_entry:: encoder/decoder the gateway
// itself uses -- this is exactly the point: the wire format is the only
// contract between the two sides.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::gateway;
using namespace mdh::net;
using namespace mdh::protocol::order_entry;
using namespace std::chrono_literals;

namespace {

constexpr InstrumentId kInstrument = 42;

// A minimal, test-only order-entry client: connects, sends whole encoded
// Messages, and accumulates/decodes bytes read back into whole Messages --
// the client-side mirror of what Connection::read_buffer +
// connection_reader_loop() do inside the gateway itself, since a real client
// faces exactly the same "TCP has no message boundaries" problem the gateway
// does (see tcp_socket.hpp's own doc comment).
class TestClient {
public:
    [[nodiscard]] bool connect_to(std::uint16_t port) {
        if (!socket_.connect("127.0.0.1", port)) {
            return false;
        }
        // Non-blocking so receive() below can poll with a bounded timeout
        // instead of risking an indefinite hang if the gateway never
        // responds (e.g. because of a bug this test is meant to catch).
        socket_.set_non_blocking();
        return true;
    }

    void send(const Message& message) {
        std::vector<std::byte> buf;
        encode_message(message, buf);
        std::size_t written = 0;
        while (written < buf.size()) {
            auto n = socket_.write(std::span(buf).subspan(written));
            if (n) {
                written += *n;
            } else {
                std::this_thread::sleep_for(1ms); // EWOULDBLOCK -- kernel send buffer momentarily full
            }
        }
    }

    // Waits up to `timeout` for one complete Message to arrive.
    [[nodiscard]] std::optional<Message> receive(std::chrono::milliseconds timeout = 1000ms) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (true) {
            if (auto message = try_decode_one()) {
                return message;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return std::nullopt;
            }
            std::array<std::byte, 512> chunk{};
            if (auto n = socket_.read(chunk); n && *n > 0) {
                buffer_.insert(buffer_.end(), chunk.begin(), chunk.begin() + static_cast<std::ptrdiff_t>(*n));
            } else {
                std::this_thread::sleep_for(1ms);
            }
        }
    }

private:
    [[nodiscard]] std::optional<Message> try_decode_one() {
        auto header_result = decode_header(buffer_);
        const auto* header = std::get_if<Header>(&header_result);
        if (!header) {
            return std::nullopt; // not enough bytes yet, or (shouldn't happen here) a bad type byte
        }
        const std::size_t frame_size = HEADER_SIZE + header->payload_size;
        if (buffer_.size() < frame_size) {
            return std::nullopt;
        }
        auto message_result = decode_message(std::span(buffer_).first(frame_size));
        buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(frame_size));
        const auto* message = std::get_if<Message>(&message_result);
        return message ? std::optional<Message>(*message) : std::nullopt;
    }

    TcpSocket socket_;
    std::vector<std::byte> buffer_;
};

// Starts a gateway on an OS-assigned ephemeral port and stops it (draining
// every connection thread) automatically at scope exit -- RAII wrapper kept
// local to this test file, same rationale as TempFile in test_market_data_e2e.cpp.
class RunningGateway {
public:
    explicit RunningGateway(const OrderEntryGatewayOptions& options = {}) : gateway_(0, options) {
        started_ = gateway_.start();
    }

    [[nodiscard]] bool started() const { return started_; }
    [[nodiscard]] std::uint16_t port() const { return *gateway_.local_port(); }
    [[nodiscard]] OrderEntryGateway& gateway() { return gateway_; }

private:
    OrderEntryGateway gateway_;
    bool started_ = false;
};

NewOrder new_order(AccountId account, ClientOrderId client_id, Side side, Price price, Quantity qty,
                    TimeInForce tif = TimeInForce::GTC) {
    return NewOrder{.account_id = account,
                     .client_order_id = client_id,
                     .instrument_id = kInstrument,
                     .side = side,
                     .price = price,
                     .quantity = qty,
                     .order_type = OrderType::Limit,
                     .time_in_force = tif};
}

} // namespace

TEST(OrderEntryGatewayE2e, NewOrderRoundTripsToAnAcceptedResponse) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(/*account_id=*/1, /*amount=*/1'000'000);

    TestClient client;
    ASSERT_TRUE(client.connect_to(server.port()));

    client.send(Message{new_order(/*account=*/1, /*client_id=*/7, Side::Buy, /*price=*/100, /*qty=*/10)});

    auto response = client.receive();
    ASSERT_TRUE(response.has_value());
    const auto* accepted = std::get_if<Accepted>(&*response);
    ASSERT_NE(accepted, nullptr);
    EXPECT_EQ(accepted->account_id, 1u);
    EXPECT_EQ(accepted->client_order_id, 7u);
    EXPECT_EQ(accepted->instrument_id, kInstrument);
    EXPECT_EQ(accepted->side, Side::Buy);
    EXPECT_EQ(accepted->price, 100);
    EXPECT_EQ(accepted->quantity, 10u);
    EXPECT_GT(accepted->exchange_order_id, 0u);

    EXPECT_EQ(server.gateway().connection_count(), 1u);
}

TEST(OrderEntryGatewayE2e, NewOrderWithoutSufficientFundsIsRejected) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    // Deliberately no deposit_cash() for this account.

    TestClient client;
    ASSERT_TRUE(client.connect_to(server.port()));
    client.send(Message{new_order(/*account=*/2, /*client_id=*/1, Side::Buy, /*price=*/100, /*qty=*/10)});

    auto response = client.receive();
    ASSERT_TRUE(response.has_value());
    const auto* rejected = std::get_if<Rejected>(&*response);
    ASSERT_NE(rejected, nullptr);
    EXPECT_EQ(rejected->account_id, 2u);
    EXPECT_EQ(rejected->client_order_id, 1u);
    EXPECT_EQ(rejected->reason, RejectReason::InsufficientFunds);
}

TEST(OrderEntryGatewayE2e, CancelOrderRoundTripsToACancelledResponse) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(/*account_id=*/3, /*amount=*/1'000'000);

    TestClient client;
    ASSERT_TRUE(client.connect_to(server.port()));

    client.send(Message{new_order(/*account=*/3, /*client_id=*/11, Side::Buy, /*price=*/50, /*qty=*/5)});
    auto accepted_response = client.receive();
    ASSERT_TRUE(accepted_response.has_value());
    ASSERT_NE(std::get_if<Accepted>(&*accepted_response), nullptr);

    client.send(Message{CancelOrder{.account_id = 3, .client_order_id = 11, .instrument_id = kInstrument}});
    auto cancel_response = client.receive();
    ASSERT_TRUE(cancel_response.has_value());
    const auto* cancelled = std::get_if<Cancelled>(&*cancel_response);
    ASSERT_NE(cancelled, nullptr);
    EXPECT_EQ(cancelled->account_id, 3u);
    EXPECT_EQ(cancelled->client_order_id, 11u);
    EXPECT_EQ(cancelled->instrument_id, kInstrument);
}

TEST(OrderEntryGatewayE2e, ReplaceOrderRoundTripsToAReplacedResponse) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(/*account_id=*/4, /*amount=*/1'000'000);

    TestClient client;
    ASSERT_TRUE(client.connect_to(server.port()));

    client.send(Message{new_order(/*account=*/4, /*client_id=*/21, Side::Buy, /*price=*/50, /*qty=*/5)});
    ASSERT_TRUE(client.receive().has_value());

    client.send(Message{ReplaceOrder{.account_id = 4,
                                      .original_client_order_id = 21,
                                      .new_client_order_id = 22,
                                      .instrument_id = kInstrument,
                                      .new_price = 55,
                                      .new_quantity = 8}});
    auto replace_response = client.receive();
    ASSERT_TRUE(replace_response.has_value());
    const auto* replaced = std::get_if<Replaced>(&*replace_response);
    ASSERT_NE(replaced, nullptr);
    EXPECT_EQ(replaced->account_id, 4u);
    EXPECT_EQ(replaced->original_client_order_id, 21u);
    EXPECT_EQ(replaced->new_client_order_id, 22u);
    EXPECT_EQ(replaced->new_price, 55);
    EXPECT_EQ(replaced->new_quantity, 8u);
}

TEST(OrderEntryGatewayE2e, CrossingOrdersFromTwoConnectionsEachGetTheirOwnTradeReport) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    constexpr AccountId kBuyer = 5;
    constexpr AccountId kSeller = 6;
    server.gateway().deposit_cash(kBuyer, 1'000'000);
    server.gateway().deposit_position(kSeller, kInstrument, 100);

    TestClient buyer_client;
    TestClient seller_client;
    ASSERT_TRUE(buyer_client.connect_to(server.port()));
    ASSERT_TRUE(seller_client.connect_to(server.port()));

    // Buyer rests a GTC bid; its own Accepted is the only thing the buyer's
    // connection gets from this message.
    buyer_client.send(Message{new_order(kBuyer, /*client_id=*/1, Side::Buy, /*price=*/100, /*qty=*/10)});
    auto buyer_accepted = buyer_client.receive();
    ASSERT_TRUE(buyer_accepted.has_value());
    ASSERT_NE(std::get_if<Accepted>(&*buyer_accepted), nullptr);

    // Seller crosses it fully -- the seller's connection first gets its own
    // Accepted, then both sides get a TradeReport (delivered to two
    // different connections, exercising route_event()'s fan-out).
    seller_client.send(Message{new_order(kSeller, /*client_id=*/1, Side::Sell, /*price=*/100, /*qty=*/10)});

    auto seller_accepted = seller_client.receive();
    ASSERT_TRUE(seller_accepted.has_value());
    ASSERT_NE(std::get_if<Accepted>(&*seller_accepted), nullptr);

    auto buyer_trade = buyer_client.receive();
    ASSERT_TRUE(buyer_trade.has_value());
    const auto* buyer_report = std::get_if<TradeReport>(&*buyer_trade);
    ASSERT_NE(buyer_report, nullptr);
    EXPECT_EQ(buyer_report->account_id, kBuyer);
    EXPECT_EQ(buyer_report->client_order_id, 1u);
    EXPECT_EQ(buyer_report->price, 100);
    EXPECT_EQ(buyer_report->quantity, 10u);
    EXPECT_EQ(buyer_report->remaining_quantity, 0u);

    auto seller_trade = seller_client.receive();
    ASSERT_TRUE(seller_trade.has_value());
    const auto* seller_report = std::get_if<TradeReport>(&*seller_trade);
    ASSERT_NE(seller_report, nullptr);
    EXPECT_EQ(seller_report->account_id, kSeller);
    EXPECT_EQ(seller_report->client_order_id, 1u);
    EXPECT_EQ(seller_report->price, 100);
    EXPECT_EQ(seller_report->quantity, 10u);
    EXPECT_EQ(seller_report->remaining_quantity, 0u);

    EXPECT_EQ(server.gateway().connection_count(), 2u);

    // snapshot() is only safe to call once the matching thread has been
    // joined (see its own doc comment) -- both TradeReports having already
    // arrived over the wire proves the trade was fully processed, but not
    // that the matching thread has gone on to fully quiesce, so stop()
    // first rather than racing it.
    server.gateway().stop();
    const auto snapshot = server.gateway().snapshot();
    EXPECT_TRUE(snapshot.instruments.empty()); // both orders fully filled -- nothing left resting
}
