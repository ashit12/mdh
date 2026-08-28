#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <optional>
#include <span>
#include <thread>
#include <vector>

#include "exchange/gateway/order_entry_gateway.hpp"
#include "net/tcp_socket.hpp"
#include "protocol/order_entry/decoder.hpp"
#include "protocol/order_entry/encoder.hpp"

// Failure injection against the live, networked
// OrderEntryGateway -- deliberately malformed/adversarial/abrupt client
// behavior sent over real TCP sockets, checking the two properties every
// fault class below must satisfy: (1) the gateway itself never crashes or
// hangs, and (2) a fault on one connection never degrades or blocks any
// *other* connection, since route_event() runs on the shared matching thread
// and submit() is MPSC into that same pipeline.
//
// Unlike tests/test_order_entry_decode_errors.cpp (unit-level: calls
// decode_message() directly on hand-built byte spans, proving the codec
// itself classifies malformed input correctly), every test here drives the
// *live* gateway over a real socket, so it also exercises
// connection_reader_loop()'s buffering/framing logic and its documented
// policy choices (see order_entry_gateway.hpp's own comment on why a bad
// header byte and an incomplete payload are treated identically) -- things
// no unit test of the codec alone can observe.
using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::gateway;
using namespace mdh::net;
using namespace mdh::protocol::order_entry;
using namespace std::chrono_literals;

namespace {

constexpr InstrumentId kInstrument = 42;

// Same minimal test-only order-entry client as
// tests/test_order_entry_gateway_e2e.cpp's TestClient (duplicated rather
// than shared across test binaries -- see that file's own comment on why
// each *_e2e.cpp/test_failure_injection_*.cpp keeps its own copy).
class TestClient {
public:
    [[nodiscard]] bool connect_to(std::uint16_t port) {
        if (!socket_.connect("127.0.0.1", port)) return false;
        socket_.set_non_blocking();
        return true;
    }

    void send(const Message& message) { send_bytes(encode_to_bytes(message)); }

    // Sends arbitrary, possibly-malformed raw bytes -- the actual fault
    // injection primitive every test below builds on. A real malicious or
    // buggy client is not restricted to bytes encode_message() would ever
    // produce.
    void send_bytes(std::span<const std::byte> data) {
        std::size_t written = 0;
        while (written < data.size()) {
            auto n = socket_.write(data.subspan(written));
            if (n) {
                written += *n;
            } else {
                std::this_thread::sleep_for(1ms);
            }
        }
    }

    [[nodiscard]] std::optional<Message> receive(std::chrono::milliseconds timeout = 1000ms) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (true) {
            if (auto message = try_decode_one()) return message;
            if (std::chrono::steady_clock::now() >= deadline) return std::nullopt;
            std::array<std::byte, 512> chunk{};
            if (auto n = socket_.read(chunk); n && *n > 0) {
                buffer_.insert(buffer_.end(), chunk.begin(), chunk.begin() + static_cast<std::ptrdiff_t>(*n));
            } else {
                std::this_thread::sleep_for(1ms);
            }
        }
    }

    void disconnect() { socket_ = TcpSocket(); } // drops the fd via move-assignment from a fresh, unconnected socket

    static std::vector<std::byte> encode_to_bytes(const Message& message) {
        std::vector<std::byte> buf;
        encode_message(message, buf);
        return buf;
    }

private:
    [[nodiscard]] std::optional<Message> try_decode_one() {
        auto header_result = decode_header(buffer_);
        const auto* header = std::get_if<Header>(&header_result);
        if (!header) return std::nullopt;
        const std::size_t frame_size = HEADER_SIZE + header->payload_size;
        if (buffer_.size() < frame_size) return std::nullopt;
        auto message_result = decode_message(std::span(buffer_).first(frame_size));
        buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(frame_size));
        const auto* message = std::get_if<Message>(&message_result);
        return message ? std::optional<Message>(*message) : std::nullopt;
    }

    TcpSocket socket_;
    std::vector<std::byte> buffer_;
};

class RunningGateway {
public:
    // The gateway trades only what it is told about, and every test here
    // uses the one instrument above.
    explicit RunningGateway(OrderEntryGatewayOptions options = {}) : gateway_(0, with_default_instrument(options)) {
        started_ = gateway_.start();
    }

    [[nodiscard]] bool started() const { return started_; }
    [[nodiscard]] std::uint16_t port() const { return *gateway_.local_port(); }
    [[nodiscard]] OrderEntryGateway& gateway() { return gateway_; }

private:
    static const OrderEntryGatewayOptions& with_default_instrument(OrderEntryGatewayOptions& options) {
        if (options.instruments.empty()) {
            options.instruments = {kInstrument};
        }
        return options;
    }

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

// NewOrder's payload byte layout (see protocol/order_entry/messages.hpp's
// payload_size_for() and src/protocol/order_entry/decoder.cpp's decode
// order): account_id(8) + client_order_id(8) + instrument_id(4) then the
// 1-byte `side` field -- offset 20 within the payload, offset
// HEADER_SIZE(3) + 20 = 23 within the whole encoded frame.
constexpr std::size_t kNewOrderSideByteOffset = HEADER_SIZE + 8 + 8 + 4;

} // namespace

TEST(FailureInjectionGateway, InvalidTypeByteDesyncsOnlyThatConnectionNotTheGatewayOrOtherConnections) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(/*account_id=*/1, /*amount=*/1'000'000);
    server.gateway().deposit_cash(/*account_id=*/2, /*amount=*/1'000'000);

    // A single byte that is not any known MessageType (see
    // protocol::order_entry::MessageType -- valid values are 1-3 and
    // 10-14). connection_reader_loop() treats an unrecognized type byte
    // identically to "header not fully arrived yet" (its own documented
    // policy, since a length-prefixed stream gives no other way to tell
    // the two apart) -- it never discards the byte, so read_buffer can
    // never resynchronize: this connection is permanently stuck, by
    // design, not a crash.
    TestClient attacker;
    ASSERT_TRUE(attacker.connect_to(server.port()));
    const std::array<std::byte, 1> garbage_type_byte{std::byte{0xFF}};
    attacker.send_bytes(garbage_type_byte);

    // Even a subsequent, perfectly well-formed message on this same
    // connection can never be recognized -- it lands behind the
    // unresolvable garbage byte in read_buffer.
    attacker.send(Message{new_order(/*account=*/1, /*client_id=*/1, Side::Buy, /*price=*/100, /*qty=*/10)});
    EXPECT_FALSE(attacker.receive(300ms).has_value());

    // The gateway process itself is unaffected: a brand new, well-behaved
    // connection round-trips normally.
    TestClient well_behaved;
    ASSERT_TRUE(well_behaved.connect_to(server.port()));
    well_behaved.send(Message{new_order(/*account=*/2, /*client_id=*/1, Side::Buy, /*price=*/50, /*qty=*/5)});
    auto response = well_behaved.receive();
    ASSERT_TRUE(response.has_value());
    EXPECT_NE(std::get_if<Accepted>(&*response), nullptr);

    EXPECT_EQ(server.gateway().connection_count(), 2u);
}

TEST(FailureInjectionGateway, WellFormedHeaderWithInvalidPayloadFieldDropsOnlyThatFrameAndKeepsTheConnectionOpen) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(/*account_id=*/3, /*amount=*/1'000'000);

    TestClient client;
    ASSERT_TRUE(client.connect_to(server.port()));

    // A structurally well-formed NewOrder frame (correct type byte,
    // correct payload_size for NewOrder) with the `side` byte mutated to a
    // value that is neither Side::Buy nor Side::Sell. Framing survives
    // (the header is trustworthy), but decode_message() itself rejects the
    // payload content -- connection_reader_loop()'s documented behavior
    // for exactly this case is to drop just this one frame and keep
    // reading, per its own doc comment ("a malformed *payload* under an
    // otherwise well-formed header drops just that one frame and keeps
    // the connection open").
    auto bad_frame = TestClient::encode_to_bytes(
        Message{new_order(/*account=*/3, /*client_id=*/1, Side::Buy, /*price=*/100, /*qty=*/10)});
    ASSERT_GT(bad_frame.size(), kNewOrderSideByteOffset);
    bad_frame[kNewOrderSideByteOffset] = std::byte{0x7F}; // neither Buy(0) nor Sell(1) -- see common/types.hpp
    client.send_bytes(bad_frame);

    // No response for the corrupted frame -- proven by the *next* message
    // being the only thing that ever arrives. Side::Buy again (not Sell --
    // this account only deposited cash, not a position, so a Sell here
    // would be a legitimate InsufficientPosition Rejected, not the
    // "corrupted frame produced no response" signal this test wants).
    client.send(Message{new_order(/*account=*/3, /*client_id=*/2, Side::Buy, /*price=*/200, /*qty=*/7)});

    auto response = client.receive();
    ASSERT_TRUE(response.has_value());
    const auto* accepted = std::get_if<Accepted>(&*response);
    ASSERT_NE(accepted, nullptr);
    // If the corrupted frame had somehow also produced a response, it
    // would have arrived first and claimed client_order_id 1 -- getting
    // client_order_id 2 first proves the corrupted frame was silently
    // dropped, not merely delayed.
    EXPECT_EQ(accepted->client_order_id, 2u);
    EXPECT_EQ(accepted->side, Side::Buy);

    // Nothing else ever arrives -- exactly one frame was ever accepted.
    EXPECT_FALSE(client.receive(200ms).has_value());
}

TEST(FailureInjectionGateway, TruncatedPayloadNeverCompletedThenAbruptDisconnectDoesNotCrashTheGateway) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(/*account_id=*/4, /*amount=*/1'000'000);

    {
        TestClient half_sent;
        ASSERT_TRUE(half_sent.connect_to(server.port()));

        // A syntactically valid header (correct type, correct
        // payload_size for NewOrder) followed by only half the promised
        // payload bytes -- connection_reader_loop() correctly waits for
        // more (see its own doc comment: "a header whose payload hasn't
        // fully arrived yet"). Those bytes never arrive; the connection is
        // instead torn down uncleanly (disconnect() below, no FIN/graceful
        // shutdown handshake initiated by this test), which is exactly
        // what a crashed or network-partitioned real client looks like
        // from the gateway's side.
        auto full_frame = TestClient::encode_to_bytes(
            Message{new_order(/*account=*/4, /*client_id=*/1, Side::Buy, /*price=*/100, /*qty=*/10)});
        ASSERT_GT(full_frame.size(), 10u);
        half_sent.send_bytes(std::span(full_frame).first(full_frame.size() / 2));
        std::this_thread::sleep_for(50ms); // let the gateway's reader thread actually consume the partial bytes
        half_sent.disconnect();
    } // half_sent's socket (and its underlying fd) is now fully gone

    // The gateway must have noticed the disconnect (read() returning
    // 0/error unblocks connection_reader_loop() -- see
    // order_entry_gateway.hpp's own comment) and kept running: a fresh,
    // well-behaved connection still works normally afterward.
    TestClient well_behaved;
    ASSERT_TRUE(well_behaved.connect_to(server.port()));
    well_behaved.send(Message{new_order(/*account=*/4, /*client_id=*/2, Side::Buy, /*price=*/50, /*qty=*/5)});
    auto response = well_behaved.receive();
    ASSERT_TRUE(response.has_value());
    EXPECT_NE(std::get_if<Accepted>(&*response), nullptr);
}

TEST(FailureInjectionGateway, AbruptDisconnectImmediatelyAfterConnectingNeverHavingSentAnythingIsHarmless) {
    RunningGateway server;
    ASSERT_TRUE(server.started());

    // The degenerate case: a TCP connection that completes the handshake
    // and then is torn down before a single byte of application data ever
    // arrives (e.g. a port scanner, or a client that crashed between
    // connect() and its first send()) -- accept_loop() must not have
    // assumed any client message is coming.
    for (int i = 0; i < 5; ++i) {
        TestClient client;
        ASSERT_TRUE(client.connect_to(server.port()));
        client.disconnect();
    }

    std::this_thread::sleep_for(100ms); // let every reader thread notice EOF and exit

    server.gateway().deposit_cash(/*account_id=*/5, /*amount=*/1'000'000);
    TestClient well_behaved;
    ASSERT_TRUE(well_behaved.connect_to(server.port()));
    well_behaved.send(Message{new_order(/*account=*/5, /*client_id=*/1, Side::Buy, /*price=*/10, /*qty=*/1)});
    auto response = well_behaved.receive();
    ASSERT_TRUE(response.has_value());
    EXPECT_NE(std::get_if<Accepted>(&*response), nullptr);

    EXPECT_EQ(server.gateway().connection_count(), 6u); // 5 abandoned + 1 well-behaved -- see its own doc comment on why this never decreases
}

TEST(FailureInjectionGateway, SlowNonReadingClientOverflowingItsOutboundQueueNeverBlocksAnotherConnection) {
    OrderEntryGatewayOptions options;
    options.outbound_queue_capacity = 4; // deliberately tiny -- easy to overflow with a handful of orders
    RunningGateway server(options);
    ASSERT_TRUE(server.started());

    constexpr AccountId kSlowAccount = 6;
    constexpr AccountId kFastAccount = 7;
    server.gateway().deposit_cash(kSlowAccount, 1'000'000);
    server.gateway().deposit_cash(kFastAccount, 1'000'000);

    // Connects, sends far more orders than outbound_queue_capacity can
    // hold, and -- the actual fault being injected -- never calls
    // receive() at all, so its outbound queue fills up and every
    // subsequent route_event() for this account must fall into the
    // documented drop-on-full path (route_event()'s own doc comment)
    // instead of blocking the matching thread.
    TestClient slow_client;
    ASSERT_TRUE(slow_client.connect_to(server.port()));
    constexpr int kOrdersFromSlowClient = 50;
    for (int i = 0; i < kOrdersFromSlowClient; ++i) {
        slow_client.send(Message{new_order(kSlowAccount, static_cast<ClientOrderId>(i + 1), Side::Buy,
                                            /*price=*/10, /*qty=*/1, TimeInForce::IOC)});
    }

    // A second, well-behaved connection must still get fast, complete
    // service -- proving the slow client's overflowing queue never
    // blocked route_event() (which runs on the shared matching thread,
    // see order_entry_gateway.hpp's own concurrency-model comment) from
    // servicing anyone else.
    TestClient fast_client;
    ASSERT_TRUE(fast_client.connect_to(server.port()));
    for (int i = 0; i < 20; ++i) {
        fast_client.send(Message{new_order(kFastAccount, static_cast<ClientOrderId>(i + 1), Side::Buy,
                                            /*price=*/10, /*qty=*/1, TimeInForce::IOC)});
        auto response = fast_client.receive(1000ms);
        ASSERT_TRUE(response.has_value()) << "fast client starved by slow client's full outbound queue at order " << i;
        EXPECT_NE(std::get_if<Accepted>(&*response), nullptr);
    }

    // The slow client, now finally draining, sees at most
    // outbound_queue_capacity responses ever delivered (everything
    // produced while its queue was already full was dropped, per policy)
    // -- proving the *system* stayed alive and consistent rather than
    // silently losing track of state, even though this one client lost
    // messages by design.
    std::size_t slow_client_responses = 0;
    while (slow_client.receive(200ms).has_value()) {
        ++slow_client_responses;
    }
    EXPECT_GT(slow_client_responses, 0u);
    EXPECT_LE(slow_client_responses, static_cast<std::size_t>(kOrdersFromSlowClient));
}

TEST(FailureInjectionGateway, FloodOfRandomBytesFromANonProtocolClientDoesNotCrashTheGatewayOrOtherConnections) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(/*account_id=*/8, /*amount=*/1'000'000);

    // A connection that never speaks the protocol at all -- 4KB of
    // deterministic, non-message-shaped bytes (a stand-in for a
    // misconfigured client, a port scanner sending an HTTP request, or
    // random noise). connection_reader_loop() must survive this without
    // ever calling anything on the malformed "message" (there isn't one)
    // and without wedging the accept thread or any other connection.
    TestClient noise_client;
    ASSERT_TRUE(noise_client.connect_to(server.port()));
    std::vector<std::byte> noise(4096);
    for (std::size_t i = 0; i < noise.size(); ++i) {
        noise[i] = static_cast<std::byte>((i * 37 + 11) % 256); // deterministic, not all-zero, not a valid header repeated
    }
    noise_client.send_bytes(noise);

    TestClient well_behaved;
    ASSERT_TRUE(well_behaved.connect_to(server.port()));
    well_behaved.send(Message{new_order(/*account=*/8, /*client_id=*/1, Side::Buy, /*price=*/10, /*qty=*/1)});
    auto response = well_behaved.receive();
    ASSERT_TRUE(response.has_value());
    EXPECT_NE(std::get_if<Accepted>(&*response), nullptr);
}
