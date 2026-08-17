#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <mutex>
#include <optional>
#include <span>
#include <thread>
#include <type_traits>
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

// How long to wait before concluding that a message is genuinely *not*
// coming (e.g. a report that must not reach a session it doesn't belong
// to). Shorter than TestClient::receive()'s default, since this one is
// paid in full on every successful assertion rather than only when
// something has gone wrong.
constexpr auto kQuietTimeout = 200ms;

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

    // Closes this client's socket, which the gateway sees as peer EOF --
    // the only way a test can make a session disconnect without stopping
    // the whole gateway.
    void disconnect() { socket_ = TcpSocket{}; }

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

// ── Sessions ───────────────────────────────────────────────────────────────
// One connection is one session, an account may have several of them, and a
// private execution report belongs to the session that originated the order
// -- see OrderEntryGateway's own session doc comment. These four tests are
// the behavioral spec for that: without it, a second connection for an
// already-connected account silently took over the first one's execution
// stream.

TEST(OrderEntryGatewayE2e, TwoSessionsOnOneAccountEachReceiveOnlyTheirOwnReports) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    constexpr AccountId kAccount = 20;
    server.gateway().deposit_cash(kAccount, 1'000'000);

    TestClient first;
    TestClient second;
    ASSERT_TRUE(first.connect_to(server.port()));
    ASSERT_TRUE(second.connect_to(server.port()));

    // Both bind the same account. Resting bids well apart from each other,
    // so neither order can trade and the only reports in play are the two
    // Accepteds.
    first.send(Message{new_order(kAccount, /*client_id=*/1, Side::Buy, /*price=*/100, /*qty=*/10)});
    auto first_accepted = first.receive();
    ASSERT_TRUE(first_accepted.has_value());
    ASSERT_NE(std::get_if<Accepted>(&*first_accepted), nullptr);
    EXPECT_EQ(std::get<Accepted>(*first_accepted).client_order_id, 1u);

    second.send(Message{new_order(kAccount, /*client_id=*/2, Side::Buy, /*price=*/50, /*qty=*/5)});
    auto second_accepted = second.receive();
    ASSERT_TRUE(second_accepted.has_value());
    ASSERT_NE(std::get_if<Accepted>(&*second_accepted), nullptr);
    EXPECT_EQ(std::get<Accepted>(*second_accepted).client_order_id, 2u);

    // Neither session sees the other's order, even though both are bound to
    // the same account: these are private reports, not an account-wide feed.
    EXPECT_FALSE(first.receive(kQuietTimeout).has_value());
    EXPECT_FALSE(second.receive(kQuietTimeout).has_value());

    // And the first session still owns its own stream after the second one
    // bound to the same account -- the case that used to silently hand
    // every one of the first session's reports to the second's socket.
    first.send(Message{new_order(kAccount, /*client_id=*/3, Side::Buy, /*price=*/40, /*qty=*/2)});
    auto third_accepted = first.receive();
    ASSERT_TRUE(third_accepted.has_value());
    ASSERT_NE(std::get_if<Accepted>(&*third_accepted), nullptr);
    EXPECT_EQ(std::get<Accepted>(*third_accepted).client_order_id, 3u);
    EXPECT_FALSE(second.receive(kQuietTimeout).has_value());
}

TEST(OrderEntryGatewayE2e, ManagingAnOrderFromAnotherSessionDoesNotStealItsReportRoute) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    constexpr AccountId kAccount = 27;
    server.gateway().deposit_cash(kAccount, 1'000'000);

    TestClient origin;
    TestClient manager;
    ASSERT_TRUE(origin.connect_to(server.port()));
    ASSERT_TRUE(manager.connect_to(server.port()));

    origin.send(Message{new_order(kAccount, /*client_id=*/1, Side::Buy, /*price=*/100, /*qty=*/10)});
    ASSERT_TRUE(origin.receive().has_value());

    // Bind the second connection to the same account before it manages the
    // first session's order.
    manager.send(Message{new_order(kAccount, /*client_id=*/2, Side::Buy, /*price=*/50, /*qty=*/1)});
    ASSERT_TRUE(manager.receive().has_value());

    manager.send(Message{CancelOrder{
        .account_id = kAccount,
        .client_order_id = 1,
        .instrument_id = kInstrument,
    }});

    auto response = origin.receive();
    ASSERT_TRUE(response.has_value());
    const auto* cancelled = std::get_if<Cancelled>(&*response);
    ASSERT_NE(cancelled, nullptr);
    EXPECT_EQ(cancelled->client_order_id, 1u);
    EXPECT_FALSE(manager.receive(kQuietTimeout).has_value());
}

TEST(OrderEntryGatewayE2e, RequestForAnAccountThisSessionIsNotBoundToIsRejectedAndNeverReachesTheBook) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    constexpr AccountId kBound = 21;
    constexpr AccountId kSomebodyElse = 22;
    server.gateway().deposit_cash(kBound, 1'000'000);
    server.gateway().deposit_cash(kSomebodyElse, 1'000'000);

    TestClient client;
    ASSERT_TRUE(client.connect_to(server.port()));
    client.send(Message{new_order(kBound, /*client_id=*/1, Side::Buy, /*price=*/100, /*qty=*/10)});
    ASSERT_TRUE(client.receive().has_value()); // binds this session to kBound

    client.send(Message{new_order(kSomebodyElse, /*client_id=*/99, Side::Buy, /*price=*/70, /*qty=*/3)});
    auto response = client.receive();
    ASSERT_TRUE(response.has_value());
    const auto* rejected = std::get_if<Rejected>(&*response);
    ASSERT_NE(rejected, nullptr);
    EXPECT_EQ(rejected->reason, RejectReason::AccountMismatch);
    EXPECT_EQ(rejected->account_id, kSomebodyElse); // echoes what was asked for, as every other rejection does
    EXPECT_EQ(rejected->client_order_id, 99u);

    // The session is still usable, and still bound to the account it started
    // with -- a wrong account_id costs the offending message, not the
    // connection.
    client.send(Message{new_order(kBound, /*client_id=*/2, Side::Buy, /*price=*/60, /*qty=*/4)});
    auto after = client.receive();
    ASSERT_TRUE(after.has_value());
    ASSERT_NE(std::get_if<Accepted>(&*after), nullptr);

    server.gateway().stop();
    const auto snapshot = server.gateway().snapshot();
    ASSERT_EQ(snapshot.instruments.size(), 1u);
    for (const auto& bid : snapshot.instruments.front().bids) {
        EXPECT_EQ(bid.account_id, kBound); // the foreign order never made it into the book at all
    }
}

TEST(OrderEntryGatewayE2e, FillOnAnOrderWhoseSessionDisconnectedGoesToASurvivingSessionOfThatAccount) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    constexpr AccountId kSeller = 23;
    constexpr AccountId kBuyer = 24;
    server.gateway().deposit_position(kSeller, kInstrument, 100);
    server.gateway().deposit_cash(kBuyer, 1'000'000);

    TestClient seller_placing;
    TestClient seller_watching;
    ASSERT_TRUE(seller_placing.connect_to(server.port()));
    ASSERT_TRUE(seller_watching.connect_to(server.port()));

    // Both sessions bind kSeller; only the first one leaves an order resting.
    seller_placing.send(Message{new_order(kSeller, /*client_id=*/1, Side::Sell, /*price=*/100, /*qty=*/10)});
    ASSERT_TRUE(seller_placing.receive().has_value());
    seller_watching.send(Message{new_order(kSeller, /*client_id=*/2, Side::Sell, /*price=*/500, /*qty=*/1)});
    ASSERT_TRUE(seller_watching.receive().has_value());

    // The session that placed the resting order goes away -- but the order
    // itself stays in the book (nothing cancels orders on disconnect).
    seller_placing.disconnect();
    std::this_thread::sleep_for(100ms); // let the gateway's reader thread observe the EOF and unbind

    TestClient buyer;
    ASSERT_TRUE(buyer.connect_to(server.port()));
    buyer.send(Message{new_order(kBuyer, /*client_id=*/1, Side::Buy, /*price=*/100, /*qty=*/10)});
    ASSERT_TRUE(buyer.receive().has_value()); // the buyer's own Accepted

    // With the owning session gone, the fill falls back to kSeller's other
    // live session rather than disappearing.
    auto surviving_report = seller_watching.receive();
    ASSERT_TRUE(surviving_report.has_value());
    const auto* trade = std::get_if<TradeReport>(&*surviving_report);
    ASSERT_NE(trade, nullptr);
    EXPECT_EQ(trade->account_id, kSeller);
    EXPECT_EQ(trade->client_order_id, 1u); // the disconnected session's order
    EXPECT_EQ(trade->quantity, 10u);
    EXPECT_EQ(trade->remaining_quantity, 0u);
}

TEST(OrderEntryGatewayE2e, ReportsForAnAccountWithNoLiveSessionAreReplayedToTheNextSessionThatBinds) {
    RunningGateway server;
    ASSERT_TRUE(server.started());
    constexpr AccountId kSeller = 25;
    constexpr AccountId kBuyer = 26;
    server.gateway().deposit_position(kSeller, kInstrument, 100);
    server.gateway().deposit_cash(kSeller, 1'000'000);
    server.gateway().deposit_cash(kBuyer, 1'000'000);

    {
        TestClient seller;
        ASSERT_TRUE(seller.connect_to(server.port()));
        seller.send(Message{new_order(kSeller, /*client_id=*/1, Side::Sell, /*price=*/100, /*qty=*/10)});
        ASSERT_TRUE(seller.receive().has_value());
    } // seller disconnects, leaving its ask resting and its account with no session at all
    std::this_thread::sleep_for(100ms);

    TestClient buyer;
    ASSERT_TRUE(buyer.connect_to(server.port()));
    buyer.send(Message{new_order(kBuyer, /*client_id=*/1, Side::Buy, /*price=*/100, /*qty=*/10)});
    ASSERT_TRUE(buyer.receive().has_value()); // Accepted
    auto buyer_trade = buyer.receive();
    ASSERT_TRUE(buyer_trade.has_value());
    ASSERT_NE(std::get_if<TradeReport>(&*buyer_trade), nullptr); // the trade really did happen

    // The seller comes back. Its retained fill is delivered ahead of the
    // response to the request it reconnected with, so a client reconciling
    // on startup reads its history in order.
    TestClient reconnected;
    ASSERT_TRUE(reconnected.connect_to(server.port()));
    reconnected.send(Message{new_order(kSeller, /*client_id=*/2, Side::Buy, /*price=*/10, /*qty=*/1)});

    auto retained = reconnected.receive();
    ASSERT_TRUE(retained.has_value());
    const auto* trade = std::get_if<TradeReport>(&*retained);
    ASSERT_NE(trade, nullptr);
    EXPECT_EQ(trade->account_id, kSeller);
    EXPECT_EQ(trade->client_order_id, 1u); // the order placed by the session that had already gone
    EXPECT_EQ(trade->quantity, 10u);

    auto accepted = reconnected.receive();
    ASSERT_TRUE(accepted.has_value());
    ASSERT_NE(std::get_if<Accepted>(&*accepted), nullptr);
    EXPECT_EQ(std::get<Accepted>(*accepted).client_order_id, 2u);
}

TEST(OrderEntryGatewayE2e, ExtraEventSinkObservesEveryEventIncludingAnonymousBookEvents) {
    std::mutex mutex;
    std::vector<ExchangeEvent> observed;
    OrderEntryGatewayOptions options;
    options.extra_event_sink = [&](const ExchangeEvent& event) {
        std::lock_guard<std::mutex> lock(mutex);
        observed.push_back(event);
    };

    RunningGateway server(options);
    ASSERT_TRUE(server.started());
    server.gateway().deposit_cash(/*account_id=*/7, /*amount=*/1'000'000);

    TestClient client;
    ASSERT_TRUE(client.connect_to(server.port()));
    client.send(Message{new_order(/*account=*/7, /*client_id=*/1, Side::Buy, /*price=*/100, /*qty=*/10)});
    ASSERT_TRUE(client.receive().has_value()); // Accepted -- proves the command was fully processed

    server.gateway().stop(); // quiesce the matching thread before reading `observed` below

    std::lock_guard<std::mutex> lock(mutex);
    // Unlike to_execution_reports() (never called from any client
    // connection because there is none subscribed to market data in this
    // test), extra_event_sink must have seen the resulting BookOrderAdded
    // in addition to the account-addressed OrderAccepted -- proving it
    // really does see the raw, untranslated event stream, not just
    // whatever route_event() forwards to a connection.
    bool saw_order_accepted = false;
    bool saw_book_order_added = false;
    for (const auto& event : observed) {
        std::visit(
            [&](const auto& ev) {
                using T = std::decay_t<decltype(ev)>;
                if constexpr (std::is_same_v<T, OrderAccepted>) {
                    saw_order_accepted = true;
                } else if constexpr (std::is_same_v<T, BookOrderAdded>) {
                    saw_book_order_added = true;
                }
            },
            event);
    }
    EXPECT_TRUE(saw_order_accepted);
    EXPECT_TRUE(saw_book_order_added);
}
