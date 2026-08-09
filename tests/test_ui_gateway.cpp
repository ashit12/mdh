#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <thread>

#include <httplib.h>
#include <nlohmann/json.hpp>

#include "exchange/gateway/order_entry_gateway.hpp"
#include "exchange/market_data/market_data_publisher.hpp"
#include "net/packet.hpp"
#include "net/udp_receiver.hpp"
#include "net/udp_socket.hpp"
#include "ui_gateway/ui_gateway.hpp"

// The Milestone 12 loop-closing test: a real OrderEntryGateway, publishing
// real market data over a real UDP socket (via the new
// OrderEntryGatewayOptions::extra_event_sink hook), observed by a real
// UiGateway reconstructing a live book and holding real
// TraderRiskGatedOms + OrderEntryClient sessions over a real TCP
// connection -- exercised entirely through UiGateway's actual REST/SSE
// surface (httplib::Client), never by reaching into its internals. Proves
// the same thing every other *_e2e.cpp in this codebase proves for its own
// milestone: the pieces built here compose correctly with everything
// beneath them, over real transports, not just in isolation.
using namespace mdh;
using namespace mdh::exchange;
using namespace std::chrono_literals;
using json = nlohmann::json;

namespace {

[[nodiscard]] std::uint16_t pick_ephemeral_udp_port() {
    net::UdpReceiver probe(0);
    return *probe.local_port(); // see RunningStack's own comment on the narrow, test-only TOCTOU this accepts
}

// Composes a real OrderEntryGateway (market data published over UDP to a
// freshly-picked ephemeral port) with a real UiGateway listening to that
// same port -- everything on ephemeral ports, so many instances of this
// harness can coexist across this file's tests without collisions.
class RunningStack {
public:
    explicit RunningStack(ui_gateway::UiGatewayOptions ui_options = {}) : market_data_port_(pick_ephemeral_udp_port()) {
        gateway_options_.extra_event_sink = [this](const ExchangeEvent& event) {
            publisher_.publish(event, [this](const protocol::Event& wire_event) {
                const std::array<protocol::Event, 1> frames{wire_event};
                auto datagram = net::pack_frames(next_packet_sequence_++, std::span<const protocol::Event>(frames));
                (void)market_data_socket_.send_to(datagram, "127.0.0.1", market_data_port_);
            });
        };
        gateway_ = std::make_unique<gateway::OrderEntryGateway>(0, gateway_options_);

        // Pre-seed every demo account before start() -- see
        // ui_gateway.hpp's own class comment on why this ordering matters.
        for (AccountId account_id : ui_options.demo_account_ids) {
            gateway_->deposit_cash(account_id, ui_options.demo_starting_cash);
            for (InstrumentId instrument_id : ui_options.demo_instrument_ids) {
                gateway_->deposit_position(account_id, instrument_id, ui_options.demo_starting_position);
            }
        }
        gateway_started_ = gateway_->start();

        ui_ = std::make_unique<ui_gateway::UiGateway>(*gateway_, *gateway_->local_port(), market_data_port_,
                                                        /*http_port=*/0, ui_options);
        ui_started_ = gateway_started_ && ui_->start();
    }

    ~RunningStack() {
        if (ui_) ui_->stop();
        if (gateway_) gateway_->stop();
    }

    RunningStack(const RunningStack&) = delete;
    RunningStack& operator=(const RunningStack&) = delete;

    [[nodiscard]] bool started() const { return ui_started_; }
    [[nodiscard]] std::uint16_t http_port() const { return *ui_->local_http_port(); }
    [[nodiscard]] gateway::OrderEntryGateway& gateway() { return *gateway_; }

private:
    std::uint16_t market_data_port_;
    market_data::MarketDataPublisher publisher_;
    net::UdpSocket market_data_socket_;
    std::uint64_t next_packet_sequence_ = 1;
    gateway::OrderEntryGatewayOptions gateway_options_;
    std::unique_ptr<gateway::OrderEntryGateway> gateway_;
    bool gateway_started_ = false;
    std::unique_ptr<ui_gateway::UiGateway> ui_;
    bool ui_started_ = false;
};

// Polls `predicate` (typically an HTTP GET + a JSON assertion) until it
// returns true or `timeout` elapses -- needed anywhere a test depends on
// the market-data UDP thread or an OrderEntryClient's reader thread having
// asynchronously caught up, exactly like every other e2e test in this
// codebase that polls for a response instead of assuming instantaneous
// delivery (see e.g. TestClient::receive() elsewhere).
[[nodiscard]] bool wait_until(const std::function<bool()>& predicate, std::chrono::milliseconds timeout = 2000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(5ms);
    }
    return predicate();
}

} // namespace

TEST(UiGatewayTest, HealthEndpointReportsOk) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());

    auto res = cli.Get("/api/health");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    EXPECT_EQ(json::parse(res->body).at("status").get<std::string>(), "ok");
}

TEST(UiGatewayTest, ListAccountsReturnsTheDemoCatalog) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());

    auto res = cli.Get("/api/accounts");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    const auto body = json::parse(res->body);
    EXPECT_EQ(body.at("account_ids").get<std::vector<AccountId>>(), (std::vector<AccountId>{1001, 1002, 1003}));
}

TEST(UiGatewayTest, GetAccountForUnknownAccountReturns404) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());

    auto res = cli.Get("/api/accounts/9999");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 404);
}

TEST(UiGatewayTest, GetAccountAutoProvisionsAndReturnsSeededCashAndPositions) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());

    auto res = cli.Get("/api/accounts/1001");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    const auto body = json::parse(res->body);
    EXPECT_EQ(body.at("account_id").get<AccountId>(), 1001u);
    EXPECT_EQ(body.at("cash").get<std::int64_t>(), 1'000'000'0000);
    EXPECT_TRUE(body.at("orders").empty());
    ASSERT_EQ(body.at("positions").size(), 2u);
    EXPECT_EQ(body.at("positions")[0].at("quantity").get<Quantity>(), 1000u);
}

TEST(UiGatewayTest, SubmitOrderIsAcceptedAndVisibleOnTheAccount) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());

    const json order{{"account_id", 1001}, {"instrument_id", 1}, {"side", "Buy"}, {"price", 100}, {"quantity", 10}};
    auto res = cli.Post("/api/orders", order.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    const auto submitted = json::parse(res->body);
    EXPECT_TRUE(submitted.at("accepted").get<bool>());
    const auto client_order_id = submitted.at("client_order_id").get<ClientOrderId>();
    EXPECT_GT(client_order_id, 0u);

    ASSERT_TRUE(wait_until([&] {
        auto account_res = cli.Get("/api/accounts/1001");
        if (!account_res || account_res->status != 200) return false;
        const auto orders = json::parse(account_res->body).at("orders");
        return orders.size() == 1 && orders[0].at("state").get<std::string>() == "Live";
    }));
}

TEST(UiGatewayTest, SubmitOrderWithInvalidSideIsRejectedAsBadRequest) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());

    const json order{{"account_id", 1001}, {"instrument_id", 1}, {"side", "Sideways"}, {"price", 100}, {"quantity", 10}};
    auto res = cli.Post("/api/orders", order.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 400);
}

TEST(UiGatewayTest, SubmitOrderForUnknownAccountReturns404) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());

    const json order{{"account_id", 9999}, {"instrument_id", 1}, {"side", "Buy"}, {"price", 100}, {"quantity", 10}};
    auto res = cli.Post("/api/orders", order.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 404);
}

TEST(UiGatewayTest, CancelOrderRoundTrips) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());

    const json order{{"account_id", 1002}, {"instrument_id", 1}, {"side", "Buy"}, {"price", 50}, {"quantity", 5}};
    auto submit_res = cli.Post("/api/orders", order.dump(), "application/json");
    ASSERT_TRUE(submit_res);
    const auto client_order_id = json::parse(submit_res->body).at("client_order_id").get<ClientOrderId>();

    // cancel_order()/replace_order() both require the OMS to have already
    // seen this order go Live (the exchange's Accepted, echoed back over
    // the real TCP connection) -- see OrderManagementSystem::cancel_order's
    // is_live() guard -- so wait for that round trip before racing ahead.
    ASSERT_TRUE(wait_until([&] {
        auto account_res = cli.Get("/api/accounts/1002");
        if (!account_res || account_res->status != 200) return false;
        const auto orders = json::parse(account_res->body).at("orders");
        return orders.size() == 1 && orders[0].at("state").get<std::string>() == "Live";
    }));

    auto cancel_res = cli.Post("/api/orders/1002/" + std::to_string(client_order_id) + "/cancel", "", "application/json");
    ASSERT_TRUE(cancel_res);
    EXPECT_EQ(cancel_res->status, 200);
    EXPECT_TRUE(json::parse(cancel_res->body).at("ok").get<bool>());

    ASSERT_TRUE(wait_until([&] {
        auto account_res = cli.Get("/api/accounts/1002");
        if (!account_res || account_res->status != 200) return false;
        const auto orders = json::parse(account_res->body).at("orders");
        return orders.size() == 1 && orders[0].at("state").get<std::string>() == "Cancelled";
    }));
}

TEST(UiGatewayTest, ReplaceOrderRoundTrips) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());

    const json order{{"account_id", 1003}, {"instrument_id", 1}, {"side", "Buy"}, {"price", 50}, {"quantity", 5}};
    auto submit_res = cli.Post("/api/orders", order.dump(), "application/json");
    ASSERT_TRUE(submit_res);
    const auto client_order_id = json::parse(submit_res->body).at("client_order_id").get<ClientOrderId>();

    // See CancelOrderRoundTrips's comment: replace_order() has the same
    // is_live() guard, so wait for the Accepted round trip first.
    ASSERT_TRUE(wait_until([&] {
        auto account_res = cli.Get("/api/accounts/1003");
        if (!account_res || account_res->status != 200) return false;
        const auto orders = json::parse(account_res->body).at("orders");
        return orders.size() == 1 && orders[0].at("state").get<std::string>() == "Live";
    }));

    const json replacement{{"new_price", 55}, {"new_quantity", 8}};
    auto replace_res =
        cli.Post("/api/orders/1003/" + std::to_string(client_order_id) + "/replace", replacement.dump(), "application/json");
    ASSERT_TRUE(replace_res);
    EXPECT_EQ(replace_res->status, 200);
    const auto replaced = json::parse(replace_res->body);
    EXPECT_TRUE(replaced.at("ok").get<bool>());
    EXPECT_GT(replaced.at("new_client_order_id").get<ClientOrderId>(), client_order_id);
}

TEST(UiGatewayTest, OrderBookReflectsLiveMarketDataOverRealUdp) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());

    // Empty before anything trades -- the live book, not a canned fixture.
    auto initial = cli.Get("/api/book/1");
    ASSERT_TRUE(initial);
    EXPECT_TRUE(json::parse(initial->body).at("bids").empty());

    const json order{{"account_id", 1001}, {"instrument_id", 1}, {"side", "Buy"}, {"price", 100}, {"quantity", 10}};
    ASSERT_TRUE(cli.Post("/api/orders", order.dump(), "application/json"));

    // This can only pass if OrderEntryGatewayOptions::extra_event_sink
    // really did publish a real UDP frame that UiGateway's own
    // market-data thread really did receive, unpack, and apply -- there is
    // no other path from "an order was accepted" to this bid appearing.
    ASSERT_TRUE(wait_until([&] {
        auto book_res = cli.Get("/api/book/1");
        if (!book_res || book_res->status != 200) return false;
        const auto bids = json::parse(book_res->body).at("bids");
        return bids.size() == 1 && bids[0].at("price").get<Price>() == 100 && bids[0].at("quantity").get<Quantity>() == 10;
    }));
}

TEST(UiGatewayTest, CrossingOrderProducesATradeVisibleOnBothAccounts) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());

    const json buy{{"account_id", 1001}, {"instrument_id", 1}, {"side", "Buy"}, {"price", 100}, {"quantity", 10}};
    ASSERT_TRUE(cli.Post("/api/orders", buy.dump(), "application/json"));

    const json sell{{"account_id", 1002}, {"instrument_id", 1}, {"side", "Sell"}, {"price", 100}, {"quantity", 10}};
    ASSERT_TRUE(cli.Post("/api/orders", sell.dump(), "application/json"));

    ASSERT_TRUE(wait_until([&] {
        auto account_res = cli.Get("/api/accounts/1001");
        if (!account_res || account_res->status != 200) return false;
        const auto orders = json::parse(account_res->body).at("orders");
        return orders.size() == 1 && orders[0].at("state").get<std::string>() == "Filled";
    }));
    ASSERT_TRUE(wait_until([&] {
        auto account_res = cli.Get("/api/accounts/1002");
        if (!account_res || account_res->status != 200) return false;
        const auto orders = json::parse(account_res->body).at("orders");
        return orders.size() == 1 && orders[0].at("state").get<std::string>() == "Filled";
    }));
    // Book empties out once both sides are fully filled.
    ASSERT_TRUE(wait_until([&] {
        auto book_res = cli.Get("/api/book/1");
        if (!book_res || book_res->status != 200) return false;
        const auto parsed = json::parse(book_res->body);
        return parsed.at("bids").empty() && parsed.at("asks").empty();
    }));
}

TEST(UiGatewayTest, StreamDeliversAnOrderEventAfterSubmission) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());

    std::mutex mutex;
    std::condition_variable cv;
    std::string collected;
    bool saw_order_event = false;

    std::thread sse_thread([&] {
        httplib::Client cli("127.0.0.1", stack.http_port());
        cli.set_read_timeout(5, 0);
        (void)cli.Get("/api/stream", [&](const char* data, std::size_t length) {
            std::lock_guard<std::mutex> lock(mutex);
            collected.append(data, length);
            if (collected.find("\"type\":\"order\"") != std::string::npos) {
                saw_order_event = true;
                cv.notify_all();
                return false; // ends the streaming request -- we saw what we needed
            }
            return true;
        });
    });

    // Give the SSE connection a moment to actually establish before the
    // order fires, so this test isn't racing the subscription itself.
    std::this_thread::sleep_for(200ms);
    httplib::Client submit_cli("127.0.0.1", stack.http_port());
    const json order{{"account_id", 1001}, {"instrument_id", 2}, {"side", "Buy"}, {"price", 42}, {"quantity", 1}};
    ASSERT_TRUE(submit_cli.Post("/api/orders", order.dump(), "application/json"));

    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cv.wait_for(lock, 3000ms, [&] { return saw_order_event; }));
    }
    sse_thread.join();
    EXPECT_TRUE(saw_order_event);
}

TEST(UiGatewayTest, GatewayHttpAndMarketDataPortsAreAllIndependentlyEphemeral) {
    // Two full stacks, back to back -- proves nothing in this milestone
    // hardcodes a port anywhere (see apps/trading_server for the CLI-args
    // equivalent of this same property in the actual binary).
    RunningStack first;
    RunningStack second;
    ASSERT_TRUE(first.started());
    ASSERT_TRUE(second.started());
    EXPECT_NE(first.http_port(), second.http_port());
}
