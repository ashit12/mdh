#include "ui_gateway/ui_gateway.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <exception>
#include <sstream>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <variant>

#include <httplib.h>
#include <nlohmann/json.hpp>

#include "common/sequence_validator.hpp"
#include "net/packet.hpp"
#include "net/udp_receiver.hpp"
#include "trader/oms/client_order.hpp"

namespace mdh::ui_gateway {

using json = nlohmann::json;

namespace {

// ── Small JSON/wire-vocabulary helpers, local to this file ────────────────
// Side has no to_string()/parser anywhere else in this codebase (unlike
// OrderType/TimeInForce/RejectReason, which exchange/core/types.hpp
// already provides for logging purposes) -- these exist purely for this
// milestone's JSON boundary, not because trading logic needs them.
[[nodiscard]] std::string side_to_string(Side side) { return side == Side::Buy ? "Buy" : "Sell"; }

[[nodiscard]] std::optional<Side> parse_side(const std::string& text) {
    if (text == "Buy") return Side::Buy;
    if (text == "Sell") return Side::Sell;
    return std::nullopt;
}

[[nodiscard]] std::optional<exchange::TimeInForce> parse_time_in_force(const std::string& text) {
    if (text == "GTC") return exchange::TimeInForce::GTC;
    if (text == "IOC") return exchange::TimeInForce::IOC;
    if (text == "FOK") return exchange::TimeInForce::FOK;
    return std::nullopt;
}

[[nodiscard]] std::string pending_action_to_string(trader::oms::PendingAction action) {
    switch (action) {
        case trader::oms::PendingAction::None:    return "None";
        case trader::oms::PendingAction::Cancel:  return "Cancel";
        case trader::oms::PendingAction::Replace: return "Replace";
    }
    return "Unknown";
}

[[nodiscard]] json order_to_json(const trader::oms::ClientOrder& order) {
    json j;
    j["account_id"] = order.account_id;
    j["client_order_id"] = order.client_order_id;
    j["exchange_order_id"] = order.exchange_order_id ? json(*order.exchange_order_id) : json(nullptr);
    j["instrument_id"] = order.instrument_id;
    j["side"] = side_to_string(order.side);
    j["price"] = order.price;
    j["quantity"] = order.quantity;
    j["remaining_quantity"] = order.remaining_quantity;
    j["order_type"] = std::string(exchange::to_string(order.order_type));
    j["time_in_force"] = std::string(exchange::to_string(order.time_in_force));
    j["state"] = std::string(trader::oms::to_string(order.state));
    j["pending_action"] = pending_action_to_string(order.pending_action);
    j["last_reject_reason"] = std::string(exchange::to_string(order.last_reject_reason));
    return j;
}

[[nodiscard]] json level_to_json(const book::PriceLevelView& level) {
    json j;
    j["price"] = level.price;
    j["quantity"] = level.aggregate_quantity;
    j["order_count"] = level.order_count;
    return j;
}

[[nodiscard]] InstrumentId instrument_id_of(const protocol::Event& event) {
    return std::visit([](const auto& ev) { return ev.instrument_id; }, event);
}

void send_json_error(httplib::Response& res, int status, const std::string& error) {
    res.status = status;
    res.set_content(json{{"error", error}}.dump(), "application/json");
}

} // namespace

// ── SseHub ──────────────────────────────────────────────────────────────────
// A tiny per-topic "latest value wins" pub/sub, deliberately NOT a full
// replay log: each topic (a book instrument, or an account's orders) keeps
// only its single most recent JSON payload plus a monotonic revision
// number. A slow or momentarily-unscheduled subscriber can therefore miss
// an *intermediate* update to a topic (e.g. an order transitioning through
// Live before landing on PartiallyFilled, if both happen within one poll
// cycle) but can never be starved of a topic indefinitely -- wait_next()
// scans every topic each time it wakes and always finds *some* topic with
// unseen data if one exists, so every topic's latest state eventually
// reaches every subscriber. This is the deliberate trade-off documented in
// ui_gateway.hpp's own class comment for choosing SSE over a guaranteed-
// delivery transport: a live dashboard cares about "what is true right
// now," not "every state that was ever true," and the REST endpoints
// remain available for a client that wants a guaranteed-fresh pull instead
// of relying on the push stream alone.
class UiGateway::SseHub {
public:
    void publish(const std::string& topic, std::string payload) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& entry = topics_[topic];
        entry.payload = std::move(payload);
        entry.revision = ++global_revision_;
        cv_.notify_all();
    }

    // `cursors` is owned by one SSE connection (see handle_stream()) and
    // tracks, per topic, the revision that connection has already
    // delivered. Blocks up to `timeout` for any topic with a newer
    // revision; returns its payload and advances `cursors` for that topic
    // only -- a subsequent call picks up any other topic still pending,
    // which is what guarantees no topic is starved (see class comment).
    [[nodiscard]] std::optional<std::string> wait_next(std::unordered_map<std::string, std::uint64_t>& cursors,
                                                         std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        std::string ready_topic;
        auto has_pending = [&]() {
            for (const auto& [topic, entry] : topics_) {
                if (entry.revision > cursors[topic]) {
                    ready_topic = topic;
                    return true;
                }
            }
            return false;
        };
        if (!cv_.wait_for(lock, timeout, has_pending)) {
            return std::nullopt;
        }
        auto& entry = topics_.at(ready_topic);
        cursors[ready_topic] = entry.revision;
        return entry.payload;
    }

private:
    struct Entry {
        std::string payload;
        std::uint64_t revision = 0;
    };

    std::mutex mutex_;
    std::condition_variable cv_;
    std::uint64_t global_revision_ = 0;
    std::unordered_map<std::string, Entry> topics_;
};

// ── Session ─────────────────────────────────────────────────────────────────
UiGateway::Session::Session(exchange::AccountId account_id, std::uint16_t exchange_tcp_port,
                              const trader::risk::TraderRiskLimits& limits,
                              trader::oms::OrderManagementSystem::OrderUpdateSink on_update)
    : gated_(
          account_id, [this](const protocol::order_entry::Message& m) { return client_.send(m); }, std::move(on_update),
          limits),
      client_([this](const protocol::order_entry::Message& m) { gated_.handle_message(m); }) {
    connected_ = client_.connect("127.0.0.1", exchange_tcp_port);
}

// ── UiGateway ────────────────────────────────────────────────────────────────
UiGateway::UiGateway(exchange::gateway::OrderEntryGateway& exchange_gateway, std::uint16_t exchange_tcp_port,
                       std::uint16_t market_data_udp_port, std::uint16_t http_port, UiGatewayOptions options)
    : exchange_gateway_(exchange_gateway),
      exchange_tcp_port_(exchange_tcp_port),
      market_data_udp_port_(market_data_udp_port),
      http_port_(http_port),
      options_(std::move(options)),
      sse_hub_(std::make_unique<SseHub>()),
      server_(std::make_unique<httplib::Server>()) {
    server_->set_default_headers({{"Access-Control-Allow-Origin", "*"}});
    server_->Options(".*", [](const httplib::Request&, httplib::Response& res) {
        res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        res.set_header("Access-Control-Allow-Headers", "Content-Type");
        res.status = 204;
    });

    server_->Get("/api/health", [this](const httplib::Request& req, httplib::Response& res) { handle_health(req, res); });
    server_->Get("/api/accounts",
                  [this](const httplib::Request& req, httplib::Response& res) { handle_list_accounts(req, res); });
    server_->Get(R"(/api/accounts/(\d+))",
                  [this](const httplib::Request& req, httplib::Response& res) { handle_get_account(req, res); });
    server_->Get(R"(/api/book/(\d+))",
                  [this](const httplib::Request& req, httplib::Response& res) { handle_get_book(req, res); });
    server_->Post("/api/orders",
                   [this](const httplib::Request& req, httplib::Response& res) { handle_submit_order(req, res); });
    server_->Post(R"(/api/orders/(\d+)/(\d+)/cancel)",
                   [this](const httplib::Request& req, httplib::Response& res) { handle_cancel_order(req, res); });
    server_->Post(R"(/api/orders/(\d+)/(\d+)/replace)",
                   [this](const httplib::Request& req, httplib::Response& res) { handle_replace_order(req, res); });
    server_->Get("/api/stream", [this](const httplib::Request& req, httplib::Response& res) { handle_stream(req, res); });

    if (!options_.static_files_dir.empty()) {
        (void)server_->set_mount_point("/", options_.static_files_dir);
    }
}

UiGateway::~UiGateway() { stop(); }

bool UiGateway::start() {
    net::UdpReceiver receiver(market_data_udp_port_);
    if (!receiver.is_open()) {
        return false;
    }

    int bound = -1;
    if (http_port_ == 0) {
        bound = server_->bind_to_any_port("0.0.0.0");
    } else {
        bound = server_->bind_to_port("0.0.0.0", http_port_) ? http_port_ : -1;
    }
    if (bound < 0) {
        return false;
    }
    bound_http_port_ = static_cast<std::uint16_t>(bound);

    // Deliberately NOT relying on std::jthread's own automatic stop_token
    // injection (which would fire on THIS jthread's own internal
    // stop_source, not on stop_source_ below) -- same explicit-shared-
    // stop_source_ convention OrderEntryGateway::accept_loop() already
    // uses, so stop() below can request-stop every background thread this
    // class owns uniformly through one stop_source_, exactly like that
    // class does across its accept + N writer threads.
    market_data_thread_ = std::jthread([this, receiver = std::move(receiver)]() mutable {
        market_data_loop(stop_source_.get_token(), std::move(receiver));
    });
    http_thread_ = std::jthread([this] { server_->listen_after_bind(); });
    // Without this, a stop() racing right behind a fast start() could
    // observe is_running_ still false and become a no-op (see
    // Server::stop()'s own `if (is_running_)` guard) -- server_->stop()
    // would then never close svr_sock_, and http_thread_'s accept loop,
    // still blocked on that now-permanently-open socket, would never
    // return, hanging http_thread_.join() forever. wait_until_ready() is
    // cpp-httplib's own documented fix for exactly this start/stop race.
    server_->wait_until_ready();
    return true;
}

void UiGateway::stop() {
    server_->stop();
    if (http_thread_.joinable()) {
        http_thread_.join();
    }

    stop_source_.request_stop();
    if (market_data_thread_.joinable()) {
        market_data_thread_.join();
    }

    std::lock_guard<std::mutex> lock(sessions_mutex_);
    for (auto& [account_id, session] : sessions_) {
        session->client_.disconnect();
    }
}

std::optional<std::uint16_t> UiGateway::local_http_port() const { return bound_http_port_; }

UiGateway::Session* UiGateway::find_or_create_session(exchange::AccountId account_id) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    auto it = sessions_.find(account_id);
    if (it != sessions_.end()) {
        return it->second.get();
    }

    const bool is_demo_account =
        std::find(options_.demo_account_ids.begin(), options_.demo_account_ids.end(), account_id) !=
        options_.demo_account_ids.end();
    if (!is_demo_account) {
        return nullptr;
    }

    auto update_sink = [this, account_id](const trader::oms::ClientOrder& order) { broadcast_order(account_id, order); };
    auto session = std::make_unique<Session>(account_id, exchange_tcp_port_, options_.per_account_risk_limits,
                                               std::move(update_sink));
    // Process-local mirror only -- never touches exchange_gateway_'s
    // Ledger, see this class's own header comment on why that matters.
    session->gated_.deposit_cash(options_.demo_starting_cash);
    for (InstrumentId instrument_id : options_.demo_instrument_ids) {
        session->gated_.deposit_position(instrument_id, options_.demo_starting_position);
    }

    auto* raw = session.get();
    sessions_.emplace(account_id, std::move(session));
    return raw;
}

void UiGateway::broadcast_order(exchange::AccountId account_id, const trader::oms::ClientOrder& order) {
    json event;
    event["type"] = "order";
    event["account_id"] = account_id;
    event["order"] = order_to_json(order);
    sse_hub_->publish("order:" + std::to_string(account_id), event.dump());
}

void UiGateway::broadcast_book(InstrumentId instrument_id) {
    json event;
    event["type"] = "book";
    {
        std::lock_guard<std::mutex> lock(books_mutex_);
        const auto* book = market_data_outcome_.books.find_book(instrument_id);
        event["instrument_id"] = instrument_id;
        json bids = json::array();
        json asks = json::array();
        if (book != nullptr) {
            for (const auto& level : book->top_bids(options_.book_depth)) bids.push_back(level_to_json(level));
            for (const auto& level : book->top_asks(options_.book_depth)) asks.push_back(level_to_json(level));
        }
        event["bids"] = bids;
        event["asks"] = asks;
    }
    sse_hub_->publish("book:" + std::to_string(instrument_id), event.dump());
}

void UiGateway::market_data_loop(std::stop_token token, net::UdpReceiver receiver) {
    SequenceValidator validator;
    const replay::ReplayOptions options{};

    while (!token.stop_requested()) {
        auto batch = receiver.receive_batch(64);
        if (batch.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }

        std::vector<InstrumentId> touched;
        {
            std::lock_guard<std::mutex> lock(books_mutex_);
            for (const auto& datagram : batch) {
                auto unpacked = net::unpack_frames(datagram.bytes);
                if (std::holds_alternative<net::PacketError>(unpacked)) {
                    continue;
                }
                for (auto& frame : std::get<net::UnpackedPacket>(unpacked).frames) {
                    if (const auto* event = std::get_if<protocol::Event>(&frame)) {
                        touched.push_back(instrument_id_of(*event));
                    }
                    (void)replay::apply_frame_result(std::move(frame), validator, options, market_data_outcome_);
                }
            }
        }

        std::sort(touched.begin(), touched.end());
        touched.erase(std::unique(touched.begin(), touched.end()), touched.end());
        for (InstrumentId instrument_id : touched) {
            broadcast_book(instrument_id);
        }
    }
}

// ── REST handlers ────────────────────────────────────────────────────────────

void UiGateway::handle_health(const httplib::Request&, httplib::Response& res) {
    res.set_content(json{{"status", "ok"}}.dump(), "application/json");
}

void UiGateway::handle_list_accounts(const httplib::Request&, httplib::Response& res) {
    res.set_content(json{{"account_ids", options_.demo_account_ids}}.dump(), "application/json");
}

void UiGateway::handle_get_account(const httplib::Request& req, httplib::Response& res) {
    try {
        const auto account_id = static_cast<exchange::AccountId>(std::stoull(req.matches[1].str()));
        Session* session = find_or_create_session(account_id);
        if (session == nullptr) {
            send_json_error(res, 404, "unknown_account");
            return;
        }

        json positions = json::array();
        for (InstrumentId instrument_id : options_.demo_instrument_ids) {
            positions.push_back(json{{"instrument_id", instrument_id}, {"quantity", session->gated_.position(instrument_id)}});
        }
        json orders = json::array();
        for (const auto& order : session->gated_.orders()) {
            orders.push_back(order_to_json(order));
        }

        json response;
        response["account_id"] = account_id;
        response["cash"] = session->gated_.cash();
        response["positions"] = positions;
        response["orders"] = orders;
        res.set_content(response.dump(), "application/json");
    } catch (const std::exception&) {
        send_json_error(res, 400, "bad_request");
    }
}

void UiGateway::handle_get_book(const httplib::Request& req, httplib::Response& res) {
    try {
        const auto instrument_id = static_cast<InstrumentId>(std::stoul(req.matches[1].str()));
        std::size_t depth = options_.book_depth;
        if (const auto raw = req.get_param_value("depth"); !raw.empty()) {
            depth = static_cast<std::size_t>(std::stoull(raw));
        }

        json response;
        response["instrument_id"] = instrument_id;
        json bids = json::array();
        json asks = json::array();
        {
            std::lock_guard<std::mutex> lock(books_mutex_);
            const auto* book = market_data_outcome_.books.find_book(instrument_id);
            if (book != nullptr) {
                for (const auto& level : book->top_bids(depth)) bids.push_back(level_to_json(level));
                for (const auto& level : book->top_asks(depth)) asks.push_back(level_to_json(level));
            }
        }
        response["bids"] = bids;
        response["asks"] = asks;
        res.set_content(response.dump(), "application/json");
    } catch (const std::exception&) {
        send_json_error(res, 400, "bad_request");
    }
}

void UiGateway::handle_submit_order(const httplib::Request& req, httplib::Response& res) {
    try {
        const auto body = json::parse(req.body);
        const auto account_id = body.at("account_id").get<exchange::AccountId>();
        Session* session = find_or_create_session(account_id);
        if (session == nullptr) {
            send_json_error(res, 404, "unknown_account");
            return;
        }

        const auto side = parse_side(body.at("side").get<std::string>());
        if (!side) {
            send_json_error(res, 400, "invalid_side");
            return;
        }

        exchange::TimeInForce time_in_force = exchange::TimeInForce::GTC;
        if (body.contains("time_in_force")) {
            const auto parsed = parse_time_in_force(body.at("time_in_force").get<std::string>());
            if (!parsed) {
                send_json_error(res, 400, "invalid_time_in_force");
                return;
            }
            time_in_force = *parsed;
        }

        const auto instrument_id = body.at("instrument_id").get<InstrumentId>();
        const auto price = body.at("price").get<Price>();
        const auto quantity = body.at("quantity").get<Quantity>();

        const auto outcome =
            session->gated_.submit_new_order(instrument_id, *side, price, quantity, exchange::OrderType::Limit, time_in_force);

        json response;
        if (outcome.client_order_id) {
            response["accepted"] = true;
            response["client_order_id"] = *outcome.client_order_id;
        } else {
            response["accepted"] = false;
            response["reject_reason"] = std::string(exchange::to_string(outcome.local_reject_reason));
        }
        res.set_content(response.dump(), "application/json");
    } catch (const std::exception&) {
        send_json_error(res, 400, "bad_request");
    }
}

void UiGateway::handle_cancel_order(const httplib::Request& req, httplib::Response& res) {
    try {
        const auto account_id = static_cast<exchange::AccountId>(std::stoull(req.matches[1].str()));
        const auto client_order_id = static_cast<exchange::ClientOrderId>(std::stoull(req.matches[2].str()));
        Session* session = find_or_create_session(account_id);
        if (session == nullptr) {
            send_json_error(res, 404, "unknown_account");
            return;
        }
        const bool ok = session->gated_.cancel_order(client_order_id);
        res.set_content(json{{"ok", ok}}.dump(), "application/json");
    } catch (const std::exception&) {
        send_json_error(res, 400, "bad_request");
    }
}

void UiGateway::handle_replace_order(const httplib::Request& req, httplib::Response& res) {
    try {
        const auto account_id = static_cast<exchange::AccountId>(std::stoull(req.matches[1].str()));
        const auto client_order_id = static_cast<exchange::ClientOrderId>(std::stoull(req.matches[2].str()));
        Session* session = find_or_create_session(account_id);
        if (session == nullptr) {
            send_json_error(res, 404, "unknown_account");
            return;
        }

        const auto body = json::parse(req.body);
        const auto new_price = body.at("new_price").get<Price>();
        const auto new_quantity = body.at("new_quantity").get<Quantity>();

        const auto new_client_order_id = session->gated_.replace_order(client_order_id, new_price, new_quantity);
        json response;
        response["ok"] = new_client_order_id.has_value();
        if (new_client_order_id) {
            response["new_client_order_id"] = *new_client_order_id;
        }
        res.set_content(response.dump(), "application/json");
    } catch (const std::exception&) {
        send_json_error(res, 400, "bad_request");
    }
}

void UiGateway::handle_stream(const httplib::Request& req, httplib::Response& res) {
    auto cursors = std::make_shared<std::unordered_map<std::string, std::uint64_t>>();
    res.set_chunked_content_provider(
        "text/event-stream", [this, &req, cursors](std::size_t, httplib::DataSink& sink) {
            if (req.is_connection_closed()) {
                sink.done();
                return true;
            }
            auto next = sse_hub_->wait_next(*cursors, std::chrono::milliseconds(200));
            if (!next) {
                static constexpr char kHeartbeat[] = ": keep-alive\n\n";
                return sink.write(kHeartbeat, sizeof(kHeartbeat) - 1);
            }
            const std::string frame = "data: " + *next + "\n\n";
            return sink.write(frame.data(), frame.size());
        });
}

} // namespace mdh::ui_gateway
