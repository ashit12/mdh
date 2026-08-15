#include "exchange/gateway/order_entry_gateway.hpp"

#include <array>
#include <chrono>
#include <span>
#include <thread>
#include <variant>

#include "protocol/order_entry/decoder.hpp"
#include "protocol/order_entry/encoder.hpp"

namespace mdh::exchange::gateway {

namespace {
using namespace std::chrono_literals;
// How long accept_loop() sleeps between polls when there was nothing to
// do -- short enough to keep latency low, long enough not to spin a core
// at 100% doing nothing (same rationale as
// net::UdpListenOptions::consumer_delay's "deterministic, not incidental"
// framing, just applied to idle-polling instead of simulated slowness).
// accept_loop() has no better option (TcpSocket::accept() has no blocking-
// with-wakeup primitive to offer it -- see tcp_socket.hpp's own
// shutdown()/accept() caveat), but connection_writer_loop() below now uses
// this only as a wait_for() safety-net timeout, not its primary wake
// mechanism -- see Connection::wake_cv's doc comment.
constexpr auto kPollInterval = 1ms;
} // namespace

OrderEntryGateway::OrderEntryGateway(std::uint16_t port, const OrderEntryGatewayOptions& options)
    : port_(port), options_(options), risk_gated_engine_(engine_, ledger_, options_.risk_limits),
      pipeline_(
          EventSink{[this](const ExchangeEvent& event) { route_event(event); }},
          sequencing::MatchingPipelineOptions{.queue_capacity = options_.matching_queue_capacity},
          sequencing::MatchingPipeline::Processor{[this](const ExchangeCommand& command, const EventSink& sink) {
              risk_gated_engine_.process(command, sink);
          }}) {}

OrderEntryGateway::~OrderEntryGateway() { stop(); }

bool OrderEntryGateway::start() {
    if (!listener_.listen(port_, options_.accept_backlog)) {
        return false;
    }
    listener_.set_non_blocking(); // accept_loop() must never block in accept() -- see its own doc comment
    accept_thread_ = std::jthread([this] { accept_loop(); });
    return true;
}

void OrderEntryGateway::stop() {
    stop_source_.request_stop();
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    std::lock_guard<std::mutex> lock(connections_mutex_);
    for (auto& conn : connections_) {
        conn->socket.shutdown(); // unblocks a blocked read() on this connection's reader thread
        conn->wake_cv.notify_all(); // unblocks a writer thread waiting in connection_writer_loop()'s wait_for()
    }
    for (auto& conn : connections_) {
        if (conn->reader_thread.joinable()) {
            conn->reader_thread.join();
        }
        if (conn->writer_thread.joinable()) {
            conn->writer_thread.join();
        }
    }

    pipeline_.stop();
}

std::size_t OrderEntryGateway::connection_count() const {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    return connections_.size();
}

bool OrderEntryGateway::submit_command(ExchangeCommand command) {
    std::lock_guard<std::mutex> lock(submit_mutex_);
    return pipeline_.submit(std::move(command));
}

// ── The six pieces ───────────────────────────────────────────────────────

void OrderEntryGateway::accept_loop() {
    const auto token = stop_source_.get_token();
    while (!token.stop_requested()) {
        auto sock = listener_.accept();
        if (!sock) {
            std::this_thread::sleep_for(kPollInterval); // nothing pending -- the expected common case, not an error
            continue;
        }

        auto conn = std::make_unique<Connection>(std::move(*sock), options_.outbound_queue_capacity);
        Connection* conn_ptr = conn.get();
        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            connections_.push_back(std::move(conn));
        }

        conn_ptr->reader_thread = std::jthread([this, conn_ptr] { connection_reader_loop(*conn_ptr); });
        conn_ptr->writer_thread = std::jthread(
            [this, conn_ptr] { connection_writer_loop(*conn_ptr, stop_source_.get_token()); });
    }
}

void OrderEntryGateway::connection_reader_loop(Connection& conn) {
    using namespace protocol::order_entry;

    std::array<std::byte, 4096> chunk{};
    while (true) {
        auto n = conn.socket.read(chunk);
        if (!n || *n == 0) {
            break; // error, peer EOF, or this socket was shutdown() by stop() -- connection is done either way
        }
        conn.read_buffer.insert(conn.read_buffer.end(), chunk.begin(),
                                 chunk.begin() + static_cast<std::ptrdiff_t>(*n));

        // Drain every complete frame currently sitting in read_buffer
        // before going back to read() for more -- a single read() can
        // return several small messages concatenated together, not just
        // one (see tcp_socket.hpp's own doc comment on TCP having no
        // atomic-message boundary).
        while (true) {
            auto header_result = decode_header(conn.read_buffer);
            if (std::holds_alternative<DecodeError>(header_result)) {
                break; // not enough bytes yet for a header, or a malformed type byte -- wait for more data either way
            }
            const auto& header = std::get<Header>(header_result);
            const std::size_t frame_size = HEADER_SIZE + header.payload_size;
            if (conn.read_buffer.size() < frame_size) {
                break; // header decoded, but the full payload hasn't arrived yet
            }

            auto message_result = decode_message(std::span(conn.read_buffer).first(frame_size));
            conn.read_buffer.erase(conn.read_buffer.begin(),
                                    conn.read_buffer.begin() + static_cast<std::ptrdiff_t>(frame_size));
            if (std::holds_alternative<DecodeError>(message_result)) {
                continue; // malformed payload for an otherwise well-formed header -- drop just this one frame
            }
            const Message& message = std::get<Message>(message_result);

            // Every message type carries account_id (see messages.hpp) --
            // opportunistic session binding, see Connection's own doc
            // comment. account_id.has_value() lets every later message on
            // this connection skip routes_mutex_ entirely.
            if (!conn.account_id.has_value()) {
                const AccountId account_id = std::visit([](const auto& m) { return m.account_id; }, message);
                {
                    std::lock_guard<std::mutex> lock(routes_mutex_);
                    routes_[account_id] = &conn;
                }
                conn.account_id = account_id;
            }

            if (auto command = to_command(message)) {
                (void)submit_command(std::move(*command));
            }
            // A message that decoded fine but isn't a valid client request
            // (to_command() returned std::nullopt, e.g. a gateway -> client
            // type arriving from a client) is silently ignored rather than
            // disconnecting the client -- this protocol has no NAK/error-
            // response message type (see messages.hpp) to report it with.
        }
    }
}

void OrderEntryGateway::connection_writer_loop(Connection& conn, std::stop_token token) {
    using namespace protocol::order_entry;

    while (!token.stop_requested()) {
        auto message = conn.outbound.try_pop();
        if (!message) {
            // wait_for()'s predicate is re-checked immediately, before ever
            // actually sleeping -- so a notify_one() (route_event() below)
            // or notify_all() (stop(), above) that already happened before
            // this wait began is never lost, only redundant with this
            // re-check. kPollInterval is a safety-net timeout only, not the
            // expected wake path -- see this class's own kPollInterval doc
            // comment.
            std::unique_lock<std::mutex> lock(conn.wake_mutex);
            conn.wake_cv.wait_for(lock, kPollInterval,
                                   [&] { return token.stop_requested() || conn.outbound.size() > 0; });
            continue;
        }

        std::vector<std::byte> buf;
        encode_message(*message, buf);

        std::size_t written = 0;
        while (written < buf.size()) {
            auto n = conn.socket.write(std::span(buf).subspan(written));
            if (!n || *n == 0) {
                return; // write error, or a 0-byte write on a live socket -- either way, this connection is done
            }
            written += *n;
        }
    }
}

void OrderEntryGateway::route_event(const ExchangeEvent& event) {
    if (options_.extra_event_sink) {
        options_.extra_event_sink(event); // e.g. market-data publishing -- see its own doc comment
    }

    auto reports = to_execution_reports(event);
    if (reports.empty()) {
        return; // e.g. a Book* event -- see to_execution_reports()'s own doc comment
    }

    std::lock_guard<std::mutex> lock(routes_mutex_);
    for (auto& [account_id, message] : reports) {
        auto it = routes_.find(account_id);
        if (it == routes_.end()) {
            continue; // no connection bound to this account yet, or it has already disconnected -- nothing to do
        }
        // A full outbound queue means that connection's writer thread has
        // fallen behind; dropped here rather than blocking (this function
        // runs on the matching thread, see class-level comment on why it
        // must not block) -- there is no synchronous caller here to hand a
        // rejection back to the way MatchingPipeline::submit() can.
        if (it->second->outbound.try_push(std::move(message))) {
            // No lock needed here -- see Connection::wake_cv's doc comment
            // on why a notification racing ahead of the writer thread
            // starting its wait is never lost, only redundant with that
            // wait's own predicate re-check.
            it->second->wake_cv.notify_one();
        }
    }
}

std::optional<ExchangeCommand> OrderEntryGateway::to_command(const protocol::order_entry::Message& message) {
    using namespace protocol::order_entry;
    return std::visit(
        [](const auto& msg) -> std::optional<ExchangeCommand> {
            using T = std::decay_t<decltype(msg)>;
            if constexpr (std::is_same_v<T, NewOrder>) {
                return ExchangeCommand{NewOrderCommand{
                    .command_sequence = 0, // assigned by MatchingPipeline::submit(), see its own doc comment
                    .account_id = msg.account_id,
                    .client_order_id = msg.client_order_id,
                    .instrument_id = msg.instrument_id,
                    .side = msg.side,
                    .price = msg.price,
                    .quantity = msg.quantity,
                    .order_type = msg.order_type,
                    .time_in_force = msg.time_in_force,
                }};
            } else if constexpr (std::is_same_v<T, CancelOrder>) {
                return ExchangeCommand{CancelOrderCommand{
                    .command_sequence = 0,
                    .account_id = msg.account_id,
                    .client_order_id = msg.client_order_id,
                    .instrument_id = msg.instrument_id,
                }};
            } else if constexpr (std::is_same_v<T, ReplaceOrder>) {
                return ExchangeCommand{ReplaceOrderCommand{
                    .command_sequence = 0,
                    .account_id = msg.account_id,
                    .original_client_order_id = msg.original_client_order_id,
                    .new_client_order_id = msg.new_client_order_id,
                    .instrument_id = msg.instrument_id,
                    .new_price = msg.new_price,
                    .new_quantity = msg.new_quantity,
                }};
            } else {
                // Accepted/Rejected/Cancelled/Replaced/TradeReport: gateway
                // -> client message types, never valid as a client request.
                return std::nullopt;
            }
        },
        message);
}

std::vector<std::pair<AccountId, protocol::order_entry::Message>> OrderEntryGateway::to_execution_reports(
    const ExchangeEvent& event) {
    using namespace protocol::order_entry;
    std::vector<std::pair<AccountId, Message>> reports;

    std::visit(
        [&reports](const auto& ev) {
            using T = std::decay_t<decltype(ev)>;
            if constexpr (std::is_same_v<T, OrderAccepted>) {
                reports.emplace_back(ev.account_id, Message{Accepted{
                                                         .account_id = ev.account_id,
                                                         .client_order_id = ev.client_order_id,
                                                         .exchange_order_id = ev.exchange_order_id,
                                                         .instrument_id = ev.instrument_id,
                                                         .side = ev.side,
                                                         .price = ev.price,
                                                         .quantity = ev.quantity,
                                                         .order_type = ev.order_type,
                                                         .time_in_force = ev.time_in_force,
                                                     }});
            } else if constexpr (std::is_same_v<T, OrderRejected>) {
                reports.emplace_back(ev.account_id, Message{Rejected{
                                                         .account_id = ev.account_id,
                                                         .client_order_id = ev.client_order_id,
                                                         .instrument_id = ev.instrument_id,
                                                         .reason = ev.reason,
                                                     }});
            } else if constexpr (std::is_same_v<T, OrderCancelled>) {
                reports.emplace_back(ev.account_id, Message{Cancelled{
                                                         .account_id = ev.account_id,
                                                         .client_order_id = ev.client_order_id,
                                                         .exchange_order_id = ev.exchange_order_id,
                                                         .instrument_id = ev.instrument_id,
                                                     }});
            } else if constexpr (std::is_same_v<T, OrderReplaced>) {
                reports.emplace_back(ev.account_id, Message{Replaced{
                                                         .account_id = ev.account_id,
                                                         .original_client_order_id = ev.original_client_order_id,
                                                         .new_client_order_id = ev.new_client_order_id,
                                                         .exchange_order_id = ev.exchange_order_id,
                                                         .instrument_id = ev.instrument_id,
                                                         .new_price = ev.new_price,
                                                         .new_quantity = ev.new_quantity,
                                                     }});
            } else if constexpr (std::is_same_v<T, TradeExecuted>) {
                reports.emplace_back(ev.buyer.account_id, Message{TradeReport{
                                                               .account_id = ev.buyer.account_id,
                                                               .client_order_id = ev.buyer.client_order_id,
                                                               .exchange_order_id = ev.buyer.exchange_order_id,
                                                               .instrument_id = ev.instrument_id,
                                                               .price = ev.price,
                                                               .quantity = ev.quantity,
                                                               .remaining_quantity = ev.buyer.remaining_quantity,
                                                           }});
                reports.emplace_back(ev.seller.account_id, Message{TradeReport{
                                                                .account_id = ev.seller.account_id,
                                                                .client_order_id = ev.seller.client_order_id,
                                                                .exchange_order_id = ev.seller.exchange_order_id,
                                                                .instrument_id = ev.instrument_id,
                                                                .price = ev.price,
                                                                .quantity = ev.quantity,
                                                                .remaining_quantity = ev.seller.remaining_quantity,
                                                            }});
            }
            // BookOrderAdded/BookOrderReduced/BookOrderRemoved: intentionally
            // no-op -- see this function's own doc comment.
        },
        event);

    return reports;
}

} // namespace mdh::exchange::gateway
