#include "exchange/gateway/order_entry_gateway.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <iterator>
#include <optional>
#include <span>
#include <thread>
#include <type_traits>
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

        auto conn =
            std::make_unique<Connection>(next_session_id_++, std::move(*sock), options_.outbound_queue_capacity);
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

            auto command = to_command(message);
            if (!command) {
                // Decoded fine but isn't a valid client request (e.g. a
                // gateway -> client type arriving from a client) -- silently
                // ignored rather than disconnecting the client, since this
                // protocol has no NAK/error-response message type (see
                // messages.hpp) to report it with. It is also not something
                // this session can bind on: identity comes from real
                // requests only.
                continue;
            }

            // Every client request carries account_id (see messages.hpp).
            // The first one binds this session; every later one must agree
            // with it. account_id.has_value() is what lets that check stay
            // on this thread's own field instead of taking sessions_mutex_
            // for every single message.
            const AccountId account_id = std::visit([](const auto& m) { return m.account_id; }, message);
            if (!conn.account_id.has_value()) {
                bind_session(conn, account_id);
            } else if (*conn.account_id != account_id) {
                reject_account_mismatch(conn, *command);
                continue;
            }

            claim_order_ownership(conn, account_id, message);
            (void)submit_command(std::move(*command));
        }
    }

    // Peer EOF, a read error, or stop()'s shutdown() -- either way this
    // session is over, and nothing should route to it anymore.
    conn.closed.store(true, std::memory_order_release);
    unbind_session(conn);
    conn.socket.shutdown();       // this connection's writer may be blocked mid-write() on a dead socket
    conn.wake_cv.notify_all();    // and if it isn't, wake it so it observes conn.closed instead of lingering until stop()
}

void OrderEntryGateway::bind_session(Connection& conn, AccountId account_id) {
    {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        account_sessions_[account_id].push_back(&conn);

        // Taken over while sessions_mutex_ is still held, i.e. before this
        // session is reachable from account_sessions_ by the matching
        // thread -- that ordering is the whole reason a retained report
        // cannot end up behind a live one (see Connection::replay_backlog).
        if (auto pending = pending_reports_.find(account_id); pending != pending_reports_.end()) {
            std::lock_guard<std::mutex> replay_lock(conn.replay_mutex);
            conn.replay_backlog.assign(std::make_move_iterator(pending->second.begin()),
                                        std::make_move_iterator(pending->second.end()));
            pending_reports_.erase(pending);
        }
    }
    conn.account_id = account_id;
    conn.wake_cv.notify_one(); // there may now be a backlog to write out
}

void OrderEntryGateway::unbind_session(Connection& conn) {
    if (!conn.account_id.has_value()) {
        return; // never sent a valid request, so it was never in any of the maps below
    }

    std::lock_guard<std::mutex> lock(sessions_mutex_);
    if (auto sessions = account_sessions_.find(*conn.account_id); sessions != account_sessions_.end()) {
        auto& bound = sessions->second;
        bound.erase(std::remove(bound.begin(), bound.end(), &conn), bound.end());
        if (bound.empty()) {
            account_sessions_.erase(sessions);
        }
    }

    // A linear scan rather than a per-connection index of the ids it owns:
    // it runs once per disconnect, on that session's own reader thread,
    // over a map bounded by the exchange's live orders -- the matching
    // thread never pays for it. The orders themselves stay in the book
    // (there is no cancel-on-disconnect here); it's only the record of
    // *which session* to report them to that goes away, after which
    // route_event() falls back to the account's other sessions.
    for (auto it = order_owner_.begin(); it != order_owner_.end();) {
        it = (it->second == &conn) ? order_owner_.erase(it) : std::next(it);
    }
}

void OrderEntryGateway::reject_account_mismatch(Connection& conn, const ExchangeCommand& command) {
    using namespace protocol::order_entry;

    // Echoes the account_id the client actually asked for, not the one
    // this session is bound to, so the rejection mirrors the request that
    // caused it -- the same convention MatchingEngine follows when it
    // rejects a command. A replace is reported under its original id, also
    // matching what the engine does for every other replace rejection.
    const Rejected rejection = std::visit(
        [](const auto& cmd) {
            using T = std::decay_t<decltype(cmd)>;
            Rejected rejected{
                .account_id = cmd.account_id,
                .client_order_id = 0,
                .instrument_id = cmd.instrument_id,
                .reason = RejectReason::AccountMismatch,
            };
            if constexpr (std::is_same_v<T, ReplaceOrderCommand>) {
                rejected.client_order_id = cmd.original_client_order_id;
            } else {
                rejected.client_order_id = cmd.client_order_id;
            }
            return rejected;
        },
        command);

    // session_outbound, not outbound: this thread is that queue's single
    // producer, whereas outbound's is the matching thread (see
    // Connection::session_outbound). A full queue drops the rejection for
    // the same reason route_event() drops a report -- the client isn't
    // reading.
    if (conn.session_outbound.try_push(Message{rejection})) {
        conn.wake_cv.notify_one();
    }
}

void OrderEntryGateway::claim_order_ownership(Connection& conn, AccountId account_id,
                                               const protocol::order_entry::Message& message) {
    using namespace protocol::order_entry;

    std::lock_guard<std::mutex> lock(sessions_mutex_);
    std::visit(
        [&](const auto& m) {
            using T = std::decay_t<decltype(m)>;
            if constexpr (std::is_same_v<T, NewOrder> || std::is_same_v<T, CancelOrder>) {
                order_owner_.try_emplace(OrderKey{account_id, m.client_order_id}, &conn);
            } else if constexpr (std::is_same_v<T, ReplaceOrder>) {
                // Only the original id: that's what the engine reports a
                // failed replace under, so this is what sends the ack back
                // to whoever asked for it. The new id inherits ownership
                // from the original if (and only if) the replace actually
                // succeeds -- see update_order_ownership().
                order_owner_.try_emplace(OrderKey{account_id, m.original_client_order_id}, &conn);
            }
        },
        message);
    // First writer wins while the order remains live. A second session of
    // the same account may manage the order, but that must not steal its
    // private report stream from the live session that originated it. If
    // the origin disconnected, unbind_session() removed its entry and this
    // command's session becomes the new owner.
}

void OrderEntryGateway::connection_writer_loop(Connection& conn, std::stop_token token) {
    using namespace protocol::order_entry;

    // Reports retained while this session's account had nobody connected,
    // handed over by bind_session(). Drained before anything else so a
    // reconnecting client reads its history before whatever happens next.
    auto next_backlog_message = [&conn]() -> std::optional<Message> {
        std::lock_guard<std::mutex> lock(conn.replay_mutex);
        if (conn.replay_backlog.empty()) {
            return std::nullopt;
        }
        Message message = std::move(conn.replay_backlog.front());
        conn.replay_backlog.erase(conn.replay_backlog.begin());
        return message;
    };
    auto has_work = [&conn] {
        std::lock_guard<std::mutex> lock(conn.replay_mutex);
        return !conn.replay_backlog.empty() || conn.session_outbound.size() > 0 || conn.outbound.size() > 0;
    };

    while (!token.stop_requested() && !conn.closed.load(std::memory_order_acquire)) {
        auto message = next_backlog_message();
        if (!message) {
            message = conn.session_outbound.try_pop(); // the gateway's own replies, e.g. an account mismatch
        }
        if (!message) {
            message = conn.outbound.try_pop();
        }
        if (!message) {
            // wait_for()'s predicate is re-checked immediately, before ever
            // actually sleeping -- so a notify_one() (route_event() below)
            // or notify_all() (stop(), above) that already happened before
            // this wait began is never lost, only redundant with this
            // re-check. kPollInterval is a safety-net timeout only, not the
            // expected wake path -- see this class's own kPollInterval doc
            // comment.
            std::unique_lock<std::mutex> lock(conn.wake_mutex);
            conn.wake_cv.wait_for(lock, kPollInterval, [&] {
                return token.stop_requested() || conn.closed.load(std::memory_order_acquire) || has_work();
            });
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

    std::lock_guard<std::mutex> lock(sessions_mutex_);
    for (auto& [key, message] : reports) {
        // 1. The session that submitted this order, if it's still here.
        if (auto owner = order_owner_.find(key);
            owner != order_owner_.end() && !owner->second->closed.load(std::memory_order_acquire)) {
            deliver(*owner->second, std::move(message));
            continue;
        }

        // 2. Otherwise one other live session of the account. Reached
        // when the owning session disconnected but left a resting order
        // behind (nothing cancels orders on disconnect), or when the
        // command came from submit_command() rather than a socket at all.
        auto sessions = account_sessions_.find(key.account_id);
        if (sessions != account_sessions_.end()) {
            Connection* fallback = nullptr;
            for (Connection* session : sessions->second) {
                if (!session->closed.load(std::memory_order_acquire)) {
                    fallback = session;
                    break;
                }
            }
            if (fallback != nullptr) {
                deliver(*fallback, std::move(message));
                continue;
            }
        }

        // 3. Otherwise nobody is connected for this account at all -- hold
        // the report so the next session to bind can reconcile against it.
        if (options_.pending_report_capacity == 0) {
            continue;
        }
        auto& pending = pending_reports_[key.account_id];
        if (pending.size() >= options_.pending_report_capacity) {
            pending.pop_front();
        }
        pending.push_back(std::move(message));
    }

    update_order_ownership(event);
}

void OrderEntryGateway::deliver(Connection& conn, protocol::order_entry::Message message) {
    // A full outbound queue means that connection's writer thread has
    // fallen behind; dropped here rather than blocking (this function runs
    // on the matching thread, see class-level comment on why it must not
    // block) -- there is no synchronous caller here to hand a rejection
    // back to the way MatchingPipeline::submit() can. Deliberately *not*
    // retained in pending_reports_ either: that exists for an account with
    // nobody listening, not for a client that is connected and not reading.
    if (conn.outbound.try_push(std::move(message))) {
        // No lock needed here -- see Connection::wake_cv's doc comment on
        // why a notification racing ahead of the writer thread starting
        // its wait is never lost, only redundant with that wait's own
        // predicate re-check.
        conn.wake_cv.notify_one();
    }
}

void OrderEntryGateway::update_order_ownership(const ExchangeEvent& event) {
    std::visit(
        [this](const auto& ev) {
            using T = std::decay_t<decltype(ev)>;
            if constexpr (std::is_same_v<T, OrderRejected>) {
                // A rejection normally means nothing is live under this id,
                // so the ownership record goes with it. The exceptions are
                // rejections that leave an existing resting order in place
                // (duplicate new-order id, invalid replace, or a
                // risk-rejected replace): those still have a ledger hold,
                // so ownership must stay with whoever placed the live
                // order. A risk-rejected *new* order never opened a hold,
                // so find_hold is empty and we erase as before.
                if (!ledger_.find_hold(ev.account_id, ev.client_order_id).has_value()) {
                    order_owner_.erase(OrderKey{ev.account_id, ev.client_order_id});
                }
            } else if constexpr (std::is_same_v<T, OrderCancelled>) {
                order_owner_.erase(OrderKey{ev.account_id, ev.client_order_id});
            } else if constexpr (std::is_same_v<T, OrderReplaced>) {
                // The order lives on under a new id, so its owner follows
                // it there -- claim_order_ownership() deliberately doesn't
                // register the new id up front, since a rejected replace
                // would then leave behind a record of an order that never
                // existed.
                const OrderKey original{ev.account_id, ev.original_client_order_id};
                if (auto owner = order_owner_.find(original); owner != order_owner_.end()) {
                    order_owner_[OrderKey{ev.account_id, ev.new_client_order_id}] = owner->second;
                    order_owner_.erase(original);
                }
            } else if constexpr (std::is_same_v<T, TradeExecuted>) {
                // Mirrors the engine erasing a fully-filled order from its
                // own live_orders_ (matching_engine.cpp) -- a partial fill
                // leaves the order, and this record, in place.
                for (const auto& side : {ev.buyer, ev.seller}) {
                    if (side.remaining_quantity == 0) {
                        order_owner_.erase(OrderKey{side.account_id, side.client_order_id});
                    }
                }
            }
            // OrderAccepted and every Book* event leave ownership as-is:
            // the former's record was made when the command was submitted,
            // the latter are anonymous.
        },
        event);
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

std::vector<std::pair<OrderEntryGateway::OrderKey, protocol::order_entry::Message>>
OrderEntryGateway::to_execution_reports(const ExchangeEvent& event) {
    using namespace protocol::order_entry;
    std::vector<std::pair<OrderKey, Message>> reports;

    std::visit(
        [&reports](const auto& ev) {
            using T = std::decay_t<decltype(ev)>;
            if constexpr (std::is_same_v<T, OrderAccepted>) {
                reports.emplace_back(OrderKey{ev.account_id, ev.client_order_id},
                                      Message{Accepted{
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
                reports.emplace_back(OrderKey{ev.account_id, ev.client_order_id},
                                      Message{Rejected{
                                          .account_id = ev.account_id,
                                          .client_order_id = ev.client_order_id,
                                          .instrument_id = ev.instrument_id,
                                          .reason = ev.reason,
                                      }});
            } else if constexpr (std::is_same_v<T, OrderCancelled>) {
                reports.emplace_back(OrderKey{ev.account_id, ev.client_order_id},
                                      Message{Cancelled{
                                          .account_id = ev.account_id,
                                          .client_order_id = ev.client_order_id,
                                          .exchange_order_id = ev.exchange_order_id,
                                          .instrument_id = ev.instrument_id,
                                      }});
            } else if constexpr (std::is_same_v<T, OrderReplaced>) {
                // Keyed on the *original* id: that's the one the requesting
                // session claimed when it submitted the replace (see
                // claim_order_ownership()); the new id only takes ownership
                // over once this event has been routed.
                reports.emplace_back(OrderKey{ev.account_id, ev.original_client_order_id},
                                      Message{Replaced{
                                          .account_id = ev.account_id,
                                          .original_client_order_id = ev.original_client_order_id,
                                          .new_client_order_id = ev.new_client_order_id,
                                          .exchange_order_id = ev.exchange_order_id,
                                          .instrument_id = ev.instrument_id,
                                          .new_price = ev.new_price,
                                          .new_quantity = ev.new_quantity,
                                      }});
            } else if constexpr (std::is_same_v<T, TradeExecuted>) {
                reports.emplace_back(OrderKey{ev.buyer.account_id, ev.buyer.client_order_id},
                                      Message{TradeReport{
                                          .account_id = ev.buyer.account_id,
                                          .client_order_id = ev.buyer.client_order_id,
                                          .exchange_order_id = ev.buyer.exchange_order_id,
                                          .instrument_id = ev.instrument_id,
                                          .price = ev.price,
                                          .quantity = ev.quantity,
                                          .remaining_quantity = ev.buyer.remaining_quantity,
                                      }});
                reports.emplace_back(OrderKey{ev.seller.account_id, ev.seller.client_order_id},
                                      Message{TradeReport{
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
