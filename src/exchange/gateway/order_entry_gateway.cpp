#include "exchange/gateway/order_entry_gateway.hpp"

#include <algorithm>
#include <array>
#include <iterator>
#include <optional>
#include <span>
#include <type_traits>
#include <variant>

#include "common/thread_affinity.hpp"
#include "exchange/latency/latency_tracer.hpp"
#include "protocol/order_entry/decoder.hpp"
#include "protocol/order_entry/encoder.hpp"

namespace mdh::exchange::gateway {

OrderEntryGateway::OrderEntryGateway(std::uint16_t port, const OrderEntryGatewayOptions& options)
    : port_(port), options_(options),
      io_metrics_enabled_(options.enable_io_metrics),
      engine_(std::span<const InstrumentId>(options.instruments), options.expected_resting_orders),
      risk_gated_engine_(engine_, ledger_, options_.risk_limits),
      pipeline_(
          EventSink{[this](const ExchangeEvent& event) { route_event(event); }},
          sequencing::MatchingPipelineOptions{
              .queue_capacity = options_.matching_queue_capacity,
              .matching_cpu = options_.matching_cpu,
          },
          sequencing::MatchingPipeline::Processor{[this](const ExchangeCommand& command, const EventSink& sink) {
              latency::tracer().stamp_exchange_begin(command);
              risk_gated_engine_.process(command, sink);
              latency::tracer().stamp_exchange_end(command);
          }}) {}

OrderEntryGateway::~OrderEntryGateway() { stop(); }

bool OrderEntryGateway::start() {
    if (!listener_.listen(port_, options_.accept_backlog)) {
        return false;
    }
    if (!listener_.set_non_blocking()) {
        return false;
    }
    if (!poller_.is_open()) {
        return false;
    }
    if (!poller_.add(listener_.raw_fd(), net::IoInterest::Read, nullptr)) {
        return false;
    }
    listen_armed_ = true;
    io_thread_ = std::jthread([this] {
        set_calling_thread_name("mdh-gateway-io");
        io_loop();
    });
    return true;
}

void OrderEntryGateway::stop() {
    stop_source_.request_stop();
    poller_.wake();
    if (io_thread_.joinable()) {
        io_thread_.join();
    }
    pipeline_.stop();
}

std::size_t OrderEntryGateway::connection_count() const {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    return connections_.size();
}

OrderEntryIoMetrics OrderEntryGateway::io_metrics() const {
    return OrderEntryIoMetrics{
        .read_syscalls = read_syscalls_.load(std::memory_order_relaxed),
        .bytes_read = bytes_read_.load(std::memory_order_relaxed),
        .frames_decoded = frames_decoded_.load(std::memory_order_relaxed),
        .write_syscalls = write_syscalls_.load(std::memory_order_relaxed),
        .bytes_written = bytes_written_.load(std::memory_order_relaxed),
        .reports_enqueued = reports_enqueued_.load(std::memory_order_relaxed),
        .reports_written = reports_written_.load(std::memory_order_relaxed),
        .write_errors = write_errors_.load(std::memory_order_relaxed),
        .outbound_drops = outbound_drops_.load(std::memory_order_relaxed),
    };
}

bool OrderEntryGateway::submit_command(ExchangeCommand command) {
    return pipeline_.submit(std::move(command));
}

void OrderEntryGateway::io_loop() {
    const auto token = stop_source_.get_token();
    std::array<net::IoEvent, 64> events{};
    while (!token.stop_requested()) {
        const std::size_t n = poller_.wait(events, std::nullopt);
        if (token.stop_requested()) {
            break;
        }
        for (std::size_t i = 0; i < n; ++i) {
            const net::IoEvent& ev = events[i];
            if (ev.wake) {
                drain_dirty();
                continue;
            }
            if (ev.user == nullptr && listen_armed_ && (ev.readable || ev.error || ev.hangup)) {
                accept_ready();
                continue;
            }
            auto* conn = static_cast<Connection*>(ev.user);
            if (conn == nullptr || conn->closed.load(std::memory_order_acquire)) {
                continue;
            }
            if (ev.error || ev.hangup) {
                if (ev.readable) {
                    handle_read(*conn);
                } else {
                    close_connection(*conn);
                }
                continue;
            }
            if (ev.readable) {
                handle_read(*conn);
            }
            if (!conn->closed.load(std::memory_order_acquire) && ev.writable) {
                handle_write(*conn);
            }
        }
    }
}

void OrderEntryGateway::pause_listen() {
    if (!listen_armed_) {
        return;
    }
    poller_.remove(listener_.raw_fd());
    listen_armed_ = false;
}

void OrderEntryGateway::maybe_resume_listen() {
    if (listen_armed_ || stop_source_.get_token().stop_requested()) {
        return;
    }
    if (poller_.add(listener_.raw_fd(), net::IoInterest::Read, nullptr)) {
        listen_armed_ = true;
    }
}

void OrderEntryGateway::accept_ready() {
    using net::IoStatus;
    while (!stop_source_.get_token().stop_requested()) {
        auto accepted = listener_.accept();
        if (accepted.status == IoStatus::WouldBlock) {
            return;
        }
        if (accepted.status != IoStatus::Ok || !accepted.socket.has_value()) {
            pause_listen();
            return;
        }
        net::TcpSocket sock = std::move(*accepted.socket);
        if (!sock.set_non_blocking()) {
            pause_listen();
            return;
        }
        auto conn = std::make_unique<Connection>(next_session_id_, std::move(sock), options_.outbound_queue_capacity);
        Connection* ptr = conn.get();
        if (!poller_.add(ptr->socket.raw_fd(), net::IoInterest::Read, ptr)) {
            pause_listen();
            return;
        }
        ++next_session_id_;
        std::lock_guard<std::mutex> lock(connections_mutex_);
        connections_.push_back(std::move(conn));
    }
}

void OrderEntryGateway::handle_read(Connection& conn) {
    using namespace protocol::order_entry;
    using net::IoStatus;

    std::array<std::byte, 4096> chunk{};
    while (!conn.closed.load(std::memory_order_acquire)) {
        auto n = conn.socket.read(chunk);
        if (io_metrics_enabled_.load(std::memory_order_relaxed)) {
            read_syscalls_.fetch_add(1, std::memory_order_relaxed);
            if (n.ok()) {
                bytes_read_.fetch_add(n.n, std::memory_order_relaxed);
            }
        }
        if (n.status == IoStatus::WouldBlock) {
            return;
        }
        if (!n.ok() || n.n == 0) {
            close_connection(conn);
            return;
        }
        conn.read_buffer.insert(conn.read_buffer.end(), chunk.begin(),
                                 chunk.begin() + static_cast<std::ptrdiff_t>(n.n));

        while (true) {
            auto header_result = decode_header(conn.read_buffer);
            if (std::holds_alternative<DecodeError>(header_result)) {
                break;
            }
            const auto& header = std::get<Header>(header_result);
            const std::size_t frame_size = HEADER_SIZE + header.payload_size;
            if (conn.read_buffer.size() < frame_size) {
                break;
            }

            auto message_result = decode_message(std::span(conn.read_buffer).first(frame_size));
            conn.read_buffer.erase(conn.read_buffer.begin(),
                                    conn.read_buffer.begin() + static_cast<std::ptrdiff_t>(frame_size));
            if (std::holds_alternative<DecodeError>(message_result)) {
                continue;
            }
            const Message& message = std::get<Message>(message_result);
            if (io_metrics_enabled_.load(std::memory_order_relaxed)) {
                frames_decoded_.fetch_add(1, std::memory_order_relaxed);
            }
            latency::tracer().stamp_server_decoded(message);

            auto command = to_command(message);
            if (!command) {
                continue;
            }

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
}

std::optional<protocol::order_entry::Message> OrderEntryGateway::pop_outbound(Connection& conn) {
    {
        std::lock_guard<std::mutex> lock(conn.replay_mutex);
        if (!conn.replay_backlog.empty()) {
            auto message = std::move(conn.replay_backlog.front());
            conn.replay_backlog.erase(conn.replay_backlog.begin());
            return message;
        }
    }
    if (auto session = conn.session_outbound.try_pop()) {
        return session;
    }
    return conn.outbound.try_pop();
}

void OrderEntryGateway::handle_write(Connection& conn) {
    using namespace protocol::order_entry;
    using net::IoStatus;

    const std::size_t batch_limit = options_.writer_batch == 0 ? 1 : options_.writer_batch;

    while (!conn.closed.load(std::memory_order_acquire)) {
        if (conn.write_offset < conn.write_buf.size()) {
            auto n = conn.socket.write(std::span(conn.write_buf).subspan(conn.write_offset));
            if (io_metrics_enabled_.load(std::memory_order_relaxed)) {
                write_syscalls_.fetch_add(1, std::memory_order_relaxed);
                if (n.ok()) {
                    bytes_written_.fetch_add(n.n, std::memory_order_relaxed);
                }
            }
            if (n.status == IoStatus::WouldBlock) {
                enable_write(conn);
                return;
            }
            if (!n.ok() || n.n == 0) {
                if (io_metrics_enabled_.load(std::memory_order_relaxed)) {
                    write_errors_.fetch_add(1, std::memory_order_relaxed);
                }
                close_connection(conn);
                return;
            }
            conn.write_offset += n.n;
            if (conn.write_offset < conn.write_buf.size()) {
                enable_write(conn);
                return;
            }
            for (const auto& message : conn.write_pending) {
                latency::tracer().stamp_socket_written(message);
            }
            if (io_metrics_enabled_.load(std::memory_order_relaxed)) {
                reports_written_.fetch_add(conn.write_pending.size(), std::memory_order_relaxed);
            }
            conn.write_buf.clear();
            conn.write_offset = 0;
            conn.write_pending.clear();
            continue;
        }

        auto first = pop_outbound(conn);
        if (!first) {
            disable_write(conn);
            return;
        }
        conn.write_buf.clear();
        conn.write_offset = 0;
        conn.write_pending.clear();
        encode_message(*first, conn.write_buf);
        conn.write_pending.push_back(std::move(*first));
        while (conn.write_pending.size() < batch_limit) {
            auto more = pop_outbound(conn);
            if (!more) {
                break;
            }
            encode_message(*more, conn.write_buf);
            conn.write_pending.push_back(std::move(*more));
        }
    }
}

void OrderEntryGateway::close_connection(Connection& conn) {
    if (conn.closed.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    unbind_session(conn);
    poller_.remove(conn.socket.raw_fd());
    conn.socket.shutdown();
    maybe_resume_listen();
}

void OrderEntryGateway::enable_write(Connection& conn) {
    if (conn.closed.load(std::memory_order_acquire) || conn.write_interest) {
        return;
    }
    if (!poller_.mod(conn.socket.raw_fd(), net::IoInterest::Read | net::IoInterest::Write, &conn)) {
        close_connection(conn);
        return;
    }
    conn.write_interest = true;
}

void OrderEntryGateway::disable_write(Connection& conn) {
    if (!conn.write_interest) {
        return;
    }
    if (!poller_.mod(conn.socket.raw_fd(), net::IoInterest::Read, &conn)) {
        close_connection(conn);
        return;
    }
    conn.write_interest = false;
}

void OrderEntryGateway::drain_dirty() {
    while (auto conn = dirty_.try_pop()) {
        (*conn)->dirty.store(false, std::memory_order_release);
        if (!(*conn)->closed.load(std::memory_order_acquire)) {
            enable_write(**conn);
        }
    }
    if (dirty_overflow_.exchange(false, std::memory_order_acq_rel)) {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        for (auto& conn : connections_) {
            if (conn->dirty.exchange(false, std::memory_order_acq_rel) &&
                !conn->closed.load(std::memory_order_acquire)) {
                enable_write(*conn);
            }
        }
    }
}

void OrderEntryGateway::mark_dirty(Connection& conn) {
    bool expected = false;
    if (conn.dirty.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        if (!dirty_.try_push(&conn)) {
            dirty_overflow_.store(true, std::memory_order_release);
        }
    }
}

void OrderEntryGateway::notify_writer(Connection& conn) {
    mark_dirty(conn);
    poller_.wake();
}

// ── Session routing (matching thread + I/O thread) ───────────────────────

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
    notify_writer(conn); // there may now be a backlog to write out
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
    Message rejection_message{rejection};
    if (conn.session_outbound.try_push(rejection_message)) {
        if (io_metrics_enabled_.load(std::memory_order_relaxed)) {
            reports_enqueued_.fetch_add(1, std::memory_order_relaxed);
        }
        latency::tracer().stamp_writer_queued(rejection_message);
        notify_writer(conn);
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

void OrderEntryGateway::route_event(const ExchangeEvent& event) {
    if (options_.extra_event_sink) {
        options_.extra_event_sink(event); // e.g. market-data publishing -- see its own doc comment
    }

    auto reports = to_execution_reports(event);
    if (reports.empty()) {
        return; // e.g. a Book* event -- see to_execution_reports()'s own doc comment
    }

    // At most two connections (a trade's two sides). Collect wakeups so
    // notify_one() runs without sessions_mutex_.
    Connection* wake[2] = {nullptr, nullptr};
    int wake_count = 0;

    {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        for (auto& [key, message] : reports) {
            Connection* target = nullptr;

            if (auto owner = order_owner_.find(key);
                owner != order_owner_.end() && !owner->second->closed.load(std::memory_order_acquire)) {
                target = owner->second;
            } else {
                auto sessions = account_sessions_.find(key.account_id);
                if (sessions != account_sessions_.end()) {
                    for (Connection* session : sessions->second) {
                        if (!session->closed.load(std::memory_order_acquire)) {
                            target = session;
                            break;
                        }
                    }
                }
            }

            if (target != nullptr) {
                if (deliver(*target, std::move(message))) {
                    if (wake_count == 0 || (wake_count == 1 && wake[0] != target)) {
                        wake[wake_count++] = target;
                    }
                }
                continue;
            }

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

    for (int i = 0; i < wake_count; ++i) {
        notify_writer(*wake[i]);
    }
}

bool OrderEntryGateway::deliver(Connection& conn, protocol::order_entry::Message message) {
    // A full outbound queue means that connection's writer has fallen
    // behind; dropped rather than blocking -- this runs on the matching
    // thread. The writer is woken by the caller after sessions_mutex_ is
    // released.
    if (conn.outbound.try_push(message)) {
        if (io_metrics_enabled_.load(std::memory_order_relaxed)) {
            reports_enqueued_.fetch_add(1, std::memory_order_relaxed);
        }
        latency::tracer().stamp_first_event_if_unset(message);
        latency::tracer().stamp_writer_queued(message);
        return true;
    }
    if (io_metrics_enabled_.load(std::memory_order_relaxed)) {
        outbound_drops_.fetch_add(1, std::memory_order_relaxed);
    }
    return false;
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
                    .command_sequence = 0, // assigned on the matching thread, see MatchingPipeline
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
