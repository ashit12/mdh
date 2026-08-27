#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <stop_token>
#include <thread>

#include "book/order_book.hpp"
#include "common/types.hpp"
#include "net/udp_receiver.hpp"
#include "replay/replay_engine.hpp"
#include "trader/strategies/strategy_runtime.hpp"

// A trader-side subscriber to a live UDP market-data feed: it owns the
// background thread that turns datagrams into a reconstructed book and then
// tells a StrategyRuntime about every event it applied.
//
// ── What this adds, given every piece of it already existed ───────────────
// Nothing in the receive path is new. The loop is
//
//   UdpReceiver::receive_batch() -> net::unpack_frames()
//     -> replay::apply_frame_result()   (sequence validation + book update)
//     -> strategies::StrategyRuntime::on_event()
//
// and the first three steps are exactly what apps/market_data_replay's
// --listen mode and ui_gateway::UiGateway's own market-data thread already
// do -- funnelling through the same shared apply_frame_result(), so decode
// errors, gap classification and book application behave identically to
// file replay. What was missing was the fourth step. StrategyRuntime's own
// header says its on_event() "is meant to be called by whatever already
// drives BookManager (a live UDP listener loop, or a test)"; until now the
// only live driver was UiGateway, which has no strategies to notify, so a
// live strategy had to reach the book some other way (apps/
// live_strategy_demo polls UiGateway's REST endpoint instead, and says so).
// This class is that missing driver, and it is deliberately a separate,
// reusable component rather than another private loop inside an app.
//
// ── Why not net::run_udp_listen(), which already runs this pipeline ───────
// That function is a whole *run*, not a subscription: it owns both a
// producer and a consumer thread, blocks its caller until the feed goes
// idle, and returns aggregate statistics. That shape is right for a replay
// tool measuring a finite feed and wrong for a long-lived participant,
// which needs to start, be queried while running, and stop on request. The
// two-thread DroppingQueue pipeline it builds also exists to make
// backpressure observable, which is not this class's subject; one thread is
// enough here, same as UiGateway's own market-data thread.
//
// ── Threading ─────────────────────────────────────────────────────────────
// One receive thread. It holds books_mutex_ across applying a batch *and*
// dispatching that batch's events to strategies, because a subscribed sink
// is handed a reference to the live book and must not have it mutated
// underneath. A strategy sink therefore runs with the mutex held and will
// briefly block a concurrent snapshot() -- including across its own
// blocking socket write, if it decides to send an order. That is accepted
// deliberately: snapshot() serves a status display, so a few hundred
// microseconds of delay costs nothing, whereas handing strategies a copied
// book on every event would cost real work on every event.
namespace mdh::trader::market_data {

struct FeedSubscriberOptions {
    // Datagrams drained per receive_batch() call -- same default as
    // UiGateway's own market-data loop.
    std::size_t receive_batch_size = 64;

    // How long the receive thread sleeps when a batch comes back empty,
    // rather than spinning on a non-blocking socket.
    std::chrono::milliseconds idle_sleep{1};

    // Passed to UdpReceiver -- see net/udp_receiver.hpp.
    std::size_t max_datagram_size = 2048;

    // Anomaly policy, forwarded to apply_frame_result(). Deliberately the
    // opposite of replay::ReplayOptions' own defaults, which stop at the
    // first gap or decode error.
    //
    // That default is right for file replay, where an anomaly means the
    // recorded file is corrupt and continuing past it produces a book that
    // is quietly wrong. It is wrong for a live UDP feed, where a gap means a
    // datagram was dropped -- routine, and not a reason to stop. Worse, it
    // would be silently permanent: apply_frame_result() does not apply the
    // event that revealed the gap, so a subscriber that kept receiving would
    // hold a book frozen at the moment of the first lost packet while
    // continuing to hand it to strategies as though it were current. A
    // counted gap and a book that carries on from whatever arrives next is
    // strictly better than a stale book presented as live, and
    // SequenceValidator advances its high-water mark past a gap, so one lost
    // datagram is reported once rather than re-reported forever.
    //
    // Set recovery_snapshot_path to re-baseline from a snapshot on a gap
    // instead; there is no snapshot-serving infrastructure in this project
    // to point it at by default.
    replay::ReplayOptions replay_options{.stop_on_sequence_error = false, .stop_on_decode_error = false};
};

// Everything a participant or a display needs to know about one
// instrument's market, copied out from under the reconstruction lock so a
// caller never formats or decides anything while holding it.
struct MarketSnapshot {
    std::optional<book::PriceLevelView> best_bid;
    std::optional<book::PriceLevelView> best_ask;
    std::uint64_t trade_count = 0;
    Quantity traded_quantity = 0;
    Price last_trade_price = 0;

    // The price to value an open position at: the book's midpoint when both
    // sides are quoted, else the last trade price, else nothing. A one-sided
    // book has no midpoint but a market that has traded still has a last
    // price, which is a better mark than refusing to produce one.
    [[nodiscard]] std::optional<Price> mark_price() const;
};

class FeedSubscriber {
public:
    // Does not own `runtime`, which must outlive this object -- the same
    // non-owning-reference convention exchange::risk::RiskGatedEngine
    // documents for the collaborators it holds.
    FeedSubscriber(std::uint16_t port, strategies::StrategyRuntime& runtime, FeedSubscriberOptions options = {});

    // Calls stop().
    ~FeedSubscriber();

    FeedSubscriber(const FeedSubscriber&) = delete;
    FeedSubscriber& operator=(const FeedSubscriber&) = delete;
    FeedSubscriber(FeedSubscriber&&) = delete;
    FeedSubscriber& operator=(FeedSubscriber&&) = delete;

    // Binds the UDP port and starts the receive thread. Returns false,
    // starting nothing, if the bind fails -- so a caller can report a port
    // clash instead of silently receiving nothing (which needs more than
    // UdpReceiver::is_open() to detect; see the implementation). Binding here rather than
    // in the constructor is what makes that failure reportable, and the
    // bound socket is then moved onto the thread rather than re-bound there,
    // avoiding the narrow bind/rebind race UiGateway::start() documents.
    [[nodiscard]] bool start();

    // Requests the receive thread stop and joins it. Safe to call more than
    // once, including via the destructor.
    void stop();

    // Meaningful after a successful start(); the only way to learn the port
    // when constructed with 0 (which is what tests want, for a
    // collision-free port).
    [[nodiscard]] std::optional<std::uint16_t> local_port() const { return bound_port_; }

    [[nodiscard]] MarketSnapshot snapshot(InstrumentId instrument_id) const;

    // Feed health, for a status display: the same counters file replay
    // reports, plus the datagram-level ones the frame pipeline never sees.
    [[nodiscard]] replay::ReplayStats stats() const;
    [[nodiscard]] std::uint64_t packets_received() const;
    [[nodiscard]] std::uint64_t packet_errors() const;

private:
    // Not taking std::stop_token as its first parameter, deliberately: that
    // makes std::jthread inject its own internal token instead of the one
    // passed here, which is the exact bug ui_gateway.hpp documents. The
    // shared stop_source_ is passed explicitly instead.
    void receive_loop(std::stop_token token, net::UdpReceiver receiver);

    std::uint16_t port_;
    strategies::StrategyRuntime& runtime_;
    FeedSubscriberOptions options_;

    mutable std::mutex books_mutex_;
    replay::ReplayOutcome outcome_; // .books is the live-reconstructed BookManager
    std::uint64_t packets_received_ = 0;
    std::uint64_t packet_errors_ = 0;

    std::optional<std::uint16_t> bound_port_;
    std::stop_source stop_source_;
    std::jthread receive_thread_;
};

} // namespace mdh::trader::market_data
