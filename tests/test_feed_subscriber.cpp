#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <mutex>
#include <span>
#include <thread>
#include <vector>

#include "net/packet.hpp"
#include "net/udp_receiver.hpp"
#include "net/udp_socket.hpp"
#include "trader/market_data/feed_subscriber.hpp"
#include "trader/strategies/strategy_runtime.hpp"

// Tests for FeedSubscriber over a real UDP socket: datagrams are packed with
// the same net::pack_frames() the exchange's publisher uses and sent to the
// port the subscriber actually bound, so what is under test is the whole
// receive path (receive_batch -> unpack_frames -> apply_frame_result ->
// StrategyRuntime), not a stubbed one. Only the publisher is replaced -- by
// a bare UdpSocket -- because these tests are about what the subscriber does
// with datagrams, including malformed and out-of-sequence ones that a real
// publisher would never send.
using namespace mdh;
using namespace mdh::protocol;
using namespace mdh::trader::market_data;
using namespace mdh::trader::strategies;
using namespace std::chrono_literals;

namespace {

constexpr InstrumentId kInstrument = 1;

[[nodiscard]] Event add_order(Sequence sequence, OrderId order_id, Price price, Quantity quantity, Side side) {
    return Event{AddOrder{.sequence_number = sequence,
                           .timestamp_ns = sequence,
                           .order_id = order_id,
                           .instrument_id = kInstrument,
                           .price = price,
                           .quantity = quantity,
                           .side = side}};
}

[[nodiscard]] Event trade(Sequence sequence, Price price, Quantity quantity) {
    return Event{Trade{.sequence_number = sequence,
                        .timestamp_ns = sequence,
                        .instrument_id = kInstrument,
                        .price = price,
                        .quantity = quantity,
                        .aggressor_side = Side::Buy}};
}

// A subscriber on an ephemeral port plus a socket aimed at it. Packet
// sequence numbers are assigned here so the common case is gap-free; the
// gap test drives them explicitly instead.
class Publisher {
public:
    explicit Publisher(StrategyRuntime& runtime, FeedSubscriberOptions options = {})
        : subscriber_(0, runtime, options) {
        started_ = subscriber_.start();
    }

    [[nodiscard]] bool started() const { return started_; }
    [[nodiscard]] FeedSubscriber& subscriber() { return subscriber_; }

    void send(std::span<const Event> events, std::uint64_t packet_sequence) {
        const auto datagram = net::pack_frames(packet_sequence, events);
        ASSERT_TRUE(socket_.send_to(datagram, "127.0.0.1", *subscriber_.local_port()));
    }

    void send(std::span<const Event> events) { send(events, next_packet_sequence_++); }
    void send(const Event& event) { send(std::span<const Event>(&event, 1)); }

    void send_raw(std::span<const std::byte> datagram) {
        ASSERT_TRUE(socket_.send_to(datagram, "127.0.0.1", *subscriber_.local_port()));
    }

private:
    FeedSubscriber subscriber_;
    net::UdpSocket socket_;
    std::uint64_t next_packet_sequence_ = 1;
    bool started_ = false;
};

// UDP delivery and the receive thread are both asynchronous, so every
// assertion about received state has to be polled rather than assumed --
// the same wait_until() convention tests/test_ui_gateway.cpp uses.
[[nodiscard]] bool wait_until(const std::function<bool()>& predicate, std::chrono::milliseconds timeout = 2000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

} // namespace

TEST(FeedSubscriber, StartBindsAnEphemeralPortAndReportsIt) {
    StrategyRuntime runtime;
    FeedSubscriber subscriber(0, runtime);

    EXPECT_FALSE(subscriber.local_port().has_value()); // nothing bound before start()
    ASSERT_TRUE(subscriber.start());
    ASSERT_TRUE(subscriber.local_port().has_value());
    EXPECT_NE(*subscriber.local_port(), 0);
}

TEST(FeedSubscriber, StartFailsWithoutStartingAThreadWhenThePortIsAlreadyBound) {
    net::UdpReceiver occupant(0);
    const std::uint16_t taken_port = *occupant.local_port();

    StrategyRuntime runtime;
    FeedSubscriber subscriber(taken_port, runtime);

    EXPECT_FALSE(subscriber.start());
    EXPECT_FALSE(subscriber.local_port().has_value());
}

TEST(FeedSubscriber, DispatchesEveryReceivedEventToASubscribedStrategySink) {
    StrategyRuntime runtime;
    std::atomic<int> dispatches{0};
    runtime.subscribe(kInstrument, [&](InstrumentId, const book::OrderBook&) { dispatches.fetch_add(1); });

    Publisher publisher(runtime);
    ASSERT_TRUE(publisher.started());

    const std::vector<Event> events{add_order(1, 1, 100, 10, Side::Buy), add_order(2, 2, 102, 10, Side::Sell),
                                     trade(3, 102, 4)};
    publisher.send(events);

    EXPECT_TRUE(wait_until([&] { return dispatches.load() == 3; }));
}

TEST(FeedSubscriber, TheSinkSeesTheBookAlreadyUpdatedByTheEventThatWokeIt) {
    StrategyRuntime runtime;

    // Recorded under a mutex because the receive thread writes these while
    // the test thread reads them.
    std::mutex mutex;
    std::vector<Price> observed_best_bids;
    runtime.subscribe(kInstrument, [&](InstrumentId, const book::OrderBook& book) {
        if (const auto best_bid = book.best_bid()) {
            const std::lock_guard lock(mutex);
            observed_best_bids.push_back(best_bid->price);
        }
    });

    Publisher publisher(runtime);
    ASSERT_TRUE(publisher.started());

    // Two adds, each improving the bid: the sink must see 100 then 101, not
    // 101 twice (which is what dispatching after applying a whole batch,
    // rather than per event, would produce).
    const std::vector<Event> events{add_order(1, 1, 100, 10, Side::Buy), add_order(2, 2, 101, 10, Side::Buy)};
    publisher.send(events);

    ASSERT_TRUE(wait_until([&] {
        const std::lock_guard lock(mutex);
        return observed_best_bids.size() == 2;
    }));
    const std::lock_guard lock(mutex);
    EXPECT_EQ(observed_best_bids[0], 100);
    EXPECT_EQ(observed_best_bids[1], 101);
}

TEST(FeedSubscriber, SnapshotReportsTheReconstructedTouchAndTradeTotals) {
    StrategyRuntime runtime;
    Publisher publisher(runtime);
    ASSERT_TRUE(publisher.started());

    const std::vector<Event> events{add_order(1, 1, 9'998, 200, Side::Buy), add_order(2, 2, 10'002, 150, Side::Sell),
                                     trade(3, 10'002, 25), trade(4, 10'001, 15)};
    publisher.send(events);

    ASSERT_TRUE(wait_until([&] { return publisher.subscriber().snapshot(kInstrument).trade_count == 2; }));

    const auto snapshot = publisher.subscriber().snapshot(kInstrument);
    ASSERT_TRUE(snapshot.best_bid.has_value());
    EXPECT_EQ(snapshot.best_bid->price, 9'998);
    EXPECT_EQ(snapshot.best_bid->aggregate_quantity, 200u);
    ASSERT_TRUE(snapshot.best_ask.has_value());
    EXPECT_EQ(snapshot.best_ask->price, 10'002);
    EXPECT_EQ(snapshot.best_ask->aggregate_quantity, 150u);
    EXPECT_EQ(snapshot.traded_quantity, 40u); // turnover across both trades
    EXPECT_EQ(snapshot.last_trade_price, 10'001);
}

TEST(FeedSubscriber, SnapshotOfAnInstrumentThatHasNeverTradedIsEmptyRatherThanAnError) {
    StrategyRuntime runtime;
    Publisher publisher(runtime);
    ASSERT_TRUE(publisher.started());

    const auto snapshot = publisher.subscriber().snapshot(kInstrument + 99);
    EXPECT_FALSE(snapshot.best_bid.has_value());
    EXPECT_FALSE(snapshot.best_ask.has_value());
    EXPECT_EQ(snapshot.trade_count, 0u);
    EXPECT_FALSE(snapshot.mark_price().has_value());
}

TEST(FeedSubscriber, StatsCountEventsByTypeAsTheyAreApplied) {
    StrategyRuntime runtime;
    Publisher publisher(runtime);
    ASSERT_TRUE(publisher.started());

    const std::vector<Event> events{add_order(1, 1, 100, 10, Side::Buy), add_order(2, 2, 102, 10, Side::Sell),
                                     trade(3, 102, 4),
                                     Event{CancelOrder{.sequence_number = 4, .timestamp_ns = 4, .order_id = 1, .instrument_id = kInstrument}}};
    publisher.send(events);

    ASSERT_TRUE(wait_until([&] { return publisher.subscriber().stats().messages_processed == 4; }));

    const auto stats = publisher.subscriber().stats();
    EXPECT_EQ(stats.adds, 2u);
    EXPECT_EQ(stats.trades, 1u);
    EXPECT_EQ(stats.cancels, 1u);
    EXPECT_EQ(stats.sequence_failures, 0u);
    EXPECT_EQ(stats.decode_failures, 0u);
    EXPECT_EQ(publisher.subscriber().packets_received(), 1u);
    EXPECT_EQ(publisher.subscriber().packet_errors(), 0u);
}

TEST(FeedSubscriber, AGapInEventSequenceIsCountedAndReconstructionContinues) {
    StrategyRuntime runtime;
    Publisher publisher(runtime);
    ASSERT_TRUE(publisher.started());

    publisher.send(add_order(1, 1, 100, 10, Side::Buy));
    ASSERT_TRUE(wait_until([&] { return publisher.subscriber().stats().messages_processed == 1; }));

    // Sequence 2 never arrives. The gap is counted, and -- the point of the
    // test -- the event that revealed it is still applied, so the book does
    // not freeze at the moment of the loss. See FeedSubscriberOptions::
    // replay_options on why this subscriber does not take replay's own
    // stop-at-the-first-gap default.
    publisher.send(add_order(3, 2, 102, 10, Side::Sell));
    ASSERT_TRUE(wait_until([&] { return publisher.subscriber().stats().sequence_failures == 1; }));
    EXPECT_TRUE(wait_until([&] { return publisher.subscriber().snapshot(kInstrument).best_ask.has_value(); }));

    // And reconstruction is still live afterwards, rather than having been
    // left permanently stale by the one gap: a later, in-sequence event is
    // applied normally and the gap is not re-reported.
    publisher.send(add_order(4, 3, 101, 10, Side::Buy));
    EXPECT_TRUE(wait_until([&] {
        const auto market = publisher.subscriber().snapshot(kInstrument);
        return market.best_bid.has_value() && market.best_bid->price == 101;
    }));
    EXPECT_EQ(publisher.subscriber().stats().sequence_failures, 1u);
}

TEST(FeedSubscriber, AMalformedDatagramIsCountedAndTheThreadKeepsReceiving) {
    StrategyRuntime runtime;
    Publisher publisher(runtime);
    ASSERT_TRUE(publisher.started());

    const std::vector<std::byte> garbage(16, std::byte{0xAB});
    publisher.send_raw(garbage);
    ASSERT_TRUE(wait_until([&] { return publisher.subscriber().packet_errors() == 1; }));

    // The point of the test: a bad packet must not be fatal to the feed.
    publisher.send(add_order(1, 1, 100, 10, Side::Buy));
    EXPECT_TRUE(wait_until([&] { return publisher.subscriber().snapshot(kInstrument).best_bid.has_value(); }));
}

TEST(FeedSubscriber, StopIsIdempotentAndSafeToCallBeforeTheDestructor) {
    StrategyRuntime runtime;
    FeedSubscriber subscriber(0, runtime);
    ASSERT_TRUE(subscriber.start());

    subscriber.stop();
    subscriber.stop(); // and again, which the destructor will also do
    SUCCEED();
}

TEST(MarketSnapshot, MarkPricePrefersTheMidpointThenFallsBackToTheLastTrade) {
    MarketSnapshot snapshot;
    EXPECT_FALSE(snapshot.mark_price().has_value()); // nothing quoted, nothing traded

    snapshot.last_trade_price = 100;
    snapshot.trade_count = 1;
    EXPECT_EQ(snapshot.mark_price().value_or(0), 100); // one-sided book, but it has traded

    snapshot.best_bid = book::PriceLevelView{.price = 110, .aggregate_quantity = 10, .order_count = 1};
    EXPECT_EQ(snapshot.mark_price().value_or(0), 100); // a bid alone is still no midpoint

    // Deliberately a midpoint that disagrees with the last trade, so this
    // asserts which of the two won rather than that they happened to match.
    snapshot.best_ask = book::PriceLevelView{.price = 112, .aggregate_quantity = 10, .order_count = 1};
    EXPECT_EQ(snapshot.mark_price().value_or(0), 111);
}
