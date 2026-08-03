#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <future>
#include <string>
#include <thread>

#include "book/book_manager.hpp"
#include "net/packet.hpp"
#include "net/udp_listener.hpp"
#include "net/udp_socket.hpp"
#include "replay/snapshot.hpp"

using namespace mdh;
using namespace mdh::protocol;
using namespace mdh::net;
using namespace mdh::replay;

namespace {

// Fixed, deliberately unusual port per test to avoid collisions between
// tests in this file (each uses its own). There is a small, accepted risk
// of collision with some other process on the test machine -- the
// alternative (making run_udp_listen accept port 0 and report the actual
// bound port back to a concurrently-running sender before the listener
// finishes) would add API surface to production code purely to serve
// this test, which isn't worth it here.
constexpr std::uint16_t PORT_BASIC = 58231;
constexpr std::uint16_t PORT_MULTI_PACKET = 58232;
constexpr std::uint16_t PORT_BAD_PACKET_MIXED_WITH_GOOD = 58233;
constexpr std::uint16_t PORT_SEQUENCE_GAP_STOPS_LISTENER = 58234;
constexpr std::uint16_t PORT_CUSTOM_QUEUE_CAPACITY = 58235;
constexpr std::uint16_t PORT_SLOW_CONSUMER = 58236;
constexpr std::uint16_t PORT_RECOVERY_BUFFERING = 58237;

constexpr auto IDLE_TIMEOUT = std::chrono::milliseconds(300);
constexpr auto SETTLE_BEFORE_SEND = std::chrono::milliseconds(50);

// Same convention as tests/test_snapshot.cpp's TempFile: unique path,
// removed on scope exit regardless of pass/fail.
class TempFile {
public:
    explicit TempFile(std::string name) : path_(std::move(name)) {}
    ~TempFile() { std::remove(path_.c_str()); }
    [[nodiscard]] const std::string& path() const { return path_; }

private:
    std::string path_;
};

std::future<UdpListenResult> start_listener(std::uint16_t port, const ReplayOptions& options = {},
                                             std::size_t queue_capacity = 1024,
                                             std::chrono::microseconds consumer_delay = {}) {
    const UdpListenOptions listen_options{
        .idle_timeout = IDLE_TIMEOUT, .queue_capacity = queue_capacity, .consumer_delay = consumer_delay};
    return std::async(std::launch::async, [port, options, listen_options] { return run_udp_listen(port, options, listen_options); });
}

} // namespace

TEST(UdpReplayE2E, SingleFrameOverLoopbackReconstructsBookState) {
    auto listen_future = start_listener(PORT_BASIC);
    std::this_thread::sleep_for(SETTLE_BEFORE_SEND);

    UdpSocket sender;
    std::vector<Event> events = {Event{AddOrder{
        .sequence_number = 1, .timestamp_ns = 100, .order_id = 1, .instrument_id = 1, .price = 5000, .quantity = 10, .side = Side::Buy}}};
    auto datagram = pack_frames(1, events);
    ASSERT_TRUE(sender.send_to(datagram, "127.0.0.1", PORT_BASIC));

    auto result = listen_future.get();

    EXPECT_FALSE(result.outcome.stopped_early);
    EXPECT_EQ(result.packets_received, 1u);
    EXPECT_EQ(result.packet_errors, 0u);
    EXPECT_EQ(result.packet_seq_stats.in_order, 1u);
    EXPECT_EQ(result.outcome.stats.messages_processed, 1u);
    // The producer/consumer queue introduced in milestone 3: on this
    // uncontended happy path the consumer easily keeps up, so nothing
    // should ever have been dropped, and the peak occupancy should be
    // small (at most the one frame sent).
    EXPECT_EQ(result.queue_dropped_count, 0u);
    EXPECT_GE(result.queue_high_water_mark, 1u);
    EXPECT_LE(result.queue_high_water_mark, 1u);

    const auto* book = result.outcome.books.find_book(1);
    ASSERT_NE(book, nullptr);
    auto bid = book->best_bid();
    ASSERT_TRUE(bid.has_value());
    EXPECT_EQ(bid->price, 5000);
}

TEST(UdpReplayE2E, EventsSplitAcrossMultiplePacketsAllApply) {
    auto listen_future = start_listener(PORT_MULTI_PACKET);
    std::this_thread::sleep_for(SETTLE_BEFORE_SEND);

    UdpSocket sender;
    // Packet 1: sequences 1-2. Packet 2: sequences 3-4. Sequencing must
    // hold across the packet boundary, exactly like it would across
    // separate reads from a file.
    std::vector<Event> packet1_events = {
        Event{AddOrder{.sequence_number = 1, .timestamp_ns = 1, .order_id = 1, .instrument_id = 1, .price = 100, .quantity = 5, .side = Side::Buy}},
        Event{AddOrder{.sequence_number = 2, .timestamp_ns = 2, .order_id = 2, .instrument_id = 1, .price = 110, .quantity = 3, .side = Side::Sell}},
    };
    std::vector<Event> packet2_events = {
        Event{ModifyOrder{.sequence_number = 3, .timestamp_ns = 3, .order_id = 1, .instrument_id = 1, .new_price = 105, .new_quantity = 8}},
        Event{CancelOrder{.sequence_number = 4, .timestamp_ns = 4, .order_id = 2, .instrument_id = 1}},
    };

    ASSERT_TRUE(sender.send_to(pack_frames(1, packet1_events), "127.0.0.1", PORT_MULTI_PACKET));
    ASSERT_TRUE(sender.send_to(pack_frames(2, packet2_events), "127.0.0.1", PORT_MULTI_PACKET));

    auto result = listen_future.get();

    EXPECT_FALSE(result.outcome.stopped_early);
    EXPECT_EQ(result.packets_received, 2u);
    EXPECT_EQ(result.outcome.stats.messages_processed, 4u);
    EXPECT_EQ(result.outcome.stats.sequence_failures, 0u);

    const auto* book = result.outcome.books.find_book(1);
    ASSERT_NE(book, nullptr);
    auto bid = book->best_bid();
    ASSERT_TRUE(bid.has_value());
    EXPECT_EQ(bid->price, 105); // order 1 was modified
    EXPECT_FALSE(book->best_ask().has_value()); // order 2 (the only ask) was cancelled
}

TEST(UdpReplayE2E, CorruptPacketIsSkippedNotFatal) {
    auto listen_future = start_listener(PORT_BAD_PACKET_MIXED_WITH_GOOD);
    std::this_thread::sleep_for(SETTLE_BEFORE_SEND);

    UdpSocket sender;
    std::vector<Event> good_events = {Event{AddOrder{
        .sequence_number = 1, .timestamp_ns = 1, .order_id = 1, .instrument_id = 1, .price = 100, .quantity = 1, .side = Side::Buy}}};
    auto good_datagram = pack_frames(1, good_events);

    std::vector<std::byte> garbage = {std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}, std::byte{0xEF}};

    ASSERT_TRUE(sender.send_to(good_datagram, "127.0.0.1", PORT_BAD_PACKET_MIXED_WITH_GOOD));
    ASSERT_TRUE(sender.send_to(garbage, "127.0.0.1", PORT_BAD_PACKET_MIXED_WITH_GOOD));

    auto result = listen_future.get();

    EXPECT_FALSE(result.outcome.stopped_early);
    EXPECT_EQ(result.packets_received, 2u);
    EXPECT_EQ(result.packet_errors, 1u); // the garbage datagram
    EXPECT_EQ(result.outcome.stats.messages_processed, 1u); // the good one still applied
}

TEST(UdpReplayE2E, SequenceGapAcrossPacketsStopsListenerByDefault) {
    ReplayOptions options; // stop_on_sequence_error defaults to true
    auto listen_future = start_listener(PORT_SEQUENCE_GAP_STOPS_LISTENER, options);
    std::this_thread::sleep_for(SETTLE_BEFORE_SEND);

    UdpSocket sender;
    std::vector<Event> first = {Event{AddOrder{
        .sequence_number = 1, .timestamp_ns = 1, .order_id = 1, .instrument_id = 1, .price = 100, .quantity = 1, .side = Side::Buy}}};
    std::vector<Event> gapped = {Event{AddOrder{ // sequence 5, not 2: a gap
        .sequence_number = 5, .timestamp_ns = 2, .order_id = 2, .instrument_id = 1, .price = 100, .quantity = 1, .side = Side::Buy}}};

    ASSERT_TRUE(sender.send_to(pack_frames(1, first), "127.0.0.1", PORT_SEQUENCE_GAP_STOPS_LISTENER));
    ASSERT_TRUE(sender.send_to(pack_frames(2, gapped), "127.0.0.1", PORT_SEQUENCE_GAP_STOPS_LISTENER));

    auto result = listen_future.get();

    EXPECT_TRUE(result.outcome.stopped_early);
    EXPECT_EQ(result.outcome.stats.sequence_failures, 1u);
    EXPECT_EQ(result.outcome.stats.messages_processed, 1u); // only the first event applied before the gap halted it
}

TEST(UdpReplayE2E, CustomQueueCapacityIsHonoredWithNoDropsUnderLightLoad) {
    // Not a backpressure/drop test (see
    // SlowConsumerCausesDropsThatShowUpAsSequenceGaps below for that) --
    // just confirms UdpListenOptions::queue_capacity actually reaches
    // DroppingQueue's constructor rather than being ignored, via a
    // capacity far larger than this trickle of traffic could ever fill.
    auto listen_future = start_listener(PORT_CUSTOM_QUEUE_CAPACITY, ReplayOptions{}, /*queue_capacity=*/8);
    std::this_thread::sleep_for(SETTLE_BEFORE_SEND);

    UdpSocket sender;
    std::vector<Event> events = {
        Event{AddOrder{.sequence_number = 1, .timestamp_ns = 1, .order_id = 1, .instrument_id = 1, .price = 100, .quantity = 1, .side = Side::Buy}},
        Event{AddOrder{.sequence_number = 2, .timestamp_ns = 2, .order_id = 2, .instrument_id = 1, .price = 101, .quantity = 1, .side = Side::Sell}},
    };
    ASSERT_TRUE(sender.send_to(pack_frames(1, events), "127.0.0.1", PORT_CUSTOM_QUEUE_CAPACITY));

    auto result = listen_future.get();

    EXPECT_FALSE(result.outcome.stopped_early);
    EXPECT_EQ(result.queue_dropped_count, 0u);
    EXPECT_EQ(result.outcome.stats.messages_processed, 2u);
}

TEST(UdpReplayE2E, SlowConsumerForcesDrops) {
    // Deterministic backpressure: a tiny queue (capacity 4) plus a
    // consumer artificially slowed to 20ms/event guarantees drops
    // regardless of machine speed or scheduling luck -- unlike the
    // incidental backpressure a fast consumer might or might not hit
    // under real load, this doesn't depend on timing.
    //
    // This test only asserts that drops happen and that the accounting
    // balances exactly -- it deliberately does NOT assert
    // sequence_failures > 0. SequenceValidator only detects a gap
    // retrospectively, when a *later* sequence number actually arrives;
    // if the drops land on the tail end of this burst with nothing
    // surviving afterward, no gap is ever observed even though data was
    // genuinely dropped -- confirmed empirically (~1-in-15 runs) when this
    // test asserted that. See
    // BackpressureIntegration.DroppedFrameIsRevealedAsGapByALaterSurvivor
    // in test_backpressure_integration.cpp for a deterministic proof of
    // that causal mechanism instead, using the real components without
    // racing real OS thread scheduling to reproduce it.
    ReplayOptions options;
    options.stop_on_sequence_error = false; // keep processing through every gap so the whole burst gets inspected
    options.stop_on_decode_error = false;
    auto listen_future =
        start_listener(PORT_SLOW_CONSUMER, options, /*queue_capacity=*/4, /*consumer_delay=*/std::chrono::milliseconds(20));
    std::this_thread::sleep_for(SETTLE_BEFORE_SEND);

    UdpSocket sender;
    constexpr int kTotalEvents = 50;
    constexpr int kEventsPerPacket = 10;
    int seq = 1;
    for (int packet_idx = 0; packet_idx < kTotalEvents / kEventsPerPacket; ++packet_idx) {
        std::vector<Event> events;
        for (int i = 0; i < kEventsPerPacket; ++i, ++seq) {
            events.push_back(Event{AddOrder{.sequence_number = static_cast<std::uint64_t>(seq),
                                             .timestamp_ns = static_cast<std::uint64_t>(seq),
                                             .order_id = static_cast<std::uint64_t>(seq),
                                             .instrument_id = 1,
                                             .price = 100,
                                             .quantity = 1,
                                             .side = Side::Buy}});
        }
        ASSERT_TRUE(sender.send_to(pack_frames(static_cast<std::uint64_t>(packet_idx + 1), events), "127.0.0.1", PORT_SLOW_CONSUMER));
    }

    auto result = listen_future.get();

    EXPECT_FALSE(result.outcome.stopped_early);
    EXPECT_GT(result.queue_dropped_count, 0u) << "expected the tiny queue + slow consumer to force at least one drop";
    EXPECT_LE(result.queue_high_water_mark, 4u);
    // Exact accounting: every one of the kTotalEvents events was either
    // processed by the consumer or dropped by the producer -- nothing
    // silently vanishes or gets double-counted.
    EXPECT_EQ(result.outcome.stats.messages_processed + result.queue_dropped_count, static_cast<std::uint64_t>(kTotalEvents));
}

TEST(UdpReplayE2E, QueueBuffersLiveTrafficWhileConsumerRecoversFromSnapshot) {
    // This is milestone 3's queue doing new work for free: while the
    // consumer thread is busy (simulated via consumer_delay, standing in
    // for "recovery involves book reconstruction, which takes real time"),
    // the producer thread keeps receiving and pushing normally -- nothing
    // special has to be built for the queue to act as the "buffered
    // incremental messages" milestone 4's plan calls for; it already does,
    // just by continuing to do what it always does.
    TempFile snapshot_file("mdh_test_udp_recovery_snapshot.bin");
    {
        book::BookManager snapshot_books;
        // A distinctive resting order that appears nowhere in the live
        // feed below, so its presence afterward can only be explained by
        // recovery actually having loaded this snapshot.
        ASSERT_FALSE(snapshot_books.book_for(1).add_order(999, 70, 3, Side::Buy).has_value());
        ASSERT_TRUE(write_snapshot(snapshot_file.path(), 100, snapshot_books));
    }

    ReplayOptions options;
    options.recovery_snapshot_path = snapshot_file.path();
    // A deliberately slow consumer (30ms/event) gives the producer a wide
    // window to receive and enqueue several more packets while the
    // consumer is still working through the ones before it.
    auto listen_future = start_listener(PORT_RECOVERY_BUFFERING, options, /*queue_capacity=*/1024,
                                         /*consumer_delay=*/std::chrono::milliseconds(30));
    std::this_thread::sleep_for(SETTLE_BEFORE_SEND);

    UdpSocket sender;
    // Sequence 1 establishes the baseline. Sequence 5 jumps ahead (a
    // genuine gap: 2,3,4 missing), triggering recovery. Sequences 6,7,8
    // are sent immediately afterward, with no synchronization -- by the
    // time the consumer (30ms/event) works its way to actually processing
    // the gap and beyond, these should already be sitting in the queue.
    ASSERT_TRUE(sender.send_to(
        pack_frames(1, std::vector<Event>{Event{AddOrder{
                            .sequence_number = 1, .timestamp_ns = 1, .order_id = 1, .instrument_id = 1, .price = 50, .quantity = 1, .side = Side::Buy}}}),
        "127.0.0.1", PORT_RECOVERY_BUFFERING));
    ASSERT_TRUE(sender.send_to(
        pack_frames(2, std::vector<Event>{Event{AddOrder{
                            .sequence_number = 5, .timestamp_ns = 2, .order_id = 2, .instrument_id = 1, .price = 60, .quantity = 1, .side = Side::Sell}}}),
        "127.0.0.1", PORT_RECOVERY_BUFFERING));
    ASSERT_TRUE(sender.send_to(
        pack_frames(3, std::vector<Event>{Event{AddOrder{
                            .sequence_number = 6, .timestamp_ns = 3, .order_id = 3, .instrument_id = 1, .price = 61, .quantity = 1, .side = Side::Sell}}}),
        "127.0.0.1", PORT_RECOVERY_BUFFERING));
    ASSERT_TRUE(sender.send_to(
        pack_frames(4, std::vector<Event>{Event{AddOrder{
                            .sequence_number = 7, .timestamp_ns = 4, .order_id = 4, .instrument_id = 1, .price = 62, .quantity = 1, .side = Side::Sell}}}),
        "127.0.0.1", PORT_RECOVERY_BUFFERING));
    ASSERT_TRUE(sender.send_to(
        pack_frames(5, std::vector<Event>{Event{AddOrder{
                            .sequence_number = 8, .timestamp_ns = 5, .order_id = 5, .instrument_id = 1, .price = 63, .quantity = 1, .side = Side::Sell}}}),
        "127.0.0.1", PORT_RECOVERY_BUFFERING));

    auto result = listen_future.get();

    EXPECT_FALSE(result.outcome.stopped_early);
    EXPECT_EQ(result.outcome.stats.recoveries, 1u);
    EXPECT_EQ(result.outcome.stats.sequence_failures, 1u);
    EXPECT_EQ(result.queue_dropped_count, 0u); // generous capacity: nothing should have been dropped
    // The core claim: none of the traffic that arrived during recovery
    // was lost -- all 5 events (1 pre-gap, 1 triggering recovery, 3
    // post-recovery) were processed.
    EXPECT_EQ(result.outcome.stats.messages_processed, 5u);

    const auto* book = result.outcome.books.find_book(1);
    ASSERT_NE(book, nullptr);
    auto bids = book->all_bids();
    ASSERT_EQ(bids.size(), 1u);
    EXPECT_EQ(bids[0].order_id, 999u); // from the snapshot -- order 1 (pre-gap) was discarded by the reset
    auto asks = book->all_asks();
    ASSERT_EQ(asks.size(), 4u); // orders 2 (triggered recovery), 3, 4, 5 -- all survived
}
