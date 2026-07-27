#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <thread>

#include "net/packet.hpp"
#include "net/udp_listener.hpp"
#include "net/udp_socket.hpp"

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

constexpr auto IDLE_TIMEOUT = std::chrono::milliseconds(300);
constexpr auto SETTLE_BEFORE_SEND = std::chrono::milliseconds(50);

std::future<UdpListenResult> start_listener(std::uint16_t port, const ReplayOptions& options = {}) {
    return std::async(std::launch::async, [port, options] { return run_udp_listen(port, options, IDLE_TIMEOUT); });
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
