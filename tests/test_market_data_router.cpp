#include <gtest/gtest.h>

#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include "exchange/market_data/market_data_router.hpp"

namespace mdh::exchange::market_data {
namespace {

using namespace std::chrono_literals;

BookOrderAdded added(EventSequence sequence, ExchangeOrderId order_id) {
    return BookOrderAdded{
        .event_sequence = sequence,
        .instrument_id = 1,
        .exchange_order_id = order_id,
        .side = Side::Buy,
        .price = 100,
        .quantity = 1,
    };
}

template <typename Predicate>
bool wait_until(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 1s;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

} // namespace

TEST(MarketDataRouter, RoutesPublicEventsInOrderAndSkipsPrivateEvents) {
    std::mutex mutex;
    std::vector<protocol::Event> routed;
    MarketDataRouter router(
        [&](std::span<const protocol::Event> events) {
            std::lock_guard<std::mutex> lock(mutex);
            routed.insert(routed.end(), events.begin(), events.end());
        },
        {}, MarketDataPublisherOptions{.clock = [] { return 1; }});
    router.start();

    router.publish(ExchangeEvent{added(1, 10)});
    router.publish(ExchangeEvent{OrderRejected{
        .event_sequence = 2,
        .command_sequence = 1,
        .account_id = 7,
        .client_order_id = 8,
        .instrument_id = 1,
        .reason = RejectReason::InvalidPrice,
    }});
    router.publish(ExchangeEvent{added(3, 11)});
    ASSERT_TRUE(wait_until([&] { return router.routed_count() == 2; }));
    router.stop();

    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ(routed.size(), 2u);
    EXPECT_EQ(std::get<protocol::AddOrder>(routed[0]).sequence_number, 1u);
    EXPECT_EQ(std::get<protocol::AddOrder>(routed[1]).sequence_number, 2u);
    EXPECT_EQ(router.dropped_count(), 0u);
}

TEST(MarketDataRouter, FullQueueDropsWithoutHidingTheFeedSequenceGap) {
    std::mutex mutex;
    std::vector<protocol::Event> routed;
    MarketDataRouter router(
        [&](std::span<const protocol::Event> events) {
            std::lock_guard<std::mutex> lock(mutex);
            routed.insert(routed.end(), events.begin(), events.end());
        },
        MarketDataRouterOptions{.queue_capacity = 1},
        MarketDataPublisherOptions{.clock = [] { return 1; }});

    router.publish(ExchangeEvent{added(1, 10)}); // sequence 1: queued
    router.publish(ExchangeEvent{added(2, 11)}); // sequence 2: dropped
    EXPECT_EQ(router.dropped_count(), 1u);

    router.start();
    ASSERT_TRUE(wait_until([&] { return router.routed_count() == 1; }));
    router.publish(ExchangeEvent{added(3, 12)}); // sequence 3: queued
    router.stop();                              // drains before joining

    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ(routed.size(), 2u);
    EXPECT_EQ(std::get<protocol::AddOrder>(routed[0]).sequence_number, 1u);
    EXPECT_EQ(std::get<protocol::AddOrder>(routed[1]).sequence_number, 3u);
    EXPECT_EQ(router.queue_high_water_mark(), 1u);
}

// The point of the batch sink: several events already waiting become one
// downstream call, and therefore one datagram, rather than one call each.
// Publishing before start() is what makes this deterministic -- the events
// queue up with no consumer, so the routing thread's first drain sees all of
// them at once, which is exactly the situation batching exists for.
TEST(MarketDataRouter, DrainsEveryQueuedEventIntoOneDownstreamCall) {
    std::mutex mutex;
    std::vector<std::size_t> batch_sizes;
    std::vector<protocol::Event> routed;
    MarketDataRouter router(
        [&](std::span<const protocol::Event> events) {
            std::lock_guard<std::mutex> lock(mutex);
            batch_sizes.push_back(events.size());
            routed.insert(routed.end(), events.begin(), events.end());
        },
        {}, MarketDataPublisherOptions{.clock = [] { return 1; }});

    for (EventSequence sequence = 1; sequence <= 5; ++sequence) {
        router.publish(ExchangeEvent{added(sequence, 10 + sequence)});
    }
    router.start();
    ASSERT_TRUE(wait_until([&] { return router.routed_count() == 5; }));
    router.stop();

    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_EQ(routed.size(), 5u);
    EXPECT_EQ(router.batch_count(), 1u);
    ASSERT_EQ(batch_sizes.size(), 1u);
    EXPECT_EQ(batch_sizes[0], 5u);
    EXPECT_EQ(router.max_batch_observed(), 5u);

    // Order is still the feed's order across the batch, which is the whole
    // reason a subscriber can detect a gap.
    for (std::size_t i = 0; i < routed.size(); ++i) {
        EXPECT_EQ(std::get<protocol::AddOrder>(routed[i]).sequence_number, i + 1);
    }
}

// A batch is capped, because the datagram it becomes has a size limit past
// which it fragments (net::MAX_FRAMES_PER_DATAGRAM). Above the cap the drain
// makes several calls rather than one oversized one.
TEST(MarketDataRouter, BatchIsCappedAtMaxBatch) {
    std::mutex mutex;
    std::vector<std::size_t> batch_sizes;
    MarketDataRouter router(
        [&](std::span<const protocol::Event> events) {
            std::lock_guard<std::mutex> lock(mutex);
            batch_sizes.push_back(events.size());
        },
        MarketDataRouterOptions{.max_batch = 2}, MarketDataPublisherOptions{.clock = [] { return 1; }});

    for (EventSequence sequence = 1; sequence <= 5; ++sequence) {
        router.publish(ExchangeEvent{added(sequence, 10 + sequence)});
    }
    router.start();
    ASSERT_TRUE(wait_until([&] { return router.routed_count() == 5; }));
    router.stop();

    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_EQ(router.max_batch_observed(), 2u);
    EXPECT_EQ(batch_sizes, std::vector<std::size_t>({2, 2, 1}));
}

} // namespace mdh::exchange::market_data
