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
        [&](const protocol::Event& event) {
            std::lock_guard<std::mutex> lock(mutex);
            routed.push_back(event);
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
        [&](const protocol::Event& event) {
            std::lock_guard<std::mutex> lock(mutex);
            routed.push_back(event);
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

} // namespace mdh::exchange::market_data
