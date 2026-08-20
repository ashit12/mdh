// SpscQueue<T> throughput -- single-threaded push/pop cost in
// isolation, and genuine two-thread producer/consumer throughput (the
// number that actually matters for every pipeline this queue backs:
// net::run_udp_listen()'s producer/consumer pair and every per-connection
// outbound queue in exchange::gateway::OrderEntryGateway). See
// spsc_queue.hpp's own class comment for the acquire/release design being
// measured here.
#include <benchmark/benchmark.h>

#include <atomic>
#include <cstdint>
#include <thread>

#include "common/spsc_queue.hpp"

using namespace mdh;

// Single-threaded: push then immediately pop from the same thread, capacity
// large enough to never fill -- isolates per-call atomic/memory-ordering
// overhead from any actual cross-core contention (that's the two-thread
// benchmark below).
static void BM_SpscQueue_SingleThreadedPushPop(benchmark::State& state) {
    SpscQueue<std::uint64_t> queue(1024);
    std::uint64_t value = 0;
    for (auto _ : state) {
        bool pushed = queue.try_push(value);
        benchmark::DoNotOptimize(pushed);
        auto popped = queue.try_pop();
        benchmark::DoNotOptimize(popped);
        ++value;
    }
}
BENCHMARK(BM_SpscQueue_SingleThreadedPushPop);

// Real two-thread producer/consumer, measuring items/sec end to end
// (wall-clock, via UseRealTime()) rather than the main thread's own CPU
// time, since the work being measured happens on two separate threads.
// The producer spins on try_push() (retrying under backpressure) instead
// of dropping, unlike DroppingQueue's policy -- this benchmark measures the
// queue's own raw throughput ceiling, not any backpressure policy layered
// on top of it.
static void BM_SpscQueue_TwoThreadThroughput(benchmark::State& state) {
    const auto item_count = static_cast<std::uint64_t>(state.range(0));
    for (auto _ : state) {
        SpscQueue<std::uint64_t> queue(4096);
        std::atomic<bool> consumer_done{false};

        std::thread consumer([&] {
            std::uint64_t received = 0;
            while (received < item_count) {
                if (auto value = queue.try_pop()) {
                    benchmark::DoNotOptimize(*value);
                    ++received;
                }
            }
            consumer_done.store(true, std::memory_order_relaxed);
        });

        for (std::uint64_t i = 0; i < item_count; ++i) {
            while (!queue.try_push(i)) {
                // Backpressure: this benchmark measures the queue's raw
                // ceiling, so retry rather than drop.
            }
        }
        consumer.join();
        benchmark::DoNotOptimize(consumer_done.load());
    }
    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(item_count));
}
BENCHMARK(BM_SpscQueue_TwoThreadThroughput)->Arg(10'000)->Arg(100'000)->UseRealTime()->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
