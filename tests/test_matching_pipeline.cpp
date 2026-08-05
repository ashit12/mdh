#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include "exchange/sequencing/matching_pipeline.hpp"

namespace mdh::exchange::sequencing {
namespace {

using namespace std::chrono_literals;

NewOrderCommand new_order(AccountId account, ClientOrderId client_id, InstrumentId instrument, Side side,
                           Price price, Quantity qty, TimeInForce tif = TimeInForce::GTC) {
    return NewOrderCommand{
        .command_sequence = 0, // overwritten by the pipeline's CommandSequencer -- see CommandSequencer's own tests
        .account_id = account,
        .client_order_id = client_id,
        .instrument_id = instrument,
        .side = side,
        .price = price,
        .quantity = qty,
        .order_type = OrderType::Limit,
        .time_in_force = tif,
    };
}

// The matching thread invokes the sink concurrently with the test's own
// (producer) thread, unlike MatchingEngine's own tests (test_matching_engine.cpp)
// which call process() synchronously on one thread -- so, unlike that file's
// CollectingSink, this one needs its own lock.
class ThreadSafeCollectingSink {
public:
    EventSink sink() {
        return [this](const ExchangeEvent& ev) {
            std::lock_guard<std::mutex> lock(mutex_);
            events_.push_back(ev);
        };
    }

    [[nodiscard]] std::vector<ExchangeEvent> events() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return events_;
    }

private:
    mutable std::mutex mutex_;
    std::vector<ExchangeEvent> events_;
};

template <class T>
bool holds(const ExchangeEvent& ev) {
    return std::holds_alternative<T>(ev);
}

// Polls `pred` until it's true or `timeout` elapses. MatchingPipeline
// processes commands on its own thread, so tests that don't call stop()
// first (which would itself block until the queue drains) need some way to
// wait for asynchronous completion; a short bounded poll is simplest and
// avoids adding a test-only synchronization hook to the production class.
template <class Pred>
bool wait_until(Pred pred, std::chrono::milliseconds timeout = 2s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return pred();
}

} // namespace

TEST(MatchingPipeline, SubmitAcceptsAndProcessesACommand) {
    ThreadSafeCollectingSink out;
    MatchingPipeline pipeline(out.sink());

    EXPECT_TRUE(pipeline.submit(new_order(100, 1, /*instrument=*/1, Side::Buy, 100, 10)));
    ASSERT_TRUE(wait_until([&] { return pipeline.commands_processed() == 1; }));

    pipeline.stop();
    const auto events = out.events();
    ASSERT_EQ(events.size(), 2u);
    EXPECT_TRUE(holds<OrderAccepted>(events[0]));
    EXPECT_TRUE(holds<BookOrderAdded>(events[1]));

    const auto snapshot = pipeline.snapshot();
    ASSERT_EQ(snapshot.instruments.size(), 1u);
    EXPECT_EQ(snapshot.instruments[0].bids.size(), 1u);
}

TEST(MatchingPipeline, SubmitAssignsStrictlyIncreasingSequenceInSubmissionOrder) {
    ThreadSafeCollectingSink out;
    MatchingPipeline pipeline(out.sink());

    constexpr int kCount = 20;
    for (int i = 0; i < kCount; ++i) {
        // Distinct instruments so nothing crosses -- keeps this test about
        // sequencing/ordering, not matching semantics (already covered by
        // test_matching_engine.cpp).
        ASSERT_TRUE(pipeline.submit(
            new_order(100, static_cast<ClientOrderId>(i), static_cast<InstrumentId>(i), Side::Buy, 100, 1)));
    }
    ASSERT_TRUE(wait_until([&] { return pipeline.commands_processed() == static_cast<std::size_t>(kCount); }));
    pipeline.stop();

    const auto events = out.events();
    ASSERT_EQ(events.size(), static_cast<std::size_t>(kCount) * 2); // OrderAccepted + BookOrderAdded per command

    CommandSequence expected = 1;
    for (const auto& ev : events) {
        if (std::holds_alternative<OrderAccepted>(ev)) {
            EXPECT_EQ(std::get<OrderAccepted>(ev).command_sequence, expected);
            ++expected;
        }
    }
    EXPECT_EQ(expected, static_cast<CommandSequence>(kCount) + 1);
}

TEST(MatchingPipeline, StopDrainsEveryAcceptedCommandBeforeReturning) {
    ThreadSafeCollectingSink out;
    MatchingPipelineOptions options;
    options.matching_delay = 2ms; // slow enough that submissions below outrun processing
    MatchingPipeline pipeline(out.sink(), options);

    constexpr int kCount = 10;
    int accepted = 0;
    for (int i = 0; i < kCount; ++i) {
        if (pipeline.submit(new_order(100, static_cast<ClientOrderId>(i), static_cast<InstrumentId>(i), Side::Buy, 100, 1))) {
            ++accepted;
        }
    }
    ASSERT_GT(accepted, 0);

    pipeline.stop(); // must not return until every already-queued command is processed
    EXPECT_EQ(pipeline.commands_processed(), static_cast<std::size_t>(accepted));
    EXPECT_EQ(pipeline.queue_size(), 0u);
}

TEST(MatchingPipeline, SubmitRejectsWithoutDroppingWhenQueueIsFull) {
    ThreadSafeCollectingSink out;
    MatchingPipelineOptions options;
    options.queue_capacity = 1; // rounds up to a power of two internally, but stays tiny
    options.matching_delay = 20ms; // slow enough to reliably keep the queue full while flooding submissions
    MatchingPipeline pipeline(out.sink(), options);

    int accepted = 0;
    int rejected = 0;
    constexpr int kAttempts = 50;
    for (int i = 0; i < kAttempts; ++i) {
        if (pipeline.submit(new_order(100, static_cast<ClientOrderId>(i), /*instrument=*/1, Side::Buy, 100, 1))) {
            ++accepted;
        } else {
            ++rejected;
        }
    }

    // The whole point of this policy (see MatchingPipeline's class-level
    // comment): a full queue is an explicit, countable rejection, never a
    // silent drop -- every attempted submission is accounted for as either
    // accepted or rejected, and rejected ones consumed no CommandSequence.
    EXPECT_GT(rejected, 0);
    EXPECT_EQ(accepted + rejected, kAttempts);
    EXPECT_EQ(pipeline.commands_rejected(), static_cast<std::size_t>(rejected));

    pipeline.stop();
    EXPECT_EQ(pipeline.commands_processed(), static_cast<std::size_t>(accepted));
}

// ── Real concurrency ──────────────────────────────────────────────────────
//
// Everything above submits from the test's own (single) thread, which never
// exercises the actual reason MatchingPipeline exists: a dedicated producer
// thread racing a dedicated matching thread across the same SpscQueue this
// class owns. Run under ThreadSanitizer (MDH_ENABLE_TSAN) to check the
// memory-ordering claims in this class's own doc comment, the same way
// SpscQueue's concurrent tests do (test_spsc_queue.cpp).
TEST(MatchingPipeline, ConcurrentProducerThreadCommandsAllProcessedExactlyOnce) {
    ThreadSafeCollectingSink out;
    MatchingPipelineOptions options;
    options.queue_capacity = 64; // small on purpose: forces frequent full/empty contention
    MatchingPipeline pipeline(out.sink(), options);

    constexpr int kCount = 20'000;
    std::atomic<int> submitted{0};

    {
        std::jthread producer([&] {
            for (int i = 0; i < kCount; ++i) {
                // One instrument per order (side Buy, ascending prices) so
                // nothing crosses -- this test asserts every submitted
                // command reaches the matching thread and is processed
                // exactly once, not matching correctness (already covered
                // elsewhere).
                while (!pipeline.submit(new_order(100, static_cast<ClientOrderId>(i), static_cast<InstrumentId>(i),
                                                   Side::Buy, 100, 1))) {
                    std::this_thread::yield(); // full: retry, exactly like SpscQueue's own concurrent test
                }
                ++submitted;
            }
        });
    } // producer joined here

    ASSERT_EQ(submitted.load(), kCount);
    ASSERT_TRUE(wait_until([&] { return pipeline.commands_processed() == static_cast<std::size_t>(kCount); }, 10s));
    pipeline.stop();

    EXPECT_EQ(pipeline.commands_processed(), static_cast<std::size_t>(kCount));
    // Rejections during the retry loop are expected (a 64-slot queue racing
    // a fast producer) and not a bug -- see SpscQueue's own concurrent test
    // for the same spin-retry-on-full pattern. What must hold regardless of
    // how many retries it took is that every one of the kCount submissions
    // *eventually* got in exactly once, which the assertions below check.

    const auto snapshot = pipeline.snapshot();
    EXPECT_EQ(snapshot.instruments.size(), static_cast<std::size_t>(kCount)); // one resting order per instrument

    // Every OrderAccepted's command_sequence must be unique and in
    // [1, kCount] -- proves no command was processed twice and none was
    // silently skipped, despite arriving from a different thread than the
    // one that sequenced and processed it.
    std::vector<bool> seen(static_cast<std::size_t>(kCount) + 1, false);
    int accepted_count = 0;
    for (const auto& ev : out.events()) {
        if (!std::holds_alternative<OrderAccepted>(ev)) {
            continue;
        }
        const auto seq = std::get<OrderAccepted>(ev).command_sequence;
        ASSERT_GE(seq, 1u);
        ASSERT_LE(seq, static_cast<CommandSequence>(kCount));
        ASSERT_FALSE(seen[seq]) << "command_sequence " << seq << " observed more than once";
        seen[seq] = true;
        ++accepted_count;
    }
    EXPECT_EQ(accepted_count, kCount);
}

} // namespace mdh::exchange::sequencing
