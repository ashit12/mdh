#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <stop_token>
#include <thread>
#include <vector>

#include "common/spsc_queue.hpp"
#include "exchange/core/commands.hpp"
#include "exchange/core/event_sink.hpp"
#include "exchange/matching/matching_engine.hpp"
#include "exchange/matching/state_snapshot.hpp"
#include "exchange/sequencing/command_sequencer.hpp"

// The sequencer and matching thread: everything between "commands arrive
// somehow" and the deterministic, single-threaded matching engine. It uses
// SpscQueue unchanged.
//
// ── Why this is not a DroppingQueue ───────────────────────────────────────
// Market data can afford to drop a frame -- downstream it shows up as a
// sequence gap the receiver can detect and repair. An inbound command
// cannot. It is a live client request, and discarding it silently would
// leave the client believing it had an order working that the exchange
// never saw, indistinguishable from an acknowledged rejection.
//
// So submit() reports a full queue as an explicit `false` instead of
// swallowing it. Turning that into a client-facing "busy, retry" reply is
// the gateway's business; this class stops at "was this command sequenced
// and queued, yes or no."
//
// ── Threads ───────────────────────────────────────────────────────────────
// One producer, one consumer, exactly as SpscQueue requires. submit() must
// be called from a single thread: the sequencer's counter is a plain
// non-atomic relying on that, the same way the queue's own head index does.
//
// The matching thread is started in the constructor and joined in stop() or
// the destructor. It is the only consumer and the only thread that ever
// calls into the matching engine, which is what preserves the engine's
// single-threaded determinism even though commands now arrive from
// elsewhere.
//
// The EventSink runs synchronously on the matching thread. This class adds
// no buffering or thread-hopping on the way out -- a caller that needs
// events on another thread must arrange that itself, for instance by having
// its sink push onto a queue of its own.
namespace mdh::exchange::sequencing {

struct MatchingPipelineOptions {
    std::size_t queue_capacity = 1024;

    // Every instrument this pipeline's engine will trade; anything else is
    // rejected. Empty means an engine that rejects everything, which is the
    // right default for a class whose job is transport: a caller that has
    // not said what it trades has not finished configuring it.
    std::vector<InstrumentId> instruments;

    // Passed to the engine -- see kDefaultExpectedRestingOrders for what it
    // buys and what guessing low costs.
    std::size_t expected_resting_orders = MatchingEngine::kDefaultExpectedRestingOrders;

    // An artificial pause after each command, so a test can exercise
    // submit()'s backpressure path deterministically instead of hoping the
    // OS scheduler produces a slow enough consumer. Zero by default.
    std::chrono::microseconds matching_delay{0};
};

class MatchingPipeline {
public:
    // A stand-in for calling the engine directly on the matching thread, so
    // a caller can wrap it with extra behaviour -- RiskGatedEngine, which
    // adds risk checks and ledger updates -- without changing any of the
    // threading or queueing guarantees here. It has the same signature as
    // both engines' own process(), so either adapts with a forwarding
    // lambda.
    using Processor = std::function<void(const ExchangeCommand&, const EventSink&)>;

    // If `processor` is supplied it runs on the matching thread for every
    // command dequeued, in place of the engine. Null means call the engine
    // directly.
    explicit MatchingPipeline(EventSink sink, const MatchingPipelineOptions& options = {},
                               Processor processor = nullptr);

    // Asks the matching thread to drain and stop, then joins it.
    ~MatchingPipeline();

    MatchingPipeline(const MatchingPipeline&) = delete;
    MatchingPipeline& operator=(const MatchingPipeline&) = delete;
    MatchingPipeline(MatchingPipeline&&) = delete;
    MatchingPipeline& operator=(MatchingPipeline&&) = delete;

    // Producer side only. Gives `command` its authoritative sequence number
    // and queues it for the matching thread. Returns false, having sequenced
    // and processed nothing, if the queue is full -- see the class comment
    // on why that is a rejection rather than a silent drop.
    [[nodiscard]] bool submit(ExchangeCommand command);

    // Asks the matching thread to stop once it has processed everything
    // already queued -- never mid-drain -- then joins it. Safe to call more
    // than once, including from the destructor. Separate from the destructor
    // so a caller can shut down and still call snapshot() afterwards.
    void stop();

    // Only safe after stop() has returned. Joining the matching thread is
    // what makes reading the engine from another thread safe; calling this
    // while it still runs is a data race, unguarded at runtime, just as
    // SpscQueue does not check its own single-producer contract.
    [[nodiscard]] EngineStateSnapshot snapshot() const { return engine_.snapshot(); }

    // Best-effort introspection, safe from any thread, with the same caveat
    // as the queue's own size() and high_water_mark().
    [[nodiscard]] std::size_t queue_size() const { return queue_.size(); }
    [[nodiscard]] std::size_t queue_high_water_mark() const { return queue_.high_water_mark(); }
    [[nodiscard]] std::size_t commands_processed() const {
        return commands_processed_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::size_t commands_rejected() const { return commands_rejected_.load(std::memory_order_relaxed); }

private:
    EventSink sink_;
    CommandSequencer sequencer_; // producer thread only
    SpscQueue<ExchangeCommand> queue_;
    MatchingEngine engine_; // matching thread only while running; see snapshot()
    Processor processor_;   // matching thread only, like engine_

    std::atomic<std::size_t> commands_processed_{0}; // matching thread writes
    std::atomic<std::size_t> commands_rejected_{0};  // producer thread writes

    MatchingPipelineOptions options_;
    std::stop_source stop_source_;
    std::jthread matching_thread_; // last: it must see a fully built *this
};

} // namespace mdh::exchange::sequencing
