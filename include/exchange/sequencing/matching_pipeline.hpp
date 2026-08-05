#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <stop_token>
#include <thread>

#include "common/spsc_queue.hpp"
#include "exchange/core/commands.hpp"
#include "exchange/core/event_sink.hpp"
#include "exchange/matching/matching_engine.hpp"
#include "exchange/matching/state_snapshot.hpp"
#include "exchange/sequencing/command_sequencer.hpp"

// The command-sequencer + matching-thread pipeline (Milestone 4): the piece
// of the architecture between "however commands arrive" (a future
// gateway/validation/risk layer, Milestones 5 and 7) and the deterministic,
// single-threaded MatchingEngine (Milestone 2). Reuses SpscQueue exactly as
// it already exists (common/spsc_queue.hpp), per the architecture doc's own
// callout that it's "directly reusable for ... sequencer -> matching ...
// boundaries as-is" -- no changes needed to that class for this milestone.
//
// ── Why this is NOT a DroppingQueue ──────────────────────────────────────
// Market data can tolerate dropping a frame -- a dropped AddOrder just
// shows up downstream as a detected, recoverable sequence gap (see
// DroppingQueue's own doc comment, common/dropping_queue.hpp). An exchange
// inbound command cannot be treated the same way: it represents a live
// client request, and silently discarding it would leave a client
// believing they submitted an order the exchange never actually saw, with
// no way to tell the difference from an acknowledged rejection. So
// MatchingPipeline::submit() surfaces a full queue as an explicit `false`
// return instead of swallowing it -- turning that into an actual
// client-facing response (e.g. a "system busy, retry" reply) is a
// gateway/risk-layer concern for a later milestone; this milestone's job
// stops at "did this command get sequenced and queued, yes or no."
//
// ── Threading model ───────────────────────────────────────────────────────
// Single producer, single consumer, exactly like SpscQueue's own contract:
// submit() must only ever be called from one thread (the same constraint
// SpscQueue::try_push() already documents) -- CommandSequencer's internal
// counter is a plain, non-atomic counter relying on that same single-writer
// invariant, the same way SpscQueue's head_ does. The matching thread,
// started in the constructor and joined in the destructor/stop(), is the
// sole consumer and the only thread that ever calls
// MatchingEngine::process() -- preserving the matching engine's documented
// single-threaded-determinism requirement even though commands now arrive
// asynchronously from a different thread than the one that processes them.
//
// The EventSink supplied at construction is invoked synchronously on the
// matching thread, exactly like every other EventSink call site in this
// codebase (see event_sink.hpp) -- MatchingPipeline adds no thread-hopping
// or buffering on the output side; a caller that needs events observed
// from a different thread must arrange that itself (e.g. by having its
// sink push onto its own queue), the same way EventSink has always left
// delivery-thread concerns to the caller.
namespace mdh::exchange::sequencing {

struct MatchingPipelineOptions {
    std::size_t queue_capacity = 1024;

    // Artificial delay applied by the matching thread after processing
    // each command -- a deterministic way to simulate a slow matching
    // core (e.g. a heavier instrument mix) without depending on incidental
    // machine/OS-scheduling timing to ever exercise submit()'s backpressure
    // path in a test. Mirrors net::UdpListenOptions::consumer_delay
    // exactly, same rationale. Zero (the default) means no artificial delay.
    std::chrono::microseconds matching_delay{0};
};

class MatchingPipeline {
public:
    explicit MatchingPipeline(EventSink sink, const MatchingPipelineOptions& options = {});

    // Requests the matching thread stop (after draining, see stop()) and
    // joins it.
    ~MatchingPipeline();

    MatchingPipeline(const MatchingPipeline&) = delete;
    MatchingPipeline& operator=(const MatchingPipeline&) = delete;
    MatchingPipeline(MatchingPipeline&&) = delete;
    MatchingPipeline& operator=(MatchingPipeline&&) = delete;

    // Producer side only (see class-level threading-model comment).
    // Assigns `command` its authoritative CommandSequence, then enqueues it
    // for the matching thread. Returns false, without sequencing or
    // processing anything, if the queue is currently full -- see the
    // class-level comment on why this is a rejection, not a silent drop.
    [[nodiscard]] bool submit(ExchangeCommand command);

    // Requests the matching thread stop once it has processed every
    // command already sitting in the queue (never mid-drain), then joins
    // it. Safe to call more than once (including implicitly via the
    // destructor); a no-op if the thread has already been joined. Exposed
    // separately from the destructor so a caller can request shutdown and
    // still call snapshot() afterwards on a still-alive object.
    void stop();

    // Only safe to call after stop() has returned (i.e. the matching
    // thread has been joined) -- std::jthread::join()'s happens-before
    // guarantee is what makes reading engine_ from another thread safe at
    // that point, exactly the same reasoning net::run_udp_listen() relies
    // on to return `outcome.books` only after both its threads are joined.
    // Calling this while the matching thread is still running is a data
    // race and not guarded against at runtime, the same way SpscQueue does
    // not runtime-check its own producer/consumer-only contract either.
    [[nodiscard]] EngineStateSnapshot snapshot() const { return engine_.snapshot(); }

    // Introspection, safe to call from any thread as a best-effort
    // snapshot (same caveat as SpscQueue::size()/high_water_mark()).
    [[nodiscard]] std::size_t queue_size() const { return queue_.size(); }
    [[nodiscard]] std::size_t queue_high_water_mark() const { return queue_.high_water_mark(); }
    [[nodiscard]] std::size_t commands_processed() const {
        return commands_processed_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::size_t commands_rejected() const { return commands_rejected_.load(std::memory_order_relaxed); }

private:
    EventSink sink_;
    CommandSequencer sequencer_; // producer-thread-only, see class-level comment
    SpscQueue<ExchangeCommand> queue_;
    MatchingEngine engine_; // matching-thread-only while running; see snapshot()'s precondition

    std::atomic<std::size_t> commands_processed_{0}; // written by the matching thread only
    std::atomic<std::size_t> commands_rejected_{0};  // written by the producer thread only (submit())

    MatchingPipelineOptions options_;
    std::stop_source stop_source_;
    std::jthread matching_thread_; // started last: must see a fully-initialized *this
};

} // namespace mdh::exchange::sequencing
