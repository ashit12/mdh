#include "exchange/sequencing/matching_pipeline.hpp"

#include <utility>

namespace mdh::exchange::sequencing {

MatchingPipeline::MatchingPipeline(EventSink sink, const MatchingPipelineOptions& options, Processor processor)
    : sink_(std::move(sink)), queue_(options.queue_capacity),
      processor_(processor ? std::move(processor)
                            : Processor([this](const ExchangeCommand& command, const EventSink& event_sink) {
                                  engine_.process(command, event_sink);
                              })),
      options_(options) {
    matching_thread_ = std::jthread([this] {
        const auto token = stop_source_.get_token();
        while (true) {
            auto command = queue_.try_pop();
            if (!command) {
                if (token.stop_requested()) {
                    break; // stop requested and the queue is now empty: drain complete
                }
                std::this_thread::yield();
                continue;
            }
            if (options_.matching_delay.count() > 0) {
                std::this_thread::sleep_for(options_.matching_delay); // simulated slow matching core, see MatchingPipelineOptions
            }
            processor_(*command, sink_);
            commands_processed_.fetch_add(1, std::memory_order_relaxed);
        }
    });
}

MatchingPipeline::~MatchingPipeline() { stop(); }

void MatchingPipeline::stop() {
    stop_source_.request_stop();
    if (matching_thread_.joinable()) {
        matching_thread_.join();
    }
}

bool MatchingPipeline::submit(ExchangeCommand command) {
    // Checked before assigning a sequence number, not after: a command that
    // never actually enters the queue must not consume an authoritative
    // CommandSequence value either, or the sequence stream would show a
    // permanent gap for a command the matching engine never even attempted
    // to process. Race-free specifically because submit() is producer-only
    // (see class-level doc comment): nothing else can grow queue_'s
    // occupancy between this check and the push below.
    if (queue_.size() >= queue_.capacity()) {
        commands_rejected_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    ExchangeCommand sequenced = sequencer_.sequence(std::move(command));
    if (queue_.try_push(std::move(sequenced))) {
        return true;
    }

    // Unreachable in practice given the single-producer invariant the check
    // above already relies on -- kept as a real, structured branch rather
    // than an assumption, matching this codebase's preference for
    // structured returns over asserts.
    commands_rejected_.fetch_add(1, std::memory_order_relaxed);
    return false;
}

} // namespace mdh::exchange::sequencing
