#include "exchange/sequencing/matching_pipeline.hpp"

#include <cstdio>
#include <span>
#include <utility>

#include "common/thread_affinity.hpp"

namespace mdh::exchange::sequencing {

MatchingPipeline::MatchingPipeline(EventSink sink, const MatchingPipelineOptions& options, Processor processor)
    : sink_(std::move(sink)), queue_(options.queue_capacity),
      engine_(std::span<const InstrumentId>(options.instruments), options.expected_resting_orders),
      processor_(processor ? std::move(processor)
                            : Processor([this](const ExchangeCommand& command, const EventSink& event_sink) {
                                  engine_.process(command, event_sink);
                              })),
      options_(options) {
    matching_thread_ = std::jthread([this] {
        matching_thread_id_.store(calling_thread_id(), std::memory_order_release);
        if (options_.matching_cpu.has_value()) {
            if (auto error = pin_calling_thread_to_cpu(*options_.matching_cpu)) {
                matching_affinity_error_ = std::move(*error);
                std::fprintf(stderr, "MatchingPipeline: %s\n", matching_affinity_error_.c_str());
            } else {
                matching_cpu_pinned_.store(true, std::memory_order_relaxed);
            }
        }
        matching_affinity_ready_.store(true, std::memory_order_release);

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
            ExchangeCommand sequenced = sequencer_.sequence(std::move(*command));
            processor_(sequenced, sink_);
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
    // Unsequenced on purpose: sequence numbers are matching-thread-only, so
    // a command that never enters the queue cannot punch a gap. See this
    // class's sequencing-semantics comment.
    if (!queue_.try_push(std::move(command))) {
        commands_rejected_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    return true;
}

} // namespace mdh::exchange::sequencing
