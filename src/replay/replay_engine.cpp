#include "replay/replay_engine.hpp"

#include <chrono>
#include <sstream>
#include <variant>

#include "protocol/errors.hpp"
#include "common/sequence_validator.hpp"
#include "replay/event_file_reader.hpp"
#include "replay/snapshot.hpp"

namespace mdh::replay {

namespace {

Sequence sequence_of(const protocol::Event& event) {
    return std::visit([](const auto& msg) { return msg.sequence_number; }, event);
}

void apply_event(const protocol::Event& event, book::BookManager& books, ReplayStats& stats) {
    std::visit(
        [&](const auto& msg) {
            using T = std::decay_t<decltype(msg)>;
            if constexpr (std::is_same_v<T, protocol::AddOrder>) {
                stats.adds += 1;
                auto err = books.book_for(msg.instrument_id).add_order(msg.order_id, msg.price, msg.quantity, msg.side);
                if (err) stats.book_errors += 1;
            } else if constexpr (std::is_same_v<T, protocol::CancelOrder>) {
                stats.cancels += 1;
                auto err = books.book_for(msg.instrument_id).cancel_order(msg.order_id);
                if (err) stats.book_errors += 1;
            } else if constexpr (std::is_same_v<T, protocol::ModifyOrder>) {
                stats.modifies += 1;
                auto err = books.book_for(msg.instrument_id).modify_order(msg.order_id, msg.new_price, msg.new_quantity);
                if (err) stats.book_errors += 1;
            } else if constexpr (std::is_same_v<T, protocol::Trade>) {
                stats.trades += 1;
                books.record_trade(msg.instrument_id, msg.price, msg.quantity);
            } else if constexpr (std::is_same_v<T, protocol::ClearBook>) {
                stats.clears += 1;
                books.book_for(msg.instrument_id).clear();
            }
        },
        event);
}

std::string describe_sequence_error(const SequenceCheck& check) {
    std::ostringstream oss;
    switch (check.outcome) {
        case SequenceOutcome::Duplicate:
            oss << "duplicate sequence " << check.observed;
            break;
        case SequenceOutcome::OutOfOrder:
            oss << "out-of-order sequence " << check.observed;
            break;
        case SequenceOutcome::Missing:
            oss << "missing sequence(s) [" << check.expected << ".." << (check.observed - 1) << "]";
            break;
        case SequenceOutcome::InOrder:
            oss << "in-order sequence " << check.observed; // unreachable in practice
            break;
    }
    return oss.str();
}

} // namespace

bool apply_frame_result(std::variant<protocol::Event, protocol::DecodeError> frame, SequenceValidator& validator,
                         const ReplayOptions& options, ReplayOutcome& outcome) {
    if (std::holds_alternative<protocol::DecodeError>(frame)) {
        outcome.stats.decode_failures += 1;
        if (options.stop_on_decode_error) {
            outcome.stopped_early = true;
            outcome.stop_reason =
                std::string("decode error: ") + std::string(protocol::to_string(std::get<protocol::DecodeError>(frame)));
            return true;
        }
        return false;
    }

    const auto& event = std::get<protocol::Event>(frame);
    const auto check = validator.check(sequence_of(event));
    if (check.outcome != SequenceOutcome::InOrder) {
        outcome.stats.sequence_failures += 1;

        if (check.outcome == SequenceOutcome::Missing && options.recovery_snapshot_path) {
            auto loaded = read_snapshot(*options.recovery_snapshot_path);
            if (!loaded) {
                outcome.stopped_early = true;
                outcome.stop_reason = "sequence gap recovery failed: could not load snapshot " + *options.recovery_snapshot_path;
                return true;
            }
            outcome.books = std::move(loaded->books);
            outcome.stats.recoveries += 1;
            // The event that revealed the gap becomes the new baseline --
            // see this function's doc comment for why (no gap-fill service
            // to reconstruct exactly what was missed in between).
            validator.reset(sequence_of(event));
        } else if (options.stop_on_sequence_error) {
            outcome.stopped_early = true;
            outcome.stop_reason = describe_sequence_error(check);
            return true;
        }
    }

    outcome.stats.messages_processed += 1;
    outcome.last_sequence_number = sequence_of(event);
    apply_event(event, outcome.books, outcome.stats);
    return false;
}

ReplayOutcome run_replay(const std::string& input_path, const ReplayOptions& options) {
    ReplayOutcome outcome;

    EventFileReader reader(input_path);
    if (!reader.is_open()) {
        outcome.stopped_early = true;
        outcome.stop_reason = "failed to open input file: " + input_path;
        return outcome;
    }

    SequenceValidator validator;
    const auto start = std::chrono::steady_clock::now();

    while (true) {
        auto frame = reader.next();
        if (!frame.has_value()) {
            break; // clean EOF
        }
        if (apply_frame_result(*frame, validator, options, outcome)) {
            break;
        }
    }

    const auto end = std::chrono::steady_clock::now();
    outcome.stats.duration_ns = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    return outcome;
}

} // namespace mdh::replay
