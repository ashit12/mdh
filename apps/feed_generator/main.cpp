// feed_generator: emits a deterministic binary market-data event file.
//
// Usage:
//   feed_generator --output events.bin --orders 100000 --seed 42 [--instruments 10]
//
// Every AddOrder/CancelOrder/ModifyOrder/Trade/ClearBook this milestone's
// generator produces is valid (well-formed and sequence-correct) -- fault
// injection (corrupt bytes, dropped/duplicated packets) is out of scope
// until milestone 6, per the project plan. --seed fully determines the
// output: the same seed and --orders value always produce a byte-identical
// file, since the only sources of "randomness" are seeded std::mt19937_64
// draws.
#include <cstdlib>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "protocol/messages.hpp"
#include "replay/event_file_writer.hpp"

using namespace mdh;
using namespace mdh::protocol;
using namespace mdh::replay;

namespace {

struct Args {
    std::string output;
    std::uint64_t orders = 0;
    std::uint64_t seed = 42;
    std::uint32_t instruments = 10;
};

std::optional<Args> parse_args(int argc, char** argv) {
    Args args;
    bool have_output = false;
    bool have_orders = false;

    for (int i = 1; i < argc; ++i) {
        const std::string flag = argv[i];
        auto next = [&]() -> std::optional<std::string> {
            if (i + 1 >= argc) return std::nullopt;
            return std::string(argv[++i]);
        };

        if (flag == "--output") {
            auto v = next();
            if (!v) return std::nullopt;
            args.output = *v;
            have_output = true;
        } else if (flag == "--orders") {
            auto v = next();
            if (!v) return std::nullopt;
            args.orders = std::stoull(*v);
            have_orders = true;
        } else if (flag == "--seed") {
            auto v = next();
            if (!v) return std::nullopt;
            args.seed = std::stoull(*v);
        } else if (flag == "--instruments") {
            auto v = next();
            if (!v) return std::nullopt;
            args.instruments = static_cast<std::uint32_t>(std::stoul(*v));
        } else {
            std::cerr << "unrecognized argument: " << flag << "\n";
            return std::nullopt;
        }
    }

    if (!have_output || !have_orders || args.orders == 0 || args.instruments == 0) {
        return std::nullopt;
    }
    return args;
}

void print_usage() {
    std::cerr << "Usage: feed_generator --output <path> --orders <N> [--seed <S>] [--instruments <N>]\n";
}

struct OpenOrder {
    InstrumentId instrument_id;
    OrderId order_id;
};

enum class EventKind { Add, Cancel, Modify, Trade, Clear };

} // namespace

int main(int argc, char** argv) {
    auto args = parse_args(argc, argv);
    if (!args) {
        print_usage();
        return EXIT_FAILURE;
    }

    EventFileWriter writer(args->output);
    if (!writer.is_open()) {
        std::cerr << "failed to open output file: " << args->output << "\n";
        return EXIT_FAILURE;
    }

    std::mt19937_64 rng(args->seed);
    std::uniform_int_distribution<int> event_kind_dist(0, 99);
    std::uniform_int_distribution<InstrumentId> instrument_dist(1, args->instruments);
    std::uniform_int_distribution<Quantity> quantity_dist(1, 100);
    std::uniform_int_distribution<Price> price_jitter_dist(-50, 50);
    std::uniform_int_distribution<int> side_dist(0, 1);
    std::uniform_int_distribution<Timestamp> timestamp_step_dist(1, 1000);

    std::vector<Price> mid_price(args->instruments + 1, 0);
    for (InstrumentId i = 1; i <= args->instruments; ++i) {
        mid_price[i] = 100000 + static_cast<Price>(i) * 1000;
    }

    std::vector<OpenOrder> open_orders;
    Sequence next_seq = 1;
    OrderId next_order_id = 1;
    Timestamp timestamp = 1'000'000'000;
    std::uint64_t adds_emitted = 0;

    auto pick_kind = [&]() -> EventKind {
        if (open_orders.empty()) return EventKind::Add;
        const int roll = event_kind_dist(rng);
        if (roll < 70) return EventKind::Add;
        if (roll < 85) return EventKind::Cancel;
        if (roll < 95) return EventKind::Modify;
        if (roll < 99) return EventKind::Trade;
        return EventKind::Clear;
    };

    while (adds_emitted < args->orders) {
        timestamp += timestamp_step_dist(rng);
        const Sequence seq = next_seq++;
        const EventKind kind = pick_kind();

        switch (kind) {
            case EventKind::Add: {
                const InstrumentId instrument = instrument_dist(rng);
                const Side side = side_dist(rng) == 0 ? Side::Buy : Side::Sell;
                mid_price[instrument] += price_jitter_dist(rng);
                if (mid_price[instrument] < 100) mid_price[instrument] = 100;
                const OrderId order_id = next_order_id++;

                writer.write(Event{AddOrder{
                    .sequence_number = seq,
                    .timestamp_ns = timestamp,
                    .order_id = order_id,
                    .instrument_id = instrument,
                    .price = mid_price[instrument],
                    .quantity = quantity_dist(rng),
                    .side = side,
                }});
                open_orders.push_back(OpenOrder{instrument, order_id});
                ++adds_emitted;
                break;
            }
            case EventKind::Cancel: {
                std::uniform_int_distribution<std::size_t> idx_dist(0, open_orders.size() - 1);
                const std::size_t idx = idx_dist(rng);
                const OpenOrder chosen = open_orders[idx];
                open_orders[idx] = open_orders.back();
                open_orders.pop_back();

                writer.write(Event{CancelOrder{
                    .sequence_number = seq,
                    .timestamp_ns = timestamp,
                    .order_id = chosen.order_id,
                    .instrument_id = chosen.instrument_id,
                }});
                break;
            }
            case EventKind::Modify: {
                std::uniform_int_distribution<std::size_t> idx_dist(0, open_orders.size() - 1);
                const OpenOrder& chosen = open_orders[idx_dist(rng)];
                Price new_price = mid_price[chosen.instrument_id] + price_jitter_dist(rng);
                if (new_price < 100) new_price = 100;

                writer.write(Event{ModifyOrder{
                    .sequence_number = seq,
                    .timestamp_ns = timestamp,
                    .order_id = chosen.order_id,
                    .instrument_id = chosen.instrument_id,
                    .new_price = new_price,
                    .new_quantity = quantity_dist(rng),
                }});
                break;
            }
            case EventKind::Trade: {
                const InstrumentId instrument = instrument_dist(rng);
                const Side aggressor = side_dist(rng) == 0 ? Side::Buy : Side::Sell;

                writer.write(Event{Trade{
                    .sequence_number = seq,
                    .timestamp_ns = timestamp,
                    .instrument_id = instrument,
                    .price = mid_price[instrument],
                    .quantity = quantity_dist(rng),
                    .aggressor_side = aggressor,
                }});
                break;
            }
            case EventKind::Clear: {
                const InstrumentId instrument = instrument_dist(rng);
                writer.write(Event{ClearBook{
                    .sequence_number = seq,
                    .timestamp_ns = timestamp,
                    .instrument_id = instrument,
                }});
                std::erase_if(open_orders, [instrument](const OpenOrder& o) { return o.instrument_id == instrument; });
                break;
            }
        }
    }

    std::cout << "wrote " << (next_seq - 1) << " events (" << adds_emitted << " adds) to " << args->output << "\n";
    return EXIT_SUCCESS;
}
