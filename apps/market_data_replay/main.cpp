// market_data_replay: reads market-data events either from a binary file
// or live over UDP, validates and decodes messages, reconstructs
// per-instrument order books, and prints measured replay statistics.
//
// Usage:
//   market_data_replay --input events.bin [--top-levels 5]
//   market_data_replay --listen <port> [--top-levels 5] [--idle-timeout-ms 1000]
//
// --listen mode has no signal-handling / graceful-shutdown story (no
// Ctrl+C handler) -- it stops itself once no packets have arrived for
// --idle-timeout-ms, on the assumption that the sender (see udp_sender)
// finished and isn't coming back. A real long-running service would need
// proper shutdown handling; that's out of scope here.
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <optional>
#include <string>

#include "net/udp_listener.hpp"
#include "replay/replay_engine.hpp"

using namespace mdh;
using namespace mdh::replay;

namespace {

struct Args {
    std::optional<std::string> input;
    std::optional<std::uint16_t> listen_port;
    std::size_t top_levels = 5;
    std::uint64_t idle_timeout_ms = 1000;
};

std::optional<Args> parse_args(int argc, char** argv) {
    Args args;

    for (int i = 1; i < argc; ++i) {
        const std::string flag = argv[i];
        auto next = [&]() -> std::optional<std::string> {
            if (i + 1 >= argc) return std::nullopt;
            return std::string(argv[++i]);
        };

        if (flag == "--input") {
            auto v = next();
            if (!v) return std::nullopt;
            args.input = *v;
        } else if (flag == "--listen") {
            auto v = next();
            if (!v) return std::nullopt;
            args.listen_port = static_cast<std::uint16_t>(std::stoul(*v));
        } else if (flag == "--top-levels") {
            auto v = next();
            if (!v) return std::nullopt;
            args.top_levels = std::stoull(*v);
        } else if (flag == "--idle-timeout-ms") {
            auto v = next();
            if (!v) return std::nullopt;
            args.idle_timeout_ms = std::stoull(*v);
        } else {
            std::cerr << "unrecognized argument: " << flag << "\n";
            return std::nullopt;
        }
    }

    // Exactly one of --input / --listen must be given.
    if (args.input.has_value() == args.listen_port.has_value()) {
        return std::nullopt;
    }
    return args;
}

void print_usage() {
    std::cerr << "Usage: market_data_replay --input <path> [--top-levels <N>]\n"
              << "   or: market_data_replay --listen <port> [--top-levels <N>] [--idle-timeout-ms <N>]\n";
}

void print_levels(const char* label, const std::vector<book::PriceLevelView>& levels) {
    if (levels.empty()) {
        std::cout << "    " << label << ": (none)\n";
        return;
    }
    for (const auto& lvl : levels) {
        std::cout << "    " << label << " " << lvl.price << " x " << lvl.aggregate_quantity
                   << " (" << lvl.order_count << " orders)\n";
    }
}

} // namespace

int main(int argc, char** argv) {
    auto args = parse_args(argc, argv);
    if (!args) {
        print_usage();
        return EXIT_FAILURE;
    }

    const ReplayOptions options;
    ReplayOutcome outcome;
    std::uint64_t packets_received = 0;
    std::uint64_t packet_errors = 0;
    std::optional<net::PacketSequenceStats> packet_seq_stats;

    if (args->input) {
        outcome = run_replay(*args->input, options);
    } else {
        auto result = net::run_udp_listen(*args->listen_port, options, std::chrono::milliseconds(args->idle_timeout_ms));
        outcome = std::move(result.outcome);
        packets_received = result.packets_received;
        packet_errors = result.packet_errors;
        packet_seq_stats = result.packet_seq_stats;
    }

    std::cout << "=== Replay Summary ===\n";
    if (args->input) {
        std::cout << "input:               " << *args->input << "\n";
    } else {
        std::cout << "listened on port:    " << *args->listen_port << "\n";
        std::cout << "packets received:    " << packets_received << "\n";
        std::cout << "packet errors:       " << packet_errors << "\n";
        if (packet_seq_stats) {
            std::cout << "packet sequencing:   in_order=" << packet_seq_stats->in_order
                       << " duplicate=" << packet_seq_stats->duplicate
                       << " out_of_order=" << packet_seq_stats->out_of_order
                       << " gaps=" << packet_seq_stats->missing_events << "\n";
        }
    }
    std::cout << "messages processed:  " << outcome.stats.messages_processed << "\n";
    std::cout << "decode failures:     " << outcome.stats.decode_failures << "\n";
    std::cout << "sequence failures:   " << outcome.stats.sequence_failures << "\n";
    std::cout << "book errors:         " << outcome.stats.book_errors << "\n";
    std::cout << "adds:                " << outcome.stats.adds << "\n";
    std::cout << "cancels:             " << outcome.stats.cancels << "\n";
    std::cout << "modifies:            " << outcome.stats.modifies << "\n";
    std::cout << "trades:              " << outcome.stats.trades << "\n";
    std::cout << "clears:              " << outcome.stats.clears << "\n";
    std::cout << "replay duration:     " << (static_cast<double>(outcome.stats.duration_ns) / 1e6) << " ms\n";
    std::cout << std::fixed << std::setprecision(1);
    std::cout << "messages/sec:        " << outcome.stats.messages_per_second() << "\n";
    std::cout.unsetf(std::ios::fixed);

    if (outcome.stopped_early) {
        std::cout << "\nreplay stopped early: " << outcome.stop_reason << "\n";
    }

    auto instruments = outcome.books.instruments();
    std::cout << "\ninstruments seen:    " << instruments.size() << "\n";

    for (InstrumentId id : instruments) {
        std::cout << "\n-- instrument " << id << " --\n";
        const auto* book = outcome.books.find_book(id);
        if (book != nullptr) {
            print_levels("bid", book->top_bids(args->top_levels));
            print_levels("ask", book->top_asks(args->top_levels));
        }
        const auto* trades = outcome.books.trade_stats(id);
        if (trades != nullptr) {
            std::cout << "    trades: " << trades->trade_count << ", volume: " << trades->traded_quantity
                       << ", last price: " << trades->last_trade_price << "\n";
        }
    }

    return outcome.stopped_early ? EXIT_FAILURE : EXIT_SUCCESS;
}
