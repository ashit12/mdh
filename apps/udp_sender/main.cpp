// udp_sender: reads an existing binary event file (produced by
// feed_generator) and streams its events over UDP, batching multiple
// event frames into each datagram.
//
// Usage:
//   udp_sender --input events.bin --host 127.0.0.1 --port 9000 [--batch-size 20]
//
// No pacing or rate limiting -- it sends as fast as it can -- and no
// deliberate loss, reordering or corruption. That is fault injection, which
// lives in the tests rather than here.
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include "net/packet.hpp"
#include "net/udp_socket.hpp"
#include "protocol/errors.hpp"
#include "replay/event_file_reader.hpp"

using namespace mdh;
using namespace mdh::protocol;
using namespace mdh::net;

namespace {

struct Args {
    std::string input;
    std::string host;
    std::uint16_t port = 0;
    std::size_t batch_size = 20; // see README: chosen to stay comfortably under a
                                  // typical 1500-byte Ethernet MTU for any message-type mix
};

std::optional<Args> parse_args(int argc, char** argv) {
    Args args;
    bool have_input = false;
    bool have_host = false;
    bool have_port = false;

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
            have_input = true;
        } else if (flag == "--host") {
            auto v = next();
            if (!v) return std::nullopt;
            args.host = *v;
            have_host = true;
        } else if (flag == "--port") {
            auto v = next();
            if (!v) return std::nullopt;
            args.port = static_cast<std::uint16_t>(std::stoul(*v));
            have_port = true;
        } else if (flag == "--batch-size") {
            auto v = next();
            if (!v) return std::nullopt;
            args.batch_size = std::stoull(*v);
        } else {
            std::cerr << "unrecognized argument: " << flag << "\n";
            return std::nullopt;
        }
    }

    if (!have_input || !have_host || !have_port || args.batch_size == 0) {
        return std::nullopt;
    }
    return args;
}

void print_usage() {
    std::cerr << "Usage: udp_sender --input <path> --host <ip> --port <port> [--batch-size <N>]\n";
}

} // namespace

int main(int argc, char** argv) {
    auto args = parse_args(argc, argv);
    if (!args) {
        print_usage();
        return EXIT_FAILURE;
    }

    replay::EventFileReader reader(args->input);
    if (!reader.is_open()) {
        std::cerr << "failed to open input file: " << args->input << "\n";
        return EXIT_FAILURE;
    }

    UdpSocket socket;
    if (!socket.is_open()) {
        std::cerr << "failed to create UDP socket\n";
        return EXIT_FAILURE;
    }

    std::vector<Event> batch;
    batch.reserve(args->batch_size);
    std::uint64_t packet_sequence = 1;
    std::uint64_t events_sent = 0;
    std::uint64_t packets_sent = 0;
    std::uint64_t decode_failures_skipped = 0;

    auto flush_batch = [&]() {
        if (batch.empty()) {
            return;
        }
        auto datagram = pack_frames(packet_sequence, batch);
        if (socket.send_to(datagram, args->host, args->port)) {
            ++packets_sent;
            events_sent += batch.size();
            ++packet_sequence;
        } else {
            std::cerr << "failed to send packet " << packet_sequence << "\n";
        }
        batch.clear();
    };

    while (true) {
        auto frame = reader.next();
        if (!frame.has_value()) {
            break; // clean EOF
        }
        if (std::holds_alternative<DecodeError>(*frame)) {
            // The input file is our own feed_generator's output, so a
            // decode failure here means a corrupted/truncated file, not
            // an expected condition -- skip it and keep going rather than
            // aborting the whole send, so a partially-corrupt file still
            // gets as much good data across as possible.
            ++decode_failures_skipped;
            continue;
        }
        batch.push_back(std::get<Event>(*frame));
        if (batch.size() >= args->batch_size) {
            flush_batch();
        }
    }
    flush_batch();

    std::cout << "sent " << events_sent << " events in " << packets_sent << " packets to " << args->host << ":"
              << args->port << "\n";
    if (decode_failures_skipped > 0) {
        std::cout << "skipped " << decode_failures_skipped << " undecodable frames from the input file\n";
    }

    return EXIT_SUCCESS;
}
