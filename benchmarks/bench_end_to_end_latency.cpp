// Milestone 13: real end-to-end order-entry latency -- submit a NewOrder
// over a real loopback TCP connection to a real, fully-wired
// OrderEntryGateway (risk + ledger + matching, exactly as
// tests/test_order_entry_gateway_e2e.cpp exercises it) and measure the wall
//-clock time until this same connection's Accepted response is fully
// decoded. This is the "end-to-end latency (p50/p99/p99.9)" this project's
// own roadmap named as a deferred benchmark
// (docs/current_system_assessment.md §9/§12) and it is measured here for
// real, with real numbers recorded in docs/benchmarks.md -- never invented,
// matching this project's own documentation discipline (see e.g.
// replay_stats.hpp's explicit non-benchmark disclaimer this project already
// carries).
//
// Not a Google Benchmark case (unlike every other bench_*.cpp here):
// Google Benchmark's model times a fixed *loop body* and reports one
// mean/median across `--benchmark_repetitions`, which is the wrong shape
// for "the full distribution of N independent round-trip samples" -- a
// p99.9 needs the individual sample values, not just their aggregate
// timing. A small hand-rolled sample loop plus a manual percentile
// computation is simpler and more honest here than contorting Google
// Benchmark's API to extract per-iteration samples it isn't designed to
// expose.
//
// Run from a Release build only -- a Debug build's numbers (unoptimized
// codec/matching-engine code, sanitizer instrumentation if enabled) are not
// representative of anything and must never be recorded in docs/benchmarks.md.
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <numeric>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "exchange/gateway/order_entry_gateway.hpp"
#include "net/tcp_socket.hpp"
#include "protocol/order_entry/decoder.hpp"
#include "protocol/order_entry/encoder.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::gateway;
using namespace mdh::net;
using namespace mdh::protocol::order_entry;
using namespace std::chrono_literals;

namespace {

constexpr InstrumentId kInstrument = 1;
constexpr AccountId kAccount = 1;

// Same minimal test-only client shape as
// tests/test_order_entry_gateway_e2e.cpp's TestClient -- duplicated rather
// than shared because that class lives in a test binary this benchmarks/
// target intentionally does not link against (see CMakeLists.txt: this
// target links mdh_core only, no gtest).
class LatencyClient {
public:
    [[nodiscard]] bool connect_to(std::uint16_t port) {
        if (!socket_.connect("127.0.0.1", port)) return false;
        socket_.set_non_blocking();
        return true;
    }

    void send(const Message& message) {
        std::vector<std::byte> buf;
        encode_message(message, buf);
        std::size_t written = 0;
        while (written < buf.size()) {
            auto n = socket_.write(std::span(buf).subspan(written));
            if (n) {
                written += *n;
            } else {
                std::this_thread::sleep_for(1ms);
            }
        }
    }

    [[nodiscard]] std::optional<Message> receive(std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (true) {
            if (auto message = try_decode_one()) return message;
            if (std::chrono::steady_clock::now() >= deadline) return std::nullopt;
            std::array<std::byte, 512> chunk{};
            if (auto n = socket_.read(chunk); n && *n > 0) {
                buffer_.insert(buffer_.end(), chunk.begin(), chunk.begin() + static_cast<std::ptrdiff_t>(*n));
            }
        }
    }

private:
    [[nodiscard]] std::optional<Message> try_decode_one() {
        auto header_result = decode_header(buffer_);
        const auto* header = std::get_if<Header>(&header_result);
        if (!header) return std::nullopt;
        const std::size_t frame_size = HEADER_SIZE + header->payload_size;
        if (buffer_.size() < frame_size) return std::nullopt;
        auto message_result = decode_message(std::span(buffer_).first(frame_size));
        buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(frame_size));
        const auto* message = std::get_if<Message>(&message_result);
        return message ? std::optional<Message>(*message) : std::nullopt;
    }

    TcpSocket socket_;
    std::vector<std::byte> buffer_;
};

double percentile(std::vector<double>& sorted_ns, double p) {
    if (sorted_ns.empty()) return 0.0;
    const auto rank = static_cast<std::size_t>(p * static_cast<double>(sorted_ns.size() - 1));
    return sorted_ns[rank];
}

// ── The transport floor ────────────────────────────────────────────────────
//
// The number above is only interpretable against what the same round trip
// costs with nothing in the middle. This is that: the identical client
// sending the identical bytes to a server whose entire job is to notice that
// a whole frame arrived and write back a canned Accepted of exactly the size
// the real gateway would send.
//
// It is a floor, not a control. It has one thread where the gateway has a
// reactor and a matching thread, so the gap between the two is not "gateway
// overhead" in any pure sense -- it includes the handoffs, which are a real
// design choice and not free. What the floor does establish is how much of
// the round trip is loopback TCP and the scheduler, i.e. the part no amount
// of work on this codebase can remove.
class EchoServer {
public:
    [[nodiscard]] bool start(std::span<const std::byte> response) {
        response_.assign(response.begin(), response.end());
        if (!listener_.listen(0)) return false;
        listener_.set_non_blocking();
        port_ = listener_.local_port().value_or(0);
        if (port_ == 0) return false;
        thread_ = std::thread([this] { serve(); });
        return true;
    }

    [[nodiscard]] std::uint16_t port() const { return port_; }

    void stop() {
        running_ = false;
        if (thread_.joinable()) thread_.join();
    }

private:
    void serve() {
        std::optional<TcpSocket> peer;
        while (running_ && !peer.has_value()) {
            peer = listener_.accept();
            if (!peer.has_value()) std::this_thread::sleep_for(100us);
        }
        if (!peer.has_value()) return;
        peer->set_non_blocking();

        std::vector<std::byte> buffer;
        std::array<std::byte, 512> chunk{};
        while (running_) {
            if (auto n = peer->read(chunk); n && *n > 0) {
                buffer.insert(buffer.end(), chunk.begin(), chunk.begin() + static_cast<std::ptrdiff_t>(*n));
            }
            // Frame the request the same way the client frames the reply, so
            // the floor pays for a header decode too rather than counting
            // bytes and pretending that is the same thing.
            while (true) {
                auto header_result = decode_header(buffer);
                const auto* header = std::get_if<Header>(&header_result);
                if (header == nullptr) break;
                const std::size_t frame_size = HEADER_SIZE + header->payload_size;
                if (buffer.size() < frame_size) break;
                buffer.erase(buffer.begin(), buffer.begin() + static_cast<std::ptrdiff_t>(frame_size));

                std::size_t written = 0;
                while (written < response_.size()) {
                    if (auto n = peer->write(std::span(response_).subspan(written)); n) written += *n;
                }
            }
        }
    }

    TcpSocket listener_;
    std::uint16_t port_ = 0;
    std::vector<std::byte> response_;
    std::atomic<bool> running_{true};
    std::thread thread_;
};

// One arm of the measurement: warm up, then time `iterations` round trips of
// the same NewOrder. Returns the samples sorted, ready for percentiles.
[[nodiscard]] std::vector<double> measure_round_trips(LatencyClient& client, std::size_t iterations) {
    const auto round_trip = [&client](ClientOrderId client_order_id) {
        client.send(Message{NewOrder{.account_id = kAccount,
                                      .client_order_id = client_order_id,
                                      .instrument_id = kInstrument,
                                      .side = Side::Buy,
                                      .price = 1,
                                      .quantity = 1,
                                      .order_type = OrderType::Limit,
                                      .time_in_force = TimeInForce::IOC}});
        return client.receive(1000ms);
    };

    // Warm-up: not recorded -- lets the OS/TCP stack settle (connection
    // fully established, any lazy first-call allocation paid for) before
    // the timed region begins, same rationale as any benchmark's warm-up
    // phase.
    for (int i = 0; i < 100; ++i) {
        (void)round_trip(static_cast<ClientOrderId>(1'000'000 + i));
    }

    std::vector<double> samples_ns;
    samples_ns.reserve(iterations);
    ClientOrderId client_order_id = 1;
    for (std::size_t i = 0; i < iterations; ++i, ++client_order_id) {
        // IOC against an empty book: accepted, immediately discarded (no
        // resting order left to accumulate across iterations), one round
        // trip per iteration -- isolates gateway+matching+risk+ledger
        // latency from any book-depth growth confound.
        const auto start = std::chrono::steady_clock::now();
        const auto response = round_trip(client_order_id);
        const auto end = std::chrono::steady_clock::now();
        if (!response.has_value()) {
            std::fprintf(stderr, "no response for iteration %zu -- aborting\n", i);
            return {};
        }
        samples_ns.push_back(std::chrono::duration<double, std::nano>(end - start).count());
    }
    std::sort(samples_ns.begin(), samples_ns.end());
    return samples_ns;
}

void report(const char* title, std::vector<double>& sorted_ns) {
    const double sum = std::accumulate(sorted_ns.begin(), sorted_ns.end(), 0.0);
    std::printf("\n%s\n", title);
    std::printf("samples: %zu\n", sorted_ns.size());
    std::printf("min:    %.2f us\n", sorted_ns.front() / 1000.0);
    std::printf("mean:   %.2f us\n", (sum / static_cast<double>(sorted_ns.size())) / 1000.0);
    std::printf("p50:    %.2f us\n", percentile(sorted_ns, 0.50) / 1000.0);
    std::printf("p90:    %.2f us\n", percentile(sorted_ns, 0.90) / 1000.0);
    std::printf("p99:    %.2f us\n", percentile(sorted_ns, 0.99) / 1000.0);
    std::printf("p99.9:  %.2f us\n", percentile(sorted_ns, 0.999) / 1000.0);
    std::printf("max:    %.2f us\n", sorted_ns.back() / 1000.0);
}

} // namespace

int main(int argc, char** argv) {
    std::size_t iterations = 10'000;
    if (argc > 1) {
        iterations = static_cast<std::size_t>(std::atoll(argv[1]));
    }

    OrderEntryGateway gateway(0, OrderEntryGatewayOptions{.instruments = {kInstrument}});
    if (!gateway.start()) {
        std::fprintf(stderr, "failed to start gateway\n");
        return EXIT_FAILURE;
    }
    gateway.deposit_cash(kAccount, 1'000'000'000'000LL);

    LatencyClient client;
    if (!client.connect_to(*gateway.local_port())) {
        std::fprintf(stderr, "failed to connect to gateway\n");
        return EXIT_FAILURE;
    }

    std::vector<double> gateway_ns = measure_round_trips(client, iterations);
    gateway.stop();
    if (gateway_ns.empty()) {
        return EXIT_FAILURE;
    }

    // The floor, measured second and in the same process, so it sees the
    // same machine state rather than a number carried over from another run.
    std::vector<std::byte> canned_response;
    encode_message(Message{Accepted{.account_id = kAccount,
                                     .client_order_id = 1,
                                     .exchange_order_id = 1,
                                     .instrument_id = kInstrument,
                                     .side = Side::Buy,
                                     .price = 1,
                                     .quantity = 1,
                                     .order_type = OrderType::Limit,
                                     .time_in_force = TimeInForce::IOC}},
                   canned_response);

    EchoServer echo;
    if (!echo.start(canned_response)) {
        std::fprintf(stderr, "failed to start echo server\n");
        return EXIT_FAILURE;
    }
    LatencyClient echo_client;
    if (!echo_client.connect_to(echo.port())) {
        std::fprintf(stderr, "failed to connect to echo server\n");
        return EXIT_FAILURE;
    }
    std::vector<double> floor_ns = measure_round_trips(echo_client, iterations);
    echo.stop();
    if (floor_ns.empty()) {
        return EXIT_FAILURE;
    }

    std::printf("mdh order-entry latency: NewOrder -> Accepted, IOC, empty book, loopback TCP\n");
    report("Fully-wired gateway (decode, risk, ledger, matching, encode, two thread handoffs)", gateway_ns);
    report("Transport floor (same bytes, same client, canned reply, nothing in between)", floor_ns);

    std::printf("\nWhat this codebase adds, floor subtracted at each percentile\n");
    for (const double p : {0.50, 0.90, 0.99, 0.999}) {
        const double added = percentile(gateway_ns, p) - percentile(floor_ns, p);
        std::printf("p%-5.4g %8.2f us  (%.0f%% of the round trip)\n", p * 100.0, added / 1000.0,
                    100.0 * added / percentile(gateway_ns, p));
    }
    return EXIT_SUCCESS;
}
