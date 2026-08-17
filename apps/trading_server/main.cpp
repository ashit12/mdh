// trading_server: the first long-running process in this codebase that
// wires together everything built across Milestones 1-11 into something a
// browser can actually talk to (Milestone 12) --
//
//   OrderEntryGateway (Milestone 7)  -- real TCP order entry, on --tcp-port
//        |
//        +--> extra_event_sink (new, additive hook -- see
//        |     OrderEntryGatewayOptions::extra_event_sink's own doc
//        |     comment) --> MarketDataPublisher (Milestone 6) --> UDP, on
//        |     --market-data-port -- this is the wiring
//        |     docs/exchange_flow.md's "Integration status" section
//        |     previously listed as not yet done.
//        |
//   UiGateway (Milestone 12, include/ui_gateway/) -- listens to that same
//        UDP port to reconstruct a live book, and holds one
//        TraderRiskGatedOms + OrderEntryClient per pre-seeded demo account,
//        connected back to --tcp-port exactly like a strategy would --
//        exposing it all as REST + Server-Sent Events on --http-port for
//        ui/ (a separate React project) to render.
//
// Usage:
//   trading_server [--tcp-port 7000] [--market-data-port 7001]
//                   [--http-port 8080] [--static-dir <path>]
//
// No real shutdown story beyond SIGINT (matches apps/market_data_replay's
// own documented scope decision to skip graceful-shutdown engineering for
// a demo app) -- Ctrl+C requests a stop; every owned piece is torn down in
// dependency order (UiGateway first, since its Sessions depend on the
// exchange gateway being reachable; then the gateway itself) before this
// process exits.
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <thread>

#include "exchange/gateway/order_entry_gateway.hpp"
#include "exchange/market_data/market_data_publisher.hpp"
#include "net/packet.hpp"
#include "net/udp_socket.hpp"
#include "ui_gateway/ui_gateway.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::gateway;

namespace {

struct Args {
    std::uint16_t tcp_port = 7000;
    std::uint16_t market_data_port = 7001;
    std::uint16_t http_port = 8080;
    std::string static_dir;
};

std::optional<Args> parse_args(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        const std::string flag = argv[i];
        auto next = [&]() -> std::optional<std::string> {
            if (i + 1 >= argc) return std::nullopt;
            return std::string(argv[++i]);
        };

        if (flag == "--tcp-port") {
            auto v = next();
            if (!v) return std::nullopt;
            args.tcp_port = static_cast<std::uint16_t>(std::stoul(*v));
        } else if (flag == "--market-data-port") {
            auto v = next();
            if (!v) return std::nullopt;
            args.market_data_port = static_cast<std::uint16_t>(std::stoul(*v));
        } else if (flag == "--http-port") {
            auto v = next();
            if (!v) return std::nullopt;
            args.http_port = static_cast<std::uint16_t>(std::stoul(*v));
        } else if (flag == "--static-dir") {
            auto v = next();
            if (!v) return std::nullopt;
            args.static_dir = *v;
        } else {
            std::cerr << "unrecognized argument: " << flag << "\n";
            return std::nullopt;
        }
    }
    return args;
}

void print_usage() {
    std::cerr << "Usage: trading_server [--tcp-port <port>] [--market-data-port <port>]\n"
              << "                       [--http-port <port>] [--static-dir <path>]\n";
}

std::atomic<bool> g_stop_requested{false};
void on_sigint(int) { g_stop_requested.store(true, std::memory_order_relaxed); }

} // namespace

int main(int argc, char** argv) {
    auto args = parse_args(argc, argv);
    if (!args) {
        print_usage();
        return EXIT_FAILURE;
    }

    // ── Market-data publishing: ExchangeEvent -> protocol::Event -> UDP ────
    // Owned here (not inside OrderEntryGateway) so the gateway itself stays
    // completely unaware market data even exists -- exactly the same
    // separation-of-concerns argument event_sink.hpp makes for every other
    // EventSink consumer. Captured by reference into extra_event_sink
    // below; both outlive the gateway, which is what that lambda's
    // lifetime actually depends on, not on gateway/publisher/socket being
    // declared in any particular order relative to each other (none of
    // them reference one another directly).
    market_data::MarketDataPublisher publisher;
    net::UdpSocket market_data_socket;
    if (!market_data_socket.is_open()) {
        std::cerr << "failed to create market-data UDP socket\n";
        return EXIT_FAILURE;
    }
    std::uint64_t next_packet_sequence = 1;

    OrderEntryGatewayOptions gateway_options;
    gateway_options.extra_event_sink = [&](const ExchangeEvent& event) {
        publisher.publish(event, [&](const protocol::Event& wire_event) {
            const std::array<protocol::Event, 1> frames{wire_event};
            auto datagram = net::pack_frames(next_packet_sequence++, std::span<const protocol::Event>(frames));
            (void)market_data_socket.send_to(datagram, "127.0.0.1", args->market_data_port);
        });
    };

    OrderEntryGateway gateway(args->tcp_port, gateway_options);

    // ── Pre-seed every demo account BEFORE start() -- see
    // ui_gateway.hpp's own class comment on why this ordering, not lazy
    // per-request seeding, is what keeps Ledger's single-writer contract
    // intact once the matching thread is live.
    ui_gateway::UiGatewayOptions ui_options;
    for (AccountId account_id : ui_options.demo_account_ids) {
        gateway.deposit_cash(account_id, ui_options.demo_starting_cash);
        for (InstrumentId instrument_id : ui_options.demo_instrument_ids) {
            gateway.deposit_position(account_id, instrument_id, ui_options.demo_starting_position);
        }
    }

    // Milestone 14: also pre-seed apps/live_strategy_demo's own dedicated
    // trading account (never one of ui_options.demo_account_ids -- see
    // that app's own top comment on why it trades as an account no
    // dashboard viewer can also be trading as). Same amounts, same
    // pre-start() timing, same reasoning as
    // the demo accounts immediately above; kept as its own loop (of one)
    // rather than folded into demo_account_ids itself specifically so
    // this account is never accidentally exposed through
    // UiGateway::find_or_create_session()'s demo-account allowlist.
    constexpr AccountId kLiveStrategyDemoAccountId = 9001;
    gateway.deposit_cash(kLiveStrategyDemoAccountId, ui_options.demo_starting_cash);
    for (InstrumentId instrument_id : ui_options.demo_instrument_ids) {
        gateway.deposit_position(kLiveStrategyDemoAccountId, instrument_id, ui_options.demo_starting_position);
    }

    if (!gateway.start()) {
        std::cerr << "failed to start order-entry gateway on port " << args->tcp_port << "\n";
        return EXIT_FAILURE;
    }
    std::cout << "order-entry gateway listening on tcp:" << *gateway.local_port() << "\n";

    ui_options.static_files_dir = args->static_dir;
    ui_gateway::UiGateway ui(gateway, args->tcp_port, args->market_data_port, args->http_port, ui_options);
    if (!ui.start()) {
        std::cerr << "failed to start UI gateway (http-port " << args->http_port << " or market-data-port "
                   << args->market_data_port << " already in use?)\n";
        gateway.stop();
        return EXIT_FAILURE;
    }
    std::cout << "ui gateway listening on http:" << *ui.local_http_port() << " (market data on udp:"
              << args->market_data_port << ")\n";
    std::cout << "demo accounts:";
    for (AccountId account_id : ui_options.demo_account_ids) {
        std::cout << " " << account_id;
    }
    std::cout << "\nlive-strategy-demo account: " << kLiveStrategyDemoAccountId
               << " (see apps/live_strategy_demo)\npress Ctrl+C to stop\n";

    std::signal(SIGINT, on_sigint);
    while (!g_stop_requested.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "\nshutting down...\n";
    ui.stop();
    gateway.stop();
    return EXIT_SUCCESS;
}
