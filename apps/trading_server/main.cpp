// trading_server: the whole stack in one long-running process, reachable
// from a browser.
//
//   OrderEntryGateway -- real TCP order entry, on --tcp-port
//        |
//        +--> extra_event_sink --> MarketDataPublisher --> UDP, on
//        |    --market-data-port
//        |
//   UiGateway -- listens on that same UDP port to reconstruct a live book,
//        and holds one trader-side OMS and client per demo account,
//        connected back to --tcp-port exactly as a strategy would be. Serves
//        the lot as REST and Server-Sent Events on --http-port, for the
//        React app in ui/ to render.
//
// Usage:
//   trading_server [--tcp-port 7000] [--market-data-port 7001]
//                   [--http-port 8080] [--static-dir <path>]
//
// Shutdown is Ctrl+C and nothing more, the same scope decision the other
// demo apps make. Everything is then torn down in dependency order: the UI
// gateway first, since its sessions need the exchange gateway to still be
// reachable, then the gateway itself.
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

    // ── Market-data publishing: exchange event -> wire event -> UDP ───────
    // Owned here rather than inside the gateway, so the gateway stays
    // unaware market data exists at all. Captured by reference into
    // extra_event_sink below; both outlive the gateway, which is all that
    // lambda's lifetime depends on -- none of these three reference each
    // other, so their declaration order does not matter.
    market_data::MarketDataPublisher publisher;
    net::UdpSocket market_data_socket;
    if (!market_data_socket.is_open()) {
        std::cerr << "failed to create market-data UDP socket\n";
        return EXIT_FAILURE;
    }
    std::uint64_t next_packet_sequence = 1;

    // Declared up here because the exchange needs the instrument list before
    // it is constructed, not just when accounts are seeded: this is the
    // whole tradeable universe of the process, and an order on anything else
    // is rejected.
    ui_gateway::UiGatewayOptions ui_options;

    OrderEntryGatewayOptions gateway_options;
    gateway_options.instruments = ui_options.demo_instrument_ids;
    gateway_options.extra_event_sink = [&](const ExchangeEvent& event) {
        publisher.publish(event, [&](const protocol::Event& wire_event) {
            const std::array<protocol::Event, 1> frames{wire_event};
            auto datagram = net::pack_frames(next_packet_sequence++, std::span<const protocol::Event>(frames));
            (void)market_data_socket.send_to(datagram, "127.0.0.1", args->market_data_port);
        });
    };

    OrderEntryGateway gateway(args->tcp_port, gateway_options);

    // Seed every demo account before start(). See ui_gateway.hpp on why this
    // ordering, rather than seeding lazily per request, is what keeps the
    // ledger's single-writer contract intact once the matching thread runs.
    for (AccountId account_id : ui_options.demo_account_ids) {
        gateway.deposit_cash(account_id, ui_options.demo_starting_cash);
        for (InstrumentId instrument_id : ui_options.demo_instrument_ids) {
            gateway.deposit_position(account_id, instrument_id, ui_options.demo_starting_position);
        }
    }

    // live_strategy_demo's own account, deliberately not one of the demo
    // accounts: it trades as an account no dashboard viewer can also be
    // trading as. Same amounts and same timing as above, but kept as its own
    // loop of one so it is never exposed through the dashboard's allowlist.
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
