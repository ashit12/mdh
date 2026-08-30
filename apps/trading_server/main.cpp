// trading_server: the whole stack in one long-running process, reachable
// from a browser.
//
//   OrderEntryGateway -- real TCP order entry, on --tcp-port
//        |
//        +--> extra_event_sink --> MarketDataRouter SPSC --> routing thread
//        |    --> UDP, on every --market-data-port given
//        |
//   UiGateway -- listens on the FIRST of those UDP ports to reconstruct a
//        live book, and holds one trader-side OMS and client per demo
//        account, connected back to --tcp-port exactly as a strategy would
//        be. Serves the lot as REST and Server-Sent Events on --http-port,
//        for the React app in ui/ to render.
//
// Usage:
//   trading_server [--tcp-port 7000] [--market-data-port 7001]...
//                   [--http-port 8080] [--static-dir <path>]
//                   [--sim-cash <ticks>] [--sim-position <qty>]
//
// ── Why --market-data-port may be repeated ────────────────────────────────
// The feed is unicast UDP (net::UdpSocket has no multicast or SO_REUSEPORT
// support -- see its own header), so exactly one process can bind any given
// port, and until now that process was always UiGateway. A second
// independent subscriber -- apps/market_simulator, which runs simulated
// participants that consume this feed and trade back in over TCP -- needs a
// feed of its own, so this flag now collects a list and every published
// event goes to all of them. Listing one port behaves exactly as before.
//
// Fanning out here, in the app that owns the socket and router, rather than
// inside the gateway, keeps order entry unaware that subscribers exist:
// MarketDataRouter emits wire events into one downstream sink, and this
// file decides where they go.
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
#include <vector>

#include "exchange/gateway/order_entry_gateway.hpp"
#include "exchange/market_data/market_data_router.hpp"
#include "net/packet.hpp"
#include "net/udp_socket.hpp"
#include "ui_gateway/ui_gateway.hpp"

using namespace mdh;
using namespace mdh::exchange;
using namespace mdh::exchange::gateway;

namespace {

// The two accounts apps/market_simulator's participants trade as --
// deliberately not UiGatewayOptions::demo_account_ids members, and
// deliberately not 9001 either (that is live_strategy_demo's), for the
// reason that app's own top comment gives: an account shared between a bot
// and a dashboard viewer leaves neither view telling the whole story.
constexpr AccountId kSimMarketMakerAccountId = 9002;
constexpr AccountId kSimMomentumAccountId = 9003;

struct Args {
    std::uint16_t tcp_port = 7000;
    // Empty means "use the default", resolved after parsing so that giving
    // --market-data-port replaces the default rather than appending to it.
    std::vector<std::uint16_t> market_data_ports;
    std::uint16_t http_port = 8080;
    std::string static_dir;
    // Simulator participants rest a whole quote ladder at a time and both
    // sides reserve against it (cash for bids, inventory for asks), so these
    // are far larger than the dashboard's own demo amounts -- a long run
    // otherwise becomes a study of risk rejections rather than of trading.
    // 10^14 ticks is 10^10 currency units; 10^6 units of inventory.
    ledger::Balance sim_cash = 100'000'000'000'000;
    Quantity sim_position = 1'000'000;
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
            args.market_data_ports.push_back(static_cast<std::uint16_t>(std::stoul(*v)));
        } else if (flag == "--sim-cash") {
            auto v = next();
            if (!v) return std::nullopt;
            args.sim_cash = static_cast<ledger::Balance>(std::stoll(*v));
        } else if (flag == "--sim-position") {
            auto v = next();
            if (!v) return std::nullopt;
            args.sim_position = static_cast<Quantity>(std::stoull(*v));
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
    if (args.market_data_ports.empty()) {
        args.market_data_ports.push_back(7001);
    }
    return args;
}

void print_usage() {
    std::cerr << "Usage: trading_server [--tcp-port <port>] [--market-data-port <port>]...\n"
              << "                       [--http-port <port>] [--static-dir <path>]\n"
              << "                       [--sim-cash <ticks>] [--sim-position <qty>]\n"
              << "\n--market-data-port may be repeated: the first is the UI gateway's, and\n"
                 "every event is published to all of them (see this file's own comment).\n";
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

    // ── Market-data publishing: exchange event -> SPSC -> routing thread -> UDP
    // The matching thread assigns the feed sequence and performs one bounded
    // push. Packet construction, fan-out and send_to() run on the router.
    net::UdpSocket market_data_socket;
    if (!market_data_socket.is_open()) {
        std::cerr << "failed to create market-data UDP socket\n";
        return EXIT_FAILURE;
    }
    std::uint64_t next_packet_sequence = 1;
    market_data::MarketDataRouter market_data_router{
        [&](const protocol::Event& wire_event) {
            const std::array<protocol::Event, 1> frames{wire_event};
            auto datagram = net::pack_frames(next_packet_sequence++, std::span<const protocol::Event>(frames));
            for (std::uint16_t port : args->market_data_ports) {
                (void)market_data_socket.send_to(datagram, "127.0.0.1", port);
            }
        },
    };
    market_data_router.start();

    // Declared up here because the exchange needs the instrument list before
    // it is constructed, not just when accounts are seeded: this is the
    // whole tradeable universe of the process, and an order on anything else
    // is rejected.
    ui_gateway::UiGatewayOptions ui_options;

    OrderEntryGatewayOptions gateway_options;
    gateway_options.instruments = ui_options.demo_instrument_ids;
    gateway_options.extra_event_sink = market_data_router.sink();

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

    // apps/market_simulator's two participants, seeded here for the same
    // reason and at the same point as every account above: the ledger has no
    // internal locking, so all seeding must happen before start(). Their
    // own process seeds a matching local mirror -- a documented, not
    // runtime-enforced, agreement between the two, exactly like the
    // market-data port itself.
    for (AccountId account_id : {kSimMarketMakerAccountId, kSimMomentumAccountId}) {
        gateway.deposit_cash(account_id, args->sim_cash);
        for (InstrumentId instrument_id : ui_options.demo_instrument_ids) {
            gateway.deposit_position(account_id, instrument_id, args->sim_position);
        }
    }

    if (!gateway.start()) {
        std::cerr << "failed to start order-entry gateway on port " << args->tcp_port << "\n";
        return EXIT_FAILURE;
    }
    std::cout << "order-entry gateway listening on tcp:" << *gateway.local_port() << "\n";

    const std::uint16_t ui_market_data_port = args->market_data_ports.front();
    ui_options.static_files_dir = args->static_dir;
    ui_gateway::UiGateway ui(gateway, args->tcp_port, ui_market_data_port, args->http_port, ui_options);
    if (!ui.start()) {
        std::cerr << "failed to start UI gateway (http-port " << args->http_port << " or market-data-port "
                   << ui_market_data_port << " already in use?)\n";
        gateway.stop();
        market_data_router.stop();
        return EXIT_FAILURE;
    }
    std::cout << "ui gateway listening on http:" << *ui.local_http_port() << " (market data on udp:"
              << ui_market_data_port << ")\n";
    std::cout << "market data published to udp:";
    for (std::uint16_t port : args->market_data_ports) {
        std::cout << " " << port;
    }
    std::cout << "\ndemo accounts:";
    for (AccountId account_id : ui_options.demo_account_ids) {
        std::cout << " " << account_id;
    }
    std::cout << "\nlive-strategy-demo account: " << kLiveStrategyDemoAccountId << " (see apps/live_strategy_demo)\n"
               << "market-simulator accounts: " << kSimMarketMakerAccountId << " (market maker) "
               << kSimMomentumAccountId << " (momentum strategy) (see apps/market_simulator)\n"
               << "press Ctrl+C to stop\n";

    std::signal(SIGINT, on_sigint);
    while (!g_stop_requested.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "\nshutting down...\n";
    ui.stop();
    gateway.stop();
    market_data_router.stop();
    std::cout << "market-data router: routed " << market_data_router.routed_count() << ", dropped "
              << market_data_router.dropped_count() << ", queue high-water "
              << market_data_router.queue_high_water_mark() << "\n";
    return EXIT_SUCCESS;
}
