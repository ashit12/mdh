cmake_minimum_required(VERSION 3.20)
project(mdh LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(MDH_ENABLE_ASAN "Build with AddressSanitizer" OFF)
option(MDH_ENABLE_UBSAN "Build with UndefinedBehaviorSanitizer" OFF)
option(MDH_ENABLE_TSAN "Build with ThreadSanitizer" OFF)

if(MDH_ENABLE_TSAN AND (MDH_ENABLE_ASAN OR MDH_ENABLE_UBSAN))
    message(FATAL_ERROR "MDH_ENABLE_TSAN cannot be combined with ASan/UBSan in the same binary -- build it separately.")
endif()

set(MDH_WARNING_FLAGS -Wall -Wextra -Wpedantic -Wshadow -Wconversion)

# ── Google Test ────────────────────────────────────────────────────────────
include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/heads/main.zip
    DOWNLOAD_EXTRACT_TIMESTAMP ON
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
# Keep vendored googletest sources out of compile_commands.json. A header has no
# entry of its own in a compile database, so clangd infers flags from the
# "closest" translation unit; gtest's _deps entries win that heuristic for
# include/**/*.hpp and supply no -Iinclude, which breaks IntelliSense on headers.
set(CMAKE_EXPORT_COMPILE_COMMANDS OFF)
FetchContent_MakeAvailable(googletest)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── cpp-httplib + nlohmann/json (UI gateway only) ──────────────────────────
# Both are single-header, MIT-licensed, and used nowhere except
# include/ui_gateway/ + apps/trading_server/ -- the hand-rolled TCP/UDP wire
# protocols everywhere else are completely untouched by this;
# see include/ui_gateway/ui_gateway.hpp's own class comment for why a
# library was chosen here instead of hand-rolling HTTP/WebSocket framing
# the way this project hand-rolled everything else.
set(CMAKE_EXPORT_COMPILE_COMMANDS OFF) # same _deps/clangd reasoning as googletest above
FetchContent_Declare(
    httplib
    URL https://github.com/yhirose/cpp-httplib/archive/refs/tags/v0.18.5.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP ON
)
FetchContent_MakeAvailable(httplib)

FetchContent_Declare(
    nlohmann_json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
    DOWNLOAD_EXTRACT_TIMESTAMP ON
)
FetchContent_MakeAvailable(nlohmann_json)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── Google Benchmark (benchmarks/ only) ────────────────────────────────────
# Same FetchContent pattern as googletest/httplib/json above, and same
# rationale as httplib: microbenchmark harness/statistics (warm-up,
# repetition counts, mean/median/stddev) is not the point here -- measuring
# the hand-rolled matching core, codecs, book, and SPSC queue is -- so a
# small, standard,
# well-regarded library is used instead of hand-rolling percentile/timing
# machinery a second time. The hand-rolled benchmark executables below cover
# latency distributions, staged TCP load/syscall measurements, workload
# scaling, and allocation profiling -- shapes that do not fit Google
# Benchmark's fixed-iteration model cleanly.
option(MDH_BUILD_BENCHMARKS "Build benchmarks/ (requires Google Benchmark via FetchContent)" ON)
if(MDH_BUILD_BENCHMARKS)
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "" FORCE)
    set(CMAKE_EXPORT_COMPILE_COMMANDS OFF) # same _deps/clangd reasoning as googletest above
    FetchContent_Declare(
        benchmark
        URL https://github.com/google/benchmark/archive/refs/tags/v1.9.1.tar.gz
        DOWNLOAD_EXTRACT_TIMESTAMP ON
    )
    FetchContent_MakeAvailable(benchmark)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
endif()

# ── Core library ───────────────────────────────────────────────────────────
add_library(mdh_core
    src/protocol/encoder.cpp
    src/protocol/decoder.cpp
    src/replay/event_file_reader.cpp
    src/replay/event_file_writer.cpp
    src/common/sequence_validator.cpp
    src/replay/replay_engine.cpp
    src/replay/snapshot.cpp
    src/book/order_book.cpp
    src/book/book_manager.cpp
    src/net/udp_socket.cpp
    src/net/tcp_socket.cpp
    src/net/packet.cpp
    src/net/udp_receiver.cpp
    src/net/udp_listener.cpp
    src/exchange/matching/matching_book.cpp
    src/exchange/matching/matching_engine.cpp
    src/exchange/persistence/command_encoder.cpp
    src/exchange/persistence/command_decoder.cpp
    src/exchange/persistence/command_journal_writer.cpp
    src/exchange/persistence/command_journal_reader.cpp
    src/exchange/persistence/state_hash.cpp
    src/exchange/persistence/command_replay.cpp
    src/exchange/sequencing/command_sequencer.cpp
    src/exchange/sequencing/matching_pipeline.cpp
    src/exchange/ledger/ledger.cpp
    src/exchange/risk/risk_engine.cpp
    src/exchange/risk/risk_gated_engine.cpp
    src/exchange/market_data/market_data_publisher.cpp
    src/protocol/order_entry/encoder.cpp
    src/protocol/order_entry/decoder.cpp
    src/exchange/gateway/order_entry_gateway.cpp
    src/trader/oms/order_entry_client.cpp
    src/trader/oms/order_management_system.cpp
    src/trader/positions/position_tracker.cpp
    src/trader/positions/pnl_tracker.cpp
    src/trader/risk/trader_risk_engine.cpp
    src/trader/risk/trader_risk_gated_oms.cpp
    src/trader/market_data/feed_subscriber.cpp
    src/trader/strategies/strategy_runtime.cpp
    src/trader/strategies/market_maker_strategy.cpp
    src/trader/strategies/cross_venue_arbitrage_strategy.cpp
    src/trader/strategies/ladder_market_maker.cpp
    src/trader/strategies/momentum_strategy.cpp
)
target_include_directories(mdh_core PUBLIC include)
target_compile_options(mdh_core PRIVATE ${MDH_WARNING_FLAGS})

# ── Experimental: boost::unordered_flat_map for MatchingBook's order index ──
# A/B toggle only, OFF by default (std::unordered_map, unchanged). Isolates
# exactly one type swap in matching_book.hpp behind MDH_USE_BOOST_FLAT_MAP so
# the existing latency/allocation benchmarks can be run unmodified against
# either container for a head-to-head comparison. Header-only (Boost.Unordered's
# flat containers need no compiled library), so this adds no link dependency.
option(MDH_USE_BOOST_FLAT_MAP
       "Use boost::unordered_flat_map instead of std::unordered_map for MatchingBook::index_ (A/B experiment)"
       OFF)
if(MDH_USE_BOOST_FLAT_MAP)
    find_package(Boost 1.81 CONFIG REQUIRED)
    target_link_libraries(mdh_core PUBLIC Boost::headers)
    target_compile_definitions(mdh_core PUBLIC MDH_USE_BOOST_FLAT_MAP)
endif()

if(MDH_ENABLE_ASAN)
    target_compile_options(mdh_core PUBLIC -fsanitize=address -fno-omit-frame-pointer)
    target_link_options(mdh_core PUBLIC -fsanitize=address)
endif()

if(MDH_ENABLE_UBSAN)
    target_compile_options(mdh_core PUBLIC -fsanitize=undefined -fno-omit-frame-pointer)
    target_link_options(mdh_core PUBLIC -fsanitize=undefined)
endif()

if(MDH_ENABLE_TSAN)
    target_compile_options(mdh_core PUBLIC -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(mdh_core PUBLIC -fsanitize=thread)
endif()

# ── UI gateway library ─────────────────────────────────────────────────────
# Kept as its own target, separate from mdh_core, so that httplib/json are
# only ever compiled into the two things that actually need them
# (trading_server, mdh_tests) -- every other app (feed_generator,
# market_data_replay, udp_sender) still links mdh_core alone, unchanged.
add_library(mdh_ui_gateway
    src/ui_gateway/ui_gateway.cpp
)
target_include_directories(mdh_ui_gateway PUBLIC include)
target_link_libraries(mdh_ui_gateway PUBLIC mdh_core PRIVATE httplib nlohmann_json::nlohmann_json)
target_compile_options(mdh_ui_gateway PRIVATE ${MDH_WARNING_FLAGS})

if(MDH_ENABLE_ASAN)
    target_compile_options(mdh_ui_gateway PUBLIC -fsanitize=address -fno-omit-frame-pointer)
    target_link_options(mdh_ui_gateway PUBLIC -fsanitize=address)
endif()
if(MDH_ENABLE_UBSAN)
    target_compile_options(mdh_ui_gateway PUBLIC -fsanitize=undefined -fno-omit-frame-pointer)
    target_link_options(mdh_ui_gateway PUBLIC -fsanitize=undefined)
endif()
if(MDH_ENABLE_TSAN)
    target_compile_options(mdh_ui_gateway PUBLIC -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(mdh_ui_gateway PUBLIC -fsanitize=thread)
endif()

# ── Apps ───────────────────────────────────────────────────────────────────
add_executable(feed_generator apps/feed_generator/main.cpp)
target_link_libraries(feed_generator PRIVATE mdh_core)
target_compile_options(feed_generator PRIVATE ${MDH_WARNING_FLAGS})

add_executable(market_data_replay apps/market_data_replay/main.cpp)
target_link_libraries(market_data_replay PRIVATE mdh_core)
target_compile_options(market_data_replay PRIVATE ${MDH_WARNING_FLAGS})

add_executable(udp_sender apps/udp_sender/main.cpp)
target_link_libraries(udp_sender PRIVATE mdh_core)
target_compile_options(udp_sender PRIVATE ${MDH_WARNING_FLAGS})

add_executable(trading_server apps/trading_server/main.cpp)
target_link_libraries(trading_server PRIVATE mdh_ui_gateway)
target_compile_options(trading_server PRIVATE ${MDH_WARNING_FLAGS})

# The end-to-end live demo -- links httplib/json directly (like
# mdh_ui_gateway does) rather than through that target, since this app
# only needs an httplib::Client to poll trading_server's REST API, never
# the UI gateway's own server-side code -- see apps/live_strategy_demo/
# main.cpp's own top comment.
add_executable(live_strategy_demo apps/live_strategy_demo/main.cpp)
target_link_libraries(live_strategy_demo PRIVATE mdh_core httplib nlohmann_json::nlohmann_json)
target_compile_options(live_strategy_demo PRIVATE ${MDH_WARNING_FLAGS})

# The simulated-participant driver. Links mdh_core alone -- no httplib/json,
# unlike live_strategy_demo, because it reaches the exchange only through the
# two real wire protocols (TCP order entry and the UDP market-data feed) and
# never through the UI gateway's REST surface. See apps/market_simulator/
# main.cpp's own top comment.
add_executable(market_simulator apps/market_simulator/main.cpp)
target_link_libraries(market_simulator PRIVATE mdh_core)
target_compile_options(market_simulator PRIVATE ${MDH_WARNING_FLAGS})

# ── Tests ──────────────────────────────────────────────────────────────────
enable_testing()

add_executable(mdh_tests
    tests/test_protocol_roundtrip.cpp
    tests/test_decoder_errors.cpp
    tests/test_sequence_validator.cpp
    tests/test_event_file_io.cpp
    tests/test_order_book.cpp
    tests/test_replay_e2e.cpp
    tests/test_udp_socket.cpp
    tests/test_tcp_socket.cpp
    tests/test_packet_framing.cpp
    tests/test_packet_sequence_tracker.cpp
    tests/test_udp_receiver.cpp
    tests/test_udp_replay_e2e.cpp
    tests/test_spsc_queue.cpp
    tests/test_mpsc_queue.cpp
    tests/test_dropping_queue.cpp
    tests/test_backpressure_integration.cpp
    tests/test_snapshot.cpp
    tests/test_sequence_recovery.cpp
    tests/test_exchange_commands.cpp
    tests/test_exchange_events.cpp
    tests/test_matching_book.cpp
    tests/test_matching_engine.cpp
    tests/test_command_codec.cpp
    tests/test_command_decode_errors.cpp
    tests/test_command_journal.cpp
    tests/test_exchange_replay.cpp
    tests/test_command_sequencer.cpp
    tests/test_matching_pipeline.cpp
    tests/test_ledger.cpp
    tests/test_risk_engine.cpp
    tests/test_risk_gated_engine.cpp
    tests/test_market_data_publisher.cpp
    tests/test_market_data_e2e.cpp
    tests/test_order_entry_codec.cpp
    tests/test_order_entry_decode_errors.cpp
    tests/test_order_entry_gateway_e2e.cpp
    tests/test_order_management_system.cpp
    tests/test_order_entry_client.cpp
    tests/test_oms_gateway_e2e.cpp
    tests/test_position_tracker.cpp
    tests/test_pnl_tracker.cpp
    tests/test_trader_risk_engine.cpp
    tests/test_trader_risk_gated_oms.cpp
    tests/test_trader_risk_gated_oms_e2e.cpp
    tests/test_strategy_runtime.cpp
    tests/test_market_maker_strategy.cpp
    tests/test_market_maker_strategy_e2e.cpp
    tests/test_cross_venue_arbitrage_strategy.cpp
    tests/test_cross_venue_arbitrage_strategy_e2e.cpp
    tests/test_reference_price_walk.cpp
    tests/test_ladder_market_maker.cpp
    tests/test_momentum_strategy.cpp
    tests/test_feed_subscriber.cpp
    tests/test_market_simulator_e2e.cpp
    tests/test_latency_tracer.cpp
    tests/test_ui_gateway.cpp
    tests/test_failure_injection_gateway.cpp
    tests/test_failure_injection_market_data.cpp
    tests/test_matching_engine_stress.cpp
)
target_link_libraries(mdh_tests PRIVATE mdh_ui_gateway httplib nlohmann_json::nlohmann_json gtest gtest_main)
target_compile_options(mdh_tests PRIVATE ${MDH_WARNING_FLAGS})

include(GoogleTest)
gtest_discover_tests(mdh_tests)

# ── Benchmarks ─────────────────────────────────────────────────────────────
# Every number these produce is meaningless in a Debug build (see
# docs/benchmarks.md's own note on this) -- they link exactly like mdh_tests
# (mdh_core/mdh_ui_gateway, unmodified) but are meant to be *run* from a
# `-DCMAKE_BUILD_TYPE=Release` build tree, never compared across build types.
if(MDH_BUILD_BENCHMARKS)
    # Each bench_*.cpp below ends with its own BENCHMARK_MAIN() (consistent
    # with wanting one self-contained .cpp per benchmark, mirroring this
    # project's one-file-per-component test convention), so link against
    # benchmark::benchmark (the library only) rather than
    # benchmark::benchmark_main (which supplies its own main() and would
    # collide with BENCHMARK_MAIN()'s).
    add_executable(bench_protocol_codec benchmarks/bench_protocol_codec.cpp)
    target_link_libraries(bench_protocol_codec PRIVATE mdh_core benchmark::benchmark)
    target_compile_options(bench_protocol_codec PRIVATE ${MDH_WARNING_FLAGS})

    add_executable(bench_matching_engine benchmarks/bench_matching_engine.cpp)
    target_link_libraries(bench_matching_engine PRIVATE mdh_core benchmark::benchmark)
    target_compile_options(bench_matching_engine PRIVATE ${MDH_WARNING_FLAGS})

    add_executable(bench_order_book benchmarks/bench_order_book.cpp)
    target_link_libraries(bench_order_book PRIVATE mdh_core benchmark::benchmark)
    target_compile_options(bench_order_book PRIVATE ${MDH_WARNING_FLAGS})

    add_executable(bench_spsc_queue benchmarks/bench_spsc_queue.cpp)
    target_link_libraries(bench_spsc_queue PRIVATE mdh_core benchmark::benchmark)
    target_compile_options(bench_spsc_queue PRIVATE ${MDH_WARNING_FLAGS})

    # Its own tiny standalone executable, not a benchmark::benchmark case --
    # see bench_end_to_end_latency.cpp's own top-of-file comment for why a
    # real TCP round trip's latency *distribution* (p50/p90/p99/p99.9) needs
    # a hand-rolled sample loop instead of Google Benchmark's fixed-iteration
    # timing model.
    add_executable(bench_end_to_end_latency benchmarks/bench_end_to_end_latency.cpp)
    target_link_libraries(bench_end_to_end_latency PRIVATE mdh_core)
    target_compile_options(bench_end_to_end_latency PRIVATE ${MDH_WARNING_FLAGS})

    # Staged order-path latency (T0-T5) over real TCP via OrderEntryClient.
    add_executable(bench_order_path_latency benchmarks/bench_order_path_latency.cpp)
    target_link_libraries(bench_order_path_latency PRIVATE mdh_core)
    target_compile_options(bench_order_path_latency PRIVATE ${MDH_WARNING_FLAGS})

    # The two matching-engine benchmarks below are also standalone rather
    # than Google Benchmark cases, for two different reasons.
    #
    # bench_matching_workload needs the individual per-operation samples to
    # report a p99.9, and one figure per configuration to draw a scaling
    # curve -- neither of which Google Benchmark's fixed-loop timing model
    # exposes (same reasoning as bench_end_to_end_latency above).
    #
    # bench_matching_memory replaces every global operator new/delete to
    # count allocations, which would silently perturb the timing of anything
    # else linked into the same binary -- hence its own executable rather
    # than another case inside bench_matching_engine.
    add_executable(bench_matching_workload benchmarks/bench_matching_workload.cpp)
    target_link_libraries(bench_matching_workload PRIVATE mdh_core)
    target_compile_options(bench_matching_workload PRIVATE ${MDH_WARNING_FLAGS})

    add_executable(bench_matching_memory benchmarks/bench_matching_memory.cpp)
    target_link_libraries(bench_matching_memory PRIVATE mdh_core)
    target_compile_options(bench_matching_memory PRIVATE ${MDH_WARNING_FLAGS})
endif()
