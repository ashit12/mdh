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
    src/net/packet.cpp
    src/net/udp_receiver.cpp
    src/net/udp_listener.cpp
)
target_include_directories(mdh_core PUBLIC include)
target_compile_options(mdh_core PRIVATE ${MDH_WARNING_FLAGS})

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
    tests/test_packet_framing.cpp
    tests/test_packet_sequence_tracker.cpp
    tests/test_udp_receiver.cpp
    tests/test_udp_replay_e2e.cpp
    tests/test_spsc_queue.cpp
    tests/test_dropping_queue.cpp
    tests/test_backpressure_integration.cpp
    tests/test_snapshot.cpp
    tests/test_sequence_recovery.cpp
    tests/test_exchange_commands.cpp
    tests/test_exchange_events.cpp
)
target_link_libraries(mdh_tests PRIVATE mdh_core gtest gtest_main)
target_compile_options(mdh_tests PRIVATE ${MDH_WARNING_FLAGS})

include(GoogleTest)
gtest_discover_tests(mdh_tests)
