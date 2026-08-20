// Decode/encode throughput for both wire codecs this project hand-rolled
// (protocol/ for market data, protocol/order_entry/ for order entry) --
// measured here rather than only asserted correct by
// tests/test_protocol_roundtrip.cpp / tests/test_order_entry_codec.cpp.
//
// Each case pre-encodes its input once in a Setup-like step outside the
// timed region (benchmark::State::KeepRunning-style loops handle this
// automatically via the for (auto _ : state) idiom below: everything before
// the loop is untimed), so decode benchmarks measure decode_event()/
// decode_message() alone, not encoding cost bleeding into the same number.
#include <benchmark/benchmark.h>

#include <cstddef>
#include <span>
#include <vector>

#include "common/types.hpp"
#include "exchange/core/types.hpp"
#include "protocol/decoder.hpp"
#include "protocol/encoder.hpp"
#include "protocol/messages.hpp"
#include "protocol/order_entry/decoder.hpp"
#include "protocol/order_entry/encoder.hpp"
#include "protocol/order_entry/messages.hpp"

using namespace mdh;

namespace {

protocol::Event make_add_order() {
    return protocol::Event{protocol::AddOrder{
        .sequence_number = 1, .timestamp_ns = 1, .order_id = 1, .instrument_id = 1, .price = 100, .quantity = 10,
        .side = Side::Buy}};
}

protocol::order_entry::Message make_new_order() {
    return protocol::order_entry::Message{protocol::order_entry::NewOrder{
        .account_id = 1,
        .client_order_id = 1,
        .instrument_id = 1,
        .side = Side::Buy,
        .price = 100,
        .quantity = 10,
        .order_type = exchange::OrderType::Limit,
        .time_in_force = exchange::TimeInForce::GTC}};
}

} // namespace

static void BM_MarketData_EncodeAddOrder(benchmark::State& state) {
    const protocol::Event event = make_add_order();
    std::vector<std::byte> out;
    for (auto _ : state) {
        out.clear();
        protocol::encode_event(event, out);
        benchmark::DoNotOptimize(out.data());
    }
    state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(out.size()));
}
BENCHMARK(BM_MarketData_EncodeAddOrder);

static void BM_MarketData_DecodeAddOrder(benchmark::State& state) {
    const protocol::Event event = make_add_order();
    std::vector<std::byte> encoded;
    protocol::encode_event(event, encoded);
    for (auto _ : state) {
        auto result = protocol::decode_event(std::span(encoded));
        benchmark::DoNotOptimize(result);
    }
    state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(encoded.size()));
}
BENCHMARK(BM_MarketData_DecodeAddOrder);

static void BM_OrderEntry_EncodeNewOrder(benchmark::State& state) {
    const auto message = make_new_order();
    std::vector<std::byte> out;
    for (auto _ : state) {
        out.clear();
        protocol::order_entry::encode_message(message, out);
        benchmark::DoNotOptimize(out.data());
    }
    state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(out.size()));
}
BENCHMARK(BM_OrderEntry_EncodeNewOrder);

static void BM_OrderEntry_DecodeNewOrder(benchmark::State& state) {
    const auto message = make_new_order();
    std::vector<std::byte> encoded;
    protocol::order_entry::encode_message(message, encoded);
    for (auto _ : state) {
        auto result = protocol::order_entry::decode_message(std::span(encoded));
        benchmark::DoNotOptimize(result);
    }
    state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(encoded.size()));
}
BENCHMARK(BM_OrderEntry_DecodeNewOrder);

BENCHMARK_MAIN();
