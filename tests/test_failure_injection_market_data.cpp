#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include <httplib.h>
#include <nlohmann/json.hpp>

#include "common/byte_io.hpp"
#include "exchange/gateway/order_entry_gateway.hpp"
#include "net/packet.hpp"
#include "net/udp_receiver.hpp"
#include "net/udp_socket.hpp"
#include "protocol/encoder.hpp"
#include "protocol/messages.hpp"
#include "ui_gateway/ui_gateway.hpp"

// Failure injection against UiGateway's live market-data UDP
// listener (market_data_loop()) -- corrupt bytes, dropped
// (gapped) sequence numbers, and duplicated events sent as raw, hand-built
// UDP datagrams directly to its market-data port, bypassing
// MarketDataPublisher entirely so this test has full, deliberate control
// over exactly what "goes wrong" on the wire. Every existing sequence-gap/
// corruption test in this codebase (tests/test_sequence_recovery.cpp,
// tests/test_udp_replay_e2e.cpp, tests/test_decoder_errors.cpp) exercises
// either the codec in isolation or the *file-replay*/net::run_udp_listen()
// path -- neither exercises UiGateway::market_data_loop() itself, which
// has its own, real, and non-obvious divergence from run_replay()'s
// default behavior: see the second test below.
using namespace mdh;
using namespace mdh::exchange;
using namespace std::chrono_literals;
using json = nlohmann::json;

namespace {

[[nodiscard]] std::uint16_t pick_ephemeral_udp_port() {
    net::UdpReceiver probe(0);
    return *probe.local_port();
}

// Same minimal harness as tests/test_ui_gateway.cpp's RunningStack, but
// deliberately does NOT wire OrderEntryGatewayOptions::extra_event_sink --
// every datagram these tests send to market_data_port() is hand-crafted by
// the test itself (see helpers below), never derived from a real matching
// event, so nothing here should route through MarketDataPublisher at all.
class RunningStack {
public:
    explicit RunningStack(ui_gateway::UiGatewayOptions ui_options = {})
        : market_data_port_(pick_ephemeral_udp_port()) {
        gateway_ = std::make_unique<exchange::gateway::OrderEntryGateway>(0);
        gateway_started_ = gateway_->start();

        ui_ = std::make_unique<ui_gateway::UiGateway>(*gateway_, *gateway_->local_port(), market_data_port_,
                                                        /*http_port=*/0, ui_options);
        ui_started_ = gateway_started_ && ui_->start();
    }

    ~RunningStack() {
        if (ui_) ui_->stop();
        if (gateway_) gateway_->stop();
    }

    RunningStack(const RunningStack&) = delete;
    RunningStack& operator=(const RunningStack&) = delete;

    [[nodiscard]] bool started() const { return ui_started_; }
    [[nodiscard]] std::uint16_t http_port() const { return *ui_->local_http_port(); }
    [[nodiscard]] std::uint16_t market_data_port() const { return market_data_port_; }

private:
    std::uint16_t market_data_port_;
    std::unique_ptr<exchange::gateway::OrderEntryGateway> gateway_;
    bool gateway_started_ = false;
    std::unique_ptr<ui_gateway::UiGateway> ui_;
    bool ui_started_ = false;
};

[[nodiscard]] bool wait_until(const std::function<bool()>& predicate, std::chrono::milliseconds timeout = 2000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(5ms);
    }
    return predicate();
}

protocol::Event add_order(Sequence seq, OrderId order_id, InstrumentId instrument_id, Price price, Quantity qty,
                           Side side) {
    return protocol::Event{protocol::AddOrder{.sequence_number = seq,
                                               .timestamp_ns = seq,
                                               .order_id = order_id,
                                               .instrument_id = instrument_id,
                                               .price = price,
                                               .quantity = qty,
                                               .side = side}};
}

protocol::Event cancel_order(Sequence seq, OrderId order_id, InstrumentId instrument_id) {
    return protocol::Event{
        protocol::CancelOrder{.sequence_number = seq, .timestamp_ns = seq, .order_id = order_id, .instrument_id = instrument_id}};
}

// AddOrder's on-wire `side` byte offset: 20-byte header + order_id(8) +
// instrument_id(4) + price(8) + quantity(8) -- see
// src/protocol/encoder.cpp's encode_event() for the exact field order this
// mirrors.
constexpr std::size_t kAddOrderSideByteOffset = protocol::HEADER_SIZE + 8 + 4 + 8 + 8;

// Builds one encoded AddOrder frame with its `side` byte forced to a value
// that is neither Side::Buy nor Side::Sell -- decode_event() will reject
// this with DecodeError::InvalidSide once unpack_frames() reaches it, while
// leaving payload_size (and therefore this frame's length -- and packet
// framing as a whole) untouched, exactly mirroring
// test_failure_injection_gateway.cpp's "well-formed header, invalid
// payload field" fault for the order-entry protocol.
[[nodiscard]] std::vector<std::byte> encode_add_order_with_invalid_side(const protocol::AddOrder& order) {
    std::vector<std::byte> bytes;
    protocol::encode_event(protocol::Event{order}, bytes);
    bytes.at(kAddOrderSideByteOffset) = std::byte{0x7F};
    return bytes;
}

// Wraps already-encoded frame bytes in a PacketHeader exactly like
// net::pack_frames() does (see net/packet.cpp) -- duplicated instead of
// reused because net::pack_frames() only accepts already-valid
// protocol::Event objects, and this file's whole point is injecting bytes
// that couldn't have come from encode_event() applied to a valid Event
// (the corrupted-side-byte case) or that combine several independently
// built frames (the duplicate/gap tests below).
[[nodiscard]] std::vector<std::byte> pack_raw(std::uint64_t packet_sequence, std::uint16_t frame_count,
                                                std::span<const std::byte> payload) {
    std::vector<std::byte> out;
    out.reserve(net::PACKET_HEADER_SIZE + payload.size());
    io::put_u32(out, net::PACKET_MAGIC);
    io::put_u16(out, net::PACKET_VERSION);
    io::put_u16(out, frame_count);
    io::put_u64(out, packet_sequence);
    io::put_u32(out, static_cast<std::uint32_t>(payload.size()));
    out.insert(out.end(), payload.begin(), payload.end());
    return out;
}

[[nodiscard]] json get_book(httplib::Client& cli, InstrumentId instrument_id) {
    auto res = cli.Get("/api/book/" + std::to_string(instrument_id));
    if (!res || res->status != 200) return json{};
    return json::parse(res->body);
}

} // namespace

TEST(FailureInjectionMarketData, CorruptWholeDatagramBadMagicIsSafelyDroppedAndDoesNotBlockLaterValidPackets) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());
    net::UdpSocket sender;

    // A datagram that fails unpack_frames() at the very first check --
    // wrong magic number, i.e. not this wire format at all (a stand-in for
    // e.g. a misdirected/foreign packet landing on this port, or line
    // noise). net::PacketError::InvalidMagic -- market_data_loop()'s own
    // `if (std::holds_alternative<net::PacketError>(unpacked)) continue;`
    // must drop the whole datagram without touching outcome/books at all.
    std::vector<std::byte> garbage_packet(40, std::byte{0xAB});
    ASSERT_TRUE(sender.send_to(garbage_packet, "127.0.0.1", stack.market_data_port()));

    // A subsequent, well-formed packet must still be processed normally --
    // proving the garbage datagram didn't wedge market_data_loop() or
    // leave it in a bad state.
    constexpr InstrumentId kInstrument = 1;
    auto valid_packet = net::pack_frames(1, std::span<const protocol::Event>(
                                                  std::array{add_order(1, 100, kInstrument, 100, 10, Side::Buy)}));
    ASSERT_TRUE(sender.send_to(valid_packet, "127.0.0.1", stack.market_data_port()));

    ASSERT_TRUE(wait_until([&] {
        auto book = get_book(cli, kInstrument);
        return book.contains("bids") && book.at("bids").size() == 1u &&
               book.at("bids")[0].at("price").get<Price>() == 100;
    }));
}

TEST(FailureInjectionMarketData, CorruptSingleEventInsideAWellFramedPacketIsDroppedButSiblingFramesStillApply) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());
    net::UdpSocket sender;
    constexpr InstrumentId kInstrument = 2;

    // One packet containing two frames: the first has an invalid `side`
    // byte (decode_event() will return DecodeError::InvalidSide for it),
    // the second is a perfectly valid AddOrder. Per packet.hpp's own
    // documented contract ("a per-frame DecodeError here does NOT
    // invalidate the packet as a whole"), unpack_frames() must still walk
    // past the first (corrupt) frame using its own correctly-set
    // payload_size and successfully decode the second.
    protocol::AddOrder corrupt_order{.sequence_number = 1,
                                      .timestamp_ns = 1,
                                      .order_id = 200,
                                      .instrument_id = kInstrument,
                                      .price = 50,
                                      .quantity = 5,
                                      .side = Side::Buy};
    auto corrupt_frame_bytes = encode_add_order_with_invalid_side(corrupt_order);

    std::vector<std::byte> valid_frame_bytes;
    protocol::encode_event(add_order(2, 201, kInstrument, 150, 20, Side::Sell), valid_frame_bytes);

    std::vector<std::byte> payload = corrupt_frame_bytes;
    payload.insert(payload.end(), valid_frame_bytes.begin(), valid_frame_bytes.end());
    auto packet = pack_raw(/*packet_sequence=*/1, /*frame_count=*/2, payload);
    ASSERT_TRUE(sender.send_to(packet, "127.0.0.1", stack.market_data_port()));

    // Only the valid (sell, order_id 201) frame ever reaches the book.
    ASSERT_TRUE(wait_until([&] {
        auto book = get_book(cli, kInstrument);
        return book.contains("asks") && book.at("asks").size() == 1u &&
               book.at("asks")[0].at("price").get<Price>() == 150;
    }));
    // The corrupted frame's own order never appears on the bid side at all.
    auto book = get_book(cli, kInstrument);
    EXPECT_TRUE(book.at("bids").empty());
}

TEST(FailureInjectionMarketData, SequenceGapDropsOnlyTheRevealingEventButThePipelineSelfResyncsAfterward) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());
    net::UdpSocket sender;
    constexpr InstrumentId kInstrument = 3;

    // A real, documented finding about UiGateway::market_data_loop(),
    // verified here rather than asserted from reading code alone: it
    // calls replay::apply_frame_result() (the exact same function
    // run_replay() uses) with default ReplayOptions -- stop_on_sequence_
    // error == true, no recovery_snapshot_path -- but discards its "should
    // stop" return value (`(void)replay::apply_frame_result(...)`), so a
    // sequence gap never actually halts the live feed the way it would
    // halt an offline run_replay() call over a file. What it DOES still do
    // is drop the *specific* event that revealed the gap (it returns
    // early, before reaching apply_event(), so that one event's own
    // mutation never happens) while leaving the SequenceValidator's
    // internal high-water mark advanced to the new sequence (see
    // SequenceValidator::check()'s own comment on why, for exactly this
    // reason: "so later messages are checked against the new high-water
    // mark rather than re-reporting the same gap forever") -- so every
    // event *after* the gap resumes being applied normally.
    //
    // sequence 1: order 300 added (baseline).
    // sequence 2 is never sent at all (simulated packet loss).
    // sequence 3: order 301 added -- this is the event that *reveals* the
    //             gap (observed=3, expected=2) and must be dropped.
    // sequence 4: order 302 added -- back in order relative to the new
    //             high-water mark (3), must be applied normally.
    auto packet1 = net::pack_frames(1, std::span<const protocol::Event>(
                                            std::array{add_order(1, 300, kInstrument, 100, 1, Side::Buy)}));
    auto packet3 = net::pack_frames(2, std::span<const protocol::Event>(
                                            std::array{add_order(3, 301, kInstrument, 200, 1, Side::Buy)}));
    auto packet4 = net::pack_frames(3, std::span<const protocol::Event>(
                                            std::array{add_order(4, 302, kInstrument, 300, 1, Side::Buy)}));
    ASSERT_TRUE(sender.send_to(packet1, "127.0.0.1", stack.market_data_port()));
    ASSERT_TRUE(wait_until([&] {
        auto book = get_book(cli, kInstrument);
        return book.contains("bids") && book.at("bids").size() == 1u;
    }));
    ASSERT_TRUE(sender.send_to(packet3, "127.0.0.1", stack.market_data_port()));
    ASSERT_TRUE(sender.send_to(packet4, "127.0.0.1", stack.market_data_port()));

    // Order 302 (sequence 4, post-gap) must land -- proving the pipeline
    // kept running and resynchronized instead of wedging on the gap.
    ASSERT_TRUE(wait_until([&] {
        auto book = get_book(cli, kInstrument);
        if (!book.contains("bids")) return false;
        for (const auto& level : book.at("bids")) {
            if (level.at("price").get<Price>() == 300) return true;
        }
        return false;
    }));

    // Order 301 (sequence 3, the gap-revealing event itself) must NEVER
    // land -- it was dropped, not merely delayed. Checked last (after the
    // post-gap event already arrived) so this isn't just a race won by
    // asking too early.
    auto book = get_book(cli, kInstrument);
    bool saw_price_200 = false;
    for (const auto& level : book.at("bids")) {
        if (level.at("price").get<Price>() == 200) saw_price_200 = true;
    }
    EXPECT_FALSE(saw_price_200);
    // Exactly the baseline (100) and the post-gap order (300) ever landed.
    EXPECT_EQ(book.at("bids").size(), 2u);
}

TEST(FailureInjectionMarketData, DuplicatedPacketNeverDoubleAppliesAndTheOrderCanStillBeCancelledExactlyOnce) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());
    net::UdpSocket sender;
    constexpr InstrumentId kInstrument = 4;
    constexpr OrderId kOrderId = 400;

    auto packet = net::pack_frames(1, std::span<const protocol::Event>(
                                           std::array{add_order(1, kOrderId, kInstrument, 100, 10, Side::Buy)}));
    ASSERT_TRUE(sender.send_to(packet, "127.0.0.1", stack.market_data_port()));
    ASSERT_TRUE(wait_until([&] {
        auto book = get_book(cli, kInstrument);
        return book.contains("bids") && book.at("bids").size() == 1u;
    }));

    // The exact same bytes again -- a duplicated UDP datagram (a real,
    // common failure mode for unreliable transport, distinct from a
    // corrupted one). SequenceValidator classifies sequence 1 as
    // Duplicate (it already saw 1); apply_frame_result() drops it via the
    // same early-return path as the sequence-gap test above, so this must
    // not create a second resting order, double the visible quantity, or
    // crash on what would otherwise be a duplicate order_id.
    ASSERT_TRUE(sender.send_to(packet, "127.0.0.1", stack.market_data_port()));
    std::this_thread::sleep_for(200ms); // give the (correctly no-op) duplicate a chance to have broken something, if it were going to

    auto book = get_book(cli, kInstrument);
    ASSERT_EQ(book.at("bids").size(), 1u);
    EXPECT_EQ(book.at("bids")[0].at("quantity").get<Quantity>(), 10u); // not 20 -- proves no double-apply

    // The single canonical order is still cleanly cancellable -- proves
    // order_index_ was never corrupted into believing there are two
    // distinct resting orders (or zero) for this order_id.
    auto cancel_packet = net::pack_frames(2, std::span<const protocol::Event>(
                                                  std::array{cancel_order(2, kOrderId, kInstrument)}));
    ASSERT_TRUE(sender.send_to(cancel_packet, "127.0.0.1", stack.market_data_port()));
    ASSERT_TRUE(wait_until([&] {
        auto after_cancel = get_book(cli, kInstrument);
        return after_cancel.contains("bids") && after_cancel.at("bids").empty();
    }));
}

TEST(FailureInjectionMarketData, TruncatedDatagramShorterThanItsOwnPacketHeaderIsSafelyDropped) {
    RunningStack stack;
    ASSERT_TRUE(stack.started());
    httplib::Client cli("127.0.0.1", stack.http_port());
    net::UdpSocket sender;
    constexpr InstrumentId kInstrument = 5;

    // Fewer than PACKET_HEADER_SIZE (20) bytes -- net::PacketError::
    // TruncatedHeader, the most degenerate possible malformed input (e.g.
    // a single stray byte reaching this port).
    const std::vector<std::byte> truncated(3, std::byte{0x01});
    ASSERT_TRUE(sender.send_to(truncated, "127.0.0.1", stack.market_data_port()));

    auto valid_packet = net::pack_frames(1, std::span<const protocol::Event>(
                                                  std::array{add_order(1, 500, kInstrument, 100, 1, Side::Buy)}));
    ASSERT_TRUE(sender.send_to(valid_packet, "127.0.0.1", stack.market_data_port()));

    ASSERT_TRUE(wait_until([&] {
        auto book = get_book(cli, kInstrument);
        return book.contains("bids") && book.at("bids").size() == 1u;
    }));
}
