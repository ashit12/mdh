#include "trader/market_data/feed_subscriber.hpp"

#include <utility>
#include <variant>

#include "common/sequence_validator.hpp"
#include "net/packet.hpp"

namespace mdh::trader::market_data {

std::optional<Price> MarketSnapshot::mark_price() const {
    if (best_bid && best_ask) {
        return (best_bid->price + best_ask->price) / 2;
    }
    if (trade_count > 0) {
        return last_trade_price;
    }
    return std::nullopt;
}

FeedSubscriber::FeedSubscriber(std::uint16_t port, strategies::StrategyRuntime& runtime, FeedSubscriberOptions options)
    : port_(port), runtime_(runtime), options_(std::move(options)) {}

FeedSubscriber::~FeedSubscriber() { stop(); }

bool FeedSubscriber::start() {
    net::UdpReceiver receiver(port_, options_.max_datagram_size);
    // is_open() is not sufficient on its own: it reports that the socket was
    // created, and UdpReceiver's constructor creates the socket before
    // attempting the bind and keeps it either way (see net/udp_receiver.cpp),
    // so a receiver whose bind lost a port race is still "open" and would
    // silently receive nothing forever. The bound port is the observable
    // difference -- getsockname() reports 0 for a socket that was never
    // bound, and a successful bind never yields 0 even when 0 was requested,
    // since that is precisely the request for an OS-assigned ephemeral port.
    const auto bound_port = receiver.local_port();
    if (!receiver.is_open() || bound_port.value_or(0) == 0) {
        return false;
    }
    bound_port_ = bound_port;
    receive_thread_ = std::jthread([this, receiver = std::move(receiver)]() mutable {
        receive_loop(stop_source_.get_token(), std::move(receiver));
    });
    return true;
}

void FeedSubscriber::stop() {
    stop_source_.request_stop();
    if (receive_thread_.joinable()) {
        receive_thread_.join();
    }
}

void FeedSubscriber::receive_loop(std::stop_token token, net::UdpReceiver receiver) {
    SequenceValidator validator;

    while (!token.stop_requested()) {
        auto batch = receiver.receive_batch(options_.receive_batch_size);
        if (batch.empty()) {
            std::this_thread::sleep_for(options_.idle_sleep);
            continue;
        }

        std::lock_guard<std::mutex> lock(books_mutex_);
        packets_received_ += batch.size();
        for (const auto& datagram : batch) {
            auto unpacked = net::unpack_frames(datagram.bytes);
            if (std::holds_alternative<net::PacketError>(unpacked)) {
                ++packet_errors_;
                continue;
            }
            for (auto& frame : std::get<net::UnpackedPacket>(unpacked).frames) {
                // The event has to be copied out before apply_frame_result()
                // consumes the frame, and the strategies have to be told
                // *after* it has been applied -- they are given the book as
                // it stands once the event is in it, which is the contract
                // StrategyRuntime::on_event() documents.
                std::optional<protocol::Event> event;
                if (const auto* decoded = std::get_if<protocol::Event>(&frame)) {
                    event = *decoded;
                }
                // Ignoring the "stop processing" return, exactly like
                // UiGateway's own loop: a subscriber to a live feed has
                // nothing to gain from going deaf after one bad frame, and
                // outcome_.stats already records that it happened.
                (void)replay::apply_frame_result(std::move(frame), validator, options_.replay_options, outcome_);
                if (event) {
                    runtime_.on_event(*event, outcome_.books);
                }
            }
        }
    }
}

MarketSnapshot FeedSubscriber::snapshot(InstrumentId instrument_id) const {
    MarketSnapshot snap;
    std::lock_guard<std::mutex> lock(books_mutex_);
    if (const auto* book = outcome_.books.find_book(instrument_id); book != nullptr) {
        snap.best_bid = book->best_bid();
        snap.best_ask = book->best_ask();
    }
    if (const auto* trades = outcome_.books.trade_stats(instrument_id); trades != nullptr) {
        snap.trade_count = trades->trade_count;
        snap.traded_quantity = trades->traded_quantity;
        snap.last_trade_price = trades->last_trade_price;
    }
    return snap;
}

replay::ReplayStats FeedSubscriber::stats() const {
    std::lock_guard<std::mutex> lock(books_mutex_);
    return outcome_.stats;
}

std::uint64_t FeedSubscriber::packets_received() const {
    std::lock_guard<std::mutex> lock(books_mutex_);
    return packets_received_;
}

std::uint64_t FeedSubscriber::packet_errors() const {
    std::lock_guard<std::mutex> lock(books_mutex_);
    return packet_errors_;
}

} // namespace mdh::trader::market_data
