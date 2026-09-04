#include "trader/oms/order_entry_client.hpp"

#include <array>
#include <span>
#include <utility>
#include <variant>

#include "common/thread_affinity.hpp"
#include "exchange/latency/latency_tracer.hpp"
#include "protocol/order_entry/decoder.hpp"
#include "protocol/order_entry/encoder.hpp"

namespace mdh::trader::oms {

OrderEntryClient::OrderEntryClient(MessageSink sink) : sink_(std::move(sink)) {}

OrderEntryClient::~OrderEntryClient() { disconnect(); }

bool OrderEntryClient::connect(const std::string& host, std::uint16_t port) {
    if (!socket_.connect(host, port)) {
        return false;
    }
    connected_.store(true, std::memory_order_relaxed);
    reader_thread_ = std::jthread([this] {
        set_calling_thread_name("mdh-oe-reader");
        reader_loop();
    });
    return true;
}

bool OrderEntryClient::send(const protocol::order_entry::Message& message) {
    latency::tracer().stamp_client_submit(message);

    std::lock_guard<std::mutex> lock(write_mutex_);
    write_buffer_.clear();
    protocol::order_entry::encode_message(message, write_buffer_);

    std::size_t written = 0;
    while (written < write_buffer_.size()) {
        auto n = socket_.write(std::span(write_buffer_).subspan(written));
        if (!n.ok() || n.n == 0) {
            return false;
        }
        written += n.n;
    }
    return true;
}

void OrderEntryClient::disconnect() {
    socket_.shutdown();
    if (reader_thread_.joinable()) {
        reader_thread_.join();
    }
}

void OrderEntryClient::reader_loop() {
    using namespace protocol::order_entry;

    std::array<std::byte, 4096> chunk{};
    while (true) {
        auto n = socket_.read(chunk);
        if (!n.ok() || n.n == 0) {
            break; // error, WouldBlock on a blocking socket, peer EOF, or shutdown()
        }
        read_buffer_.insert(read_buffer_.end(), chunk.begin(), chunk.begin() + static_cast<std::ptrdiff_t>(n.n));

        while (true) {
            auto header_result = decode_header(read_buffer_);
            const auto* header = std::get_if<Header>(&header_result);
            if (!header) {
                break; // not enough bytes yet for a header, or a malformed type byte -- wait for more data either way
            }
            const std::size_t frame_size = HEADER_SIZE + header->payload_size;
            if (read_buffer_.size() < frame_size) {
                break; // header decoded, but the full payload hasn't arrived yet
            }

            auto message_result = decode_message(std::span(read_buffer_).first(frame_size));
            read_buffer_.erase(read_buffer_.begin(), read_buffer_.begin() + static_cast<std::ptrdiff_t>(frame_size));
            if (const auto* message = std::get_if<Message>(&message_result)) {
                latency::tracer().stamp_client_decoded(*message);
                sink_(*message);
            }
            // A malformed payload under an otherwise well-formed header
            // drops just this one frame and keeps reading -- same policy as
            // the gateway's own connection_reader_loop().
        }
    }
    connected_.store(false, std::memory_order_relaxed);
}

} // namespace mdh::trader::oms
