#include "replay/snapshot.hpp"

#include <fstream>
#include <variant>

#include "common/byte_io.hpp"
#include "protocol/decoder.hpp"
#include "protocol/encoder.hpp"

namespace mdh::replay {

namespace {

void append_side(const book::OrderBook& book, InstrumentId id, Side side, bool is_bid, Sequence sequence_number,
                  std::uint64_t& entry_count, std::vector<std::byte>& out) {
    const auto orders = is_bid ? book.all_bids() : book.all_asks();
    for (const auto& o : orders) {
        protocol::encode_event(protocol::Event{protocol::AddOrder{.sequence_number = sequence_number,
                                                                    .timestamp_ns = 0,
                                                                    .order_id = o.order_id,
                                                                    .instrument_id = id,
                                                                    .price = o.price,
                                                                    .quantity = o.quantity,
                                                                    .side = side}},
                               out);
        ++entry_count;
    }
}

} // namespace

bool write_snapshot(const std::string& path, Sequence sequence_number, const book::BookManager& books) {
    std::vector<std::byte> entries;
    std::uint64_t entry_count = 0;

    for (InstrumentId id : books.instruments()) {
        const auto* book = books.find_book(id);
        if (book == nullptr) {
            continue;
        }
        append_side(*book, id, Side::Buy, /*is_bid=*/true, sequence_number, entry_count, entries);
        append_side(*book, id, Side::Sell, /*is_bid=*/false, sequence_number, entry_count, entries);
    }

    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        return false;
    }

    std::vector<std::byte> header;
    header.reserve(SNAPSHOT_HEADER_SIZE);
    io::put_u32(header, SNAPSHOT_MAGIC);
    io::put_u16(header, SNAPSHOT_VERSION);
    io::put_u16(header, 0); // reserved
    io::put_u64(header, sequence_number);
    io::put_u64(header, entry_count);

    file.write(reinterpret_cast<const char*>(header.data()), static_cast<std::streamsize>(header.size()));
    file.write(reinterpret_cast<const char*>(entries.data()), static_cast<std::streamsize>(entries.size()));
    return file.good();
}

std::optional<SnapshotLoadResult> read_snapshot(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        return std::nullopt;
    }

    std::vector<std::byte> header_buf(SNAPSHOT_HEADER_SIZE);
    file.read(reinterpret_cast<char*>(header_buf.data()), static_cast<std::streamsize>(SNAPSHOT_HEADER_SIZE));
    if (static_cast<std::size_t>(file.gcount()) < SNAPSHOT_HEADER_SIZE) {
        return std::nullopt;
    }

    io::ByteReader r(header_buf);
    auto magic = r.get_u32();
    auto version = r.get_u16();
    auto reserved = r.get_u16();
    auto sequence_number = r.get_u64();
    auto entry_count = r.get_u64();

    if (!magic || !version || !reserved || !sequence_number || !entry_count) {
        return std::nullopt;
    }
    if (*magic != SNAPSHOT_MAGIC || *version != SNAPSHOT_VERSION || *reserved != 0) {
        return std::nullopt;
    }

    SnapshotLoadResult result;
    result.sequence_number = *sequence_number;

    std::vector<std::byte> frame_buf;
    for (std::uint64_t i = 0; i < *entry_count; ++i) {
        frame_buf.resize(protocol::HEADER_SIZE);
        file.read(reinterpret_cast<char*>(frame_buf.data()), static_cast<std::streamsize>(protocol::HEADER_SIZE));
        if (static_cast<std::size_t>(file.gcount()) < protocol::HEADER_SIZE) {
            return std::nullopt;
        }

        auto header_result = protocol::decode_header(frame_buf);
        if (std::holds_alternative<protocol::DecodeError>(header_result)) {
            return std::nullopt;
        }
        const auto& header = std::get<protocol::Header>(header_result);
        if (header.type != protocol::MessageType::AddOrder) {
            return std::nullopt; // a well-formed snapshot only ever contains AddOrder entries
        }

        frame_buf.resize(protocol::HEADER_SIZE + header.payload_size);
        file.read(reinterpret_cast<char*>(frame_buf.data()) + protocol::HEADER_SIZE,
                   static_cast<std::streamsize>(header.payload_size));
        if (static_cast<std::size_t>(file.gcount()) < header.payload_size) {
            return std::nullopt;
        }

        auto event_result = protocol::decode_event(frame_buf);
        if (std::holds_alternative<protocol::DecodeError>(event_result)) {
            return std::nullopt;
        }
        const auto& add = std::get<protocol::AddOrder>(std::get<protocol::Event>(event_result));
        auto err = result.books.book_for(add.instrument_id).add_order(add.order_id, add.price, add.quantity, add.side);
        if (err) {
            return std::nullopt; // a well-formed snapshot should never hit a book error (e.g. duplicate id)
        }
    }

    return result;
}

} // namespace mdh::replay
