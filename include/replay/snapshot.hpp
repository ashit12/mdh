#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "book/book_manager.hpp"
#include "common/types.hpp"

namespace mdh::replay {

// A snapshot captures every resting order across every instrument's book,
// tagged with the sequence number of the last event fully applied before
// the snapshot was taken -- everything a recovery needs to reset book
// state to a known-good point and resume, without replaying full history.
//
// Trade statistics (book::InstrumentStats) are NOT captured: they are
// already documented as informational-only (see docs/protocol.md), so
// losing them across a recovery is a deliberate, minor simplification, not
// an oversight -- book depth (what recovery actually needs to be correct)
// is fully captured.
//
// File format: a 24-byte SnapshotHeader (magic, version, reserved,
// sequence_number, entry_count), followed by `entry_count` ordinary
// AddOrder wire frames (see protocol/messages.hpp) -- one per
// currently-resting order. This reuses encode_event()/decode_event()
// as-is rather than inventing a second per-order encoding: a snapshot
// entry and a live AddOrder message are wire-identical, they just arrive
// from a different source. (Not net::PacketHeader's format: that header's
// frame_count is only 16 bits, sized for what fits in one UDP datagram --
// a snapshot has no such bound and needs the wider 64-bit entry_count.)
inline constexpr std::uint32_t SNAPSHOT_MAGIC = 0x4D444832; // ASCII "MDH2" -- distinct from net::PACKET_MAGIC ("MDH1")
inline constexpr std::uint16_t SNAPSHOT_VERSION = 1;
inline constexpr std::size_t SNAPSHOT_HEADER_SIZE = 24;

// Writes a snapshot of `books`' current state to `path`, tagged with
// `sequence_number`. Returns false on I/O failure (e.g. an unwritable
// path). Entries are computed and encoded in memory before anything is
// written to `path`, so entry_count in the header is always accurate --
// unlike EventFileWriter's streaming append, a snapshot's header has to be
// known before the first byte goes out.
[[nodiscard]] bool write_snapshot(const std::string& path, Sequence sequence_number, const book::BookManager& books);

struct SnapshotLoadResult {
    Sequence sequence_number = 0;
    book::BookManager books;
};

// Reads a snapshot written by write_snapshot(). Returns std::nullopt on
// I/O failure or a malformed/incompatible file (bad magic/version,
// truncated header/entries, a non-AddOrder frame, or an entry that fails
// to apply to a fresh BookManager e.g. a duplicate order id -- none of
// which a well-formed snapshot should ever produce).
[[nodiscard]] std::optional<SnapshotLoadResult> read_snapshot(const std::string& path);

} // namespace mdh::replay
