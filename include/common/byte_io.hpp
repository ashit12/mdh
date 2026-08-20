#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

// Explicit big-endian (network byte order) byte encode/decode helpers.
//
// Files and UDP share one encoding, so a replay file is a raw capture of
// exactly the bytes a receiver would see. It is big-endian to match the
// "network byte order" convention every other wire protocol follows, and
// that htons/htonl exist for.
//
// Structs are deliberately never memcpy'd onto or off the wire: their layout
// depends on the compiler's padding and alignment and on host endianness,
// none of which are safe assumptions for a binary protocol. Every helper
// here reads and writes a byte at a time with shifts, so it is correct on
// any host and never performs an unaligned or reinterpreted read.
//
// Callers (encoder.cpp/decoder.cpp) only ever call put_u16/get_u16 etc. --
// they never needed to know or care which byte order this file picks, which
// is exactly why switching it was a one-file change.
namespace mdh::io {

inline void put_u8(std::vector<std::byte>& buf, std::uint8_t v) {
    buf.push_back(std::byte{v});
}

inline void put_u16(std::vector<std::byte>& buf, std::uint16_t v) {
    buf.push_back(std::byte((v >> 8) & 0xFF));
    buf.push_back(std::byte(v & 0xFF));
}

inline void put_u32(std::vector<std::byte>& buf, std::uint32_t v) {
    for (int shift = 24; shift >= 0; shift -= 8) {
        buf.push_back(std::byte((v >> shift) & 0xFF));
    }
}

inline void put_u64(std::vector<std::byte>& buf, std::uint64_t v) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        buf.push_back(std::byte((v >> shift) & 0xFF));
    }
}

inline void put_i64(std::vector<std::byte>& buf, std::int64_t v) {
    put_u64(buf, static_cast<std::uint64_t>(v));
}

// Bounds-checked reader over a byte span. Every getter returns std::nullopt
// (rather than reading out of bounds or throwing) when there isn't enough
// data left, so callers can turn that directly into a structured decode
// error instead of risking undefined behaviour on truncated input.
class ByteReader {
public:
    explicit ByteReader(std::span<const std::byte> data) : data_(data) {}

    [[nodiscard]] std::size_t remaining() const { return data_.size() - pos_; }

    [[nodiscard]] std::optional<std::uint8_t> get_u8() {
        if (remaining() < 1) return std::nullopt;
        auto v = std::to_integer<std::uint8_t>(data_[pos_]);
        pos_ += 1;
        return v;
    }

    [[nodiscard]] std::optional<std::uint16_t> get_u16() {
        if (remaining() < 2) return std::nullopt;
        std::uint16_t v = 0;
        for (std::size_t i = 0; i < 2; ++i) {
            v = static_cast<std::uint16_t>((v << 8) | std::to_integer<std::uint8_t>(data_[pos_ + i]));
        }
        pos_ += 2;
        return v;
    }

    [[nodiscard]] std::optional<std::uint32_t> get_u32() {
        if (remaining() < 4) return std::nullopt;
        std::uint32_t v = 0;
        for (std::size_t i = 0; i < 4; ++i) {
            v = (v << 8) | std::to_integer<std::uint8_t>(data_[pos_ + i]);
        }
        pos_ += 4;
        return v;
    }

    [[nodiscard]] std::optional<std::uint64_t> get_u64() {
        if (remaining() < 8) return std::nullopt;
        std::uint64_t v = 0;
        for (std::size_t i = 0; i < 8; ++i) {
            v = (v << 8) | std::to_integer<std::uint8_t>(data_[pos_ + i]);
        }
        pos_ += 8;
        return v;
    }

    [[nodiscard]] std::optional<std::int64_t> get_i64() {
        auto v = get_u64();
        if (!v) return std::nullopt;
        return static_cast<std::int64_t>(*v);
    }

private:
    std::span<const std::byte> data_;
    std::size_t pos_ = 0;
};

} // namespace mdh::io
