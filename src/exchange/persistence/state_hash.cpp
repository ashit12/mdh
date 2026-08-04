#include "exchange/persistence/state_hash.hpp"

#include <vector>

#include "common/byte_io.hpp"

namespace mdh::exchange::persistence {

namespace {

void put_order(std::vector<std::byte>& buf, const ExchangeRestingOrder& order) {
    io::put_u64(buf, order.exchange_order_id);
    io::put_u64(buf, order.client_order_id);
    io::put_u64(buf, order.account_id);
    io::put_u32(buf, order.instrument_id);
    io::put_u8(buf, static_cast<std::uint8_t>(order.side));
    io::put_i64(buf, order.price);
    io::put_u64(buf, order.original_quantity);
    io::put_u64(buf, order.remaining_quantity);
    io::put_u8(buf, static_cast<std::uint8_t>(order.time_in_force));
    io::put_u64(buf, order.order_sequence);
}

// FNV-1a, 64-bit. Simple, dependency-free, and more than adequate for a
// test-facing fingerprint -- this is not a cryptographic or collision-
// resistance requirement, just a convenient single-value stand-in for
// EngineStateSnapshot equality.
constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

std::uint64_t fnv1a(const std::vector<std::byte>& bytes) {
    std::uint64_t hash = kFnvOffsetBasis;
    for (const std::byte b : bytes) {
        hash ^= std::to_integer<std::uint8_t>(b);
        hash *= kFnvPrime;
    }
    return hash;
}

} // namespace

std::uint64_t hash_state_snapshot(const EngineStateSnapshot& snapshot) {
    std::vector<std::byte> buf;
    io::put_u64(buf, snapshot.instruments.size());
    for (const auto& instrument : snapshot.instruments) {
        io::put_u32(buf, instrument.instrument_id);
        io::put_u64(buf, instrument.bids.size());
        for (const auto& order : instrument.bids) {
            put_order(buf, order);
        }
        io::put_u64(buf, instrument.asks.size());
        for (const auto& order : instrument.asks) {
            put_order(buf, order);
        }
    }
    return fnv1a(buf);
}

} // namespace mdh::exchange::persistence
