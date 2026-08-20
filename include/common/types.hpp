#pragma once

#include <cstdint>

// Primitive types shared by the protocol and book-reconstruction layers.
namespace mdh {

using Sequence = std::uint64_t;
using Timestamp = std::uint64_t;  // nanoseconds since an arbitrary epoch
using OrderId = std::uint64_t;
using InstrumentId = std::uint32_t;

// Scaled-integer price: ticks, not a floating-point unit. Floating-point
// comparison is unreliable for price-level routing and equality checks
// (0.1 + 0.2 != 0.3 in IEEE 754), so all prices are integers throughout.
// The scale is fixed at 1 tick = 0.0001 currency unit
// (4 implied decimal places); feed_generator and any future consumer must
// agree on that scale out of band, same as real fixed-point feeds do.
using Price = std::int64_t;
using Quantity = std::uint64_t;

enum class Side : std::uint8_t {
    Buy = 0,
    Sell = 1,
};

} // namespace mdh
