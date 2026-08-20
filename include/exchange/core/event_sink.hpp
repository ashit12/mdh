#pragma once

#include <functional>

#include "exchange/core/events.hpp"

// The matching engine's only output boundary.
//
// A std::function alias rather than a virtual interface: every use -- real
// dispatch, and a test collecting events into a vector -- is satisfied by a
// callable, so a base class, a unique_ptr and a vtable would buy nothing.
// Worth revisiting only against a measured need, such as avoiding
// std::function's type erasure on a hot path.
namespace mdh::exchange {

using EventSink = std::function<void(const ExchangeEvent&)>;

} // namespace mdh::exchange
