#pragma once

#include <functional>

#include "exchange/core/events.hpp"

// The matching engine's only output boundary (Milestone 2). A plain
// std::function alias rather than a virtual interface: every current and
// near-term use (production dispatch, a test collecting events into a
// vector) is satisfied by a callable, and std::function avoids imposing
// virtual-dispatch/ownership complexity (a base class, a unique_ptr, a
// vtable) that nothing here needs yet. Revisit only if a concrete, measured
// need (e.g. avoiding std::function's type-erasure allocation on a hot path)
// actually requires it -- not preemptively.
namespace mdh::exchange {

using EventSink = std::function<void(const ExchangeEvent&)>;

} // namespace mdh::exchange
