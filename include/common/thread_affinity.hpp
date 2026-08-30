#pragma once

#include <cstdint>
#include <optional>
#include <string>

// Thread-affinity helpers for the matching thread. Linux-only in
// implementation; other platforms return a message instead of pretending
// the pin succeeded. Never throws, never changes scheduling policy.
namespace mdh {

// Pins the calling thread to one logical CPU. On success the optional is
// empty. On failure — unknown CPU, unsupported platform, or the syscall
// itself — it holds a message and the thread is left where the scheduler
// already had it.
[[nodiscard]] std::optional<std::string> pin_calling_thread_to_cpu(unsigned cpu);

// Linux gettid() of the calling thread, 0 on other platforms. The value
// that names a directory under /proc/<pid>/task/.
[[nodiscard]] std::uint64_t calling_thread_id() noexcept;

} // namespace mdh
