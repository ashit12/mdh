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

// Labels the calling thread so that a profiler can tell this project's
// long-lived threads apart. Without it every one of them is an anonymous
// entry in `top -H`, `ps -M`, Instruments or a spindump, and attributing CPU
// to "the matching thread" versus "the market-data routing thread" comes
// down to reading stacks and guessing.
//
// Best effort and never fatal: the name is a debugging aid, and a platform
// that will not take it changes nothing about how the thread runs. Both
// platforms truncate (macOS at 64 bytes, Linux at 16 including the
// terminator), so names are chosen short.
void set_calling_thread_name(const char* name) noexcept;

} // namespace mdh
