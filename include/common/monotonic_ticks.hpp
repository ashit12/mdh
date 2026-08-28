#pragma once

#include <chrono>
#include <cstdint>

// The project's single cheap monotonic tick source. Shared by the
// production latency tracer and the test/benchmark hr_timer so both
// convert the same counter the same way.
//
// Characterisation (resolution, read cost, empty-interval noise) lives in
// include/exchange/testing/hr_timer.hpp -- this header is only the read.
namespace mdh {

enum class TimerSource {
    ArmGenericCounter,
    X86Tsc,
    SteadyClock,
};

#if defined(__aarch64__)

inline constexpr TimerSource kTimerSource = TimerSource::ArmGenericCounter;
inline constexpr const char* kTimerSourceName = "AArch64 CNTVCT_EL0 (isb-serialised)";

[[nodiscard]] inline std::uint64_t monotonic_ticks() noexcept {
    std::uint64_t ticks = 0;
    __asm__ __volatile__("isb\n\tmrs %0, cntvct_el0" : "=r"(ticks) : : "memory");
    return ticks;
}

[[nodiscard]] inline double declared_ticks_per_second() noexcept {
    std::uint64_t freq = 0;
    __asm__ __volatile__("mrs %0, cntfrq_el0" : "=r"(freq));
    return static_cast<double>(freq);
}

#elif defined(__x86_64__)

inline constexpr TimerSource kTimerSource = TimerSource::X86Tsc;
inline constexpr const char* kTimerSourceName = "x86-64 RDTSC (lfence-serialised)";

[[nodiscard]] inline std::uint64_t monotonic_ticks() noexcept {
    __builtin_ia32_lfence();
    return __builtin_ia32_rdtsc();
}

[[nodiscard]] inline double declared_ticks_per_second() noexcept { return 0.0; }

#else

inline constexpr TimerSource kTimerSource = TimerSource::SteadyClock;
inline constexpr const char* kTimerSourceName = "std::chrono::steady_clock";

[[nodiscard]] inline std::uint64_t monotonic_ticks() noexcept {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

[[nodiscard]] inline double declared_ticks_per_second() noexcept { return 1e9; }

#endif

} // namespace mdh
