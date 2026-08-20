#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <vector>

// Milestone 15: a high-resolution monotonic tick source, plus the honest
// characterisation of its own limits, for measuring individual
// matching-engine operations whose true cost is in the low hundreds of
// nanoseconds.
//
// Header-only and test/benchmark-only: nothing under src/ or apps/ includes
// this, and it deliberately does not go anywhere near the matching engine's
// own code. It exists so benchmarks/ and tests/ can share one timing story
// instead of each rolling their own.
//
// Why not just std::chrono::steady_clock everywhere: on this project's
// primary development machine (Apple Silicon, macOS) a steady_clock::now()
// pair costs ~37 ns, which is a 20-25% tax on a ~170 ns operation. Reading
// the ARM generic counter directly is ~3x cheaper. Neither is free, and
// calibrate() below measures exactly how un-free, so a reader can judge the
// numbers rather than trust them.
//
// ── What this timer can and cannot justify ─────────────────────────────────
// The *unit* a source counts in is not the same thing as the granularity it
// actually advances in, and conflating them is how benchmarks end up
// claiming nanosecond accuracy they do not have:
//
//   - AArch64 (incl. Apple Silicon): CNTVCT_EL0 is a free-running counter
//     whose declared frequency is CNTFRQ_EL0. On Apple Silicon CNTFRQ_EL0
//     reads 1'000'000'000 (i.e. the counter's *unit* is 1 ns), but the
//     underlying hardware counter ticks at 24 MHz, so the observed value
//     advances in jumps of 41 or 42 units. Effective resolution is therefore
//     ~41.67 ns, not 1 ns. calibrate() measures this directly
//     (`effective_resolution_ns`) instead of trusting CNTFRQ_EL0.
//   - x86-64: RDTSC's invariant-TSC rate is not queryable portably, so it is
//     calibrated against steady_clock here. Serialised with LFENCE, since an
//     unserialised RDTSC can be reordered across the code being measured.
//   - Anything else: std::chrono::steady_clock.
//
// Consequence for per-operation sampling, stated once here and repeated in
// docs/matching_engine_baseline.md: a single sample of a sub-microsecond
// operation is quantised to a multiple of the effective resolution and
// additionally carries the cost of the two timer reads that delimit it.
// Individual samples are therefore only meaningful in aggregate
// (percentiles over a large population), and any mean or throughput figure
// should be derived from one long batch measurement -- where both the
// quantisation and the read cost are amortised across millions of
// operations -- rather than from averaging per-operation samples.
namespace mdh::exchange::testing {

enum class TimerSource {
    ArmGenericCounter,
    X86Tsc,
    SteadyClock,
};

#if defined(__aarch64__)

inline constexpr TimerSource kTimerSource = TimerSource::ArmGenericCounter;
inline constexpr const char* kTimerSourceName = "AArch64 CNTVCT_EL0 (isb-serialised)";

// `isb` before the read stops the counter read from being satisfied out of
// order with respect to the surrounding instructions -- without it an
// aggressive out-of-order core can retire the read early and shrink (or
// stretch) the interval being measured. It costs ~11 ns, which calibrate()
// reports rather than hides.
[[nodiscard]] inline std::uint64_t timer_ticks() noexcept {
    std::uint64_t ticks = 0;
    __asm__ __volatile__("isb\n\tmrs %0, cntvct_el0" : "=r"(ticks) : : "memory");
    return ticks;
}

// The frequency the architecture *declares* for the counter. Reported
// alongside the measured rate so a discrepancy is visible instead of
// silently assumed away.
[[nodiscard]] inline double declared_ticks_per_second() noexcept {
    std::uint64_t freq = 0;
    __asm__ __volatile__("mrs %0, cntfrq_el0" : "=r"(freq));
    return static_cast<double>(freq);
}

#elif defined(__x86_64__)

inline constexpr TimerSource kTimerSource = TimerSource::X86Tsc;
inline constexpr const char* kTimerSourceName = "x86-64 RDTSC (lfence-serialised)";

[[nodiscard]] inline std::uint64_t timer_ticks() noexcept {
    __builtin_ia32_lfence();
    return __builtin_ia32_rdtsc();
}

// No portable way to ask the hardware; calibrate() measures it instead.
[[nodiscard]] inline double declared_ticks_per_second() noexcept { return 0.0; }

#else

inline constexpr TimerSource kTimerSource = TimerSource::SteadyClock;
inline constexpr const char* kTimerSourceName = "std::chrono::steady_clock";

[[nodiscard]] inline std::uint64_t timer_ticks() noexcept {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

[[nodiscard]] inline double declared_ticks_per_second() noexcept { return 1e9; }

#endif

struct TimerCalibration {
    const char* source_name = kTimerSourceName;
    // What the hardware/architecture claims, 0.0 when unavailable.
    double declared_ticks_per_second = 0.0;
    // What the counter was actually observed to do, measured against
    // steady_clock over a busy-wait window. This is the rate every
    // ticks-to-nanoseconds conversion in this project uses.
    double measured_ticks_per_second = 0.0;
    // The granularity a single sample is quantised to, which on some
    // platforms is far coarser than one tick. Taken as the *median* non-zero
    // advance between consecutive reads rather than the minimum: on Apple
    // Silicon the counter normally steps by 41 or 42 units but very
    // occasionally by less, and reporting that rare small step as "the
    // resolution" would overstate what a single sample can distinguish by
    // more than a factor of two.
    double effective_resolution_ns = 0.0;
    // The smallest non-zero advance seen at all, for completeness.
    double smallest_advance_ns = 0.0;
    // Amortised cost of one timer_ticks() call, measured over a long
    // unrolled loop.
    double read_cost_ns = 0.0;
    // What an operation that takes literally zero time measures as: the
    // distribution of `b - a` for two back-to-back reads with nothing
    // between them. This is the noise floor of every per-operation sample.
    double empty_interval_p50_ns = 0.0;
    double empty_interval_p99_ns = 0.0;
};

[[nodiscard]] inline double ticks_to_ns(double ticks, double ticks_per_second) {
    return ticks_per_second > 0.0 ? ticks * 1e9 / ticks_per_second : 0.0;
}

[[nodiscard]] inline TimerCalibration calibrate_timer() {
    TimerCalibration cal;
    cal.declared_ticks_per_second = declared_ticks_per_second();

    // Rate: busy-wait (not sleep) so the comparison window is wall-clock
    // time this thread actually spent running.
    {
        const auto wall_start = std::chrono::steady_clock::now();
        const std::uint64_t tick_start = timer_ticks();
        while (std::chrono::steady_clock::now() - wall_start < std::chrono::milliseconds(200)) {
        }
        const std::uint64_t tick_end = timer_ticks();
        const auto wall_end = std::chrono::steady_clock::now();
        const double seconds = std::chrono::duration<double>(wall_end - wall_start).count();
        cal.measured_ticks_per_second = static_cast<double>(tick_end - tick_start) / seconds;
    }

    // Effective resolution and empty-interval noise floor, from the same
    // population of back-to-back reads.
    {
        constexpr int kSamples = 1'000'000;
        std::vector<std::uint64_t> deltas;
        deltas.reserve(kSamples);
        for (int i = 0; i < kSamples; ++i) {
            const std::uint64_t a = timer_ticks();
            const std::uint64_t b = timer_ticks();
            deltas.push_back(b - a);
        }
        std::sort(deltas.begin(), deltas.end());
        cal.empty_interval_p50_ns =
            ticks_to_ns(static_cast<double>(deltas[deltas.size() / 2]), cal.measured_ticks_per_second);
        cal.empty_interval_p99_ns =
            ticks_to_ns(static_cast<double>(deltas[deltas.size() * 99 / 100]), cal.measured_ticks_per_second);

        const auto first_nonzero = std::upper_bound(deltas.begin(), deltas.end(), std::uint64_t{0});
        if (first_nonzero != deltas.end()) {
            const auto nonzero_count = static_cast<std::size_t>(std::distance(first_nonzero, deltas.end()));
            cal.smallest_advance_ns = ticks_to_ns(static_cast<double>(*first_nonzero), cal.measured_ticks_per_second);
            cal.effective_resolution_ns = ticks_to_ns(
                static_cast<double>(*(first_nonzero + static_cast<std::ptrdiff_t>(nonzero_count / 2))),
                cal.measured_ticks_per_second);
        }
    }

    // Amortised per-read cost: many reads back to back, timed as one block,
    // so the measurement of the timer does not itself depend on the timer.
    {
        constexpr int kReads = 2'000'000;
        const auto wall_start = std::chrono::steady_clock::now();
        std::uint64_t sink = 0;
        for (int i = 0; i < kReads; ++i) {
            sink += timer_ticks();
        }
        const auto wall_end = std::chrono::steady_clock::now();
        // Keep the loop from being optimised away without pulling in a
        // Google Benchmark dependency (this header is used by non-gbench
        // binaries too).
        __asm__ __volatile__("" : : "r"(sink) : "memory");
        cal.read_cost_ns = std::chrono::duration<double, std::nano>(wall_end - wall_start).count() / kReads;
    }

    return cal;
}

inline void print_timer_calibration(const TimerCalibration& cal) {
    std::printf("Timer source:               %s\n", cal.source_name);
    if (cal.declared_ticks_per_second > 0.0) {
        std::printf("  declared rate:            %.0f Hz (1 tick = %.3f ns nominal)\n", cal.declared_ticks_per_second,
                    1e9 / cal.declared_ticks_per_second);
    } else {
        std::printf("  declared rate:            not queryable on this platform\n");
    }
    std::printf("  measured rate:            %.0f Hz\n", cal.measured_ticks_per_second);
    std::printf("  effective resolution:     %.2f ns  <- single samples quantise to multiples of this\n",
                cal.effective_resolution_ns);
    std::printf("  smallest advance seen:    %.2f ns\n", cal.smallest_advance_ns);
    std::printf("  cost of one read:         %.2f ns  (a sampled interval pays this twice)\n", cal.read_cost_ns);
    std::printf("  zero-work interval p50:   %.2f ns  <- what a 0 ns operation measures as\n",
                cal.empty_interval_p50_ns);
    std::printf("  zero-work interval p99:   %.2f ns\n", cal.empty_interval_p99_ns);
}

// ── Latency distributions ──────────────────────────────────────────────────

struct LatencySummary {
    std::size_t count = 0;
    double min_ns = 0.0;
    double p50_ns = 0.0;
    double p90_ns = 0.0;
    double p99_ns = 0.0;
    double p999_ns = 0.0;
    double max_ns = 0.0;
    // Mean of the individual samples. Reported for completeness, but a
    // throughput-derived mean (one long timed batch divided by operation
    // count) is the more trustworthy figure -- it does not pay two timer
    // reads per operation. See docs/matching_engine_baseline.md.
    double sampled_mean_ns = 0.0;
};

// `samples_ticks` is sorted in place.
[[nodiscard]] inline LatencySummary summarise_latency(std::vector<std::uint64_t>& samples_ticks,
                                                       double ticks_per_second) {
    LatencySummary summary;
    if (samples_ticks.empty()) {
        return summary;
    }
    std::sort(samples_ticks.begin(), samples_ticks.end());
    const auto at = [&](double quantile) {
        const auto last = static_cast<double>(samples_ticks.size() - 1);
        const auto rank = static_cast<std::size_t>(quantile * last);
        return ticks_to_ns(static_cast<double>(samples_ticks[rank]), ticks_per_second);
    };
    long double total = 0.0L;
    for (const auto sample : samples_ticks) {
        total += static_cast<long double>(sample);
    }
    summary.count = samples_ticks.size();
    summary.min_ns = ticks_to_ns(static_cast<double>(samples_ticks.front()), ticks_per_second);
    summary.p50_ns = at(0.50);
    summary.p90_ns = at(0.90);
    summary.p99_ns = at(0.99);
    summary.p999_ns = at(0.999);
    summary.max_ns = ticks_to_ns(static_cast<double>(samples_ticks.back()), ticks_per_second);
    summary.sampled_mean_ns =
        ticks_to_ns(static_cast<double>(total / static_cast<long double>(samples_ticks.size())), ticks_per_second);
    return summary;
}

} // namespace mdh::exchange::testing
