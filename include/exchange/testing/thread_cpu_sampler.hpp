#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <map>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/thread_affinity.hpp"

// Per-thread CPU accounting for a benchmark, sampled from inside the process
// under test.
//
// A latency regression that appears when a thread is added has two very
// different explanations -- the new thread costs the old ones work, or the
// machine simply ran out of cores -- and only per-thread CPU tells them
// apart. External tools can say the same thing (`top -H` on Linux, `ps -M`
// or Instruments on macOS), but sampling in-process means the numbers land
// in the same output file as the latencies they explain, over exactly the
// measured window rather than whatever window the operator managed to catch.
//
// Threads are identified by the name they gave themselves via
// mdh::set_calling_thread_name(); anything unnamed is aggregated into a
// single row, since an anonymous thread's CPU is not attributable anyway.
//
// Sampling is delta-based: CPU time is read per thread per tick and
// differenced, so a thread that starts or exits mid-window contributes only
// the time it actually ran.
namespace mdh::exchange::testing {

struct ThreadCpuSample {
    std::string name;
    std::uint64_t cpu_ns = 0;        // user + system CPU consumed during the window
    double mean_core_fraction = 0.0; // cpu_ns / window; 1.0 means one core fully saturated
    double peak_core_fraction = 0.0; // worst single interval, which a mean over the whole window hides
    std::uint64_t distinct_threads = 0;
    std::uint64_t runnable_observations = 0; // ticks where the kernel called this thread running or runnable
    std::uint64_t observations = 0;
};

struct ThreadCpuReport {
    bool supported = false;
    std::string unsupported_reason;
    std::uint64_t window_ns = 0;
    std::uint64_t ticks = 0;
    std::vector<ThreadCpuSample> threads; // descending by cpu_ns
    std::uint64_t peak_runnable_threads = 0;
    double mean_runnable_threads = 0.0;
};

} // namespace mdh::exchange::testing

#if defined(__APPLE__)

#include <mach/mach.h>
#include <mach/mach_init.h>
#include <mach/task.h>
#include <mach/thread_act.h>
#include <mach/thread_info.h>

namespace mdh::exchange::testing {

namespace detail {

struct ThreadTickReading {
    std::uint64_t id = 0;
    std::string name;
    std::uint64_t cpu_ns = 0;
    bool runnable = false;
};

// One pass over every thread in this task. Returns false only if the kernel
// refused to enumerate them at all, which is the one failure worth reporting
// as unsupported rather than skipping.
inline bool read_thread_tick(std::vector<ThreadTickReading>& out, std::string& error) {
    thread_act_array_t threads = nullptr;
    mach_msg_type_number_t count = 0;
    if (task_threads(mach_task_self(), &threads, &count) != KERN_SUCCESS) {
        error = "task_threads() failed";
        return false;
    }

    out.clear();
    for (mach_msg_type_number_t i = 0; i < count; ++i) {
        const thread_act_t thread = threads[i];

        thread_identifier_info_data_t identity{};
        mach_msg_type_number_t identity_count = THREAD_IDENTIFIER_INFO_COUNT;
        thread_basic_info_data_t basic{};
        mach_msg_type_number_t basic_count = THREAD_BASIC_INFO_COUNT;
        const bool have_identity = thread_info(thread, THREAD_IDENTIFIER_INFO,
                                               reinterpret_cast<thread_info_t>(&identity),
                                               &identity_count) == KERN_SUCCESS;
        const bool have_basic = thread_info(thread, THREAD_BASIC_INFO, reinterpret_cast<thread_info_t>(&basic),
                                            &basic_count) == KERN_SUCCESS;
        if (have_identity && have_basic) {
            thread_extended_info_data_t extended{};
            mach_msg_type_number_t extended_count = THREAD_EXTENDED_INFO_COUNT;
            std::string name;
            if (thread_info(thread, THREAD_EXTENDED_INFO, reinterpret_cast<thread_info_t>(&extended),
                            &extended_count) == KERN_SUCCESS) {
                name.assign(extended.pth_name, // fixed-size field, not guaranteed terminated
                            static_cast<std::size_t>(
                                std::find(std::begin(extended.pth_name), std::end(extended.pth_name), '\0') -
                                std::begin(extended.pth_name)));
            }

            const auto to_ns = [](const time_value_t& value) {
                return static_cast<std::uint64_t>(value.seconds) * 1'000'000'000ULL +
                       static_cast<std::uint64_t>(value.microseconds) * 1'000ULL;
            };
            out.push_back(ThreadTickReading{
                .id = identity.thread_id,
                .name = std::move(name),
                .cpu_ns = to_ns(basic.user_time) + to_ns(basic.system_time),
                // TH_STATE_RUNNING covers both on-core and waiting-for-a-core,
                // which is exactly the "runnable" population to compare against
                // the core count.
                .runnable = basic.run_state == TH_STATE_RUNNING,
            });
        }
        mach_port_deallocate(mach_task_self(), thread);
    }
    vm_deallocate(mach_task_self(), reinterpret_cast<vm_address_t>(threads),
                  static_cast<vm_size_t>(count) * sizeof(thread_act_t));
    return true;
}

inline constexpr bool kThreadCpuSupported = true;
inline constexpr const char* kThreadCpuUnsupportedReason = "";

} // namespace detail
} // namespace mdh::exchange::testing

#elif defined(__linux__)

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <unistd.h>

namespace mdh::exchange::testing {
namespace detail {

struct ThreadTickReading {
    std::uint64_t id = 0;
    std::string name;
    std::uint64_t cpu_ns = 0;
    bool runnable = false;
};

inline bool read_thread_tick(std::vector<ThreadTickReading>& out, std::string& error) {
    DIR* dir = ::opendir("/proc/self/task");
    if (dir == nullptr) {
        error = "opendir(/proc/self/task) failed";
        return false;
    }
    const auto ticks_per_second = static_cast<std::uint64_t>(::sysconf(_SC_CLK_TCK));
    const std::uint64_t ns_per_tick = ticks_per_second == 0 ? 0 : 1'000'000'000ULL / ticks_per_second;

    out.clear();
    while (const dirent* entry = ::readdir(dir)) {
        if (entry->d_name[0] == '.') {
            continue;
        }
        const std::string stat_path = std::string("/proc/self/task/") + entry->d_name + "/stat";
        std::FILE* file = std::fopen(stat_path.c_str(), "re");
        if (file == nullptr) {
            continue; // thread exited between readdir and open
        }
        char line[4096];
        const bool read_ok = std::fgets(line, sizeof(line), file) != nullptr;
        std::fclose(file);
        if (!read_ok) {
            continue;
        }

        // comm sits in parentheses and may itself contain spaces, so parse
        // fields after the final ')' rather than by counting from the start.
        char* comm_open = std::strchr(line, '(');
        char* comm_close = std::strrchr(line, ')');
        if (comm_open == nullptr || comm_close == nullptr || comm_close < comm_open) {
            continue;
        }
        std::string name(comm_open + 1, static_cast<std::size_t>(comm_close - comm_open - 1));
        char state = 0;
        unsigned long utime = 0;
        unsigned long stime = 0;
        // Fields after comm: state ppid pgrp session tty tpgid flags minflt
        // cminflt majflt cmajflt utime stime ...
        if (std::sscanf(comm_close + 2, "%c %*d %*d %*d %*d %*d %*u %*lu %*lu %*lu %*lu %lu %lu", &state, &utime,
                        &stime) != 3) {
            continue;
        }
        out.push_back(ThreadTickReading{
            .id = std::strtoull(entry->d_name, nullptr, 10),
            .name = std::move(name),
            .cpu_ns = (static_cast<std::uint64_t>(utime) + stime) * ns_per_tick,
            .runnable = state == 'R',
        });
    }
    ::closedir(dir);
    return true;
}

inline constexpr bool kThreadCpuSupported = true;
inline constexpr const char* kThreadCpuUnsupportedReason = "";

} // namespace detail
} // namespace mdh::exchange::testing

#else

namespace mdh::exchange::testing {
namespace detail {

struct ThreadTickReading {
    std::uint64_t id = 0;
    std::string name;
    std::uint64_t cpu_ns = 0;
    bool runnable = false;
};

inline bool read_thread_tick(std::vector<ThreadTickReading>&, std::string& error) {
    error = "per-thread CPU sampling requires macOS (mach thread_info) or Linux (/proc/self/task)";
    return false;
}

inline constexpr bool kThreadCpuSupported = false;
inline constexpr const char* kThreadCpuUnsupportedReason =
    "per-thread CPU sampling requires macOS (mach thread_info) or Linux (/proc/self/task)";

} // namespace detail
} // namespace mdh::exchange::testing

#endif

namespace mdh::exchange::testing {

class ThreadCpuSampler {
public:
    explicit ThreadCpuSampler(std::chrono::milliseconds interval) : interval_(interval) {}

    ~ThreadCpuSampler() { stop(); }

    ThreadCpuSampler(const ThreadCpuSampler&) = delete;
    ThreadCpuSampler& operator=(const ThreadCpuSampler&) = delete;
    ThreadCpuSampler(ThreadCpuSampler&&) = delete;
    ThreadCpuSampler& operator=(ThreadCpuSampler&&) = delete;

    void start() {
        if (sampler_thread_.joinable()) {
            return;
        }
        if (!detail::kThreadCpuSupported) {
            report_.unsupported_reason = detail::kThreadCpuUnsupportedReason;
            return;
        }
        running_.store(true, std::memory_order_release);
        sampler_thread_ = std::thread([this] { sample_loop(); });
    }

    // Idempotent, so the destructor can call it after the caller already has.
    ThreadCpuReport stop() {
        running_.store(false, std::memory_order_release);
        if (sampler_thread_.joinable()) {
            sampler_thread_.join();
        }
        return report_;
    }

private:
    struct Accumulator {
        std::uint64_t cpu_ns = 0;
        double peak_core_fraction = 0.0;
        std::uint64_t runnable_observations = 0;
        std::uint64_t observations = 0;
        std::vector<std::uint64_t> ids;
    };

    void sample_loop() {
        set_calling_thread_name("mdh-cpu-sampler");

        std::vector<detail::ThreadTickReading> readings;
        std::unordered_map<std::uint64_t, std::uint64_t> previous_cpu_ns;
        std::map<std::string, Accumulator> by_name;
        std::string error;

        const auto window_begin = std::chrono::steady_clock::now();
        auto previous_tick = window_begin;
        // Prime the deltas: the first pass only establishes each thread's
        // baseline CPU time, since a thread's total since process start says
        // nothing about the window being measured.
        if (!detail::read_thread_tick(readings, error)) {
            report_.unsupported_reason = error;
            return;
        }
        for (const auto& reading : readings) {
            previous_cpu_ns[reading.id] = reading.cpu_ns;
        }

        std::uint64_t ticks = 0;
        std::uint64_t runnable_total = 0;
        std::uint64_t peak_runnable = 0;

        while (running_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(interval_);
            if (!detail::read_thread_tick(readings, error)) {
                break;
            }

            const auto now = std::chrono::steady_clock::now();
            const auto interval_ns =
                static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now - previous_tick).count());
            previous_tick = now;
            ++ticks;

            std::uint64_t runnable_this_tick = 0;
            for (const auto& reading : readings) {
                const auto previous = previous_cpu_ns.find(reading.id);
                const std::uint64_t delta =
                    previous == previous_cpu_ns.end() || reading.cpu_ns < previous->second
                        ? 0 // first sighting, or a counter that went backwards: no window to attribute
                        : reading.cpu_ns - previous->second;
                previous_cpu_ns[reading.id] = reading.cpu_ns;

                auto& accumulator = by_name[reading.name.empty() ? std::string("(unnamed)") : reading.name];
                accumulator.cpu_ns += delta;
                accumulator.observations += 1;
                accumulator.runnable_observations += reading.runnable ? 1 : 0;
                if (interval_ns > 0) {
                    accumulator.peak_core_fraction = std::max(
                        accumulator.peak_core_fraction, static_cast<double>(delta) / static_cast<double>(interval_ns));
                }
                if (std::find(accumulator.ids.begin(), accumulator.ids.end(), reading.id) == accumulator.ids.end()) {
                    accumulator.ids.push_back(reading.id);
                }
                runnable_this_tick += reading.runnable ? 1 : 0;
            }
            runnable_total += runnable_this_tick;
            peak_runnable = std::max(peak_runnable, runnable_this_tick);
        }

        const auto window_ns = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - window_begin)
                .count());

        report_.supported = true;
        report_.window_ns = window_ns;
        report_.ticks = ticks;
        report_.peak_runnable_threads = peak_runnable;
        report_.mean_runnable_threads = ticks == 0 ? 0.0 : static_cast<double>(runnable_total) / static_cast<double>(ticks);
        for (auto& [name, accumulator] : by_name) {
            report_.threads.push_back(ThreadCpuSample{
                .name = name,
                .cpu_ns = accumulator.cpu_ns,
                .mean_core_fraction =
                    window_ns == 0 ? 0.0 : static_cast<double>(accumulator.cpu_ns) / static_cast<double>(window_ns),
                .peak_core_fraction = accumulator.peak_core_fraction,
                .distinct_threads = accumulator.ids.size(),
                .runnable_observations = accumulator.runnable_observations,
                .observations = accumulator.observations,
            });
        }
        std::sort(report_.threads.begin(), report_.threads.end(),
                  [](const ThreadCpuSample& left, const ThreadCpuSample& right) { return left.cpu_ns > right.cpu_ns; });
    }

    std::chrono::milliseconds interval_;
    std::atomic<bool> running_{false};
    std::thread sampler_thread_;
    ThreadCpuReport report_;
};

} // namespace mdh::exchange::testing
