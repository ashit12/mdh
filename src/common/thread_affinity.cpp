#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "common/thread_affinity.hpp"

#include <cerrno>
#include <cstring>
#include <string>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace mdh {

std::optional<std::string> pin_calling_thread_to_cpu(unsigned cpu) {
#if defined(__linux__)
    if (cpu >= CPU_SETSIZE) {
        return "cpu " + std::to_string(cpu) + " is outside CPU_SETSIZE (" + std::to_string(CPU_SETSIZE) + ")";
    }
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(static_cast<int>(cpu), &cpuset);
    const int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        const int err = rc == -1 ? errno : rc;
        return std::string("pthread_setaffinity_np(cpu=") + std::to_string(cpu) + "): " + std::strerror(err);
    }
    return std::nullopt;
#else
    return "thread affinity requires Linux (requested cpu " + std::to_string(cpu) + ")";
#endif
}

std::uint64_t calling_thread_id() noexcept {
#if defined(__linux__)
    return static_cast<std::uint64_t>(syscall(SYS_gettid));
#else
    return 0;
#endif
}

} // namespace mdh
