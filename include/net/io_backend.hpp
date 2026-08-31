#pragma once

// Compile-time I/O backend: exactly one of these is defined, and CMake
// compiles exactly one matching .cpp. A Linux build cannot see kqueue;
// a macOS/BSD build cannot see epoll. There is no runtime switch.

#if defined(__linux__)
#define MDH_IO_EPOLL 1
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
#define MDH_IO_KQUEUE 1
#else
#error "mdh requires epoll (Linux) or kqueue (macOS/BSD)"
#endif

namespace mdh::net {

enum class IoBackend {
    Epoll,
    Kqueue,
};

#if defined(MDH_IO_EPOLL)
inline constexpr IoBackend kNativeIoBackend = IoBackend::Epoll;
#else
inline constexpr IoBackend kNativeIoBackend = IoBackend::Kqueue;
#endif

} // namespace mdh::net
