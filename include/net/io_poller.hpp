#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <span>
#include <unordered_map>

#include "net/io_backend.hpp"

namespace mdh::net {

enum class IoInterest : std::uint32_t {
    None = 0,
    Read = 1u << 0,
    Write = 1u << 1,
};

[[nodiscard]] constexpr IoInterest operator|(IoInterest a, IoInterest b) {
    return static_cast<IoInterest>(static_cast<std::uint32_t>(a) | static_cast<std::uint32_t>(b));
}

[[nodiscard]] constexpr IoInterest operator&(IoInterest a, IoInterest b) {
    return static_cast<IoInterest>(static_cast<std::uint32_t>(a) & static_cast<std::uint32_t>(b));
}

[[nodiscard]] constexpr bool has_interest(IoInterest mask, IoInterest bit) {
    return (mask & bit) != IoInterest::None;
}

struct IoEvent {
    int fd = -1;
    void* user = nullptr;
    bool readable = false;
    bool writable = false;
    bool hangup = false;
    bool error = false;
    bool wake = false;
};

// Level-triggered readiness wait. Linux builds this as epoll; macOS/BSD as
// kqueue. Matching-thread wake() is safe to call concurrently with wait().
class IoPoller {
public:
    IoPoller();
    ~IoPoller();

    IoPoller(const IoPoller&) = delete;
    IoPoller& operator=(const IoPoller&) = delete;
    IoPoller(IoPoller&&) = delete;
    IoPoller& operator=(IoPoller&&) = delete;

    [[nodiscard]] bool is_open() const { return fd_ >= 0; }

    [[nodiscard]] static IoBackend backend() { return kNativeIoBackend; }

    [[nodiscard]] bool add(int fd, IoInterest interest, void* user);
    [[nodiscard]] bool mod(int fd, IoInterest interest, void* user);
    bool remove(int fd);

    // Thread-safe. Unblocks wait() even when no socket is ready.
    void wake();

    // Blocks until at least one event, timeout, or wake(). timeout ==
    // nullopt waits indefinitely. Returns the number of events written to
    // `out` (0 on timeout, EINTR, or a wait error).
    std::size_t wait(std::span<IoEvent> out, std::optional<std::chrono::milliseconds> timeout);

private:
    int fd_ = -1;
    std::unordered_map<int, IoInterest> interest_;
#if defined(MDH_IO_EPOLL)
    int wake_fd_ = -1;
#endif
};

} // namespace mdh::net
