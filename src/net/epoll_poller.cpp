#include "net/io_poller.hpp"

// CMake only adds this file to the build on Linux, so the guard below is never
// what selects the backend. It exists so that clangd -- which has no compile
// command for this file on a macOS checkout and therefore falls back to guessed
// flags -- parses it as an empty translation unit instead of reporting every
// epoll symbol as undeclared.
#if defined(__linux__)

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <algorithm>
#include <cerrno>
#include <cstdint>

namespace mdh::net {
namespace {

[[nodiscard]] uint32_t to_epoll_events(IoInterest interest) {
    uint32_t events = 0;
    if (has_interest(interest, IoInterest::Read)) {
        events |= EPOLLIN | EPOLLRDHUP;
    }
    if (has_interest(interest, IoInterest::Write)) {
        events |= EPOLLOUT;
    }
    return events;
}

} // namespace

IoPoller::IoPoller() : fd_(::epoll_create1(EPOLL_CLOEXEC)) {
    if (fd_ < 0) {
        return;
    }
    wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (wake_fd_ < 0) {
        ::close(fd_);
        fd_ = -1;
        return;
    }
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.ptr = &wake_fd_;
    if (::epoll_ctl(fd_, EPOLL_CTL_ADD, wake_fd_, &ev) != 0) {
        ::close(wake_fd_);
        ::close(fd_);
        wake_fd_ = -1;
        fd_ = -1;
    }
}

IoPoller::~IoPoller() {
    if (wake_fd_ >= 0) {
        ::close(wake_fd_);
    }
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

bool IoPoller::add(int sock, IoInterest interest, void* user) {
    if (fd_ < 0 || sock < 0) {
        return false;
    }
    epoll_event ev{};
    ev.events = to_epoll_events(interest);
    ev.data.ptr = user;
    if (::epoll_ctl(fd_, EPOLL_CTL_ADD, sock, &ev) != 0) {
        return false;
    }
    interest_[sock] = interest;
    return true;
}

bool IoPoller::mod(int sock, IoInterest interest, void* user) {
    if (fd_ < 0 || sock < 0) {
        return false;
    }
    epoll_event ev{};
    ev.events = to_epoll_events(interest);
    ev.data.ptr = user;
    if (::epoll_ctl(fd_, EPOLL_CTL_MOD, sock, &ev) != 0) {
        return false;
    }
    interest_[sock] = interest;
    return true;
}

bool IoPoller::remove(int sock) {
    if (fd_ < 0 || sock < 0) {
        return false;
    }
    (void)::epoll_ctl(fd_, EPOLL_CTL_DEL, sock, nullptr);
    interest_.erase(sock);
    return true;
}

void IoPoller::wake() {
    if (wake_fd_ < 0) {
        return;
    }
    const std::uint64_t one = 1;
    (void)::write(wake_fd_, &one, sizeof(one));
}

std::size_t IoPoller::wait(std::span<IoEvent> out, std::optional<std::chrono::milliseconds> timeout) {
    if (fd_ < 0 || out.empty()) {
        return 0;
    }

    const int timeout_ms = timeout.has_value() ? static_cast<int>(timeout->count()) : -1;
    std::array<epoll_event, 64> evs{};
    const int max_events = static_cast<int>(std::min(out.size(), evs.size()));
    const int n = ::epoll_wait(fd_, evs.data(), max_events, timeout_ms);
    if (n <= 0) {
        return 0;
    }

    std::size_t written = 0;
    for (int i = 0; i < n && written < out.size(); ++i) {
        const epoll_event& ev = evs[static_cast<std::size_t>(i)];
        if (ev.data.ptr == &wake_fd_) {
            std::uint64_t ignored = 0;
            while (::read(wake_fd_, &ignored, sizeof(ignored)) > 0) {
            }
            IoEvent& dst = out[written++];
            dst = {};
            dst.wake = true;
            continue;
        }
        IoEvent& dst = out[written++];
        dst = {};
        dst.user = ev.data.ptr;
        dst.readable = (ev.events & (EPOLLIN | EPOLLRDHUP)) != 0;
        dst.writable = (ev.events & EPOLLOUT) != 0;
        dst.hangup = (ev.events & (EPOLLHUP | EPOLLRDHUP)) != 0;
        dst.error = (ev.events & EPOLLERR) != 0;
    }
    return written;
}

} // namespace mdh::net

#endif // defined(__linux__)
