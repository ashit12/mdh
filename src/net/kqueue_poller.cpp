#include "net/io_poller.hpp"

// Mirror of the guard in epoll_poller.cpp: CMake gates this file to macOS/BSD,
// and the guard keeps a Linux checkout's clangd from erroring on <sys/event.h>.
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) ||                        \
    defined(__DragonFly__)

#include <sys/event.h>
#include <unistd.h>

#include <array>
#include <algorithm>

namespace mdh::net {
namespace {

constexpr uintptr_t kWakeIdent = 1;

[[nodiscard]] bool apply_filters(int kq, int fd, IoInterest previous, IoInterest next, void* user) {
    std::array<struct kevent, 4> changes{};
    int n = 0;

    const bool had_read = has_interest(previous, IoInterest::Read);
    const bool want_read = has_interest(next, IoInterest::Read);
    const bool had_write = has_interest(previous, IoInterest::Write);
    const bool want_write = has_interest(next, IoInterest::Write);

    if (want_read && !had_read) {
        EV_SET(&changes[static_cast<std::size_t>(n++)], static_cast<uintptr_t>(fd), EVFILT_READ, EV_ADD, 0, 0, user);
    } else if (!want_read && had_read) {
        EV_SET(&changes[static_cast<std::size_t>(n++)], static_cast<uintptr_t>(fd), EVFILT_READ, EV_DELETE, 0, 0,
               nullptr);
    } else if (want_read) {
        EV_SET(&changes[static_cast<std::size_t>(n++)], static_cast<uintptr_t>(fd), EVFILT_READ, EV_ADD, 0, 0, user);
    }

    if (want_write && !had_write) {
        EV_SET(&changes[static_cast<std::size_t>(n++)], static_cast<uintptr_t>(fd), EVFILT_WRITE, EV_ADD, 0, 0, user);
    } else if (!want_write && had_write) {
        EV_SET(&changes[static_cast<std::size_t>(n++)], static_cast<uintptr_t>(fd), EVFILT_WRITE, EV_DELETE, 0, 0,
               nullptr);
    } else if (want_write) {
        EV_SET(&changes[static_cast<std::size_t>(n++)], static_cast<uintptr_t>(fd), EVFILT_WRITE, EV_ADD, 0, 0, user);
    }

    if (n == 0) {
        return true;
    }
    return ::kevent(kq, changes.data(), n, nullptr, 0, nullptr) >= 0;
}

} // namespace

IoPoller::IoPoller() : fd_(::kqueue()) {
    if (fd_ < 0) {
        return;
    }
    struct kevent ev {};
    EV_SET(&ev, kWakeIdent, EVFILT_USER, EV_ADD | EV_CLEAR, NOTE_FFNOP, 0, nullptr);
    if (::kevent(fd_, &ev, 1, nullptr, 0, nullptr) < 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

IoPoller::~IoPoller() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

bool IoPoller::add(int fd, IoInterest interest, void* user) {
    if (fd_ < 0 || fd < 0) {
        return false;
    }
    if (!apply_filters(fd_, fd, IoInterest::None, interest, user)) {
        return false;
    }
    interest_[fd] = interest;
    return true;
}

bool IoPoller::mod(int fd, IoInterest interest, void* user) {
    if (fd_ < 0 || fd < 0) {
        return false;
    }
    const IoInterest previous = interest_.contains(fd) ? interest_[fd] : IoInterest::None;
    if (!apply_filters(fd_, fd, previous, interest, user)) {
        return false;
    }
    if (interest == IoInterest::None) {
        interest_.erase(fd);
    } else {
        interest_[fd] = interest;
    }
    return true;
}

bool IoPoller::remove(int fd) {
    if (fd_ < 0 || fd < 0) {
        return false;
    }
    std::array<struct kevent, 2> changes{};
    EV_SET(&changes[0], static_cast<uintptr_t>(fd), EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    EV_SET(&changes[1], static_cast<uintptr_t>(fd), EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    (void)::kevent(fd_, changes.data(), 2, nullptr, 0, nullptr); // ENOENT is fine if a filter was never added
    interest_.erase(fd);
    return true;
}

void IoPoller::wake() {
    if (fd_ < 0) {
        return;
    }
    struct kevent ev {};
    EV_SET(&ev, kWakeIdent, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
    (void)::kevent(fd_, &ev, 1, nullptr, 0, nullptr);
}

std::size_t IoPoller::wait(std::span<IoEvent> out, std::optional<std::chrono::milliseconds> timeout) {
    if (fd_ < 0 || out.empty()) {
        return 0;
    }

    struct timespec ts {};
    const struct timespec* tsp = nullptr;
    if (timeout.has_value()) {
        ts.tv_sec = timeout->count() / 1000;
        ts.tv_nsec = (timeout->count() % 1000) * 1'000'000L;
        tsp = &ts;
    }

    std::array<struct kevent, 64> evs{};
    const int max_events = static_cast<int>(std::min(out.size(), evs.size()));
    const int n = ::kevent(fd_, nullptr, 0, evs.data(), max_events, tsp);
    if (n <= 0) {
        return 0;
    }

    std::size_t written = 0;
    for (int i = 0; i < n && written < out.size(); ++i) {
        const struct kevent& ev = evs[static_cast<std::size_t>(i)];
        IoEvent& dst = out[written++];
        dst = {};
        if (ev.filter == EVFILT_USER) {
            dst.wake = true;
            continue;
        }
        dst.fd = static_cast<int>(ev.ident);
        dst.user = ev.udata;
        dst.error = (ev.flags & EV_ERROR) != 0;
        dst.hangup = (ev.flags & EV_EOF) != 0;
        if (ev.filter == EVFILT_READ) {
            dst.readable = true;
        } else if (ev.filter == EVFILT_WRITE) {
            dst.writable = true;
        }
    }
    return written;
}

} // namespace mdh::net

#endif // kqueue platforms
