#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include "net/io_poller.hpp"

using namespace mdh::net;
using namespace std::chrono_literals;

TEST(IoPoller, BackendMatchesThisPlatform) {
#if defined(__linux__)
    EXPECT_EQ(IoPoller::backend(), IoBackend::Epoll);
#else
    EXPECT_EQ(IoPoller::backend(), IoBackend::Kqueue);
#endif
}

TEST(IoPoller, WaitReportsReadableAfterAWrite) {
    int fds[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

    IoPoller poller;
    ASSERT_TRUE(poller.is_open());
    ASSERT_TRUE(poller.add(fds[0], IoInterest::Read, &fds[0]));

    const char byte = 'x';
    ASSERT_EQ(::write(fds[1], &byte, 1), 1);

    std::array<IoEvent, 8> events{};
    const std::size_t n = poller.wait(events, 500ms);
    ASSERT_GE(n, 1u);
    EXPECT_TRUE(events[0].readable);
    EXPECT_EQ(events[0].user, &fds[0]);

    (void)poller.remove(fds[0]);
    ::close(fds[0]);
    ::close(fds[1]);
}

TEST(IoPoller, WakeUnblocksWaitFromAnotherThread) {
    IoPoller poller;
    ASSERT_TRUE(poller.is_open());

    std::atomic<bool> woke{false};
    std::jthread waiter([&] {
        std::array<IoEvent, 8> events{};
        const std::size_t n = poller.wait(events, 2000ms);
        for (std::size_t i = 0; i < n; ++i) {
            if (events[i].wake) {
                woke.store(true, std::memory_order_release);
            }
        }
    });

    std::this_thread::sleep_for(20ms);
    poller.wake();
    waiter.join();
    EXPECT_TRUE(woke.load(std::memory_order_acquire));
}

TEST(IoPoller, RemoveStopsDeliveringEvents) {
    int fds[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

    IoPoller poller;
    ASSERT_TRUE(poller.add(fds[0], IoInterest::Read, nullptr));
    ASSERT_TRUE(poller.remove(fds[0]));

    const char byte = 'x';
    ASSERT_EQ(::write(fds[1], &byte, 1), 1);

    std::array<IoEvent, 8> events{};
    const std::size_t n = poller.wait(events, 50ms);
    for (std::size_t i = 0; i < n; ++i) {
        EXPECT_FALSE(events[i].readable && events[i].user == nullptr && !events[i].wake);
    }

    ::close(fds[0]);
    ::close(fds[1]);
}
