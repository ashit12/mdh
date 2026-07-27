#include <gtest/gtest.h>

#include <array>
#include <cstring>

#include "net/udp_socket.hpp"

using namespace mdh::net;

TEST(UdpSocket, DefaultConstructedSocketIsOpen) {
    UdpSocket sock;
    EXPECT_TRUE(sock.is_open());
}

TEST(UdpSocket, BindToEphemeralPortReportsANonZeroPort) {
    UdpSocket sock;
    ASSERT_TRUE(sock.bind(0)); // 0 = let the OS pick a free port
    auto port = sock.local_port();
    ASSERT_TRUE(port.has_value());
    EXPECT_NE(*port, 0);
}

TEST(UdpSocket, LoopbackSendAndReceiveRoundTrip) {
    UdpSocket receiver;
    ASSERT_TRUE(receiver.bind(0));
    const auto port = receiver.local_port();
    ASSERT_TRUE(port.has_value());

    UdpSocket sender;
    ASSERT_TRUE(sender.is_open());

    const std::array<std::byte, 5> payload = {
        std::byte{'h'}, std::byte{'e'}, std::byte{'l'}, std::byte{'l'}, std::byte{'o'}};
    ASSERT_TRUE(sender.send_to(payload, "127.0.0.1", *port));

    std::array<std::byte, 64> recv_buf{};
    auto received = receiver.receive(recv_buf);
    ASSERT_TRUE(received.has_value());
    ASSERT_EQ(*received, payload.size());
    EXPECT_TRUE(std::memcmp(recv_buf.data(), payload.data(), payload.size()) == 0);
}

TEST(UdpSocket, NonBlockingReceiveReturnsNulloptWhenNothingPending) {
    UdpSocket receiver;
    ASSERT_TRUE(receiver.bind(0));
    receiver.set_non_blocking();

    std::array<std::byte, 64> recv_buf{};
    auto received = receiver.receive(recv_buf); // nothing was ever sent
    EXPECT_FALSE(received.has_value());
}

TEST(UdpSocket, SendToRejectsNonIpv4Literal) {
    UdpSocket sender;
    const std::array<std::byte, 1> payload = {std::byte{1}};
    EXPECT_FALSE(sender.send_to(payload, "not-an-ip", 12345));
}

TEST(UdpSocket, MoveTransfersOwnership) {
    UdpSocket a;
    ASSERT_TRUE(a.bind(0));
    const auto port = a.local_port();

    UdpSocket b(std::move(a));
    EXPECT_TRUE(b.is_open());
    EXPECT_EQ(b.local_port(), port);
    // a is in a moved-from state; is_open() on it is not exercised here to
    // avoid depending on unspecified moved-from behaviour beyond "safe to
    // destroy", which the destructor's fd_ >= 0 check guarantees.
}
