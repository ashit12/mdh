#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstring>
#include <thread>

#include "net/tcp_socket.hpp"

using namespace mdh::net;

TEST(TcpSocket, DefaultConstructedSocketIsOpen) {
    TcpSocket sock;
    EXPECT_TRUE(sock.is_open());
}

TEST(TcpSocket, ListenOnEphemeralPortReportsANonZeroPort) {
    TcpSocket listener;
    ASSERT_TRUE(listener.listen(0));
    auto port = listener.local_port();
    ASSERT_TRUE(port.has_value());
    EXPECT_NE(*port, 0);
}

TEST(TcpSocket, LoopbackConnectAcceptReadWriteRoundTrip) {
    TcpSocket listener;
    ASSERT_TRUE(listener.listen(0));
    const auto port = listener.local_port();
    ASSERT_TRUE(port.has_value());

    TcpSocket client;
    ASSERT_TRUE(client.connect("127.0.0.1", *port));

    auto accepted = listener.accept();
    ASSERT_EQ(accepted.status, IoStatus::Ok);
    ASSERT_TRUE(accepted.socket.has_value());

    const std::array<std::byte, 5> payload = {
        std::byte{'h'}, std::byte{'e'}, std::byte{'l'}, std::byte{'l'}, std::byte{'o'}};
    auto written = client.write(payload);
    ASSERT_TRUE(written.ok());
    EXPECT_EQ(written.n, payload.size());

    std::array<std::byte, 64> recv_buf{};
    auto received = accepted.socket->read(recv_buf);
    ASSERT_TRUE(received.ok());
    ASSERT_EQ(received.n, payload.size());
    EXPECT_TRUE(std::memcmp(recv_buf.data(), payload.data(), payload.size()) == 0);
}

TEST(TcpSocket, NonBlockingAcceptReturnsWouldBlockWhenNothingPending) {
    TcpSocket listener;
    ASSERT_TRUE(listener.listen(0));
    ASSERT_TRUE(listener.set_non_blocking());

    auto conn = listener.accept();
    EXPECT_EQ(conn.status, IoStatus::WouldBlock);
    EXPECT_FALSE(conn.socket.has_value());
}

TEST(TcpSocket, NonBlockingReadReturnsWouldBlockWhenEmpty) {
    TcpSocket listener;
    ASSERT_TRUE(listener.listen(0));
    ASSERT_TRUE(listener.set_non_blocking());
    const auto port = listener.local_port();
    ASSERT_TRUE(port.has_value());
    TcpSocket client;
    ASSERT_TRUE(client.connect("127.0.0.1", *port));
    AcceptResult accepted{};
    for (int i = 0; i < 100 && accepted.status != IoStatus::Ok; ++i) {
        accepted = listener.accept();
        if (accepted.status == IoStatus::WouldBlock) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    ASSERT_EQ(accepted.status, IoStatus::Ok);
    ASSERT_TRUE(accepted.socket->set_non_blocking());
    std::array<std::byte, 16> buf{};
    const auto n = accepted.socket->read(buf);
    EXPECT_EQ(n.status, IoStatus::WouldBlock);
    EXPECT_NE(n.status, IoStatus::Error);
}

TEST(TcpSocket, ConnectRejectsNonIpv4Literal) {
    TcpSocket client;
    EXPECT_FALSE(client.connect("not-an-ip", 12345));
}

TEST(TcpSocket, ConnectFailsWhenNothingIsListening) {
    std::uint16_t port = 0;
    {
        TcpSocket probe;
        ASSERT_TRUE(probe.listen(0));
        auto p = probe.local_port();
        ASSERT_TRUE(p.has_value());
        port = *p;
    }

    TcpSocket client;
    EXPECT_FALSE(client.connect("127.0.0.1", port));
}

TEST(TcpSocket, ShutdownUnblocksBlockedRead) {
    TcpSocket listener;
    ASSERT_TRUE(listener.listen(0));
    const auto port = listener.local_port();
    ASSERT_TRUE(port.has_value());

    TcpSocket client;
    ASSERT_TRUE(client.connect("127.0.0.1", *port));
    auto accepted = listener.accept();
    ASSERT_EQ(accepted.status, IoStatus::Ok);
    ASSERT_TRUE(accepted.socket.has_value());

    IoResult read_result{};
    std::jthread reader([&] {
        std::array<std::byte, 64> buf{};
        read_result = accepted.socket->read(buf);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    accepted.socket->shutdown();
    reader.join();

    if (read_result.ok()) {
        EXPECT_EQ(read_result.n, std::size_t{0});
    }
}

TEST(TcpSocket, AcceptedConnectionIsBlockingRegardlessOfListenersMode) {
    TcpSocket listener;
    ASSERT_TRUE(listener.listen(0));
    ASSERT_TRUE(listener.set_non_blocking());
    const auto port = listener.local_port();
    ASSERT_TRUE(port.has_value());

    TcpSocket client;
    ASSERT_TRUE(client.connect("127.0.0.1", *port));

    AcceptResult accepted{};
    for (int attempt = 0; attempt < 100 && accepted.status != IoStatus::Ok; ++attempt) {
        accepted = listener.accept();
        if (accepted.status == IoStatus::WouldBlock) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    ASSERT_EQ(accepted.status, IoStatus::Ok);

    IoResult read_result{};
    std::jthread reader([&] {
        std::array<std::byte, 64> buf{};
        read_result = accepted.socket->read(buf);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const std::array<std::byte, 3> payload = {std::byte{'h'}, std::byte{'i'}, std::byte{'!'}};
    ASSERT_TRUE(client.write(payload).ok());
    reader.join();

    ASSERT_TRUE(read_result.ok());
    EXPECT_EQ(read_result.n, payload.size());
}

TEST(TcpSocket, MoveTransfersOwnership) {
    TcpSocket a;
    ASSERT_TRUE(a.listen(0));
    const auto port = a.local_port();

    TcpSocket b(std::move(a));
    EXPECT_TRUE(b.is_open());
    EXPECT_EQ(b.local_port(), port);
}
