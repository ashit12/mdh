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
    ASSERT_TRUE(listener.listen(0)); // 0 = let the OS pick a free port
    auto port = listener.local_port();
    ASSERT_TRUE(port.has_value());
    EXPECT_NE(*port, 0);
}

TEST(TcpSocket, LoopbackConnectAcceptReadWriteRoundTrip) {
    TcpSocket listener;
    ASSERT_TRUE(listener.listen(0));
    const auto port = listener.local_port();
    ASSERT_TRUE(port.has_value());

    // connect() completes the TCP handshake itself; the resulting
    // connection sits in the listen backlog until accept() is called, so
    // no separate thread is needed here -- accept() below returns
    // immediately rather than actually blocking.
    TcpSocket client;
    ASSERT_TRUE(client.connect("127.0.0.1", *port));

    auto server_conn = listener.accept();
    ASSERT_TRUE(server_conn.has_value());

    const std::array<std::byte, 5> payload = {
        std::byte{'h'}, std::byte{'e'}, std::byte{'l'}, std::byte{'l'}, std::byte{'o'}};
    auto written = client.write(payload);
    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(*written, payload.size());

    std::array<std::byte, 64> recv_buf{};
    auto received = server_conn->read(recv_buf);
    ASSERT_TRUE(received.has_value());
    ASSERT_EQ(*received, payload.size());
    EXPECT_TRUE(std::memcmp(recv_buf.data(), payload.data(), payload.size()) == 0);
}

TEST(TcpSocket, NonBlockingAcceptReturnsNulloptWhenNothingPending) {
    TcpSocket listener;
    ASSERT_TRUE(listener.listen(0));
    listener.set_non_blocking();

    auto conn = listener.accept(); // nobody ever connected
    EXPECT_FALSE(conn.has_value());
}

TEST(TcpSocket, ConnectRejectsNonIpv4Literal) {
    TcpSocket client;
    EXPECT_FALSE(client.connect("not-an-ip", 12345));
}

TEST(TcpSocket, ConnectFailsWhenNothingIsListening) {
    // Connecting to a port that is bound-but-not-listening can be slow to
    // fail on some platforms (no immediate RST). Using a port that was
    // *closed* -- listened on, then the socket destroyed -- gets a
    // consistent, immediate ECONNREFUSED instead.
    std::uint16_t port = 0;
    {
        TcpSocket probe;
        ASSERT_TRUE(probe.listen(0));
        auto p = probe.local_port();
        ASSERT_TRUE(p.has_value());
        port = *p;
    } // probe's destructor closes the fd here.

    TcpSocket client;
    EXPECT_FALSE(client.connect("127.0.0.1", port));
}

TEST(TcpSocket, ShutdownUnblocksBlockedRead) {
    // Unlike accept() on a listening socket (see tcp_socket.hpp's own
    // shutdown() doc comment on the macOS caveat), shutdown() on an
    // already-connected socket reliably unblocks a thread blocked in
    // read() on it, on every platform this project targets.
    TcpSocket listener;
    ASSERT_TRUE(listener.listen(0));
    const auto port = listener.local_port();
    ASSERT_TRUE(port.has_value());

    TcpSocket client;
    ASSERT_TRUE(client.connect("127.0.0.1", *port));
    auto server_conn = listener.accept();
    ASSERT_TRUE(server_conn.has_value());

    std::optional<std::size_t> read_result;
    std::jthread reader([&] {
        std::array<std::byte, 64> buf{};
        read_result = server_conn->read(buf); // blocks until data, EOF, or shutdown()
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50)); // let the reader actually reach read()
    server_conn->shutdown();
    reader.join();

    // Either outcome (EOF or an error) is an acceptable, unblocked result;
    // what this test actually pins down is that reader.join() above
    // returned at all instead of hanging.
    if (read_result.has_value()) {
        EXPECT_EQ(*read_result, 0);
    }
}

TEST(TcpSocket, MoveTransfersOwnership) {
    TcpSocket a;
    ASSERT_TRUE(a.listen(0));
    const auto port = a.local_port();

    TcpSocket b(std::move(a));
    EXPECT_TRUE(b.is_open());
    EXPECT_EQ(b.local_port(), port);
    // a is in a moved-from state; is_open() on it is not exercised here to
    // avoid depending on unspecified moved-from behaviour beyond "safe to
    // destroy", which the destructor's fd_ >= 0 check guarantees -- same
    // convention as UdpSocket's own MoveTransfersOwnership test.
}
