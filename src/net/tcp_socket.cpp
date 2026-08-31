#include "net/tcp_socket.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <utility>

namespace mdh::net {

namespace {
void configure_connected_socket(int fd) {
    const int flag = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
#if defined(SO_NOSIGPIPE)
    ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &flag, sizeof(flag));
#endif
}

[[nodiscard]] IoStatus status_from_errno(int err) {
    if (err == EAGAIN || err == EWOULDBLOCK) {
        return IoStatus::WouldBlock;
    }
    return IoStatus::Error;
}
} // namespace

TcpSocket::TcpSocket() : fd_(::socket(AF_INET, SOCK_STREAM, 0)) {}

TcpSocket::~TcpSocket() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

TcpSocket::TcpSocket(TcpSocket&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

TcpSocket& TcpSocket::operator=(TcpSocket&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
}

bool TcpSocket::listen(std::uint16_t port, int backlog) {
    if (!is_open()) {
        return false;
    }

    const int reuse = 1;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    if (::bind(fd_, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
        return false;
    }

    return ::listen(fd_, backlog) == 0;
}

AcceptResult TcpSocket::accept() {
    if (!is_open()) {
        return {.status = IoStatus::Error, .socket = std::nullopt};
    }
    for (;;) {
        const auto val = ::accept(fd_, nullptr, nullptr);
        if (val >= 0) {
            const int flags = ::fcntl(val, F_GETFL, 0);
            if (flags >= 0) {
                ::fcntl(val, F_SETFL, flags & ~O_NONBLOCK);
            }
            configure_connected_socket(val);
            return {.status = IoStatus::Ok, .socket = TcpSocket(val)};
        }
        if (errno == EINTR) {
            continue;
        }
        return {.status = status_from_errno(errno), .socket = std::nullopt};
    }
}

bool TcpSocket::connect(const std::string& host, std::uint16_t port) {
    if (!is_open()) {
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        return false;
    }

    const auto val = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (val < 0) {
        return false;
    }
    configure_connected_socket(fd_);
    return true;
}

std::optional<std::uint16_t> TcpSocket::local_port() const {
    if (!is_open()) {
        return std::nullopt;
    }

    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        return std::nullopt;
    }
    return ntohs(addr.sin_port);
}

IoResult TcpSocket::read(std::span<std::byte> buf) {
    if (!is_open()) {
        return {.status = IoStatus::Error};
    }
    for (;;) {
        const auto val = ::read(fd_, buf.data(), buf.size());
        if (val >= 0) {
            return {.status = IoStatus::Ok, .n = static_cast<std::size_t>(val)};
        }
        if (errno == EINTR) {
            continue;
        }
        return {.status = status_from_errno(errno)};
    }
}

IoResult TcpSocket::write(std::span<const std::byte> data) {
    if (!is_open()) {
        return {.status = IoStatus::Error};
    }
#if defined(MSG_NOSIGNAL)
    constexpr int flags = MSG_NOSIGNAL;
#else
    constexpr int flags = 0;
#endif
    for (;;) {
        const auto val = ::send(fd_, data.data(), data.size(), flags);
        if (val >= 0) {
            return {.status = IoStatus::Ok, .n = static_cast<std::size_t>(val)};
        }
        if (errno == EINTR) {
            continue;
        }
        return {.status = status_from_errno(errno)};
    }
}

bool TcpSocket::set_non_blocking() {
    if (!is_open()) {
        return false;
    }
    const int flags = ::fcntl(fd_, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return ::fcntl(fd_, F_SETFL, flags | O_NONBLOCK) == 0;
}

void TcpSocket::shutdown() {
    if (!is_open()) {
        return;
    }
    ::shutdown(fd_, SHUT_RDWR);
}

} // namespace mdh::net
