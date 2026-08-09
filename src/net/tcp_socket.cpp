#include "net/tcp_socket.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <utility>

namespace mdh::net {

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

    // Lets a test or restarted process immediately rebind a port still in
    // TIME_WAIT from a just-closed prior socket -- see this method's header
    // doc comment. Not fatal if it fails; bind()/listen() below are the
    // operations that actually matter.
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

std::optional<TcpSocket> TcpSocket::accept() {
    if (!is_open()) {
        return std::nullopt;
    }
    const auto val = ::accept(fd_, nullptr, nullptr);
    if (val < 0) {
        return std::nullopt;
    }
    return TcpSocket(val);
}

bool TcpSocket::connect(const std::string& host, std::uint16_t port) {
    if (!is_open()) {
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        return false; // host must be an IPv4 dotted-decimal literal, e.g. "127.0.0.1"
    }

    const auto val = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (val < 0) {
        return false;
    }
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

std::optional<std::size_t> TcpSocket::read(std::span<std::byte> buf) {
    if (!is_open()) {
        return std::nullopt;
    }
    const auto val = ::read(fd_, buf.data(), buf.size());
    if (val < 0) {
        return std::nullopt;
    }
    return val;
}

std::optional<std::size_t> TcpSocket::write(std::span<const std::byte> data) {
    if (!is_open()) {
        return std::nullopt;
    }
    const auto val = ::write(fd_, data.data(), data.size());
    if (val < 0) {
        return std::nullopt;
    }
    return val;
}

void TcpSocket::set_non_blocking() {
    if (!is_open()) {
        return;
    }
    const int flags = ::fcntl(fd_, F_GETFL, 0);
    if (flags >= 0) {
        ::fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
    }
}

void TcpSocket::shutdown() {
    if (!is_open()) {
        return;
    }
    ::shutdown(fd_, SHUT_RDWR);
}

} // namespace mdh::net
