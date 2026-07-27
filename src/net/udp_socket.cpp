#include "net/udp_socket.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <utility>

namespace mdh::net {

UdpSocket::UdpSocket() : fd_(::socket(AF_INET, SOCK_DGRAM, 0)) {}

UdpSocket::~UdpSocket() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

UdpSocket::UdpSocket(UdpSocket&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

UdpSocket& UdpSocket::operator=(UdpSocket&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
}

bool UdpSocket::bind(std::uint16_t port) {
    if (!is_open()) {
        return false;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    return ::bind(fd_, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) == 0;
}

std::optional<std::uint16_t> UdpSocket::local_port() const {
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

bool UdpSocket::send_to(std::span<const std::byte> data, const std::string& host, std::uint16_t port) {
    if (!is_open()) {
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        return false; // host must be an IPv4 dotted-decimal literal, e.g. "127.0.0.1"
    }

    const auto sent = ::sendto(fd_, data.data(), data.size(), 0, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
    return sent == static_cast<ssize_t>(data.size());
}

std::optional<std::size_t> UdpSocket::receive(std::span<std::byte> buf) {
    if (!is_open()) {
        return std::nullopt;
    }
    const auto received = ::recvfrom(fd_, buf.data(), buf.size(), 0, nullptr, nullptr);
    if (received < 0) {
        return std::nullopt; // includes EWOULDBLOCK/EAGAIN in non-blocking mode
    }
    return static_cast<std::size_t>(received);
}

void UdpSocket::set_non_blocking() {
    if (!is_open()) {
        return;
    }
    const int flags = ::fcntl(fd_, F_GETFL, 0);
    if (flags >= 0) {
        ::fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
    }
}

} // namespace mdh::net
