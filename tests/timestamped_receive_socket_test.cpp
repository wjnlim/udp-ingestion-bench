#include "udp_ingestion/timestamped_receive.hpp"

#include <arpa/inet.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <system_error>

namespace {

int failures = 0;

void expect(bool condition, const char* description) {
    if (!condition) {
        std::cerr << "FAIL: " << description << '\n';
        ++failures;
    }
}

void require_success(int result, const char* operation) {
    if (result < 0) {
        const int error = errno;
        throw std::system_error(
            error, std::generic_category(), operation);
    }
}

class SocketFixture {
public:
    explicit SocketFixture(bool timestamp_enabled = true) {
        fd_ = ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
        require_success(fd_, "test socket");

        try {
            if (timestamp_enabled) {
                udp_ingestion::enable_rx_timestamp(fd_);
            }

            address_.sin_family = AF_INET;
            address_.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            address_.sin_port = 0;

            require_success(
                ::bind(
                    fd_,
                    reinterpret_cast<const sockaddr*>(&address_),
                    sizeof(address_)),
                "test bind");

            socklen_t size = sizeof(address_);
            require_success(
                ::getsockname(
                    fd_, reinterpret_cast<sockaddr*>(&address_), &size),
                "test getsockname");
        } catch (...) {
            ::close(fd_);
            throw;
        }
    }

    ~SocketFixture() {
        ::close(fd_);
    }

    SocketFixture(const SocketFixture&) = delete;
    SocketFixture& operator=(const SocketFixture&) = delete;

    int get_fd() const {
        return fd_;
    }

    void send_and_wait(const std::uint8_t* data, std::size_t size) {
        const auto sent = ::sendto(
            fd_, data, size, 0,
            reinterpret_cast<const sockaddr*>(&address_),
            sizeof(address_));

        if (sent < 0) {
            const int error = errno;
            throw std::system_error(
                error, std::generic_category(), "test sendto");
        }
        if (sent != static_cast<ssize_t>(size)) {
            throw std::runtime_error("unexpected test send size");
        }

        pollfd readiness{};
        readiness.fd = fd_;
        readiness.events = POLLIN;

        const int ready = ::poll(&readiness, 1, 2000);
        require_success(ready, "test poll");

        if (ready == 0) {
            throw std::runtime_error("test receive readiness timeout");
        }
        if ((readiness.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0
            || (readiness.revents & POLLIN) == 0) {
            throw std::runtime_error("unexpected test socket readiness");
        }
    }

private:
    int fd_ = -1;
    sockaddr_in address_{};
};

void expect_empty_socket(int fd) {
    udp_ingestion::ReceivedDatagram output{};

    const auto received =
        udp_ingestion::receive_timestamped_datagram(fd, output);
    const int error = errno;

    expect(received == -1, "empty socket returns minus one");
    expect(error == EAGAIN || error == EWOULDBLOCK,
           "empty socket preserves EAGAIN or EWOULDBLOCK");
}

void test_payload_sizes() {
    SocketFixture fixture;

    std::array<std::uint8_t, 128> sent{};
    for (std::size_t i = 0; i < sent.size(); ++i) {
        sent[i] = static_cast<std::uint8_t>(i);
    }

    // Includes short input, exact protocol size, oversized input,
    // and a normal datagram after truncation.
    const std::array<std::size_t, 5> sizes{
        udp_ingestion::kProtocolV1BufSize,
        0,
        udp_ingestion::kProtocolV1BufSize - 1,
        sent.size(),
        udp_ingestion::kProtocolV1BufSize
    };

    expect_empty_socket(fixture.get_fd());

    udp_ingestion::ReceivedDatagram output{};

    for (const auto size : sizes) {
        fixture.send_and_wait(sent.data(), size);

        output.rx_timestamp_ns = -1;

        const auto received =
            udp_ingestion::receive_timestamped_datagram(
                fixture.get_fd(), output);
        const auto expected_size =
            std::min(size, output.payload.size());

        expect(received == static_cast<ssize_t>(expected_size),
               "receive returns the number of payload bytes copied");
        expect(output.rx_timestamp_ns > 0,
               "each received datagram has a kernel RX timestamp");

        if (received == static_cast<ssize_t>(expected_size)) {
            for (std::size_t i = 0; i < expected_size; ++i) {
                expect(output.payload[i] == sent[i],
                       "received payload bytes match");
            }
        }

        expect_empty_socket(fixture.get_fd());
    }
}

void test_missing_timestamp() {
    SocketFixture fixture(false);
    const std::uint8_t payload = 42;
    fixture.send_and_wait(&payload, sizeof(payload));

    udp_ingestion::ReceivedDatagram output{};
    bool rejected = false;

    try {
        static_cast<void>(
            udp_ingestion::receive_timestamped_datagram(
                fixture.get_fd(), output));
    } catch (const std::runtime_error&) {
        rejected = true;
    }

    expect(rejected,
           "received datagram without timestamp metadata is rejected");

    // The datagram was consumed even though metadata validation failed.
    expect_empty_socket(fixture.get_fd());
}

void test_invalid_descriptor() {
    udp_ingestion::ReceivedDatagram output{};

    const auto received =
        udp_ingestion::receive_timestamped_datagram(-1, output);
    const int error = errno;

    expect(received == -1, "invalid descriptor returns minus one");
    expect(error == EBADF, "recvmsg EBADF is preserved");

    bool rejected = false;

    try {
        udp_ingestion::enable_rx_timestamp(-1);
    } catch (const std::system_error& exception) {
        rejected = exception.code().value() == EBADF;
    }

    expect(rejected,
           "timestamp setup reports EBADF for an invalid descriptor");
}

} // namespace

int main() {
    try {
        test_payload_sizes();
        test_missing_timestamp();
        test_invalid_descriptor();
    } catch (const std::exception& error) {
        std::cerr << "test error: " << error.what() << '\n';
        return 1;
    }

    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return 1;
    }

    std::cout << "all timestamped receive socket tests passed\n";
    return 0;
}