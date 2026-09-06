#include "udp_ingestion/protocol_v1.hpp"

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

constexpr std::size_t kChannelCount = 2;

class UdpSocket {
public:
    explicit UdpSocket(std::uint16_t port) : descriptor_(::socket(AF_INET, SOCK_DGRAM, 0)) {
        if (descriptor_ < 0) {
            throw std::runtime_error(std::string("socket: ") + std::strerror(errno));
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);
        if (::bind(descriptor_, reinterpret_cast<const sockaddr*>(&address),
                   sizeof(address)) != 0) {
            const std::string message = std::string("bind: ") + std::strerror(errno);
            ::close(descriptor_);
            descriptor_ = -1;
            throw std::runtime_error(message);
        }
    }

    ~UdpSocket() {
        if (descriptor_ >= 0) {
            ::close(descriptor_);
        }
    }
    UdpSocket(const UdpSocket&) = delete;
    UdpSocket& operator=(const UdpSocket&) = delete;
    int get_fd() const { return descriptor_; }

private:
    int descriptor_;
};

std::uint64_t parse_value(const char* text, const char* name) {
    std::size_t parsed = 0;
    const std::string value_text = text;
    unsigned long long value = 0;
    try {
        value = std::stoull(value_text, &parsed);
    } catch (const std::exception&) {
        throw std::invalid_argument(std::string(name) + " requires an integer");
    }
    if (parsed != value_text.size()) {
        throw std::invalid_argument(std::string(name) + " requires an integer");
    }
    return static_cast<std::uint64_t>(value);
}

void run(std::uint16_t base_port, std::uint64_t expected_per_channel) {
    std::array<UdpSocket, kChannelCount> sockets{
        UdpSocket(base_port), UdpSocket(static_cast<std::uint16_t>(base_port + 1))};
    std::array<pollfd, kChannelCount> poll_descriptors{{
        {sockets[0].get_fd(), POLLIN, 0},
        {sockets[1].get_fd(), POLLIN, 0},
    }};
    std::array<std::uint64_t, kChannelCount> received{};
    std::array<std::uint64_t, kChannelCount> expected_sequence{1, 1};

    while (received[0] < expected_per_channel ||
           received[1] < expected_per_channel) {
        const int ready = ::poll(poll_descriptors.data(), poll_descriptors.size(), 3000);
        if (ready == 0) {
            throw std::runtime_error("timed out waiting for UDP packets");
        }
        if (ready < 0) {
            throw std::runtime_error(std::string("poll: ") + std::strerror(errno));
        }

        for (std::size_t channel = 0; channel < kChannelCount; ++channel) {
            if ((poll_descriptors[channel].revents & POLLIN) == 0 ||
                received[channel] >= expected_per_channel) {
                continue;
            }
            std::array<std::uint8_t, 128> buffer{};
            const auto size =
                ::recv(sockets[channel].get_fd(), buffer.data(), buffer.size(), 0);
            if (size != static_cast<ssize_t>(udp_ingestion::kProtocolV1WireSize)) {
                throw std::runtime_error("received payload size is not 34 bytes");
            }
            udp_ingestion::MarketDataMessage message{};
            if (!udp_ingestion::decode_protocol_v1(
                    buffer.data(), static_cast<std::size_t>(size), message)) {
                throw std::runtime_error("failed to decode Protocol v1 payload");
            }
            if (message.sequence != expected_sequence[channel]++) {
                throw std::runtime_error("channel sequence is not contiguous");
            }
            ++received[channel];
        }
    }
    std::cout << "validated " << received[0] << " packets on channel 0 and "
              << received[1] << " packets on channel 1\n";
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        if (argc != 3) {
            std::cerr << "Usage: " << argv[0]
                      << " BASE_PORT EXPECTED_PACKETS_PER_CHANNEL\n";
            return 1;
        }
        const auto port = parse_value(argv[1], "BASE_PORT");
        const auto count = parse_value(argv[2], "EXPECTED_PACKETS_PER_CHANNEL");
        if (port == 0 || port > 65534 || count == 0) {
            throw std::invalid_argument("port or packet count is out of range");
        }
        run(static_cast<std::uint16_t>(port), count);
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
