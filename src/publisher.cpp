#include "udp_ingestion/protocol_v1.hpp"
#include "udp_ingestion/synthetic_message.hpp"
#include "udp_ingestion/cpu_affinity.hpp"

#include <arpa/inet.h>
#include <sched.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <iomanip>

namespace {

constexpr std::size_t kChannelCount = 2;

struct PublisherConfig {
    std::string destination_address = "127.0.0.1";
    std::uint16_t base_port = 9000;
    std::uint64_t rate = 1'000;
    std::uint64_t count = 1'000;
    std::uint32_t instrument_count = 16;
    std::optional<int> cpu;
};

class UdpSocket {
public:
    UdpSocket() : descriptor_(::socket(AF_INET, SOCK_DGRAM, 0)) {
        if (descriptor_ < 0) {
            throw std::runtime_error(std::string("socket: ") + std::strerror(errno));
        }
    }

    ~UdpSocket() {
        ::close(descriptor_);
    }
    UdpSocket(const UdpSocket&) = delete;
    UdpSocket& operator=(const UdpSocket&) = delete;
    int get_fd() const {
        return descriptor_;
    }

private:
    int descriptor_;
};

std::uint64_t parse_unsigned(std::string_view val_str, const std::string& option) {
    std::uint64_t value = 0;
    const auto result = std::from_chars(val_str.data(), val_str.data() + val_str.size(), value);
    if (val_str.empty() || result.ec != std::errc{} ||
        result.ptr != val_str.data() + val_str.size()) {
        throw std::invalid_argument(option + " requires an unsigned decimal integer");
    }
    return value;
}

void print_usage(const char* program) {
    std::cout << "Usage: " << program
              << " [--address IPv4] [--base-port PORT] [--rate PPS]"
                 " [--count MESSAGES] [--instruments COUNT] [--cpu N]\n";
}

PublisherConfig parse_arguments(int argc, char* argv[]) {
    PublisherConfig config;
    for (int i = 1; i < argc; ++i) {
        const std::string option = argv[i];
        if (option == "--help") {
            print_usage(argv[0]);
            std::exit(0);
        }
        if (i + 1 >= argc) {
            throw std::invalid_argument(option + " requires a value");
        }
        const std::string_view value = argv[++i];
        if (option == "--address") {
            config.destination_address = std::string(value);
        } else if (option == "--base-port") {
            const auto port = parse_unsigned(value, option);
            if (port == 0 || port > 65534) {
                throw std::invalid_argument("--base-port must be in [1, 65534]");
            }
            config.base_port = static_cast<std::uint16_t>(port);
        } else if (option == "--rate") {
            config.rate = parse_unsigned(value, option);
            if (config.rate == 0) {
                throw std::invalid_argument("--rate must be positive");
            }
        } else if (option == "--count") {
            config.count = parse_unsigned(value, option);
            if (config.count == 0) {
                throw std::invalid_argument("--count must be positive");
            }
        } else if (option == "--instruments") {
            const auto instr = parse_unsigned(value, option);
            if (instr == 0 || instr > std::numeric_limits<std::uint32_t>::max()) {
                throw std::invalid_argument("--instruments is out of range");
            }
            config.instrument_count = static_cast<std::uint32_t>(instr);
        } else if (option == "--cpu") {
            const auto cpu = parse_unsigned(value, option);
            if (cpu >= CPU_SETSIZE) {
                throw std::invalid_argument("--cpu is outside the supported CPU set");
            }
            config.cpu = static_cast<int>(cpu);
        } else {
            throw std::invalid_argument("unknown option: " + option);
        }
    }
    return config;
}

std::array<sockaddr_in, kChannelCount> make_destinations(const PublisherConfig& config) {
    std::array<sockaddr_in, kChannelCount> destinations{};
    in_addr address{};
    if (::inet_pton(AF_INET, config.destination_address.c_str(), &address) != 1) {
        throw std::invalid_argument("--address must be a valid IPv4 address");
    }
    for (std::size_t channel = 0; channel < kChannelCount; ++channel) {
        destinations[channel].sin_family = AF_INET;
        destinations[channel].sin_addr = address;
        destinations[channel].sin_port =
            htons(static_cast<std::uint16_t>(config.base_port + channel));
    }
    return destinations;
}

void run_publisher(const PublisherConfig& config) {
    if (config.cpu.has_value()) {
        udp_ingestion::pin_current_thread(*config.cpu, "publisher");
    }

    UdpSocket socket;
    const auto destinations = make_destinations(config);
    std::array<std::uint64_t, kChannelCount> next_sequence{1, 1};
    const auto start = std::chrono::steady_clock::now();
    const auto period = std::chrono::duration<double>(1.0 / config.rate);

    for (std::uint64_t sent = 0; sent < config.count; ++sent) {
        const std::size_t channel = sent % kChannelCount;
        const auto message = udp_ingestion::make_synthetic_message(
            channel, next_sequence[channel]++, config.instrument_count);
        const auto pbuf = udp_ingestion::encode_protocol_v1(message);
        const auto& destination = destinations[channel];
        const auto result =
            ::sendto(socket.get_fd(), pbuf.data(), pbuf.size(), 0,
                     reinterpret_cast<const sockaddr*>(&destination), sizeof(destination));
        if (result < 0) {
            throw std::runtime_error(std::string("sendto: ") + std::strerror(errno));
        }
        if (static_cast<std::size_t>(result) != pbuf.size()) {
            throw std::runtime_error("sendto returned an unexpected byte count");
        }

        const auto deadline =
            start + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                        period * static_cast<double>(sent + 1));
        std::this_thread::sleep_until(deadline);
    }

    const auto finish = std::chrono::steady_clock::now();
    const auto elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
    if (elapsed_ns <= 0) {
        throw std::runtime_error("publisher elapsed time must be positive");
    }
    const auto achieved_pps =
        static_cast<double>(config.count) * 1'000'000'000.0 / static_cast<double>(elapsed_ns);

    std::cout << "sent " << config.count << " packets across " << kChannelCount
              << " channels at target aggregate rate " << config.rate << " pps\n";

    const auto previous_precision = std::cout.precision();
    std::cout << std::setprecision(10) << "publisher_rate_scope=full_paced_run\n"
              << "publisher_requested_pps=" << config.rate
              << "\npublisher_sent_packets=" << config.count
              << "\npublisher_elapsed_ns=" << elapsed_ns
              << "\npublisher_achieved_pps=" << achieved_pps << '\n';

    std::cout.precision(previous_precision);
}

} // namespace

int main(int argc, char* argv[]) {
    try {
        const auto config = parse_arguments(argc, argv);
        run_publisher(config);
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
