#include "udp_ingestion/receive_worker.hpp"
#include "udp_ingestion/cpu_affinity.hpp"
#include "udp_ingestion/timestamped_receive.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

// #include <array>
#include <cerrno>
#include <stdexcept>
#include <system_error>
#include <string>

namespace udp_ingestion {

ReceiveWorker::ReceiveWorker(const ReceiveWorkerConfig& config) : config_(config) {
    if (config_.port == 0) {
        throw std::invalid_argument("receive port must be positive");
    }
    // if (config_.port == 0 || config_.port > 65535) {
    //     throw std::invalid_argument("receive port must be positive");
    // }

    // if (config_.idle_timeout <= std::chrono::milliseconds::zero()) {
    //     throw std::invalid_argument("idle timeout must be positive");
    // }

    if (config_.idle_timeout <= std::chrono::milliseconds::zero() ||
        config_.idle_timeout > std::chrono::milliseconds{86'400'000}) {
        throw std::invalid_argument(
            "idle timeout must be in [1, 86400000] ms");
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(config_.port);

    if (::inet_pton(AF_INET, config_.bind_address.c_str(), &address.sin_addr) != 1) {
        throw std::invalid_argument("bind address must be a valid IPv4 address");        
    }

    fd = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(), "socket");
    }

    try {
        enable_rx_timestamp(fd);

        if (::bind(fd, reinterpret_cast<const sockaddr*>(&address),
                                                    sizeof(address)) != 0) {
            throw std::system_error(errno, std::generic_category(), "bind");
        }
    } catch (...) {
        ::close(fd);
        fd = -1;
        throw;
    }
}

ReceiveWorker::~ReceiveWorker() {
    if (fd >= 0) {
        ::close(fd);
    }
}

ReceiveWorkerResult ReceiveWorker::run(const std::atomic<bool>& stop_requested,
                                                        PipelineChannel& output,
                                                        StartupGate& startup) {
    try {
        if (config_.cpu.has_value()) {
            pin_current_thread(*config_.cpu, 
                    "RX worker on port " + std::to_string(config_.port));
        }
        
        ReceiveWorkerResult result{};
        // one extra byte for detecting oversized datagrams.
        // std::array<std::uint8_t, kProtocolV1BufSize + 1> buffer{};
        ReceivedDatagram datagram{};
        PipelineEvent event{};

        if (!startup.arrive_and_wait()) {
            result.stop_reason = ReceiveStopReason::StopRequested;
            output.mark_producer_done();
            return result;
        }

        auto last_receive = std::chrono::steady_clock::now();

        while (result.state.received_packets < config_.expected_packets) {
            if (stop_requested.load(std::memory_order_relaxed)) {
                result.stop_reason = ReceiveStopReason::StopRequested;
                break;
            }

            // const auto size = ::recv(fd, buffer.data(), buffer.size(), 0);
            const auto size = receive_timestamped_datagram(fd, datagram);

            if (size >= 0) {
                last_receive = std::chrono::steady_clock::now();

                // process_datagram(buffer.data(), static_cast<std::size_t>(size), 
                //                                                     result.state);
                if (decode_and_track_datagram(datagram.payload.data(), 
                                              static_cast<std::size_t>(size),
                                              event.message,
                                              result.state)) {
                    event.rx_timestamp_ns = datagram.rx_timestamp_ns;
                    output.try_enqueue(event);
                }
                continue;
            }

            const int receive_error = errno;
            if (receive_error != EAGAIN && receive_error != EWOULDBLOCK
                                                && receive_error != EINTR) {
                throw std::system_error(receive_error, std::generic_category(), 
                                                                    "recvmsg");
            }

            if (std::chrono::steady_clock::now() - last_receive >= config_.idle_timeout) {
                result.stop_reason = ReceiveStopReason::IdleTimeout;
                break;
            }
        }

        // result.stop_reason = ReceiveStopReason::CountReached;
        output.mark_producer_done();
        return result;

    } catch (...) {
        output.mark_producer_done();
        startup.cancel();
        throw;
    }
}

} // namespace udp_ingestion