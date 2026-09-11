#include "udp_ingestion/epoll_receive_worker.hpp"
#include "udp_ingestion/cpu_affinity.hpp"

#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <stdexcept>
#include <string>
#include <system_error>
#include <chrono>
#include <array>

namespace udp_ingestion {

namespace {
constexpr std::size_t kReceiveAttemptBudget = 64;
} // namespace

EpollReceiveWorker::EpollReceiveWorker(const EpollReceiveWorkerConfig& config)
    : config_(config) {

    if (config_.base_port == 0 || config_.base_port > 65534) {
        throw std::invalid_argument("base port must be in [1, 65534]");
    }

    if (config_.expected_total_packets == 0) {
        throw std::invalid_argument("expected total packets must be positive");
    }

    if (config_.idle_timeout <= std::chrono::milliseconds::zero() ||
        config_.idle_timeout > std::chrono::milliseconds{86'400'000}) {
        throw std::invalid_argument(
            "idle timeout must be in [1, 86400000] ms");
    }

    in_addr bind_address{};
    if (::inet_pton(AF_INET, config_.bind_address.c_str(), &bind_address) != 1) {
        throw std::invalid_argument(
            "bind address must be a valid IPv4 address");
    }

    try {
        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ < 0) {
            const int error = errno;
            throw std::system_error(
                error, std::generic_category(), "epoll_create1");
        }

        for (std::size_t i = 0; i < kEpollChannelCount; ++i) {
            auto& channel = channels_[i];

            channel.expected_packets = 
                config_.expected_total_packets / kEpollChannelCount;
            if (i == 0) {
                channel.expected_packets += 
                    config_.expected_total_packets % kEpollChannelCount;
            }

            channel.port = static_cast<std::uint16_t>(config_.base_port + i);

            sockaddr_in address{};
            address.sin_family = AF_INET;
            address.sin_port = htons(channel.port);
            address.sin_addr = bind_address;

            channel.fd = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
            if (channel.fd < 0) {
                const int error = errno;
                throw std::system_error(
                    error, std::generic_category(),
                    "socket for channel " + std::to_string(i));
            }

            if (::bind(channel.fd, reinterpret_cast<const sockaddr*>(&address),
                            sizeof(address)) != 0) {
                const int error = errno;
                throw std::system_error(
                    error, std::generic_category(), 
                    "bind for channel " + std::to_string(i) +
                    " on port " + std::to_string(channel.port));
            }

            if (channel.expected_packets == 0) {
                channel.result.stop_reason = ReceiveStopReason::CountReached;
                continue;
            }

            epoll_event event{};
            event.events = EPOLLIN;
            event.data.u32 = static_cast<std::uint32_t>(i);

            if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, channel.fd, &event) != 0) {
                const int error = errno;
                throw std::system_error(
                    error, std::generic_category(),
                    "epoll_ctl ADD for channel " + std::to_string(i) +
                    " on port " + std::to_string(channel.port));
            }

            channel.active = true;
        }
    } catch (...) {
        close_descriptors();
        throw;
    }

}

EpollReceiveWorker::~EpollReceiveWorker() {
    close_descriptors();
}

void EpollReceiveWorker::close_descriptors() noexcept {
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
    for (auto& channel : channels_) {
        if (channel.fd >= 0) {
            ::close(channel.fd);
            channel.fd = -1;
        }
        channel.active = false;
    }
}

void EpollReceiveWorker::finish_channel(
    std::size_t channel_idx, ReceiveStopReason reason) {
    auto& channel = channels_[channel_idx];

    if (!channel.active) {
        return;
    }

    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, channel.fd, nullptr) != 0) {
        const int error = errno;
        throw std::system_error(
            error, std::generic_category(),
            "epoll_ctl DEL for channel " + std::to_string(channel_idx));
    }

    channel.active = false;
    channel.result.stop_reason = reason;
}

bool EpollReceiveWorker::has_active_channels() const {
    for (const auto& channel : channels_) {
        if (channel.active) {
            return true;
        }
    }

    return false;
}

void EpollReceiveWorker::expire_idle_channels(SteadyClock::time_point now) {
    for (std::size_t i = 0; i < kEpollChannelCount; ++i) {
        const auto& channel = channels_[i];

        if (!channel.active) {
            continue;
        }

        if (now - channel.last_receive >= config_.idle_timeout) {
            finish_channel(i, ReceiveStopReason::IdleTimeout);
        }
    }
}

int EpollReceiveWorker::wait_timeout_ms(SteadyClock::time_point now) const {
    if (!has_active_channels()) {
        return 0;
    }

    auto timeout = config_.idle_timeout;
    for (const auto& channel : channels_) {
        if (!channel.active) {
            continue;
        }

        const auto deadline =
            channel.last_receive + config_.idle_timeout;
        
        if (now >= deadline) {
            return 0;
        }

        const auto remaining = 
            std::chrono::ceil<std::chrono::milliseconds>(deadline-now);
        
        if (remaining < timeout) {
            timeout = remaining;
        }
    }

    // Constructor bounds the timeout value at most 86,400,000 ms
    return static_cast<int>(timeout.count());
}

void EpollReceiveWorker::receive_ready_channel(std::size_t channel_idx) {
    auto& channel = channels_[channel_idx];

    if (!channel.active) {
        return;
    }

    // One extra byte for detecting oversized datagrams
    std::array<std::uint8_t, kProtocolV1BufSize+1> buffer{};

    for (std::size_t i = 0; i < kReceiveAttemptBudget; ++i) {
        const auto size = ::recv(channel.fd, buffer.data(), buffer.size(), 0);

        if (size >= 0) {
            channel.last_receive = SteadyClock::now();

            process_datagram(buffer.data(), static_cast<std::size_t>(size),
                                                        channel.result.state);
            if (channel.result.state.received_packets >= channel.expected_packets) {
                finish_channel(channel_idx, ReceiveStopReason::CountReached);
                return;
            }

            continue;
        }

        const int error = errno;
        if (error == EAGAIN || error == EWOULDBLOCK) {
            return;
        }
        if (error == EINTR) {
            continue;
        }

        throw std::system_error(error, std::generic_category(), 
                                "recv for channel " + std::to_string(channel_idx));
    }
}

EpollReceiveResult EpollReceiveWorker::run() {
    if (config_.cpu.has_value()) {
        pin_current_thread(*config_.cpu, "epoll RX");
    }

    const auto start = SteadyClock::now();

    for (auto& channel : channels_) {
        if (channel.active) {
            channel.last_receive = start;
        }
    }

    std::array<epoll_event, kEpollChannelCount> events{};

    while (has_active_channels()) {
        const int timeout = wait_timeout_ms(SteadyClock::now());

        const int ready = ::epoll_wait(epoll_fd_, events.data(),
                            static_cast<int>(events.size()), timeout);
        if (ready < 0) {
            const int error = errno;
            if (error == EINTR) {
                expire_idle_channels(SteadyClock::now());
                continue;
            }

            throw std::system_error(error, std::generic_category(), "epoll_wait");
        }

        for (int i = 0; i < ready; ++i) {
            const auto& event = events[i];
            const auto channel_idx = static_cast<std::size_t>(event.data.u32);

            if (channel_idx >= channels_.size()) {
                throw std::runtime_error("epoll returned an invalid channel index");
            }

            const auto& channel = channels_[channel_idx];

            if (!channel.active) {
                continue;
            }

            if ((event.events & EPOLLERR) != 0) {
                int socket_error = 0;
                socklen_t error_size = sizeof(socket_error);

                if (::getsockopt(channel.fd, SOL_SOCKET, SO_ERROR,
                                    &socket_error, &error_size) != 0) {
                    const int error = errno;
                    throw std::system_error(
                        error, std::generic_category(),
                        "getsockopt SO_ERROR for channel " +
                        std::to_string(channel_idx));
                }

                if (socket_error != 0) {
                    throw std::system_error(socket_error, std::generic_category(),
                        "socket error for channel " +
                            std::to_string(channel_idx));
                }

                throw std::runtime_error(
                    "EPOLLERR without SO_ERROR for channel " +
                        std::to_string(channel_idx));
            }

            if ((event.events & EPOLLHUP) != 0) {
                throw std::runtime_error(
                    "unexpected EPOLLHUP for UDP channel " +
                        std::to_string(channel_idx));
            }

            if ((event.events & EPOLLIN) != 0) {
                receive_ready_channel(channel_idx);
            }
        }

        expire_idle_channels(SteadyClock::now());
    }

    EpollReceiveResult results{};
    for (std::size_t i = 0; i < kEpollChannelCount; ++i) {
        const auto& channel = channels_[i];

        results[i] = EpollChannelResult{
            channel.port,
            channel.expected_packets,
            channel.result
        };
    }

    return results;
}

}// namespace udp_ingestion