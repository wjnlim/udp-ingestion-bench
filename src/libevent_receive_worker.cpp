#include "udp_ingestion/libevent_receive_worker.hpp"
#include "udp_ingestion/cpu_affinity.hpp"
#include "udp_ingestion/timestamped_receive.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <system_error>
#include <sys/time.h>

namespace udp_ingestion {

namespace {

constexpr std::size_t kReceiveAttemptBudget = 64;

} // namespace

LibeventReceiveWorker::LibeventReceiveWorker(
    const LibeventReceiveWorkerConfig& config, 
                        const StopEvent& stop_event) : config_(config) {
    if (config_.base_port == 0 ||
        static_cast<std::size_t>(config_.base_port) + kLibeventChannelCount - 1 > 65535) {
        throw std::invalid_argument("channel ports must be in [1, 65535]");
    }

    if (config_.expected_total_packets == 0) {
        throw std::invalid_argument("expected total packets must be positive");
    }

    if (config_.idle_timeout <= std::chrono::milliseconds::zero() ||
        config_.idle_timeout > std::chrono::milliseconds{86'400'000}) {
        throw std::invalid_argument("idle timeout must be in [1, 86400000] ms");
    }

    in_addr bind_address{};
    if (::inet_pton(AF_INET, config_.bind_address.c_str(), &bind_address) != 1) {
        throw std::invalid_argument("bind address must be a valid IPv4 address");
    }

    try {
        base_ = event_base_new();
        if (base_ == nullptr) {
            throw std::runtime_error("event_base_new failed");
        }

        if (std::strcmp(backend_poller(), "epoll") != 0) {
            throw std::runtime_error("libevent comparison requires the epoll backend");
        }

        stop_event_ = event_new(base_, stop_event.get_fd(),
                                EV_READ | EV_PERSIST, stop_callback, this);
        if (stop_event_ == nullptr) {
            throw std::runtime_error("event_new for stop event failed");
        }

        if (event_add(stop_event_, nullptr) != 0) {
            throw std::runtime_error("event_add for stop event failed");
        }

        idle_event_ = evtimer_new(base_, idle_callback, this);
        if (idle_event_ == nullptr) {
            throw std::runtime_error("evtimer_new failed");
        }

        for (std::size_t i = 0; i < kLibeventChannelCount; ++i) {
            auto& channel = channels_[i];
            channel.owner = this;
            channel.index = i;

            channel.expected_packets =
                config_.expected_total_packets / kLibeventChannelCount +
                (i < config_.expected_total_packets % kLibeventChannelCount ? 1 : 0);
            channel.port = static_cast<std::uint16_t>(config_.base_port + i);

            sockaddr_in address{};
            address.sin_family = AF_INET;
            address.sin_port = htons(channel.port);
            address.sin_addr = bind_address;

            channel.fd = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
            if (channel.fd < 0) {
                const int error = errno;
                throw std::system_error(error, std::generic_category(),
                                        "socket for channel " + std::to_string(i));
            }

            enable_rx_timestamp(channel.fd);

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

            channel.read_event = event_new(
                base_, channel.fd, EV_READ | EV_PERSIST, read_callback, &channel);
            if (channel.read_event == nullptr) {
                throw std::runtime_error(
                    "event_new for channel " + std::to_string(i) + " failed");
            }

            if (event_add(channel.read_event, nullptr) != 0) {
                throw std::runtime_error(
                    "event_add for channel " + std::to_string(i) + " failed");
            }

            channel.active = true;
            ++active_channel_count_;
        }
    } catch (...) {
        release_resources();
        throw;
    }
}

LibeventReceiveWorker::~LibeventReceiveWorker() {
    release_resources();
}

void LibeventReceiveWorker::release_resources() noexcept {
    for (auto& channel : channels_) {
        if (channel.read_event != nullptr) {
            event_free(channel.read_event);
            channel.read_event = nullptr;
        }

        if (channel.fd >= 0) {
            ::close(channel.fd);
            channel.fd = -1;
        }
        channel.active = false;
    }

    if (stop_event_ != nullptr) {
        event_free(stop_event_);
        stop_event_ = nullptr;
    }

    if (idle_event_ != nullptr) {
        event_free(idle_event_);
        idle_event_ = nullptr;
    }

    if (base_ != nullptr) {
        event_base_free(base_);
        base_ = nullptr;
    }

    active_channel_count_ = 0;
}

void LibeventReceiveWorker::apply_cpu_affinity() {
    if (config_.cpu.has_value()) {
        pin_current_thread(*config_.cpu, "libevent RX");
    }
}

const char* LibeventReceiveWorker::backend_poller() const noexcept {
    return event_base_get_method(base_);
}

void LibeventReceiveWorker::finish_channel(std::size_t channel_idx, 
                                            ReceiveStopReason reason) {
    auto& channel = channels_[channel_idx];
    if (!channel.active) {
        return;
    }

    if (event_del(channel.read_event) != 0) {
        throw std::runtime_error(
            "event_del for channel " + std::to_string(channel_idx) + " failed");
    }
    channel.active = false;
    --active_channel_count_;
    channel.result.stop_reason = reason;
    output_->channels[channel_idx].mark_producer_done();

    if (active_channel_count_ == 0) {
        if (event_base_loopbreak(base_) != 0) {
            throw std::runtime_error("event_base_loopbreak failed");
        }
    }
}

void LibeventReceiveWorker::receive_ready_channel(std::size_t channel_idx) {
    auto& channel = channels_[channel_idx];

    if (!channel.active) {
        return;
    }

    ReceivedDatagram datagram{};
    PipelineEvent event{};
    auto& output = output_->channels[channel_idx];

    for (std::size_t i = 0; i < kReceiveAttemptBudget; ++i) {
        const auto size = receive_timestamped_datagram(channel.fd, datagram);

        if (size >= 0) {
            channel.last_receive = SteadyClock::now();

            if (decode_and_track_datagram(
                    datagram.payload.data(), static_cast<std::size_t>(size),
                    event.message, channel.result.state)) {
                event.rx_timestamp_ns = datagram.rx_timestamp_ns;
                output.try_enqueue(event);
            }

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

        throw std::system_error(
            error, std::generic_category(),
            "recvmsg for channel " + std::to_string(channel_idx));
    }
}

void LibeventReceiveWorker::expire_idle_channels(SteadyClock::time_point now) {
    for (std::size_t i = 0; i < kLibeventChannelCount; ++i) {
        const auto& channel = channels_[i];

        if (!channel.active) {
            continue;
        }

        if (now - channel.last_receive >= config_.idle_timeout) {
            finish_channel(i, ReceiveStopReason::IdleTimeout);
        }
    }
}

void LibeventReceiveWorker::arm_idle_timer() {
    if (active_channel_count_ == 0) {
        return;
    }

    if (event_base_update_cache_time(base_) != 0) {
        throw std::runtime_error("event_base_update_cache_time failed");
    }

    const auto now = SteadyClock::now();
    auto timeout = config_.idle_timeout;

    for (const auto& channel : channels_) {
        if (!channel.active) {
            continue;
        }

        const auto deadline = channel.last_receive + config_.idle_timeout;

        if (now >= deadline) {
            timeout = std::chrono::milliseconds::zero();
            break;
        }

        const auto remaining =
            std::chrono::ceil<std::chrono::milliseconds>(deadline - now);

        if (remaining < timeout) {
            timeout = remaining;
        }
    }

    const auto timeout_ms = timeout.count();

    timeval delay{};
    delay.tv_sec = static_cast<decltype(delay.tv_sec)>(timeout_ms / 1000);
    delay.tv_usec = static_cast<decltype(delay.tv_usec)>((timeout_ms % 1000) * 1000);

    if (evtimer_add(idle_event_, &delay) != 0) {
        throw std::runtime_error("evtimer_add failed");
    }
}

void LibeventReceiveWorker::read_callback(evutil_socket_t, short events,
                                          void* context) noexcept {
    auto& channel = *static_cast<Channel*>(context);
    auto& worker = *channel.owner;

    try {
        if ((events & EV_READ) == 0) {
            throw std::runtime_error("unexpected event in read callback");
        }

        worker.receive_ready_channel(channel.index);
    } catch (...) {
        worker.handle_callback_error();
    }
}

void LibeventReceiveWorker::stop_callback(evutil_socket_t, short events,
                                          void* context) noexcept {
    auto& worker = *static_cast<LibeventReceiveWorker*>(context);

    try {
        if ((events & EV_READ) == 0) {
            throw std::runtime_error("unexpected event in stop callback");
        }

        for (std::size_t i = 0; i < kLibeventChannelCount; ++i) {
            worker.finish_channel(i, ReceiveStopReason::StopRequested);
        }
    } catch (...) {
        worker.handle_callback_error();
    }
}

void LibeventReceiveWorker::idle_callback(evutil_socket_t, short events,
                                          void* context) noexcept {
    auto& worker = *static_cast<LibeventReceiveWorker*>(context);

    try {
        if ((events & EV_TIMEOUT) == 0) {
            throw std::runtime_error("unexpected event in idle callback");
        }

        worker.expire_idle_channels(SteadyClock::now());
        worker.arm_idle_timer();
    } catch (...) {
        worker.handle_callback_error();
    }
}

void LibeventReceiveWorker::handle_callback_error() noexcept {
    // Preserve the first exception for run() to rethrow after dispatch returns.
    if (!callback_error_) {
        callback_error_ = std::current_exception();
    }

    if (event_base_loopbreak(base_) != 0) {
        // Cannot safely propagate an exception or guarantee loop termination.
        std::terminate();
    }
}

LibeventReceiveResult LibeventReceiveWorker::run(Pipeline& output) {
    output_ = &output;
    callback_error_ = nullptr;

    try {
        const auto start = SteadyClock::now();

        for (std::size_t i = 0; i < kLibeventChannelCount; ++i) {
            auto& channel = channels_[i];

            if (channel.active) {
                channel.last_receive = start;
            } else {
                output.channels[i].mark_producer_done();
            }
        }

        if (active_channel_count_ != 0) {
            arm_idle_timer();
            // event loop
            const int result = event_base_dispatch(base_);

            if (callback_error_) {
                std::rethrow_exception(callback_error_);
            }

            if (result < 0) {
                throw std::runtime_error("event_base_dispatch failed");
            }

            if (active_channel_count_ != 0) {
                throw std::runtime_error(
                    "event loop exited with active receive channels");
            }
        }

        if (evtimer_del(idle_event_) != 0) {
            throw std::runtime_error("evtimer_del failed");
        }

        if (event_del(stop_event_) != 0) {
            throw std::runtime_error("event_del for stop event failed");
        }

        LibeventReceiveResult results{};
        for (std::size_t i = 0; i < kLibeventChannelCount; ++i) {
            const auto& channel = channels_[i];

            results[i] = LibeventChannelResult{
                channel.port, channel.expected_packets, channel.result};
        }

        output_ = nullptr;
        return results;
    } catch (...) {
        for (auto& channel : output.channels) {
            channel.mark_producer_done();
        }

        output_ = nullptr;
        throw;
    }
}

} //namespace udp_ingestion