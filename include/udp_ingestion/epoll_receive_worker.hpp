#pragma once

#include "udp_ingestion/receive_worker.hpp"
#include "udp_ingestion/pipeline.hpp"
#include "udp_ingestion/stop_event.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace udp_ingestion {

inline constexpr std::size_t kEpollChannelCount = 2;

struct EpollReceiveWorkerConfig {
    std::string bind_address = "127.0.0.1";
    std::uint16_t base_port = 9000;

    // Aggregate datagram count across both channels, including invalid input.
    std::uint64_t expected_total_packets = 1'000;

    // Per-channel idle timeout, including the initial wait.
    std::chrono::milliseconds idle_timeout{3000};

    std::optional<int> cpu;
};

struct EpollChannelResult {
    std::uint16_t port = 0;
    std::uint64_t expected_packets = 0;
    ReceiveWorkerResult result{};
};

using EpollReceiveResult = std::array<EpollChannelResult, kEpollChannelCount>;

class EpollReceiveWorker {
public:
    EpollReceiveWorker(const EpollReceiveWorkerConfig& config, const StopEvent& stop_event);
    ~EpollReceiveWorker();

    EpollReceiveWorker(const EpollReceiveWorker&) = delete;
    EpollReceiveWorker& operator=(const EpollReceiveWorker&) = delete;

    void apply_cpu_affinity();
    // Runs polling loop on the calling thread; does not create a thread.
    EpollReceiveResult run(Pipeline& output);

private:
    using SteadyClock = std::chrono::steady_clock;

    struct Channel {
        int fd = -1;
        std::uint16_t port = 0;
        std::uint64_t expected_packets = 0;
        ReceiveWorkerResult result{};
        SteadyClock::time_point last_receive{};
        bool active = false;
    };

    void close_descriptors() noexcept;
    void finish_channel(std::size_t channel_idx, ReceiveStopReason reason, PipelineChannel& output);
    bool has_active_channels() const;
    void expire_idle_channels(SteadyClock::time_point now, Pipeline& output);
    int wait_timeout_ms(SteadyClock::time_point now) const;
    void receive_ready_channel(std::size_t channel_idx, PipelineChannel& output);

    EpollReceiveWorkerConfig config_;
    int epoll_fd_ = -1;
    std::array<Channel, kEpollChannelCount> channels_{};
    std::size_t active_channel_count_ = 0;
};

} // namespace udp_ingestion
