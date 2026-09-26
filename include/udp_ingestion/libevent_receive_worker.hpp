#pragma once

#include "udp_ingestion/receive_worker.hpp"
#include "udp_ingestion/pipeline.hpp"
#include "udp_ingestion/stop_event.hpp"

#include <event2/event.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <string>

namespace udp_ingestion {

inline constexpr std::size_t kLibeventChannelCount = kPipelineChannelCount;

struct LibeventReceiveWorkerConfig {
    std::string bind_address = "127.0.0.1";
    std::uint16_t base_port = 9000;

    // Aggregate datagram count across both channels, including invalid input.
    std::uint64_t expected_total_packets = 1'000;

    // Per-channel idle timeout, including the initial wait.
    std::chrono::milliseconds idle_timeout{3000};

    std::optional<int> cpu;
};

struct LibeventChannelResult {
    std::uint16_t port = 0;
    std::uint64_t expected_packets = 0;
    ReceiveWorkerResult result{};
};

using LibeventReceiveResult =
    std::array<LibeventChannelResult, kLibeventChannelCount>;

class LibeventReceiveWorker {
public:
    LibeventReceiveWorker(const LibeventReceiveWorkerConfig& config,
                            const StopEvent& stop_event);
    ~LibeventReceiveWorker();
    LibeventReceiveWorker(const LibeventReceiveWorker&) = delete;
    LibeventReceiveWorker& operator=(const LibeventReceiveWorker&) = delete;
    
    void apply_cpu_affinity();

    const char* backend_poller() const noexcept;
    // Call once, after applying RX affinity and releasing the startup gate.
    // Runs on the calling thread; does not create a thread.
    LibeventReceiveResult run(Pipeline& output);

private:
    using SteadyClock = std::chrono::steady_clock;

    struct Channel {
        LibeventReceiveWorker* owner = nullptr;
        std::size_t index = 0;

        int fd = -1;
        event* read_event = nullptr;

        std::uint16_t port = 0;
        std::uint64_t expected_packets = 0;
        ReceiveWorkerResult result{};
        SteadyClock::time_point last_receive{};
        bool active = false;
    };

    static void read_callback(evutil_socket_t fd, short events, void* context) noexcept;
    static void stop_callback(evutil_socket_t fd, short events, void* context) noexcept;
    static void idle_callback(evutil_socket_t fd, short events, void* context) noexcept;

    void receive_ready_channel(std::size_t channel_idx);
    void finish_channel(std::size_t channel_idx, ReceiveStopReason reason);
    void expire_idle_channels(SteadyClock::time_point now);
    void arm_idle_timer();

    void handle_callback_error() noexcept;
    void release_resources() noexcept;

    LibeventReceiveWorkerConfig config_;
    event_base* base_ = nullptr;
    event* stop_event_ = nullptr;
    event* idle_event_ = nullptr;

    std::array<Channel, kLibeventChannelCount> channels_{};
    std::size_t active_channel_count_ = 0;

    Pipeline* output_ = nullptr;
    std::exception_ptr callback_error_;
};

} // namespace udp_ingestion