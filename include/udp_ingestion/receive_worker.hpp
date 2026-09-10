#pragma once

#include "udp_ingestion/receive_processing.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

namespace udp_ingestion {

struct ReceiveWorkerConfig {
    std::string bind_address = "127.0.0.1";
    std::uint16_t port = 9000;

    // Per-channel datagram count, including invalid datagrams
    std::uint64_t expected_packets = 500;
    
    // This is also applied while waiting for the first datagram.
    std::chrono::milliseconds idle_timeout{3000};
};

enum class ReceiveStopReason {
    CountReached,
    IdleTimeout,
    StopRequested,
};

struct ReceiveWorkerResult {
    ChannelState state{};
    ReceiveStopReason stop_reason = ReceiveStopReason::CountReached;
};

/*
    A ReceiveWorker also manages its socket
*/
class ReceiveWorker {
public:
    explicit ReceiveWorker(const ReceiveWorkerConfig& config);
    ~ReceiveWorker();

    ReceiveWorker(const ReceiveWorker&) = delete;
    ReceiveWorker& operator=(const ReceiveWorker&) = delete;

    // Receive loop
    ReceiveWorkerResult run(const std::atomic<bool>& stop_requested);

private:
    ReceiveWorkerConfig config_;
    int fd = -1;
};

} // namespace udp_ingestion
