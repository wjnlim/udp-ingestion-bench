#pragma once

#include "udp_ingestion/pipeline.hpp"
#include "udp_ingestion/receive_processing.hpp"
#include "udp_ingestion/startup_gate.hpp"
#include "udp_ingestion/latency.hpp"
#include "udp_ingestion/run_timing.hpp"

#include <array>
#include <cstdint>
#include <optional>

namespace udp_ingestion {

using DownstreamResult = std::array<DownstreamState, kPipelineChannelCount>;
using ChannelLatencySamples = std::array<std::optional<LatencySamples>, kPipelineChannelCount>;

struct ProcessingMeasurement {
    RunTiming timing{};

    // Per-channel completion-count over the timed interval.
    // Excludes events completed before the start snapshot.
    std::array<std::uint64_t, kPipelineChannelCount> processed_packets{};

    // Aggregate downstream completions
    // (the total processed packets across the channels)
    //  per second over the timed interval.
    double processed_pps = 0.0;
};

class DownstreamWorker {
public:
    explicit DownstreamWorker(Pipeline& pipeline, ChannelLatencySamples& latency_samples,
                              std::optional<int> cpu = std::nullopt);

    DownstreamWorker(const DownstreamWorker&) = delete;
    DownstreamWorker& operator=(const DownstreamWorker&) = delete;

    // Runs on the calling thread; does not create a thread.
    // Returns after every producer is done and both queues are drained.
    // Returns empty results if startup is cancelled
    DownstreamResult run(StartupGate& startup);

    // A getter; the actual measurement is done in the downstream thread.
    // Called(Read) only after the downstream thread has been joined.
    const std::optional<ProcessingMeasurement>& measurement() const noexcept {
        return measurement_;
    }

private:
    Pipeline& pipeline_;
    ChannelLatencySamples& latency_samples_;
    std::optional<int> cpu_;
    std::optional<ProcessingMeasurement> measurement_;
};

} // namespace udp_ingestion
