#pragma once

#include "udp_ingestion/pipeline.hpp"
#include "udp_ingestion/receive_processing.hpp"
#include "udp_ingestion/startup_gate.hpp"
#include "udp_ingestion/latency_samples.hpp"

#include <array>
#include <optional>

namespace udp_ingestion {

using DownstreamResult = 
    std::array<DownstreamState, kPipelineChannelCount>;
using ChannelLatencySamples = 
    std::array<LatencySamples, kPipelineChannelCount>;

class DownstreamWorker {
public:
    explicit DownstreamWorker(Pipeline& pipeline,
                                ChannelLatencySamples& latency_samples,
                                std::optional<int> cpu = std::nullopt);

    DownstreamWorker(const DownstreamWorker&) = delete;
    DownstreamWorker& operator=(const DownstreamWorker&) = delete;

    // Runs on the calling thread; does not create a thread.
    // Returns after every producer is done and both queues are drained.
    // Returns empty results if startup is cancelled
    DownstreamResult run(StartupGate& startup);
private:
    Pipeline& pipeline_;
    ChannelLatencySamples& latency_samples_;
    std::optional<int> cpu_;
};

} // namespace udp_ingestion