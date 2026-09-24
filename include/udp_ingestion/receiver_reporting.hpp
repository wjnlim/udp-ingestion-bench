#pragma once

#include "udp_ingestion/downstream_worker.hpp"
#include "udp_ingestion/latency.hpp"
#include "udp_ingestion/receive_worker.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>

namespace udp_ingestion {

// Call after the workers have finished; these functions do not modify their results.
void print_result(std::size_t channel, std::uint16_t port, std::uint64_t expected_packets,
                  const ReceiveWorkerResult& result, const ProducerStats& producer,
                  const DownstreamState& downstream, const LatencySamples& latency_samples,
                  const std::optional<LatencyStatistics>& latency_statistics);

void print_measurement(const std::optional<ProcessingMeasurement>& measurement);

// Reports accounting mismatches and returns the receiver's final per-channel verdict.
bool validate_result(std::size_t channel, const ReceiveWorkerResult& result,
                     const ProducerStats& producer, const DownstreamState& processed,
                     const LatencySamples& latency_samples);

} // namespace udp_ingestion
