#include "udp_ingestion/receiver_reporting.hpp"

#include <iomanip>
#include <iostream>

namespace udp_ingestion {
namespace {

const char* stop_reason_name(ReceiveStopReason reason) {
    switch (reason) {
    case ReceiveStopReason::CountReached:
        return "count_reached";
    case ReceiveStopReason::IdleTimeout:
        return "idle_timeout";
    case ReceiveStopReason::StopRequested:
        return "stop_requested";
    }

    return "unknown";
}

bool is_clean_result(const ReceiveWorkerResult& result) {
    return result.stop_reason == ReceiveStopReason::CountReached &&
           result.state.invalid_packets == 0 && result.state.gap_events == 0 &&
           result.state.late_or_duplicate_packets == 0;
}

} // namespace

void print_result(std::size_t channel, std::uint16_t port, std::uint64_t expected_packets,
                  const ReceiveWorkerResult& result, const ProducerStats& producer,
                  const DownstreamState& downstream, const LatencySamples& latency_samples,
                  const std::optional<LatencyStatistics>& latency_statistics) {
    const auto& state = result.state;

    std::cout << "\nchannel=" << channel << "\nport=" << port
              << "\nstop=" << stop_reason_name(result.stop_reason)
              << "\ntarget=" << expected_packets << "\nreceived=" << state.received_packets
              << "\nvalid=" << state.valid_packets << "\ninvalid=" << state.invalid_packets
              << "\ngap_events=" << state.gap_events << "\nmissing=" << state.missing_packets
              << "\nlate_or_duplicate=" << state.late_or_duplicate_packets
              << "\nexpected_sequence=" << state.expected_sequence
              << "\nsequence_exhausted=" << state.sequence_exhausted
              << "\nenqueued=" << producer.enqueued_packets
              << "\nqueue_full_drops=" << producer.queue_full_drops
              << "\nprocessed=" << downstream.processed_packets
              << "\nchecksum=" << downstream.checksum
              << "\nwarmup_processed=" << latency_samples.warmup_processed()
              << "\nlatency_samples=" << latency_samples.sample_count() << '\n';
    if (latency_statistics) {
        std::cout << "latency_p50_ns=" << latency_statistics->p50_ns
                  << "\nlatency_p99_ns=" << latency_statistics->p99_ns
                  << "\nlatency_p999_ns=" << latency_statistics->p999_ns << '\n';
    } else {
        std::cout << "latency_p50_ns=NA\n"
                  << "latency_p99_ns=NA\n"
                  << "latency_p999_ns=NA\n";
    }
}

void print_measurement(const std::optional<ProcessingMeasurement>& measurement) {
    std::cout << "\nmeasurement_scope=post_warmup_to_drain\n"
              << "measurement_available=" << (measurement ? 1 : 0) << '\n';
    if (!measurement) {
        std::cout << "measurement_elapsed_ns=NA\n"
                  << "measurement_process_cpu_ns=NA\n"
                  << "measurement_cpu_percent=NA\n"
                  << "measurement_processed_pps=NA\n";

        for (std::size_t i = 0; i < kPipelineChannelCount; ++i) {
            std::cout << "measurement_processed_channel_" << i << "=NA\n";
        }
        return;
    }

    const auto previous_precision = std::cout.precision();
    std::cout << std::setprecision(10)
              << "measurement_elapsed_ns=" << measurement->timing.elapsed_ns
              << "\nmeasurement_process_cpu_ns=" << measurement->timing.process_cpu_ns
              << "\nmeasurement_cpu_percent=" << measurement->timing.cpu_utilization_percent
              << "\nmeasurement_processed_pps=" << measurement->processed_pps << '\n';
    std::cout.precision(previous_precision);

    for (std::size_t i = 0; i < kPipelineChannelCount; ++i) {
        std::cout << "measurement_processed_channel_" << i << '='
                  << measurement->processed_packets[i] << '\n';
    }
}

bool validate_result(std::size_t channel, const ReceiveWorkerResult& result,
                     const ProducerStats& producer, const DownstreamState& processed,
                     const LatencySamples& latency_samples) {
    const bool accounting_matches =
        result.state.valid_packets == producer.enqueued_packets + producer.queue_full_drops &&
        processed.processed_packets == producer.enqueued_packets &&
        processed.processed_packets ==
            latency_samples.warmup_processed() + latency_samples.sample_count();

    if (!accounting_matches) {
        std::cerr << "channel=" << channel << " pipeline accounting mismatch\n";
    }

    return is_clean_result(result) && producer.queue_full_drops == 0 && accounting_matches;
}

} // namespace udp_ingestion
