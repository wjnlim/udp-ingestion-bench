#include "udp_ingestion/downstream_worker.hpp"
#include "udp_ingestion/cpu_affinity.hpp"
#include "udp_ingestion/latency.hpp"
#include "udp_ingestion/run_timing.hpp"

#include <cstdint>
#include <array>
#include <cstddef>
#include <optional>

namespace udp_ingestion {

DownstreamWorker::DownstreamWorker(Pipeline& pipeline, ChannelLatencySamples& latency_samples,
                                   std::optional<int> cpu)
    : pipeline_(pipeline), latency_samples_(latency_samples), cpu_(cpu) {}

DownstreamResult DownstreamWorker::run(StartupGate& startup) {
    measurement_.reset();

    try {
        DownstreamResult results{};
        std::array<bool, kPipelineChannelCount> finished{};
        std::size_t remaining_channels = kPipelineChannelCount;

        std::array<std::uint64_t, kPipelineChannelCount> warmup_seen{};
        std::size_t remaining_warmup_channels = 0;
        bool warmup_valid = true;

        for (std::size_t i = 0; i < kPipelineChannelCount; ++i) {
            if (latency_samples_[i]->last_warmup_sequence() != 0) {
                ++remaining_warmup_channels;
            }
        }

        std::optional<RunTimingSnapshot> measurement_begin;
        std::array<std::uint64_t, kPipelineChannelCount> processed_at_begin{};

        const auto begin_measurement = [&]() {
            for (std::size_t i = 0; i < kPipelineChannelCount; ++i) {
                processed_at_begin[i] = results[i].processed_packets;
            }
            measurement_begin = capture_run_timing();
        };

        PipelineEvent event{};

        if (cpu_.has_value()) {
            pin_current_thread(*cpu_, "downstream");
        }

        if (!startup.arrive_and_wait()) {
            return results;
        }

        // draining loop
        while (remaining_channels != 0) {
            for (std::size_t i = 0; i < kPipelineChannelCount; ++i) {
                if (finished[i]) {
                    continue;
                }

                auto& channel = pipeline_.channels[i];

                /*
                    Check producer_done first and then empty;
                    Otherwise, some data might arrive between the checking empty
                    and checking producer_done
                */
                const bool producer_done = channel.producer_done();

                if (channel.try_dequeue(event)) {
                    if (!measurement_begin && warmup_valid && remaining_warmup_channels == 0) {
                        begin_measurement();
                    }

                    process_downstream_message(event.message, results[i]);

                    const auto completion_ns = realtime_now_ns();
                    latency_samples_[i]->record_processed(event.message.sequence,
                                                          event.rx_timestamp_ns, completion_ns);

                    if (warmup_valid) {
                        const auto boundary = latency_samples_[i]->last_warmup_sequence();
                        const auto sequence = event.message.sequence;

                        // the current packet is a warmup packet
                        if (sequence <= boundary) {
                            // duplicated or out-of-order
                            if (warmup_seen[i] == boundary || sequence != warmup_seen[i] + 1) {
                                warmup_valid = false;
                            } else {
                                ++warmup_seen[i];

                                if (warmup_seen[i] == boundary) {
                                    --remaining_warmup_channels;
                                }
                            }
                        } else if (warmup_seen[i] != boundary) {
                            warmup_valid = false;
                        }
                    }

                    if (!measurement_begin && warmup_valid && remaining_warmup_channels == 0) {
                        begin_measurement();
                    }
                } else if (producer_done) {
                    finished[i] = true;
                    --remaining_channels;
                }
            }
        }

        if (measurement_begin && warmup_valid) {
            const auto measurement_end = capture_run_timing();

            ProcessingMeasurement measurement{};
            bool has_measured_packets = false;
            std::uint64_t total_processed = 0;

            for (std::size_t i = 0; i < kPipelineChannelCount; ++i) {
                const auto count = results[i].processed_packets - processed_at_begin[i];
                measurement.processed_packets[i] = count;
                has_measured_packets = has_measured_packets || count != 0;
                total_processed += count;
            }
            if (has_measured_packets) {
                measurement.timing = calculate_run_timing(*measurement_begin, measurement_end);
                measurement.processed_pps = static_cast<double>(total_processed) * 1'000'000'000.0 /
                                            static_cast<double>(measurement.timing.elapsed_ns);
                measurement_ = measurement;
            }
        }

        return results;
    } catch (...) {
        startup.cancel();
        throw;
    }
}

} // namespace udp_ingestion
