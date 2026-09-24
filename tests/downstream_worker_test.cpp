#include "udp_ingestion/downstream_worker.hpp"
#include "udp_ingestion/synthetic_message.hpp"
#include "udp_ingestion/latency.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <cmath>
#include <optional>
#include <vector>

namespace {

int failures = 0;

void expect(bool condition, const char* description) {
    if (!condition) {
        std::cerr << "FAIL: " << description << '\n';
        ++failures;
    }
}

void mark_all_done(udp_ingestion::Pipeline& pipeline) {
    for (auto& channel : pipeline.channels) {
        channel.mark_producer_done();
    }
}

void test_drain(bool release_before_producing, std::uint64_t last_warmup_sequence) {
    constexpr std::size_t capacity = 8;
    constexpr std::uint64_t attempts_per_channel = 1000;

    udp_ingestion::Pipeline pipeline(capacity);
    udp_ingestion::StartupGate startup(1);

    udp_ingestion::ChannelLatencySamples latency_samples{};
    for (auto& samples : latency_samples) {
        samples.emplace(static_cast<std::size_t>(attempts_per_channel), last_warmup_sequence);
    }

    udp_ingestion::DownstreamWorker worker(pipeline, latency_samples);

    udp_ingestion::DownstreamResult results{};
    udp_ingestion::DownstreamResult expected{};
    std::array<std::size_t, udp_ingestion::kPipelineChannelCount> expected_warmup{};

    std::exception_ptr worker_error;
    std::exception_ptr coordinator_error;

    const auto rx_timestamp_ns = udp_ingestion::realtime_now_ns();

    std::thread consumer([&]() {
        try {
            results = worker.run(startup);
        } catch (...) {
            worker_error = std::current_exception();
        }
    });

    try {
        if (!startup.wait_until_ready()) {
            throw std::runtime_error("downstream startup cancelled");
        }

        if (release_before_producing) {
            startup.release();
        }

        // Main is the sole producer for both queues.
        for (std::size_t i = 0; i < udp_ingestion::kPipelineChannelCount; ++i) {
            auto& channel = pipeline.channels[i];

            for (std::uint64_t sequence = 1; sequence <= attempts_per_channel; ++sequence) {
                const udp_ingestion::PipelineEvent event{
                    udp_ingestion::make_synthetic_message(i, sequence, 16), rx_timestamp_ns};

                // No retry
                if (channel.try_enqueue(event)) {
                    udp_ingestion::process_downstream_message(event.message, expected[i]);

                    if (sequence <= last_warmup_sequence) {
                        ++expected_warmup[i];
                    }
                }
            }

            channel.mark_producer_done();
        }

        if (!release_before_producing) {
            startup.release();
        }
    } catch (...) {
        coordinator_error = std::current_exception();
        startup.cancel();
    }

    // No more enqueue operations occur after this point.
    mark_all_done(pipeline);
    consumer.join();

    if (worker_error) {
        std::rethrow_exception(worker_error);
    }
    if (coordinator_error) {
        std::rethrow_exception(coordinator_error);
    }

    const auto finished_ns = udp_ingestion::realtime_now_ns();
    const auto maximum_latency_ns =
        udp_ingestion::calculate_latency_ns(rx_timestamp_ns, finished_ns);

    for (std::size_t i = 0; i < udp_ingestion::kPipelineChannelCount; ++i) {
        auto& channel = pipeline.channels[i];
        const auto producer = channel.producer_stats();

        expect(channel.producer_done(), "producer completion remains visible");
        expect(producer.enqueued_packets + producer.queue_full_drops == attempts_per_channel,
               "every enqueue attempt is accounted for");
        expect(results[i].processed_packets == producer.enqueued_packets,
               "downstream processes every accepted message");
        expect(results[i].processed_packets == expected[i].processed_packets,
               "processed count matches accepted-message reference");
        expect(results[i].checksum == expected[i].checksum,
               "checksum matches accepted-message reference");

        const auto& samples = *latency_samples[i];

        expect(samples.warmup_processed() == expected_warmup[i],
               "warmup count matches accepted warmup messages");
        expect(samples.sample_count() == expected[i].processed_packets - expected_warmup[i],
               "only accepted non-warmup messages produce samples");
        expect(results[i].processed_packets == samples.warmup_processed() + samples.sample_count(),
               "every processed message is warmup or measured");

        bool samples_in_range = true;
        for (std::size_t sample = 0; sample < samples.sample_count(); ++sample) {
            const auto latency_ns = samples.data()[sample];
            if (latency_ns < 0 || latency_ns > maximum_latency_ns) {
                samples_in_range = false;
                break;
            }
        }
        expect(samples_in_range, "samples lie within the test clock interval");

        if (!release_before_producing) {
            expect(producer.enqueued_packets == capacity,
                   "closed startup gate leaves exactly one full queue");
            expect(producer.queue_full_drops == attempts_per_channel - capacity,
                   "overflow before consumer startup is counted");
        }

        udp_ingestion::PipelineEvent event{};
        expect(!channel.try_dequeue(event), "downstream leaves the queue empty");
    }
}

void test_startup_cancellation() {
    udp_ingestion::Pipeline pipeline(1);
    udp_ingestion::StartupGate startup(1);

    udp_ingestion::ChannelLatencySamples latency_samples{};
    for (auto& samples : latency_samples) {
        samples.emplace(1, 0);
    }

    udp_ingestion::DownstreamWorker worker(pipeline, latency_samples);

    const udp_ingestion::PipelineEvent queued{udp_ingestion::make_synthetic_message(0, 1, 16)};

    expect(pipeline.channels[0].try_enqueue(queued), "cancellation test queues one message");
    mark_all_done(pipeline);

    udp_ingestion::DownstreamResult results{};
    std::exception_ptr worker_error;
    std::exception_ptr coordinator_error;

    std::thread consumer([&]() {
        try {
            results = worker.run(startup);
        } catch (...) {
            worker_error = std::current_exception();
        }
    });

    try {
        if (!startup.wait_until_ready()) {
            throw std::runtime_error("unexpected startup cancellation");
        }

        // Cancel while downstream is waiting for release.
        startup.cancel();
    } catch (...) {
        coordinator_error = std::current_exception();
        startup.cancel();
    }

    consumer.join();

    if (worker_error) {
        std::rethrow_exception(worker_error);
    }
    if (coordinator_error) {
        std::rethrow_exception(coordinator_error);
    }

    expect(!worker.measurement().has_value(), "cancelled startup does not produce a measurement");

    for (const auto& result : results) {
        expect(result.processed_packets == 0, "cancelled startup does not process messages");
        expect(result.checksum == 0, "cancelled startup returns empty processing results");
    }

    for (const auto& samples : latency_samples) {
        expect(samples->warmup_processed() == 0, "cancelled startup does not record warmup");
        expect(samples->sample_count() == 0, "cancelled startup does not record latency samples");
    }

    udp_ingestion::PipelineEvent event{};
    const bool dequeued = pipeline.channels[0].try_dequeue(event);

    expect(dequeued, "startup cancellation leaves queued data untouched");

    if (dequeued) {
        expect(udp_ingestion::encode_protocol_v1(event.message) ==
                   udp_ingestion::encode_protocol_v1(queued.message),
               "queued message is unchanged after startup cancellation");
    }

    expect(!pipeline.channels[0].try_dequeue(event),
           "cancellation test contains no extra messages");
}

using ChannelSequences =
    std::array<std::vector<std::uint64_t>, udp_ingestion::kPipelineChannelCount>;

using ChannelCounts = std::array<std::uint64_t, udp_ingestion::kPipelineChannelCount>;

void test_measurement_case(const char* name, const ChannelSequences& sequences,
                           const ChannelCounts& warmup_boundaries,
                           const std::optional<ChannelCounts>& expected_counts) {
    const auto failures_before = failures;

    constexpr std::size_t capacity = 16;
    udp_ingestion::Pipeline pipeline(capacity);
    udp_ingestion::StartupGate startup(1);
    udp_ingestion::ChannelLatencySamples latency_samples{};
    udp_ingestion::DownstreamResult expected{};

    const auto rx_timestamp_ns = udp_ingestion::realtime_now_ns();

    for (std::size_t i = 0; i < udp_ingestion::kPipelineChannelCount; ++i) {
        latency_samples[i].emplace(capacity, warmup_boundaries[i]);

        for (const auto sequence : sequences[i]) {
            const udp_ingestion::PipelineEvent event{
                udp_ingestion::make_synthetic_message(i, sequence, 16), rx_timestamp_ns};

            if (!pipeline.channels[i].try_enqueue(event)) {
                throw std::runtime_error("measurement fixture queue is full");
            }

            udp_ingestion::process_downstream_message(event.message, expected[i]);
        }
    }

    mark_all_done(pipeline);

    udp_ingestion::DownstreamWorker worker(pipeline, latency_samples);
    udp_ingestion::DownstreamResult results{};
    std::exception_ptr worker_error;

    std::thread consumer([&]() {
        try {
            results = worker.run(startup);
        } catch (...) {
            worker_error = std::current_exception();
        }
    });

    // All input is already queued. Release may precede the worker's arrival.
    startup.release();
    consumer.join();

    if (worker_error) {
        std::rethrow_exception(worker_error);
    }

    const auto& measurement = worker.measurement();

    expect(measurement.has_value() == expected_counts.has_value(),
           "measurement availability matches the expected outcome");

    if (measurement && expected_counts) {
        expect(measurement->processed_packets == *expected_counts,
               "measurement counts only completions inside the interval");
        expect(measurement->timing.elapsed_ns > 0, "measurement elapsed time is positive");
        expect(measurement->timing.process_cpu_ns >= 0, "measurement CPU time is nonnegative");
        expect(std::isfinite(measurement->timing.cpu_utilization_percent) &&
                   measurement->timing.cpu_utilization_percent >= 0.0,
               "measurement CPU utilization is finite and nonnegative");
        expect(std::isfinite(measurement->processed_pps) && measurement->processed_pps > 0.0,
               "measurement throughput is finite and positive");

        std::uint64_t total = 0;
        for (const auto count : *expected_counts) {
            total += count;
        }

        const auto expected_pps = static_cast<double>(total) * 1'000'000'000.0 /
                                  static_cast<double>(measurement->timing.elapsed_ns);

        expect(std::abs(measurement->processed_pps - expected_pps) <= expected_pps * 1e-12,
               "throughput uses interval completion counts");
    }

    for (std::size_t i = 0; i < udp_ingestion::kPipelineChannelCount; ++i) {
        expect(results[i].processed_packets == expected[i].processed_packets,
               "measurement does not prevent draining accepted messages");
        expect(results[i].checksum == expected[i].checksum,
               "measurement preserves downstream processing");

        udp_ingestion::PipelineEvent event{};
        expect(!pipeline.channels[i].try_dequeue(event),
               "measurement case leaves each queue empty");
    }

    if (failures != failures_before) {
        std::cerr << "measurement case: " << name << '\n';
    }
}

void test_measurement_boundaries() {
    test_measurement_case("no warmup", ChannelSequences{{{1, 2, 3}, {1, 2}}}, ChannelCounts{0, 0},
                          ChannelCounts{3, 2});

    test_measurement_case("both channels finish warmup",
                          ChannelSequences{{{1, 2, 3, 4}, {1, 2, 3, 4}}}, ChannelCounts{2, 2},
                          ChannelCounts{2, 2});

    test_measurement_case("early channel completions are excluded",
                          ChannelSequences{{{1, 2, 3, 4}, {1, 2, 3}}}, ChannelCounts{1, 2},
                          ChannelCounts{2, 1});

    test_measurement_case("one empty zero-warmup channel", ChannelSequences{{{1}, {}}},
                          ChannelCounts{0, 0}, ChannelCounts{1, 0});

    test_measurement_case("no input", ChannelSequences{}, ChannelCounts{0, 0}, std::nullopt);

    test_measurement_case("warmup only", ChannelSequences{{{1, 2}, {1, 2}}}, ChannelCounts{2, 2},
                          std::nullopt);

    test_measurement_case("producer ends before warmup completes", ChannelSequences{{{1}, {1, 2}}},
                          ChannelCounts{2, 2}, std::nullopt);

    test_measurement_case("missing warmup sequence", ChannelSequences{{{1, 3, 4}, {1, 2, 3}}},
                          ChannelCounts{2, 2}, std::nullopt);

    test_measurement_case("duplicate warmup cannot replace a missing sequence",
                          ChannelSequences{{{1, 1, 3}, {1, 2, 3}}}, ChannelCounts{2, 2},
                          std::nullopt);

    test_measurement_case("out-of-order warmup", ChannelSequences{{{2, 1, 3}, {1, 2, 3}}},
                          ChannelCounts{2, 2}, std::nullopt);

    test_measurement_case("late warmup invalidates a started measurement",
                          ChannelSequences{{{1, 2, 1, 3}, {1, 2, 3}}}, ChannelCounts{1, 1},
                          std::nullopt);
}

} // namespace

int main() {
    try {
        test_drain(false, 0);
        test_drain(true, 0);
        test_drain(false, 3);
        test_drain(true, 3);
        test_startup_cancellation();
        test_measurement_boundaries();
    } catch (const std::exception& error) {
        std::cerr << "test error: " << error.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "test error: unknown exception\n";
        return 1;
    }

    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return 1;
    }

    std::cout << "all downstream worker tests passed\n";
    return 0;
}
