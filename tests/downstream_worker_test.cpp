#include "udp_ingestion/downstream_worker.hpp"
#include "udp_ingestion/synthetic_message.hpp"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <thread>

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

void test_drain(bool release_before_producing) {
    constexpr std::size_t capacity = 8;
    constexpr std::uint64_t attempts_per_channel = 1000;

    udp_ingestion::Pipeline pipeline(capacity);
    udp_ingestion::StartupGate startup(1);
    udp_ingestion::DownstreamWorker worker(pipeline);

    udp_ingestion::DownstreamResult results{};
    udp_ingestion::DownstreamResult expected{};
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
            throw std::runtime_error("downstream startup cancelled");
        }

        if (release_before_producing) {
            startup.release();
        }

        // Main is the sole producer for both queues.
        for (std::size_t i = 0;
             i < udp_ingestion::kPipelineChannelCount; ++i) {
            auto& channel = pipeline.channels[i];

            for (std::uint64_t sequence = 1;
                 sequence <= attempts_per_channel;
                 ++sequence) {
                const udp_ingestion::PipelineEvent event{
                    udp_ingestion::make_synthetic_message(i, sequence, 16)
                };

                // No retry: mirror the production queue-full policy.
                if (channel.try_enqueue(event)) {
                    udp_ingestion::process_downstream_message(
                        event.message, expected[i]);
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

    for (std::size_t i = 0;
         i < udp_ingestion::kPipelineChannelCount; ++i) {
        auto& channel = pipeline.channels[i];
        const auto producer = channel.producer_stats();

        expect(channel.producer_done(),
               "producer completion remains visible");
        expect(producer.enqueued_packets + producer.queue_full_drops ==
                   attempts_per_channel,
               "every enqueue attempt is accounted for");
        expect(results[i].processed_packets == producer.enqueued_packets,
               "downstream processes every accepted message");
        expect(results[i].processed_packets == expected[i].processed_packets,
               "processed count matches accepted-message reference");
        expect(results[i].checksum == expected[i].checksum,
               "checksum matches accepted-message reference");

        if (!release_before_producing) {
            expect(producer.enqueued_packets == capacity,
                   "closed startup gate leaves exactly one full queue");
            expect(producer.queue_full_drops ==
                       attempts_per_channel - capacity,
                   "overflow before consumer startup is counted");
        }

        udp_ingestion::PipelineEvent event{};
        expect(!channel.try_dequeue(event),
               "downstream leaves the queue empty");
    }
}


void test_startup_cancellation() {
    udp_ingestion::Pipeline pipeline(1);
    udp_ingestion::StartupGate startup(1);
    udp_ingestion::DownstreamWorker worker(pipeline);

    const udp_ingestion::PipelineEvent queued{
        udp_ingestion::make_synthetic_message(0, 1, 16)
    };

    expect(pipeline.channels[0].try_enqueue(queued),
           "cancellation test queues one message");
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

    for (const auto& result : results) {
        expect(result.processed_packets == 0,
               "cancelled startup does not process messages");
        expect(result.checksum == 0,
               "cancelled startup returns empty processing results");
    }

    udp_ingestion::PipelineEvent event{};
    const bool dequeued = pipeline.channels[0].try_dequeue(event);

    expect(dequeued, "startup cancellation leaves queued data untouched");

    if (dequeued) {
        expect(
            udp_ingestion::encode_protocol_v1(event.message) ==
                udp_ingestion::encode_protocol_v1(queued.message),
            "queued message is unchanged after startup cancellation");
    }

    expect(!pipeline.channels[0].try_dequeue(event),
           "cancellation test contains no extra messages");
}

} // namespace


int main() {
    try {
        test_drain(false);
        test_drain(true);
        test_startup_cancellation();
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