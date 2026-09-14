#include "udp_ingestion/downstream_worker.hpp"
#include "udp_ingestion/cpu_affinity.hpp"
#include "udp_ingestion/latency_clock.hpp"

#include <array>
#include <cstddef>

namespace udp_ingestion {

DownstreamWorker::DownstreamWorker(Pipeline& pipeline, 
                                    ChannelLatencySamples& latency_samples,
                                    std::optional<int> cpu) 
    : pipeline_(pipeline), latency_samples_(latency_samples), cpu_(cpu) {

}

DownstreamResult DownstreamWorker::run(StartupGate& startup) {
    try {
        DownstreamResult results{};
        std::array<bool, kPipelineChannelCount> finished{};
        std::size_t remaining_channels = kPipelineChannelCount;

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
                    process_downstream_message(event.message, results[i]);

                    const auto completion_ns = realtime_now_ns();
                    latency_samples_[i].record_processed(
                        event.message.sequence,
                        event.rx_timestamp_ns,
                        completion_ns);
                } else if (producer_done) {
                    finished[i] = true;
                    --remaining_channels;
                }
            }
        }

        return results;
    } catch (...) {
        startup.cancel();
        throw;
    }
}

}// namespace udp_ingestion