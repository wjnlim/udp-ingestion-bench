#pragma once

#include "udp_ingestion/spsc_queue.hpp"
#include "udp_ingestion/protocol_v1.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace udp_ingestion {

inline constexpr std::size_t kPipelineChannelCount = 2;

struct PipelineEvent {
    MarketDataMessage message{};
    
    // Kernel software RX time in CLOCK_REALTIME epoch nanoseconds.
    std::int64_t rx_timestamp_ns = 0;
};

struct ProducerStats {
    std::uint64_t enqueued_packets = 0;
    std::uint64_t queue_full_drops = 0;
};

class PipelineChannel {
public:
    explicit PipelineChannel(std::size_t capacity) : queue_(capacity) {

    }

    PipelineChannel(const PipelineChannel&) = delete;
    PipelineChannel& operator=(const PipelineChannel&) = delete;
    PipelineChannel(PipelineChannel&&) = delete;
    PipelineChannel& operator=(PipelineChannel&&) = delete;
    // Producer only.
    bool try_enqueue(const PipelineEvent& event) noexcept {
        if (!queue_.try_push(event)) {
            ++stats_.queue_full_drops;
            return false;
        }

        ++stats_.enqueued_packets;
        return true;
    }

    // Consumer only.
    bool try_dequeue(PipelineEvent& event) noexcept {
        return queue_.try_pop(event);
    }

    void mark_producer_done() noexcept {
        producer_done_.store(true, std::memory_order_release);
    }

    bool producer_done() const noexcept {
        return producer_done_.load(std::memory_order_acquire);
    }

    ProducerStats producer_stats() const noexcept {
        return stats_;
    }

private:
    static constexpr std::size_t kCacheLineSize = 64;

    SpscQueue<PipelineEvent> queue_;

    alignas(kCacheLineSize) ProducerStats stats_{};
    alignas(kCacheLineSize) std::atomic<bool> producer_done_{false};
};

struct Pipeline {
    explicit Pipeline(std::size_t capacity) 
    : channels{
            PipelineChannel(capacity),
            PipelineChannel(capacity) } {
                
    }

    Pipeline(const Pipeline&) = delete;
    Pipeline& operator=(const Pipeline&) = delete;
    Pipeline(Pipeline&&) = delete;
    Pipeline& operator=(Pipeline&&) = delete;

    std::array<PipelineChannel, kPipelineChannelCount> channels;
};


} // namespace udp_ingestion