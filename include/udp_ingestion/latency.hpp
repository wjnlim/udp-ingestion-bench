#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace udp_ingestion {

// Returns CLOCK_REALTIME epoch nanoseconds.
std::int64_t realtime_now_ns();

std::int64_t calculate_latency_ns(std::int64_t rx_timestamp_ns,
                                  std::int64_t completion_timestamp_ns);

class LatencySamples {
public:
    // capacity: maximum sample count for this channel.
    // last_warmup_sequence: final sequence excluded as warmup.
    LatencySamples(std::size_t capacity, std::uint64_t last_warmup_sequence);

    LatencySamples(const LatencySamples&) = delete;
    LatencySamples& operator=(const LatencySamples&) = delete;

    void record_processed(std::uint64_t sequence, std::int64_t rx_timestamp_ns,
                          std::int64_t completion_timestamp_ns);
    std::size_t sample_count() const noexcept {
        return sample_count_;
    }

    std::size_t warmup_processed() const noexcept {
        return warmup_processed_;
    }

    std::uint64_t last_warmup_sequence() const noexcept {
        return last_warmup_sequence_;
    }

    const std::int64_t* data() const noexcept {
        return samples_.data();
    }

private:
    std::vector<std::int64_t> samples_;
    std::uint64_t last_warmup_sequence_;
    std::size_t sample_count_ = 0;
    std::size_t warmup_processed_ = 0;
};

struct LatencyStatistics {
    std::size_t sample_count = 0;
    std::int64_t p50_ns = 0;
    std::int64_t p99_ns = 0;
    std::int64_t p999_ns = 0;
};

// Call only after the downstream worker has finished.
//
// Copies and sorts the copied samples without modifying the source.
// Uses nearest-rank percentiles: rank = ceil(p * sample_count).
// Returns std::nullopt when no samples were recorded.
std::optional<LatencyStatistics> calculate_latency_statistics(const LatencySamples& samples);

} // namespace udp_ingestion
