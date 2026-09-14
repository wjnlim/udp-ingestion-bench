#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace udp_ingestion {

class LatencySamples {
public:
    // capacity: maximum sample count for this channel.
    // last_warmup_sequence: final sequence excluded as warmup.
    LatencySamples(std::size_t capacity, std::uint64_t last_warmup_sequence);

    LatencySamples(const LatencySamples&) = delete;
    LatencySamples& operator=(const LatencySamples&) = delete;

    void record_processed(std::uint64_t sequence, 
                            std::int64_t rx_timestamp_ns,
                            std::int64_t completion_timestamp_ns);
    std::size_t sample_count() const noexcept {
        return sample_count_;
    }

    std::size_t warmup_processed() const noexcept {
        return warmup_processed_;
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

} // namespace udp_ingestion