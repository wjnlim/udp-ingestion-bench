#include "udp_ingestion/latency_samples.hpp"
#include "udp_ingestion/latency_clock.hpp"

#include <stdexcept>


namespace udp_ingestion {

LatencySamples::LatencySamples(std::size_t capacity, 
                                std::uint64_t last_warmup_sequence) 
                                : last_warmup_sequence_(last_warmup_sequence){
    if (last_warmup_sequence > capacity) {
        throw std::invalid_argument(
                "warmup sequence exceeds channel sample capacity");
    }

    samples_.resize(capacity);
}

void LatencySamples::record_processed(std::uint64_t sequence, 
                            std::int64_t rx_timestamp_ns,
                            std::int64_t completion_timestamp_ns) {
    const auto latency_ns = calculate_latency_ns(rx_timestamp_ns, 
                                                    completion_timestamp_ns);
    if (sequence <= last_warmup_sequence_) {
        ++warmup_processed_;
        return;
    }

    if (sample_count_ >= samples_.size()) {
        throw std::runtime_error("latency sample storage exhausted");
    }

    samples_[sample_count_] = latency_ns;
    ++sample_count_;
}


} // namespace udp_ingestion