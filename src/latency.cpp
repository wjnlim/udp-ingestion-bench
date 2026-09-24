#include "udp_ingestion/latency.hpp"

#include <time.h>

#include <algorithm>
#include <cerrno>
#include <limits>
#include <stdexcept>
#include <system_error>

namespace udp_ingestion {

namespace {

constexpr std::int64_t kNanosecondsPerSecond = 1'000'000'000;

// count > 0, 1 <= per_mille <= 1000.
std::size_t nearest_rank_index(std::size_t count, std::size_t per_mille) {
    // Splitting the calculation avoids overflowing count * per_mille.
    //     ceil(count * per_mille / 1000)
    //     count = 1000 * quotient + remainder.
    //     rank = quotient * per_mille
    //          + ceil(remainder * per_mille / 1000).
    //     For nonnegative integer x, ceil(x / 1000)
    //     is computed as (x + 999) / 1000.
    const auto rank = (count / 1000) * per_mille + ((count % 1000) * per_mille + 999) / 1000;

    return rank - 1;
}

} // namespace

std::int64_t realtime_now_ns() {
    timespec timestamp{};

    if (::clock_gettime(CLOCK_REALTIME, &timestamp) != 0) {
        const int error = errno;
        throw std::system_error(error, std::generic_category(), "clock_gettime CLOCK_REALTIME");
    }

    if (timestamp.tv_sec < 0 || timestamp.tv_nsec < 0 ||
        timestamp.tv_nsec >= kNanosecondsPerSecond) {
        throw std::runtime_error("invalid CLOCK_REALTIME value");
    }

    constexpr auto maximum = std::numeric_limits<std::int64_t>::max();
    constexpr auto max_seconds = maximum / kNanosecondsPerSecond;
    constexpr auto max_nanoseconds = maximum % kNanosecondsPerSecond;

    if (timestamp.tv_sec > max_seconds ||
        (timestamp.tv_sec == max_seconds && timestamp.tv_nsec > max_nanoseconds)) {
        throw std::runtime_error("CLOCK_REALTIME exceeds int64 nanoseconds");
    }

    return static_cast<std::int64_t>(timestamp.tv_sec) * kNanosecondsPerSecond +
           static_cast<std::int64_t>(timestamp.tv_nsec);
}

std::int64_t calculate_latency_ns(std::int64_t rx_timestamp_ns,
                                  std::int64_t completion_timestamp_ns) {
    if (rx_timestamp_ns < 0 || completion_timestamp_ns < 0) {
        throw std::runtime_error("negative epoch timestamp");
    }

    if (completion_timestamp_ns < rx_timestamp_ns) {
        throw std::runtime_error("completion timestamp precedes RX timestamp");
    }

    return completion_timestamp_ns - rx_timestamp_ns;
}

LatencySamples::LatencySamples(std::size_t capacity, std::uint64_t last_warmup_sequence)
    : last_warmup_sequence_(last_warmup_sequence) {
    if (last_warmup_sequence > capacity) {
        throw std::invalid_argument("warmup sequence exceeds channel sample capacity");
    }

    samples_.resize(capacity);
}

void LatencySamples::record_processed(std::uint64_t sequence, std::int64_t rx_timestamp_ns,
                                      std::int64_t completion_timestamp_ns) {
    const auto latency_ns = calculate_latency_ns(rx_timestamp_ns, completion_timestamp_ns);
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

std::optional<LatencyStatistics> calculate_latency_statistics(const LatencySamples& samples) {
    const auto count = samples.sample_count();
    if (count == 0) {
        return std::nullopt;
    }

    std::vector<std::int64_t> sorted(samples.data(), samples.data() + count);
    std::sort(sorted.begin(), sorted.end());

    return LatencyStatistics{count, sorted[nearest_rank_index(count, 500)],
                             sorted[nearest_rank_index(count, 990)],
                             sorted[nearest_rank_index(count, 999)]};
}

} // namespace udp_ingestion
