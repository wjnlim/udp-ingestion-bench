#include "udp_ingestion/latency_clock.hpp"

#include <time.h>

#include <cerrno>
#include <limits>
#include <stdexcept>
#include <system_error>

namespace udp_ingestion { 

namespace {

constexpr std::int64_t kNanosecondsPerSecond = 1'000'000'000;

} // namespace

std::int64_t realtime_now_ns() {
    timespec timestamp{};

    if (::clock_gettime(CLOCK_REALTIME, &timestamp) != 0) {
        const int error = errno;
        throw std::system_error(error, std::generic_category(),
                            "clock_gettime CLOCK_REALTIME");
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
        throw std::runtime_error(
            "CLOCK_REALTIME exceeds int64 nanoseconds");
    }

    return static_cast<std::int64_t>(timestamp.tv_sec) * kNanosecondsPerSecond
            + static_cast<std::int64_t>(timestamp.tv_nsec);
}

std::int64_t calculate_latency_ns(std::int64_t rx_timestamp_ns, 
                                    std::int64_t completion_timestamp_ns) {
    if (rx_timestamp_ns < 0 || completion_timestamp_ns < 0) {
        throw std::runtime_error("negative epoch timestamp");
    }

    if (completion_timestamp_ns < rx_timestamp_ns) {
        throw std::runtime_error(
            "completion timestamp precedes RX timestamp");
    }

    return completion_timestamp_ns - rx_timestamp_ns;
}

} // namespace udp_ingestion