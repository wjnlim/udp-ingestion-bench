#include "udp_ingestion/run_timing.hpp"

#include <time.h>

#include <cerrno>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>

namespace udp_ingestion {
namespace {

constexpr std::int64_t kNanosecondsPerSecond = 1'000'000'000;

std::int64_t read_clock_ns(clockid_t clock_id, const char* name) {
    timespec timestamp{};

    if (::clock_gettime(clock_id, &timestamp) != 0) {
        const int error = errno;
        throw std::system_error(error, std::generic_category(),
                                std::string("clock_gettime ") + name);
    }

    if (timestamp.tv_sec < 0 || timestamp.tv_nsec < 0 ||
        timestamp.tv_nsec >= kNanosecondsPerSecond) {
        throw std::runtime_error(std::string("invalid clock value: ") + name);
    }

    constexpr auto maximum = std::numeric_limits<std::int64_t>::max();
    constexpr auto max_seconds = maximum / kNanosecondsPerSecond;
    constexpr auto max_nanoseconds = maximum % kNanosecondsPerSecond;

    if (timestamp.tv_sec > max_seconds ||
        (timestamp.tv_sec == max_seconds && timestamp.tv_nsec > max_nanoseconds)) {
        throw std::runtime_error(std::string("clock exceeds int64 nanoseconds: ") + name);
    }

    return static_cast<std::int64_t>(timestamp.tv_sec) * kNanosecondsPerSecond +
           static_cast<std::int64_t>(timestamp.tv_nsec);
}
} // namespace

RunTimingSnapshot capture_run_timing() {
    const auto monotonic_ns = read_clock_ns(CLOCK_MONOTONIC, "CLOCK_MONOTONIC");
    const auto process_cpu_ns = read_clock_ns(CLOCK_PROCESS_CPUTIME_ID, "CLOCK_PROCESS_CPUTIME_ID");

    return RunTimingSnapshot{monotonic_ns, process_cpu_ns};
}

RunTiming calculate_run_timing(const RunTimingSnapshot& begin, const RunTimingSnapshot& end) {

    if (begin.monotonic_ns < 0 || end.monotonic_ns < 0 || begin.process_cpu_ns < 0 ||
        end.process_cpu_ns < 0) {
        throw std::runtime_error("negative run timing snapshot");
    }

    // Not allow 0 elapsed time, since division is impossible
    if (end.monotonic_ns <= begin.monotonic_ns) {
        throw std::runtime_error("run elapsed time must be positive");
    }

    if (end.process_cpu_ns < begin.process_cpu_ns) {
        throw std::runtime_error("process CPU time moved backwards");
    }

    const auto elapsed_ns = end.monotonic_ns - begin.monotonic_ns;
    const auto process_cpu_ns = end.process_cpu_ns - begin.process_cpu_ns;
    const auto cpu_utilization_percent =
        100.0 * static_cast<double>(process_cpu_ns) / static_cast<double>(elapsed_ns);

    return RunTiming{elapsed_ns, process_cpu_ns, cpu_utilization_percent};
}

} // namespace udp_ingestion