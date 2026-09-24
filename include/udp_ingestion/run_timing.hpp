#pragma once

#include <cstdint>

namespace udp_ingestion {

struct RunTimingSnapshot {
    std::int64_t monotonic_ns = 0;
    std::int64_t process_cpu_ns = 0;
};

struct RunTiming {
    std::int64_t elapsed_ns = 0;
    std::int64_t process_cpu_ns = 0;

    // One fully utilized logical CPU corresponds to 100%.
    double cpu_utilization_percent = 0.0;
};

// Reads CLOCK_MONOTONIC and CLOCK_PROCESS_CPUTIME_ID.
// The two clocks are captured sequentially
RunTimingSnapshot capture_run_timing();

RunTiming calculate_run_timing(const RunTimingSnapshot& begin, const RunTimingSnapshot& end);

} // namespace udp_ingestion