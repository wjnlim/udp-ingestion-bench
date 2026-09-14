#pragma once

#include <cstdint>

namespace udp_ingestion {

// Returns CLOCK_REALTIME epoch nanoseconds.
std::int64_t realtime_now_ns();

std::int64_t calculate_latency_ns(std::int64_t rx_timestamp_ns, 
                                    std::int64_t completion_timestamp_ns);

} // namespace udp_ingestion