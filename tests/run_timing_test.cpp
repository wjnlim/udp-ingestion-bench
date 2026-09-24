#include "udp_ingestion/run_timing.hpp"

#include <time.h>

#include <cerrno>
#include <cmath>
#include <cstdint>
#include <exception>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <system_error>

namespace {

int failures = 0;

void expect(bool condition, const char* description) {
    if (!condition) {
        std::cerr << "FAIL: " << description << '\n';
        ++failures;
    }
}

void expect_rejected(const udp_ingestion::RunTimingSnapshot& begin,
                     const udp_ingestion::RunTimingSnapshot& end, const char* description) {
    bool rejected = false;

    try {
        static_cast<void>(udp_ingestion::calculate_run_timing(begin, end));
    } catch (const std::runtime_error&) {
        rejected = true;
    }

    expect(rejected, description);
}

void test_timing_values() {
    using udp_ingestion::calculate_run_timing;

    const auto normal = calculate_run_timing({100, 200}, {300, 250});
    expect(normal.elapsed_ns == 200, "elapsed time is end minus begin");
    expect(normal.process_cpu_ns == 50, "CPU time is end minus begin");
    expect(normal.cpu_utilization_percent == 25.0,
           "CPU utilization uses matching time differences");

    const auto idle = calculate_run_timing({100, 200}, {300, 200});
    expect(idle.process_cpu_ns == 0, "zero CPU-time difference is valid");
    expect(idle.cpu_utilization_percent == 0.0, "zero CPU time produces zero utilization");

    const auto parallel = calculate_run_timing({100, 200}, {200, 450});
    expect(parallel.cpu_utilization_percent == 250.0,
           "CPU utilization is not capped at 100 percent");

    const auto fractional = calculate_run_timing({0, 0}, {3, 1});
    expect(std::abs(fractional.cpu_utilization_percent - 100.0 / 3.0) < 1e-12,
           "CPU utilization preserves fractional results");
}

void test_large_values() {
    using udp_ingestion::calculate_run_timing;

    constexpr auto maximum = std::numeric_limits<std::int64_t>::max();

    const auto full = calculate_run_timing({0, 0}, {maximum, maximum});

    expect(full.elapsed_ns == maximum, "maximum elapsed time is representable");
    expect(full.process_cpu_ns == maximum, "maximum CPU time is representable");
    expect(std::abs(full.cpu_utilization_percent - 100.0) < 1e-12,
           "large equal differences produce 100 percent");

    const auto near_limit = calculate_run_timing({maximum - 2, maximum - 1}, {maximum, maximum});

    expect(near_limit.elapsed_ns == 2, "small elapsed difference near INT64_MAX is preserved");
    expect(near_limit.process_cpu_ns == 1, "small CPU difference near INT64_MAX is preserved");
    expect(near_limit.cpu_utilization_percent == 50.0,
           "integer subtraction precedes floating-point conversion");
}

void test_invalid_inputs() {
    expect_rejected({-1, 0}, {1, 0}, "negative begin monotonic value is rejected");
    expect_rejected({0, 0}, {-1, 0}, "negative end monotonic value is rejected");
    expect_rejected({0, -1}, {1, 0}, "negative begin CPU value is rejected");
    expect_rejected({0, 0}, {1, -1}, "negative end CPU value is rejected");

    expect_rejected({100, 200}, {100, 250}, "zero elapsed time is rejected");
    expect_rejected({100, 200}, {99, 250}, "backward monotonic time is rejected");
    expect_rejected({100, 200}, {101, 199}, "backward CPU time is rejected");

    constexpr auto minimum = std::numeric_limits<std::int64_t>::min();
    constexpr auto maximum = std::numeric_limits<std::int64_t>::max();

    expect_rejected({minimum, 0}, {maximum, 0}, "invalid range is rejected before subtraction");
}

timespec read_reference_clock(clockid_t clock_id) {
    timespec timestamp{};

    if (::clock_gettime(clock_id, &timestamp) != 0) {
        const int error = errno;
        throw std::system_error(error, std::generic_category(), "reference clock_gettime");
    }

    return timestamp;
}

bool is_between(std::int64_t observed_ns, const timespec& before, const timespec& after) {
    if (observed_ns < 0) {
        return false;
    }

    constexpr std::int64_t scale = 1'000'000'000;
    const auto seconds = observed_ns / scale;
    const auto nanoseconds = observed_ns % scale;

    const bool not_before =
        seconds > before.tv_sec || (seconds == before.tv_sec && nanoseconds >= before.tv_nsec);

    const bool not_after =
        seconds < after.tv_sec || (seconds == after.tv_sec && nanoseconds <= after.tv_nsec);

    return not_before && not_after;
}

void test_clock_capture() {
    const auto monotonic_before = read_reference_clock(CLOCK_MONOTONIC);
    const auto cpu_before = read_reference_clock(CLOCK_PROCESS_CPUTIME_ID);

    const auto observed = udp_ingestion::capture_run_timing();

    const auto cpu_after = read_reference_clock(CLOCK_PROCESS_CPUTIME_ID);
    const auto monotonic_after = read_reference_clock(CLOCK_MONOTONIC);

    expect(is_between(observed.monotonic_ns, monotonic_before, monotonic_after),
           "monotonic snapshot matches surrounding reference reads");
    expect(is_between(observed.process_cpu_ns, cpu_before, cpu_after),
           "CPU snapshot matches surrounding process CPU clock reads");
}

} // namespace

int main() {
    try {
        test_timing_values();
        test_large_values();
        test_invalid_inputs();
        test_clock_capture();
    } catch (const std::exception& error) {
        std::cerr << "test error: " << error.what() << '\n';
        return 1;
    }

    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return 1;
    }

    std::cout << "all run timing tests passed\n";
    return 0;
}