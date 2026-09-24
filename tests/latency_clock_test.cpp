#include "udp_ingestion/latency.hpp"

#include <time.h>

#include <cerrno>
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

void expect_rejected(std::int64_t rx, std::int64_t completion, const char* description) {
    bool rejected = false;

    try {
        static_cast<void>(udp_ingestion::calculate_latency_ns(rx, completion));
    } catch (const std::runtime_error&) {
        rejected = true;
    }

    expect(rejected, description);
}

void test_latency_values() {
    using udp_ingestion::calculate_latency_ns;

    constexpr auto maximum = std::numeric_limits<std::int64_t>::max();

    expect(calculate_latency_ns(100, 125) == 25, "latency is completion minus reception");
    expect(calculate_latency_ns(0, 0) == 0, "zero timestamps produce zero latency");
    expect(calculate_latency_ns(100, 100) == 0, "equal timestamps produce zero latency");

    expect(calculate_latency_ns(0, maximum) == maximum,
           "maximum representable latency is accepted");
    expect(calculate_latency_ns(maximum - 1, maximum) == 1,
           "subtraction near INT64_MAX is correct");
    expect(calculate_latency_ns(maximum, maximum) == 0, "equal maximum timestamps are accepted");
}

void test_invalid_latency_inputs() {
    constexpr auto minimum = std::numeric_limits<std::int64_t>::min();
    constexpr auto maximum = std::numeric_limits<std::int64_t>::max();

    expect_rejected(-1, 1, "negative RX timestamp is rejected");
    expect_rejected(1, -1, "negative completion timestamp is rejected");
    expect_rejected(-1, -1, "equal negative timestamps are rejected");
    expect_rejected(101, 100, "completion before RX is rejected");
    expect_rejected(maximum, 0, "large backward interval is rejected");
    expect_rejected(minimum, maximum,
                    "potentially overflowing subtraction is rejected before arithmetic");
}

timespec read_reference_clock() {
    timespec timestamp{};

    if (::clock_gettime(CLOCK_REALTIME, &timestamp) != 0) {
        const int error = errno;
        throw std::system_error(error, std::generic_category(),
                                "reference clock_gettime CLOCK_REALTIME");
    }

    return timestamp;
}

void test_realtime_read() {
    constexpr std::int64_t scale = 1'000'000'000;

    const auto before = read_reference_clock();
    const auto observed = udp_ingestion::realtime_now_ns();
    const auto after = read_reference_clock();

    expect(observed > 0, "current epoch nanoseconds are positive");

    // Split instead of multiplying reference seconds, avoiding overflow
    // in the test's reference comparison.
    const auto seconds = observed / scale;
    const auto nanoseconds = observed % scale;

    const bool not_before_reference =
        seconds > before.tv_sec || (seconds == before.tv_sec && nanoseconds >= before.tv_nsec);

    const bool not_after_reference =
        seconds < after.tv_sec || (seconds == after.tv_sec && nanoseconds <= after.tv_nsec);

    expect(not_before_reference && not_after_reference,
           "clock helper matches the surrounding CLOCK_REALTIME reads");
}

} // namespace

int main() {
    try {
        test_latency_values();
        test_invalid_latency_inputs();
        test_realtime_read();
    } catch (const std::exception& error) {
        std::cerr << "test error: " << error.what() << '\n';
        return 1;
    }

    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return 1;
    }

    std::cout << "all latency clock tests passed\n";
    return 0;
}
