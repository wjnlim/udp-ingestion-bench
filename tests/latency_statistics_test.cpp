#include "udp_ingestion/latency.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>

namespace {

int failures = 0;

void expect(bool condition, const char* description) {
    if (!condition) {
        std::cerr << "FAIL: " << description << '\n';
        ++failures;
    }
}

void test_empty_samples() {
    udp_ingestion::LatencySamples empty(0, 0);
    expect(!udp_ingestion::calculate_latency_statistics(empty).has_value(),
           "zero-capacity storage has no statistics");

    udp_ingestion::LatencySamples unused(8, 0);
    expect(!udp_ingestion::calculate_latency_statistics(unused).has_value(),
           "unused capacity does not count as samples");

    udp_ingestion::LatencySamples warmup_only(2, 2);
    warmup_only.record_processed(1, 100, 110);
    warmup_only.record_processed(2, 100, 120);

    expect(!udp_ingestion::calculate_latency_statistics(warmup_only).has_value(),
           "warmup-only storage has no statistics");
}

void test_single_sample() {
    udp_ingestion::LatencySamples samples(1, 0);
    samples.record_processed(1, 100, 100);

    const auto statistics = udp_ingestion::calculate_latency_statistics(samples);

    expect(statistics.has_value(), "zero latency is a valid measurement");
    if (!statistics) {
        return;
    }

    expect(statistics->sample_count == 1, "single sample is counted");
    expect(statistics->p50_ns == 0, "single-sample p50 is zero");
    expect(statistics->p99_ns == 0, "single-sample p99 is zero");
    expect(statistics->p999_ns == 0, "single-sample p99.9 is zero");
}

void test_sorting_and_source_preservation() {
    // Extra capacity must not contribute zero-filled entries.
    udp_ingestion::LatencySamples samples(16, 0);
    constexpr std::array<std::int64_t, 5> values{40, 10, 50, 20, 30};

    for (std::size_t i = 0; i < values.size(); ++i) {
        samples.record_processed(static_cast<std::uint64_t>(i + 1), 100, 100 + values[i]);
    }

    const auto* storage = samples.data();
    const auto statistics = udp_ingestion::calculate_latency_statistics(samples);

    expect(statistics.has_value(), "nonempty samples produce statistics");
    if (!statistics) {
        return;
    }

    expect(statistics->sample_count == 5, "only recorded samples are counted");
    expect(statistics->p50_ns == 30, "odd-count median uses ceiling rank");
    expect(statistics->p99_ns == 50, "small-sample p99 selects maximum");
    expect(statistics->p999_ns == 50, "small-sample p99.9 selects maximum");

    expect(samples.data() == storage, "source storage is unchanged");
    expect(samples.sample_count() == values.size(), "source sample count is unchanged");

    for (std::size_t i = 0; i < values.size(); ++i) {
        expect(samples.data()[i] == values[i], "source recording order is preserved");
    }
}

void test_percentile_boundaries() {
    // Descending input requires sorting. Values are exactly 1..1000.
    udp_ingestion::LatencySamples samples(1001, 0);

    for (std::uint64_t i = 0; i < 1000; ++i) {
        samples.record_processed(i + 1, 0, static_cast<std::int64_t>(1000 - i));
    }

    const auto exact = udp_ingestion::calculate_latency_statistics(samples);

    expect(exact.has_value(), "1000 samples produce statistics");
    if (!exact) {
        return;
    }

    expect(exact->sample_count == 1000, "1000 samples are counted");
    expect(exact->p50_ns == 500, "p50 uses rank 500, not interpolation");
    expect(exact->p99_ns == 990, "p99 uses rank 990");
    expect(exact->p999_ns == 999, "p99.9 uses rank 999");

    samples.record_processed(1001, 0, 1001);

    const auto rounded = udp_ingestion::calculate_latency_statistics(samples);

    expect(rounded.has_value(), "1001 samples produce statistics");
    if (!rounded) {
        return;
    }

    expect(rounded->sample_count == 1001, "1001 samples are counted");
    expect(rounded->p50_ns == 501, "p50 fractional rank rounds upward");
    expect(rounded->p99_ns == 991, "p99 fractional rank rounds upward");
    expect(rounded->p999_ns == 1000, "p99.9 fractional rank rounds upward");
}

void test_warmup_and_duplicate_values() {
    udp_ingestion::LatencySamples samples(6, 2);

    samples.record_processed(1, 0, 10000);
    samples.record_processed(2, 0, 20000);

    for (std::uint64_t i = 3; i <= 6; ++i) {
        samples.record_processed(i, 100, 107);
    }

    const auto statistics = udp_ingestion::calculate_latency_statistics(samples);

    expect(statistics.has_value(), "post-warmup samples produce statistics");
    if (!statistics) {
        return;
    }

    expect(statistics->sample_count == 4, "warmup samples are excluded");
    expect(statistics->p50_ns == 7, "duplicate values retain p50");
    expect(statistics->p99_ns == 7, "warmup outliers do not affect p99");
    expect(statistics->p999_ns == 7, "warmup outliers do not affect p99.9");
    expect(samples.warmup_processed() == 2, "warmup count is unchanged");
}

} // namespace

int main() {
    try {
        test_empty_samples();
        test_single_sample();
        test_sorting_and_source_preservation();
        test_percentile_boundaries();
        test_warmup_and_duplicate_values();
    } catch (const std::exception& error) {
        std::cerr << "test error: " << error.what() << '\n';
        return 1;
    }

    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return 1;
    }

    std::cout << "all latency statistics tests passed\n";
    return 0;
}
