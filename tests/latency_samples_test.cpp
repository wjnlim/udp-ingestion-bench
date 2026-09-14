#include "udp_ingestion/latency_samples.hpp"

#include <exception>
#include <iostream>
#include <stdexcept>

namespace {

int failures = 0;

void expect(bool condition, const char* description) {
    if (!condition) {
        std::cerr << "FAIL: " << description << '\n';
        ++failures;
    }
}

void test_recording() {
    udp_ingestion::LatencySamples samples(3, 0);
    const auto* storage = samples.data();

    expect(samples.sample_count() == 0, "initial sample count is zero");
    expect(samples.warmup_processed() == 0, "initial warmup count is zero");

    samples.record_processed(1, 100, 125);
    samples.record_processed(2, 200, 200);
    samples.record_processed(3, 300, 310);

    expect(samples.sample_count() == 3, "three measured events are recorded");
    expect(samples.warmup_processed() == 0, "no events are warmup");
    expect(samples.data() == storage, "recording does not relocate storage");

    if (samples.sample_count() == 3) {
        expect(samples.data()[0] == 25, "first latency is preserved");
        expect(samples.data()[1] == 0, "zero latency is preserved");
        expect(samples.data()[2] == 10, "samples retain recording order");
    }
}

void test_warmup_sequence_boundary() {
    udp_ingestion::LatencySamples samples(6, 2);

    samples.record_processed(1, 100, 110);
    samples.record_processed(3, 200, 230);
    samples.record_processed(2, 300, 310);
    samples.record_processed(1, 400, 410);
    samples.record_processed(3, 500, 550);
    samples.record_processed(4, 600, 660);

    expect(samples.warmup_processed() == 3,
           "late and duplicate warmup sequences remain excluded");
    expect(samples.sample_count() == 3,
           "events beyond the warmup boundary are sampled");
    expect(samples.warmup_processed() + samples.sample_count() == 6,
           "every successful record is classified");

    if (samples.sample_count() == 3) {
        expect(samples.data()[0] == 30, "first measured latency is correct");
        expect(samples.data()[1] == 50,
               "duplicate measured sequence has its own completion sample");
        expect(samples.data()[2] == 60, "last measured latency is correct");
    }
}

void test_capacity_failure_preserves_state() {
    udp_ingestion::LatencySamples samples(1, 0);
    samples.record_processed(1, 100, 120);

    const auto* storage = samples.data();
    bool rejected = false;

    try {
        samples.record_processed(2, 200, 240);
    } catch (const std::runtime_error&) {
        rejected = true;
    }

    expect(rejected, "recording beyond capacity is rejected");
    expect(samples.sample_count() == 1, "capacity failure preserves count");
    expect(samples.warmup_processed() == 0,
           "capacity failure does not change warmup count");
    expect(samples.data() == storage, "capacity failure does not reallocate");

    if (samples.sample_count() == 1) {
        expect(samples.data()[0] == 20,
               "capacity failure preserves the existing sample");
    }
}

void test_invalid_time_preserves_state() {
    udp_ingestion::LatencySamples samples(3, 1);
    samples.record_processed(2, 100, 125);

    bool rejected = false;

    try {
        samples.record_processed(3, 200, 199);
    } catch (const std::runtime_error&) {
        rejected = true;
    }

    expect(rejected, "negative measured latency is rejected");
    expect(samples.sample_count() == 1,
           "invalid measured time does not append a sample");

    rejected = false;

    try {
        samples.record_processed(1, -1, 100);
    } catch (const std::runtime_error&) {
        rejected = true;
    }

    expect(rejected, "warmup does not hide invalid timestamps");
    expect(samples.warmup_processed() == 0,
           "invalid warmup time does not increment its counter");

    samples.record_processed(1, 300, 310);
    samples.record_processed(3, 400, 440);

    expect(samples.warmup_processed() == 1,
           "valid warmup can be recorded after rejection");
    expect(samples.sample_count() == 2,
           "valid measured event can be recorded after rejection");

    if (samples.sample_count() == 2) {
        expect(samples.data()[0] == 25,
               "invalid timestamp preserves the first sample");
        expect(samples.data()[1] == 40,
               "rejected event leaves no hole in sample storage");
    }
}

void test_zero_capacity() {
    udp_ingestion::LatencySamples samples(0, 0);

    expect(samples.sample_count() == 0, "zero-target storage starts empty");
    expect(samples.warmup_processed() == 0, "zero-target warmup starts empty");

    bool rejected = false;

    try {
        samples.record_processed(1, 100, 110);
    } catch (const std::runtime_error&) {
        rejected = true;
    }

    expect(rejected, "zero capacity rejects a measured sample");
    expect(samples.sample_count() == 0, "zero-capacity failure preserves count");

    // Do not dereference data() for an empty storage.
}

void test_invalid_configuration() {
    bool rejected = false;

    try {
        udp_ingestion::LatencySamples samples(1, 2);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }

    expect(rejected, "warmup boundary beyond channel capacity is rejected");
}

} // namespace

int main() {
    try {
        test_recording();
        test_warmup_sequence_boundary();
        test_capacity_failure_preserves_state();
        test_invalid_time_preserves_state();
        test_zero_capacity();
        test_invalid_configuration();
    } catch (const std::exception& error) {
        std::cerr << "test error: " << error.what() << '\n';
        return 1;
    }

    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return 1;
    }

    std::cout << "all latency sample tests passed\n";
    return 0;
}