#include "udp_ingestion/timestamped_receive.hpp"

#include <linux/time_types.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <limits>
#include <stdexcept>

namespace {

int failures = 0;

void expect(bool condition, const char* description) {
    if (!condition) {
        std::cerr << "FAIL: " << description << '\n';
        ++failures;
    }
}

class ControlFixture {
public:
    static constexpr std::size_t kMessageSpace =
        CMSG_SPACE(sizeof(__kernel_timespec));

    ControlFixture() {
        reset();
    }

    ControlFixture(const ControlFixture&) = delete;
    ControlFixture& operator=(const ControlFixture&) = delete;

    void reset() {
        buffer_.fill(0);
        message = {};
        message.msg_control = buffer_.data();
        message.msg_controllen = kMessageSpace;

        auto* cmsg = CMSG_FIRSTHDR(&message);
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type = SO_TIMESTAMPNS_NEW;
        cmsg->cmsg_len = CMSG_LEN(sizeof(__kernel_timespec));

        set_timestamp(1, 25);
    }

    cmsghdr* header() {
        return CMSG_FIRSTHDR(&message);
    }

    void set_timestamp(std::int64_t seconds, std::int64_t nanoseconds) {
        const __kernel_timespec timestamp{seconds, nanoseconds};
        std::memcpy(CMSG_DATA(header()), &timestamp, sizeof(timestamp));
    }

    void duplicate_timestamp() {
        message.msg_controllen = buffer_.size();
        std::memcpy(
            buffer_.data() + kMessageSpace,
            buffer_.data(),
            kMessageSpace);
    }

    msghdr message{};

private:
    alignas(cmsghdr)
        std::array<unsigned char, 2 * kMessageSpace> buffer_{};
};

void expect_rejected(msghdr& message, const char* description) {
    bool rejected = false;

    try {
        static_cast<void>(
            udp_ingestion::extract_rx_timestamp_ns(message));
    } catch (const std::runtime_error&) {
        rejected = true;
    }

    expect(rejected, description);
}

void test_valid_values() {
    ControlFixture fixture;

    expect(
        udp_ingestion::extract_rx_timestamp_ns(fixture.message) ==
            1'000'000'025,
        "seconds and nanoseconds are combined");

    fixture.set_timestamp(0, 0);
    expect(
        udp_ingestion::extract_rx_timestamp_ns(fixture.message) == 0,
        "epoch zero is accepted");

    constexpr std::int64_t scale = 1'000'000'000;
    constexpr auto maximum = std::numeric_limits<std::int64_t>::max();

    fixture.set_timestamp(maximum / scale, maximum % scale);
    expect(
        udp_ingestion::extract_rx_timestamp_ns(fixture.message) == maximum,
        "INT64_MAX nanoseconds is accepted");
}

void test_timestamp_range_errors() {
    constexpr std::int64_t scale = 1'000'000'000;
    constexpr auto maximum = std::numeric_limits<std::int64_t>::max();

    const std::array<__kernel_timespec, 5> invalid_values{{
        {-1, 0},
        {0, -1},
        {0, scale},
        {maximum / scale, maximum % scale + 1},
        {maximum / scale + 1, 0}
    }};

    for (const auto& timestamp : invalid_values) {
        ControlFixture fixture;
        fixture.set_timestamp(timestamp.tv_sec, timestamp.tv_nsec);

        expect_rejected(
            fixture.message,
            "invalid or overflowing timestamp is rejected");
    }
}

void test_truncation_flags() {
    ControlFixture fixture;

    fixture.message.msg_flags = MSG_TRUNC;
    expect(
        udp_ingestion::extract_rx_timestamp_ns(fixture.message) ==
            1'000'000'025,
        "payload truncation does not invalidate intact timestamp metadata");

    fixture.message.msg_flags = MSG_CTRUNC;
    expect_rejected(
        fixture.message,
        "ancillary truncation is rejected");

    fixture.message.msg_flags = MSG_TRUNC | MSG_CTRUNC;
    expect_rejected(
        fixture.message,
        "ancillary truncation is rejected when both flags are set");
}

void test_missing_and_wrong_type() {
    msghdr empty{};
    expect_rejected(empty, "missing control buffer is rejected");

    ControlFixture fixture;
    fixture.message.msg_controllen = CMSG_LEN(0) - 1;
    expect_rejected(
        fixture.message,
        "control buffer shorter than a header is rejected");

    fixture.reset();
    fixture.header()->cmsg_type = SO_TIMESTAMPNS_OLD;
    expect_rejected(
        fixture.message,
        "OLD timestamp is not accepted as NEW");

    fixture.reset();
    fixture.header()->cmsg_level = SOL_SOCKET + 1;
    expect_rejected(
        fixture.message,
        "timestamp with the wrong control level is not accepted");
}

void test_control_lengths_and_duplicates() {
    ControlFixture fixture;

    fixture.header()->cmsg_len = CMSG_LEN(0) - 1;
    expect_rejected(
        fixture.message,
        "message length shorter than its header is rejected");

    fixture.reset();
    fixture.header()->cmsg_len = fixture.message.msg_controllen + 1;
    expect_rejected(
        fixture.message,
        "message length beyond the control buffer is rejected");

    fixture.reset();
    fixture.header()->cmsg_len =
        CMSG_LEN(sizeof(__kernel_timespec)) - 1;
    expect_rejected(
        fixture.message,
        "incorrect timestamp payload size is rejected");

    fixture.reset();
    fixture.duplicate_timestamp();
    expect_rejected(
        fixture.message,
        "duplicate NEW timestamps are rejected");
}

} // namespace

int main() {
    try {
        test_valid_values();
        test_timestamp_range_errors();
        test_truncation_flags();
        test_missing_and_wrong_type();
        test_control_lengths_and_duplicates();
    } catch (const std::exception& error) {
        std::cerr << "test error: " << error.what() << '\n';
        return 1;
    }

    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return 1;
    }

    std::cout << "all timestamp extraction tests passed\n";
    return 0;
}