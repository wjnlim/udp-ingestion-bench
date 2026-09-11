#include "udp_ingestion/epoll_receive_worker.hpp"
#include "udp_ingestion/synthetic_message.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <system_error>

namespace {

using udp_ingestion::ChannelState;
using udp_ingestion::EpollReceiveResult;
using udp_ingestion::EpollReceiveWorker;
using udp_ingestion::EpollReceiveWorkerConfig;
using udp_ingestion::ReceiveStopReason;

constexpr std::uint16_t kTestBasePort = 29000;
constexpr std::size_t kChannelCount = udp_ingestion::kEpollChannelCount;

int failures = 0;

void expect(bool condition, const char* description) {
    if (!condition) {
        std::cerr << "FAIL: " << description << '\n';
        ++failures;
    }
}

EpollReceiveWorkerConfig make_config(
    std::uint64_t total,
    std::chrono::milliseconds timeout = std::chrono::milliseconds{1000}) {
    EpollReceiveWorkerConfig config;
    config.base_port = kTestBasePort;
    config.expected_total_packets = total;
    config.idle_timeout = timeout;
    return config;
}

class Fixture {
public:
    explicit Fixture(
        std::uint64_t total,
        std::chrono::milliseconds timeout = std::chrono::milliseconds{1000})
        : worker(make_config(total, timeout)) {
        sender_fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (sender_fd_ < 0) {
            const int error = errno;
            throw std::system_error(
                error, std::generic_category(), "test sender socket");
        }
    }

    ~Fixture() {
        if (sender_fd_ >= 0) {
            ::close(sender_fd_);
        }
    }

    Fixture(const Fixture&) = delete;
    Fixture& operator=(const Fixture&) = delete;

    void send_datagram(
        std::size_t channel, const std::uint8_t* data, std::size_t size) {
        if (channel >= kChannelCount) {
            throw std::invalid_argument("invalid test channel");
        }

        sockaddr_in destination{};
        destination.sin_family = AF_INET;
        destination.sin_port =
            htons(static_cast<std::uint16_t>(kTestBasePort + channel));
        destination.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

        const auto sent = ::sendto(
            sender_fd_, data, size, 0,
            reinterpret_cast<const sockaddr*>(&destination),
            sizeof(destination));

        if (sent < 0) {
            const int error = errno;
            throw std::system_error(
                error, std::generic_category(), "test sendto");
        }

        if (static_cast<std::size_t>(sent) != size) {
            throw std::runtime_error("test sendto returned unexpected size");
        }

        // Reference application-level processing, without the socket path.
        udp_ingestion::process_datagram(data, size, expected[channel]);
    }

    void send_message(std::size_t channel, std::uint64_t sequence) {
        const auto message =
            udp_ingestion::make_synthetic_message(channel, sequence, 16);
        const auto buffer = udp_ingestion::encode_protocol_v1(message);
        send_datagram(channel, buffer.data(), buffer.size());
    }

    EpollReceiveWorker worker;
    std::array<ChannelState, kChannelCount> expected{};

private:
    int sender_fd_ = -1;
};

void expect_state(
    const ChannelState& actual, const ChannelState& expected) {
    expect(actual.received_packets == expected.received_packets,
           "received count matches direct processing");
    expect(actual.valid_packets == expected.valid_packets,
           "valid count matches direct processing");
    expect(actual.invalid_packets == expected.invalid_packets,
           "invalid count matches direct processing");
    expect(actual.expected_sequence == expected.expected_sequence,
           "expected sequence matches direct processing");
    expect(actual.sequence_exhausted == expected.sequence_exhausted,
           "sequence exhaustion matches direct processing");
    expect(actual.gap_events == expected.gap_events,
           "gap events match direct processing");
    expect(actual.missing_packets == expected.missing_packets,
           "missing count matches direct processing");
    expect(actual.late_or_duplicate_packets ==
               expected.late_or_duplicate_packets,
           "late/duplicate count matches direct processing");
    expect(actual.checksum == expected.checksum,
           "checksum matches direct processing");
}

void expect_results(
    const Fixture& fixture,
    const EpollReceiveResult& results,
    std::uint64_t total) {
    for (std::size_t i = 0; i < kChannelCount; ++i) {
        auto target = total / kChannelCount;
        if (i == 0) {
            target += total % kChannelCount;
        }

        expect(results[i].port == kTestBasePort + i,
               "result contains the bound channel port");
        expect(results[i].expected_packets == target,
               "result contains the assigned target");
        expect_state(results[i].result.state, fixture.expected[i]);
    }
}

void test_normal_count(std::uint64_t total) {
    Fixture fixture(total);

    for (std::uint64_t i = 0; i < total; ++i) {
        const auto channel = static_cast<std::size_t>(i % kChannelCount);
        const auto sequence = i / kChannelCount + 1;
        fixture.send_message(channel, sequence);
    }

    const auto results = fixture.worker.run();
    expect_results(fixture, results, total);

    for (const auto& result : results) {
        expect(result.result.stop_reason == ReceiveStopReason::CountReached,
               "normal channel reaches its target");
    }
}

void test_invalid_input_and_independent_sequences() {
    Fixture fixture(18);

    const auto message =
        udp_ingestion::make_synthetic_message(0, 1, 16);
    const auto buffer = udp_ingestion::encode_protocol_v1(message);

    // Six invalid datagrams on channel 0.
    fixture.send_datagram(0, buffer.data(), 0);
    fixture.send_datagram(0, buffer.data(), buffer.size() - 1);

    std::array<std::uint8_t, udp_ingestion::kProtocolV1BufSize + 1>
        oversized{};
    fixture.send_datagram(0, oversized.data(), oversized.size());

    std::array<std::uint8_t, 128> much_larger{};
    fixture.send_datagram(0, much_larger.data(), much_larger.size());

    auto bad_type = buffer;
    bad_type[32] = 4;
    fixture.send_datagram(0, bad_type.data(), bad_type.size());

    auto bad_side = buffer;
    bad_side[33] = 2;
    fixture.send_datagram(0, bad_side.data(), bad_side.size());

    fixture.send_message(0, 1);
    fixture.send_message(0, 4);
    fixture.send_message(0, 3);

    for (std::uint64_t i = 1; i <= 9; ++i) {
        fixture.send_message(1, i);
    }

    const auto results = fixture.worker.run();
    expect_results(fixture, results, 18);

    for (const auto& result : results) {
        expect(result.result.stop_reason == ReceiveStopReason::CountReached,
               "invalid datagrams also count toward the target");
    }

    const auto& state0 = results[0].result.state;
    expect(state0.invalid_packets == 6, "six invalid datagrams received");
    expect(state0.gap_events == 1, "channel zero observes one gap");
    expect(state0.missing_packets == 2, "channel zero observes two missing");
    expect(state0.late_or_duplicate_packets == 1,
           "channel zero observes one late packet");

    const auto& state1 = results[1].result.state;
    expect(state1.valid_packets == 9, "channel one receives nine valid packets");
    expect(state1.gap_events == 0 &&
               state1.late_or_duplicate_packets == 0,
           "channel one sequence state remains independent");
}

void test_initial_timeout() {
    Fixture fixture(2, std::chrono::milliseconds{100});

    const auto start = std::chrono::steady_clock::now();
    const auto results = fixture.worker.run();
    const auto elapsed = std::chrono::steady_clock::now() - start;

    expect_results(fixture, results, 2);

    for (const auto& result : results) {
        expect(result.result.stop_reason == ReceiveStopReason::IdleTimeout,
               "silent channel reaches idle timeout");
    }

    expect(elapsed >= std::chrono::milliseconds{100},
           "initial timeout does not finish early");
}

void test_completed_channel_and_partial_peer() {
    Fixture fixture(4, std::chrono::milliseconds{100});

    fixture.send_message(0, 1);
    fixture.send_message(0, 2);
    fixture.send_message(1, 1);

    const auto results = fixture.worker.run();
    expect_results(fixture, results, 4);

    expect(results[0].result.stop_reason == ReceiveStopReason::CountReached,
           "completed channel retains count completion");
    expect(results[1].result.stop_reason == ReceiveStopReason::IdleTimeout,
           "partial peer terminates through idle timeout");
}

} // namespace

int main() {
    try {
        // Both sockets are ready, with more than the current 64-attempt budget.
        test_normal_count(140);
        test_normal_count(21);
        test_normal_count(1);
        test_invalid_input_and_independent_sequences();
        test_initial_timeout();
        test_completed_channel_and_partial_peer();
    } catch (const std::exception& error) {
        std::cerr << "test error: " << error.what() << '\n';
        return 1;
    }

    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return 1;
    }

    std::cout << "all epoll receive worker tests passed\n";
    return 0;
}