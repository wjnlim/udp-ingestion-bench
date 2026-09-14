#include "udp_ingestion/epoll_receive_worker.hpp"
#include "udp_ingestion/synthetic_message.hpp"
#include "udp_ingestion/pipeline.hpp"
#include "udp_ingestion/stop_event.hpp"

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
#include <vector>
#include <thread>

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
        std::chrono::milliseconds timeout = std::chrono::milliseconds{1000},
        std::size_t queue_capacity = 256)
        : pipeline(queue_capacity), worker(make_config(total, timeout), stop_event) {
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

        // Reference application-level processing, without the socket/queue path.
        // udp_ingestion::process_datagram(data, size, expected[channel]);
        udp_ingestion::MarketDataMessage message{};
        if (udp_ingestion::decode_and_track_datagram(
                data, size, message, expected[channel])) {
            expected_messages[channel].push_back(message);
            udp_ingestion::process_downstream_message(
                message, expected_downstream[channel]);
        }
    }

    EpollReceiveResult run() {
        worker.apply_cpu_affinity();
        return worker.run(pipeline);
    }

    void send_message(std::size_t channel, std::uint64_t sequence) {
        const auto message =
            udp_ingestion::make_synthetic_message(channel, sequence, 16);
        const auto buffer = udp_ingestion::encode_protocol_v1(message);
        send_datagram(channel, buffer.data(), buffer.size());
    }

    udp_ingestion::Pipeline pipeline;
    udp_ingestion::StopEvent stop_event;
    EpollReceiveWorker worker;

    std::array<ChannelState, kChannelCount> expected{};
    std::array<udp_ingestion::DownstreamState, kChannelCount>
                                                expected_downstream{};
    std::array<std::vector<udp_ingestion::MarketDataMessage>, kChannelCount>
                                                expected_messages{};
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
    // expect(actual.checksum == expected.checksum,
    //        "checksum matches direct processing");
}

void expect_results(
    Fixture& fixture,
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

        auto& channel = fixture.pipeline.channels[i];

        expect(channel.producer_done(),
               "producer signals completion before run returns");

        const auto producer = channel.producer_stats();

        expect(producer.enqueued_packets ==
                   fixture.expected[i].valid_packets,
               "every valid datagram is enqueued");
        expect(producer.queue_full_drops == 0,
               "existing scenarios do not overflow the queue");

        udp_ingestion::DownstreamState processed{};
        udp_ingestion::PipelineEvent event{};
        std::size_t message_idx = 0;

        while (channel.try_dequeue(event)) {
            expect(event.rx_timestamp_ns > 0,
                   "queued message contains a kernel RX timestamp");

            if (message_idx < fixture.expected_messages[i].size()) {
                const auto actual =
                    udp_ingestion::encode_protocol_v1(event.message);
                const auto expected =
                    udp_ingestion::encode_protocol_v1(
                        fixture.expected_messages[i][message_idx]);

                expect(actual == expected,
                       "queued message content and FIFO order match");
            } else {
                expect(false, "queue contains an unexpected extra message");
            }

            udp_ingestion::process_downstream_message(
                event.message, processed);
            ++message_idx;
        }

        expect(message_idx == fixture.expected_messages[i].size(),
               "queue contains exactly the expected valid messages");
        expect(processed.processed_packets == producer.enqueued_packets,
               "draining processes every enqueued message");
        expect(processed.processed_packets ==
                   fixture.expected_downstream[i].processed_packets,
               "processed count matches direct processing");
        expect(processed.checksum ==
                   fixture.expected_downstream[i].checksum,
               "downstream checksum matches direct processing");
    }
}

void test_normal_count(std::uint64_t total) {
    Fixture fixture(total);

    for (std::uint64_t i = 0; i < total; ++i) {
        const auto channel = static_cast<std::size_t>(i % kChannelCount);
        const auto sequence = i / kChannelCount + 1;
        fixture.send_message(channel, sequence);
    }

    const auto results = fixture.run();
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

    const auto results = fixture.run();
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
    const auto results = fixture.run();
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

    const auto results = fixture.run();
    expect_results(fixture, results, 4);

    expect(results[0].result.stop_reason == ReceiveStopReason::CountReached,
           "completed channel retains count completion");
    expect(results[1].result.stop_reason == ReceiveStopReason::IdleTimeout,
           "partial peer terminates through idle timeout");
}

void test_queue_full_drops_preserve_rx_tracking() {
    Fixture fixture(8, std::chrono::milliseconds{1000}, 1);

    for (std::uint64_t i = 1; i <= 4; ++i) {
        fixture.send_message(0, i);
        fixture.send_message(1, i);
    }

    const auto results = fixture.run();

    for (std::size_t i = 0; i < kChannelCount; ++i) {
        const auto& result = results[i].result;
        auto& channel = fixture.pipeline.channels[i];
        const auto producer = channel.producer_stats();

        expect_state(result.state, fixture.expected[i]);

        expect(result.stop_reason == ReceiveStopReason::CountReached,
               "queue full does not prevent reaching the receive target");
        expect(result.state.valid_packets == 4,
               "all four valid datagrams are tracked before queue admission");
        expect(result.state.expected_sequence == 5,
               "RX sequence tracking advances across queue drops");
        expect(result.state.gap_events == 0 &&
                   result.state.missing_packets == 0,
               "queue drops do not become RX sequence gaps");

        expect(channel.producer_done(),
               "producer completes even when its queue is full");
        expect(producer.enqueued_packets == 1,
               "capacity-one queue accepts exactly one message");
        expect(producer.queue_full_drops == 3,
               "remaining messages are counted as queue-full drops");
        expect(result.state.valid_packets ==
                   producer.enqueued_packets + producer.queue_full_drops,
               "valid packets equal enqueued packets plus queue drops");

        udp_ingestion::PipelineEvent event{};
        const bool dequeued = channel.try_dequeue(event);

        expect(dequeued, "the accepted message remains available");

        if (dequeued) {
            expect(event.rx_timestamp_ns > 0,
                   "accepted message retains its RX timestamp when queue is full");
                   
            const auto actual =
                udp_ingestion::encode_protocol_v1(event.message);
            const auto expected =
                udp_ingestion::encode_protocol_v1(
                    fixture.expected_messages[i].front());

            expect(actual == expected,
                   "queue full does not overwrite the first message");
        }

        expect(!channel.try_dequeue(event),
               "dropped messages were not placed in the queue");
    }
}

void test_stop_before_run(std::uint64_t total) {
    Fixture fixture(total);

    fixture.stop_event.notify();

    const auto results = fixture.run();
    expect_results(fixture, results, total);

    for (std::size_t i = 0; i < kChannelCount; ++i) {
        const auto expected_reason =
            results[i].expected_packets == 0
                ? ReceiveStopReason::CountReached
                : ReceiveStopReason::StopRequested;

        expect(results[i].result.stop_reason == expected_reason,
               "stop affects active channels without changing zero-target completion");
        expect(results[i].result.state.received_packets == 0,
               "pre-run stop completes without receiving datagrams");
    }
}

void test_stop_after_peer_completion() {
    Fixture fixture(2, std::chrono::milliseconds{5000});

    // Channel 0 can complete; channel 1 remains silent.
    fixture.send_message(0, 1);

    EpollReceiveResult results{};
    std::exception_ptr receiver_error;
    std::exception_ptr notifier_error;

    std::thread receiver([&]() {
        try {
            results = fixture.run();
        } catch (...) {
            receiver_error = std::current_exception();
        }
    });

    bool peer_completed = false;

    try {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds{2};

        while (std::chrono::steady_clock::now() < deadline) {
            if (fixture.pipeline.channels[0].producer_done()) {
                peer_completed = true;
                break;
            }

            // Test-only waiting; not part of the receiver implementation.
            std::this_thread::yield();
        }

        // Notify even if the completion check timed out, so cleanup proceeds.
        fixture.stop_event.notify();
    } catch (...) {
        notifier_error = std::current_exception();
    }

    // With no further traffic, the idle timeout also bounds normal cleanup
    // if notification fails or the stop event is not handled.
    receiver.join();

    if (receiver_error) {
        std::rethrow_exception(receiver_error);
    }
    if (notifier_error) {
        std::rethrow_exception(notifier_error);
    }

    expect(peer_completed,
           "channel zero completes before the stop notification");

    expect_results(fixture, results, 2);

    expect(results[0].result.stop_reason == ReceiveStopReason::CountReached,
           "stop preserves the completed channel's reason");
    expect(results[1].result.stop_reason == ReceiveStopReason::StopRequested,
           "cross-thread stop terminates the remaining active channel");
    expect(results[0].result.state.received_packets == 1,
           "completed channel received its datagram");
    expect(results[1].result.state.received_packets == 0,
           "silent channel stops without receiving a datagram");
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

        test_queue_full_drops_preserve_rx_tracking();
        test_stop_before_run(2);
        test_stop_before_run(1);
        test_stop_after_peer_completion();
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