#include "udp_ingestion/libevent_receive_worker.hpp"
#include "udp_ingestion/pipeline.hpp"
#include "udp_ingestion/stop_event.hpp"
#include "udp_ingestion/synthetic_message.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <exception>
#include <iostream>
#include <thread>
#include <cerrno>
#include <cstdint>
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

void test_stop_before_run() {
    udp_ingestion::LibeventReceiveWorkerConfig config;
    config.base_port = 29100;
    config.expected_total_packets = 10;
    config.idle_timeout = std::chrono::milliseconds{30000};

    udp_ingestion::Pipeline pipeline(16);
    udp_ingestion::StopEvent stop_event;
    udp_ingestion::LibeventReceiveWorker worker(config, stop_event);

    worker.apply_cpu_affinity();
    stop_event.notify();

    const auto results = worker.run(pipeline);

    for (std::size_t i = 0; i < results.size(); ++i) {
        const auto& result = results[i];
        auto& channel = pipeline.channels[i];

        expect(result.port == config.base_port + i,
               "result contains the channel port");
        expect(result.expected_packets == 5,
               "result preserves the assigned target");
        expect(result.result.stop_reason ==
                   udp_ingestion::ReceiveStopReason::StopRequested,
               "pending stop event terminates each active channel");
        expect(result.result.state.received_packets == 0,
               "stop without traffic receives no packets");
        expect(channel.producer_done(),
               "stop signals producer completion");

        const auto producer = channel.producer_stats();
        expect(producer.enqueued_packets == 0,
               "stop without traffic enqueues no packets");
        expect(producer.queue_full_drops == 0,
               "stop without traffic drops no packets");

        udp_ingestion::PipelineEvent event{};
        expect(!channel.try_dequeue(event),
               "queue remains empty after stop");
    }
}

void test_stop_from_another_thread() {
    udp_ingestion::LibeventReceiveWorkerConfig config;
    config.base_port = 29100;
    config.expected_total_packets = 10;
    config.idle_timeout = std::chrono::milliseconds{30000};

    udp_ingestion::Pipeline pipeline(16);
    udp_ingestion::StopEvent stop_event;
    udp_ingestion::LibeventReceiveWorker worker(config, stop_event);

    worker.apply_cpu_affinity();

    std::exception_ptr notifier_error;
    std::thread notifier([&]() {
        try {
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
            stop_event.notify();
        } catch (...) {
            notifier_error = std::current_exception();
        }
    });

    udp_ingestion::LibeventReceiveResult results{};

    try {
        results = worker.run(pipeline);
    } catch (...) {
        notifier.join();
        throw;
    }

    notifier.join();

    if (notifier_error) {
        std::rethrow_exception(notifier_error);
    }

    for (std::size_t i = 0; i < results.size(); ++i) {
        expect(results[i].result.stop_reason ==
                   udp_ingestion::ReceiveStopReason::StopRequested,
               "asynchronous stop terminates each active channel");
        expect(results[i].result.state.received_packets == 0,
               "asynchronous stop requires no UDP traffic");
        expect(pipeline.channels[i].producer_done(),
               "asynchronous stop signals producer completion");

        udp_ingestion::PipelineEvent event{};
        expect(!pipeline.channels[i].try_dequeue(event),
               "queue remains empty after asynchronous stop");
    }
}

void test_receive_beyond_service_budget(std::size_t queue_capacity) {
    constexpr std::uint64_t packets_per_channel = 65;

    const std::uint64_t expected_enqueued =
        queue_capacity < packets_per_channel
            ? queue_capacity
            : packets_per_channel;

    udp_ingestion::LibeventReceiveWorkerConfig config;
    config.base_port = 29100;
    config.expected_total_packets =
        packets_per_channel * udp_ingestion::kLibeventChannelCount;
    config.idle_timeout = std::chrono::milliseconds{1000};

    // Drain after run(): each queue must hold all 65 messages.
    udp_ingestion::Pipeline pipeline(queue_capacity);
    udp_ingestion::StopEvent stop_event;
    udp_ingestion::LibeventReceiveWorker worker(config, stop_event);

    const int sender_fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (sender_fd < 0) {
        throw std::system_error(
            errno, std::generic_category(), "test sender socket");
    }

    try {
        for (std::size_t i = 0;
             i < udp_ingestion::kLibeventChannelCount; ++i) {
            sockaddr_in destination{};
            destination.sin_family = AF_INET;
            destination.sin_port =
                htons(static_cast<std::uint16_t>(config.base_port + i));
            destination.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

            for (std::uint64_t sequence = 1;
                 sequence <= packets_per_channel; ++sequence) {
                const auto message =
                    udp_ingestion::make_synthetic_message(i, sequence, 16);
                const auto buffer =
                    udp_ingestion::encode_protocol_v1(message);

                const auto sent = ::sendto(
                    sender_fd, buffer.data(), buffer.size(), 0,
                    reinterpret_cast<const sockaddr*>(&destination),
                    sizeof(destination));

                if (sent < 0) {
                    throw std::system_error(
                        errno, std::generic_category(), "test sendto");
                }

                if (static_cast<std::size_t>(sent) != buffer.size()) {
                    throw std::runtime_error(
                        "test sendto returned unexpected size");
                }
            }
        }
    } catch (...) {
        ::close(sender_fd);
        throw;
    }

    ::close(sender_fd);

    worker.apply_cpu_affinity();
    const auto results = worker.run(pipeline);

    for (std::size_t i = 0; i < results.size(); ++i) {
        const auto& result = results[i];
        const auto& state = result.result.state;
        auto& channel = pipeline.channels[i];

        expect(result.result.stop_reason ==
                   udp_ingestion::ReceiveStopReason::CountReached,
               "receive continues beyond one callback budget");
        expect(result.expected_packets == packets_per_channel,
               "channel target is 65 packets");
        expect(state.received_packets == packets_per_channel,
               "all queued datagrams are received");
        expect(state.valid_packets == packets_per_channel,
               "all received datagrams are valid");
        expect(state.invalid_packets == 0,
               "no invalid datagrams");
        expect(state.gap_events == 0 && state.missing_packets == 0,
               "no sequence gaps");
        expect(state.late_or_duplicate_packets == 0,
               "no late or duplicate packets");
        expect(state.expected_sequence == packets_per_channel + 1,
               "sequence tracking reaches the final packet");
        expect(channel.producer_done(),
               "completed channel signals producer completion");

        const auto producer = channel.producer_stats();
        expect(producer.enqueued_packets == expected_enqueued,
               "enqueue count matches available queue capacity");
        expect(producer.queue_full_drops ==
                   packets_per_channel - expected_enqueued,
               "messages exceeding queue capacity are counted as drops");
        expect(state.valid_packets ==
                   producer.enqueued_packets + producer.queue_full_drops,
               "every valid packet is either enqueued or dropped");

        udp_ingestion::PipelineEvent event{};
        std::uint64_t drained = 0;

        while (channel.try_dequeue(event)) {
            ++drained;

            const auto expected =
                udp_ingestion::make_synthetic_message(i, drained, 16);

            expect(
                udp_ingestion::encode_protocol_v1(event.message) ==
                    udp_ingestion::encode_protocol_v1(expected),
                "queued message content and FIFO order match");
            expect(event.rx_timestamp_ns > 0,
                   "queued message contains an RX timestamp");
        }

        expect(drained == expected_enqueued,
               "all accepted messages can be drained");
    }
}

void test_completed_channel_and_idle_channel() {
    udp_ingestion::LibeventReceiveWorkerConfig config;
    config.base_port = 29100;
    config.expected_total_packets = 2;
    config.idle_timeout = std::chrono::milliseconds{100};

    udp_ingestion::Pipeline pipeline(16);
    udp_ingestion::StopEvent stop_event;
    udp_ingestion::LibeventReceiveWorker worker(config, stop_event);

    const auto message =
        udp_ingestion::make_synthetic_message(0, 1, 16);
    const auto buffer = udp_ingestion::encode_protocol_v1(message);

    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port = htons(config.base_port);
    destination.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    const int sender_fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (sender_fd < 0) {
        throw std::system_error(
            errno, std::generic_category(), "test sender socket");
    }

    const auto sent = ::sendto(
        sender_fd, buffer.data(), buffer.size(), 0,
        reinterpret_cast<const sockaddr*>(&destination),
        sizeof(destination));
    const int send_error = errno;
    ::close(sender_fd);

    if (sent < 0) {
        throw std::system_error(
            send_error, std::generic_category(), "test sendto");
    }

    if (static_cast<std::size_t>(sent) != buffer.size()) {
        throw std::runtime_error(
            "test sendto returned unexpected size");
    }

    worker.apply_cpu_affinity();
    const auto results = worker.run(pipeline);

    expect(results[0].result.stop_reason ==
               udp_ingestion::ReceiveStopReason::CountReached,
           "completed channel retains count_reached");
    expect(results[0].result.state.received_packets == 1,
           "channel zero receives its packet");
    expect(results[0].result.state.valid_packets == 1,
           "channel zero receives a valid packet");

    expect(results[1].result.stop_reason ==
               udp_ingestion::ReceiveStopReason::IdleTimeout,
           "remaining channel terminates on idle timeout");
    expect(results[1].result.state.received_packets == 0,
           "idle channel receives no packets");

    for (std::size_t i = 0; i < results.size(); ++i) {
        expect(results[i].expected_packets == 1,
               "each channel has a target of one");
        expect(pipeline.channels[i].producer_done(),
               "both completion paths signal producer completion");

        const auto producer = pipeline.channels[i].producer_stats();
        expect(producer.enqueued_packets == (i == 0 ? 1u : 0u),
               "only channel zero enqueues a message");
        expect(producer.queue_full_drops == 0,
               "neither channel drops a queued message");
    }

    udp_ingestion::PipelineEvent event{};
    const bool dequeued = pipeline.channels[0].try_dequeue(event);
    expect(dequeued, "completed channel remains drainable");

    if (dequeued) {
        expect(udp_ingestion::encode_protocol_v1(event.message) == buffer,
               "drained message matches the sent message");
        expect(event.rx_timestamp_ns > 0,
               "drained message contains an RX timestamp");
    }

    expect(!pipeline.channels[0].try_dequeue(event),
           "completed channel has no extra messages");
    expect(!pipeline.channels[1].try_dequeue(event),
           "idle channel queue remains empty");
}

void test_invalid_datagram_counts_toward_target() {
    udp_ingestion::LibeventReceiveWorkerConfig config;
    config.base_port = 29100;
    config.expected_total_packets = 1;
    config.idle_timeout = std::chrono::milliseconds{1000};

    udp_ingestion::Pipeline pipeline(16);
    udp_ingestion::StopEvent stop_event;
    udp_ingestion::LibeventReceiveWorker worker(config, stop_event);

    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port = htons(config.base_port);
    destination.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    const int sender_fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (sender_fd < 0) {
        throw std::system_error(
            errno, std::generic_category(), "test sender socket");
    }

    const char payload = 0;
    const auto sent = ::sendto(
        sender_fd, &payload, 0, 0,
        reinterpret_cast<const sockaddr*>(&destination),
        sizeof(destination));
    const int send_error = errno;
    ::close(sender_fd);

    if (sent < 0) {
        throw std::system_error(
            send_error, std::generic_category(), "test sendto");
    }

    if (sent != 0) {
        throw std::runtime_error(
            "empty datagram send returned unexpected size");
    }

    worker.apply_cpu_affinity();
    const auto results = worker.run(pipeline);

    const auto& state = results[0].result.state;

    expect(results[0].expected_packets == 1,
           "channel zero expects one datagram");
    expect(state.received_packets == 1,
           "empty UDP datagram counts as received");
    expect(state.invalid_packets == 1,
           "empty UDP datagram is invalid protocol input");
    expect(state.valid_packets == 0,
           "empty datagram is not a valid message");
    expect(state.expected_sequence == 1,
           "invalid input does not advance sequence tracking");
    expect(state.gap_events == 0 && state.missing_packets == 0,
           "invalid input does not create a sequence gap");

    expect(results[1].expected_packets == 0,
           "channel one has no receive target");
    expect(results[1].result.state.received_packets == 0,
           "channel one receives no datagrams");

    for (std::size_t i = 0; i < results.size(); ++i) {
        expect(results[i].result.stop_reason ==
                   udp_ingestion::ReceiveStopReason::CountReached,
               "datagram targets complete without idle timeout");
        expect(pipeline.channels[i].producer_done(),
               "both channels signal producer completion");

        const auto producer = pipeline.channels[i].producer_stats();
        expect(producer.enqueued_packets == 0,
               "invalid input is not enqueued");
        expect(producer.queue_full_drops == 0,
               "invalid input is not counted as a queue drop");

        udp_ingestion::PipelineEvent event{};
        expect(!pipeline.channels[i].try_dequeue(event),
               "pipeline contains no messages");
    }
}

} // namespace

int main() {
    try {
        test_stop_before_run();
        test_stop_from_another_thread();
        test_receive_beyond_service_budget(128);
        test_receive_beyond_service_budget(1);
        test_completed_channel_and_idle_channel();
        test_invalid_datagram_counts_toward_target();
    } catch (const std::exception& error) {
        std::cerr << "FAIL: unexpected exception: " << error.what() << '\n';
        return 1;
    }

    if (failures != 0) {
        return 1;
    }

    std::cout << "libevent_receive_worker_test passed\n";
    return 0;
}