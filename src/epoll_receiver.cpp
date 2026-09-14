#include "udp_ingestion/epoll_receive_worker.hpp"
#include "udp_ingestion/pipeline.hpp"
#include "udp_ingestion/downstream_worker.hpp"
#include "udp_ingestion/startup_gate.hpp"
#include "udp_ingestion/stop_event.hpp"

#include <sched.h>

#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <exception>
#include <cstddef>
#include <limits>
#include <optional>
#include <thread>

namespace {

struct ReceiverConfig {
    udp_ingestion::EpollReceiveWorkerConfig rx;
    std::size_t queue_capacity = 4096;
    std::optional<int> downstream_cpu;
};

void print_usage(const char* program) {
    const ReceiverConfig defaults{};
    const auto& rx = defaults.rx;

    std::cout
        << "Usage: " << program
        << " [--bind-address IPv4] [--base-port PORT]"
           " [--count DATAGRAMS] [--idle-timeout-ms MS]"
           " [--queue-capacity N]"
           " [--rx-cpu N] [--downstream-cpu N]\n"
        << "  One event-loop execution context handles both UDP channels.\n"
        << "  --count is the aggregate expected datagram count.\n"
        << "  --idle-timeout-ms must be in [1, 86400000].\n"
        << "  --queue-capacity is the slot count per channel;"
           " it must be a positive power of two.\n"
        << "  --rx-cpu selects a Linux logical CPU for the event loop.\n"
        << "  --downstream-cpu selects a Linux logical CPU for downstream.\n"
        << "  Omitted CPU options preserve inherited affinity.\n"
        << "  Defaults: " << rx.bind_address
        << ", ports " << rx.base_port
        << "/" << rx.base_port + 1
        << ", count " << rx.expected_total_packets
        << ", idle timeout " << rx.idle_timeout.count()
        << " ms, queue capacity " << defaults.queue_capacity
        << " per channel, no explicit affinity.\n";
}

std::uint64_t parse_unsigned(std::string_view val_str, const std::string& option) {
    std::uint64_t value = 0;
    const auto result =
        std::from_chars(val_str.data(), val_str.data() + val_str.size(), value);
    
    if (val_str.empty() || 
        result.ec != std::errc{} ||
        result.ptr != val_str.data() + val_str.size()) {
        throw std::invalid_argument(option + " requires an unsigned decimal integer");
    }

    return value;
}

ReceiverConfig parse_argument (int argc, char* argv[]) {
    // udp_ingestion::EpollReceiveWorkerConfig config;
    ReceiverConfig options{};
    auto& config = options.rx;

    for (int i = 1; i < argc; ++i) {
        const std::string option = argv[i];

        if (option == "--help") {
            print_usage(argv[0]);
            std::exit(0);
        }

        if (i + 1 >= argc) {
            throw std::invalid_argument(option + " requires a value");
        }

        const std::string_view value = argv[++i];

        if (option == "--bind-address") {
            config.bind_address = std::string(value);
        } else if (option == "--base-port") {
            const auto port = parse_unsigned(value, option);

            if (port == 0 || port > 65534) {
                throw std::invalid_argument(
                    "--base-port must be in [1, 65534]");
            }

            config.base_port = static_cast<std::uint16_t>(port);
        } else if (option == "--count") {
            config.expected_total_packets = parse_unsigned(value, option);

            if (config.expected_total_packets == 0) {
                throw std::invalid_argument("--count must be positive");
            }
        } else if (option == "--idle-timeout-ms") {
            const auto timeout = parse_unsigned(value, option);

            if (timeout == 0 || timeout > 86'400'000) {
                throw std::invalid_argument(
                    "--idle-timeout-ms must be in [1, 86400000]");
            }

            config.idle_timeout = std::chrono::milliseconds{
                static_cast<std::chrono::milliseconds::rep>(timeout)};
        } else if (option == "--queue-capacity") {
            const auto capacity = parse_unsigned(value, option);

            if (capacity == 0 || (capacity & (capacity - 1)) != 0) {
                throw std::invalid_argument(
                    "--queue-capacity must be a positive power of two");
            }

            const auto maximum =
                std::numeric_limits<std::size_t>::max()
                / sizeof(udp_ingestion::PipelineEvent);

            if (capacity > maximum) {
                throw std::invalid_argument(
                    "--queue-capacity storage size is too large");
            }

            options.queue_capacity = static_cast<std::size_t>(capacity);
        } else if (option == "--rx-cpu" ||
                   option == "--downstream-cpu") {
            const auto cpu = parse_unsigned(value, option);

            if (cpu >= static_cast<std::uint64_t>(CPU_SETSIZE)) {
                throw std::invalid_argument(
                    option + ": CPU " + std::to_string(cpu) +
                    " is outside the supported CPU set");
            }

            const auto logical_cpu = static_cast<int>(cpu);

            if (option == "--rx-cpu") {
                config.cpu = logical_cpu;
            } else {
                options.downstream_cpu = logical_cpu;
            }
        } else {
            throw std::invalid_argument("unknown option: " + option);
        }
    }

    return options;
}

const char* stop_reason_name(udp_ingestion::ReceiveStopReason reason) {
    switch (reason) {
    case udp_ingestion::ReceiveStopReason::CountReached:
        return "count_reached";
    case udp_ingestion::ReceiveStopReason::IdleTimeout:
        return "idle_timeout";
    case udp_ingestion::ReceiveStopReason::StopRequested:
        return "stop_requested";
    }

    return "unknown";
}

void print_result(
    std::size_t channel,
    const udp_ingestion::EpollChannelResult& result,
    const udp_ingestion::ProducerStats& producer,
    const udp_ingestion::DownstreamState& downstream) {
    const auto& state = result.result.state;

    std::cout
        << "\nchannel=" << channel
        << "\nport=" << result.port
        << "\nstop=" << stop_reason_name(result.result.stop_reason)
        << "\ntarget=" << result.expected_packets
        << "\nreceived=" << state.received_packets
        << "\nvalid=" << state.valid_packets
        << "\ninvalid=" << state.invalid_packets
        << "\ngap_events=" << state.gap_events
        << "\nmissing=" << state.missing_packets
        << "\nlate_or_duplicate=" << state.late_or_duplicate_packets
        << "\nexpected_sequence=" << state.expected_sequence
        << "\nsequence_exhausted=" << state.sequence_exhausted
        << "\nenqueued=" << producer.enqueued_packets
        << "\nqueue_full_drops=" << producer.queue_full_drops
        << "\nprocessed=" << downstream.processed_packets
        << "\nchecksum=" << downstream.checksum
        << '\n';
}

bool is_clean_result(const udp_ingestion::ReceiveWorkerResult& result) {
    return result.stop_reason == udp_ingestion::ReceiveStopReason::CountReached
        && result.state.invalid_packets == 0
        && result.state.gap_events == 0
        && result.state.late_or_duplicate_packets == 0;
}

int run_receiver(const ReceiverConfig& options) {
    static_assert(
        udp_ingestion::kEpollChannelCount == udp_ingestion::kPipelineChannelCount,
        "Receiver and pipeline channel counts must match");
    
    const auto& config = options.rx;

    udp_ingestion::Pipeline pipeline(options.queue_capacity);
    udp_ingestion::StartupGate startup(1); // only downstream

    udp_ingestion::StopEvent stop_event;
    udp_ingestion::EpollReceiveWorker worker(config, stop_event);

    udp_ingestion::DownstreamWorker downstream(
                                pipeline, options.downstream_cpu);

    udp_ingestion::EpollReceiveResult results{};
    udp_ingestion::DownstreamResult downstream_results{};

    std::exception_ptr coordinator_error;
    std::exception_ptr downstream_error;
    std::thread downstream_thread;

    const auto print_affinity_request = [](
        const std::string& role, const std::optional<int>& cpu) {
        std::cout << role << ": ";

        if (cpu.has_value()) {
            std::cout << "requested CPU " << *cpu;
        } else {
            std::cout << "preserve inherited affinity";
        }

        std::cout << '\n';
    };

    print_affinity_request("epoll RX", config.cpu);
    print_affinity_request("downstream", options.downstream_cpu);

    std::cout
        << "Bound " << config.bind_address
        << ":" << config.base_port
        << " and " << config.bind_address
        << ":" << config.base_port + 1
        << "\nqueue_capacity=" << options.queue_capacity
        << " per channel"
        << "\nidle_timeout_ms=" << config.idle_timeout.count()
        << '\n';
    
    bool startup_released = false;
    // create downstream thread;
    // it sets up cpu affinity and wait for release.
    // when it is ready release it to start its drain loops and
    // start RX loop (worker) in the main thread
    try {
        downstream_thread = std::thread([&](){
            try {
                downstream_results = downstream.run(startup);
            } catch (...) {
                downstream_error = std::current_exception();

                // wake up main RX if it has entered epoll_wait().
                try {
                    stop_event.notify();
                } catch (...) {
                    // If stop mechanism itself failed,
                    // cannot guarantee RX termination; thus terminate
                    std::terminate();
                }
            }
        });

        worker.apply_cpu_affinity();

        if (startup.wait_until_ready()) {
            std::cout << "READY\n" << std::flush;

            if (!std::cout) {
                throw std::runtime_error("failed to write READY");
            }

            startup.release();
            startup_released = true;

            // Main is the RX execution context.
            results = worker.run(pipeline);
        }
    } catch (...) {
        coordinator_error = std::current_exception();
    }

    if (!startup_released) {
        startup.cancel();
    }

    for (auto& channel : pipeline.channels) {
        channel.mark_producer_done();
    }

    if (downstream_thread.joinable()) {
        downstream_thread.join();
    }

    const auto report_error = [](
        const std::string& role, const std::exception_ptr& error) {
        if (!error) {
            return false;
        }

        try {
            std::rethrow_exception(error);
        } catch (const std::exception& exception) {
            std::cerr << role << " error: " << exception.what() << '\n';
        } catch (...) {
            std::cerr << role << " error: unknown exception\n";
        }

        return true;
    };

    bool failed = false;
    /*
        report each thread's error
    */

    if (report_error("epoll RX/main", coordinator_error)) {
        failed = true;
    }

    if (report_error("downstream", downstream_error)) {
        failed = true;
    }

    if (!startup_released) {
        std::cerr << "startup cancelled\n";
        return 1;
    }

    if (failed) {
        return 1;
    }

    bool success = true;
    // report results
    for (std::size_t i = 0; i < udp_ingestion::kEpollChannelCount; ++i) {
        const auto producer = pipeline.channels[i].producer_stats();
        const auto& processed = downstream_results[i];

        print_result(i, results[i], producer, processed);

        const bool accounting_matches =
            results[i].result.state.valid_packets ==
                producer.enqueued_packets + producer.queue_full_drops
            && processed.processed_packets == producer.enqueued_packets;

        if (!accounting_matches) {
            std::cerr << "channel=" << i
                      << " pipeline accounting mismatch\n";
        }

        if (!is_clean_result(results[i].result)
            || producer.queue_full_drops != 0
            || !accounting_matches) {
            success = false;
        }
    }

    return success ? 0 : 1;
}

} // namespace

int main(int argc, char* argv[]) {
    try {
        const auto config = parse_argument(argc, argv);
        return run_receiver(config);
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }
}