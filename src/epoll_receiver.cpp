#include "udp_ingestion/epoll_receive_worker.hpp"

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

namespace {

void print_usage(const char* program) {
    const udp_ingestion::EpollReceiveWorkerConfig defaults{};

    std::cout
        << "Usage: " << program
        << " [--bind-address IPv4] [--base-port PORT]"
           " [--count DATAGRAMS] [--idle-timeout-ms MS]"
           " [--rx-cpu N]\n"
        << "  One event-loop execution context handles both UDP channels.\n"
        << "  --count is the aggregate expected datagram count.\n"
        << "  --idle-timeout-ms must be in [1, 86400000].\n"
        << "  --rx-cpu selects a Linux logical CPU for the event loop.\n"
        << "  Omitted --rx-cpu preserves inherited affinity.\n"
        << "  Defaults: " << defaults.bind_address
        << ", ports " << defaults.base_port
        << "/" << defaults.base_port + 1
        << ", count " << defaults.expected_total_packets
        << ", idle timeout " << defaults.idle_timeout.count()
        << " ms, no explicit affinity.\n";
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

udp_ingestion::EpollReceiveWorkerConfig parse_argument (int argc, char* argv[]) {
    udp_ingestion::EpollReceiveWorkerConfig config;

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
        } else if (option == "--rx-cpu") {
            const auto cpu = parse_unsigned(value, option);

            if (cpu >= static_cast<std::uint64_t>(CPU_SETSIZE)) {
                throw std::invalid_argument(
                    option + ": CPU " + std::to_string(cpu) +
                    " is outside the supported CPU set");
            }

            config.cpu = static_cast<int>(cpu);
        } else {
            throw std::invalid_argument("unknown option: " + option);
        }
    }

    return config;
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
    const udp_ingestion::EpollChannelResult& result) {
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
        << "\nchecksum=" << state.checksum
        << '\n';
}

bool is_clean_result(const udp_ingestion::ReceiveWorkerResult& result) {
    return result.stop_reason == udp_ingestion::ReceiveStopReason::CountReached
        && result.state.invalid_packets == 0
        && result.state.gap_events == 0
        && result.state.late_or_duplicate_packets == 0;
}

int run_receiver(const udp_ingestion::EpollReceiveWorkerConfig& config) {
    udp_ingestion::EpollReceiveWorker worker(config);

    std::cout << "epoll RX: ";
    if (config.cpu.has_value()) {
        std::cout << "requested CPU " << *config.cpu;
    } else {
        std::cout << "preserve inherited affinity";
    }
    std::cout << '\n';

    std::cout
        << "Bound " << config.bind_address
        << ":" << config.base_port
        << " and " << config.bind_address
        << ":" << config.base_port + 1
        << "\nstarting LT epoll loop on the main thread"
        << " (idle timeout " << config.idle_timeout.count() << " ms)."
        << std::endl;

    const auto results = worker.run();

    bool success = true;
    for (std::size_t i = 0; i < udp_ingestion::kEpollChannelCount; ++i) {
        print_result(i, results[i]);

        if (!is_clean_result(results[i].result)) {
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