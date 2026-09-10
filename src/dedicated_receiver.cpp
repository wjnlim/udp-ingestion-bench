#include "udp_ingestion/receive_worker.hpp"
#include "udp_ingestion/cpu_affinity.hpp"

#include <cstdint>
#include <cstdlib>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <sched.h>
#include <optional>

namespace {

constexpr std::size_t kChannelCount = 2;

struct ReceiverConfig {
    std::string bind_address = "127.0.0.1";
    std::uint16_t base_port = 9000;
    std::uint64_t expected_total_packets = 1'000;
    std::chrono::milliseconds idle_timeout{3000};
    // bool help = false;

    std::array<std::optional<int>, kChannelCount> rx_cpus{};
    std::optional<int> main_cpu;
};

void print_usage(const char* program) {
    std::cout 
        << "Usage: " << program
        << " [--bind-address IPv4] [--base-port PORT]"
           " [--count DATAGRAMS] [--idle-timeout-ms MS]"
           " [--rx-cpu0 N] [--rx-cpu1 N] [--main-cpu N]\n"
        << "  --count is the aggregate expected packet count across two channels.\n"
        << "  --idle-timeout-ms must be in [1, 86400000].\n"
        << "  CPU options select Linux logical CPUs and are optional.\n"
        << "  Omitted CPU options preserve inherited affinity.\n"
        << "  Defaults: 127.0.0.1, ports 9000/9001, count 1000,"
           " idle timeout 3000 ms, no explicit affinity.\n";
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

ReceiverConfig parse_argument(int argc, char* argv[]) {
    ReceiverConfig config;
    for (int i = 1; i < argc; ++i) {
        const std::string option = argv[i];
        if (option == "--help") {
            print_usage(argv[0]);
            std::exit(0);
        }

        if (i+1 >= argc) {
            throw std::invalid_argument(option + " requires a value");
        }
        const std::string_view value = argv[++i];

        if (option == "--bind-address") {
            config.bind_address = std::string(value);
        } else if (option == "--base-port") {
            const auto port = parse_unsigned(value, option);
            if (port == 0 || port > 65534) {
                throw std::invalid_argument("--base-port must be in [1, 65534]");
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
        } else if (option == "--rx-cpu0" ||
                   option == "--rx-cpu1" ||
                   option == "--main-cpu") {
            const auto cpu = parse_unsigned(value, option);
            
            if (cpu >= static_cast<std::uint64_t>(CPU_SETSIZE)) {
                throw std::invalid_argument(
                    option + ": CPU " + std::to_string(cpu) +
                    " is outside the supported CPU set");
            }

            const auto logical_cpu = static_cast<int>(cpu);

            if (option == "--rx-cpu0") {
                config.rx_cpus[0] = logical_cpu;
            } else if (option == "--rx-cpu1") {
                config.rx_cpus[1] = logical_cpu;
            } else {
                config.main_cpu = logical_cpu;
            }

        } else {
            throw std::invalid_argument("unknown option: " + option);
        }
    }

    return config;
}

udp_ingestion::ReceiveWorkerConfig make_worker_config (
    const ReceiverConfig& config, std::size_t channel) {
    udp_ingestion::ReceiveWorkerConfig worker_config;

    worker_config.bind_address = config.bind_address;
    worker_config.port = static_cast<std::uint16_t>(config.base_port + channel);
    worker_config.expected_packets = config.expected_total_packets / kChannelCount;

    if (channel == 0) {
        worker_config.expected_packets += config.expected_total_packets % kChannelCount;
    }

    worker_config.idle_timeout = config.idle_timeout;
    worker_config.cpu = config.rx_cpus[channel];

    return worker_config;
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

void print_result(std::size_t channel,
                    const udp_ingestion::ReceiveWorkerConfig& config,
                    const udp_ingestion::ReceiveWorkerResult& result) {
    const auto& state = result.state;

    std::cout
        << "\nchannel=" << channel
        << "\nport=" << config.port
        << "\nstop=" << stop_reason_name(result.stop_reason)
        << "\ntarget=" << config.expected_packets
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

void print_affinity_request(const std::string& role,
                            const std::optional<int>& cpu) {
    std::cout << role << ": ";

    if (cpu.has_value()) {
        std::cout << "requested CPU " << *cpu;
    } else {
        std::cout << "preserve inherited affinity";
    }

    std::cout << '\n';
}

int run_receiver(const ReceiverConfig& config) {
    const std::array<udp_ingestion::ReceiveWorkerConfig, kChannelCount>
        worker_configs{
            make_worker_config(config, 0),
            make_worker_config(config, 1)
        };
    
    std::array<udp_ingestion::ReceiveWorker, kChannelCount> workers {
        udp_ingestion::ReceiveWorker(worker_configs[0]),
        udp_ingestion::ReceiveWorker(worker_configs[1])
    };

    std::atomic<bool> stop_requested{false};

    std::array<udp_ingestion::ReceiveWorkerResult, kChannelCount> results {};
    std::array<std::exception_ptr, kChannelCount> errors{};
    std::array<std::thread, kChannelCount> threads{};

    print_affinity_request("main", config.main_cpu);
    print_affinity_request("RX worker 0", worker_configs[0].cpu);
    print_affinity_request("RX worker 1", worker_configs[1].cpu);

    // sockets are bound. RX threads start
    std::cout 
        << "Bound " << config.bind_address
        << ":" << worker_configs[0].port
        << " and " << config.bind_address
        << ":" << worker_configs[1].port
        << "\nstarting RX worker threads"
        << " (idle timeout " << config.idle_timeout.count() << "ms)."
        << std::endl;

    try {
        for (std::size_t i = 0; i < kChannelCount; ++i) {
            threads[i] = std::thread([&, i](){
                try {
                    results[i] = workers[i].run(stop_requested);
                } catch(...) {
                    errors[i] = std::current_exception();
                    stop_requested.store(true, std::memory_order_relaxed);
                }
            });
        }
        // RX threads have inherited main's original affinity mask.
        // Changing main's affinity now does not change their masks.
        if (config.main_cpu.has_value()) {
            udp_ingestion::pin_current_thread(*config.main_cpu, "main");
        }

    } catch (...) {
        stop_requested.store(true, std::memory_order_relaxed);
        for (auto& thread : threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        throw;
    }
    // threads are started successfully
    for (auto& thread : threads) {
        thread.join();
    }

    bool success = true;

    for (std::size_t i = 0; i < kChannelCount; ++i) {
        if (errors[i]) {
            success = false;

            try {
                std::rethrow_exception(errors[i]);
            } catch (const std::exception& error) {
                std::cerr << "channel=" << i
                          << " error: " << error.what() << '\n';
            } catch (...) {
                std::cerr << "channel=" << i
                          << " error: unknown exception \n";
            }

            continue;
        }

        print_result(i, worker_configs[i], results[i]);
        if (!is_clean_result(results[i])) {
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