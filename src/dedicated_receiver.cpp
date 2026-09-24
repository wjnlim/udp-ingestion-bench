#include "udp_ingestion/receiver_reporting.hpp"
#include "udp_ingestion/receive_worker.hpp"
#include "udp_ingestion/cpu_affinity.hpp"
#include "udp_ingestion/pipeline.hpp"
#include "udp_ingestion/downstream_worker.hpp"
#include "udp_ingestion/startup_gate.hpp"
#include "udp_ingestion/latency.hpp"

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
#include <cstddef>
#include <limits>
#include <vector>

namespace {

constexpr std::size_t kChannelCount = 2;

struct ReceiverConfig {
    std::string bind_address = "127.0.0.1";
    std::uint16_t base_port = 9000;
    std::uint64_t expected_total_packets = 1'000;
    std::uint64_t warmup_packets = 0;
    std::chrono::milliseconds idle_timeout{3000};
    std::size_t queue_capacity = 4096;

    std::array<std::optional<int>, kChannelCount> rx_cpus{};
    std::optional<int> main_cpu;
    std::optional<int> downstream_cpu;

    bool rx_pause = false;
};

void print_usage(const char* program) {
    const ReceiverConfig defaults{};
    std::cout
        << "Usage: " << program
        << " [--bind-address IPv4] [--base-port PORT]"
           " [--expected-packets DATAGRAMS] [--warmup-packets DATAGRAMS]"
           " [--idle-timeout-ms MS]"
           " [--queue-capacity N]"
           " [--rx-pause on|off]"
           " [--rx-cpu0 N] [--rx-cpu1 N]"
           " [--main-cpu N] [--downstream-cpu N]\n"
        << "  --expected-packets is the aggregate expected packet count across two channels.\n"
        << "  --warmup-packets excludes the first W published messages from"
           " latency samples; W is included in --expected-packets and must be smaller.\n"
        << "  --idle-timeout-ms must be in [1, 86400000].\n"
        << "  --queue-capacity is the slot count per channel;"
           " it must be a positive power of two.\n"
        << "  --rx-pause applies _mm_pause() on EAGAIN/EWOULDBLOCK"
           " in both RX workers; default: off.\n"
        << "  CPU options select Linux logical CPUs and are optional.\n"
        << "  Omitted CPU options preserve inherited affinity.\n"
        << "  Defaults: " << defaults.bind_address << ", ports " << defaults.base_port << "/"
        << defaults.base_port + 1 << ", expected-packets " << defaults.expected_total_packets
        << ", warmup-packets " << defaults.warmup_packets << ", idle timeout "
        << defaults.idle_timeout.count() << " ms, queue capacity " << defaults.queue_capacity
        << " per channel, no explicit affinity.\n";
}

std::uint64_t parse_unsigned(std::string_view val_str, const std::string& option) {
    std::uint64_t value = 0;
    const auto result = std::from_chars(val_str.data(), val_str.data() + val_str.size(), value);

    if (val_str.empty() || result.ec != std::errc{} ||
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

        if (i + 1 >= argc ||
            std::string_view(argv[i + 1]).substr(0, 2) == "--") {
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
        } else if (option == "--expected-packets") {
            config.expected_total_packets = parse_unsigned(value, option);
            if (config.expected_total_packets == 0) {
                throw std::invalid_argument("--expected-packets must be positive");
            }

        } else if (option == "--warmup-packets") {
            config.warmup_packets = parse_unsigned(value, option);
        } else if (option == "--rx-pause") {
            if (value == "on") {
                config.rx_pause = true;
            } else if (value == "off") {
                config.rx_pause = false;
            } else {
                throw std::invalid_argument("--rx-pause must be on or off");
            }
        } else if (option == "--idle-timeout-ms") {
            const auto timeout = parse_unsigned(value, option);
            if (timeout == 0 || timeout > 86'400'000) {
                throw std::invalid_argument("--idle-timeout-ms must be in [1, 86400000]");
            }
            config.idle_timeout =
                std::chrono::milliseconds{static_cast<std::chrono::milliseconds::rep>(timeout)};
        } else if (option == "--queue-capacity") {
            const auto capacity = parse_unsigned(value, option);

            if (capacity == 0 || (capacity & (capacity - 1)) != 0) {
                throw std::invalid_argument("--queue-capacity must be a positive power of two");
            }

            const auto maximum =
                std::numeric_limits<std::size_t>::max() / sizeof(udp_ingestion::PipelineEvent);
            if (capacity > maximum) {
                throw std::invalid_argument("--queue-capacity storage size is too large");
            }

            config.queue_capacity = static_cast<std::size_t>(capacity);
        } else if (option == "--rx-cpu0" || option == "--rx-cpu1" || option == "--main-cpu" ||
                   option == "--downstream-cpu") {
            const auto cpu = parse_unsigned(value, option);

            if (cpu >= static_cast<std::uint64_t>(CPU_SETSIZE)) {
                throw std::invalid_argument(option + ": CPU " + std::to_string(cpu) +
                                            " is outside the supported CPU set");
            }

            const auto logical_cpu = static_cast<int>(cpu);

            if (option == "--rx-cpu0") {
                config.rx_cpus[0] = logical_cpu;
            } else if (option == "--rx-cpu1") {
                config.rx_cpus[1] = logical_cpu;
            } else if (option == "--main-cpu") {
                config.main_cpu = logical_cpu;
            } else {
                config.downstream_cpu = logical_cpu;
            }

        } else {
            throw std::invalid_argument("unknown option: " + option);
        }
    }

    if (config.warmup_packets >= config.expected_total_packets) {
        throw std::invalid_argument("--warmup-packets must be smaller than --expected-packets");
    }

    return config;
}

udp_ingestion::ReceiveWorkerConfig make_worker_config(const ReceiverConfig& config,
                                                      std::size_t channel) {
    udp_ingestion::ReceiveWorkerConfig worker_config;

    worker_config.bind_address = config.bind_address;
    worker_config.port = static_cast<std::uint16_t>(config.base_port + channel);
    worker_config.expected_packets = config.expected_total_packets / kChannelCount;

    if (channel == 0) {
        worker_config.expected_packets += config.expected_total_packets % kChannelCount;
    }

    worker_config.idle_timeout = config.idle_timeout;
    worker_config.cpu = config.rx_cpus[channel];
    worker_config.rx_pause = config.rx_pause;

    return worker_config;
}

int run_receiver(const ReceiverConfig& config) {
    static_assert(kChannelCount == udp_ingestion::kPipelineChannelCount,
                  "Receiver and pipeline channel counts must match");

    udp_ingestion::Pipeline pipeline(config.queue_capacity);
    udp_ingestion::StartupGate startup(kChannelCount + 1); // 2 RX, 1 downstream

    // lambda for initializing a const container(vector) using a loop
    const std::vector<udp_ingestion::ReceiveWorkerConfig> worker_configs = [&config]() {
        std::vector<udp_ingestion::ReceiveWorkerConfig> wc;
        wc.reserve(kChannelCount);
        for (std::size_t i = 0; i < kChannelCount; ++i) {
            wc.push_back(make_worker_config(config, i));
        }
        return wc;
    }();

    // std::optional for direct-initialization of a type (ReceiveWorker)
    // which does not support copy/move initialization
    std::array<std::optional<udp_ingestion::ReceiveWorker>, kChannelCount> workers{};
    for (std::size_t i = 0; i < kChannelCount; ++i) {
        workers[i].emplace(worker_configs[i]);
    }

    // create storage (buffers) for latency samples
    udp_ingestion::ChannelLatencySamples latency_samples{};
    constexpr auto maximum_capacity =
        std::numeric_limits<std::size_t>::max() / sizeof(std::int64_t);

    for (std::size_t i = 0; i < kChannelCount; ++i) {
        const auto expected_packets = worker_configs[i].expected_packets;

        if (expected_packets > maximum_capacity) {
            throw std::invalid_argument("channel latency sample storage size is too large");
        }

        const auto last_warmup_sequence = config.warmup_packets / kChannelCount +
                                          (i < config.warmup_packets % kChannelCount ? 1 : 0);

        latency_samples[i].emplace(static_cast<std::size_t>(expected_packets),
                                   last_warmup_sequence);
    }

    // create a downstream
    udp_ingestion::DownstreamWorker downstream(pipeline, latency_samples, config.downstream_cpu);

    std::atomic<bool> stop_requested{false};

    // create result containers
    std::array<udp_ingestion::ReceiveWorkerResult, kChannelCount> results{};
    udp_ingestion::DownstreamResult downstream_results{};
    // create error containers
    std::array<std::exception_ptr, kChannelCount> errors{};
    std::exception_ptr downstream_error;
    std::exception_ptr coordinator_error;

    std::array<std::thread, kChannelCount> threads{};
    std::thread downstream_thread;

    const auto print_affinity_request = [](const std::string& role, const std::optional<int>& cpu) {
        std::cout << role << ": ";

        if (cpu.has_value()) {
            std::cout << "requested CPU " << *cpu;
        } else {
            std::cout << "preserve inherited affinity";
        }

        std::cout << '\n';
    };

    print_affinity_request("main", config.main_cpu);
    for (std::size_t i = 0; i < kChannelCount; ++i) {
        print_affinity_request("RX worker " + std::to_string(i), worker_configs[i].cpu);
    }
    print_affinity_request("downstream", config.downstream_cpu);

    // sockets are bound. RX threads start
    std::cout << "Bound " << config.bind_address << ":" << worker_configs[0].port << " and "
              << config.bind_address << ":" << worker_configs[1].port
              << "\nqueue_capacity=" << config.queue_capacity << " per channel"
              << "\nwarmup_packets=" << config.warmup_packets
              << "\nrx_pause=" << (config.rx_pause ? "on" : "off")
              << "\nidle_timeout_ms=" << config.idle_timeout.count() << '\n';

    bool startup_released = false;
    // create downstream thread and RX threads;
    // each thread sets up cpu affinity and wait for release.
    // when they are ready release them to start their drain loops.
    try {
        downstream_thread = std::thread([&]() {
            try {
                downstream_results = downstream.run(startup);
            } catch (...) {
                downstream_error = std::current_exception();
                stop_requested.store(true, std::memory_order_relaxed);
            }
        });

        for (std::size_t i = 0; i < kChannelCount; ++i) {
            threads[i] = std::thread([&, i]() {
                try {
                    results[i] = workers[i]->run(stop_requested, pipeline.channels[i], startup);
                } catch (...) {
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

        if (startup.wait_until_ready()) {
            std::cout << "READY\n" << std::flush;

            if (!std::cout) {
                throw std::runtime_error("failed to write READY");
            }
            startup.release();
            startup_released = true;
        }

    } catch (...) {
        coordinator_error = std::current_exception();
    }

    if (!startup_released) {
        startup.cancel();

        for (std::size_t i = 0; i < kChannelCount; ++i) {
            if (!threads[i].joinable()) {
                pipeline.channels[i].mark_producer_done();
            }
        }
    }

    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    if (downstream_thread.joinable()) {
        downstream_thread.join();
    }

    const auto report_error = [](const std::string& role, const std::exception_ptr& error) {
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

    if (report_error("main", coordinator_error)) {
        failed = true;
    }

    for (std::size_t i = 0; i < kChannelCount; ++i) {
        if (report_error("RX " + std::to_string(i), errors[i])) {
            failed = true;
        }
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
    for (std::size_t i = 0; i < kChannelCount; ++i) {
        const auto producer = pipeline.channels[i].producer_stats();
        const auto& processed = downstream_results[i];

        const auto latency_statistics =
            udp_ingestion::calculate_latency_statistics(*latency_samples[i]);

        udp_ingestion::print_result(i, worker_configs[i].port, worker_configs[i].expected_packets,
                                    results[i], producer, processed, *latency_samples[i],
                                    latency_statistics);

        if (!udp_ingestion::validate_result(i, results[i], producer, processed,
                                            *latency_samples[i])) {
            success = false;
        }
    }

    udp_ingestion::print_measurement(downstream.measurement());

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
