#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>

namespace udp_ingestion {

// Startup coordinator; Only used before packet-processing loops
class StartupGate {
public:
    explicit StartupGate(std::size_t expected_workers)
    : expected_workers_(expected_workers) {

    }

    StartupGate(const StartupGate&) = delete;
    StartupGate& operator=(const StartupGate&) = delete;
    
    // Each worker calls this once, after successful initialization.
    // Returns false if startup was cancelled.
    bool arrive_and_wait() {
        std::unique_lock<std::mutex> lock(mutex_);

        if (cancelled_) {
            return false;
        }

        ++ready_workers_;
        condition_.notify_all();

        condition_.wait(lock, [this]() {
            return released_ || cancelled_;
        });

        return !cancelled_;
    }

    // Called by the startup coordinator, which is not counted as a worker.
    bool wait_until_ready() {
        std::unique_lock<std::mutex> lock(mutex_);

        condition_.wait(lock, [this]() {
            return ready_workers_ == expected_workers_ || cancelled_;
        });

        return !cancelled_;
    }
    
    // Called after wait_until_ready() succeeds.
    void release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_ = true;
        }
        condition_.notify_all();
    }

    // Wakes up both the coordinator and workers if startup fails.
    void cancel() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            cancelled_ = true;
        }
        condition_.notify_all();
    }

private:
    const std::size_t expected_workers_;
    std::size_t ready_workers_ = 0;
    bool released_ = false;
    bool cancelled_ = false;

    std::mutex mutex_;
    std::condition_variable condition_;
};

} // namespace udp_ingestion 