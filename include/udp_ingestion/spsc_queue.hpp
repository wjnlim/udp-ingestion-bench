#pragma once

#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <stdexcept>
#include <type_traits>

namespace udp_ingestion {

template <typename T> class SpscQueue {
    static_assert(std::is_trivially_copyable<T>::value,
                  "SpscQueue requires a trivially copyable payload");
    static_assert(std::atomic<std::size_t>::is_always_lock_free,
                  "SpscQueue requires lock-free index atomics");

public:
    explicit SpscQueue(std::size_t capacity)
        : capacity_(checked_capacity(capacity)), mask_(capacity_ - 1),
          slots_(std::make_unique<T[]>(capacity_)) {}

    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;
    SpscQueue(SpscQueue&&) = delete;
    SpscQueue& operator=(SpscQueue&&) = delete;

    bool try_push(const T& value) noexcept {
        const auto tail = tail_.value.load(std::memory_order_relaxed);
        const auto head = head_.value.load(std::memory_order_acquire);

        if (tail - head == capacity_) {
            return false;
        }

        slots_[tail & mask_] = value;
        tail_.value.store(tail + 1, std::memory_order_release);
        return true;
    }

    bool try_pop(T& value) noexcept {
        const auto head = head_.value.load(std::memory_order_relaxed);
        const auto tail = tail_.value.load(std::memory_order_acquire);

        if (tail - head == 0) {
            return false;
        }

        value = slots_[head & mask_];

        head_.value.store(head + 1, std::memory_order_release);
        return true;
    }

private:
    static constexpr std::size_t kCacheLineSize = 64;

    struct alignas(kCacheLineSize) Position {
        std::atomic<std::size_t> value{0};
    };

    static std::size_t checked_capacity(std::size_t capacity) {
        if (capacity == 0 || (capacity & (capacity - 1)) != 0) {
            throw std::invalid_argument("SpscQueue capacity must be a positive power of two");
        }

        const auto maximum = std::numeric_limits<std::size_t>::max();

        if (capacity > maximum / sizeof(T)) {
            throw std::invalid_argument("SpscQueue storage size is too large");
        }

        return capacity;
    }

    const std::size_t capacity_;
    const std::size_t mask_;
    std::unique_ptr<T[]> slots_;

    Position tail_;
    Position head_;
};

} // namespace udp_ingestion
