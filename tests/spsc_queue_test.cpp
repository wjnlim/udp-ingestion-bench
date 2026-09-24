#include "udp_ingestion/spsc_queue.hpp"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <thread>

namespace {

using udp_ingestion::SpscQueue;

int failures = 0;

void expect(bool condition, const char* description) {
    if (!condition) {
        std::cerr << "FAIL: " << description << '\n';
        ++failures;
    }
}

struct Payload {
    std::uint64_t sequence;
    std::uint64_t inverse;
    std::uint64_t doubled;
};

Payload make_payload(std::uint64_t sequence) {
    return Payload{sequence, ~sequence, sequence * 2};
}

bool matches(const Payload& value, std::uint64_t sequence) {
    return value.sequence == sequence && value.inverse == ~sequence &&
           value.doubled == sequence * 2;
}

void expect_invalid_capacity(std::size_t capacity) {
    bool rejected = false;

    try {
        SpscQueue<Payload> queue(capacity);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }

    expect(rejected, "invalid capacity is rejected before allocation");
}

void test_invalid_capacity() {
    expect_invalid_capacity(0);
    expect_invalid_capacity(3);
    expect_invalid_capacity(6);

    // A representable power of two whose Payload array size would overflow.
    const auto largest_power_of_two = std::numeric_limits<std::size_t>::max() / 2 + 1;
    expect_invalid_capacity(largest_power_of_two);
}

void test_empty_and_single_slot() {
    SpscQueue<Payload> queue(1);
    auto output = make_payload(99);

    expect(!queue.try_pop(output), "new queue is empty");
    expect(matches(output, 99), "failed pop leaves output unchanged");

    expect(queue.try_push(make_payload(1)), "capacity-one queue accepts one");
    expect(!queue.try_push(make_payload(2)), "capacity-one queue is then full");

    expect(queue.try_pop(output), "queued value can be popped");
    expect(matches(output, 1), "failed push did not overwrite queued value");
    expect(!queue.try_pop(output), "queue is empty after pop");

    expect(queue.try_push(make_payload(3)), "single slot can be reused");
    expect(queue.try_pop(output), "reused slot can be popped");
    expect(matches(output, 3), "reused slot contains the new value");
}

void test_full_fifo_and_reuse() {
    constexpr std::size_t capacity = 8;
    SpscQueue<Payload> queue(capacity);
    Payload output{};

    for (std::uint64_t round = 0; round < 1000; ++round) {
        const auto first = round * capacity;

        for (std::size_t i = 0; i < capacity; ++i) {
            expect(queue.try_push(make_payload(first + i)), "all capacity slots are usable");
        }

        expect(!queue.try_push(make_payload(999999)), "push fails when all slots are occupied");

        for (std::size_t i = 0; i < capacity; ++i) {
            expect(queue.try_pop(output), "full queue can be drained");
            expect(matches(output, first + i), "FIFO order is preserved");
        }

        expect(!queue.try_pop(output), "drained queue is empty");
    }
}

void test_partial_drain_and_wrap() {
    SpscQueue<Payload> queue(4);
    Payload output{};

    for (std::uint64_t i = 0; i < 4; ++i) {
        expect(queue.try_push(make_payload(i)), "initial fill succeeds");
    }

    for (std::uint64_t i = 0; i < 2; ++i) {
        expect(queue.try_pop(output), "partial drain succeeds");
        expect(matches(output, i), "partial drain preserves order");
    }

    expect(queue.try_push(make_payload(4)), "wrapped slot zero is reusable");
    expect(queue.try_push(make_payload(5)), "wrapped slot one is reusable");
    expect(!queue.try_push(make_payload(6)), "wrapped queue is full");

    for (std::uint64_t i = 2; i < 6; ++i) {
        expect(queue.try_pop(output), "wrapped queue can be drained");
        expect(matches(output, i), "wrap preserves unread values and FIFO");
    }

    expect(!queue.try_pop(output), "wrapped queue becomes empty");
}

void test_failed_push_accounting() {
    SpscQueue<Payload> queue(2);
    std::uint64_t enqueued = 0;
    std::uint64_t drops = 0;

    for (std::uint64_t i = 0; i < 8; ++i) {
        if (queue.try_push(make_payload(i))) {
            ++enqueued;
        } else {
            ++drops;
        }
    }

    expect(enqueued == 2, "two events were enqueued");
    expect(drops == 6, "six failed pushes can be counted as drops");
    expect(enqueued + drops == 8, "every enqueue attempt is accounted for");

    Payload output{};
    std::uint64_t processed = 0;

    while (queue.try_pop(output)) {
        expect(matches(output, processed), "drops do not overwrite accepted events");
        ++processed;
    }

    expect(processed == enqueued, "draining processes all accepted events");
}

void test_concurrent_transfer(std::size_t capacity) {
    constexpr std::uint64_t count = 200'000;
    SpscQueue<Payload> queue(capacity);

    // Only this thread calls try_push().
    std::thread producer([&queue]() {
        for (std::uint64_t i = 0; i < count; ++i) {
            const auto value = make_payload(i);

            while (!queue.try_push(value)) {
                std::this_thread::yield();
            }
        }
    });

    // The test's main thread is the only consumer.
    bool values_match = true;
    Payload output{};

    for (std::uint64_t i = 0; i < count; ++i) {
        while (!queue.try_pop(output)) {
            std::this_thread::yield();
        }

        if (!matches(output, i)) {
            values_match = false;
        }
    }

    producer.join();

    expect(values_match, "concurrent transfer preserves FIFO and every payload field");
    expect(!queue.try_pop(output), "concurrent transfer leaves no extra events");
}

} // namespace

int main() {
    try {
        test_invalid_capacity();
        test_empty_and_single_slot();
        test_full_fifo_and_reuse();
        test_partial_drain_and_wrap();
        test_failed_push_accounting();
        test_concurrent_transfer(1);
        test_concurrent_transfer(8);
        test_concurrent_transfer(1024);
    } catch (const std::exception& error) {
        std::cerr << "test error: " << error.what() << '\n';
        return 1;
    }

    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return 1;
    }

    std::cout << "all SPSC queue tests passed\n";
    return 0;
}