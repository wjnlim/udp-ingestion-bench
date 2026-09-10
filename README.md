# udp-ingestion-bench

`udp-ingestion-bench` is intended to support controlled experiments with
high-rate UDP ingestion architectures in C++17 on Linux. Phase 1 provides a
controllable synthetic market-data publisher and an explicit 34-byte protocol.
Phase 2 adds two dedicated RX threads, each polling one non-blocking UDP socket,
decoding messages, tracking sequences, and updating a small accumulator.
Phase 3 adds optional per-thread CPU affinity to the same receive architecture.

The eventual comparison is dedicated userspace polling versus epoll-based
event-driven readiness. No performance advantage is assumed or established.
Epoll, inter-core handoff, and latency measurement are deferred.

Design records:

- [Phase 1: protocol and publisher](docs/phase1-design.md)
- [Phase 2: dedicated receiver](docs/phase2-design.md)
- [Phase 3: receiver CPU affinity](docs/phase3-design.md)

Design records under `docs/` are currently local-only and Git-ignored; these
links are available in the local workspace but may not resolve on GitHub.

## Development host

The following records the initial environment inspection; it is not a fresh
measurement of the host configuration on every run.

- Environment: bare-metal Ubuntu 24.04.4 LTS (Noble), not a virtual machine
- Kernel: 7.0.0-31-generic, x86-64
- Filesystem: ext4 on the Ubuntu NVMe partition
- CPU: Intel Core i7-10700K at 3.80 GHz
- Topology: one socket, eight physical cores, 16 logical CPUs, SMT enabled
- SMT sibling pairs: 0/8, 1/9, 2/10, 3/11, 4/12, 5/13, 6/14, 7/15
- NUMA: one node containing logical CPUs 0-15
- NIC: Intel Ethernet Connection (11) I219-V (`eno1`), using the `e1000e`
  kernel driver
- Primary toolchain: GCC/G++ 13.3.0, CMake 3.28.3, and Ninja 1.11.1
- CPU frequency governor: `powersave` on all logical CPUs
- Performance monitoring: `perf` 7.0.14 is installed and the Intel hardware PMU
  is visible; `kernel.perf_event_paranoid=4` restricts unprivileged counter access

No deliberate low-latency tuning has been applied. In particular, CPU
isolation, IRQ affinity tuning, and changes to SMT, the frequency governor, or
other system settings have not been configured for this project.

## Protocol v1

Protocol v1 is synthetic and is not intended to reproduce any production
exchange protocol. Every message has the same 34-byte wire representation:

| Offset | Size | Field | Host type |
| ---: | ---: | --- | --- |
| 0 | 8 | sequence | `std::uint64_t` |
| 8 | 8 | order_id | `std::uint64_t` |
| 16 | 8 | price | `std::int64_t` |
| 24 | 4 | instrument_id | `std::uint32_t` |
| 28 | 4 | quantity | `std::uint32_t` |
| 32 | 1 | message_type | `Add`, `Modify`, `Cancel`, or `Trade` |
| 33 | 1 | side | `Buy` or `Sell` |

All multi-byte fields use network byte order (big endian). Price is a signed
fixed-point integer with 10,000 units per price unit. The publisher performs no
floating-point price conversion in its generation loop.

The naturally aligned host-side `MarketDataMessage` is deliberately separate
from the wire buffer. Explicit encode and decode functions define the protocol;
compiler padding and C++ structure layout never enter a UDP datagram.

## Synthetic publication

The publisher sends two independent logical channels to consecutive destination
ports (`--base-port` and `--base-port + 1`). They are not redundant A/B feeds.
Packets alternate between the channels, and each channel starts its own sequence
at one and increments it independently. A channel can carry multiple instrument
IDs.

`--rate` means aggregate offered packets per second across both channels. The
publisher uses a simple `steady_clock` schedule and `sleep_until` pacing. This is
intended for controllable Phase 1 workloads, not precise high-rate traffic
shaping; scheduler and timer granularity will limit accuracy at high rates.

`--cpu N` optionally pins the publisher's single execution context to Linux
logical CPU N. If omitted, the application does not change its affinity. This
control is for keeping later experiments repeatable, not for optimizing the
publisher, and no CPU number is hardcoded.

UDP unicast is currently used as an experimental simplification. The publisher
must be given the receiver address and base port. Protocol v1 and synthetic
message generation do not depend on unicast; later multicast support should
only require changing destination/socket setup and not the generation, encoding,
or decoding pipeline.

## Build

Configure and build with Ninja, selecting either `Debug` or `Release` as the
build type:

```sh
cmake -S . -B build-debug -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug
```

The same commands can be run with `build-release` and
`-DCMAKE_BUILD_TYPE=Release` for a Release build.

With `BUILD_TESTING=ON` (the default), CTest runs `protocol_v1_test` and
`receive_processing_test`. These exercise protocol and receiver processing
semantics without opening network sockets. Run them with:

```sh
ctest --test-dir build-debug --output-on-failure
```

## Run

The defaults send 1,000 total packets to localhost ports 9000 and 9001 at an
aggregate target rate of 1,000 packets per second:

```sh
./build-debug/synthetic_publisher
```

For an explicit workload and optional affinity:

```sh
./build-debug/synthetic_publisher \
  --address 127.0.0.1 --base-port 9000 \
  --rate 100000 --count 10000 --instruments 64

./build-debug/synthetic_publisher \
  --address 127.0.0.1 --base-port 9000 \
  --rate 100000 --count 10000 --instruments 64 --cpu 1
```

Choose `--cpu` from the CPUs available to the process and according to the host
topology; CPU 1 above is only an example.

For a small localhost correctness check, start the validation receiver first,
then publish twice the per-channel count:

```sh
./build-debug/udp_validation_receiver 9000 10
# In another terminal:
./build-debug/synthetic_publisher --base-port 9000 --rate 1000 --count 20
```

The receiver helper only checks Phase 1 datagram size, decoding, and per-channel
sequence continuity. It is not a benchmark receiver and is not a starting point
for an optimized architecture.

## Dedicated receiver

Start the receiver in terminal A:

```sh
./build-debug/dedicated_receiver \
  --bind-address 127.0.0.1 --base-port 19000 \
  --count 20 --idle-timeout-ms 10000
echo "receiver exit=$?"
```

After the receiver prints its `Bound` message, run the publisher in terminal B:

```sh
./build-debug/synthetic_publisher \
  --address 127.0.0.1 --base-port 19000 --count 20 --rate 1000
```

Both channels should report `count_reached`, 10 received and valid datagrams,
expected sequence 11, and zero invalid/gap/late-or-duplicate counters.
The receiver should exit with code 0.

| Option | Meaning | Default |
| --- | --- | --- |
| `--bind-address` | Local IPv4 address to bind | `127.0.0.1` |
| `--base-port` | Channel 0 port; channel 1 uses the next port | `9000` |
| `--count` | Aggregate expected datagram count | `1000` |
| `--idle-timeout-ms` | Idle interval, including initial waiting | `3000` |
| `--rx-cpu0` | Logical CPU for RX worker 0 | Inherited affinity |
| `--rx-cpu1` | Logical CPU for RX worker 1 | Inherited affinity |
| `--main-cpu` | Logical CPU for main | Inherited affinity |

Base port must be 1-65534, count must be positive, and timeout must be
1-86400000 ms. Allow enough time to start the publisher and accommodate gaps
in the configured workload.

For total count N, channel 0 expects `N / 2 + N % 2` datagrams and channel 1
expects `N / 2`. A zero-target worker completes without receiving, after applying
any requested affinity.

Receiver count includes invalid and duplicate datagrams. Count completion alone
does not establish correctness. Exit code 0 requires both channels to complete
with no invalid packets, forward gaps, or late-or-duplicate packets. Other
outcomes, including timeout and receive errors, return code 1.

The checksum simulates minimal processing and is printed after the run.
The executable does not compare it against an expected checksum.

### Receiver CPU affinity

First inspect the available CPUs and physical-core topology:

```sh
lscpu -e=CPU,CORE,SOCKET,ONLINE
taskset -pc $$
```

The following example uses main=0, publisher=1, RX0=2, and RX1=3. Use these
numbers only if they are available and suitable for the current host. Prefer
distinct physical cores for hot workers; on the recorded host, CPUs 10 and 11
are SMT siblings of RX CPUs 2 and 3, not independent cores.

Start the receiver in a shell at the project root:

```sh
./build-release/dedicated_receiver \
  --count 20 --idle-timeout-ms 60000 \
  --main-cpu 0 --rx-cpu0 2 --rx-cpu1 3 &
receiver_pid=$!
```

After the `Bound` message, run these commands in the same shell before the
idle timeout expires:

```sh
taskset -apc "$receiver_pid"
./build-release/synthetic_publisher --count 20 --rate 100 --cpu 1
wait "$receiver_pid"
echo "receiver exit=$?"
```

The main TID equals the process ID and should have mask `0`; the other two
threads should have masks `2` and `3` (listing order is not guaranteed).
Both channels should receive 10 valid packets, finish cleanly, and produce
receiver exit code 0. Startup output describes requested placement, not verified
placement; inspect the actual masks rather than relying on that output alone.

Each RX worker pins itself before polling. Main pins itself only after creating
both workers, so omitted RX options retain the original inherited mask rather
than inheriting main's new single-CPU mask. Omitting all CPU options leaves all
three masks unchanged. CPU IDs must be unsigned decimal integers below
`CPU_SETSIZE`; actual availability is checked when affinity is applied.
Explicit affinity failure is fatal and requests cleanup of started threads.

Affinity controls scheduler placement; it does not reserve cores, move IRQs,
enable real-time scheduling, or establish a performance improvement.

## Validation status

During manual Phase 2 validation, the operator reported successful Debug and
Release builds and CTest runs. These localhost cases also matched expectations:

- Debug and Release: 20 packets, split 10/10, with matching channel checksums.
- Odd count: 21 packets, split 11/10.
- Count 1: channel 0 receives one packet; channel 1 completes with target zero.
- No publisher: both workers terminate through idle timeout with exit code 1.
- Partial publication: each channel receives five of ten expected packets,
  then terminates through idle timeout with exit code 1.

For Phase 3, the operator also reported successful Debug/Release builds and both
CTest tests, plus manual checks of actual thread masks, pinned localhost
publication, main-only and omitted-affinity inheritance, invalid CLI inputs,
and fatal RX/main affinity failures. Existing CTest tests cover protocol and
processing behavior, not runtime affinity. Raw validation logs are not archived.

These are correctness checks, not throughput or latency measurements. See the
design records for validation coverage and limitations.

## Current limitations

- IPv4 UDP unicast only; multicast is deferred.
- Exactly two logical channels on consecutive ports.
- Total message count controls a run; duration-based runs are not implemented.
- One ordinary `sendto` call per datagram; there is no batching or kernel bypass.
- Pacing accuracy is subject to normal Linux scheduling and timer granularity.
- No packet recovery, order book, exchange business semantics, latency
  measurement, performance counters, or system tuning.
- Each RX thread actively retries after EAGAIN, consuming CPU even when idle.
- No epoll baseline, SPSC handoff, receive batching, polling backoff,
  `_mm_pause()`, or application-enabled `SO_BUSY_POLL`.
- Count and idle-timeout termination assume a controlled finite run. Unrelated
  or duplicate traffic can consume the expected count.
- Sequence statistics describe observed gaps and late/duplicate arrivals;
  they do not establish exact final network loss.
