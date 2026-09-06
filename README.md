# udp-ingestion-bench

`udp-ingestion-bench` is intended to support controlled experiments with
high-rate UDP ingestion architectures in C++ on Linux. Phase 1 provides a
controllable synthetic market-data publisher and correctness validation for a
small binary protocol. It does not yet contain either receiver architecture or
performance benchmark logic.

## Development host

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

Run the automated protocol tests with:

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

## Phase 1 limitations

- IPv4 UDP unicast only; multicast is deferred.
- Exactly two logical channels on consecutive ports.
- Total message count controls a run; duration-based runs are not implemented.
- One ordinary `sendto` call per datagram; there is no batching or kernel bypass.
- Pacing accuracy is subject to normal Linux scheduling and timer granularity.
- No packet recovery, order book, exchange business semantics, receiver CPU
  pinning, latency measurement, performance counters, or system tuning.
