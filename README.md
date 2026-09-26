# udp-ingestion-bench

`udp-ingestion-bench` is a C++17/Linux benchmark for comparing two UDP ingestion architectures under the same synthetic workload:

- **Dedicated active polling:** one CPU-pinned, non-blocking RX thread per UDP channel, continuously retrying receive.
- **Readiness-driven multiplexing:** one CPU-pinned, level-triggered `epoll` RX execution loop multiplexing UDP channels.

Here, “active polling” means repeatedly issuing non-blocking receive calls in userspace; it does **not** refer to the Linux `poll()` API.

Both designs use the same wire protocol, decoding and sequence validation, bounded SPSC handoff, downstream processing, timestamping, warmup policy, and result accounting. The comparison intentionally gives the two designs different RX execution-context and CPU budgets: this is an end-to-end architecture/resource-allocation comparison, **not** a microbenchmark of `recvmsg()` versus `epoll_wait()`.

The main question is:
> What latency/resource trade-off appears when a small number of continuously active UDP feeds are given dedicated active-polling workers instead of being multiplexed through a readiness-driven event loop?

The benchmark does not claim that either architecture is universally faster or better. Instead, the results characterize the latency-versus-CPU trade-off created by these two design choices under a controlled workload.

## Design overview

The synthetic publisher emits two independent logical channels over ordinary IPv4 UDP. Each channel carries the same fixed-width market-data-like message and has its own sequence space.
The two primary receiver paths differ only in how the sockets are serviced before the common pipeline:

```text
Dedicated

UDP ch0 -> RX0: active polling -> decode/validate -> SPSC0 --\
                                                          -> downstream -> checksum + latency
UDP ch1 -> RX1: active polling -> decode/validate -> SPSC1 --/
```

```text
Direct epoll

UDP ch0 --\                     /--decode/validate -> SPSC0--\
           -> one LT epoll RX ->                              -> downstream -> checksum + latency
UDP ch1 --/                     \--decode/validate -> SPSC1--/
```

The downstream consumer is common to both designs. It alternates between the per-channel queues, performs deterministic checksum work, and records completion time.

A third executable based on **libevent** is included only as a bounded sanity check for the custom direct-epoll implementation. It uses libevent's epoll backend and matching level-triggered/read-budget semantics; it is not a third primary benchmark architecture.

## Why these comparison points?

The dedicated path models the decision to spend CPU resources on a small set of latency-sensitive, continuously active feeds: each socket has a fixed owner and an RX thread that remains runnable. The epoll path models a resource-efficient alternative in which one RX execution context waits for readiness and multiplexes both descriptors.

The epoll baseline uses level-triggered operation with a fixed budget of **64 receive attempts per ready descriptor**. The budget prevents an indefinitely readable socket from monopolizing the loop while leaving unread data eligible for later LT service. `64` is a fixed service quantum chosen for the baseline, not a claimed optimum or receive batching.

No application-enabled `SO_BUSY_POLL`, kernel bypass, receive batching, real-time scheduling, CPU isolation, IRQ tuning, or frequency-governor tuning is used in the primary benchmark baseline.

## Protocol and synthetic publisher

Each UDP datagram carries one fixed 34-byte Protocol v1 message containing a per-channel sequence, order ID, fixed-point price, instrument ID, quantity, message type, and side. Multi-byte fields are explicitly encoded in network byte order; the naturally aligned host-side structure is never transmitted directly, so compiler padding and ABI layout do not define the wire format.

The traffic is intentionally synthetic. It is not a production exchange protocol and does not implement an order book, matching engine, or complete exchange business semantics.

The publisher alternates packets between two consecutive UDP ports, keeps an independent sequence for each channel, and deterministically generates message fields. `--rate` is the aggregate offered rate across both channels. One `sendto()` is used per datagram, and the publisher can be pinned with `--cpu`.

Pacing uses absolute `steady_clock` deadlines with `sleep_until()`. Using absolute deadlines prevents small per-iteration delays from accumulating and gradually shifting the send schedule. Normal Linux scheduling and timer granularity can still make the publisher late; when that happens, subsequent sends can occur as catch-up bursts. The publisher is a controlled synthetic source, not a precision traffic generator.

## Receiver pipeline

### Dedicated polling

The dedicated receiver creates one non-blocking UDP socket and one RX thread per channel. A worker continuously retries timestamped `recvmsg()` after `EAGAIN/EWOULDBLOCK`; there is no sleep, yield, adaptive backoff, or application-enabled `SO_BUSY_POLL`.

`_mm_pause()` is available as an empty-poll option, but an on/off comparison of `_mm_pause()` showed no meaningful reduction in receiver CPU usage and slightly higher tail latency when enabled. The primary benchmark therefore uses `--rx-pause off`.

### Direct epoll

The direct epoll receiver has one main/RX execution thread and one downstream thread. Both sockets are non-blocking and registered with `EPOLLIN` in LT mode. Each ready descriptor gets at most 64 receive attempts; `EAGAIN` ends the turn and unread data can remain ready for later service.

No RX worker pool is used because an additional dispatch/handoff layer would introduce queueing, synchronization, and scheduling effects that could obscure the receive-architecture comparison.

### Common staged pipeline
Each channel has its own bounded, preallocated SPSC queue:

```text
RX producer -> SPSC[channel] -> common downstream consumer
```

The queue uses single-producer/single-consumer ownership with acquire/release semantics and cache-line-aware producer/consumer state. A full queue drops the new pipeline message and increments `queue_full_drops`; RX never blocks waiting for downstream capacity.

Each channel independently tracks expected sequence numbers, forward gaps, late/duplicate packets, invalid input, queue drops, and final counts. The downstream checksum supplies deterministic work and cross-run consistency evidence, not a proof of packet identity.

## Measurement boundary

Primary latency is measured from the **kernel software RX timestamp** obtained through timestamped `recvmsg()` to completion of downstream deterministic processing:

```text
kernel RX -> receive/decode/sequence -> SPSC -> downstream processing -> completion
```

Both latency endpoints use the `CLOCK_REALTIME` domain. This is not sender-to-receiver, NIC hardware, or physical-wire latency; wall-clock adjustments remain a limitation.

Warmup packets traverse the full pipeline but are excluded from latency samples. Receiver CPU uses a common post-warmup-to-drain window with `CLOCK_PROCESS_CPUTIME_ID / CLOCK_MONOTONIC elapsed time`. It covers the receiver process, excludes the publisher, and may exceed 100%; roughly 300% corresponds to about three logical CPUs' worth of execution. Per-channel p50/p99/p99.9 use nearest-rank percentiles.

## CPU placement

Affinity is a benchmark control, not a universal optimization claim. Primary placement was:

| Role | Dedicated | Direct epoll |
| --- | --- | --- |
| Publisher | CPU 1 | CPU 1 |
| RX | CPUs 2 and 3 | CPU 2 |
| Downstream | CPU 4 | CPU 4 |
| Main (Thread Coordinator) | CPU 0 | — |

These IDs mapped to distinct physical cores on the measured host. Affinity does not reserve cores against unrelated work or SMT siblings. The intentional architectural difference is two dedicated RX contexts versus one multiplexed RX context.

## Development and benchmark host

Primary measurements were collected on bare-metal Ubuntu 24.04.4 LTS with Linux 7.0.0-31-generic, an Intel Core i7-10700K (8 physical / 16 logical CPUs, SMT enabled, one NUMA node), GCC/G++ 13.3, CMake 3.28, Ninja 1.11, and `perf` 7.0.14.

No deliberate low-latency system tuning was applied: CPU isolation, IRQ affinity tuning, SMT changes, real-time scheduling, and governor changes were outside the baseline. The primary experiments use IPv4 **loopback**, so the results characterize receiver architecture and host scheduling/queueing behavior rather than a physical NIC or network path.

## Primary benchmark

The primary non-profiled benchmark contains exactly **30 runs**:

```text
2 architectures
x 3 aggregate offered loads: 100k / 200k / 400k pps
x 5 repetitions
```
Each run lasts about ten seconds with the first second used as warmup. Queue capacity is 4096/channel; dedicated uses PAUSE(`_mm_pause`) off and epoll uses the fixed LT budget of 64. Run/load and architecture order were varied across repetitions.

All 30 recorded benchmark runs completed cleanly:
- receiver and publisher exited normally;
- no invalid packets, sequence gaps, late/duplicate packets, or SPSC queue drops;
- target, receive, enqueue, and processed counts matched;
- publisher average achieved rate stayed near requested rate;
- checksums matched for the same load/channel across architectures.

These clean runs establish the primary latency/CPU comparison. They do not prove that the same rates are loss-free under every later run or host condition.

### Receiver CPU and throughput

Values are median `[min-max]` across five runs. PPS is aggregate across both channels.

| Load | Receiver | CPU % | Processed pps |
| ---: | --- | ---: | ---: |
| 100k | Dedicated | 300.003 [299.980-300.010] | 100000.004 [99999.901-100000.113] |
| 100k | Epoll | 115.365 [114.828-115.808] | 99999.975 [99999.544-100000.163] |
| 200k | Dedicated | 300.002 [300.000-300.003] | 200000.004 [199999.995-200000.074] |
| 200k | Epoll | 129.104 [128.486-130.139] | 200000.215 [199999.852-200000.433] |
| 400k | Dedicated | 300.001 [300.000-300.004] | 400000.090 [399998.755-400001.500] |
| 400k | Epoll | 158.252 [157.471-159.777] | 399999.786 [399999.123-400000.794] |

At the same offered loads, the epoll process used approximately **61.5%, 57.0%, and 47.2% less receiver CPU** at 100k, 200k, and 400k pps respectively. These are process-CPU reductions, not throughput speedups or energy measurements.

### Latency

The table below shows the median of the five per-run percentile values for each channel. Values are microseconds; they are **not** percentiles recomputed from pooled samples.

| Load | Receiver | p50 ch0 / ch1 | p99 ch0 / ch1 | p99.9 ch0 / ch1 |
| ---: | --- | ---: | ---: | ---: |
| 100k | Dedicated | 1.210 / 1.201 | 1.521 / 1.495 | 1.639 / 1.589 |
| 100k | Epoll | 2.220 / 2.212 | 4.452 / 4.694 | 20.600 / 20.812 |
| 200k | Dedicated | 1.208 / 1.191 | 1.481 / 1.460 | 1.573 / 1.562 |
| 200k | Epoll | 2.141 / 2.105 | 4.489 / 4.486 | 20.740 / 20.680 |
| 400k | Dedicated | 1.188 / 1.176 | 1.420 / 1.412 | 2.516 / 2.480 |
| 400k | Epoll | 2.069 / 2.079 | 4.456 / 4.477 | 20.324 / 20.300 |

Under this workload, dedicated polling kept lower measured latency while consuming roughly three logical CPUs across RX plus downstream. Epoll used substantially less receiver CPU, but had higher p50/p99 and a repeatable roughly 20 µs p99.9 region.

This is a **CPU-for-latency trade-off between complete architectures**; it does not isolate `epoll_wait()`, active polling, dedicated ownership, wakeup scheduling, or any one syscall as an independent cause.

## Supplementary `perf stat` profiling

A separate 200k pps profiling series used two fixed event sets and three scheduled runs per architecture/set. Failed runs were retained rather than replaced. Eleven of twelve scheduled runs completed cleanly; one epoll run lost packets and timed out.

Among successful runs:
| Metric | Dedicated | Epoll |
| --- | ---: | ---: |
| task-clock | 30.0747 s [30.0728-30.0749] | 13.2875 s [13.2731-13.3019] |
| context switches | 47 [38-127] | 348,554 [346,879-350,229] |
| CPU migrations | 4 [3-4] | 3 [3-3] |
| cycles | 120.600 B [120.594-120.607] | 53.283 B [53.183-53.384] |
| instructions | 234.543 B [234.036-234.810] | 161.491 B [160.858-162.124] |
| IPC | 1.945 [1.941-1.947] | 3.031 [3.025-3.037] |

Epoll used about **55.8% less task-clock** in successful runs and showed orders of magnitude more context switches, consistent with readiness-driven sleep/wakeup versus continuously runnable polling. This supports the observed latency/resource trade-off, although directly attributing the tail-latency gap to scheduler activity would require additional tracing or event-level correlation.

The branch/cache event set also did **not** support an initial hypothesis that multiplexing the two small per-channel working sets would necessarily create more cache misses:
| Metric | Dedicated | Epoll |
| --- | ---: | ---: |
| branch-miss ratio | 0.0224% | 0.0524% |
| cache references | 305.799 M | 444.776 M |
| cache misses | 1.659 M | 1.576 M |
| cache-miss/reference ratio | 0.5244% | 0.3565% |

Generic `perf` cache events are not treated as a universal all-cache miss rate or evidence of a specific coherence bottleneck; the benchmark's per-channel working sets are small.

### Intermittent receive-buffer loss

Separate exploratory and profiling runs occasionally showed UDP receive-buffer drops on the readiness-driven path, while the 30 primary benchmark runs remained clean. The publisher can generate catch-up bursts after scheduling delays, and transient RX scheduling delay, finite socket-buffer capacity, and the fixed receive-attempt budget may all affect burst tolerance. Isolating these factors is outside the scope of the current benchmark, so socket-buffer and service-budget sensitivity are deferred to follow-up experiments.

## Libevent sanity check

An optional libevent 2.1.12-stable receiver, verified to use its epoll backend, checks whether the direct-epoll result is mainly an artifact of the hand-written loop. It keeps one main/RX context, persistent LT read events, the same 64-attempt callback budget, and the same receive/processing/SPSC/downstream measurement path.

A bounded three-round 200k pps comparison produced:
| Metric | Direct epoll | Libevent |
| --- | ---: | ---: |
| Clean receiver completion | 3/3 | 2/3 |
| Clean-run CPU | 131.02-132.71% | 132.11-132.17% |
| Clean-run p50 | 2.228-2.429 µs | 2.362-2.401 µs |
| Clean-run p99 | 4.509-4.616 µs | 4.691-4.716 µs |
| Clean-run p99.9 | 18.675-20.780 µs | 18.117-20.455 µs |

The clean implementations were not markedly different in CPU or latency, reducing the likelihood that the direct-epoll result is mainly a custom-loop artifact. Three rounds do not establish statistical equivalence. One libevent run missed 250 packets and timed out; it is retained rather than replaced, and its exact loss cause is not claimed.

## Build

Default builds do not require libevent.
```sh
cmake -S . -B build-release -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON

cmake --build build-release -j

ctest --test-dir build-release --output-on-failure
```
For a Debug build, use a separate `build-debug` directory and `-DCMAKE_BUILD_TYPE=Debug`.

The tests cover protocol/sequence processing, SPSC handoff, downstream processing, timestamp parsing/socket reception, latency statistics, run timing, and epoll worker behavior.

To build the optional libevent receiver on Ubuntu:
```sh
sudo apt-get install libevent-dev pkg-config

cmake -S . -B build-release -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON \
  -DBUILD_LIBEVENT_RECEIVER=ON

cmake --build build-release -j

ctest --test-dir build-release --output-on-failure

./build-release/libevent_backend_probe
```
## Quick run example

Example dedicated receiver (choose CPU IDs appropriate for the host):

```sh
./build-release/dedicated_receiver \
  --expected-packets 2000000 \
  --warmup-packets 200000 \
  --queue-capacity 4096 \
  --idle-timeout-ms 30000 \
  --rx-pause off \
  --main-cpu 0 --rx-cpu0 2 --rx-cpu1 3 --downstream-cpu 4
```
Example direct epoll receiver:

```sh
./build-release/epoll_receiver \
  --expected-packets 2000000 \
  --warmup-packets 200000 \
  --queue-capacity 4096 \
  --idle-timeout-ms 30000 \
  --rx-cpu 2 --downstream-cpu 4
```
After the chosen receiver prints `READY`, run the publisher in another terminal:

```sh
./build-release/synthetic_publisher \
  --address 127.0.0.1 \
  --base-port 9000 \
  --rate 200000 \
  --count 2000000 \
  --cpu 1
```
A small Python standard-library harness is included for repeated dedicated/epoll runs; the C++ executables remain responsible for application-level validation while the harness handles orchestration, logs, and CSV capture.

## Limitations

- IPv4 UDP unicast and exactly two logical channels are implemented.
- The synthetic source uses ordinary `sendto()` pacing; primary measurements use loopback and kernel software timestamps, not a physical NIC/wire or hardware timestamps.
- The benchmark compares unequal RX CPU budgets intentionally.
- No receive batching, kernel bypass, `SO_BUSY_POLL`, real-time scheduling, CPU isolation, or IRQ tuning is used.
- The default baseline does not explicitly resize `SO_RCVBUF`.
- Publisher scheduling can create catch-up bursts; average achieved pps does not prove smooth inter-packet spacing.
- Five repetitions per condition provide repeatability evidence, not statistical significance or a reliability guarantee.
- Generic `perf` counters are supporting observations, not direct causal attribution.
- Count/idle-timeout termination assumes controlled finite runs; sequence statistics describe observed gaps rather than proving exact final network loss.

## Future experiments

The current baseline is intentionally focused. 
Natural follow-up experiments include:
- **Burst and buffer sensitivity:** vary `SO_RCVBUF` and the epoll receive-attempt budget to study how burst tolerance, packet loss, and queueing latency change.
- **Architecture decomposition:** add a dedicated readiness-waiting receiver (one pinned worker per socket) to separate the effects of active polling from dedicated per-feed ownership.
- **Kernel-side polling:** evaluate `SO_BUSY_POLL` as a separate extension rather than mixing it into the current baseline.
- **Physical-network validation:** move beyond loopback to a separate sender/NIC setup, then consider multicast, IRQ placement, and hardware timestamping.
- **Deeper diagnostics where needed:** use scheduler tracing or larger per-feed working sets only when investigating wakeup-latency or cache-locality questions directly.

These are follow-up experiments, not prerequisites for interpreting the current primary comparison.
