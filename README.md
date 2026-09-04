# udp-ingestion-bench

`udp-ingestion-bench` is intended to support controlled experiments with
high-rate UDP ingestion architectures in C++ on Linux. The project is currently
in Phase 0: its only executable verifies the compiler, CMake configuration, and
standard-library thread support. No networking or benchmark implementation is
present yet.

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

## Build validation

Configure and build with Ninja, selecting either `Debug` or `Release` as the
build type:

```sh
cmake -S . -B build-debug -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug
./build-debug/phase0_smoke
```

The same commands can be run with `build-release` and
`-DCMAKE_BUILD_TYPE=Release` for a Release build.
