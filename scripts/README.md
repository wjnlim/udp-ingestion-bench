# Minimal benchmark harness

Build the C++ executables first, then run one localhost experiment:

```sh
cmake --build build-release
python3 -B scripts/run_once.py --architecture dedicated \
  --count 10000 --rate 10000 --warmup-packets 1000 \
  --run-index 1 --output-dir build-release/results/dedicated-1
```

Use `--architecture epoll` for the other receiver. Each invocation needs a new
output directory. `--help` lists CPU assignments and timeout options.

## Responsibilities

C++ owns argument ranges, packet validation, sequence accounting, checksums,
latency statistics and CPU/throughput calculations. Python waits for `READY`,
starts the publisher, drains receiver output, checks process exits and writes CSV.
It checks only that required output fields exist and are unambiguous; it does not
recompute or cross-check application metrics.

Timeouts and interruption terminate and reap the child processes. There are no
automatic retries or recovery runs. Nonzero exits remain failed experiments.

## Output

- `commands.log`: exact receiver and publisher commands.
- `{receiver,publisher}.{stdout,stderr}.log`: raw output.
- `result.csv`: two rows, one per channel, with explicit columns.

Global measurement and publisher columns repeat on both rows; **do not sum them**.
Values are copied from C++ output, including `NA`. Status is `ok`, `failed`,
`timeout`, `interrupted`, `output_error` or `no_measurement`. Only `ok` returns 0;
interruption returns 130, other run failures return 1. `ok` means successful
execution with a timed interval, not proof of a representative steady-state run.
New runs do not produce JSON. Existing result directories are not modified.

Metadata is limited to git commit, receiver architecture, requested load, run
index, requested CPU assignments, SPSC capacity, epoll service budget, pause,
kernel version and build type. CPU `inherited` means no explicit pin was requested;
`NA` means that role does not apply. The current C++ baseline has a fixed epoll
budget of 64 and no pause instruction (`off`); update these metadata constants if
that baseline changes. They are not runtime switches.

Build type comes from CMakeCache; git commit does not identify uncommitted edits
or prove that binaries are fresh. Rebuild and use a recorded commit for published
measurements. Complete cache dumps, dirty-state snapshots and binary hashes are
intentionally omitted.

## Harness tests

```sh
python3 -B tests/benchmark_harness_test.py
```

These test orchestration, CSV extraction, failures, timeouts and child cleanup.
Application correctness remains covered by the C++ tests.
