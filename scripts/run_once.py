#!/usr/bin/env python3
"""Run one localhost benchmark. C++ owns packet validation and accounting."""

import argparse
import os
import selectors
import shlex
import subprocess
import time
from contextlib import ExitStack
from pathlib import Path

from benchmark_metadata import collect_metadata
from benchmark_output import parse_outputs, require_complete_output, write_result

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def positive(text):
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)

    parser.add_argument("--architecture", choices=("dedicated", "epoll"), required=True)
    parser.add_argument("--build-dir", default="build-release")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--rate", type=int, required=True)
    parser.add_argument("--warmup-packets", type=int, default=0)
    parser.add_argument("--base-port", type=int, default=9000)
    parser.add_argument("--queue-capacity", type=int, default=4096)
    parser.add_argument("--rx-pause", choices=("off", "on"), default="off")
    parser.add_argument("--idle-timeout-ms", type=int, default=3000)
    parser.add_argument("--run-index", type=positive, default=1)
    parser.add_argument("--startup-timeout-seconds", type=positive, default=10)
    parser.add_argument("--run-timeout-seconds", type=positive, default=120)
    for option in ("publisher-cpu", "downstream-cpu", "rx-cpu", "rx-cpu0", "rx-cpu1", "main-cpu"):
        parser.add_argument("--" + option, type=int)

    args = parser.parse_args()

    if args.architecture == "epoll" and args.rx_pause == "on":
        parser.error("--rx-pause on is only supported by dedicated")
    if args.architecture == "dedicated" and args.rx_cpu is not None:
        parser.error("--rx-cpu is for epoll; use --rx-cpu0/--rx-cpu1")
    if args.architecture == "epoll" and any(
        value is not None for value in (args.rx_cpu0, args.rx_cpu1, args.main_cpu)
    ):
        parser.error("--rx-cpu0/--rx-cpu1/--main-cpu are for dedicated")

    for name in ("build_dir", "output_dir"):
        setattr(args, name, (PROJECT_ROOT / getattr(args, name)).resolve())

    if args.output_dir.exists():
        parser.error("output directory already exists; choose a new one")

    return args


def make_commands(args):
    receiver_cmd = [str(args.build_dir / (args.architecture + "_receiver")),
                "--expected-packets", str(args.count),
                "--warmup-packets", str(args.warmup_packets),
                "--base-port", str(args.base_port),
                "--queue-capacity", str(args.queue_capacity),
                "--idle-timeout-ms", str(args.idle_timeout_ms)]
    publisher_cmd = [str(args.build_dir / "synthetic_publisher"),
                 "--count", str(args.count), "--rate", str(args.rate),
                 "--base-port", str(args.base_port)]
    roles = ["downstream_cpu"] + (
        ["rx_cpu0", "rx_cpu1", "main_cpu"] if args.architecture == "dedicated" 
                                                                else ["rx_cpu"]
    )
    for role in roles:
        if getattr(args, role) is not None:
            receiver_cmd.extend(["--" + role.replace("_", "-"), str(getattr(args, role))])

    if args.architecture == "dedicated":
        receiver_cmd.extend(["--rx-pause", args.rx_pause])
    
    if args.publisher_cpu is not None:
        publisher_cmd.extend(["--cpu", str(args.publisher_cpu)])

    return receiver_cmd, publisher_cmd


def pump_stdout(receiver, log, deadline, publisher=None):
    """Before launch, wait for READY; afterwards drain stdout and watch exits."""

    pending = b""
    stdout_open = True

    with selectors.DefaultSelector() as selector:
        selector.register(receiver.stdout, selectors.EVENT_READ)

        while True:
            if publisher is not None:
                for name, process in (("receiver", receiver), ("publisher", publisher)):
                    if process.poll() not in (None, 0):
                        raise RuntimeError(f"{name} exited with code {process.returncode}")
                if not stdout_open and receiver.poll() is not None and publisher.poll() is not None:
                    return
                
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("run timeout" if publisher is not None else "READY timeout")

            for key, _ in selector.select(min(remaining, 0.1)):
                chunk = os.read(key.fd, 65536)
                if not chunk:
                    if publisher is None:
                        raise RuntimeError("receiver stdout closed before READY")
                    
                    selector.unregister(key.fileobj)
                    stdout_open = False
                    continue

                log.write(chunk)
                log.flush()
                if publisher is None:
                    pending += chunk
                    while b"\n" in pending:
                        line, pending = pending.split(b"\n", 1)
                        if line == b"READY":
                            return


def stop_process(process):
    if process is None:
        return
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
    process.wait()


def run_once(args):
    metadata_log = collect_metadata(args, PROJECT_ROOT)  # Outside the measured run.
    receiver_command, publisher_command = make_commands(args)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    
    with (args.output_dir / "commands.log").open("x") as commands:
        commands.write(shlex.join(receiver_command) + "\n" + shlex.join(publisher_command) + "\n")

    execution_log = {"status": "failed", "error": "", "receiver_exit_code": "",
                 "publisher_exit_code": ""}

    receiver = publisher = None
    with ExitStack() as files:
        logs = {name: files.enter_context((args.output_dir / (name + ".log")).open("xb"))
                for name in ("receiver.stdout", "receiver.stderr", 
                             "publisher.stdout", "publisher.stderr")}
        try:
            receiver = subprocess.Popen(receiver_command, cwd=PROJECT_ROOT,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=logs["receiver.stderr"], bufsize=0)
            
            pump_stdout(receiver, logs["receiver.stdout"], 
                            time.monotonic() + args.startup_timeout_seconds)

            if receiver.poll() is not None:
                raise RuntimeError("receiver exited before publisher startup")
            
            deadline = time.monotonic() + args.run_timeout_seconds
            publisher = subprocess.Popen(publisher_command, cwd=PROJECT_ROOT,
                        stdin=subprocess.DEVNULL, stdout=logs["publisher.stdout"], 
                                                    stderr=logs["publisher.stderr"])

            pump_stdout(receiver, logs["receiver.stdout"], deadline, publisher)

            execution_log["status"] = "ok"
        except KeyboardInterrupt:
            execution_log.update(status="interrupted", error="interrupted by user")
        except TimeoutError as error:
            execution_log.update(status="timeout", error=str(error))
        except (OSError, RuntimeError) as error:
            execution_log["error"] = str(error)
        finally:
            try:
                stop_process(publisher)
            finally:
                stop_process(receiver)
            if receiver is not None:
                # receiver's stdout is a PIPE; thus flush it
                with receiver.stdout:
                    while chunk := os.read(receiver.stdout.fileno(), 65536):
                        logs["receiver.stdout"].write(chunk)
                execution_log["receiver_exit_code"] = receiver.returncode
            if publisher is not None:
                execution_log["publisher_exit_code"] = publisher.returncode

    channels_log, metrics_log, publisher_log = {}, {}, {}
    try:
        channels_log, metrics_log, publisher_log = parse_outputs(
            args.output_dir / "receiver.stdout.log",
            args.output_dir / "publisher.stdout.log")
        
        if execution_log["status"] == "ok":
            require_complete_output(channels_log, metrics_log, publisher_log)
            if metrics_log["measurement_available"] != "1":
                execution_log.update(status="no_measurement", 
                                 error="receiver produced no timed interval")

    except (ValueError, OSError) as error:
        if execution_log["status"] == "ok":
            execution_log.update(status="output_error", error=str(error))
        else:
            execution_log["error"] += f"; output: {error}"

    write_result(args.output_dir / "result.csv", metadata_log, 
                 execution_log, channels_log, metrics_log, publisher_log)

    print(f"status={execution_log['status']}\noutput_dir={args.output_dir}")
    if execution_log["error"]:
        print(f"error={execution_log['error']}")
    return 130 if execution_log["status"] == "interrupted" else (0 if execution_log["status"] == "ok" else 1)


def main():
    args = parse_arguments()
    try:
        return run_once(args)
    except OSError as error:
        print(f"error: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
