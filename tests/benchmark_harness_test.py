"""Harness contracts only; packet accounting remains covered by C++ tests."""

import contextlib
import csv
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import run_once
from benchmark_metadata import collect_metadata
from benchmark_output import (
    CHANNEL_FIELDS, MEASUREMENT_FIELDS, PUBLISHER_FIELDS,
    parse_outputs, require_complete_output, write_result,
)


def output_fixture(available="1"):
    receiver_log = "\n".join(
        f"channel={i}\n" + "\n".join(f"{key}=0" for key in CHANNEL_FIELDS)
        for i in range(2)
    )
    receiver_log += "\n" + "\n".join(
        f"{key}={available if key == 'measurement_available' else 'NA'}"
        for key in MEASUREMENT_FIELDS
    )
    publisher_log = "\n".join(f"{key}=0" for key in PUBLISHER_FIELDS)
    return receiver_log, publisher_log


class HarnessTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="udp-harness-test-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / "run"
        argv = ["run_once", "--architecture", "epoll", "--count", "20",
                "--rate", "100", "--output-dir", str(self.directory)]
        with patch.object(sys, "argv", argv):
            self.args = run_once.parse_arguments()

    def execute(self, receiver_code, publisher_code, interrupt=False):
        commands = ([sys.executable, "-u", "-c", receiver_code],
                    [sys.executable, "-u", "-c", publisher_code])
        children = []
        real_popen = subprocess.Popen

        def launch(*args, **kwargs):
            child = real_popen(*args, **kwargs)
            children.append(child)
            return child

        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(run_once, "make_commands", return_value=commands))
            stack.enter_context(patch.object(run_once, "collect_metadata", return_value={}))
            stack.enter_context(patch.object(subprocess, "Popen", side_effect=launch))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            if interrupt:
                stack.enter_context(patch.object(run_once, "pump_stdout", side_effect=KeyboardInterrupt))
            exit_code = run_once.run_once(self.args)
        self.assertTrue(all(child.poll() is not None for child in children))
        with (self.directory / "result.csv").open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual(len(rows), 2)
        # self.assertFalse((self.directory / "result.json").exists())
        return exit_code, rows

    def test_csv_and_cpp_authority(self):
        receiver_log, publisher_log = output_fixture()
        exit_code, rows = self.execute(
            "import time; print('READY', flush=True); time.sleep(.2); "
            f"print('x' * 200000); print({receiver_log!r})", f"print({publisher_log!r})")
        self.assertEqual(exit_code, 0)  # No Python re-check of synthetic packet/CPU math.
        self.assertEqual(rows[0]["status"], "ok")
        self.assertEqual(rows[1]["channel"], "1")
        self.assertEqual(rows[0]["measurement_cpu_percent"], "NA")
        self.assertGreater((self.directory / "receiver.stdout.log").stat().st_size, 200000)
        with self.assertRaises(FileExistsError):
            run_once.run_once(self.args)

    def test_no_measurement(self):
        receiver_log, publisher_log = output_fixture("0")
        exit_code, rows = self.execute(
            f"import time; print('READY', flush=True); time.sleep(.2); print({receiver_log!r})",
            f"print({publisher_log!r})")
        self.assertEqual((exit_code, rows[0]["status"]), (1, "no_measurement"))

    def test_missing_output(self):
        exit_code, rows = self.execute(
            "import time; print('READY', flush=True); time.sleep(.2)", "pass")
        self.assertEqual((exit_code, rows[0]["status"]), (1, "output_error"))

    def test_receiver_failure(self):
        exit_code, rows = self.execute("raise SystemExit(7)", "pass")
        self.assertEqual((exit_code, rows[0]["receiver_exit_code"]), (1, "7"))
        self.assertEqual(rows[0]["publisher_exit_code"], "")

    def test_publisher_failure(self):
        exit_code, rows = self.execute(
            "import time; print('READY', flush=True); time.sleep(60)", "raise SystemExit(9)")
        self.assertEqual((exit_code, rows[0]["publisher_exit_code"]), (1, "9"))

    def test_startup_timeout(self):
        self.args.startup_timeout_seconds = .2
        exit_code, rows = self.execute("import time; time.sleep(60)", "pass")
        self.assertEqual((exit_code, rows[0]["status"]), (1, "timeout"))

    def test_run_timeout(self):
        self.args.run_timeout_seconds = .2
        exit_code, rows = self.execute(
            "import time; print('READY', flush=True); time.sleep(60)",
            "import time; time.sleep(60)")
        self.assertEqual((exit_code, rows[0]["status"]), (1, "timeout"))

    def test_interruption(self):
        exit_code, rows = self.execute("import time; time.sleep(60)", "pass", interrupt=True)
        self.assertEqual((exit_code, rows[0]["status"]), (130, "interrupted"))

    def test_parser_contract(self):
        receiver_log, publisher_log = output_fixture()
        receiver_path = Path(self.temp.name) / "receiver.log"
        publisher_path = Path(self.temp.name) / "publisher.log"
        receiver_path.write_text(receiver_log, encoding="utf-8")
        publisher_path.write_text(publisher_log, encoding="utf-8")
        require_complete_output(*parse_outputs(receiver_path, publisher_path))
        receiver_path.write_text(receiver_log + "\nchannel=0", encoding="utf-8")
        with self.assertRaises(ValueError):
            parse_outputs(receiver_path, publisher_path)
        receiver_path.write_text("", encoding="utf-8")
        with self.assertRaises(ValueError):
            require_complete_output(*parse_outputs(receiver_path, publisher_path))

    def test_parser_line_endings(self):
        receiver_path = Path(self.temp.name) / "receiver.log"
        publisher_path = Path(self.temp.name) / "publisher.log"
        for newline in ("\n", "\r\n"):
            for trailing in ("", newline):
                with self.subTest(newline=newline, trailing=trailing):
                    receiver_path.write_bytes(
                        (f"READY{newline}channel=0{newline}port=9000" + trailing).encode())
                    publisher_path.write_bytes(("publisher_sent_packets=10" + trailing).encode())
                    self.assertEqual(
                        parse_outputs(str(receiver_path), publisher_path),
                        ({"0": {"port": "9000"}}, {}, {"publisher_sent_packets": "10"}))

    def test_parser_missing_file(self):
        receiver_path = Path(self.temp.name) / "receiver.log"
        publisher_path = Path(self.temp.name) / "publisher.log"
        with self.assertRaises(FileNotFoundError):
            parse_outputs(receiver_path, publisher_path)
        receiver_path.write_text("", encoding="utf-8")
        with self.assertRaises(FileNotFoundError):
            parse_outputs(receiver_path, publisher_path)

    def test_metadata_build_type(self):
        self.args.build_dir = Path(self.temp.name)
        cache = self.args.build_dir / "CMakeCache.txt"
        cases = (
            ("// Build configuration\nCMAKE_BUILD_TYPE:STRING=Release\n", "Release"),
            ("CMAKE_BUILD_TYPE:STRING=Debug\r\n", "Debug"),
            ("CMAKE_BUILD_TYPE:STRING=RelWithDebInfo", "RelWithDebInfo"),
            ("CMAKE_BUILD_TYPE:STRING=\n", "unknown"),
            ("CMAKE_BUILD_TYPE:STRING=", "unknown"),
            ("UNRELATED:STRING=value\n", "unknown"),
            ("", "unknown"),
        )
        for contents, expected in cases:
            with self.subTest(contents=contents):
                cache.write_bytes(contents.encode("utf-8"))
                metadata = collect_metadata(self.args, run_once.PROJECT_ROOT)
                self.assertEqual(metadata["build_type"], expected)

    def test_metadata_missing_cache(self):
        self.args.build_dir = Path(self.temp.name)
        metadata = collect_metadata(self.args, run_once.PROJECT_ROOT)
        self.assertEqual(metadata["build_type"], "unknown")

    def test_metadata_and_cpu_zero(self):
        self.args.publisher_cpu = 0
        metadata = collect_metadata(self.args, run_once.PROJECT_ROOT)
        self.assertEqual(metadata["publisher_cpu"], 0)
        self.assertEqual(metadata["epoll_service_budget"], 64)
        self.assertEqual(metadata["rx_cpu0"], "NA")
        self.assertEqual(metadata["pause"], "off")
        self.assertIn("--cpu", run_once.make_commands(self.args)[1])

    def test_dedicated_pause_option(self):
        for value in (None, "off", "on"):
            with self.subTest(value=value):
                expected = "off" if value is None else value
                argv = [
                    "run_once", "--architecture", "dedicated",
                    "--count", "20", "--rate", "100",
                    "--output-dir", str(self.directory),
                ]
                if value is not None:
                    argv.extend(["--rx-pause", value])

                with patch.object(sys, "argv", argv):
                    args = run_once.parse_arguments()

                self.assertEqual(args.rx_pause, expected)

                receiver_cmd, publisher_cmd = run_once.make_commands(args)
                self.assertEqual(receiver_cmd.count("--rx-pause"), 1)
                option_index = receiver_cmd.index("--rx-pause")
                self.assertEqual(receiver_cmd[option_index + 1], expected)
                self.assertNotIn("--rx-pause", publisher_cmd)

                metadata = collect_metadata(args, run_once.PROJECT_ROOT)
                self.assertEqual(metadata["pause"], expected)

                result_path = Path(self.temp.name) / f"pause-{value}.csv"
                write_result(result_path, metadata, {}, {}, {}, {})
                with result_path.open(newline="", encoding="utf-8") as stream:
                    rows = list(csv.DictReader(stream))
                self.assertEqual([row["pause"] for row in rows],
                                 [expected, expected])

    def test_epoll_pause_off(self):
        for value in (None, "off"):
            with self.subTest(value=value):
                argv = [
                    "run_once", "--architecture", "epoll",
                    "--count", "20", "--rate", "100",
                    "--output-dir", str(self.directory),
                ]
                if value is not None:
                    argv.extend(["--rx-pause", value])

                with patch.object(sys, "argv", argv):
                    args = run_once.parse_arguments()

                self.assertEqual(args.rx_pause, "off")
                receiver_cmd, publisher_cmd = run_once.make_commands(args)
                self.assertNotIn("--rx-pause", receiver_cmd)
                self.assertNotIn("--rx-pause", publisher_cmd)

                metadata = collect_metadata(args, run_once.PROJECT_ROOT)
                self.assertEqual(metadata["pause"], "off")

    def test_pause_option_rejected(self):
        cases = (
            ("epoll", ["--rx-pause", "on"], "only supported by dedicated"),
            ("dedicated", ["--rx-pause", "invalid"], "invalid choice"),
            ("dedicated", ["--rx-pause"], "expected one argument"),
        )
        for architecture, options, message in cases:
            with self.subTest(architecture=architecture, options=options):
                argv = [
                    "run_once", "--architecture", architecture,
                    "--count", "20", "--rate", "100",
                    "--output-dir", str(self.directory),
                    *options,
                ]
                stderr = io.StringIO()
                with patch.object(sys, "argv", argv):
                    with contextlib.redirect_stderr(stderr):
                        with self.assertRaises(SystemExit) as error:
                            run_once.parse_arguments()

                self.assertEqual(error.exception.code, 2)
                self.assertIn(message, stderr.getvalue())
                self.assertFalse(self.directory.exists())


if __name__ == "__main__":
    unittest.main()
