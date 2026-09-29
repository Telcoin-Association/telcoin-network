"""Deterministic collector regressions; no node, traffic generator or Linux host needed."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import tempfile
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("collector", Path(__file__).with_name("collect.py"))
collector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(collector)

HEADER = "sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode ref pointer drops\n"
UDP = HEADER + "0: 0100007F:C350 00000000:0000 07 00000000:00000100 00:00000000 00000000 1000 0 101 2 0 7\n"
UDP6 = HEADER + "1: 00000000000000000000000001000000:C351 00000000000000000000000000000000:0000 07 00000000:00000200 00:00000000 00000000 1000 0 102 2 0 9\n"


def ok(value):
    """Construct a successful observation, including legitimate zero values."""
    return {"status": "ok", "value": value}


class ParserTests(unittest.TestCase):
    """Keep attribution, missing data and counter lifetimes explicit."""

    def test_udp_only_includes_target_sockets(self):
        result = collector.parse_udp(UDP + UDP.splitlines(keepends=True)[1].replace("101", "999"), {101})
        self.assertEqual(result, [{"inode": 101, "local_hex": "0100007F:C350",
                                  "remote_hex": "00000000:0000", "tx_queue_bytes": 0,
                                  "rx_queue_bytes": 256, "drops": 7}])

    def test_udp6_uses_the_same_socket_accounting(self):
        result = collector.parse_udp(UDP6, {102})
        self.assertEqual(result[0]["rx_queue_bytes"], 512)
        self.assertEqual(result[0]["drops"], 9)

    def test_empty_valid_socket_table_is_distinct_from_missing_table(self):
        self.assertEqual(collector.parse_udp(HEADER, {101}), [])
        self.assertEqual(collector.observe(lambda: collector.parse_udp("", {101}))["status"], "error")

    def test_truncated_socket_row_is_an_error(self):
        self.assertEqual(collector.observe(lambda: collector.parse_udp(HEADER + "0: bad", {101}))["status"], "error")

    def test_udp_counter_headers_determine_names(self):
        result = collector.udp_counters("Ip: Forwarding\nIp: 1\nUdp: RcvbufErrors InDatagrams\nUdp: 2 45\n")
        self.assertEqual(result, {"RcvbufErrors": 2, "InDatagrams": 45})

    def test_missing_or_mismatched_counter_headers_fail(self):
        for value in ("", "Udp: A B\nUdp: 1\n"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                collector.udp_counters(value)

    def test_ipv6_counters_are_separate(self):
        self.assertEqual(collector.udp6_counters("Ip6InReceives 99\nUdp6InDatagrams 10\nUdp6RcvbufErrors 3\n"),
                         {"Udp6InDatagrams": 10, "Udp6RcvbufErrors": 3})
        with self.assertRaises(ValueError):
            collector.udp6_counters("")

    def test_counter_delta_and_real_zero(self):
        self.assertEqual(collector.counter_delta(ok({"rx": 10}), ok({"rx": 14})), ok({"rx": 4}))
        self.assertEqual(collector.counter_delta(ok({"rx": 10}), ok({"rx": 10})), ok({"rx": 0}))

    def test_counter_reset_has_no_numeric_delta(self):
        self.assertEqual(collector.counter_delta(ok({"rx": 10}), ok({"rx": 2})),
                         {"status": "reset", "counters": ["rx"]})

    def test_unavailable_and_changed_counter_sets_have_no_numeric_delta(self):
        for before, after in (({"status": "error"}, ok({"rx": 0})),
                              (ok({"rx": 0}), {"status": "error"}),
                              (ok({"rx": 0}), ok({"tx": 0}))):
            with self.subTest(before=before, after=after):
                self.assertEqual(collector.counter_delta(before, after)["status"], "unavailable")

    def test_intermediate_reset_is_not_hidden_by_recovery(self):
        total = collector.accumulate_delta(ok({"rx": 20}), ok({"rx": 20}), ok({"rx": 0}))
        recovered = collector.accumulate_delta(total, ok({"rx": 0}), ok({"rx": 30}))
        self.assertEqual(recovered, {"status": "reset", "counters": ["rx"]})

    def test_intermediate_missing_sample_is_not_hidden_by_recovery(self):
        total = collector.accumulate_delta(ok({"rx": 20}), ok({"rx": 20}), {"status": "error"})
        self.assertEqual(collector.accumulate_delta(total, ok({"rx": 0}), ok({"rx": 30})),
                         {"status": "unavailable"})

    def test_accumulated_deltas_include_every_interval(self):
        self.assertEqual(collector.accumulate_delta(ok({"rx": 4}), ok({"rx": 14}), ok({"rx": 20})),
                         ok({"rx": 10}))

    def test_command_failures_are_preserved(self):
        failed = subprocess.CompletedProcess(["ss"], 1, "", "permission denied")
        with mock.patch.object(collector.subprocess, "run", return_value=failed):
            result = collector.command(["ss"])
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["stderr"], "permission denied")
        for error in (FileNotFoundError("ss"), subprocess.TimeoutExpired(["ss"], 10)):
            with self.subTest(error=error), mock.patch.object(collector.subprocess, "run", side_effect=error):
                self.assertEqual(collector.command(["ss"])["status"], "error")

    def test_command_uses_trusted_path_and_c_locale(self):
        """Inherited executable search paths and locale never reach diagnostics."""
        success = subprocess.CompletedProcess(["ss"], 0, "", "")
        with mock.patch.dict(os.environ, {"PATH": "/untrusted", "LC_ALL": "fr_FR.UTF-8"}), \
                mock.patch.object(collector.subprocess, "run", return_value=success) as run:
            self.assertEqual(collector.command(["ss"])["status"], "ok")
        self.assertEqual(run.call_args.kwargs["env"],
                         {"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"})

    def test_sampling_bounds_reject_nonfinite_and_nonpositive_values(self):
        for value in ("nan", "inf", "-inf", "0", "-1"):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                collector.positive_seconds(value)


class CaptureTests(unittest.TestCase):
    """Exercise the real capture path using procfs/sysfs fixtures and a fake clock."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.proc = self.root / "proc"
        self.sysfs = self.root / "sys"
        self.process = self.proc / "42"
        self.executable = self.root / "node"
        self.put(self.executable, "fixture executable")
        self.process.mkdir(parents=True)
        (self.process / "exe").symlink_to(self.executable)
        self.put(self.process / "stat", "42 (tn worker ) name) S " + "0 " * 18 + "123 0\n")
        self.put(self.proc / "sys/kernel/random/boot_id", "boot-id\n")
        for owner in ("42", "self"):
            namespace = self.proc / owner / "ns/net"
            namespace.parent.mkdir(parents=True)
            namespace.symlink_to("net:[7]")
        (self.process / "fd").mkdir()
        (self.process / "fd/4").symlink_to("socket:[101]")
        (self.process / "fd/5").symlink_to("/dev/null")
        self.put(self.process / "net/udp", UDP)
        self.put(self.process / "net/udp6", HEADER)
        self.put(self.process / "net/snmp", "Udp: InDatagrams RcvbufErrors\nUdp: 10 0\n")
        self.put(self.process / "net/snmp6", "Udp6InDatagrams 0\nUdp6RcvbufErrors 0\n")
        self.put(self.process / "schedstat", "1 2 3\n")
        self.put(self.proc / "stat", "cpu 1 2 3\n")
        self.put(self.proc / "softirqs", "NET_RX: 0\n")
        self.put(self.proc / "net/softnet_stat", "00000001 00000002 00000003\n")
        self.interface = self.sysfs / "class/net/eth0"
        for counter in ("rx_bytes", "rx_packets", "rx_dropped", "rx_errors",
                        "tx_bytes", "tx_packets", "tx_dropped", "tx_errors"):
            self.put(self.interface / "statistics" / counter, "10\n")
        manifest = self.root / "run.json"
        self.put(manifest, '{"run_id": "fixture", "baseline": "unqualified"}')
        self.args = argparse.Namespace(pid=42, interface="eth0", manifest=manifest,
                                       output=self.root / "capture", duration=1.0, interval=1.0)

    def put(self, path, text):
        """Create one fixture file and its parents."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def run_capture(self, between=None, clock=(0.0, 2.0, 3.0), host=None):
        """Use deterministic sampling deadlines and timestamps without sleeping."""
        with mock.patch.object(collector.platform, "system", return_value="Linux"), \
                mock.patch.object(collector, "host_evidence", return_value={}, side_effect=host), \
                mock.patch.object(collector.time, "monotonic", side_effect=clock), \
                mock.patch.object(collector.time, "monotonic_ns", side_effect=range(0, 20_000_000_000, 1_000_000_000)), \
                mock.patch.object(collector.time, "sleep", side_effect=between):
            return collector.capture(self.args, self.proc, self.sysfs)

    def test_pid_identity_handles_parentheses_in_process_name(self):
        identity = collector.process_identity(self.proc, 42)
        self.assertEqual(identity["start_ticks"], 123)
        self.assertEqual(identity["executable"], str(self.executable))
        self.assertEqual(identity["executable_device"], self.executable.stat().st_dev)
        self.assertEqual(identity["executable_inode"], self.executable.stat().st_ino)

    def test_capture_records_actual_deltas_and_never_qualifies(self):
        def advance(delay):
            """Change both interface and socket counters between observations."""
            self.put(self.interface / "statistics/rx_bytes", "30\n")
            self.put(self.process / "net/udp", UDP.replace("2 0 7", "2 0 9"))
        result = self.run_capture(advance)
        self.assertEqual(result, 0)
        summary = json.loads((self.args.output / "summary.json").read_text())
        self.assertEqual(summary["qualification"], "not_evaluated")
        self.assertEqual(summary["samples"], 2)
        self.assertEqual(summary["counter_deltas"]["nic"]["value"]["rx_bytes"], 20)
        self.assertEqual(summary["socket_drop_deltas"]["udp"], ok({"101": ok({"drops": 2})}))
        self.assertEqual(summary["socket_drop_deltas"]["udp6"], ok({}))
        samples = [json.loads(line) for line in (self.args.output / "samples.jsonl").read_text().splitlines()]
        self.assertEqual(samples[0]["sockets"]["udp"]["value"][0]["inode"], 101)

    def test_pid_reuse_invalidates_capture_and_suppresses_deltas(self):
        result = self.run_capture(lambda delay: self.put(self.process / "stat",
                                                       "42 (replacement) S " + "0 " * 18 + "124 0\n"))
        self.assertEqual(result, 3)
        summary = json.loads((self.args.output / "summary.json").read_text())
        self.assertEqual(summary["capture_status"], "invalid_process_identity")
        self.assertEqual(summary["counter_deltas"], {})
        self.assertEqual(summary["socket_drop_deltas"], {})

    def test_interrupted_capture_is_not_complete(self):
        self.assertEqual(self.run_capture(KeyboardInterrupt), 3)
        summary = json.loads((self.args.output / "summary.json").read_text())
        self.assertEqual(summary["capture_status"], "interrupted")
        self.assertEqual(summary["counter_deltas"], {})
        self.assertIsNone(summary["mean_sample_interval_seconds"])

    def test_sample_failure_preserves_incomplete_summary(self):
        with mock.patch.object(collector, "snapshot", side_effect=OSError("fixture failure")):
            self.assertEqual(self.run_capture(), 3)
        summary = json.loads((self.args.output / "summary.json").read_text())
        self.assertEqual(summary["capture_status"], "incomplete")
        self.assertIn("fixture failure", summary["capture_error"])

    def test_capture_preserves_a_middle_reset_after_recovery(self):
        """A final counter above its initial value must not hide a sampled reset."""
        values = iter(("2\n", "30\n"))
        self.assertEqual(self.run_capture(
            lambda delay: self.put(self.interface / "statistics/rx_bytes", next(values)),
            clock=(0.0, 0.25, 0.5, 1.0)), 0)
        summary = json.loads((self.args.output / "summary.json").read_text())
        self.assertEqual(summary["samples"], 3)
        self.assertEqual(summary["counter_deltas"]["nic"], {"status": "reset", "counters": ["rx_bytes"]})

    def test_numeric_non_socket_fd_targets_are_excluded(self):
        """Digits in pipe and terminal names do not identify socket inodes."""
        (self.process / "fd/6").symlink_to("pipe:[123]")
        (self.process / "fd/7").symlink_to("/dev/pts/0")
        self.assertEqual(collector.socket_inodes(self.process), {101})

    def test_final_identity_after_change_invalidates_capture(self):
        """Check the trailing identity even when the leading identity is unchanged."""
        original = collector.snapshot
        calls = 0
        def snapshot(*args):
            """Change only the final sample's trailing identity."""
            nonlocal calls
            sample = original(*args)
            calls += 1
            if calls == 2:
                sample["identity_after"]["value"]["start_ticks"] += 1
            return sample
        with mock.patch.object(collector, "snapshot", side_effect=snapshot):
            self.assertEqual(self.run_capture(), 3)
        summary = json.loads((self.args.output / "summary.json").read_text())
        self.assertEqual(summary["capture_status"], "invalid_process_identity")
        self.assertEqual(summary["counter_deltas"], {})
        self.assertEqual(summary["socket_drop_deltas"], {})

    def test_executable_symlink_change_invalidates_capture(self):
        """A path change is detected even when both paths name the same inode."""
        alias = self.root / "node-alias"
        os.link(self.executable, alias)
        def replace(delay):
            """Repoint exe without changing PID or start time."""
            (self.process / "exe").unlink()
            (self.process / "exe").symlink_to(alias)
        self.assertEqual(self.run_capture(replace), 3)
        summary = json.loads((self.args.output / "summary.json").read_text())
        self.assertEqual(summary["capture_status"], "invalid_process_identity")

    def test_executable_inode_change_invalidates_capture(self):
        """Replacing the file at the same executable path changes its identity."""
        replacement = self.root / "replacement"
        self.put(replacement, "replacement executable")
        self.assertEqual(self.run_capture(lambda delay: replacement.replace(self.executable)), 3)
        summary = json.loads((self.args.output / "summary.json").read_text())
        self.assertEqual(summary["capture_status"], "invalid_process_identity")

    def test_softnet_is_sampled_and_host_limits_are_recorded(self):
        """Keep per-interval softnet observations and their host configuration."""
        updated = "00000005 00000006 00000007\n"
        self.assertEqual(self.run_capture(lambda delay: self.put(self.proc / "net/softnet_stat", updated)), 0)
        samples = [json.loads(line) for line in (self.args.output / "samples.jsonl").read_text().splitlines()]
        self.assertEqual(samples[0]["host_softnet"], ok("00000001 00000002 00000003\n"))
        self.assertEqual(samples[1]["host_softnet"], ok(updated))
        paths = ("sys/net/core/netdev_max_backlog", "sys/net/core/netdev_budget",
                 "sys/net/core/netdev_budget_usecs", "sys/net/ipv4/udp_rmem_min")
        for index, path in enumerate(paths):
            self.put(self.proc / path, str(index + 100))
        with mock.patch.object(collector, "command", return_value={}):
            host = collector.host_evidence(self.proc, 42, "eth0")
        self.assertEqual({path: host["proc"][path] for path in paths},
                         {path: ok(str(index + 100)) for index, path in enumerate(paths)})

    def test_invalid_interface_names_and_missing_statistics_are_rejected(self):
        """Reject path components, option-like names and non-interface directories."""
        (self.sysfs / "class/net/ghost0").mkdir()
        for directory in (self.sysfs / "class/net/statistics", self.sysfs / "class/statistics",
                          self.sysfs / "class/net/-eth0/statistics"):
            directory.mkdir(parents=True)
        for index, name in enumerate((".", "..", "-eth0", "ghost0")):
            with self.subTest(name=name):
                self.args.output = self.root / f"invalid-interface-{index}"
                self.args.interface = name
                with self.assertRaisesRegex(ValueError, "interface"):
                    self.run_capture()
                self.assertFalse(self.args.output.exists())

    def test_mean_interval_uses_measured_sample_timestamps(self):
        """Three sample starts at zero, two and four seconds imply a two-second mean."""
        self.assertEqual(self.run_capture(clock=(0.0, 0.25, 0.5, 1.0)), 0)
        summary = json.loads((self.args.output / "summary.json").read_text())
        self.assertEqual(summary["samples"], 3)
        self.assertEqual(summary["elapsed_seconds"], 4.0)
        self.assertEqual(summary["mean_sample_interval_seconds"], 2.0)

    def test_socket_drop_summary_preserves_lifetime_gaps_and_resets(self):
        """Reappearance, endpoint changes, missing tables and resets stay explicit."""
        cases = (
            ("closed", HEADER, ok({})),
            ("local_changed", UDP.replace("0100007F:C350", "0100007F:C351"), ok({})),
            ("remote_changed", UDP.replace("00000000:0000", "0100007F:C351"), ok({})),
            ("missing", None, {"status": "unavailable"}),
            ("reset", UDP.replace("2 0 7", "2 0 2"),
             ok({"101": {"status": "reset", "counters": ["drops"]}})),
        )
        for name, middle, expected in cases:
            with self.subTest(name=name):
                self.args.output = self.root / name
                self.put(self.process / "net/udp", UDP)
                values = iter((middle, UDP.replace("2 0 7", "2 0 12"), UDP.replace("2 0 7", "2 0 15")))
                def advance(delay):
                    """Recover after one middle sample with different socket evidence."""
                    value = next(values)
                    if value is None:
                        (self.process / "net/udp").unlink()
                    else:
                        self.put(self.process / "net/udp", value)
                self.assertEqual(self.run_capture(advance, clock=(0.0, 0.25, 0.5, 0.75, 1.0)), 0)
                summary = json.loads((self.args.output / "summary.json").read_text())
                self.assertEqual(summary["socket_drop_deltas"]["udp"], expected)

    def test_startup_interrupt_preserves_summary(self):
        """An interrupt during host_before collection still records a partial capture."""
        self.assertEqual(self.run_capture(host=[KeyboardInterrupt(), {}]), 3)
        summary = json.loads((self.args.output / "summary.json").read_text())
        self.assertEqual(summary["capture_status"], "interrupted")
        self.assertEqual(summary["samples"], 0)
        self.assertIsNone(summary["mean_sample_interval_seconds"])
        self.assertEqual(summary["counter_deltas"], {})

    def test_metadata_write_interrupt_preserves_summary(self):
        """An interrupt while writing metadata is covered by the capture guard."""
        write = collector.write_json
        def interrupt(path, value):
            """Interrupt only metadata, leaving the summary writer available."""
            if path.name == "metadata.json":
                raise KeyboardInterrupt
            write(path, value)
        with mock.patch.object(collector, "write_json", side_effect=interrupt):
            self.assertEqual(self.run_capture(), 3)
        summary = json.loads((self.args.output / "summary.json").read_text())
        self.assertEqual(summary["capture_status"], "interrupted")

    def test_final_diagnostics_interrupt_preserves_summary(self):
        """A late interrupt must not discard the already collected samples."""
        self.assertEqual(self.run_capture(host=[{}, KeyboardInterrupt()]), 3)
        summary = json.loads((self.args.output / "summary.json").read_text())
        self.assertEqual(summary["capture_status"], "interrupted")
        self.assertEqual(summary["samples"], 2)
        self.assertEqual(summary["counter_deltas"], {})

    def test_summary_write_failure_returns_partial_capture_code(self):
        """An unwritable summary cannot turn a started capture into a preflight failure."""
        write = collector.write_json
        for name, error in (("io", OSError("fixture summary failure")),
                            ("interrupt", KeyboardInterrupt())):
            with self.subTest(name=name):
                self.args.output = self.root / name
                def fail(path, value):
                    """Fail only the final artifact write."""
                    if path.name == "summary.json":
                        raise error
                    write(path, value)
                with mock.patch.object(collector, "write_json", side_effect=fail):
                    self.assertEqual(self.run_capture(), 3)
                self.assertTrue((self.args.output / "samples.jsonl").exists())

    def test_different_network_namespace_is_rejected_before_capture(self):
        (self.proc / "self/ns/net").unlink()
        (self.proc / "self/ns/net").symlink_to("net:[8]")
        with self.assertRaisesRegex(ValueError, "network namespace"):
            self.run_capture()
        self.assertFalse(self.args.output.exists())

    def test_existing_capture_is_never_overwritten(self):
        self.args.output.mkdir()
        self.put(self.args.output / "metadata.json", "previous capture")
        with self.assertRaises(FileExistsError):
            self.run_capture()
        self.assertEqual((self.args.output / "metadata.json").read_text(), "previous capture")

    def test_missing_counter_remains_unavailable(self):
        (self.process / "net/snmp").unlink()
        self.assertEqual(self.run_capture(), 0)
        summary = json.loads((self.args.output / "summary.json").read_text())
        self.assertEqual(summary["counter_deltas"]["udp"]["status"], "unavailable")

    def test_fd_scan_failure_is_not_an_empty_socket_set(self):
        with mock.patch.object(collector, "socket_inodes", side_effect=PermissionError("fixture")):
            sample = collector.snapshot(self.proc, 42, self.interface)
        self.assertEqual(sample["socket_inodes"]["status"], "error")
        self.assertEqual(sample["sockets"]["udp"]["status"], "unavailable")


@unittest.skipUnless(platform.system() == "Linux", "real procfs/sysfs smoke test requires Linux")
class LinuxSmokeTests(unittest.TestCase):
    """Exercise the collector against an owned socket, without a traffic generator."""

    def test_running_process_socket_is_captured(self):
        with tempfile.TemporaryDirectory() as directory, socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
            udp.bind(("127.0.0.1", 0))
            root = Path(directory)
            manifest = root / "run.json"
            manifest.write_text('{"run_id": "loopback-smoke", "qualification": "not_applicable"}')
            args = argparse.Namespace(pid=os.getpid(), interface="lo", manifest=manifest,
                                      output=root / "capture", duration=0.01, interval=0.01)
            self.assertEqual(collector.capture(args), 0)
            samples = [json.loads(line) for line in (args.output / "samples.jsonl").read_text().splitlines()]
            port = f"{udp.getsockname()[1]:04X}"
            owned = samples[0]["sockets"]["udp"]
            self.assertEqual(owned["status"], "ok")
            self.assertTrue(any(row["local_hex"].endswith(":" + port) for row in owned["value"]))
            metadata = json.loads((args.output / "metadata.json").read_text())
            self.assertEqual(metadata["host_before"]["executable_sha256"]["status"], "ok")
            summary = json.loads((args.output / "summary.json").read_text())
            self.assertEqual(summary["qualification"], "not_evaluated")
            self.assertEqual(summary["counter_deltas"]["nic"]["status"], "ok")


if __name__ == "__main__":
    unittest.main()
