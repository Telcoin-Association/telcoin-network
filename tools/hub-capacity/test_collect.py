"""Collector rejection tests use synthetic proc and metric fixtures, never capacity evidence."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("hub_collect", ROOT / "collect.py")
COLLECT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COLLECT)


def telemetry():
    lines = []
    for network in COLLECT.SWARMS:
        for name, value in {
            "established_connections": 7, "established_connection_limit": 86,
            "ordinary_peers_connected": 7, "dao_observers_connected": 8,
            "inbound_streams_per_connection_limit": 16,
            "receive_credit_per_connection_bytes": 4161790,
            "command_queue_occupancy": 2, "inbound_requests_pending": 3,
            "record_queries_pending": 1, "outbound_requests_pending": 4,
            "px_disconnects_pending": 0,
        }.items():
            lines.append(f'tn_network_{name}{{network="{network}"}} {value}')
        for service in COLLECT.CLASSES[network]:
            lines.append(f'tn_network_serve_tasks_active{{network="{network}",class="{service}"}} 1')
            lines.append(f'tn_network_serve_tasks_limit{{network="{network}",class="{service}"}} 5')
        lines.append(f'tn_network_serve_rejections_total{{network="{network}",class="batch_stream",reason="global_limit"}} 2')
    for name in ("source_address_rows", "source_peer_rows", "source_prefix_rows", "source_connections"):
        lines.append(f'tn_network_{name} 21')
    lines.extend(["progress 8", "dao 8", "tn_network_source_accounting_enabled 1"])
    return "\n".join(lines)


class CollectorTests(unittest.TestCase):
    def test_compact_evidence_preserves_values_order_and_semantic_digest(self):
        evidence = {"samples": [{"elapsed_seconds": 5, "value": 2**80},
                                {"elapsed_seconds": 0, "value": -0.125}],
                    "operations": {"committee_progress": [
                        {"id": "second", "reason": "snowman \u2603, astral \U0001f680, newline\n"},
                        {"id": "first", "success": False, "reason": None}]},
                    "phase": "candidate"}
        expected = json.dumps(evidence, sort_keys=True, separators=(",", ":"),
                              ensure_ascii=True, allow_nan=False).encode("ascii")
        with tempfile.TemporaryDirectory(prefix="capacity-evidence-roundtrip-") as directory:
            path = Path(directory) / "evidence.json"
            reordered = Path(directory) / "reordered.json"
            COLLECT.write_evidence(path, evidence)
            COLLECT.write_evidence(reordered, dict(reversed(list(evidence.items()))))
            self.assertEqual(path.read_bytes(), expected)
            self.assertEqual(reordered.read_bytes(), expected)
            decoded = COLLECT.QUALIFY.read_json(path, maximum_bytes=COLLECT.QUALIFY.EVIDENCE_MAX_BYTES)
            self.assertEqual(decoded, evidence)
            self.assertEqual(COLLECT.QUALIFY.digest(decoded), COLLECT.QUALIFY.digest(evidence))
            self.assertEqual(COLLECT.file_hash(path), COLLECT.QUALIFY.digest(evidence))

    def test_evidence_writer_accepts_exact_reader_cap_and_rejects_next_byte(self):
        self.assertEqual(COLLECT.QUALIFY.MAX_BYTES, 16 * 1024**2)
        self.assertEqual(COLLECT.QUALIFY.EVIDENCE_MAX_BYTES, 24 * 1024**2)
        prefix = b'{"payload":"'
        suffix = b'"}'
        payload = "x" * (COLLECT.QUALIFY.EVIDENCE_MAX_BYTES - len(prefix) - len(suffix))
        with tempfile.TemporaryDirectory(prefix="capacity-evidence-boundary-") as directory:
            root = Path(directory)
            accepted, rejected = root / "accepted.json", root / "rejected.json"
            evidence = {"payload": payload}
            COLLECT.write_evidence(accepted, evidence)
            self.assertEqual(accepted.stat().st_size, COLLECT.QUALIFY.EVIDENCE_MAX_BYTES)
            self.assertEqual(COLLECT.QUALIFY.read_json(
                accepted, maximum_bytes=COLLECT.QUALIFY.EVIDENCE_MAX_BYTES), evidence)
            with self.assertRaisesRegex(ValueError, "exceeds 24 MiB"):
                COLLECT.write_evidence(rejected, {"payload": payload + "x"})
            self.assertFalse(rejected.exists())
            self.assertEqual(list(root.iterdir()), [accepted])
            with accepted.open("ab") as stream:
                stream.write(b" ")
            with self.assertRaisesRegex(ValueError, "exceeds 24 MiB"):
                COLLECT.QUALIFY.read_json(accepted, maximum_bytes=COLLECT.QUALIFY.EVIDENCE_MAX_BYTES)

    def test_evidence_writer_preserves_existing_destination(self):
        with tempfile.TemporaryDirectory(prefix="capacity-evidence-existing-") as directory:
            path = Path(directory) / "evidence.json"
            path.write_bytes(b"existing evidence")
            for evidence in ({"phase": "candidate"}, {"value": float("nan")}):
                with self.subTest(evidence=evidence), self.assertRaises(FileExistsError):
                    COLLECT.write_evidence(path, evidence)
                self.assertEqual(path.read_bytes(), b"existing evidence")

    def test_evidence_writer_removes_partial_nonfinite_output(self):
        with tempfile.TemporaryDirectory(prefix="capacity-evidence-nonfinite-") as directory:
            path = Path(directory) / "evidence.json"
            for value in (float("nan"), float("inf"), -float("inf")):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    COLLECT.write_evidence(path, {"a": "already encoded", "z": value})
                self.assertFalse(path.exists())

    def test_production_log_retention_preserves_large_raw_input_and_scoped_guard(self):
        with tempfile.TemporaryDirectory(prefix="capacity-log-retention-") as directory:
            root = Path(directory)
            source = root / "validator.jsonl"
            with source.open("wb") as stream:
                stream.write(b"raw diagnostic header\n")
                stream.seek(65 * 1024**2 - 20)
                stream.write(b"raw diagnostic tail\n")
            output = root / "retained"
            output.mkdir()
            artifacts = COLLECT.retain_protocol_logs([source], output)
            retained = output / artifacts[0]["path"]
            self.assertEqual(artifacts[0]["sha256"], COLLECT.file_hash(source))
            self.assertEqual(retained.stat().st_size, source.stat().st_size)
            with source.open("rb") as incoming, retained.open("rb") as copied:
                for chunk in iter(lambda: incoming.read(1024 * 1024), b""):
                    self.assertEqual(copied.read(len(chunk)), chunk)
                self.assertEqual(copied.read(1), b"")
            COLLECT.QUALIFY.verify_artifacts({"artifacts": artifacts}, output)
            with self.assertRaisesRegex(ValueError, "exceeds"):
                COLLECT.retain_file(source, output / "topology.json")
            self.assertFalse((output / "topology.json").exists())
            for name in ("workload.log", "protocol-00.jsonl.extra", "unrelated-protocol-00.jsonl"):
                retained.rename(output / name)
                with self.subTest(name=name), self.assertRaisesRegex(ValueError, "exceeds"):
                    COLLECT.QUALIFY.verify_artifacts({"artifacts": [
                        {"path": name, "sha256": artifacts[0]["sha256"]},
                    ]}, output)
                (output / name).rename(retained)

    def test_production_log_budget_exhaustion_removes_partial_copy(self):
        with tempfile.TemporaryDirectory(prefix="capacity-log-exhaustion-") as directory:
            root = Path(directory)
            source = root / "validator.jsonl"
            with source.open("wb") as stream:
                stream.truncate(1024**2 + 1)
            output = root / "retained"
            output.mkdir()
            with mock.patch.object(COLLECT.QUALIFY, "MAX_PROTOCOL_LOG_BYTES", 1024**2):
                with self.assertRaisesRegex(ValueError, "exceeds"):
                    COLLECT.retain_protocol_logs([source], output)
                self.assertEqual(list(output.iterdir()), [])
                # Even a supplied matching hash cannot make an oversized log pass scoring.
                source.rename(output / "protocol-00.jsonl")
                artifact = {"path": "protocol-00.jsonl", "sha256": COLLECT.file_hash(output / "protocol-00.jsonl")}
                with self.assertRaisesRegex(ValueError, "exceeds"):
                    COLLECT.QUALIFY.verify_artifacts({"artifacts": [artifact]}, output)

    def test_production_log_512mib_limit_rejects_next_byte_without_partial_copy(self):
        self.assertEqual(COLLECT.QUALIFY.MAX_PROTOCOL_LOG_BYTES, 512 * 1024**2)
        with tempfile.TemporaryDirectory(prefix="capacity-log-boundary-") as directory:
            root = Path(directory)
            source = root / "validator.jsonl"
            with source.open("wb") as stream:
                stream.truncate(COLLECT.QUALIFY.MAX_PROTOCOL_LOG_BYTES + 1)
            self.assertEqual(source.stat().st_size, 536870913)
            output = root / "retained"
            output.mkdir()
            with self.assertRaisesRegex(ValueError, "exceeds 512 MiB"):
                COLLECT.retain_protocol_logs([source], output)
            self.assertEqual(list(output.iterdir()), [])
            source.rename(output / "protocol-00.jsonl")
            with self.assertRaisesRegex(ValueError, "exceeds 512 MiB"):
                COLLECT.QUALIFY.verify_artifacts({"artifacts": [
                    {"path": "protocol-00.jsonl", "sha256": "0" * 64},
                ]}, output)

    def test_rejected_empty_copy_and_existing_destination_remain_distinct(self):
        with tempfile.TemporaryDirectory(prefix="capacity-log-copy-") as directory:
            root = Path(directory)
            source, destination = root / "source", root / "retained"
            source.write_bytes(b"")
            with self.assertRaisesRegex(ValueError, "empty"):
                COLLECT.retain_file(source, destination)
            self.assertFalse(destination.exists())
            destination.write_bytes(b"existing evidence")
            source.write_bytes(b"replacement")
            with self.assertRaises(FileExistsError):
                COLLECT.retain_file(source, destination)
            self.assertEqual(destination.read_bytes(), b"existing evidence")

    def test_operation_finishing_during_metrics_fetch_is_captured(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = root / "profile.json"
            topology = root / "topology.json"
            protocol_log = root / "validator.jsonl"
            profile.write_text("{}")
            topology.write_text(json.dumps({"population": {"validators": [{"bls_key": "synthetic"}]}}))
            protocol_log.write_bytes(b"synthetic production log\n")
            phase = {"revision": "a" * 40, "profile": {},
                     "binary_sha256": {"telcoin-network": "b" * 64}}
            plan = {"hubs": ["hub"], "baseline": phase, "adapter_command": "synthetic-workload",
                    "envelope": {"duration_seconds": 4, "committee_peers": 1}}
            frozen = {"plan": plan, "plan_sha256": COLLECT.QUALIFY.digest(plan)}
            bindings = {"hubs": {"hub": {"revision": phase["revision"], "profile_path": str(profile),
                        "pid": 42, "metrics_url": "http://synthetic.invalid/metrics",
                        "progress": {"name": "synthetic_progress"}}},
                        "workload": ["synthetic-workload"], "topology_artifact": str(topology),
                        "protocol_logs": [str(protocol_log)]}
            output = root / "evidence"
            clock = {"time": 0.0, "reads": 0, "completed": False}

            def metrics_read(_maximum):
                clock["reads"] += 1
                if clock["reads"] == 3:
                    clock["time"] += 0.5
                    clock["completed"] = True
                    operation = {"scenario": "record_lookup", "id": "closing-operation",
                                 "success": False, "latency_ms": 500,
                                 "elapsed_seconds": clock["time"], "rejection_reason": "fixture"}
                    (output / "operations.jsonl").write_text(json.dumps(operation) + "\n")
                return (f'tn_primary_vote_observation_allocated{{generation="{"a" * 32}"}} '
                        f'{max(0, clock["reads"] - 3)}\n').encode()

            def sleep(seconds):
                clock["time"] += seconds

            child = mock.Mock()
            child.poll.side_effect = lambda: 0 if clock["completed"] else None
            child.wait.return_value = 0
            response = mock.MagicMock()
            response.__enter__.return_value = response
            response.read.side_effect = metrics_read
            # The synthetic deployment bypasses hardware validation. Exercise the real
            # collection loop and retained operation timestamps, without qualifying capacity.
            with mock.patch.object(COLLECT.QUALIFY, "validate_plan"), \
                 mock.patch.object(COLLECT.QUALIFY, "validate_evidence"), \
                 mock.patch.object(COLLECT.QUALIFY, "verify_artifacts"), \
                 mock.patch.object(COLLECT, "validate_process"), \
                 mock.patch.object(COLLECT, "file_hash", return_value="b" * 64), \
                 mock.patch.object(COLLECT, "process_sample", return_value=(
                     {"rss_bytes": 1, "cpu_seconds": 0}, 1, "synthetic proc stat")), \
                 mock.patch.object(COLLECT, "observations", return_value={}), \
                 mock.patch.object(COLLECT.subprocess, "Popen", return_value=child), \
                 mock.patch.object(COLLECT, "metrics_get", side_effect=lambda _url, _deadline: metrics_read(4 * 1024**2 + 1)), \
                 mock.patch.object(COLLECT.time, "monotonic", side_effect=lambda: clock["time"]), \
                 mock.patch.object(COLLECT.time, "time_ns", side_effect=lambda: 1_700_000_000_000_000_000 + int(clock["time"] * 1_000_000_000)), \
                 mock.patch.object(COLLECT.time, "sleep", side_effect=sleep):
                evidence = COLLECT.collect(frozen, bindings, "baseline", output)
            evidence_path = output / "evidence.json"
            decoded = COLLECT.QUALIFY.read_json(evidence_path, maximum_bytes=COLLECT.QUALIFY.EVIDENCE_MAX_BYTES)
            self.assertEqual(decoded, evidence)
            self.assertEqual(COLLECT.QUALIFY.digest(decoded), COLLECT.QUALIFY.digest(evidence))
            self.assertEqual(COLLECT.file_hash(evidence_path), COLLECT.QUALIFY.digest(evidence))
            operation = evidence["operations"]["record_lookup"][0]
            self.assertEqual(operation["elapsed_seconds"], 4.5)
            self.assertFalse(operation["success"])
            self.assertGreaterEqual(evidence["samples"][-1]["elapsed_seconds"], operation["elapsed_seconds"])
            self.assertEqual(clock["reads"], 4)
            fence_file = json.loads((output / "committee-fences.json").read_text())
            producer_fence = fence_file["fences"]["synthetic"]
            self.assertEqual(producer_fence["allocated_request_count"], 0)
            self.assertEqual(producer_fence["scrape_started_elapsed_seconds"], 4)
            self.assertEqual(producer_fence["scrape_started_unix_us"], evidence["measurement_start_unix_us"] + 4_000_000)
            self.assertIn({"path": "committee-fences.json", "sha256": "b" * 64}, evidence["artifacts"])
            self.assertLessEqual(len(evidence["artifacts"]), 64)
            self.assertEqual((output / "protocol-00.jsonl").read_bytes(), protocol_log.read_bytes())
            self.assertIn({"path": "protocol-00.jsonl", "sha256": "b" * 64}, evidence["artifacts"])

    def test_final_scrapes_wake_on_exit_without_extending_drain(self):
        for completion in (628.89, 630.1):
            with self.subTest(completion=completion), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                profile = root / "profile.json"
                topology = root / "topology.json"
                profile.write_text("{}")
                topology.write_text(json.dumps({"population": {"validators": [
                    {"bls_key": "synthetic-0"}, {"bls_key": "synthetic-1"}]}}))
                protocol_logs = [root / f"validator-{index}.jsonl" for index in range(2)]
                for path in protocol_logs:
                    path.write_bytes(b"synthetic production log\n")
                phase = {"revision": "a" * 40, "profile": {},
                         "binary_sha256": {"telcoin-network": "b" * 64}}
                plan = {"hubs": ["hub-0", "hub-1"], "baseline": phase,
                        "adapter_command": "synthetic-workload",
                        "envelope": {"duration_seconds": 600, "committee_peers": 2}}
                frozen = {"plan": plan, "plan_sha256": COLLECT.QUALIFY.digest(plan)}
                bindings = {"hubs": {hub: {
                    "revision": phase["revision"], "profile_path": str(profile), "pid": 42 + index,
                    "metrics_url": f"http://synthetic.invalid/{hub}",
                    "progress": {"name": "synthetic_progress"}}
                    for index, hub in enumerate(plan["hubs"])},
                    "workload": ["synthetic-workload"], "topology_artifact": str(topology),
                    "protocol_logs": [str(path) for path in protocol_logs]}
                output = root / "evidence"
                clock = {"time": 0.0, "completed": False, "stopped": False, "sleeps": 0}
                deadlines = []

                def poll():
                    if clock["stopped"]:
                        return -9
                    if clock["time"] >= completion and not clock["completed"]:
                        clock["completed"] = True
                        operation = {"scenario": "gossip_two_hops", "id": "last-gossip",
                                     "success": False, "latency_ms": 29000,
                                     "elapsed_seconds": completion, "rejection_reason": "timeout"}
                        (output / "operations.jsonl").write_text(json.dumps(operation) + "\n")
                    return 0 if clock["completed"] else None

                def wait(timeout):
                    status = poll()
                    if status is not None:
                        return status
                    if completion <= clock["time"] + timeout:
                        clock["time"] = completion
                        return poll()
                    clock["time"] += timeout
                    raise COLLECT.subprocess.TimeoutExpired("synthetic-workload", timeout)

                def sleep(seconds):
                    clock["time"] += seconds
                    # A scheduling delay puts the last ordinary scrape at 627.72s,
                    # as in the failed run. Later sleeps preserve the normal cadence.
                    if clock["sleeps"] == 0:
                        clock["time"] += 1.72
                    clock["sleeps"] += 1

                def metrics(url, deadline):
                    deadlines.append((clock["time"], deadline))
                    end = clock["time"] + 0.5
                    if end >= deadline:
                        clock["time"] = deadline
                        raise TimeoutError("synthetic metrics body deadline")
                    clock["time"] = end
                    generation = "a" * 32 if url.endswith("hub-0") else "c" * 32
                    return f'tn_primary_vote_observation_allocated{{generation="{generation}"}} 0\n'.encode()

                child = mock.Mock()
                child.poll.side_effect = poll
                child.wait.side_effect = wait
                child.terminate.side_effect = lambda: clock.update(stopped=True)
                child.kill.side_effect = lambda: clock.update(stopped=True)
                # Use the real collection loop with a deterministic clock and two hubs.
                # Hardware and capacity scoring are outside this timing regression.
                with mock.patch.object(COLLECT.QUALIFY, "validate_plan"), \
                     mock.patch.object(COLLECT.QUALIFY, "validate_evidence"), \
                     mock.patch.object(COLLECT.QUALIFY, "verify_artifacts"), \
                     mock.patch.object(COLLECT, "validate_process"), \
                     mock.patch.object(COLLECT, "file_hash", return_value="b" * 64), \
                     mock.patch.object(COLLECT, "process_sample", return_value=(
                         {"rss_bytes": 1, "cpu_seconds": 0}, 1, "synthetic proc stat")), \
                     mock.patch.object(COLLECT, "observations", return_value={}), \
                     mock.patch.object(COLLECT.subprocess, "Popen", return_value=child), \
                     mock.patch.object(COLLECT, "metrics_get", side_effect=metrics), \
                     mock.patch.object(COLLECT.time, "monotonic", side_effect=lambda: clock["time"]), \
                     mock.patch.object(COLLECT.time, "time_ns", side_effect=lambda:
                         1_700_000_000_000_000_000 + int(clock["time"] * 1_000_000_000)), \
                     mock.patch.object(COLLECT.time, "sleep", side_effect=sleep):
                    if completion < 630:
                        evidence = COLLECT.collect(frozen, bindings, "baseline", output)
                        self.assertAlmostEqual(evidence["samples"][-1]["elapsed_seconds"], completion)
                        self.assertLess(clock["time"], 630)
                    else:
                        with self.assertRaisesRegex(TimeoutError, "synthetic metrics body deadline"):
                            COLLECT.collect(frozen, bindings, "baseline", output)
                        self.assertEqual(clock["time"], 630)
                        self.assertTrue(clock["stopped"])
                        self.assertFalse((output / "evidence.json").exists())
                self.assertTrue(deadlines)
                for start, deadline in deadlines:
                    self.assertAlmostEqual(deadline, min(start + 2, 630))
                rows = [json.loads(line) for path in sorted(output.glob("telemetry-*.jsonl"))
                        for line in path.read_text().splitlines()]
                completed = [row for row in rows if row["workload_completed_before_sample"]]
                if completion < 630:
                    self.assertEqual({row["hub"] for row in completed}, set(plan["hubs"]))
                    self.assertEqual(len(completed), 2)
                    self.assertTrue(all(row["scrape_started_elapsed_seconds"] >= completion
                                        for row in completed))
                else:
                    self.assertEqual(completed, [])

    def test_full_mapping_and_worker_omission(self):
        binding = {"progress": {"name": "progress"}, "dao_connected": {"name": "dao"}}
        parsed = COLLECT.parse_metrics(telemetry())
        result = COLLECT.observations(parsed, binding, "candidate")
        self.assertEqual(result["tasks"]["batch_stream"], 2)
        self.assertEqual(result["tasks"]["epoch_stream"], 1)
        self.assertEqual(result["swarms"]["worker-1"]["queue_occupancy"], 10)
        self.assertEqual(result["source_rows"], 21)
        self.assertEqual(result["dao_connected"], 8)
        self.assertEqual(sum(result["swarms"]["worker-0"]["rejections"].values()), 2)
        incomplete = "\n".join(line for line in telemetry().splitlines() if 'network="worker-1"' not in line)
        with self.assertRaisesRegex(ValueError, "missing metric"):
            COLLECT.observations(COLLECT.parse_metrics(incomplete), binding, "candidate")

    def test_capacity_sampling_preserves_required_validation(self):
        binding = {"progress": {"name": "progress"}}
        expected = COLLECT.observations(COLLECT.parse_metrics(telemetry()), binding, "candidate")
        raw = telemetry() + '\nreth_unrelated_histogram_bucket{le="+Inf"} 9000'
        selected = COLLECT.capacity_metrics(raw, "progress")
        self.assertEqual(COLLECT.observations(selected, binding, "candidate"), expected)
        self.assertNotIn(("reth_unrelated_histogram_bucket", (("le", "+Inf"),)), selected)
        for invalid in ("progress NaN", "tn_network_source_address_rows 3",
                        "tn_network_connections{broken} 1"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                COLLECT.capacity_metrics(raw + "\n" + invalid, "progress")

    def test_duplicate_nonfinite_and_malformed_metrics(self):
        for text in ('metric 1\nmetric 2', 'metric NaN', 'metric inf', 'metric{a="x",a="y"} 1',
                     'metric{broken} 1'):
            with self.subTest(text=text), self.assertRaises(ValueError):
                COLLECT.parse_metrics(text)
        escaped = COLLECT.parse_metrics('metric{a="quote\\\" and slash\\\\"} 2')
        self.assertEqual(COLLECT.select(escaped, "metric", {"a": 'quote" and slash\\'}), 2)
        with self.assertRaisesRegex(ValueError, "integer"):
            COLLECT.select(COLLECT.parse_metrics('metric 1.5'), "metric")

    def test_process_cpu_rss_and_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            process = Path(directory) / "42"
            process.mkdir()
            fields = ["0"] * 22
            fields[0], fields[11], fields[12], fields[19], fields[21] = "S", "200", "50", "1000", "256"
            (process / "stat").write_text("42 (comm with ) spaces) " + " ".join(fields))
            sample, identity, raw = COLLECT.process_sample(42, Path(directory), ticks=100, page_size=4096)
            self.assertEqual(sample, {"rss_bytes": 1048576, "cpu_seconds": 2.5})
            self.assertEqual(identity, 1000)
            self.assertIn("comm with ) spaces", raw)
            fields[19] = "1001"
            (process / "stat").write_text("42 (replacement) " + " ".join(fields))
            self.assertNotEqual(COLLECT.process_sample(42, Path(directory), 100, 4096)[1], identity)

    def test_raw_artifacts_and_failed_operations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = COLLECT.RawLog(root)
            log.append({"metric": "raw evidence"})
            artifacts = log.artifacts()
            self.assertEqual(artifacts[0]["sha256"], COLLECT.file_hash(root / artifacts[0]["path"]))
            operations = root / "operations.jsonl"
            failed = {"scenario": "record_lookup", "id": "1", "success": False,
                      "latency_ms": 100, "elapsed_seconds": 1, "rejection_reason": "timeout"}
            operations.write_text(json.dumps(failed) + "\n")
            result = COLLECT.read_operations(operations)
            self.assertFalse(result["record_lookup"][0]["success"])
            self.assertEqual(result["record_lookup"][0]["rejection_reason"], "timeout")
            self.assertEqual(set(result), COLLECT.QUALIFY.SCENARIOS)


if __name__ == "__main__":
    unittest.main()
