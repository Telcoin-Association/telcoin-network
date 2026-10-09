"""Exercise native population collection and hash-bound completeness, never live capacity."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


WORKLOAD = load("workload")
QUALIFY = WORKLOAD.QUALIFY
OBSERVATIONS = load("observations")
COLLECT = load("collect")
ORIGIN = 1_000_000


def event(source, number, *, terminal=False, outcome="vote", started=ORIGIN + 10,
          latency=1000, watermark=2, generation=None):
    fields = {"event": "committee_request" if terminal else "committee_request_start",
              "generation": generation or ("a" if source == "source-0" else "b") * 32,
              "request_id": number, "allocated_request_count": watermark,
              "process_id": 11 if source == "source-0" else 12,
              "started_unix_us": str(started), "header": f"header-{number}", "peer": "voter"}
    if terminal:
        fields.update(unix_us=str(started + latency), latency_us=str(latency),
                      completed=outcome != "cancelled", success=outcome in {"vote", "missing_parents"},
                      outcome=outcome, error="native RPC error" if outcome == "rpc_error" else "",
                      retry_count=3)
    record = {"target": "network::capacity", "fields": fields}
    encoded = (json.dumps(record, separators=(",", ":")) + "\n").encode()
    return {"source": source, "record": record, "offset": 0,
            "line_sha256": hashlib.sha256(encoded).hexdigest()}


def batch(entries=(), *, caught_up=True, queued=0, started_through=None, source="source-0"):
    if entries:
        source = entries[0]["source"]
    if started_through is None:
        starts = {entry["record"]["fields"]["request_id"] for entry in entries
                  if entry["record"]["fields"]["event"] == "committee_request_start"}
        started_through = 0
        while started_through + 1 in starts:
            started_through += 1
    return {"success": True, "collector_status": "batch" if entries else "empty",
            "trace": {"observations": list(entries), "offset": 10,
                      "size": 10 if caught_up else 20, "caught_up": caught_up, "queued": queued,
                      "allocation": {"generation": ("a" if source == "source-0" else "b") * 32,
                                     "process_id": 11 if source == "source-0" else 12,
                                     "started_through": started_through}, "allocation_holes": 0}}


@contextmanager
def consumer_fences(duration=600, count=4):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "fences.json"
        fences = {}
        for index in range(2):
            source, generation = f"source-{index}", ("a" if index == 0 else "b") * 32
            line = f'tn_primary_vote_observation_allocated{{generation="{generation}"}} {count}'
            fences[source] = {"source": source, "hub": f"hub-{index}", "generation": generation,
                "allocated_request_count": count, "process_id": 11 + index, "process_identity": 1,
                "metrics_url": f"http://127.0.0.1:{9000 + index}",
                "scrape_started_elapsed_seconds": duration, "scrape_completed_elapsed_seconds": duration + 0.01,
                "scrape_started_unix_us": ORIGIN + duration * 1_000_000,
                "metric_line": line, "metric_line_sha256": hashlib.sha256(line.encode()).hexdigest()}
        COLLECT.write_committee_fences(path, ORIGIN, duration, fences)
        with patch.dict(os.environ, {"HUB_CAPACITY_COMMITTEE_FENCES": str(path)}):
            yield


def agent(source="source-0"):
    return {"identity": source, "argv": ["python3", "-B", "-I", str(ROOT / "control.py"),
            "--identity", source, "--observations", "http://127.0.0.1:9400"]}


def fixture(root, phase="candidate", reverse=False, outside=False, request_count=2, successes_only=False,
            outside_terminal=True):
    """Write raw native logs, actual projected operations and independent producer snapshots."""
    sources = ["source-0", "source-1"]
    topology = {"population": {"validators": [
        {"name": f"validator-{index}", "hub": index < 2, "bls_key": f"source-{index}"}
        for index in range(4)]}}
    (root / "topology.json").write_text(json.dumps(topology))
    operations = []
    for index, source in enumerate(sources):
        generation = (("a" if index == 0 else "b") if phase == "baseline" else ("c" if index == 0 else "d")) * 32
        count = request_count + int(outside)
        numbers = list(range(1, request_count + 1))
        records = [event(source, number, watermark=count, generation=generation)
                   for number in (reversed(numbers) if reverse else numbers)]
        records += [event(source, number, terminal=True,
                          outcome="vote" if successes_only or number == 1 else "rpc_error",
                          latency=10_000 if successes_only else 1000 if number == 1 else 2_000_000,
                          watermark=count, generation=generation) for number in numbers]
        if outside:
            records.append(event(source, count, started=ORIGIN + 600_000_000, watermark=count, generation=generation))
            if outside_terminal:
                records.append(event(source, count, terminal=True, started=ORIGIN + 600_000_000,
                                     watermark=count, generation=generation))
        data = bytearray()
        for observation in records:
            observation["offset"] = len(data)
            line = (json.dumps(observation["record"], separators=(",", ":")) + "\n").encode()
            data.extend(line)
            if observation["record"]["fields"]["event"] == "committee_request" and observation["record"]["fields"]["request_id"] <= request_count:
                operations.append(QUALIFY.committee_operation(observation, ORIGIN, 600))
        (root / f"protocol-{index:02}.jsonl").write_bytes(data)
        operations.append({"kind": "collector_telemetry", "scenario": "committee_progress",
                           "source": source, "state": "complete", "pending": 0, "started": request_count,
                           "terminals": request_count, "elapsed_seconds": 600.5, "measurement_start_unix_us": ORIGIN,
                           "follower": {"caught_up": True, "queued": 0}})
    for index in (2, 3):
        (root / f"protocol-{index:02}.jsonl").write_text('{}\n')
    (root / "operations.jsonl").write_text("".join(json.dumps(row) + "\n" for row in operations))
    telemetry = [{"hub": f"hub-{index}", "pid": 11 + index, "elapsed_seconds": 600.5,
                  "workload_completed_before_sample": True,
                  "metrics": f'tn_primary_vote_observation_allocated{{generation="{(("a" if index == 0 else "b") if phase == "baseline" else ("c" if index == 0 else "d")) * 32}"}} {request_count + int(outside)}\n'}
                 for index in range(2)]
    (root / "telemetry-000.jsonl").write_text("".join(json.dumps(row) + "\n" for row in telemetry))
    evidence = {"phase": phase, "envelope": {"committee_peers": 4, "duration_seconds": 600},
                "measurement_start_unix_us": ORIGIN,
                "committee_sources": dict(zip(("hub-0", "hub-1"), sources)),
                "operations": COLLECT.read_operations(root / "operations.jsonl"), "artifacts": []}
    rehash(root, evidence)
    return evidence


def rehash(root, evidence):
    evidence["artifacts"] = [{"path": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                             for path in sorted(root.iterdir()) if path.is_file()]


class CommitteeTests(unittest.TestCase):
    def test_two_consumers_preserve_simultaneous_five_second_no_request_gap(self):
        barrier = threading.Barrier(2)
        rows = []

        def post(_url, payload, **_kwargs):
            barrier.wait(timeout=2)
            return batch(source=payload["identity"])

        with consumer_fences(duration=5, count=0), patch.object(WORKLOAD.CONTROL, "post", post), patch.object(WORKLOAD.time, "monotonic", return_value=5):
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(WORKLOAD.consume_committee, agent(f"source-{index}"),
                                           0, ORIGIN, 5, rows.append) for index in range(2)]
                for future in futures:
                    future.result()
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["kind"] == "collector_telemetry" and row["state"] == "complete" for row in rows))
        self.assertEqual({row["source"] for row in rows}, {"source-0", "source-1"})

    def test_real_errors_cancellations_slow_success_and_measurement_end_tail(self):
        starts = [event("source-0", number, started=ORIGIN + 599_000_000, watermark=4) for number in (1, 2, 3)]
        ends = [event("source-0", number, terminal=True, outcome=outcome,
                      started=ORIGIN + 599_000_000, latency=2_000_000, watermark=4)
                for number, outcome in ((1, "vote"), (2, "rpc_error"), (3, "cancelled"))]
        late = event("source-0", 4, started=ORIGIN + 600_000_000, watermark=4)
        rows = []
        clock = [599]
        polls = iter([(599.9, batch(starts, started_through=3)),
                      (600.2, batch(started_through=3)), (601, batch(ends + [late], started_through=4))])
        def post(*_args, **_kwargs):
            clock[0], response = next(polls)
            return response
        with consumer_fences(), patch.object(WORKLOAD.CONTROL, "post", post), \
                patch.object(WORKLOAD.time, "monotonic", side_effect=lambda: clock[0]):
            WORKLOAD.consume_committee(agent(), 0, ORIGIN, 600, rows.append)
        outcomes = [row for row in rows if row.get("kind") != "collector_telemetry"]
        self.assertEqual([row["success"] for row in outcomes], [True, False, False])
        self.assertEqual([row["cancelled"] for row in outcomes], [False, False, True])
        self.assertEqual([row["latency_ms"] for row in outcomes], [2000, 2000, 2000])
        self.assertEqual(rows[-1]["started"], 3)
        self.assertEqual([row["state"] for row in rows if row.get("kind") == "collector_telemetry"],
                         ["batch", "empty", "complete"])

    def test_follower_queue_http_binding_orphan_and_missing_outcome_are_fatal(self):
        cases = [batch([event("source-0", 1, terminal=True)]),
                 batch([event("source-0", 1), event("source-0", 1)]),
                 batch([event("source-1", 1)]),
                 {"success": False, "collector_status": "error", "rejection_reason": "follower failed"},
                 {"success": False, "collector_status": "error", "rejection_reason": "queue overflow"}]
        for response in cases:
            with self.subTest(response=response), consumer_fences(), patch.object(WORKLOAD.CONTROL, "post", return_value=response), \
                    patch.object(WORKLOAD.time, "monotonic", return_value=601):
                with self.assertRaises(ValueError):
                    WORKLOAD.consume_committee(agent(), 0, ORIGIN, 600, lambda _row: None)
        with consumer_fences(), patch.object(WORKLOAD.CONTROL, "post", side_effect=OSError("HTTP unavailable")), \
                patch.object(WORKLOAD.time, "monotonic", return_value=601):
            with self.assertRaises(OSError):
                WORKLOAD.consume_committee(agent(), 0, ORIGIN, 600, lambda _row: None)
        clock = [599]

        def missing(*_args, **_kwargs):
            clock[0] = 630
            return batch([event("source-0", 1, started=ORIGIN + 599_000_000)])

        with consumer_fences(), patch.object(WORKLOAD.CONTROL, "post", missing), patch.object(WORKLOAD.time, "monotonic", side_effect=lambda: clock[0]):
            with self.assertRaisesRegex(ValueError, "drain incomplete"):
                WORKLOAD.consume_committee(agent(), 0, ORIGIN, 600, lambda _row: None)

    def test_observation_queue_is_bounded_and_identity_failure_is_fatal(self):
        observations = OBSERVATIONS.Observations(["source-0", "source-1"])
        observations.query({"scenario": "committee_progress", "identity": "source-0",
                            "not_before_unix_us": ORIGIN}, timeout=0)
        for number in range(1, 1025):
            observations.ingest("source-0", event("source-0", number, watermark=1025)["record"])
        with self.assertRaisesRegex(ValueError, "allocation exhausted"):
            observations.ingest("source-0", event("source-0", 1025, watermark=1025)["record"])
        with self.assertRaisesRegex(ValueError, "identity failed"):
            observations.ingest("source-0", {"target": "network::capacity", "fields": {"event": "committee_observation_error"}})

    def test_exact_raw_reconciliation_accepts_arbitrary_append_order_and_excludes_end_boundary(self):
        for phase in ("baseline", "candidate"):
            for outside_terminal in (False, True):
                with self.subTest(phase=phase, outside_terminal=outside_terminal), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    evidence = fixture(root, phase, reverse=True, outside=True, outside_terminal=outside_terminal)
                    QUALIFY.verify_artifacts(evidence, root)
                    self.assertEqual(len(evidence["operations"]["committee_progress"]), 4)

    def test_raw_reconciliation_rejects_missing_duplicate_or_modified_evidence(self):
        modes = ["missing_start", "missing_terminal", "duplicate_start", "duplicate_terminal",
                 "lost_trailing_pair", "missing_operation", "duplicate_operation", "changed_latency",
                 "wrong_source", "missing_final_drain", "collector_error", "changed_summary",
                 "missing_watermark", "wrong_pid", "inexact_watermark"]
        for mode in modes:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                evidence = fixture(root)
                protocol = (root / "protocol-00.jsonl").read_text().splitlines()
                operations = [json.loads(line) for line in (root / "operations.jsonl").read_text().splitlines()]
                telemetry = [json.loads(line) for line in (root / "telemetry-000.jsonl").read_text().splitlines()]
                if mode == "missing_start":
                    protocol.pop(0)
                elif mode == "missing_terminal":
                    protocol.pop(3)
                elif mode == "duplicate_start":
                    protocol.append(protocol[0])
                elif mode == "duplicate_terminal":
                    protocol.append(protocol[2])
                elif mode == "lost_trailing_pair":
                    telemetry[0]["metrics"] = telemetry[0]["metrics"].replace(' 2\n', ' 3\n')
                elif mode == "missing_operation":
                    operations.pop(0)
                elif mode == "duplicate_operation":
                    operations.append(copy.deepcopy(operations[0]))
                elif mode == "changed_latency":
                    operations[0]["latency_ms"] += 1
                elif mode == "wrong_source":
                    operations[0]["committee_request"]["source"] = "source-1"
                elif mode == "missing_final_drain":
                    operations.pop(2)
                elif mode == "collector_error":
                    operations[2]["state"] = "error"
                elif mode == "changed_summary":
                    evidence["operations"]["committee_progress"][0]["latency_ms"] += 1
                elif mode == "missing_watermark":
                    telemetry[0]["workload_completed_before_sample"] = False
                elif mode == "wrong_pid":
                    telemetry[0]["pid"] = 777
                elif mode == "inexact_watermark":
                    telemetry[0]["metrics"] = telemetry[0]["metrics"].replace(' 2\n', f' {2**53 + 1}\n')
                (root / "protocol-00.jsonl").write_text("\n".join(protocol) + "\n")
                (root / "operations.jsonl").write_text("".join(json.dumps(row) + "\n" for row in operations))
                (root / "telemetry-000.jsonl").write_text("".join(json.dumps(row) + "\n" for row in telemetry))
                rehash(root, evidence)
                with self.assertRaises(ValueError):
                    QUALIFY.verify_artifacts(evidence, root)


if __name__ == "__main__":
    unittest.main()
