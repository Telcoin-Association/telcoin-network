"""Exercise producer-fenced drain with real file appends, never live capacity claims."""

import hashlib
from contextlib import contextmanager
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import socketserver
import threading
import time
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent


def load(name):
    spec = importlib.util.spec_from_file_location(f"fence_{name}", ROOT / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


WORKLOAD, OBSERVATIONS, COLLECT = (load(name) for name in ("workload", "observations", "collect"))
MEASUREMENT = 1_700_000_000_000_000
DURATION = 600
CUTOFF = MEASUREMENT + DURATION * 1_000_000
GENERATION = "a" * 32
SOURCE = "hub-1"
AGENT = {"identity": SOURCE, "argv": ["python3", "-B", "-I", str(ROOT / "control.py"),
                                     "--identity", SOURCE, "--observations", "http://127.0.0.1:1"]}


def fence(count, **changes):
    line = f'tn_primary_vote_observation_allocated{{generation="{GENERATION}"}} {count}'
    return {"source": SOURCE, "hub": "hub", "generation": GENERATION,
            "allocated_request_count": count, "process_id": 1, "process_identity": 2,
            "metrics_url": "http://127.0.0.1:9000", "scrape_started_elapsed_seconds": 600,
            "scrape_completed_elapsed_seconds": 600.01, "scrape_started_unix_us": CUTOFF,
            "metric_line": line, "metric_line_sha256": hashlib.sha256(line.encode()).hexdigest(),
            **changes}


def native(request_id, started, *, terminal=False, generation=GENERATION, process_id=1):
    fields = {"event": "committee_request" if terminal else "committee_request_start",
              "generation": generation, "request_id": request_id,
              "allocated_request_count": request_id, "process_id": process_id,
              "started_unix_us": started, "header": "header", "peer": "peer"}
    if terminal:
        fields.update({"unix_us": started + 19_000, "latency_us": 19_000,
                       "outcome": "cancelled", "success": False, "completed": False,
                       "retry_count": 0})
    return {"target": "network::capacity", "fields": fields}


def empty(through=0, *, generation=None, process_id=None, holes=0):
    return {"success": True, "collector_status": "empty", "trace": {
        "observations": [], "offset": 0, "size": 0, "caught_up": True, "queued": 0,
        "allocation": {"generation": generation, "process_id": process_id,
                       "started_through": through}, "allocation_holes": holes}}


class CommitteeFenceTests(unittest.TestCase):
    def test_late_pre_cutoff_append_after_transient_eof_is_drained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log, fences = root / "native.jsonl", root / "fences.json"
            log.write_text(json.dumps(native(1, MEASUREMENT - 100_000)) + "\n" +
                           json.dumps(native(1, MEASUREMENT - 100_000, terminal=True)) + "\n")
            COLLECT.write_committee_fences(fences, MEASUREMENT, DURATION, {SOURCE: fence(2)})
            observations = OBSERVATIONS.Observations([SOURCE])
            stop, first_empty = threading.Event(), threading.Event()
            rows, failures = [], []

            class FollowPath:
                def open(self, mode):
                    return log.open(mode)

                def stat(self):
                    if stop.is_set():
                        raise OSError("fixture follower stopped")
                    return log.stat()

            follower = threading.Thread(target=OBSERVATIONS.follow,
                                        args=(observations, SOURCE, FollowPath()))
            follower.start()

            def record(row):
                rows.append(row)
                if row.get("kind") == "collector_telemetry" and row["state"] == "empty":
                    first_empty.set()

            def consume():
                try:
                    WORKLOAD.consume_committee(AGENT, time.monotonic() - DURATION,
                                               MEASUREMENT, DURATION, record)
                except BaseException as error:
                    failures.append(error)

            consumer = threading.Thread(target=consume)
            try:
                with observations.condition:
                    self.assertTrue(observations.condition.wait_for(
                        lambda: observations.positions.get(SOURCE) == (log.stat().st_size, log.stat().st_size),
                        timeout=2))
                with mock.patch.dict(os.environ, {"HUB_CAPACITY_COMMITTEE_FENCES": str(fences)}), \
                     mock.patch.object(WORKLOAD.CONTROL, "post", side_effect=lambda _url, payload, **_kw:
                                       observations.query(payload, timeout=0.05)):
                    consumer.start()
                    self.assertTrue(first_empty.wait(2), "collector did not stay open at transient EOF")
                    self.assertTrue(consumer.is_alive(), "collector completed before fenced start publication")
                    self.assertEqual(rows[0]["allocation"]["started_through"], 1)
                    self.assertTrue(rows[0]["follower"]["caught_up"])
                    with log.open("a") as stream:
                        stream.write(json.dumps(native(2, CUTOFF - 9_000)) + "\n")
                        stream.write(json.dumps(native(2, CUTOFF - 9_000, terminal=True)) + "\n")
                        stream.flush()
                    consumer.join(2)
                    self.assertFalse(consumer.is_alive())
                self.assertEqual(failures, [])
                operations = [row for row in rows if row.get("kind") != "collector_telemetry"]
                self.assertEqual(len(operations), 1)
                self.assertFalse(operations[0]["success"])
                self.assertTrue(operations[0]["cancelled"])
                self.assertEqual(operations[0]["latency_ms"], 19)
                self.assertEqual(rows[-1]["state"], "complete")
                self.assertEqual((rows[-1]["started"], rows[-1]["terminals"], rows[-1]["pending"]), (1, 1, 0))
                self.assertEqual(rows[-1]["allocation"]["started_through"], 2)
            finally:
                stop.set()
                follower.join(2)
                if consumer.ident is not None:
                    consumer.join(2)
                self.assertFalse(follower.is_alive())
                self.assertFalse(consumer.is_alive())

    def invoke(self, path, responses, *, record=None):
        clock = {"now": 600.0}
        rows, deadlines = [], []
        iterator = iter(responses)

        def post(_url, _payload, *, deadline):
            deadlines.append(deadline)
            try:
                response = next(iterator)
            except StopIteration:
                clock["now"] = 630
                return empty()
            clock["now"] += 0.1
            return response() if callable(response) else response

        def emit(row):
            rows.append(row)
            if record:
                record(row)

        with mock.patch.dict(os.environ, {"HUB_CAPACITY_COMMITTEE_FENCES": str(path)}), \
             mock.patch.object(WORKLOAD.CONTROL, "post", side_effect=post), \
             mock.patch.object(WORKLOAD.time, "monotonic", side_effect=lambda: clock["now"]):
            WORKLOAD.consume_committee(AGENT, 0, MEASUREMENT, DURATION, emit)
        self.assertTrue(all(deadline == 630 for deadline in deadlines))
        return rows

    def test_quiet_zero_count_needs_no_post_cutoff_request(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fences.json"
            COLLECT.write_committee_fences(path, MEASUREMENT, DURATION, {SOURCE: fence(0)})
            rows = self.invoke(path, [empty()])
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["state"], "complete")
            self.assertEqual(rows[0]["started"], 0)

    def test_out_of_order_prefix_requires_hole_and_not_post_window_terminal(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fences.json"
            COLLECT.write_committee_fences(path, MEASUREMENT, DURATION, {SOURCE: fence(2)})
            observations = OBSERVATIONS.Observations([SOURCE])
            observations.positions[SOURCE] = (0, 0)

            def publish(request_id):
                observations.ingest(SOURCE, native(request_id, CUTOFF))
                return observations.query({"scenario": "committee_progress", "identity": SOURCE,
                                           "not_before_unix_us": MEASUREMENT}, timeout=0)

            rows = self.invoke(path, [lambda: publish(2), lambda: publish(1)])
            self.assertEqual([row["state"] for row in rows], ["batch", "complete"])
            self.assertEqual(rows[0]["allocation"]["started_through"], 0)
            self.assertEqual(rows[0]["allocation_holes"], 1)
            self.assertEqual(rows[-1]["allocation"]["started_through"], 2)
            self.assertEqual(rows[-1]["allocation_holes"], 0)
            self.assertTrue(all(row["started"] == row["terminals"] == 0 for row in rows))

    def test_missing_fence_or_prefix_hole_fails_at_original_tail(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fences.json"
            with self.assertRaisesRegex(ValueError, "drain incomplete"):
                self.invoke(path, [empty()])
            COLLECT.write_committee_fences(path, MEASUREMENT, DURATION, {SOURCE: fence(2)})
            with self.assertRaisesRegex(ValueError, "drain incomplete"):
                self.invoke(path, [empty(0, generation=GENERATION, process_id=1, holes=1)])

    def test_selected_start_requires_real_terminal_and_original_follower_drain(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fences.json"
            COLLECT.write_committee_fences(path, MEASUREMENT, DURATION, {SOURCE: fence(1)})
            observations = OBSERVATIONS.Observations([SOURCE])
            observations.positions[SOURCE] = (0, 0)
            observations.ingest(SOURCE, native(1, CUTOFF - 1))
            response = observations.query({"scenario": "committee_progress", "identity": SOURCE,
                                           "not_before_unix_us": MEASUREMENT}, timeout=0)
            with self.assertRaisesRegex(ValueError, "drain incomplete"):
                self.invoke(path, [response])
            COLLECT.write_committee_fences(path, MEASUREMENT, DURATION, {SOURCE: fence(0)})
            for fields in ({"queued": 1}, {"size": 1, "caught_up": False}):
                with self.subTest(fields=fields):
                    response = empty()
                    response["trace"].update(fields)
                    with self.assertRaisesRegex(ValueError, "drain incomplete"):
                        self.invoke(path, [response])

    def test_source_generation_process_and_frozen_fence_errors_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fences.json"
            for changes, response, message in [
                ({"source": "other"}, empty(), "source mismatch"),
                ({}, empty(2, generation="b" * 32, process_id=1), "generation or process mismatch"),
                ({}, empty(2, generation=GENERATION, process_id=2), "generation or process mismatch"),
            ]:
                with self.subTest(changes=changes, response=response):
                    COLLECT.write_committee_fences(path, MEASUREMENT, DURATION, {SOURCE: fence(2, **changes)})
                    with self.assertRaisesRegex(ValueError, message):
                        self.invoke(path, [response])
            COLLECT.write_committee_fences(path, MEASUREMENT, DURATION, {SOURCE: fence(2)})

            def replace(_row):
                COLLECT.write_committee_fences(path, MEASUREMENT, DURATION, {SOURCE: fence(3)})

            with self.assertRaisesRegex(ValueError, "fence changed"):
                self.invoke(path, [empty(), empty()], record=replace)

    def test_stale_measurement_scrape_precision_and_file_bounds_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fences.json"
            for changes in ({"scrape_started_elapsed_seconds": 599.999},
                            {"scrape_started_unix_us": CUTOFF - 1},
                            {"scrape_completed_elapsed_seconds": 630},
                            {"scrape_completed_elapsed_seconds": float("inf")}):
                with self.subTest(changes=changes):
                    # Bypass the writer only for malformed-input validation.
                    path.write_text(json.dumps({"version": 1, "measurement_start_unix_us": MEASUREMENT,
                        "window_end_unix_us": CUTOFF, "fences": {SOURCE: fence(0, **changes)}}))
                    with self.assertRaisesRegex(ValueError, "post-cutoff drain"):
                        WORKLOAD.committee_fence(path, SOURCE, MEASUREMENT, DURATION)
            COLLECT.write_committee_fences(path, MEASUREMENT - 1, DURATION, {SOURCE: fence(0)})
            with self.assertRaisesRegex(ValueError, "measurement binding"):
                WORKLOAD.committee_fence(path, SOURCE, MEASUREMENT, DURATION)
            COLLECT.write_committee_fences(path, MEASUREMENT, DURATION, {SOURCE: fence(2**53 + 1)})
            with self.assertRaisesRegex(ValueError, "exact native gauge"):
                WORKLOAD.committee_fence(path, SOURCE, MEASUREMENT, DURATION)
            path.write_bytes(b"x" * (32 * 1024 + 1))
            with self.assertRaisesRegex(ValueError, "bounded storage"):
                WORKLOAD.committee_fence(path, SOURCE, MEASUREMENT, DURATION)

    def test_response_at_absolute_deadline_cannot_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fences.json"
            COLLECT.write_committee_fences(path, MEASUREMENT, DURATION, {SOURCE: fence(0)})
            rows = []
            clock = {"now": 600}

            def late_response(*_args, **kwargs):
                self.assertEqual(kwargs["deadline"], 630)
                clock["now"] = 630
                return empty()

            with mock.patch.dict(os.environ, {"HUB_CAPACITY_COMMITTEE_FENCES": str(path)}), \
                 mock.patch.object(WORKLOAD.CONTROL, "post", side_effect=late_response), \
                 mock.patch.object(WORKLOAD.time, "monotonic", side_effect=lambda: clock["now"]):
                with self.assertRaisesRegex(ValueError, "drain incomplete"):
                    WORKLOAD.consume_committee(AGENT, 0, MEASUREMENT, DURATION, rows.append)
            self.assertEqual(rows, [])

    def test_duplicate_restart_and_untrusted_large_id_keep_bounded_coverage(self):
        observations = OBSERVATIONS.Observations([SOURCE])
        observations.ingest(SOURCE, native(2**64 - 1, CUTOFF))
        self.assertEqual(len(observations.allocations[SOURCE]["ahead"]), 1)
        self.assertEqual(observations.allocations[SOURCE]["started_through"], 0)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            observations.ingest(SOURCE, native(2**64 - 1, CUTOFF))
        with self.assertRaisesRegex(ValueError, "generation or process changed"):
            observations.ingest(SOURCE, native(1, CUTOFF, generation="b" * 32))
        for request_id in range(2, 1025):
            observations.ingest(SOURCE, native(request_id, MEASUREMENT - 1))
        self.assertEqual(len(observations.allocations[SOURCE]["ahead"]), 1024)
        with self.assertRaisesRegex(ValueError, "coverage allocation exhausted"):
            observations.ingest(SOURCE, native(1025, MEASUREMENT - 1))
        self.assertEqual(len(observations.allocations[SOURCE]["ahead"]), 1024)
        observations.ingest(SOURCE, native(1, MEASUREMENT - 1))
        self.assertEqual(observations.allocations[SOURCE]["started_through"], 1024)
        self.assertEqual(len(observations.allocations[SOURCE]["ahead"]), 1)
        self.assertEqual(len(observations.committee[SOURCE]), 1024)


@contextmanager
def metrics_server(reply):
    requests = []

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            self.connection.settimeout(2)
            requests.append(self.rfile.readline().strip())
            while self.rfile.readline().strip():
                pass
            try:
                reply(self.connection)
            except OSError:
                pass

    class Server(socketserver.ThreadingTCPServer):
        daemon_threads = True

    with Server(("127.0.0.1", 0), Handler) as endpoint:
        worker = threading.Thread(target=endpoint.serve_forever, kwargs={"poll_interval": 0.01})
        worker.start()
        try:
            yield f"http://127.0.0.1:{endpoint.server_address[1]}/metrics", requests
        finally:
            endpoint.shutdown()
            worker.join(2)


class MetricsFenceTests(unittest.TestCase):
    def test_collection_freezes_first_dual_clock_fence_and_retains_raw_provenance(self):
        self.collection_fixture()

    def test_final_sample_processing_cannot_cross_absolute_tail(self):
        with self.assertRaisesRegex(ValueError, "sample processing exceeded workload drain deadline"):
            self.collection_fixture("processing_deadline")

    def test_existing_sample_sleep_is_capped_by_remaining_tail(self):
        with self.assertRaisesRegex(ValueError, "drain deadline"):
            self.collection_fixture("sleep_cap")
        self.assertEqual(self.collection_sleeps[-1], 0.5)

    def collection_fixture(self, mode="dual_clock"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile, topology, protocol = root / "profile", root / "topology", root / "native"
            profile.write_text("{}")
            topology.write_text(json.dumps({"population": {"validators": [{"bls_key": SOURCE}]}}))
            protocol.write_bytes(b"fixture retained native log\n")
            phase = {"revision": "a" * 40, "profile": {},
                     "binary_sha256": {"telcoin-network": "b" * 64}}
            plan = {"hubs": ["hub"], "baseline": phase, "adapter_command": "fixture",
                    "envelope": {"duration_seconds": 4, "committee_peers": 1}}
            bindings = {"hubs": {"hub": {"revision": phase["revision"], "profile_path": str(profile),
                "pid": 1, "metrics_url": "http://127.0.0.1:9000", "progress": {"name": "fixture"}}},
                "workload": ["fixture"], "topology_artifact": str(topology), "protocol_logs": [str(protocol)]}
            output = root / "evidence"
            clock = {"now": 0, "reads": 0}
            self.collection_sleeps = []

            def scrape(_url, deadline):
                self.assertEqual(deadline, min(clock["now"] + 2, 34))
                clock["reads"] += 1
                return f'tn_primary_vote_observation_allocated{{generation="{GENERATION}"}} {clock["reads"]}\n'.encode()

            def wall_now():
                # The first monotonic post-cutoff scrape is still one microsecond before native T1.
                wall = 3_999_999 if clock["now"] == 4 else int(clock["now"] * 1_000_000)
                return (MEASUREMENT + wall) * 1000

            def start_child(*_args, **kwargs):
                self.assertEqual(kwargs["env"]["HUB_CAPACITY_COMMITTEE_FENCES"],
                                 str((output / "committee-fences.json").resolve()))
                (output / "operations.jsonl").write_text("")
                return child

            child = mock.Mock()
            child.poll.side_effect = lambda: 0 if clock["reads"] >= 4 else None

            def wait(timeout):
                if child.poll() is None:
                    pause(timeout)
                    raise COLLECT.subprocess.TimeoutExpired("fixture", timeout)
                return 0

            child.wait.side_effect = wait

            def sample_observations(*_args):
                if mode == "processing_deadline" and clock["reads"] == 5:
                    clock["now"] = 34
                return {}

            def pause(seconds):
                self.collection_sleeps.append(seconds)
                if mode == "sleep_cap" and clock["reads"] == 2:
                    clock["now"] = 33.5
                else:
                    clock["now"] += seconds

            with mock.patch.object(COLLECT.QUALIFY, "validate_plan"), \
                 mock.patch.object(COLLECT.QUALIFY, "validate_evidence"), \
                 mock.patch.object(COLLECT.QUALIFY, "verify_artifacts"), \
                 mock.patch.object(COLLECT, "validate_process"), \
                 mock.patch.object(COLLECT, "file_hash", return_value="b" * 64), \
                 mock.patch.object(COLLECT, "process_sample", return_value=({"rss_bytes": 1, "cpu_seconds": 0}, 1, "proc stat")), \
                 mock.patch.object(COLLECT, "observations", side_effect=sample_observations), \
                 mock.patch.object(COLLECT, "metrics_get", side_effect=scrape), \
                 mock.patch.object(COLLECT, "RawLog", wraps=COLLECT.RawLog) as raw_logs, \
                 mock.patch.object(COLLECT.subprocess, "Popen", side_effect=start_child), \
                 mock.patch.object(COLLECT.time, "monotonic", side_effect=lambda: clock["now"]), \
                 mock.patch.object(COLLECT.time, "time_ns", side_effect=wall_now), \
                 mock.patch.object(COLLECT.time, "sleep", side_effect=pause):
                evidence = COLLECT.collect({"plan": plan, "plan_sha256": COLLECT.QUALIFY.digest(plan)},
                                           bindings, "baseline", output)
            saved = json.loads((output / "committee-fences.json").read_text())["fences"][SOURCE]
            self.assertEqual((saved["allocated_request_count"], saved["scrape_started_elapsed_seconds"]), (4, 6))
            self.assertEqual(saved["scrape_started_unix_us"], MEASUREMENT + 6_000_000)
            raw = [json.loads(line) for line in (output / "telemetry-000.jsonl").read_text().splitlines()]
            self.assertIsNone(raw[2]["committee_fence"])
            self.assertEqual(raw[-1]["committee_fence"], saved)
            self.assertIn(f' {5}\n', raw[-1]["metrics"])
            self.assertEqual(evidence["committee_sources"], {"hub": SOURCE})
            self.assertIn({"path": "committee-fences.json", "sha256": "b" * 64}, evidence["artifacts"])
            self.assertLessEqual(len(evidence["artifacts"]), 64)
            self.assertEqual(raw_logs.call_args.kwargs["maximum_segments"], 64 - 4 - 1)

    def test_metrics_get_keeps_request_body_and_rejects_redirect_without_retry(self):
        for status in (200, 302):
            with self.subTest(status=status):
                def reply(connection):
                    connection.sendall(f"HTTP/1.1 {status} status\r\nContent-Length: 3\r\n"
                                       "Connection: close\r\n\r\n".encode() + b"raw")

                with metrics_server(reply) as (url, requests):
                    if status == 200:
                        self.assertEqual(COLLECT.metrics_get(url, time.monotonic() + 0.3), b"raw")
                    else:
                        with self.assertRaisesRegex(ValueError, "metrics HTTP status 302"):
                            COLLECT.metrics_get(url, time.monotonic() + 0.3)
                    self.assertEqual(requests, [b"GET /metrics HTTP/1.1"])

    def test_metrics_deadline_bounds_dripping_headers_and_partial_or_dripping_body(self):
        for mode in ("header", "partial", "body"):
            with self.subTest(mode=mode):
                def reply(connection):
                    if mode == "header":
                        data = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n"
                    else:
                        connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
                        data = b"x" * 100
                    if mode == "partial":
                        connection.sendall(b"x")
                        time.sleep(0.4)
                    else:
                        for byte in data:
                            connection.sendall(bytes([byte]))
                            time.sleep(0.02)

                with metrics_server(reply) as (url, requests):
                    started = time.monotonic()
                    deadline = started + 0.12
                    budgets = []
                    original_remaining = COLLECT.CONTROL.remaining_timeout
                    original_socket = COLLECT.socket.socket

                    def remaining_budget(bound):
                        budget = original_remaining(bound)
                        budgets.append(budget)
                        return budget

                    class RecordingSocket:
                        def __init__(inner, raw):
                            inner.raw = raw

                        def settimeout(inner, budget):
                            self.assertEqual(budget, budgets[-1], "read reset the original deadline budget")
                            inner.raw.settimeout(budget)

                        def __getattr__(inner, name):
                            return getattr(inner.raw, name)

                    def socket_factory(*args, **kwargs):
                        raw = original_socket(*args, **kwargs)
                        # Accepted server sockets are outside the client's deadline accounting.
                        return raw if "fileno" in kwargs else RecordingSocket(raw)

                    with mock.patch.object(COLLECT.CONTROL, "remaining_timeout",
                                           side_effect=remaining_budget) as remaining, \
                         mock.patch.object(COLLECT.socket, "socket", side_effect=socket_factory):
                        with self.assertRaises(TimeoutError):
                            COLLECT.metrics_get(url, deadline)
                    self.assertGreater(len(remaining.call_args_list), 1)
                    self.assertTrue(all(call.args == (deadline,) for call in remaining.call_args_list))
                    self.assertEqual(requests, [b"GET /metrics HTTP/1.1"])

    def test_metrics_connect_uses_remaining_tail_and_body_limit_is_unchanged(self):
        with mock.patch.object(COLLECT.socket, "socket") as sockets:
            raw = sockets.return_value
            raw.connect.side_effect = TimeoutError("fixture connect timeout")
            with self.assertRaisesRegex(TimeoutError, "fixture connect timeout"):
                COLLECT.metrics_get("http://127.0.0.1:9000", time.monotonic() + 0.1)
            self.assertGreater(raw.settimeout.call_args.args[0], 0)
            self.assertLessEqual(raw.settimeout.call_args.args[0], 0.1)
            raw.close.assert_called_once()
        with mock.patch.object(COLLECT.socket, "socket"), \
             mock.patch.object(COLLECT.http.client, "HTTPConnection") as connections:
            response = connections.return_value.getresponse.return_value.__enter__.return_value
            response.status = 200
            response.read.return_value = b"x" * (4 * 1024**2 + 1)
            with self.assertRaisesRegex(ValueError, "exceeds 4 MiB"):
                COLLECT.metrics_get("http://127.0.0.1:9000", time.monotonic() + 2)
            response.read.assert_called_once_with(4 * 1024**2 + 1)
            connections.return_value.close.assert_called_once()

    def test_metrics_watermark_requires_exact_generation_and_safe_integer(self):
        line = fence(0)["metric_line"]
        self.assertEqual(COLLECT.producer_watermark(line), (GENERATION, 0, line))
        self.assertIsNone(COLLECT.producer_watermark("# no initialized native producer"))
        for metrics in (line + "\n" + line, line.replace(" 0", " 0.5"),
                        line.replace(" 0", " NaN"), line.replace("a" * 32, "bad"),
                        line.replace(" 0", f" {2**53 + 1}")):
            with self.subTest(metrics=metrics), self.assertRaises(ValueError):
                COLLECT.producer_watermark(metrics)


if __name__ == "__main__":
    unittest.main()
