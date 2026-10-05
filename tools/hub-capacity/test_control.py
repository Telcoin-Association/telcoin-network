"""Bounded local HTTP tests exercise the adapter, never qualification capacity."""

import argparse
from contextlib import contextmanager, redirect_stdout
import gc
import importlib.util
import io
import json
import os
from pathlib import Path
import socketserver
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import warnings


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("hub_control_workload_tests", ROOT / "workload.py")
WORKLOAD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORKLOAD)
CONTROL = WORKLOAD.CONTROL


@contextmanager
def server(reply):
    requests = []

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            self.connection.settimeout(2)
            self.rfile.readline()
            headers = {}
            while line := self.rfile.readline().strip():
                key, value = line.decode().split(":", 1)
                headers[key.lower()] = value.strip()
            payload = json.loads(self.rfile.read(int(headers["content-length"])))
            requests.append(payload)
            try:
                reply(self.connection, payload)
            except OSError:
                pass

    class Server(socketserver.ThreadingTCPServer):
        daemon_threads = True

    with Server(("127.0.0.1", 0), Handler) as endpoint:
        worker = threading.Thread(target=endpoint.serve_forever, kwargs={"poll_interval": 0.01})
        worker.start()
        try:
            yield f"http://127.0.0.1:{endpoint.server_address[1]}", requests
        finally:
            endpoint.shutdown()
            worker.join(timeout=2)


def send(connection, payload):
    body = json.dumps(payload, ensure_ascii=False).encode()
    connection.sendall(f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body)


def acknowledgement(payload, **extra):
    return {"operation_id": payload["operation_id"], "scenario": payload["scenario"],
            "identity": "peer", "success": True, "trace": {"native": "witness", "generation": 7},
            **extra}


def agent(url, *options):
    return {"identity": "peer", "argv": [sys.executable, "-B", "-I", str(ROOT / "control.py"),
                                           "--url", url, *options]}


def environment(scenario="public_join"):
    return {"HUB_CAPACITY_OPERATION_ID": "nonce", "HUB_CAPACITY_SCENARIO": scenario,
            "HUB_CAPACITY_MEASUREMENT_UNIX_US": "123456", "HUB_CAPACITY_PHASE": "candidate"}


class ControlTests(unittest.TestCase):
    def test_shared_cli_and_deadline_adapter_preserve_payload_and_witness(self):
        with server(lambda connection, payload: send(connection, acknowledgement(payload))) as (url, requests):
            args = argparse.Namespace(url=url, identity=None, observations=None, bulk_root=None)
            direct = CONTROL.invoke(args, environment(), deadline=time.monotonic() + 2)
            output = io.StringIO()
            with patch.dict(os.environ, environment()), patch.object(sys, "argv", ["control.py", "--url", url]), redirect_stdout(output):
                CONTROL.main()
            standalone = json.loads(output.getvalue())
        self.assertEqual(requests, [{"operation_id": "nonce", "scenario": "public_join",
                                     "not_before_unix_us": 123456}] * 2)
        self.assertEqual({key: value for key, value in direct.items() if key != "control_timing"},
                         {key: value for key, value in standalone.items() if key != "control_timing"})
        for result in (direct, standalone):
            timing = result["control_timing"]
            self.assertEqual(len(timing["requests"]), 1)
            self.assertLessEqual(timing["started_unix_us"], timing["requests"][0]["request_started_unix_us"])
            self.assertLessEqual(timing["requests"][0]["response_completed_unix_us"], timing["completed_unix_us"])

    def test_known_adapter_uses_no_process_and_keeps_full_latency(self):
        def reply(connection, payload):
            time.sleep(0.04)
            send(connection, acknowledgement(payload))

        with server(reply) as (url, requests), patch.object(WORKLOAD.subprocess, "Popen", side_effect=AssertionError("unexpected process")):
            result = WORKLOAD.execute(agent(url), "public_join", "nonce", time.monotonic() - 0.1, 2)
        self.assertTrue(result["success"])
        self.assertEqual(result["driver_execution_mode"], "in_process_control")
        self.assertNotIn("driver_spawn_completed_unix_us", result)
        self.assertNotIn("driver_child_completed_unix_us", result)
        self.assertGreaterEqual(result["command_latency_ms"], 40)
        self.assertEqual(result["latency_ms"], result["command_latency_ms"])
        self.assertGreaterEqual(result["elapsed_seconds"], 0.14)
        self.assertEqual(result["trace"], {"native": "witness", "generation": 7})
        self.assertLessEqual(result["driver_started_unix_us"], result["driver_adapter_started_unix_us"])
        self.assertLessEqual(result["driver_adapter_started_unix_us"], result["driver_adapter_completed_unix_us"])
        self.assertEqual(len(requests), 1)

    def test_unknown_adapter_and_cli_forms_retain_subprocess(self):
        known = agent("http://127.0.0.1:9401")["argv"]
        for argv in ([*known[:3], str(ROOT / "nested/control.py"), *known[4:]],
                     [known[0], "-I", "-B", *known[3:]],
                     [*known, "--unknown", "value"], [*known, "--url", "http://127.0.0.1:9402"],
                     [*known[:5], "https://127.0.0.1:9401"], [*known[:5], "http://example.invalid"]):
            with self.subTest(argv=argv):
                self.assertIsNone(WORKLOAD.control_arguments(argv))
        command = {"identity": "peer", "argv": [sys.executable, "-c", "import json,os; print(json.dumps({'operation_id':os.environ['HUB_CAPACITY_OPERATION_ID'],'scenario':os.environ['HUB_CAPACITY_SCENARIO'],'identity':'peer','success':True,'trace':{'native':True}}))"]}
        result = WORKLOAD.execute(command, "public_join", "nonce", time.monotonic(), 2)
        self.assertTrue(result["success"])
        self.assertEqual(result["driver_execution_mode"], "subprocess")
        self.assertIn("driver_spawn_completed_unix_us", result)
        self.assertIn("driver_child_completed_unix_us", result)
        self.assertNotIn("driver_adapter_started_unix_us", result)

    def test_gossip_and_bulk_metadata_use_identical_shared_payloads(self):
        def reply(connection, payload):
            if "trace" in payload:
                send(connection, acknowledgement(payload, trace={**payload["trace"], "publication": "native"}))
            else:
                send(connection, acknowledgement(payload))

        with server(reply) as (url, requests):
            args = argparse.Namespace(url=url, identity=None, observations=url, bulk_root=None)
            result = CONTROL.invoke(args, environment("gossip_two_hops"), deadline=time.monotonic() + 2)
            self.assertEqual(requests[1]["trace"], {"native": "witness", "generation": 7})
            self.assertEqual(result["trace"]["publication"], "native")
            self.assertEqual(len(result["control_timing"]["requests"]), 2)
            with tempfile.TemporaryDirectory() as directory:
                fixture = Path(directory) / "candidate"
                fixture.mkdir()
                targets = {"sync_epoch": 5, "batch_digests": [f"0x{index:064x}" for index in range(4)]}
                (fixture / "bulk-targets.json").write_text(json.dumps(targets))
                args.bulk_root = directory
                CONTROL.invoke(args, environment("concurrent_sync"), deadline=time.monotonic() + 2)
                self.assertEqual({key: requests[2][key] for key in targets}, targets)
                (fixture / "bulk-targets.json").write_text(" " * 8193)
                with self.assertRaisesRegex(ValueError, "8 KiB"):
                    CONTROL.invoke(args, environment("concurrent_sync"), deadline=time.monotonic() + 2)

    def test_committee_identity_and_native_cancellation_witness_are_preserved(self):
        observation = {"source": "peer", "record": {"fields": {"completed": False, "success": False}}}
        with server(lambda connection, payload: send(connection, acknowledgement(payload, trace={"observations": [observation]}))) as (url, requests):
            args = argparse.Namespace(url=None, identity="peer", observations=url, bulk_root=None)
            result = CONTROL.invoke(args, environment("committee_progress"), deadline=time.monotonic() + 2)
        self.assertEqual(requests, [{"operation_id": "nonce", "scenario": "committee_progress",
                                     "not_before_unix_us": 123456, "identity": "peer"}])
        self.assertEqual(result["trace"]["observations"], [observation])

    def test_native_failure_and_validation_remain_in_population(self):
        for extra, reason in [({"success": False, "rejection_reason": "native_cancelled"}, "native_cancelled"),
                              ({"identity": "other"}, "declared peer"),
                              ({"operation_id": "other"}, "workload command"),
                              ({"success": "true"}, "must be boolean")]:
            with self.subTest(extra=extra), server(lambda connection, payload: send(connection, acknowledgement(payload, **extra))) as (url, _):
                result = WORKLOAD.execute(agent(url), "public_join", "nonce", time.monotonic(), 2)
                self.assertFalse(result["success"])
                self.assertIn(reason, result["rejection_reason"])
                self.assertGreater(result["command_latency_ms"], 0)

    def test_flattened_committee_rows_keep_cancellation_latency_and_execution_mode(self):
        def reply(connection, payload):
            time.sleep(0.003)
            observation = {"source": "peer", "record": {"fields": {
                "event": "committee_request", "success": False, "completed": False,
                "latency_us": 1000, "unix_us": time.time_ns() // 1000}}}
            send(connection, acknowledgement(payload, trace={"observations": [observation]}))

        with server(reply) as (url, _):
            result = WORKLOAD.execute(agent(url, "--identity", "peer", "--observations", url),
                                      "committee_progress", "nonce", time.monotonic(), 2)
        self.assertTrue(result["success"])
        measured = result["committee_observations"][0]
        self.assertFalse(measured["success"])
        self.assertTrue(measured["cancelled"])
        self.assertEqual(measured["latency_ms"], 1)
        self.assertEqual(measured["driver_execution_mode"], "in_process_control")

    def test_response_and_serialized_output_limits_remain_bounded(self):
        for text, reason in [("x" * 65536, "64 KiB"), ("€" * 12000, "agent_output_limit")]:
            with self.subTest(reason=reason), server(lambda connection, payload: send(connection, acknowledgement(payload, trace={"raw": text}))) as (url, _):
                result = WORKLOAD.execute(agent(url), "public_join", "nonce", time.monotonic(), 2)
                self.assertFalse(result["success"])
                self.assertIn(reason, result["rejection_reason"])
                self.assertLessEqual(len(result.get("stdout", "").encode()), 65536)

    def test_absolute_deadline_bounds_stalled_and_dripping_headers_and_body(self):
        def reply(mode):
            def respond(connection, payload):
                if mode == "connect_headers":
                    time.sleep(0.5)
                elif mode == "header_drip":
                    for byte in b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n":
                        connection.sendall(bytes([byte]))
                        time.sleep(0.02)
                elif mode == "partial_body":
                    connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{")
                    time.sleep(0.5)
                else:
                    connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
                    for _ in range(100):
                        connection.sendall(b" ")
                        time.sleep(0.02)
            return respond

        for mode in ("connect_headers", "header_drip", "partial_body", "body_drip"):
            with self.subTest(mode=mode), server(reply(mode)) as (url, _):
                started = time.monotonic()
                result = WORKLOAD.execute(agent(url), "public_join", "nonce", started, 0.1)
                elapsed = time.monotonic() - started
                self.assertFalse(result["success"])
                self.assertEqual(result["rejection_reason"], "timeout")
                self.assertGreaterEqual(elapsed, 0.09)
                self.assertLess(elapsed, 0.35)
                self.assertIn("driver_adapter_completed_unix_us", result)

    def test_second_gossip_request_shares_original_deadline(self):
        def reply(connection, payload):
            time.sleep(0.07)
            send(connection, acknowledgement(payload))

        with server(reply) as (url, requests):
            started = time.monotonic()
            result = WORKLOAD.execute(agent(url, "--observations", url), "gossip_two_hops", "nonce", started, 0.11)
            self.assertEqual(result["rejection_reason"], "timeout")
            self.assertEqual(len(requests), 2)
            self.assertLess(time.monotonic() - started, 0.3)

    def test_connect_timeout_uses_original_deadline_and_closes_socket(self):
        with patch.object(CONTROL.socket, "socket") as socket_factory:
            connection = socket_factory.return_value
            connection.connect.side_effect = TimeoutError("timed out")
            result = WORKLOAD.execute(agent("http://127.0.0.1:9401"), "public_join", "nonce", time.monotonic(), 0.1)
            self.assertEqual(result["rejection_reason"], "timeout")
            self.assertGreater(connection.settimeout.call_args.args[0], 0)
            self.assertLessEqual(connection.settimeout.call_args.args[0], 0.1)
            connection.close.assert_called()

    def test_partial_response_and_http_error_have_typed_failures(self):
        for body, reason in [(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{", "driver_validation: Expecting property name"),
                             (b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n", "HTTP Error 503"),
                             (b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/undeclared\r\nContent-Length: 0\r\n\r\n", "HTTP Error 302"),
                             (b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n[]", "acknowledgement must be an object")]:
            with self.subTest(reason=reason), warnings.catch_warnings(record=True) as captured:
                warnings.simplefilter("always", ResourceWarning)
                with server(lambda connection, _: connection.sendall(body)) as (url, requests), patch.object(WORKLOAD.subprocess, "Popen", side_effect=AssertionError("unexpected retry process")):
                    result = WORKLOAD.execute(agent(url), "public_join", "nonce", time.monotonic(), 2)
                    self.assertFalse(result["success"])
                    self.assertIn(reason, result["rejection_reason"])
                    self.assertEqual(len(requests), 1)
                gc.collect()
                self.assertEqual([warning for warning in captured
                                  if issubclass(warning.category, ResourceWarning)], [])


if __name__ == "__main__":
    unittest.main()
