"""Verify failure acknowledgements preserve the measured peer and operation identity."""

import importlib.util
from concurrent.futures import ThreadPoolExecutor
import http.client
import io
import json
from pathlib import Path
import subprocess
import threading
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("capacity_supervise", Path(__file__).with_name("supervise.py"))
SUPERVISE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUPERVISE)


class SupervisorTests(unittest.TestCase):
    def test_regular_commands_for_one_peer_enter_transport_concurrently(self):
        for nat in (False, True):
            with self.subTest(nat=nat):
                self.regular_command_overlap(nat)

    def regular_command_overlap(self, nat):
        peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": nat},
                              Path("unused"), Path("unused"))
        entered = threading.Barrier(2, timeout=1)

        def transport(request, timeout):
            payload = json.loads(request.data)
            self.assertGreater(timeout, 0)
            self.assertLessEqual(timeout, 29)
            entered.wait()
            response = mock.MagicMock()
            response.__enter__.return_value.read.return_value = json.dumps({
                **payload, "identity": "declared-peer", "success": True}).encode()
            return response

        requests = [{"operation_id": f"operation-{index}", "scenario": scenario}
                    for index, scenario in enumerate(("gossip_two_hops", "record_lookup"))]
        with mock.patch.object(SUPERVISE.urllib.request, "urlopen", side_effect=transport), \
                ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(peer.command, request) for request in requests]
            results = [future.result(timeout=3) for future in futures]
        self.assertEqual([result["operation_id"] for result in results],
                         [request["operation_id"] for request in requests])
        self.assertTrue(all(result["success"] for result in results))

    def test_nat_restart_excludes_transport_to_the_stopped_process(self):
        peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": True},
                              Path("unused"), Path("unused"))
        restarting, release, ordinary_waiting = threading.Event(), threading.Event(), threading.Event()
        class CoordinatedCondition(threading.Condition):
            def wait(self, timeout=None):
                if restarting.is_set():
                    ordinary_waiting.set()
                return super().wait(timeout)

        peer.condition = CoordinatedCondition()
        peer.process = mock.Mock(pid=123)
        peer.process.poll.return_value = None

        def start():
            peer.process = mock.Mock(pid=456)
            peer.process.poll.return_value = None
            peer.generation = 1

        def ready(timeout):
            self.assertGreater(timeout, 0)
            self.assertLessEqual(timeout, 12)
            restarting.set()
            if not release.wait(timeout=2):
                raise TimeoutError("test did not release restart")
            restarting.clear()
            return {"identity": "declared-peer"}

        def transport(request, timeout):
            payload = json.loads(request.data)
            was_restarting = restarting.is_set()
            if payload["scenario"] == "gossip_two_hops":
                ordinary_waiting.set()
                if was_restarting:
                    raise ConnectionRefusedError("the owned process is stopped")
            response = mock.MagicMock()
            response.__enter__.return_value.read.return_value = json.dumps({
                **payload, "identity": "declared-peer", "success": True}).encode()
            return response

        with mock.patch.object(peer, "stop"), mock.patch.object(peer, "start", side_effect=start), \
                mock.patch.object(peer, "wait_ready", side_effect=ready), \
                mock.patch.object(SUPERVISE.urllib.request, "urlopen", side_effect=transport), \
                ThreadPoolExecutor(max_workers=2) as executor:
            reconnect = executor.submit(peer.command, {"operation_id": "reconnect", "scenario": "shared_nat_reconnect"})
            self.assertTrue(restarting.wait(timeout=2))
            ordinary = executor.submit(peer.command, {"operation_id": "ordinary", "scenario": "gossip_two_hops"})
            try:
                self.assertTrue(ordinary_waiting.wait(timeout=2))
            finally:
                release.set()
            results = [reconnect.result(timeout=2), ordinary.result(timeout=2)]
        self.assertTrue(all(result["success"] for result in results))
        self.assertEqual(results[0]["trace"]["restart"]["old_pid"], 123)
        self.assertEqual(results[0]["trace"]["restart"]["new_pid"], 456)
        self.assertEqual(results[0]["trace"]["restart"]["generation"], 1)
        self.assertEqual([result["supervisor"] for result in results],
                         [{"generation": 1, "pid": 456}] * 2)

    def test_hung_forward_does_not_serialize_ordinary_calls_and_releases_restart(self):
        peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": True},
                              Path("unused"), Path("unused"))
        peer.process = mock.Mock(pid=123)
        peer.process.poll.return_value = None
        peer.generation = 0
        entered, release, draining = threading.Event(), threading.Event(), threading.Event()

        class DrainCondition(threading.Condition):
            def wait(self, timeout=None):
                if peer.restarting:
                    draining.set()
                return super().wait(timeout)

        peer.condition = DrainCondition()

        def start():
            peer.process = mock.Mock(pid=456)
            peer.process.poll.return_value = None
            peer.generation = 1

        def transport(request, timeout):
            self.assertGreater(timeout, 0)
            self.assertLessEqual(timeout, 29)
            payload = json.loads(request.data)
            if payload["operation_id"] == "hung":
                entered.set()
                if not release.wait(timeout=2):
                    raise TimeoutError("test did not release hung transport")
                raise TimeoutError("peer transport reached its 29-second deadline")
            response = mock.MagicMock()
            response.__enter__.return_value.read.return_value = json.dumps({
                **payload, "identity": "declared-peer", "success": True}).encode()
            return response

        with mock.patch.object(peer, "stop") as stop, \
                mock.patch.object(peer, "start", side_effect=start), \
                mock.patch.object(peer, "wait_ready", return_value={"identity": "declared-peer"}), \
                mock.patch.object(SUPERVISE.urllib.request, "urlopen", side_effect=transport), \
                ThreadPoolExecutor(max_workers=3) as executor:
            hung = executor.submit(peer.command, {"operation_id": "hung", "scenario": "record_lookup"})
            try:
                self.assertTrue(entered.wait(1))
                ordinary = executor.submit(peer.command, {"operation_id": "ordinary", "scenario": "gossip_two_hops"})
                before = ordinary.result(timeout=1)
                self.assertEqual(before["supervisor"], {"generation": 0, "pid": 123})
                self.assertFalse(hung.done())
                reconnect = executor.submit(peer.command, {"operation_id": "restart", "scenario": "shared_nat_reconnect"})
                self.assertTrue(draining.wait(1))
                stop.assert_not_called()
            finally:
                release.set()
            with self.assertRaisesRegex(TimeoutError, "29-second deadline"):
                hung.result(timeout=1)
            after = reconnect.result(timeout=1)
            self.assertEqual(after["supervisor"], {"generation": 1, "pid": 456})
            self.assertEqual(after["trace"]["restart"]["generation"], 1)
            self.assertTrue(peer.command({"operation_id": "after", "scenario": "record_lookup"})["success"])
        self.assertEqual(peer.active_forwards, 0)
        self.assertFalse(peer.restarting)

    def test_public_join_drains_only_its_peer_and_excludes_new_transfers(self):
        peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": False},
                              Path("unused"), Path("unused"))
        other = SUPERVISE.Peer({"identity": "other-peer", "ip": "10.147.1.2", "nat": False},
                               Path("unused"), Path("unused"))
        peer.process = mock.Mock(pid=123)
        peer.process.poll.return_value = None
        peer.generation = 0
        sync_entered, release_sync = threading.Event(), threading.Event()
        join_entered, release_join = threading.Event(), threading.Event()
        draining, ordinary_waiting, ordinary_entered = (threading.Event() for _ in range(3))
        operation = threading.local()

        class OperationCondition(threading.Condition):
            def wait(self, timeout=None):
                if operation.scenario == "public_join":
                    draining.set()
                else:
                    ordinary_waiting.set()
                return super().wait(timeout)

        peer.condition = OperationCondition()

        def command(scenario):
            operation.scenario = scenario
            return peer.command({"operation_id": scenario, "scenario": scenario})

        def forward(request, forwarded, restart, timeout=29):
            if request["scenario"] == "concurrent_sync":
                sync_entered.set()
                if not release_sync.wait(2):
                    raise TimeoutError("test did not release active sync")
            elif request["scenario"] == "public_join":
                join_entered.set()
                if not release_join.wait(2):
                    raise TimeoutError("test did not release public join")
            else:
                ordinary_entered.set()
            return {"success": True}

        with mock.patch.object(peer, "forward", side_effect=forward), \
                mock.patch.object(peer, "start") as start, \
                mock.patch.object(peer, "stop") as stop, \
                mock.patch.object(other, "forward", return_value={"success": True}), \
                ThreadPoolExecutor(max_workers=3) as executor:
            sync = executor.submit(command, "concurrent_sync")
            try:
                self.assertTrue(sync_entered.wait(1))
                join = executor.submit(command, "public_join")
                self.assertTrue(draining.wait(1))
                self.assertFalse(join_entered.is_set())
                ordinary = executor.submit(command, "record_lookup")
                self.assertTrue(ordinary_waiting.wait(1))
                self.assertTrue(other.command({"operation_id": "other", "scenario": "record_lookup"})["success"])
                release_sync.set()
                self.assertTrue(sync.result(timeout=1)["success"])
                self.assertTrue(join_entered.wait(1))
                self.assertFalse(ordinary_entered.is_set())
                release_join.set()
                self.assertTrue(join.result(timeout=1)["success"])
                self.assertTrue(ordinary.result(timeout=1)["success"])
            finally:
                release_sync.set()
                release_join.set()
            start.assert_not_called()
            stop.assert_not_called()
        self.assertEqual(peer.active_forwards, 0)
        self.assertFalse(peer.restarting)
        self.assertEqual(peer.generation, 0)

    def test_expired_public_join_never_enters_transport(self):
        peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": False},
                              Path("unused"), Path("unused"))
        now = [0]

        def drain(predicate, timeout=None):
            now[0] = 30_000_000_000
            return True

        with mock.patch.object(SUPERVISE.time, "monotonic_ns", side_effect=lambda: now[0]), \
                mock.patch.object(peer.condition, "wait_for", side_effect=drain), \
                mock.patch.object(peer, "forward") as forward:
            with self.assertRaises(TimeoutError):
                peer.command({"operation_id": "expired", "scenario": "public_join"})
            forward.assert_not_called()
        self.assertEqual(peer.active_forwards, 0)
        self.assertFalse(peer.restarting)
        self.assertTrue(peer.lock.acquire(blocking=False))
        peer.lock.release()

    def test_nat_admission_expiry_preserves_the_existing_process(self):
        for stage in ("lock", "drain", "expired_drain"):
            with self.subTest(stage=stage):
                peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": True},
                                      Path("unused"), Path("unused"))
                peer.process = mock.Mock(pid=101)
                peer.process.poll.return_value = None
                peer.lock = mock.Mock()
                peer.lock.acquire.return_value = stage != "lock"
                now = [0]

                def drain(predicate, timeout=None):
                    self.assertEqual(timeout, 29)
                    now[0] = 30_000_000_000
                    return stage == "expired_drain"

                with mock.patch.object(SUPERVISE.time, "monotonic_ns", side_effect=lambda: now[0]), \
                        mock.patch.object(peer.condition, "wait_for", side_effect=drain), \
                        mock.patch.object(peer, "stop") as stop, \
                        mock.patch.object(peer, "start") as start, \
                        mock.patch.object(peer, "forward") as forward:
                    with self.assertRaises(TimeoutError):
                        peer.command({"operation_id": "expired-nat", "scenario": "shared_nat_reconnect"})
                    peer.lock.acquire.assert_called_once_with(timeout=29)
                    stop.assert_not_called()
                    start.assert_not_called()
                    forward.assert_not_called()
                self.assertFalse(peer.stopping)
                self.assertFalse(peer.restarting)
                self.assertEqual(peer.active_forwards, 0)
                self.assertEqual(peer.lock.release.call_count, int(stage != "lock"))

    def test_nat_restart_consumes_one_admission_and_transport_budget(self):
        peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": True},
                              Path("unused"), Path("unused"))
        peer.process = mock.Mock(pid=101)
        peer.process.poll.return_value = None
        now = [0]

        def drain(predicate, timeout=None):
            self.assertEqual(timeout, 29)
            now[0] = 20_000_000_000
            return True

        def stop():
            now[0] = 22_000_000_000

        def ready(timeout):
            self.assertEqual(timeout, 7)
            now[0] = 24_000_000_000
            return {"identity": "declared-peer"}

        with mock.patch.object(SUPERVISE.time, "monotonic_ns", side_effect=lambda: now[0]), \
                mock.patch.object(peer.condition, "wait_for", side_effect=drain), \
                mock.patch.object(peer, "stop", side_effect=stop), \
                mock.patch.object(peer, "start"), \
                mock.patch.object(peer, "wait_ready", side_effect=ready), \
                mock.patch.object(peer, "forward", return_value={"success": True}) as forward:
            result = peer.command({"operation_id": "bounded-nat", "scenario": "shared_nat_reconnect"})
            self.assertEqual(forward.call_args.kwargs["timeout"], 5)
            self.assertEqual(forward.call_args.args[1]["scenario"], "dao_connectivity")
            self.assertTrue(result["success"])
        self.assertFalse(peer.stopping)
        self.assertFalse(peer.restarting)
        self.assertEqual(peer.active_forwards, 0)

    def test_incomplete_nat_replacement_is_quarantined(self):
        for stage in ("stop", "ready"):
            with self.subTest(stage=stage):
                peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": True},
                                      Path("unused"), Path("unused"))
                peer.process = mock.Mock(pid=101)
                peer.process.poll.return_value = None
                now = [0]

                def stop():
                    if stage == "stop":
                        now[0] = 30_000_000_000

                with mock.patch.object(SUPERVISE.time, "monotonic_ns", side_effect=lambda: now[0]), \
                        mock.patch.object(peer, "stop", side_effect=stop) as stopped, \
                        mock.patch.object(peer, "start") as start, \
                        mock.patch.object(peer, "wait_ready", side_effect=TimeoutError("startup expired")), \
                        mock.patch.object(peer, "forward") as forward:
                    with self.assertRaises(TimeoutError):
                        peer.command({"operation_id": "incomplete-nat", "scenario": "shared_nat_reconnect"})
                    self.assertEqual(stopped.call_count, 2)
                    self.assertEqual(start.call_count, int(stage == "ready"))
                    forward.assert_not_called()
                self.assertTrue(peer.stopping)
                self.assertFalse(peer.restarting)
                self.assertEqual(peer.active_forwards, 0)
                self.assertTrue(peer.lock.acquire(blocking=False))
                peer.lock.release()
                with self.assertRaisesRegex(ValueError, "shutting down"):
                    peer.command({"operation_id": "after-timeout", "scenario": "record_lookup"})

    def test_nat_expiry_after_replacement_lease_quarantines_without_forwarding(self):
        peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": True},
                              Path("unused"), Path("unused"))
        peer.process = mock.Mock(pid=101)
        peer.process.poll.return_value = None
        now = [0]

        def diagnostic(*args):
            now[0] = 30_000_000_000

        with mock.patch.object(SUPERVISE.time, "monotonic_ns", side_effect=lambda: now[0]), \
                mock.patch.object(peer, "stop") as stop, \
                mock.patch.object(peer, "start"), \
                mock.patch.object(peer, "wait_ready", return_value={"identity": "declared-peer"}), \
                mock.patch.object(peer, "diagnostic", side_effect=diagnostic), \
                mock.patch.object(peer, "forward") as forward:
            with self.assertRaises(TimeoutError):
                peer.command({"operation_id": "leased-expiry", "scenario": "shared_nat_reconnect"})
            forward.assert_not_called()
            self.assertEqual(stop.call_count, 2)
        self.assertTrue(peer.stopping)
        self.assertFalse(peer.restarting)
        self.assertEqual(peer.active_forwards, 0)
        self.assertTrue(peer.lock.acquire(blocking=False))
        peer.lock.release()

    def test_acknowledged_failed_public_join_releases_lifecycle_and_command_capacity(self):
        peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": False},
                              Path("unused"), Path("unused"))
        with mock.patch.object(peer, "forward", side_effect=[{"success": False}, {"success": True}]):
            self.assertFalse(peer.command({"operation_id": "join", "scenario": "public_join"})["success"])
            self.assertFalse(peer.restarting)
            self.assertEqual(peer.active_forwards, 0)
            self.assertTrue(peer.lock.acquire(blocking=False))
            peer.lock.release()
            self.assertTrue(peer.command({"operation_id": "after", "scenario": "record_lookup"})["success"])
        leases = [peer.command_slots.acquire(blocking=False) for _ in range(72)]
        self.assertTrue(all(leases))
        self.assertFalse(peer.command_slots.acquire(blocking=False))
        for _ in leases:
            peer.command_slots.release()

    def test_public_join_transport_timeout_quarantines_before_releasing_lease(self):
        for cleanup_error in (None, TimeoutError("child termination failed")):
            with self.subTest(cleanup_error=cleanup_error):
                peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": False},
                                      Path("unused"), Path("unused"))

                def stop():
                    self.assertTrue(peer.stopping)
                    self.assertTrue(peer.restarting)
                    self.assertEqual(peer.active_forwards, 1)
                    self.assertFalse(peer.lock.acquire(blocking=False))
                    with self.assertRaisesRegex(ValueError, "shutting down"):
                        peer.command({"operation_id": "during", "scenario": "concurrent_sync"})
                    if cleanup_error:
                        raise cleanup_error

                with mock.patch.object(peer, "forward", side_effect=TimeoutError("join timed out")) as forward, \
                        mock.patch.object(peer, "stop", side_effect=stop) as stopped:
                    with self.assertRaises(TimeoutError):
                        peer.command({"operation_id": "join", "scenario": "public_join"})
                    stopped.assert_called_once()
                    with self.assertRaisesRegex(ValueError, "shutting down"):
                        peer.command({"operation_id": "after", "scenario": "record_lookup"})
                    forward.assert_called_once()
                self.assertEqual(peer.active_forwards, 0)
                self.assertFalse(peer.restarting)
                self.assertTrue(peer.lock.acquire(blocking=False))
                peer.lock.release()

    def test_public_join_drain_consumes_the_existing_transport_budget(self):
        peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": False},
                              Path("unused"), Path("unused"))
        now = [0]

        def drain(predicate, timeout=None):
            self.assertEqual(timeout, 29)
            now[0] = 5_000_000_000
            return True

        request = {"operation_id": "join", "scenario": "public_join"}
        with mock.patch.object(SUPERVISE.time, "monotonic_ns", side_effect=lambda: now[0]), \
                mock.patch.object(peer.condition, "wait_for", side_effect=drain), \
                mock.patch.object(peer, "forward", return_value={"success": True}) as forward:
            self.assertTrue(peer.command(request)["success"])
            forward.assert_called_once_with(request, request, None, timeout=24)

    def test_command_admission_is_bounded_without_queued_transport(self):
        peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": True},
                              Path("unused"), Path("unused"))
        entered, release = threading.Event(), threading.Event()
        count = 0
        lock = threading.Lock()

        def transport(request, timeout):
            nonlocal count
            self.assertGreater(timeout, 0)
            self.assertLessEqual(timeout, 29)
            with lock:
                count += 1
                if count == 72:
                    entered.set()
            if not release.wait(timeout=3):
                raise TimeoutError("test did not release admitted transports")
            payload = json.loads(request.data)
            response = mock.MagicMock()
            response.__enter__.return_value.read.return_value = json.dumps({
                **payload, "identity": "declared-peer", "success": True}).encode()
            return response

        with mock.patch.object(peer, "diagnostic"), \
                mock.patch.object(SUPERVISE.urllib.request, "urlopen", side_effect=transport), \
                ThreadPoolExecutor(max_workers=72) as executor:
            futures = [executor.submit(peer.command, {"operation_id": str(index), "scenario": "record_lookup"})
                       for index in range(72)]
            try:
                self.assertTrue(entered.wait(2))
                with self.assertRaisesRegex(ValueError, "admission exhausted"):
                    peer.command({"operation_id": "excess", "scenario": "record_lookup"})
                self.assertEqual(count, 72)
            finally:
                release.set()
            self.assertTrue(all(future.result(timeout=2)["success"] for future in futures))
            self.assertTrue(peer.command({"operation_id": "reused", "scenario": "record_lookup"})["success"])
        self.assertEqual(peer.active_forwards, 0)

    def test_shutdown_wakes_commands_waiting_for_restart(self):
        peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": True},
                              Path("unused"), Path("unused"))
        waiting = threading.Event()

        class WaitingCondition(threading.Condition):
            def wait(self, timeout=None):
                waiting.set()
                return super().wait(timeout)

        peer.condition = WaitingCondition()
        peer.restarting = True
        with mock.patch.object(peer, "forward") as forward, ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(peer.command, {"operation_id": "waiting", "scenario": "record_lookup"})
            self.assertTrue(waiting.wait(1))
            SUPERVISE.stop_peers([peer])
            with self.assertRaisesRegex(ValueError, "shutting down"):
                pending.result(timeout=1)
            forward.assert_not_called()
        self.assertEqual(peer.active_forwards, 0)

    def test_diagnostics_are_bounded_and_do_not_override_transport_failure(self):
        peer = SUPERVISE.Peer({"identity": "declared-peer", "ip": "10.147.1.1", "nat": True},
                              Path("unused"), Path("unused"))
        output = io.StringIO()
        request = {"operation_id": "x" * 1000, "scenario": "record_lookup"}
        with mock.patch.object(SUPERVISE.sys, "stderr", output), \
                mock.patch.object(peer, "forward", side_effect=TimeoutError("original timeout")):
            with self.assertRaisesRegex(TimeoutError, "original timeout"):
                peer.command(request)
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([event["event"] for event in events], ["forward_start", "forward_end"])
        self.assertTrue(all(len(event["operation_id"]) == 128 for event in events))
        self.assertTrue(all("admission_wait_us" in event and "forward_us" in event for event in events))
        for diagnostic_error in (OSError("diagnostic unavailable"), ValueError("diagnostic stream closed")):
            with self.subTest(error=type(diagnostic_error).__name__), \
                    mock.patch.object(SUPERVISE.sys.stderr, "write", side_effect=diagnostic_error), \
                    mock.patch.object(peer, "forward", side_effect=TimeoutError("original timeout")):
                with self.assertRaisesRegex(TimeoutError, "original timeout"):
                    peer.command(request)
        self.assertEqual(peer.active_forwards, 0)

    def test_shutdown_signals_every_child_before_shared_wait(self):
        peers = [SUPERVISE.Peer({}, Path("unused"), Path("unused")) for _ in range(4)]
        clock = [0.0]
        signalled = []
        killed = []
        for index, peer in enumerate(peers):
            process = mock.Mock()
            process.poll.side_effect = lambda index=index: -9 if index in killed else None
            process.send_signal.side_effect = lambda _signal, index=index: signalled.append(index)
            process.kill.side_effect = lambda index=index: killed.append(index)

            def wait(timeout, index=index):
                self.assertEqual(len(signalled), len(peers))
                if index not in killed:
                    clock[0] += timeout
                    raise subprocess.TimeoutExpired("owned peer", timeout)
                self.assertEqual(len(killed), len(peers))
                return -9

            process.wait.side_effect = wait
            peer.process = process
            peer.log = mock.Mock()
        with mock.patch.object(SUPERVISE.time, "monotonic", side_effect=lambda: clock[0]):
            SUPERVISE.stop_peers(peers)
        self.assertEqual(clock[0], 5)
        self.assertEqual(signalled, list(range(4)))
        self.assertEqual(killed, list(range(4)))
        self.assertTrue(all(peer.stopping for peer in peers))
        for peer in peers:
            peer.log.close.assert_called_once()

    def test_stopped_supervisor_cannot_restart_a_peer(self):
        peer = SUPERVISE.Peer({}, Path("unused"), Path("unused"))
        SUPERVISE.stop_peers([peer])
        with mock.patch.object(SUPERVISE.subprocess, "Popen") as launch:
            with self.assertRaisesRegex(ValueError, "shutting down"):
                peer.start()
            with self.assertRaisesRegex(ValueError, "shutting down"):
                peer.command({"scenario": "shared_nat_reconnect"})
            with self.assertRaisesRegex(ValueError, "shutting down"):
                peer.command({"scenario": "record_lookup"})
        launch.assert_not_called()

    def test_control_server_close_does_not_wait_for_active_handlers(self):
        entered = threading.Event()
        release = threading.Event()
        closed = threading.Event()

        class Handler(SUPERVISE.OBSERVATIONS.handler_for(None)):
            def do_POST(self):
                entered.set()
                release.wait(5)
                self.send_response(200)
                self.end_headers()

        server = SUPERVISE.OBSERVATIONS.BoundedServer(("127.0.0.1", 0), Handler)
        serving = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        serving.start()

        def request():
            connection = http.client.HTTPConnection(*server.server_address, timeout=5)
            try:
                connection.request("POST", "/", body=b"{}")
                connection.getresponse().read()
            except OSError:
                pass
            finally:
                connection.close()

        client = threading.Thread(target=request)
        client.start()

        def close():
            server.shutdown()
            server.server_close()
            closed.set()

        closing = threading.Thread(target=close)
        try:
            self.assertTrue(entered.wait(2))
            closing.start()
            self.assertTrue(closed.wait(2), "control handler blocked owned peer shutdown")
        finally:
            release.set()
            server.shutdown()
            server.server_close()
            serving.join(5)
            client.join(5)
            if closing.ident is not None:
                closing.join(5)

    def test_transport_failure_keeps_peer_identity_and_reason(self):
        peer = mock.Mock()
        peer.declaration = {"identity": "declared-peer"}
        peer.command.side_effect = OSError("peer connection closed")
        handler_type = SUPERVISE.handler_for({"ordinary-00": peer})
        handler = handler_type.__new__(handler_type)
        handler.path = "/peer/ordinary-00"
        payload = {"operation_id": "operation-1", "scenario": "record_lookup"}
        body = json.dumps(payload).encode()
        handler.headers = {"Content-Length": str(len(body))}
        handler.rfile = io.BytesIO(body)
        handler.wfile = io.BytesIO()
        handler.send_response = mock.Mock()
        handler.send_header = mock.Mock()
        handler.end_headers = mock.Mock()
        handler.do_POST()
        response = json.loads(handler.wfile.getvalue())
        self.assertEqual(response, {**payload, "identity": "declared-peer", "success": False,
                                    "rejection_reason": "peer connection closed"})
        handler.send_response.assert_called_once_with(200)


if __name__ == "__main__":
    unittest.main()
