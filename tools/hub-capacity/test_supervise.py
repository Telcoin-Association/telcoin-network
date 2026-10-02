"""Verify failure acknowledgements preserve the measured peer and operation identity."""

import importlib.util
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
