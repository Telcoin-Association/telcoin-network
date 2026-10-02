"""Verify failure acknowledgements preserve the measured peer and operation identity."""

import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("capacity_supervise", Path(__file__).with_name("supervise.py"))
SUPERVISE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUPERVISE)


class SupervisorTests(unittest.TestCase):
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
