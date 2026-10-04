"""Exercise production-log correlation, failure retention, and its finite allocations."""

import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("capacity_observations", Path(__file__).with_name("observations.py"))
OBSERVATIONS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(OBSERVATIONS)


class ObservationTests(unittest.TestCase):
    def test_follow_accepts_larger_logs_through_shared_protocol_bound(self):
        self.assertEqual(OBSERVATIONS.QUALIFY.MAX_PROTOCOL_LOG_BYTES, 512 * 1024**2)
        record = {"target": "network::capacity", "fields": {
            "event": "gossip_publish", "message_id": "id", "source": "sender"}}
        for size in (64 * 1024**2 + 1, 512 * 1024**2):
            with self.subTest(size=size):
                observations = OBSERVATIONS.Observations(["hub-1"])
                stream = io.BytesIO(json.dumps(record).encode() + b"\n")
                path = mock.Mock()
                path.open.return_value = stream
                path.stat.side_effect = [SimpleNamespace(st_size=size), OSError("fixture complete")]
                with mock.patch.object(stream, "readline", wraps=stream.readline) as read:
                    OBSERVATIONS.follow(observations, "hub-1", path)
                    read.assert_called_once_with(65537)
                path.open.assert_called_once_with("rb")
                self.assertEqual(observations.publications[("id", "sender")]["record"], record)
                self.assertEqual(observations.error, "hub-1: fixture complete")

    def test_follow_rejects_shared_bound_next_byte_before_read(self):
        observations = OBSERVATIONS.Observations(["hub-1"])
        stream = io.BytesIO(b"{}\n")
        path = mock.Mock()
        path.open.return_value = stream
        path.stat.return_value = SimpleNamespace(st_size=512 * 1024**2 + 1)
        with mock.patch.object(stream, "readline", wraps=stream.readline) as read:
            OBSERVATIONS.follow(observations, "hub-1", path)
            read.assert_not_called()
        self.assertEqual(observations.error, "hub-1: production log exceeds 512 MiB")
        with self.assertRaisesRegex(ValueError, "production log exceeds 512 MiB"):
            observations.query({"scenario": "committee_progress", "identity": "hub-1",
                                "not_before_unix_us": 0}, timeout=0)

    def test_follow_line_limit_and_partial_eof_remain_bounded(self):
        observations = OBSERVATIONS.Observations(["hub-1"])
        stream = io.BytesIO(b"x" * 65537)
        path = mock.Mock()
        path.open.return_value = stream
        path.stat.return_value = SimpleNamespace(st_size=65537)
        with mock.patch.object(stream, "readline", wraps=stream.readline) as read:
            OBSERVATIONS.follow(observations, "hub-1", path)
            read.assert_called_once_with(65537)
        self.assertEqual(observations.error, "hub-1: production log line exceeds 64 KiB")

        observations = OBSERVATIONS.Observations(["hub-1"])
        record = {"target": "network::capacity", "fields": {
            "event": "gossip_publish", "message_id": "id", "source": "sender"}}
        partial = json.dumps(record).encode()
        stream = io.BytesIO(partial)
        path = mock.Mock()
        path.open.return_value = stream
        path.stat.side_effect = [SimpleNamespace(st_size=len(partial)),
                                 SimpleNamespace(st_size=len(partial) + 1), OSError("fixture complete")]

        def complete_line(delay):
            self.assertEqual(delay, 0.02)
            self.assertEqual(stream.tell(), 0)
            stream.seek(0, 2)
            stream.write(b"\n")
            stream.seek(0)

        with mock.patch.object(stream, "readline", wraps=stream.readline) as read, \
                mock.patch.object(stream, "seek", wraps=stream.seek) as seek, \
                mock.patch.object(OBSERVATIONS.time, "sleep", side_effect=complete_line) as sleep:
            OBSERVATIONS.follow(observations, "hub-1", path)
            self.assertEqual(read.call_args_list, [mock.call(65537), mock.call(65537)])
            self.assertEqual(seek.call_args_list[0], mock.call(0))
            sleep.assert_called_once_with(0.02)
        path.open.assert_called_once_with("rb")
        self.assertEqual(observations.publications[("id", "sender")]["record"], record)
        self.assertEqual(observations.error, "hub-1: fixture complete")

    def test_message_and_author_must_both_match(self):
        observations = OBSERVATIONS.Observations(["hub-1", "hub-2"])
        record = {"target": "network::capacity", "fields": {
            "event": "gossip_publish", "message_id": "id", "source": "sender", "unix_us": "100"}}
        observations.ingest("hub-1", record)
        request = {"scenario": "gossip_two_hops", "trace": {
            "receipt": {"message_id": "id", "received_unix_us": 200},
            "route": ["sender", "relay", "receiver"]}}
        self.assertEqual(observations.query(request)["trace"]["publication"]["record"], record)
        request["trace"]["route"][0] = "other-author"
        with self.assertRaises(TimeoutError):
            observations.query(request, timeout=0)
        for number in range(8192):
            observations.ingest("hub-1", {"target": "network::capacity", "fields": {
                "event": "gossip_publish", "message_id": str(number), "source": "sender"}})
        self.assertEqual(len(observations.publications), 8192)
        self.assertNotIn(("id", "sender"), observations.publications)

    def test_warmup_is_excluded_and_failures_are_consumed(self):
        observations = OBSERVATIONS.Observations(["hub-1", "hub-2"])
        def entry(ended, success):
            return {"target": "network::capacity", "fields": {
                "event": "committee_request", "unix_us": str(ended), "latency_us": "10", "success": success}}
        observations.ingest("hub-1", entry(100, True))
        observations.ingest("hub-1", entry(200, False))
        observations.ingest("hub-1", entry(300, True))
        request = {"scenario": "committee_progress", "identity": "hub-1", "not_before_unix_us": 150}
        batch = observations.query(request)["trace"]["observations"]
        self.assertEqual([entry["record"]["fields"]["success"] for entry in batch], [False, True])
        with self.assertRaises(TimeoutError):
            observations.query(request, timeout=0)
        for _ in range(1024):
            observations.ingest("hub-2", entry(200, True))
        observations.ingest("hub-2", entry(300, False))
        self.assertEqual(len(observations.committee["hub-2"]), 1024)
        self.assertFalse(observations.committee["hub-2"][-1]["record"]["fields"]["success"])
        observations.query({"scenario": "committee_progress", "identity": "hub-2",
                            "not_before_unix_us": 150})
        for _ in range(32):
            observations.ingest("hub-2", entry(400, True))
        with self.assertRaisesRegex(ValueError, "allocation exhausted"):
            observations.ingest("hub-2", entry(200, True))
        self.assertEqual(len(observations.committee["hub-2"]), 1024)


if __name__ == "__main__":
    unittest.main()
