"""Exercise production-log correlation, failure retention, and its finite allocations."""

import importlib.util
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location("capacity_observations", Path(__file__).with_name("observations.py"))
OBSERVATIONS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(OBSERVATIONS)


class ObservationTests(unittest.TestCase):
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
