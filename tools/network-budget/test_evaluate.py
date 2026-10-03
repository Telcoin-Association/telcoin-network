"""Tests that evaluation reports pass, fail or pending and never grants acceptance."""

import json
from pathlib import Path
import tempfile
import unittest

import evaluate

THRESHOLDS = Path(__file__).with_name("thresholds.proposed.json")
TOPOLOGY = {"validators": 2, "workers_per_node": 1, "cpus_per_node": 4, "ram_bytes_per_node": 1000}
SERVICE = evaluate.SERVICE


def item(metric, value, **labels):
    return {"metric": metric, "labels": {"network": "primary", **labels}, "value": value}


def plain(metric, value):
    return {"metric": metric, "labels": {}, "value": value}


def record(sample, node, time, observations=(), block=None):
    return {"sample": sample, "node": node, "started_unix_seconds": time, "observations": list(observations),
            "block": {"number": block} if block is not None else {"missing": "no rpc_url"}}


def write_run(root, build, phase, records, extra=None):
    directory = root / build / phase
    directory.mkdir(parents=True)
    (directory / "manifest.json").write_text(json.dumps({"manifest": {"topology": TOPOLOGY}}))
    (directory / "observations.jsonl").write_text("".join(json.dumps(entry) + "\n" for entry in records))
    if extra is not None:
        (directory / "phase.json").write_text(json.dumps(extra))


class EvaluateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)

    def tearDown(self):
        self.directory.cleanup()

    def test_missing_evidence_is_pending_and_acceptance_stays_pending(self):
        document = json.loads(THRESHOLDS.read_text())
        self.assertEqual({entry["status"] for entry in document["thresholds"]}, {"proposed"})
        self.assertTrue(all(entry["rationale"] for entry in document["thresholds"]))
        report = evaluate.evaluate(document, self.root)
        self.assertEqual(report["acceptance"], "pending maintainer decision")
        self.assertEqual({entry["status"] for entry in report["results"]}, {"pending"})
        with self.assertRaises(ValueError):
            evaluate.evaluate({**document, "acceptance": "accepted"}, self.root)

    def test_service_p99_from_summary_or_buckets_and_empty_class_is_pending(self):
        vote = {"class": "vote"}
        write_run(self.root, "candidate", "catch-up", [record(0, "node-0", 0, [
            item(SERVICE, 0.4, quantile="0.99", **vote), item(f"{SERVICE}_count", 5, **vote)])])
        buckets = lambda sample, low, high, total: record(sample, "node-0", sample, [
            item(f"{SERVICE}_bucket", low, le="0.5", **vote), item(f"{SERVICE}_bucket", high, le="1", **vote),
            item(f"{SERVICE}_bucket", total, le="+Inf", **vote), item(f"{SERVICE}_count", total, **vote)])
        write_run(self.root, "candidate", "mixed", [buckets(0, 0, 0, 0), buckets(1, 90, 99, 100)])
        threshold = {"class": "vote", "phases": ["catch-up", "mixed"], "limit_seconds": 1.0}
        self.assertEqual(evaluate.service_p99(threshold, self.root), ("pass", 1.0))
        self.assertEqual(evaluate.service_p99({**threshold, "limit_seconds": 0.5}, self.root)[0], "fail")
        self.assertEqual(evaluate.service_p99({**threshold, "class": "epoch_record"}, self.root)[0], "pending")

    def test_service_p99_in_the_inf_bucket_fails_with_a_json_value(self):
        vote = {"class": "vote"}
        buckets = lambda sample, total: record(sample, "node-0", sample, [
            item(f"{SERVICE}_bucket", 0, le="1", **vote), item(f"{SERVICE}_bucket", total, le="+Inf", **vote),
            item(f"{SERVICE}_count", total, **vote)])
        write_run(self.root, "candidate", "mixed", [buckets(0, 0), buckets(1, 100)])
        result = evaluate.service_p99({"class": "vote", "phases": ["mixed"], "limit_seconds": 1.0}, self.root)
        self.assertEqual(result, ("fail", "+Inf"))
        json.dumps(result, allow_nan=False)

    def test_critical_sheds_absent_is_pending_and_increase_fails(self):
        threshold = {"phases": ["steady"], "limit": 0}
        write_run(self.root, "candidate", "steady", [record(0, "node-0", 0, [item(evaluate.ESTABLISHED, 1)])])
        self.assertEqual(evaluate.critical_sheds(threshold, self.root)[0], "pending")
        sheds = lambda sample, vote: record(sample, "node-0", sample, [
            item(evaluate.SHED, vote, **{"class": "vote", "reason": "queue_full"}),
            item(evaluate.SHED, 0, **{"class": "epoch_record", "reason": "queue_full"})])
        write_run(self.root, "candidate", "hostile", [sheds(0, 0), sheds(1, 0)])
        write_run(self.root, "candidate", "mixed", [sheds(0, 1), sheds(1, 3)])
        self.assertEqual(evaluate.critical_sheds({**threshold, "phases": ["hostile"]}, self.root), ("pass", 0))
        self.assertEqual(evaluate.critical_sheds({**threshold, "phases": ["mixed"]}, self.root), ("fail", 2))

    def test_allocation_and_hostile_recovery(self):
        steady = [record(0, "node-0", 0, [item(evaluate.ESTABLISHED, 5), item(evaluate.LIMIT, 8)])]
        write_run(self.root, "candidate", "steady", steady)
        hostile = lambda sample, rejected, established: record(sample, "node-0", sample, [
            item(evaluate.REJECTIONS, rejected), item(evaluate.ESTABLISHED, established), item(evaluate.LIMIT, 8)])
        write_run(self.root, "candidate", "hostile", [hostile(0, 0, 8), hostile(1, 7, 5)])
        write_run(self.root, "candidate", "mixed", [record(0, "node-0", 0, [item(evaluate.ESTABLISHED, 3), item(evaluate.LIMIT, 0)])])
        self.assertEqual(evaluate.within_allocation({"phases": ["steady", "hostile"]}, self.root), ("pass", 1.0))
        self.assertEqual(evaluate.within_allocation({"phases": ["mixed"]}, self.root)[0], "fail")
        self.assertEqual(evaluate.hostile_recovery({}, self.root), ("pass", {"rejections": 7, "recovered": True}))

    def test_persistence_and_catch_up_compare_builds(self):
        write_run(self.root, "baseline", "steady", [record(0, "node-0", 0, block=0), record(1, "node-0", 10, block=10)])
        write_run(self.root, "candidate", "steady", [record(0, "node-0", 0, block=0), record(1, "node-0", 10, block=9)])
        self.assertEqual(evaluate.persistence_ratio({"phases": ["steady"], "limit": 0.95}, self.root), ("fail", 0.9))
        self.assertEqual(evaluate.persistence_ratio({"phases": ["mixed"], "limit": 0.95}, self.root)[0], "pending")
        lagging = {"catch_up_node": "node-1"}
        write_run(self.root, "baseline", "catch-up", [
            record(0, "node-0", 0, block=100), record(0, "node-1", 0, block=0),
            record(1, "node-0", 10, block=110), record(1, "node-1", 10, block=110)], lagging)
        write_run(self.root, "candidate", "catch-up", [
            record(0, "node-0", 0, block=100), record(0, "node-1", 0, block=0),
            record(1, "node-0", 10, block=110), record(1, "node-1", 10, block=105),
            record(2, "node-0", 20, block=120), record(2, "node-1", 20, block=120)], lagging)
        self.assertEqual(evaluate.catch_up_ratio({"limit": 1.10}, self.root), ("fail", 2.0))

    def test_rss_and_cpu_fractions(self):
        write_run(self.root, "candidate", "steady", [
            record(0, "node-0", 0, [plain(evaluate.RSS, 400), plain(evaluate.CPU, 0)]),
            record(1, "node-0", 10, [plain(evaluate.RSS, 300), plain(evaluate.CPU, 20)])])
        self.assertEqual(evaluate.rss_fraction({"limit": 0.5}, self.root), ("pass", 0.4))
        status, measured = evaluate.cpu_fraction({"limit": 0.75}, self.root)
        self.assertEqual((status, measured["cpu_p95_fraction"]), ("pass", 0.5))
        self.assertTrue(measured["network_share"].startswith("pending"))


if __name__ == "__main__":
    unittest.main()
