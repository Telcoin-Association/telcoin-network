"""Tests that evaluation reports pass, fail or pending and never grants acceptance."""

import json
from pathlib import Path
import tempfile
import unittest

import evaluate

THRESHOLDS = Path(__file__).with_name("thresholds.proposed.json")
TOPOLOGY = {"validators": 2, "workers_per_node": 1, "cpus_per_node": 4, "ram_bytes_per_node": 1000}
NODES = ("node-0", "node-1")
NETWORKS = ("primary", "worker-0")
SERVICE = evaluate.SERVICE
STREAMS = "tn_network_inbound_streams_per_connection_limit"
COMPLETE = {"failed_scrapes": 0, "failures": [], "acceptance": "pending"}
CRITICAL = ("vote", "epoch_record", "batch")


def item(metric, value, **labels):
    return {"metric": metric, "labels": {"network": "primary", **labels}, "value": value}


def plain(metric, value):
    return {"metric": metric, "labels": {}, "value": value}


def record(sample, node, time, observations=(), block=None):
    """One scrape. A stream limit sample on every swarm keeps the (node, network) coverage complete."""
    return {"sample": sample, "node": node, "started_unix_seconds": time, "finished_unix_seconds": time,
            "observations": [*observations, *(item(STREAMS, 1, network=network) for network in NETWORKS)],
            "block": {"number": block} if block is not None else {"missing": "no rpc_url"}}


def everywhere(sample, time, observations=(), block=None):
    """The same scrape from every node in the topology."""
    return [record(sample, node, time, observations, block) for node in NODES]


def write_run(root, build, phase, records, extra=None, result=COMPLETE):
    directory = root / build / phase
    directory.mkdir(parents=True)
    manifest = {"topology": TOPOLOGY, "nodes": [{"name": node} for node in NODES]}
    (directory / "manifest.json").write_text(json.dumps({"manifest": manifest}))
    (directory / "observations.jsonl").write_text("".join(json.dumps(entry) + "\n" for entry in records))
    if extra is not None:
        (directory / "phase.json").write_text(json.dumps(extra))
    if result is not None:
        (directory / "result.json").write_text(json.dumps(result))


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
        service = [entry for entry in document["thresholds"] if entry["check"] == "service_p99"]
        self.assertEqual(({entry["aggregation"] for entry in service}, {entry["class"] for entry in service}),
                         ({"worst_pair"}, set(CRITICAL)))
        self.assertEqual(document["parameters_defaults"]["batch_vote_timeout_seconds"], 10.0)
        report = evaluate.evaluate(document, self.root)
        self.assertEqual(report["acceptance"], "pending maintainer decision")
        self.assertEqual({entry["status"] for entry in report["results"]}, {"pending"})
        with self.assertRaises(ValueError):
            evaluate.evaluate({**document, "acceptance": "accepted"}, self.root)

    def test_partial_or_failed_capture_is_pending_unless_inside_an_expected_down_window(self):
        counters = [item(evaluate.SHED, 0, **{"class": name, "reason": "queue_full"}) for name in CRITICAL]
        failure = {"sample": 1, "node": "node-1", "started_unix_seconds": 5, "finished_unix_seconds": 6, "error": "timed out"}
        failed = {"failed_scrapes": 1, "failures": [failure], "acceptance": "pending"}
        window = {"expected_down": [{"node": "node-1", "start_unix_seconds": 4, "end_unix_seconds": 8}]}
        down = [*everywhere(0, 0, counters), {**record(1, "node-1", 5), **failure, "observations": []},
                record(1, "node-0", 5, counters), *everywhere(2, 10, counters)]
        cases = {
            "two-node partial": ([record(0, "node-0", 0, counters)], None, COMPLETE, "pending"),
            "missing swarm": ([record(0, "node-0", 0, counters), {**record(0, "node-1", 0), "observations": [item(STREAMS, 1)]}],
                              None, COMPLETE, "pending"),
            "unfinished capture": (everywhere(0, 0, counters), None, None, "pending"),
            "exit without failures": (everywhere(0, 0, counters), {"capture_exit": 1}, COMPLETE, "pending"),
            "failure out of window": (down, {"capture_exit": 1}, failed, "pending"),
            "failure in window": (down, {"capture_exit": 1, **window}, failed, "pass"),
        }
        for name, (records, extra, result, status) in cases.items():
            with self.subTest(case=name):
                root = self.root / name.replace(" ", "-")
                write_run(root, "candidate", "reconnect", records, extra, result)
                self.assertEqual(evaluate.critical_sheds({"phases": ["reconnect"], "limit": 0}, root)[0], status)
        partial = self.root / "partial-process"
        write_run(partial, "candidate", "steady", [record(0, "node-0", 0, [plain(evaluate.RSS, 400)]), record(0, "node-1", 0)])
        self.assertEqual(evaluate.rss_fraction({"limit": 0.5}, partial)[0], "pending")

    def test_service_p99_from_summary_or_buckets_and_empty_class_is_pending(self):
        vote = {"class": "vote"}
        write_run(self.root, "candidate", "catch-up", everywhere(0, 0, [
            item(SERVICE, 0.4, quantile="0.99", **vote), item(f"{SERVICE}_count", 5, **vote)]))
        buckets = lambda sample, low, high, total: everywhere(sample, sample, [
            item(f"{SERVICE}_bucket", low, le="0.5", **vote), item(f"{SERVICE}_bucket", high, le="1", **vote),
            item(f"{SERVICE}_bucket", total, le="+Inf", **vote), item(f"{SERVICE}_count", total, **vote)])
        write_run(self.root, "candidate", "mixed", [*buckets(0, 0, 0, 0), *buckets(1, 90, 99, 100)])
        threshold = {"class": "vote", "phases": ["catch-up", "mixed"], "limit_seconds": 1.0}
        self.assertEqual(evaluate.service_p99(threshold, self.root), ("pass", 1.0))
        self.assertEqual(evaluate.service_p99({**threshold, "limit_seconds": 0.5}, self.root)[0], "fail")
        self.assertEqual(evaluate.service_p99({**threshold, "class": "epoch_record"}, self.root)[0], "pending")

    def test_service_p99_is_the_worst_node_network_pair(self):
        # Pooled, node-0's 1000 fast requests would hide node-1's ten slow ones: the pooled p99 is 0.5 s.
        vote = {"class": "vote"}
        buckets = lambda sample, node, fast, slow: record(sample, node, sample, [
            item(f"{SERVICE}_bucket", fast, le="0.5", **vote), item(f"{SERVICE}_bucket", fast + slow, le="1", **vote),
            item(f"{SERVICE}_bucket", fast + slow, le="+Inf", **vote), item(f"{SERVICE}_count", fast + slow, **vote)])
        write_run(self.root, "candidate", "mixed", [buckets(0, "node-0", 0, 0), buckets(0, "node-1", 0, 0),
                                                     buckets(1, "node-0", 1000, 0), buckets(1, "node-1", 0, 10)])
        threshold = {"class": "vote", "phases": ["mixed"], "limit_seconds": 0.5}
        self.assertEqual(evaluate.service_p99(threshold, self.root), ("fail", 1.0))

    def test_service_p99_in_the_inf_bucket_fails_with_a_json_value(self):
        vote = {"class": "vote"}
        buckets = lambda sample, total: everywhere(sample, sample, [
            item(f"{SERVICE}_bucket", 0, le="1", **vote), item(f"{SERVICE}_bucket", total, le="+Inf", **vote),
            item(f"{SERVICE}_count", total, **vote)])
        write_run(self.root, "candidate", "mixed", [*buckets(0, 0), *buckets(1, 100)])
        result = evaluate.service_p99({"class": "vote", "phases": ["mixed"], "limit_seconds": 1.0}, self.root)
        self.assertEqual(result, ("fail", "+Inf"))
        json.dumps(result, allow_nan=False)

    def test_critical_sheds_and_failures_cover_batch_and_every_reason(self):
        threshold = {"phases": ["steady"], "limit": 0}
        write_run(self.root, "candidate", "steady", everywhere(0, 0, [item(evaluate.ESTABLISHED, 1)]))
        self.assertEqual(evaluate.critical_sheds(threshold, self.root)[0], "pending")
        self.assertEqual(evaluate.critical_failures(threshold, self.root)[0], "pending")
        sheds = lambda sample, batch: everywhere(sample, sample, [
            *(item(evaluate.SHED, 0, **{"class": name, "reason": "queue_full"}) for name in CRITICAL),
            item(evaluate.SHED, batch, **{"class": "batch", "reason": "admission"})])
        write_run(self.root, "candidate", "hostile", [*sheds(0, 0), *sheds(1, 0)])
        write_run(self.root, "candidate", "mixed", [*sheds(0, 1), *sheds(1, 3)])
        self.assertEqual(evaluate.critical_sheds({**threshold, "phases": ["hostile"]}, self.root), ("pass", 0))
        self.assertEqual(evaluate.critical_sheds({**threshold, "phases": ["mixed"]}, self.root), ("fail", 4))
        failures = lambda sample, vote: everywhere(sample, sample, [
            *(item(evaluate.FAILED, 0, **{"class": name, "outcome": "timeout"}) for name in CRITICAL),
            item(evaluate.FAILED, vote, **{"class": "vote", "outcome": "closed"})])
        write_run(self.root, "candidate", "catch-up", [*failures(0, 0), *failures(1, 0)])
        write_run(self.root, "candidate", "reconnect", [*failures(0, 0), *failures(1, 1)])
        self.assertEqual(evaluate.critical_failures({**threshold, "phases": ["catch-up"]}, self.root), ("pass", 0))
        self.assertEqual(evaluate.critical_failures({**threshold, "phases": ["reconnect"]}, self.root), ("fail", 2))

    def test_allocation_and_hostile_recovery_count_established_rejections_only(self):
        steady = everywhere(0, 0, [item(evaluate.ESTABLISHED, 5), item(evaluate.LIMIT, 8)])
        hostile = lambda sample, pending, established, connections: everywhere(sample, sample, [
            item(evaluate.REJECTIONS, pending, reason="pending_incoming"),
            *(item(evaluate.REJECTIONS, established, reason=reason) for reason in evaluate.ESTABLISHED_REASONS),
            item(evaluate.ESTABLISHED, connections), item(evaluate.LIMIT, 8)])
        write_run(self.root, "candidate", "steady", steady)
        write_run(self.root, "candidate", "hostile", [*hostile(0, 0, 0, 8), *hostile(1, 7, 1, 5)])
        write_run(self.root, "candidate", "mixed", everywhere(0, 0, [item(evaluate.ESTABLISHED, 3), item(evaluate.LIMIT, 0)]))
        self.assertEqual(evaluate.within_allocation({"phases": ["steady", "hostile"]}, self.root), ("pass", 1.0))
        self.assertEqual(evaluate.within_allocation({"phases": ["mixed"]}, self.root)[0], "fail")
        self.assertEqual(evaluate.hostile_recovery({}, self.root), ("pass", {"rejections": 6, "recovered": True}))
        pending_only = self.root / "pending-only"
        write_run(pending_only, "candidate", "steady", steady)
        write_run(pending_only, "candidate", "hostile", [*hostile(0, 0, 0, 8), *hostile(1, 7, 0, 5)])
        self.assertEqual(evaluate.hostile_recovery({}, pending_only)[0], "fail")

    def test_persistence_and_catch_up_compare_builds(self):
        write_run(self.root, "baseline", "steady", [*everywhere(0, 0, block=0), *everywhere(1, 10, block=10)])
        write_run(self.root, "candidate", "steady", [*everywhere(0, 0, block=0), *everywhere(1, 10, block=9)])
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
            *everywhere(0, 0, [plain(evaluate.RSS, 400), plain(evaluate.CPU, 0)]),
            *everywhere(1, 10, [plain(evaluate.RSS, 300), plain(evaluate.CPU, 20)])])
        self.assertEqual(evaluate.rss_fraction({"limit": 0.5}, self.root), ("pass", 0.4))
        status, measured = evaluate.cpu_fraction({"limit": 0.75}, self.root)
        self.assertEqual((status, measured["cpu_p95_fraction"]), ("pass", 0.5))
        self.assertTrue(measured["network_share"].startswith("pending"))


if __name__ == "__main__":
    unittest.main()
