"""Deterministic scrape-derivation regressions; no node endpoint, no HTTP."""

import importlib.util
import json
import math
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("measure", Path(__file__).with_name("measure.py"))
measure = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(measure)

BOUNDS = ("0.005", "0.01", "0.025", "+Inf")


def histogram(name, labels, counts, total):
    """Cumulative bucket lines plus _sum and _count for one label set."""
    lines = [f'{name}_bucket{{{labels},le="{le}"}} {count}' for le, count in zip(BOUNDS, counts)]
    return "\n".join(lines + [f"{name}_sum{{{labels}}} {total}", f"{name}_count{{{labels}}} {counts[-1]}"])


def exposition(retry, accept, round_value, committed, subdags, cpu, rss, established, failures,
               handshake, response, inbound):
    """One scrape of every metric the tool derives from, except the commit latency histogram."""
    return "\n".join([
        "# HELP tn_network_quic_incoming_outcomes_total Inbound QUIC attempts by outcome.",
        "# TYPE tn_network_quic_incoming_outcomes_total counter",
        f'tn_network_quic_incoming_outcomes_total{{network="primary",stage="unvalidated",decision="retry",reason="none"}} {retry}',
        f'tn_network_quic_incoming_outcomes_total{{network="primary",stage="validated",decision="accept",reason="none"}} {accept}',
        "# TYPE tn_primary_round gauge",
        f"tn_primary_round {round_value}",
        "# TYPE tn_primary_committed_round gauge",
        f"tn_primary_committed_round {committed}",
        "# TYPE tn_primary_subdags_committed_total counter",
        f"tn_primary_subdags_committed_total {subdags}",
        "# TYPE reth_process_cpu_seconds_total counter",
        f"reth_process_cpu_seconds_total {cpu}",
        "# TYPE reth_process_resident_memory_bytes gauge",
        f"reth_process_resident_memory_bytes {rss}",
        "# TYPE tn_network_connections_established_total counter",
        f'tn_network_connections_established_total{{network="primary",direction="out"}} {established}',
        f'tn_network_connections_established_total{{network="primary",direction="in"}} 50',
        "# TYPE tn_network_dial_failures_total counter",
        f'tn_network_dial_failures_total{{network="primary"}} {failures}',
        "# TYPE tn_network_connection_established_seconds histogram",
        histogram("tn_network_connection_established_seconds", 'network="primary",direction="outbound"',
                  handshake, 0.1),
        "# TYPE tn_network_outbound_request_duration_seconds histogram",
        histogram("tn_network_outbound_request_duration_seconds", 'network="primary",outcome="response"',
                  response, 0.2),
        histogram("tn_network_outbound_request_duration_seconds", 'network="primary",outcome="failure"',
                  (0, 0, 0, 1), 1.0),
        "# TYPE tn_network_inbound_request_service_seconds histogram",
        histogram("tn_network_inbound_request_service_seconds", 'network="primary",class="consensus"',
                  inbound, 0.01),
        "",
    ])


SCRAPES = [
    exposition(100, 10, 5, 3, 2, 10.0, 100_000_000, 4, 1, (0, 0, 0, 0), (0, 0, 0, 0), (0, 1, 1, 1)),
    exposition(300, 20, 7, 5, 4, 10.5, 110_000_000, 8, 1, (0, 2, 5, 6), (1, 3, 4, 6), (0, 2, 2, 2)),
    exposition(500, 30, 9, 7, 6, 11.5, 120_000_000, 12, 3, (0, 2, 5, 6), (1, 3, 4, 8), (0, 2, 2, 2)),
]


def scrape(index):
    """A parsed scrape at monotonic second `index`."""
    samples, types = measure.parse_exposition(SCRAPES[index])
    return {"timestamp_utc": f"t{index}", "monotonic": float(index), "samples": samples, "types": types}


class ParserTests(unittest.TestCase):
    def test_types_labels_and_values(self):
        samples, types = measure.parse_exposition(SCRAPES[0])
        self.assertEqual(types["tn_primary_round"], "gauge")
        self.assertEqual(types["tn_network_connection_established_seconds"], "histogram")
        key = (("decision", "retry"), ("network", "primary"), ("reason", "none"), ("stage", "unvalidated"))
        self.assertEqual(samples["tn_network_quic_incoming_outcomes_total"][key], 100.0)
        self.assertEqual(samples["tn_primary_round"][()], 5.0)
        infinite = [labels for labels in samples["tn_network_connection_established_seconds_bucket"]
                    if ("le", "+Inf") in labels]
        self.assertEqual(len(infinite), 1)
        self.assertTrue(math.isinf(float(dict(infinite[0])["le"])))

    def test_escaped_label_values(self):
        self.assertEqual(measure.parse_labels('a="x\\"y",b="z\\n"'), (("a", 'x"y'), ("b", "z\n")))

    def test_malformed_lines_raise(self):
        for text in ('metric{a="x"', "metric", 'metric{a=x} 1', "metric notanumber"):
            with self.assertRaises(ValueError, msg=text):
                measure.parse_exposition(text)


class MathTests(unittest.TestCase):
    def test_interval_rate_and_reset(self):
        before = {"monotonic": 0.0, "samples": {"c": {(): 100.0}}}
        after = {"monotonic": 2.0, "samples": {"c": {(): 150.0}}}
        self.assertEqual(measure.interval_rates(before, after, "c", ()), {"all": 25.0})
        self.assertEqual(measure.run_delta(before, after, "c", ()), {"all": 50.0})
        reset = {"monotonic": 3.0, "samples": {"c": {(): 20.0}}}
        self.assertEqual(measure.interval_rates(after, reset, "c", ()), {"all": None})
        self.assertEqual(measure.interval_rates(before, after, "absent", ()), {})

    def test_group_reduce_filters_and_sums(self):
        table = {(("direction", "outbound"), ("network", "primary")): 4.0,
                 (("direction", "outbound"), ("network", "worker-0")): 6.0,
                 (("direction", "inbound"), ("network", "primary")): 50.0}
        self.assertEqual(measure.group_reduce(table, (), measure.add, {"direction": "outbound"}), {(): 10.0})
        self.assertEqual(measure.group_reduce(table, ("direction",), measure.add),
                         {(("direction", "outbound"),): 10.0, (("direction", "inbound"),): 50.0})

    def test_quantile_interpolation_and_overflow(self):
        buckets = [(0.005, 0), (0.01, 2), (0.025, 5), (math.inf, 6)]
        p50 = measure.histogram_quantile(0.5, buckets)
        self.assertAlmostEqual(p50["value"], 0.015)
        self.assertEqual((p50["lower_bound"], p50["upper_bound"], p50["in_overflow_bucket"]), (0.01, 0.025, False))
        p999 = measure.histogram_quantile(0.999, buckets)
        self.assertEqual(p999, {"value": 0.025, "lower_bound": 0.025, "upper_bound": None,
                                "in_overflow_bucket": True})
        self.assertIsNone(measure.histogram_quantile(0.5, [(0.005, 0), (math.inf, 0)]))
        self.assertIsNone(measure.histogram_quantile(0.5, []))

    def test_success_fraction(self):
        self.assertEqual(measure.success_fraction(8, 2), 0.8)
        self.assertIsNone(measure.success_fraction(0, 0))
        self.assertIsNone(measure.success_fraction(None, 2))

    def test_distribution(self):
        self.assertEqual(measure.distribution([3.0, None, 1.0, 2.0]),
                         {"min": 1.0, "median": 2.0, "p99": 3.0, "max": 3.0, "count": 3})
        self.assertIsNone(measure.distribution([None]))


class DerivationTests(unittest.TestCase):
    def setUp(self):
        self.rows, self.summary, self.unavailable, self.bounds = measure.derive([scrape(0), scrape(1), scrape(2)])

    def test_received_rates(self):
        self.assertEqual([row["received_total_pps"] for row in self.rows], [210.0, 210.0])
        self.assertEqual(self.rows[0]["received_by_stage_pps"],
                         {"network=primary,stage=unvalidated": 200.0, "network=primary,stage=validated": 10.0})
        self.assertEqual(self.summary["received"]["run_rate_pps"], 210.0)
        self.assertEqual(self.summary["received"]["total"]["max"], 210.0)
        self.assertIn("network=primary,stage=unvalidated,decision=retry", self.summary["received"]["by_outcome"])

    def test_consensus_and_process(self):
        consensus = self.summary["consensus"]
        self.assertEqual(consensus["round"], {"first": 5.0, "last": 9.0, "advance": 4.0})
        self.assertEqual(consensus["committed_round"]["advance"], 4.0)
        self.assertEqual(consensus["subdags_committed"], {"delta": 4.0, "rate_per_second": 2.0})
        self.assertIsNone(consensus["commit_latency_seconds"])
        process = self.summary["process"]
        self.assertEqual(process["cpu_cores"]["p99"], 1.0)
        self.assertEqual(process["resident_memory_bytes"],
                         {"min": 110_000_000.0, "median": 110_000_000.0, "max": 120_000_000.0, "last": 120_000_000.0})

    def test_honest_dials_and_latency(self):
        self.assertEqual(self.summary["honest_dials"],
                         {"established_outbound": 8.0, "dial_failures": 2.0, "success_fraction": 0.8})
        handshake = self.summary["handshake_time_seconds"]["direction=outbound"]
        self.assertAlmostEqual(handshake["p50"]["value"], 0.015)
        self.assertTrue(handshake["p99_9"]["in_overflow_bucket"])
        response = self.summary["established_latency"]["outbound_request_response"]
        self.assertAlmostEqual(response["p50"]["value"], 0.025)
        self.assertTrue(response["p99"]["in_overflow_bucket"])
        self.assertEqual(self.summary["established_latency"]["inbound_request_service"]["p50"]["upper_bound"], 0.01)

    def test_unavailable_and_bounds(self):
        self.assertEqual(self.unavailable, ["tn_primary_commit_latency_seconds"])
        self.assertEqual(self.bounds["tn_network_connection_established_seconds"], [0.005, 0.01, 0.025, math.inf])
        self.assertNotIn("tn_primary_commit_latency_seconds", self.bounds)
        self.assertEqual(set(measure.definitions()), {
            "received_by_outcome_pps", "received_by_stage_pps", "received_total_pps", "received.run_rate_pps",
            "consensus.round", "consensus.committed_round", "consensus.subdags_committed",
            "consensus.commit_latency_seconds", "cpu_cores", "resident_memory_bytes",
            "honest_dials.established_outbound", "honest_dials.dial_failures", "honest_dials.success_fraction",
            "handshake_time_seconds", "established_latency.outbound_request_response",
            "established_latency.inbound_request_service"})


class CollectTests(unittest.TestCase):
    def test_failed_scrape_is_a_gap_not_zeros(self):
        texts = iter([SCRAPES[0], OSError("refused"), SCRAPES[2]])
        clock = {"now": 0.0}

        def fetch():
            item = next(texts)
            if isinstance(item, Exception):
                raise item
            return item

        def sleep(seconds):
            clock["now"] += seconds

        scrapes, gaps = measure.collect(fetch, 3, 1.0, lambda: clock["now"], sleep, lambda: "now")
        self.assertEqual(len(scrapes), 2)
        self.assertEqual([entry["error"] for entry in gaps], ["OSError: refused"])
        self.assertEqual([entry["monotonic"] for entry in scrapes], [0.0, 2.0])


class CommandLineTests(unittest.TestCase):
    def test_from_files_end_to_end(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, text in enumerate(SCRAPES):
                (root / f"scrape-{index:03d}.txt").write_text(text)
            generator_summary = root / "initial.json"
            generator_summary.write_text(json.dumps({"swarm": "initial", "offered_rate_pps": 990.0}))
            output = root / "out" / "measurement.json"
            code = measure.main(["--from-files", str(root), "--interval", "1", "--generator", str(generator_summary),
                                 "--output", str(output)])
            self.assertEqual(code, 0)
            document = json.loads(output.read_text())
            self.assertEqual(document["schema_version"], measure.SCHEMA_VERSION)
            self.assertIsNone(document["metrics_url"])
            self.assertEqual(document["scrape_count"], 3)
            self.assertEqual(len(document["samples"]), 2)
            self.assertEqual(document["summary"]["received"]["run_rate_pps"], 210.0)
            self.assertEqual(document["unavailable"], ["tn_primary_commit_latency_seconds"])
            self.assertEqual(document["gaps"], [])
            self.assertEqual(document["offered"][0]["summary"]["swarm"], "initial")
            self.assertIn("received_total_pps", document["definitions"])

    def test_single_scrape_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scrape-000.txt").write_text(SCRAPES[0])
            output = root / "measurement.json"
            self.assertEqual(measure.main(["--from-files", str(root), "--output", str(output)]), 3)
            document = json.loads(output.read_text())
            self.assertIsNone(document["summary"])
            self.assertEqual(document["samples"], [])

    def test_bad_interval_exits_2(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(measure.main(["--from-files", directory, "--interval", "0",
                                           "--output", str(Path(directory) / "m.json")]), 2)


if __name__ == "__main__":
    unittest.main()
