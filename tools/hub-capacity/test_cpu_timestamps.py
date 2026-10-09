"""Synthetic CPU timing counterexamples and raw evidence rejection tests."""

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("cpu_qualification_fixture", ROOT / "test_qualify.py")
FIXTURE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIXTURE)
QUALIFY = FIXTURE.QUALIFY


class CpuTimestampTests(unittest.TestCase):
    def timing_run(self, preceding_delays, first_interval_cores):
        plan = FIXTURE.declaration()
        plan["envelope"]["cpus_per_hub"] = 1
        plan["thresholds"]["max_cpu_cores"] = 0.75
        run = FIXTURE.evidence(plan)
        # Match the collector's existing two-second loop scheduler. The five-second
        # evidence gate remains an upper bound for both loops and CPU divisors.
        template = run["samples"][0]
        run["samples"] = [copy.deepcopy(template) for _ in range(301)]
        first_time, second_time = preceding_delays[0], 2 + preceding_delays[1]
        delta = first_interval_cores * (second_time - first_time)
        for index, sample in enumerate(run["samples"]):
            loop = index * 2
            sample["elapsed_seconds"] = loop
            for hub in sample["hubs"].values():
                hub["progress"] = 1 + loop
                hub.update(FIXTURE.process_timing(loop))
            sample["hubs"]["hub-0"]["cpu_seconds"] = loop * 0.5
            hub = sample["hubs"]["hub-1"]
            hub.update(FIXTURE.process_timing(loop + preceding_delays[min(index, 1)]))
            hub["cpu_seconds"] = 100 if index == 0 else 100 + delta + (loop - 2) * 0.5
        QUALIFY.validate_plan(plan)
        QUALIFY.validate_evidence(plan, run, "candidate")
        return plan, run

    def test_preceding_scrape_increase_does_not_cause_false_cpu_failure(self):
        plan, run = self.timing_run((0.1, 1.6), 0.7)
        first, second = run["samples"][:2]
        delta = second["hubs"]["hub-1"]["cpu_seconds"] - first["hubs"]["hub-1"]["cpu_seconds"]
        old_rate = delta / (second["elapsed_seconds"] - first["elapsed_seconds"])
        self.assertAlmostEqual(old_rate, 1.225)
        self.assertGreater(old_rate, plan["thresholds"]["max_cpu_cores"])
        self.assertTrue(QUALIFY.score(plan, run)["passed"])

    def test_preceding_scrape_decrease_does_not_hide_true_cpu_failure(self):
        plan, run = self.timing_run((1.6, 0.1), 0.9)
        first, second = run["samples"][:2]
        delta = second["hubs"]["hub-1"]["cpu_seconds"] - first["hubs"]["hub-1"]["cpu_seconds"]
        old_rate = delta / (second["elapsed_seconds"] - first["elapsed_seconds"])
        self.assertAlmostEqual(old_rate, 0.225)
        self.assertLess(old_rate, plan["thresholds"]["max_cpu_cores"])
        report = QUALIFY.score(plan, run)
        self.assertFalse(report["passed"])
        self.assertEqual(report["failures"], ["hub-1: CPU headroom exhausted or process restarted"])

    def test_read_uncertainty_uses_shortest_interval_instead_of_midpoints(self):
        plan, run = self.timing_run((0, 0), 0.74)
        run["samples"][0]["hubs"]["hub-1"].update(FIXTURE.process_timing(0, 0.4))
        run["samples"][1]["hubs"]["hub-1"].update(FIXTURE.process_timing(2, 2.4))
        QUALIFY.validate_evidence(plan, run, "candidate")
        first, second = [sample["hubs"]["hub-1"] for sample in run["samples"][:2]]
        delta = second["cpu_seconds"] - first["cpu_seconds"]
        midpoint_rate = delta / (second["process_sample_elapsed_seconds"] - first["process_sample_elapsed_seconds"])
        conservative_rate = delta / (second["process_sample_started_elapsed_seconds"] - first["process_sample_completed_elapsed_seconds"])
        self.assertAlmostEqual(midpoint_rate, 0.74)
        self.assertAlmostEqual(conservative_rate, 0.925)
        self.assertFalse(QUALIFY.score(plan, run)["passed"])

    def test_overlong_cpu_interval_is_rejected_even_when_loop_cadence_is_valid(self):
        plan, _ = self.timing_run((0, 0), 0.5)
        run = FIXTURE.evidence(plan)
        run["samples"][1]["hubs"]["hub-1"].update(FIXTURE.process_timing(5.25))
        with self.assertRaisesRegex(ValueError, "CPU interval no more than five seconds"):
            QUALIFY.validate_evidence(plan, run, "candidate")
        with self.assertRaisesRegex(ValueError, "CPU interval no more than five seconds"):
            QUALIFY.score(plan, run)

    def test_per_interval_spike_and_negative_cpu_delta_still_fail(self):
        for cores in (0.75, 0.7501):
            plan, run = self.timing_run((0, 0), cores)
            with self.subTest(cores=cores):
                self.assertEqual(QUALIFY.score(plan, run)["passed"], cores == 0.75)
        plan, run = self.timing_run((0, 0), 0.5)
        run["samples"][1]["hubs"]["hub-1"]["cpu_seconds"] = 99
        self.assertIn("hub-1: CPU headroom exhausted or process restarted", QUALIFY.score(plan, run)["failures"])

    def test_process_timing_requires_all_finite_nonnegative_numeric_fields(self):
        plan, original = self.timing_run((0, 0), 0.5)
        for field in FIXTURE.process_timing(0):
            for value in (None, float("nan"), float("inf"), -1, True, "5"):
                with self.subTest(field=field, value=value):
                    run = copy.deepcopy(original)
                    run["samples"][1]["hubs"]["hub-1"][field] = value
                    with self.assertRaisesRegex(ValueError, "finite number"):
                        QUALIFY.validate_evidence(plan, run, "candidate")
            run = copy.deepcopy(original)
            del run["samples"][1]["hubs"]["hub-1"][field]
            with self.assertRaisesRegex(ValueError, "finite number"):
                QUALIFY.validate_evidence(plan, run, "candidate")

    def test_regressing_overlapping_out_of_loop_and_tampered_midpoint_are_rejected(self):
        plan, original = self.timing_run((0, 0), 0.5)
        mutations = (
            (1, FIXTURE.process_timing(1)),
            (1, FIXTURE.process_timing(2, 1)),
            (1, FIXTURE.process_timing(5)),
            (0, FIXTURE.process_timing(0, 2)),
            (1, {"process_sample_elapsed_seconds": 3}),
            (-1, FIXTURE.process_timing(631)),
        )
        for index, timing in mutations:
            with self.subTest(index=index, timing=timing):
                run = copy.deepcopy(original)
                run["samples"][index]["hubs"]["hub-1"].update(timing)
                with self.assertRaises(ValueError):
                    QUALIFY.validate_evidence(plan, run, "candidate")

    def raw_fixture(self, directory):
        plan = FIXTURE.declaration()
        run = FIXTURE.evidence(plan)
        run.pop("operations")
        (directory / "synthetic.json").write_bytes(b"synthetic")
        path = FIXTURE.retain_process_telemetry(run, directory)
        return run, path

    def rewrite_raw(self, run, path, rows):
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        run["artifacts"][-1]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()

    def test_raw_cpu_timestamp_counter_pid_and_identity_traceability(self):
        with tempfile.TemporaryDirectory() as directory:
            run, _ = self.raw_fixture(Path(directory))
            QUALIFY.verify_artifacts(run, Path(directory))
        for mutation in ("summary-time", "summary-cpu", "raw-time", "raw-nonfinite", "raw-boolean", "raw-object", "raw-cpu", "raw-pid", "restart", "ticks", "scrape", "missing", "duplicate", "old-evidence"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                run, path = self.raw_fixture(root)
                rows = [json.loads(line) for line in path.read_text().splitlines()]
                if mutation == "summary-time":
                    run["samples"][1]["hubs"]["hub-1"].update(FIXTURE.process_timing(5.25))
                elif mutation == "summary-cpu":
                    run["samples"][1]["hubs"]["hub-1"]["cpu_seconds"] += 1
                elif mutation == "raw-time":
                    rows[3].update(FIXTURE.process_timing(5.25))
                elif mutation == "raw-nonfinite":
                    rows[3]["process_sample_elapsed_seconds"] = float("nan")
                elif mutation == "raw-boolean":
                    rows[3]["process_sample_elapsed_seconds"] = True
                elif mutation == "raw-object":
                    rows[3] = []
                elif mutation in ("raw-cpu", "restart"):
                    fields = rows[3]["stat"].split(") ", 1)[1].split()
                    fields[11 if mutation == "raw-cpu" else 19] = "124"
                    rows[3]["stat"] = "43 (synthetic hub) " + " ".join(fields)
                elif mutation == "raw-pid":
                    rows[3]["pid"] = 44
                elif mutation == "ticks":
                    rows[3]["clock_ticks_per_second"] = 200
                elif mutation == "scrape":
                    rows[3]["scrape_started_elapsed_seconds"] = 4.99
                elif mutation == "missing":
                    rows.pop(3)
                elif mutation == "duplicate":
                    rows.append(rows[3])
                elif mutation == "old-evidence":
                    for field in FIXTURE.process_timing(0):
                        del rows[3][field]
                self.rewrite_raw(run, path, rows)
                with self.assertRaises(ValueError):
                    QUALIFY.verify_artifacts(run, root)

    def test_raw_digest_tampering_and_absent_telemetry_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run, path = self.raw_fixture(root)
            path.write_text(path.read_text() + "{}\n")
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                QUALIFY.verify_artifacts(run, root)
            run["artifacts"].pop()
            with self.assertRaisesRegex(ValueError, "raw process telemetry"):
                QUALIFY.verify_artifacts(run, root)

    def test_malformed_cpu_telemetry_rejects_even_a_recomputed_valid_artifact_hash(self):
        for corruption in ("malformed", "blank", "whitespace", "partial", "unterminated", "duplicate-key", "nonfinite-extra"):
            with self.subTest(corruption=corruption), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                run, path = self.raw_fixture(root)
                original = path.read_bytes()
                if corruption == "malformed":
                    altered = original + b"not-json\n"
                elif corruption == "blank":
                    altered = original + b"\n"
                elif corruption == "whitespace":
                    altered = original + b" \t\n"
                elif corruption == "partial":
                    altered = original + b'{"hub":'
                elif corruption == "unterminated":
                    altered = original[:-1]
                elif corruption == "duplicate-key":
                    altered = original.replace(b'"hub": "hub-1"', b'"hub": "forged", "hub": "hub-1"', 1)
                elif corruption == "nonfinite-extra":
                    altered = original.replace(b'"clock_ticks_per_second": 100', b'"unused": NaN, "clock_ticks_per_second": 100', 1)
                self.assertNotEqual(altered, original)
                path.write_bytes(altered)
                run["artifacts"][-1]["sha256"] = hashlib.sha256(altered).hexdigest()
                self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), run["artifacts"][-1]["sha256"])
                with self.assertRaises(ValueError):
                    QUALIFY.verify_artifacts(run, root)

    def test_protocol_raw_parser_tolerance_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "protocol-00.jsonl"
            data = b'ordinary logger banner\n\n{"target":"ordinary","value":1}\n{"target":"ordinary","value":2}'
            path.write_bytes(data)
            records = list(QUALIFY.raw_records(path, hashlib.sha256(data).hexdigest(), 65536))
            self.assertEqual([record for _, _, record in records], [
                {"target": "ordinary", "value": 1}, {"target": "ordinary", "value": 2}])


if __name__ == "__main__":
    unittest.main()
