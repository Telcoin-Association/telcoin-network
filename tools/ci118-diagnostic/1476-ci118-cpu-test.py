"""In-memory CPU diagnostic checks, retaining all reviewed reader tests."""
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cpu = load("cpu_diagnostic", "1476-ci118-cpu-reader.py")
previous = load("previous_diagnostic_tests", "1476-ci118-remote-diagnostic-test.py")
DiagnosticTests = previous.DiagnosticTests


def phase():
    samples = []
    for index in range(4):
        hubs = {}
        for hub, values in (("hub-0", [0, 0.2, 0.4, 0.6]), ("hub-1", [0, 0.5, 1.5, 1.7])):
            hubs[hub] = {"cpu_seconds": values[index], "rss_bytes": 100 * 1024**2 + index,
                         "progress": index, "tasks": {"batch_stream": index},
                         "swarms": {"worker-0": {"tasks": {"batch_stream": index}, "queue_occupancy": index}}}
        samples.append({"elapsed_seconds": index, "hubs": hubs})
    return {"phase": "candidate", "revision": cpu.base.SOURCE_HEAD,
            "plan_sha256": "b" * 64, "samples": samples, "artifacts": []}


def ci_report():
    value = previous.report()
    value["candidate"]["failures"] = ["hub-1: CPU headroom exhausted or process restarted"]
    return value


def raw_rows(value):
    rows = []
    for index, sample in enumerate(value["samples"]):
        for hub_index, (hub, measured) in enumerate(sample["hubs"].items()):
            fields = ["0"] * 22
            fields[0], fields[11], fields[12], fields[19], fields[21] = "R", str(round(measured["cpu_seconds"] * 100)), "0", "12345", "10"
            pid = 101 + hub_index
            offset = (0.01 if hub_index == 0 else (0.95 if index == 2 else 0.5))
            complete = sample["elapsed_seconds"] + (0.90 if index == 2 else 0.40) if hub_index == 0 else sample["elapsed_seconds"] + offset + 0.04
            rows.append({"hub": hub, "elapsed_seconds": sample["elapsed_seconds"], "pid": pid,
                         "stat": f"{pid} (name with ) parentheses) " + " ".join(fields), "metrics": "unused raw metrics",
                         "scrape_started_elapsed_seconds": sample["elapsed_seconds"] + offset,
                         "scrape_completed_elapsed_seconds": complete,
                         "scrape_started_unix_us": 1_800_000_000_000_000 + index * 1_000_000 + round(offset * 1_000_000),
                         "committee_fence": {"process_identity": 12345}})
    return rows


def evidence_archive(value=None, rows=None, prefix="proof/"):
    value = copy.deepcopy(phase() if value is None else value)
    rows = raw_rows(value) if rows is None else rows
    raw = b"".join(json.dumps(row).encode() + b"\n" for row in rows)
    value["artifacts"] = [{"path": "telemetry-000.jsonl", "sha256": hashlib.sha256(raw).hexdigest()}]
    return previous.archive([(prefix + "report.json", json.dumps(ci_report()).encode()),
                             (prefix + "candidate-evidence/evidence.json", json.dumps(value).encode()),
                             (prefix + "candidate-evidence/telemetry-000.jsonl", raw)])


class CpuTests(unittest.TestCase):
    def diagnose(self, raw, limits=None, handle=None):
        metadata = previous.official_metadata(raw)
        with patch.multiple(cpu.base, EXPECTED_SIZE=len(raw), ARCHIVE_SHA256=hashlib.sha256(raw).hexdigest(),
                            METADATA_SHA256=hashlib.sha256(metadata).hexdigest()), \
                patch.multiple(cpu, **(limits or {"CPU_THRESHOLD": 0.75})):
            return cpu.diagnose(metadata, io.BytesIO(raw) if handle is None else handle)

    def invalid(self, result, reason):
        self.assertEqual(result["cpu_diagnostic_status"], "invalid")
        self.assertIn(reason, result["reason"])
        self.assertNotIn("cpu_analysis", result)
        self.assertFalse(result["capacity_qualified"])
        self.assertFalse(result["independently_rescored"])

    def test_exact_breach_and_full_endpoint_context(self):
        result = self.diagnose(evidence_archive())
        self.assertEqual(result["cpu_diagnostic_status"], "present")
        analysis = result["cpu_analysis"]
        self.assertEqual(analysis["frozen_max_cpu_cores"], 0.75)
        self.assertEqual(analysis["breach_count"], 1)
        hub = analysis["per_hub"]["hub-1"]
        self.assertEqual(hub["max_cpu_cores"], 1.0)
        self.assertAlmostEqual(hub["mean_interval_cpu_cores"], 1.7 / 3)
        breach = analysis["all_breaches"][0]
        self.assertEqual((breach["before_sample"], breach["after_sample"]), (1, 2))
        self.assertEqual(breach["delta_cpu_seconds"], 1.0)
        self.assertEqual(breach["loop_start_interval_seconds"], 1)
        self.assertAlmostEqual(breach["raw_comparison"]["scrape_start_interval_seconds"], 1.45)
        context = analysis["nearby_contexts"]["2"]
        self.assertEqual(set(context["raw"]), {"hub-0", "hub-1"})
        self.assertEqual(context["raw"]["hub-1"]["total_cpu_ticks"], 150)
        self.assertEqual(context["raw"]["hub-1"]["starttime_identity"], 12345)
        self.assertAlmostEqual(context["raw"]["hub-0"]["scrape_duration_seconds"], 0.89)
        self.assertAlmostEqual(context["raw"]["hub-1"]["scrape_start_offset_from_loop_seconds"], 0.95)
        self.assertEqual(context["hubs"]["hub-1"]["swarms"]["worker-0"]["queue_occupancy"], 2)
        self.assertEqual(result["raw_telemetry"]["rows"], 8)
        self.assertFalse(result["capacity_qualified"])
        self.assertFalse(result["independently_rescored"])

    def test_negative_deltas_and_identity_changes_remain_distinct(self):
        value = phase()
        value["samples"][2]["hubs"]["hub-1"]["cpu_seconds"] = 0.1
        rows = raw_rows(value)
        for row in rows[4:]:
            if row["hub"] == "hub-1":
                row["stat"] = row["stat"].replace("12345", "54321")
        result = self.diagnose(evidence_archive(value, rows))
        self.assertEqual(result["cpu_analysis"]["per_hub"]["hub-1"]["negative_cpu_delta_count"], 1)
        negative = result["cpu_analysis"]["all_breaches"][0]
        self.assertTrue(negative["negative_cpu_delta"])
        self.assertTrue(negative["raw_comparison"]["starttime_changed"])
        self.assertFalse(result["cpu_analysis"]["nearby_contexts"]["2"]["raw"]["hub-1"]["fence_identity_matches_stat"])

    def test_threshold_equality_is_not_a_breach(self):
        value = phase()
        for index, sample in enumerate(value["samples"]):
            sample["hubs"]["hub-1"]["cpu_seconds"] = index * 0.75
        result = self.diagnose(evidence_archive(value))
        self.assertEqual(result["cpu_analysis"]["breach_count"], 0)
        self.assertEqual(result["cpu_analysis"]["nearby_contexts"], {})

    def test_every_breach_for_every_hub(self):
        value = phase()
        for sample in value["samples"]:
            for measured in sample["hubs"].values():
                measured["cpu_seconds"] = sample["elapsed_seconds"]
        result = self.diagnose(evidence_archive(value))
        self.assertEqual(result["cpu_analysis"]["breach_count"], 6)
        self.assertEqual([hub["above_threshold_count"] for hub in result["cpu_analysis"]["per_hub"].values()], [3, 3])

    def test_malformed_sample_timing(self):
        for timestamp in (0, -1, 7, True, float("inf")):
            value = phase()
            value["samples"][1]["elapsed_seconds"] = timestamp
            with self.subTest(timestamp=timestamp):
                self.invalid(self.diagnose(evidence_archive(value, rows=[])), "JSON" if timestamp == float("inf") else "sample")

    def test_phase_source_and_plan_binding(self):
        for field, altered in (("revision", "0" * 40), ("plan_sha256", "c" * 64), ("phase", "baseline")):
            value = phase()
            value[field] = altered
            self.invalid(self.diagnose(evidence_archive(value)), "binding")

    def test_malformed_cpu_and_context_fields(self):
        for field, altered in (("cpu_seconds", True), ("rss_bytes", 0), ("tasks", {"serve": -1})):
            value = phase()
            value["samples"][1]["hubs"]["hub-1"][field] = altered
            self.invalid(self.diagnose(evidence_archive(value, rows=[])), "must")

    def test_selected_phase_and_sample_breach_bounds(self):
        for limits, reason in (({"MAX_PHASE": 1}, "member byte"), ({"MAX_SAMPLES": 2}, "sample count"),
                               ({"MAX_BREACHES": 0}, "all-breach output")):
            self.invalid(self.diagnose(evidence_archive(), limits), reason)

    def test_raw_timing_and_stat_failure(self):
        rows = raw_rows(phase())
        rows[0]["scrape_completed_elapsed_seconds"] = -1
        self.invalid(self.diagnose(evidence_archive(rows=rows)), "finite")
        rows = raw_rows(phase())
        rows[0]["stat"] = "incomplete"
        self.invalid(self.diagnose(evidence_archive(rows=rows)), "process stat")

    def test_duplicate_and_missing_raw_rows(self):
        rows = raw_rows(phase())
        self.invalid(self.diagnose(evidence_archive(rows=rows + [rows[0]])), "duplicate raw")
        self.invalid(self.diagnose(evidence_archive(rows=rows[:-1])), "coverage incomplete")

    def test_raw_hash_mismatch(self):
        value = phase()
        raw = b"".join(json.dumps(row).encode() + b"\n" for row in raw_rows(value))
        value["artifacts"] = [{"path": "telemetry-000.jsonl", "sha256": "0" * 64}]
        zipped = previous.archive([("report.json", json.dumps(previous.report()).encode()),
                                   ("candidate-evidence/evidence.json", json.dumps(value).encode()),
                                   ("candidate-evidence/telemetry-000.jsonl", raw)])
        self.invalid(self.diagnose(zipped), "artifact SHA256 mismatch")

    def test_raw_byte_and_line_bounds(self):
        for limits, reason in (({"MAX_RAW_MEMBER": 1}, "member byte"), ({"MAX_RAW_TOTAL": 1}, "total byte"),
                               ({"MAX_RAW_LINE": 1}, "line byte")):
            self.invalid(self.diagnose(evidence_archive(), limits), reason)

    def test_missing_candidate_or_raw_is_explicit(self):
        result = self.diagnose(previous.archive())
        self.assertEqual(result["cpu_diagnostic_status"], "missing")
        self.assertIn("candidate-evidence/evidence.json", result["reason"])
        value = phase()
        value["artifacts"] = [{"path": "telemetry-000.jsonl", "sha256": "0" * 64}]
        raw = previous.archive([("report.json", json.dumps(previous.report()).encode()),
                                ("candidate-evidence/evidence.json", json.dumps(value).encode())])
        result = self.diagnose(raw)
        self.assertEqual(result["raw_telemetry"]["status"], "missing")
        self.assertEqual(result["cpu_diagnostic_status"], "incomplete")

    def test_tick_rate_inference_zero_and_rounding(self):
        value = phase()
        value["samples"][1]["hubs"]["hub-1"]["cpu_seconds"] = 0.29
        result = self.diagnose(evidence_archive(value))
        self.assertEqual(result["raw_telemetry"]["inferred_clock_ticks_per_second"], 100)
        for context in result["cpu_analysis"]["nearby_contexts"].values():
            self.assertTrue(context["raw"]["hub-1"]["ticks_consistent_with_scored_cpu"])
        rows = raw_rows(value)
        rows[0]["stat"] = rows[0]["stat"].replace("R 0 0 0 0 0 0 0 0 0 0 0 0", "R 0 0 0 0 0 0 0 0 0 0 1 0")
        self.invalid(self.diagnose(evidence_archive(value, rows)), "zero scored CPU")

    def test_raw_ticks_inconsistent_with_scored_cpu(self):
        rows = raw_rows(phase())
        rows[4]["stat"] = rows[4]["stat"].replace(" 40 0 ", " 41 0 ")
        self.invalid(self.diagnose(evidence_archive(rows=rows)), "tick rate")

    def test_underlying_reads_never_exceed_64_kib(self):
        raw = evidence_archive()

        class Tracked(io.BytesIO):
            def read(self, size=-1):
                if not 0 <= size <= 65536:
                    raise AssertionError("underlying read exceeded 64 KiB")
                return super().read(size)

        self.assertEqual(self.diagnose(raw, handle=Tracked(raw))["cpu_diagnostic_status"], "present")

    def test_large_logical_read_preserves_zip_tail_semantics(self):
        raw = bytes(range(256)) * 512
        wrapped = cpu.BoundedArchive(io.BytesIO(raw))
        self.assertEqual(wrapped.read(65557), raw[:65557])
        self.assertEqual(wrapped.tell(), 65557)
        self.assertTrue(wrapped.seekable())

    def test_output_bound_rejects_complete_oversized_summary(self):
        raw = evidence_archive()
        with patch.object(cpu.base, "MAX_SUMMARY", 4200):
            self.invalid(self.diagnose(raw), "output byte bound")

    def test_report_cpu_attribution_mismatch_is_explicit(self):
        value = phase()
        for sample in value["samples"]:
            sample["hubs"]["hub-1"]["cpu_seconds"] = sample["elapsed_seconds"] * 0.25
        result = self.diagnose(evidence_archive(value))
        self.assertEqual(result["cpu_diagnostic_status"], "inconsistent")
        self.assertEqual(result["report_cpu_attribution"]["reported_cpu_failure_hubs"], ["hub-1"])
        self.assertEqual(result["report_cpu_attribution"]["observed_cpu_breach_hubs"], [])

    def test_canonical_phase_and_raw_paths_at_archive_root_or_prefix(self):
        for prefix in ("", "qualification/"):
            with self.subTest(prefix=prefix):
                result = self.diagnose(evidence_archive(prefix=prefix))
                self.assertEqual(result["cpu_diagnostic_status"], "present")
                self.assertEqual(result["candidate_member"], prefix + "candidate-evidence/evidence.json")
                self.assertEqual(result["raw_telemetry"]["segments"][0]["member"],
                                 prefix + "candidate-evidence/telemetry-000.jsonl")

    def test_incorrect_legacy_phase_directory_is_not_selected(self):
        value = phase()
        raw = previous.archive([("report.json", json.dumps(ci_report()).encode()),
                                ("candidate/evidence.json", json.dumps(value).encode())])
        result = self.diagnose(raw)
        self.assertEqual(result["cpu_diagnostic_status"], "missing")


if __name__ == "__main__":
    unittest.main()
