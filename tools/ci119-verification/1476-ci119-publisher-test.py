"""In-memory README CPU regressions against the actual authenticated scorer."""
import hashlib
import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace
import unittest


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


root = Path(__file__).parent
source = Path(os.environ["QUALIFICATION_SOURCE_CHECKOUT"]) / "tools/hub-capacity"
assert hashlib.sha256((source / "qualify.py").read_bytes()).hexdigest() == "ea48c403e8d470671ccf9a07b93714b9b0bd34ac21656dbb528753ef88b4828b"
assert hashlib.sha256((source / "test_cpu_timestamps.py").read_bytes()).hexdigest() == "7a30f165d357d24f9cefdf53942e1eaffa04c3512a133479e808ae36db8d81c8"
fixtures = load("actual_cpu_fixtures", source / "test_cpu_timestamps.py")
publisher = load("repaired_publisher", root / "1476-ci119-publish-evidence.proposed.py")
v3 = load("rebound_v3", root / "1476-ci119-stream-qualified-evidence-v3.proposed.py")


class PublisherCpuTests(unittest.TestCase):
    def timing(self, delays, rate):
        return fixtures.CpuTimestampTests().timing_run(delays, rate)

    def test_true_pass_readme_rate_matches_conservative_scorer(self):
        plan, run = self.timing((0.1, 1.6), 0.7)
        self.assertTrue(fixtures.QUALIFY.score(plan, run)["passed"])
        actual = publisher.peak_cpu_cores(fixtures.QUALIFY, run["samples"], "hub-1")
        self.assertAlmostEqual(actual, 0.7)
        self.assertLessEqual(actual, plan["thresholds"]["max_cpu_cores"])
        first, second = run["samples"][:2]
        old = (second["hubs"]["hub-1"]["cpu_seconds"] - first["hubs"]["hub-1"]["cpu_seconds"]) / (second["elapsed_seconds"] - first["elapsed_seconds"])
        self.assertAlmostEqual(old, 1.225)
        self.assertGreater(old, plan["thresholds"]["max_cpu_cores"])

    def test_true_failure_is_not_hidden_in_readme(self):
        plan, run = self.timing((1.6, 0.1), 0.9)
        self.assertFalse(fixtures.QUALIFY.score(plan, run)["passed"])
        self.assertAlmostEqual(publisher.peak_cpu_cores(fixtures.QUALIFY, run["samples"], "hub-1"), 0.9)

    def test_read_bracket_uncertainty_uses_shortest_interval(self):
        plan, run = self.timing((0, 0), 0.74)
        run["samples"][0]["hubs"]["hub-1"].update(fixtures.FIXTURE.process_timing(0, 0.4))
        run["samples"][1]["hubs"]["hub-1"].update(fixtures.FIXTURE.process_timing(2, 2.4))
        self.assertFalse(fixtures.QUALIFY.score(plan, run)["passed"])
        self.assertAlmostEqual(publisher.peak_cpu_cores(fixtures.QUALIFY, run["samples"], "hub-1"), 0.925)

    def test_threshold_boundary_matches_full_score(self):
        for rate in (0.75, 0.7501):
            plan, run = self.timing((0, 0), rate)
            actual = publisher.peak_cpu_cores(fixtures.QUALIFY, run["samples"], "hub-1")
            self.assertAlmostEqual(actual, rate)
            self.assertEqual(fixtures.QUALIFY.score(plan, run)["passed"], actual <= 0.75)

    def test_legacy_authenticated_scorer_preserves_loop_divisor(self):
        samples = [{"elapsed_seconds": 0, "hubs": {"hub-0": {"cpu_seconds": 1}}},
                   {"elapsed_seconds": 2, "hubs": {"hub-0": {"cpu_seconds": 2}}}]
        self.assertEqual(publisher.peak_cpu_cores(SimpleNamespace(), samples, "hub-0"), 0.5)

    def test_bad_new_bracket_is_rejected_without_legacy_fallback(self):
        _, run = self.timing((0, 0), 0.5)
        run["samples"][1]["hubs"]["hub-1"]["process_sample_elapsed_seconds"] += 0.25
        with self.assertRaisesRegex(ValueError, "midpoint"):
            publisher.peak_cpu_cores(fixtures.QUALIFY, run["samples"], "hub-1")

    def test_rebound_v3_loads_exact_sibling_helpers(self):
        loaded_publisher, loaded_bindings = v3.trusted_helpers()
        self.assertTrue(callable(loaded_publisher.peak_cpu_cores))
        self.assertTrue(callable(loaded_bindings.verify))
        self.assertEqual(v3.PUBLISHER_SHA256, hashlib.sha256((root / "1476-ci119-publish-evidence.proposed.py").read_bytes()).hexdigest())


if __name__ == "__main__":
    unittest.main()
