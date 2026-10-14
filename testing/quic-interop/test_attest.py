"""Checks of the attest lane's preconditions that need no toolchain or privileged networking."""

import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("quic_attest", Path(__file__).with_name("attest.py"))
ATTEST = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ATTEST)


class RetryMutationTests(unittest.TestCase):
    """The Retry mutation must anchor on the formatted candidate source, before any build runs."""

    def test_anchor_matches_committed_candidate_once(self):
        """The rustfmt output of the listener call is the form the attest box mutates."""
        source = (ATTEST.ROOT / ATTEST.MUTATION_SOURCE).read_text()
        mutated = ATTEST.disable_retry(source)
        self.assertNotEqual(mutated, source)
        self.assertIsNone(ATTEST.RETRY_ANCHOR.search(mutated))
        self.assertEqual(len(mutated), len(source) + len("false") - len("retry"))

    def test_anchor_matches_single_line_form(self):
        """A call that fits on one line is the same experiment."""
        mutated = ATTEST.disable_retry("limits.apply(config, retry, Arc::clone(&stats));")
        self.assertEqual(mutated, "limits.apply(config, false, Arc::clone(&stats));")

    def test_changed_anchor_is_rejected(self):
        """A renamed argument must stop the lane instead of running an unmutated candidate."""
        with self.assertRaises(RuntimeError):
            ATTEST.disable_retry("limits.apply(config, enable_retry, Arc::clone(&stats));")

    def test_repeated_anchor_is_rejected(self):
        """Two matches make it unclear which listener the mutation disables."""
        call = "limits.apply(config, retry, Arc::clone(&stats));\n"
        with self.assertRaises(RuntimeError):
            ATTEST.disable_retry(call * 2)


class PassedTestsTests(unittest.TestCase):
    """A test filter that selects nothing must not count as a passing regression."""

    def test_empty_selection_counts_zero(self):
        """libtest exits 0 when a filter matches no test name."""
        log = "running 0 tests\n\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 312 filtered out\n"
        self.assertEqual(ATTEST.passed_tests(log), 0)

    def test_counts_every_binary(self):
        """Each test binary prints its own summary line."""
        log = ("test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 308 filtered out\n"
               "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n")
        self.assertEqual(ATTEST.passed_tests(log), 5)

    def test_failed_summary_counts_zero(self):
        """A failing run is reported by its exit code, not counted as passed tests."""
        log = "test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out\n"
        self.assertEqual(ATTEST.passed_tests(log), 0)


if __name__ == "__main__":
    unittest.main()
