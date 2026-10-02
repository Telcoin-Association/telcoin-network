"""Validate the real runner's manifest against the real workload consumer."""

import importlib.util
from pathlib import Path
import unittest

from test_qualify import declaration
from test_source_provenance import RUNNER


SPEC = importlib.util.spec_from_file_location("manifest_workload", Path(__file__).with_name("workload.py"))
WORKLOAD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORKLOAD)


class ManifestContractTests(unittest.TestCase):
    def test_runner_manifest_satisfies_workload_contract(self):
        population = {
            "ordinary": [{"identity": f"ordinary-{index}", "name": f"ordinary-{index}", "nat": index < 16}
                         for index in range(64)],
            "dao": [{"identity": f"dao-{index}", "name": f"dao-{index}"} for index in range(8)],
            "validators": [{"bls_key": f"validator-{index}", "hub": index < 2} for index in range(4)]}
        manifest = RUNNER.workload_manifest(population)
        WORKLOAD.validate_manifest(declaration(), manifest)
        self.assertEqual(manifest["scenarios"]["concurrent_sync"]["burst_size"], 8)
        self.assertTrue(manifest["topology_artifact"])
