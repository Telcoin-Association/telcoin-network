"""Run etc/backend-gate.sh against planted workspaces in a throwaway tree."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


GATE = Path(__file__).resolve().with_name("backend-gate.sh")
WORKSPACE = """[workspace]
resolver = "2"
members = ["crates/tn-reth", "crates/e2e-tests", "bin/telcoin-network", "bin/worker-gateway",
           "bin/node-record-api"]
"""
TN_RETH = """[package]
name = "tn-reth"
version = "0.1.0"
edition = "2021"

[features]
default = []
mdbx = []
test-utils = []
"""
TN_RETH_DEP = 'tn-reth = { path = "../../crates/tn-reth" }'
# The node package's features, as bin/telcoin-network has them: three that leave tn-reth's
# backend alone, and `mdbx`, the opt-in MDBX build the gate does not resolve.
NODE_FEATURES = {"adiri": [], "faucet": [], "js-tracer": [], "mdbx": ['"tn-reth/mdbx"']}
# package directory -> (name, the tn-reth dependency line or None, the target file); the node's
# manifest is `node_manifest()`
PACKAGES = {
    "bin/telcoin-network": ("telcoin-network", TN_RETH_DEP, "src/main.rs"),
    "bin/worker-gateway": ("tn-worker-gateway", None, "src/main.rs"),
    "bin/node-record-api": ("tn-node-record-api", None, "src/main.rs"),
}
E2E = """[package]
name = "e2e-tests"
version = "0.1.0"
edition = "2021"

[dev-dependencies]
tn-reth = {{ path = "../tn-reth", features = [{features}] }}
"""


def manifest(name, dependency):
    deps = f"\n[dependencies]\n{dependency}\n" if dependency else ""
    return f'[package]\nname = "{name}"\nversion = "0.1.0"\nedition = "2021"\n{deps}'


def node_manifest(dependency=TN_RETH_DEP, features=None):
    """The node package's manifest: NODE_FEATURES with `features` overriding entries."""
    table = {**NODE_FEATURES, **(features or {})}
    lines = "".join(f'"{name}" = [{", ".join(values)}]\n' for name, values in table.items())
    return manifest("telcoin-network", dependency) + "\n[features]\n" + lines


class BackendGate(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root)
        (self.root / "etc").mkdir()
        shutil.copy(GATE, self.root / "etc" / GATE.name)
        (self.root / "Cargo.toml").write_text(WORKSPACE)
        self.write("crates/tn-reth/Cargo.toml", TN_RETH)
        self.write("crates/tn-reth/src/lib.rs", "")
        self.write("crates/e2e-tests/Cargo.toml", E2E.format(features='"test-utils"'))
        self.write("crates/e2e-tests/src/lib.rs", "")
        for directory, (name, dependency, target) in PACKAGES.items():
            text = node_manifest() if name == "telcoin-network" else manifest(name, dependency)
            self.write(f"{directory}/Cargo.toml", text)
            self.write(f"{directory}/{target}", "fn main() {}\n")
        self.relock()

    def relock(self):
        subprocess.run(["cargo", "generate-lockfile", "--offline"], cwd=self.root, check=True,
                       capture_output=True, env=self.env())

    def write(self, path, text):
        (self.root / path).parent.mkdir(parents=True, exist_ok=True)
        (self.root / path).write_text(text)

    def env(self):
        # cargo (and rustup's proxy) find their homes through these
        keep = ("PATH", "HOME", "CARGO_HOME", "RUSTUP_HOME", "RUSTUP_TOOLCHAIN")
        return {k: os.environ[k] for k in keep if k in os.environ}

    def gate(self):
        r = subprocess.run(["bash", str(self.root / "etc" / GATE.name)], capture_output=True,
                           text=True, env=self.env())
        return r.returncode, r.stdout + r.stderr

    def test_clean_tree_passes(self):
        rc, out = self.gate()
        self.assertEqual(rc, 0, out)
        self.assertIn("backend guard passed", out)

    def test_a_release_dependency_enabling_mdbx_fails(self):
        self.write("bin/telcoin-network/Cargo.toml", node_manifest(
            'tn-reth = { path = "../../crates/tn-reth", features = ["mdbx"] }'))
        rc, out = self.gate()
        self.assertEqual(rc, 1, out)
        self.assertIn("the release graph of telcoin-network enables tn-reth's mdbx feature", out)

    def test_a_default_feature_enabling_mdbx_fails(self):
        self.write("crates/tn-reth/Cargo.toml", TN_RETH.replace("default = []",
                                                                'default = ["mdbx"]'))
        rc, out = self.gate()
        self.assertEqual(rc, 1, out)
        self.assertIn("the release graph of telcoin-network enables", out)
        self.assertIn("the workspace graph (test-default) enables", out)

    def test_a_node_feature_enabling_mdbx_fails(self):
        # The images and releases build the node with one feature list; a list that forwards
        # tn-reth's mdbx moves that build onto MDBX while the default graph stays clean.
        for feature in ("adiri", "faucet", "js-tracer"):
            with self.subTest(feature=feature):
                self.write("bin/telcoin-network/Cargo.toml",
                           node_manifest(features={feature: ['"tn-reth/mdbx"']}))
                rc, out = self.gate()
                self.assertEqual(rc, 1, out)
                self.assertIn(f"the release graph of telcoin-network --features {feature} "
                              "enables tn-reth's mdbx feature", out)
                self.assertNotIn("the release graph of telcoin-network enables", out)
                self.assertNotIn("the workspace graph", out)

    def test_a_workspace_dev_dependency_enabling_mdbx_fails(self):
        self.write("crates/e2e-tests/Cargo.toml", E2E.format(features='"test-utils", "mdbx"'))
        rc, out = self.gate()
        self.assertEqual(rc, 1, out)
        self.assertIn("the workspace graph (test-default) enables", out)
        self.assertNotIn("the release graph", out)

    def test_a_node_binary_that_no_longer_reaches_tn_reth_fails_closed(self):
        self.write("bin/telcoin-network/Cargo.toml", node_manifest(None, {"mdbx": []}))
        self.relock()
        rc, out = self.gate()
        self.assertEqual(rc, 1, out)
        self.assertIn("telcoin-network does not reach tn-reth", out)

    def test_a_cargo_failure_fails_closed(self):
        self.write("bin/telcoin-network/Cargo.toml", "not a manifest")
        rc, out = self.gate()
        self.assertNotEqual(rc, 0, out)
        self.assertNotIn("backend guard passed", out)


if __name__ == "__main__":
    unittest.main()
