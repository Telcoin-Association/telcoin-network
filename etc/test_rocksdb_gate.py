"""Run etc/rocksdb-gate.sh against planted lockfiles and manifests in a throwaway tree."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


GATE = Path(__file__).resolve().with_name("rocksdb-gate.sh")
LOCK = """version = 4

[[package]]
name = "serde"
version = "1.0.228"
source = "registry+https://github.com/rust-lang/crates.io-index"
"""
STANZA = """
[[package]]
name = "{name}"
version = "0.36.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
"""
MANIFEST = '[package]\nname = "x"\nversion = "0.1.0"\n'


class RocksdbGate(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root)
        (self.root / "etc").mkdir()
        shutil.copy(GATE, self.root / "etc" / GATE.name)
        (self.root / "Cargo.lock").write_text(LOCK)
        (self.root / "vendor/reth/tn").mkdir(parents=True)
        (self.root / "vendor/reth/tn/test-only.lock").write_text(LOCK)
        (self.root / "Cargo.toml").write_text('[workspace]\nmembers = ["crates/x"]\n')
        for d in ("vendor/reth/crates/x", "bin/x", "crates/x", "examples/x", "patches/x",
                  "testing/x"):
            (self.root / d).mkdir(parents=True)
            (self.root / d / "Cargo.toml").write_text(MANIFEST)

    def gate(self):
        r = subprocess.run(["bash", str(self.root / "etc" / GATE.name)], capture_output=True,
                           text=True, env={"PATH": os.environ["PATH"]})
        return r.returncode, r.stdout + r.stderr

    def test_clean_tree_passes(self):
        self.assertEqual(self.gate(), (0, "rocksdb guard passed: no rocksdb package in either "
                                          "lockfile and no rocksdb in any manifest\n"))

    def test_any_rocksdb_package_in_the_lockfile_fails(self):
        for name in ("rocksdb", "librocksdb-sys", "rust-librocksdb-sys", "rust-rocksdb"):
            with self.subTest(name=name):
                (self.root / "Cargo.lock").write_text(LOCK + STANZA.format(name=name))
                rc, out = self.gate()
                self.assertEqual(rc, 1)
                self.assertIn(f'name = "{name}"', out)

    def test_rocksdb_pinned_for_the_vendored_tests_fails(self):
        (self.root / "vendor/reth/tn/test-only.lock").write_text(
            LOCK + STANZA.format(name="librocksdb-sys"))
        rc, out = self.gate()
        self.assertEqual(rc, 1)
        self.assertIn("vendor/reth/tn/test-only.lock contains RocksDB packages", out)

    def test_missing_test_only_lock_fails_closed(self):
        (self.root / "vendor/reth/tn/test-only.lock").unlink()
        rc, out = self.gate()
        self.assertEqual(rc, 1)
        self.assertIn("vendor/reth/tn/test-only.lock not found", out)

    def test_manifest_under_patches_fails(self):
        (self.root / "patches/x/Cargo.toml").write_text(
            MANIFEST + '\n[dependencies]\nrust-rocksdb = "0.36"\n')
        rc, out = self.gate()
        self.assertEqual(rc, 1)
        self.assertIn("patches/x/Cargo.toml", out)

    def test_manifest_under_testing_fails(self):
        (self.root / "testing/x/Cargo.toml").write_text(
            MANIFEST + '\n[dependencies]\nrocksdb = "0.24"\n')
        rc, out = self.gate()
        self.assertEqual(rc, 1)
        self.assertIn("testing/x/Cargo.toml", out)

    @unittest.skipIf(os.geteuid() == 0, "root reads a file whatever its mode")
    def test_unreadable_lockfile_fails_closed(self):
        (self.root / "Cargo.lock").chmod(0)
        rc, out = self.gate()
        self.assertEqual(rc, 1)
        self.assertIn("rocksdb guard FAILED: grep", out)

    @unittest.skipIf(os.geteuid() == 0, "root reads a file whatever its mode")
    def test_unreadable_manifest_fails_closed(self):
        (self.root / "crates/x/Cargo.toml").chmod(0)
        rc, out = self.gate()
        self.assertEqual(rc, 1)
        self.assertIn("rocksdb guard FAILED: grep", out)

    def test_comment_mentioning_rocksdb_passes(self):
        (self.root / "crates/x/Cargo.toml").write_text(MANIFEST + "# rocksdb was removed (D14)\n")
        self.assertEqual(self.gate()[0], 0)

    def test_missing_scanned_directory_fails_closed(self):
        shutil.rmtree(self.root / "patches")
        rc, out = self.gate()
        self.assertEqual(rc, 1)
        self.assertIn("patches not found", out)


if __name__ == "__main__":
    unittest.main()
