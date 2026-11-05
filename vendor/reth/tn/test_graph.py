"""graph.py against a throwaway cargo workspace: a feature lost only by the run-time unit must
show (T4-5). Needs cargo; the workspace has path dependencies only, so no network."""

from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


GRAPH = Path(__file__).resolve().with_name("graph.py")
# cargo runs from the repository, as graph.py's own calls do, so rust-toolchain.toml picks the
# toolchain. CI removes the default toolchain, and a bare `cargo` outside the repository then fails
# or installs whatever stable is current.
REPO = GRAPH.parents[3]
TARGET = "x86_64-unknown-linux-gnu"
PKG = '[package]\nname = "{}"\nversion = "0.1.0"\nedition = "2021"\n'
# `app` uses `c` at run time and in its build script; the proc macro `m` uses `c` too.
FILES = {
    "Cargo.toml": '[workspace]\nmembers = ["app", "c", "m"]\nresolver = "2"\n',
    "c/Cargo.toml": PKG.format("c") + "\n[features]\nf = []\ng = []\n",
    "m/Cargo.toml": PKG.format("m") + '\n[lib]\nproc-macro = true\n\n'
                    '[dependencies]\nc = { path = "../c", features = ["g"] }\n',
    "app/Cargo.toml": PKG.format("app") + '\n'
                      '[dependencies]\nc = { path = "../c", features = ["f"] }\n'
                      'm = { path = "../m" }\n\n'
                      '[build-dependencies]\nc = { path = "../c", features = ["f"] }\n',
    "app/build.rs": "fn main() {}\n",
    "app/src/lib.rs": "", "c/src/lib.rs": "", "m/src/lib.rs": "",
}


class GraphViews(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir)
        for rel, text in FILES.items():
            (self.dir / "ws" / rel).parent.mkdir(parents=True, exist_ok=True)
            (self.dir / "ws" / rel).write_text(text)
        subprocess.run(["cargo", "generate-lockfile", "--offline", "--manifest-path",
                        self.dir / "ws" / "Cargo.toml"], cwd=REPO, check=True, capture_output=True)

    def graph(self, *args):
        return subprocess.run([sys.executable, "-I", "-B", str(GRAPH), *map(str, args)],
                              capture_output=True, text=True)

    def snapshot(self, name):
        out = self.dir / name
        r = self.graph("snapshot", out, "--", "--manifest-path", self.dir / "ws" / "Cargo.toml",
                       "-p", "app", "--target", TARGET, "--offline")
        self.assertEqual(r.returncode, 0, r.stderr)
        return out

    def test_views_keep_run_and_build_units_apart(self):
        lines = self.snapshot("s.txt").read_text().splitlines()
        self.assertIn(f"run {TARGET} c v0.1.0 f", lines)
        self.assertIn(f"build {TARGET} c v0.1.0 f,g", lines)  # build script's f, proc macro's g
        self.assertIn(f"build {TARGET} m v0.1.0 ", lines)
        self.assertNotIn(f"run {TARGET} m v0.1.0 ", lines)

    def test_feature_lost_only_at_run_time_shows(self):
        before = self.snapshot("before.txt")
        manifest = self.dir / "ws/app/Cargo.toml"
        manifest.write_text(manifest.read_text().replace(
            '[dependencies]\nc = { path = "../c", features = ["f"] }',
            '[dependencies]\nc = { path = "../c" }'))
        after = self.snapshot("after.txt")
        r = self.graph("diff", before, after)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        run, build = r.stdout.split("\n\n")
        self.assertIn("  ~ c v0.1.0: -f", run)
        self.assertIn("feature changes on surviving packages: 0", build)

    def test_old_snapshots_diff_only_against_each_other(self):
        old = self.dir / "old.txt"
        old.write_text("c v0.1.0 f\n")
        self.assertEqual(self.graph("diff", old, old).returncode, 0)
        self.assertEqual(self.graph("diff", old, self.snapshot("new.txt")).returncode, 2)


if __name__ == "__main__":
    unittest.main()
