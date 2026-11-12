"""vendor.py against throwaway upstream and TN repositories: upstream renames (T4-2), git-ignored
files (T4-3), upstream [patch] entries (T4-6) and the ledger check. Needs git; no network, no
cargo."""

import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


VENDOR_PY = Path(__file__).resolve().with_name("vendor.py")
SPEC = importlib.util.spec_from_file_location("vendor", VENDOR_PY)
vendor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(vendor)

# Keep the user's git configuration (signing, hooks, aliases) out of the throwaway repositories.
ENV = {"PATH": os.environ["PATH"], "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
       "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
       "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com"}

MANIFEST = """[workspace]
members = [
    "crates/bar",
    "crates/foo",
]
resolver = "2"

[workspace.dependencies]
bar = { path = "crates/bar" }
foo = { path = "crates/foo" }

[patch.crates-io]
# baz = { git = "https://example.com/baz" }
"""
LIB = "//! foo\nmod limits;\npub use limits::max_subscriptions;\n"
LIMITS = """//! Limits.

/// Maximum subscriptions per connection.
pub fn max_subscriptions() -> u32 {
    1024
}

/// Some other helper.
pub fn other() -> u32 {
    7
}
"""
PLAIN = "//! A file TN never changes.\n\n/// Does nothing.\npub fn plain() {}\n"
TN_LIMIT = "    // TN(TN-0001): TN caps subscriptions at 16.\n    16\n"
LIMITS_SRC, SUB_LIMITS_SRC = "crates/foo/src/limits.rs", "crates/foo/src/sub_limits.rs"


class Repos:
    """An upstream repository and a TN repository that vendors upstream's crates/foo with the
    vendor.py under test. TN's root .gitignore hides `debug/` and `data/`, as TN's does."""

    def __init__(self, root):
        self.up, self.tn = root / "up", root / "tn"
        self.up.mkdir()
        self.git(self.up, "init", "-q", "-b", "main")
        self.v1 = self.commit_upstream({
            "Cargo.toml": MANIFEST, "LICENSE-MIT": "MIT\n",
            "crates/foo/Cargo.toml": '[package]\nname = "foo"\nversion = "0.1.0"\n',
            "crates/foo/src/lib.rs": LIB, LIMITS_SRC: LIMITS, "crates/foo/src/plain.rs": PLAIN,
            "crates/bar/Cargo.toml": '[package]\nname = "bar"\nversion = "0.1.0"\n',
            "crates/bar/src/lib.rs": "", "crates/bar/data/fixture.json": "{}\n",
        }, "v1")
        (self.tn / "vendor/reth/tn").mkdir(parents=True)
        self.git(self.tn, "init", "-q", "-b", "main")
        shutil.copy(VENDOR_PY, self.tn / "vendor/reth/tn/vendor.py")
        (self.tn / "vendor/reth/tn/crates.txt").write_text("crates/foo\n")
        (self.tn / "vendor/reth/tn/upstream.toml").write_text(
            f'repo = "{self.up}"\ntag = "v1"\ncommit = "{self.v1}"\n')
        (self.tn / ".gitignore").write_text("debug/\ndata/\n")
        r = self.vendor("import")
        assert r.returncode == 0, r.stdout + r.stderr
        self.git(self.tn, "add", "-A")
        self.git(self.tn, "commit", "-q", "-m", "import")

    def git(self, repo, *args):
        return subprocess.run(["git", "-C", str(repo), *args], env=ENV, check=True,
                              capture_output=True, text=True).stdout.strip()

    def commit_upstream(self, files, tag):
        write(self.up, files)
        self.git(self.up, "add", "-A")
        self.git(self.up, "commit", "-q", "-m", tag)
        self.git(self.up, "tag", tag)
        return self.git(self.up, "rev-parse", "HEAD")

    def commit_tn(self, files):
        """TN's change to its vendored copy; returns the commit to reset to between runs."""
        write(self.tn / "vendor/reth", files)
        self.git(self.tn, "add", "-A")
        self.git(self.tn, "commit", "-q", "-m", "TN change")
        return self.git(self.tn, "rev-parse", "HEAD")

    def reset_tn(self, commit):
        self.git(self.tn, "reset", "-q", "--hard", commit)
        self.git(self.tn, "clean", "-qfdx")

    def vendor(self, *args):
        script = self.tn / "vendor/reth/tn/vendor.py"
        return subprocess.run([sys.executable, "-I", "-B", str(script), *args],
                              env={**ENV, "RETH_UPSTREAM": str(self.up)},
                              capture_output=True, text=True)

    def read(self, rel):
        p = self.tn / "vendor/reth" / rel
        return p.read_text() if p.exists() else None


def write(root, files):
    for rel, text in files.items():
        p = Path(root) / rel
        if text is None:
            p.unlink()
        else:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(text)


def renamed_upstream(limits=LIMITS):
    """Upstream's change: limits.rs -> sub_limits.rs (with `limits` as its new text), and
    plain.rs -> plain2.rs."""
    return {LIMITS_SRC: None, SUB_LIMITS_SRC: limits,
            "crates/foo/src/lib.rs": LIB.replace("limits", "sub_limits"),
            "crates/foo/src/plain.rs": None, "crates/foo/src/plain2.rs": PLAIN}


class Fixture(unittest.TestCase):
    def setUp(self):
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root)
        self.repos = Repos(root)

    def each_command(self, v2, check):
        """Run sync and pick of the same upstream commit, each from TN's current commit, and
        `check` each one's result."""
        start = self.repos.git(self.repos.tn, "rev-parse", "HEAD")
        for args in (("sync", "v2"), ("pick", v2)):
            with self.subTest(command=args[0]):
                self.repos.reset_tn(start)
                check(self.repos.vendor(*args))


class Renames(Fixture):
    def test_tn_change_follows_the_rename(self):
        r = self.repos
        r.commit_tn({LIMITS_SRC: LIMITS.replace("    1024\n", TN_LIMIT)})
        v2 = r.commit_upstream(renamed_upstream(LIMITS.replace("    7\n", "    8\n")), "v2")

        def check(out):
            self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
            self.assertIn(f"merged (TN + upstream)  {SUB_LIMITS_SRC} (renamed upstream from "
                          f"{LIMITS_SRC})", out.stdout)
            self.assertIsNone(r.read(LIMITS_SRC))
            self.assertEqual(r.read(SUB_LIMITS_SRC),
                             LIMITS.replace("    1024\n", TN_LIMIT).replace("    7\n", "    8\n"))
            self.assertIsNone(r.read("crates/foo/src/plain.rs"))
            self.assertEqual(r.read("crates/foo/src/plain2.rs"), PLAIN)
        self.each_command(v2, check)

    def test_conflict_names_both_paths(self):
        r = self.repos
        r.commit_tn({LIMITS_SRC: LIMITS.replace("    1024\n", TN_LIMIT)})
        v2 = r.commit_upstream(renamed_upstream(LIMITS.replace("    1024\n", "    2048\n")), "v2")

        def check(out):
            self.assertEqual(out.returncode, 1)
            self.assertIn(f"{SUB_LIMITS_SRC}: 1 conflict(s), markers left in file; renamed "
                          f"upstream from {LIMITS_SRC}, TN's version moved here", out.stdout)
            self.assertIsNone(r.read(LIMITS_SRC))
            self.assertIn("<<<<<<< TN", r.read(SUB_LIMITS_SRC))
        self.each_command(v2, check)

    def test_file_tn_deleted_stays_deleted(self):
        r = self.repos
        r.commit_tn({LIMITS_SRC: None})
        v2 = r.commit_upstream(renamed_upstream(LIMITS.replace("    7\n", "    8\n")), "v2")

        def check(out):
            self.assertEqual(out.returncode, 1)
            self.assertIn(f"{LIMITS_SRC}: deleted by TN, renamed upstream to {SUB_LIMITS_SRC} and "
                          "changed (left deleted)", out.stdout)
            self.assertIsNone(r.read(SUB_LIMITS_SRC))
        self.each_command(v2, check)


class IgnoredFiles(Fixture):
    def test_sync_and_pick_warn(self):
        r = self.repos
        v2 = r.commit_upstream({"crates/foo/src/debug/mod.rs": "//! debug\n"}, "v2")

        def check(out):
            self.assertEqual(out.returncode, 1)
            self.assertIn("warning: 1 file(s) in the vendored scope are git-ignored", out.stdout)
            self.assertIn("vendor/reth/crates/foo/src/debug/mod.rs", out.stdout)
        self.each_command(v2, check)

    def test_add_warns(self):
        out = self.repos.vendor("add", "crates/bar")
        self.assertEqual(out.returncode, 1)
        self.assertIn("vendor/reth/crates/bar/data/fixture.json", out.stdout)


class Patches(Fixture):
    def test_trim_manifest_drops_patch_entries(self):
        upstream = MANIFEST + """inkwell = { git = "https://example.com/inkwell", rev = "1" }

# evm2 is patched through its old URL.
[patch."https://example.com/evm2"]
evm2 = { git = "https://example.com/fork", rev = "2" }

[patch.crates-io.qux]
git = "https://example.com/qux"

[profile.release]
lto = true
"""
        dropped = []
        self.assertEqual(vendor.trim_manifest(upstream, ["crates/foo"], dropped), """[workspace]
members = [
    "crates/foo",
]
resolver = "2"

[workspace.dependencies]
foo = { path = "crates/foo" }

[patch.crates-io]
# baz = { git = "https://example.com/baz" }

# evm2 is patched through its old URL.
[patch."https://example.com/evm2"]


[profile.release]
lto = true
""")
        self.assertEqual(dropped, [
            '[patch.crates-io] inkwell = { git = "https://example.com/inkwell", rev = "1" }',
            '[patch."https://example.com/evm2"] '
            'evm2 = { git = "https://example.com/fork", rev = "2" }',
            "[patch.crates-io.qux]",
            '[patch.crates-io.qux] git = "https://example.com/qux"',
        ])

    def test_sync_lists_upstream_patch_entries_and_leaves_them_out(self):
        r = self.repos
        entry = 'inkwell = { git = "https://example.com/inkwell", rev = "1" }'
        r.commit_upstream({"Cargo.toml": MANIFEST + entry + "\n"}, "v2")
        out = r.vendor("sync", "v2")
        self.assertEqual(out.returncode, 1)
        self.assertIn(f"  + [patch.crates-io] {entry}", out.stdout)
        self.assertNotIn("inkwell", r.read("Cargo.toml"))



def entry(tn, files):
    return f"- **{tn}** — A change. *Kind*: fix.\n  *Files*: {files}.\n  *Why*: a reason.\n\n"


class Ledger(Fixture):
    def test_paths_expand_wrap_and_resolve_against_the_entry_crate(self):
        text = "## Section\n\n" + entry(
            "TN-0001", "`crates/foo/src/{lib.rs, sub/{a,b}.rs}`, `src/\n  plain.rs`, "
            "`tests/` and the workspace manifest") + entry("TN-0002", "`crates/foo/src/lib.rs`")
        named = vendor.ledger_paths(text, ["crates/foo", "crates/bar"])
        self.assertEqual(named, {
            "Cargo.toml": {"TN-0001"},
            "crates/foo/src/lib.rs": {"TN-0001", "TN-0002"},
            "crates/foo/src/sub/a.rs": {"TN-0001"},
            "crates/foo/src/sub/b.rs": {"TN-0001"},
            "crates/foo/src/plain.rs": {"TN-0001"},
            "crates/foo/tests/": {"TN-0001"},
        })
        self.assertTrue(vendor.names_path(named, "crates/foo/tests/it/main.rs"))
        self.assertFalse(vendor.names_path(named, "crates/foo/src/other.rs"))

    def test_prose_tokens_and_bare_directories_name_nothing(self):
        # R28: a backticked crate path in the *Why* prose covered every file of that crate, and
        # a bare directory token did the same; only the *Files* sentence names paths, and only a
        # trailing slash makes a token a directory.
        text = ("## Section\n\n- **TN-0001** — A change. *Kind*: fix.\n"
                "  *Files*: `crates/foo/src/lib.rs`, `crates/foo/src`.\n"
                "  *Why*: this leaves `crates/bar` and `crates/foo/src/plain.rs` alone.\n\n")
        named = vendor.ledger_paths(text, ["crates/foo", "crates/bar"])
        self.assertEqual(named, {"crates/foo/src/lib.rs": {"TN-0001"}, "crates/foo/src": {"TN-0001"}})
        self.assertFalse(vendor.names_path(named, "crates/bar/src/lib.rs"))
        self.assertFalse(vendor.names_path(named, "crates/foo/src/plain.rs"))
        self.assertFalse(vendor.names_path(named, "crates/foo/src/sub/a.rs"))

    def test_reports_unnamed_files_and_undefined_ids(self):
        r = self.repos
        r.commit_tn({LIMITS_SRC: LIMITS.replace("    1024\n", TN_LIMIT),
                     "crates/foo/src/plain.rs": PLAIN + "// TN(TN-0001, TN-0002): two ids.\n",
                     "CHANGES.md": entry("TN-0001", f"`{LIMITS_SRC}`")})
        out = r.vendor("ledger")
        self.assertEqual(out.returncode, 1, out.stdout + out.stderr)
        self.assertIn("not in CHANGES.md: modified  crates/foo/src/plain.rs", out.stdout)
        self.assertNotIn(LIMITS_SRC, out.stdout)
        self.assertIn("no CHANGES.md entry for TN-0002: crates/foo/src/plain.rs", out.stdout)

        write(r.tn / "vendor/reth", {"CHANGES.md": entry("TN-0001", f"`{LIMITS_SRC}`")
                                     + entry("TN-0002", "`crates/foo/src/plain.rs`")})
        out = r.vendor("ledger")
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertIn("names all 2 changed files", out.stdout)


if __name__ == "__main__":
    unittest.main()
