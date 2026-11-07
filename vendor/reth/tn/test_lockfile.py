"""lockfile.py's seed, overlay and checks on synthetic lockfiles and metadata. No cargo, no
network."""

import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "lockfile", Path(__file__).resolve().with_name("lockfile.py"))
lockfile = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(lockfile)

REG = "registry+https://github.com/rust-lang/crates.io-index"


def pkg(name, version, source=REG, deps=()):
    lines = [f'name = "{name}"', f'version = "{version}"']
    if source:
        lines.append(f'source = "{source}"')
    if deps:
        lines.append("dependencies = [" + ", ".join(f'"{d}"' for d in deps) + "]")
    return "\n".join(lines)


def lock(*blocks, header="version = 4\n"):
    return header + "".join("\n[[package]]\n" + b for b in blocks) + "\n"


ROOT = lock(pkg("reth-x", "1.0.0", source=None, deps=["libc"]), pkg("libc", "0.2.183"),
            pkg("rand", "0.8.5"), pkg("rand", "0.9.2"))


def names(text):
    return [(p["name"], p["version"]) for p in lockfile.packages(text)]


class Seed(unittest.TestCase):
    def test_adds_overlay_entries_the_root_lacks(self):
        overlay = lock(pkg("libc", "0.2.183"), pkg("nix", "0.31.2", deps=["libc"]),
                       pkg("local", "0.1.0", source=None), header=lockfile.OVERLAY_HEADER)
        seeded = lockfile.seed(ROOT, overlay)
        self.assertEqual(names(seeded), names(ROOT) + [("nix", "0.31.2")])
        self.assertTrue(seeded.startswith("version = 4\n"))

    def test_overlay_round_trips_through_seed(self):
        nested = lock(pkg("reth-x", "1.0.0", source=None, deps=["libc"]), pkg("libc", "0.2.183"),
                      pkg("nix", "0.31.2", deps=["libc"]), pkg("rand", "0.9.2"))
        overlay = lockfile.overlay(nested, ROOT)
        self.assertTrue(overlay.startswith(lockfile.OVERLAY_HEADER + "\n[[package]]\n"))
        self.assertEqual(names(overlay), [("nix", "0.31.2")])
        self.assertEqual(lockfile.overlay(lockfile.seed(ROOT, overlay), ROOT), overlay)


class Checks(unittest.TestCase):
    def test_unpinned_lists_packages_neither_file_pins(self):
        overlay = lock(pkg("nix", "0.31.2"))
        nested = lock(pkg("libc", "0.2.183"), pkg("nix", "0.31.3"), pkg("nix", "0.31.2"),
                      pkg("local", "0.1.0", source=None))
        self.assertEqual(lockfile.unpinned(nested, ROOT, overlay), [("nix", "0.31.3")])

    def test_drift_covers_the_production_closure_only(self):
        def node(pid, *deps):
            return {"id": pid, "deps": [{"pkg": d, "dep_kinds": [{"kind": k}]} for d, k in deps]}

        def meta(pid, name, version, source=REG):
            return {"id": pid, "name": name, "version": version, "source": source}

        metadata = {
            "workspace_members": ["x"],
            "packages": [meta("x", "reth-x", "1.0.0", None), meta("libc", "libc", "0.2.191"),
                         meta("nix", "nix", "0.31.3"), meta("rand", "rand", "0.9.2"),
                         meta("rand7", "rand", "0.7.3"), meta("cc", "cc", "1.2.0")],
            "resolve": {"nodes": [
                node("x", ("libc", None), ("nix", "dev"), ("cc", "build")),
                node("libc", ("rand", None)), node("nix", ("rand7", None)),
                node("rand", ), node("rand7", ), node("cc", ),
            ]},
        }
        # libc is shipped at a version TN lacks; nix (dev) and what it pulls are not shipped; rand
        # 0.9.2 matches one of TN's two versions; cc is not in TN's lockfile at all
        self.assertEqual(lockfile.drift(metadata, ROOT), [("libc", "0.2.191", ["0.2.183"])])


if __name__ == "__main__":
    unittest.main()
