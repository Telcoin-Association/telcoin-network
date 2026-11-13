#!/usr/bin/env python3
"""Pin the packages only reth's tests need, and check that the nested workspace tests the code TN
ships at the versions TN ships.

`tn/cargo.sh` seeds the nested lockfile (`vendor/reth/Cargo.lock`, git-ignored) on every run: TN's
root `Cargo.lock`, plus the entries of `tn/test-only.lock` that it lacks. Cargo prefers every
seeded version, so the packages TN ships keep TN's versions and the packages only reth's tests
need (dev-dependencies and what they pull in) keep the versions pinned here, instead of whatever
crates.io holds the day a lane runs.

  python3 -I vendor/reth/tn/lockfile.py check    # what the test-vendor-reth lane runs
  python3 -I vendor/reth/tn/lockfile.py update   # rewrite tn/test-only.lock from a fresh resolve

`check` resolves the nested workspace from the seed and fails when:
  - the resolve holds a registry or git package that neither TN's lockfile nor tn/test-only.lock
    pins (a dependency reth's tests gained, or a pin that no longer fits TN's versions);
  - a package in the production closure of the vendored crates (normal and build edges from the
    workspace members) has a version TN's lockfile does not: reth's tests would then run TN's code
    against a dependency version TN does not ship. A test-only package can force this by needing a
    newer shared dependency; pin it lower with
    `vendor/reth/tn/cargo.sh update -p <pkg>@<version> --precise <older>` and run `update`.

`update` seeds from TN's lockfile plus the extra entries of the current nested lockfile (or of
tn/test-only.lock when there is none), resolves (adding what is missing, at versions that support
the nested workspace's `rust-version`), writes every resolved registry or git package TN's
lockfile lacks to tn/test-only.lock, and runs the drift part of `check`. So a pin made with
`cargo.sh update --precise` survives it, and TN's current versions win for the packages it ships.
Run `update` after each such pin: the next `cargo.sh` run reseeds from tn/test-only.lock.
"""

import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

HERE = Path(__file__).resolve().parent  # vendor/reth/tn
VENDOR = HERE.parent  # vendor/reth
REPO = VENDOR.parent.parent
ROOT_LOCK = REPO / "Cargo.lock"
NESTED_LOCK = VENDOR / "Cargo.lock"
OVERLAY = HERE / "test-only.lock"
MARKER = "\n[[package]]\n"
OVERLAY_HEADER = """\
# Packages only reth's tests need, pinned for the nested workspace (vendor/reth). Written by
# tn/lockfile.py update; tn/cargo.sh adds the entries TN's Cargo.lock lacks to its seed. Not a
# lockfile on its own.
"""


def split(text):
    """The header of a lockfile's text and its `[[package]]` blocks, each without the marker."""
    head, *blocks = text.split(MARKER)
    return head, blocks


def key(pkg):
    return pkg["name"], pkg["version"], pkg.get("source")


def block_key(block):
    return key(tomllib.loads(block))


def packages(text):
    return tomllib.loads(text).get("package", [])


def seed(root_text, overlay_text):
    """TN's lockfile plus the overlay's registry and git entries it does not already hold."""
    have = {key(p) for p in packages(root_text)}
    extra = [b for b in split(overlay_text)[1]
             if (k := block_key(b))[2] is not None and k not in have]
    return root_text.rstrip("\n") + "".join(MARKER + b.rstrip("\n") for b in extra) + "\n"


def overlay(nested_text, root_text):
    """The nested lockfile's registry and git entries that TN's lockfile does not hold."""
    have = {key(p) for p in packages(root_text)}
    keep = [b.rstrip("\n") for b in split(nested_text)[1]
            if (k := block_key(b))[2] is not None and k not in have]
    return OVERLAY_HEADER + "".join(MARKER + b for b in keep) + "\n"


def unpinned(nested_text, root_text, overlay_text):
    """Resolved registry and git packages that neither TN's lockfile nor the overlay pins."""
    pinned = {key(p) for p in packages(root_text)} | {key(p) for p in packages(overlay_text)}
    return sorted(k[:2] for p in packages(nested_text)
                  if (k := key(p))[2] is not None and k not in pinned)


def drift(metadata, root_text):
    """Packages in the vendored crates' production closure whose version TN's lockfile lacks,
    as (name, version, TN's versions), for names TN's lockfile has."""
    versions = {}
    for p in packages(root_text):
        if p.get("source") is not None:
            versions.setdefault(p["name"], set()).add(p["version"])
    nodes = {n["id"]: n for n in metadata["resolve"]["nodes"]}
    by_id = {p["id"]: p for p in metadata["packages"]}
    seen, todo = set(), list(metadata["workspace_members"])
    while todo:
        pid = todo.pop()
        if pid in seen:
            continue
        seen.add(pid)
        for dep in nodes[pid]["deps"]:
            if any(k["kind"] in (None, "build") for k in dep["dep_kinds"]):
                todo.append(dep["pkg"])
    out = []
    for pid in seen:
        p = by_id[pid]
        if p.get("source") and p["name"] in versions and p["version"] not in versions[p["name"]]:
            out.append((p["name"], p["version"], sorted(versions[p["name"]])))
    return sorted(out)


# TN-0085: the carried patch (patches/revm-inspectors) must be what both workspaces
# resolve. A `[patch]` entry drops out silently when the dependency requirement stops matching the
# patch crate's version (a sync to 0.35 up to 0.44.1 would), and the registry crate links instead.
PATCHED = ("revm-inspectors",)


def unpatched(metadata, root_text):
    """Carried patch crates that resolve from a registry or git source: in the nested resolve
    (`cargo metadata` packages) or in TN's lockfile (its entry has a `source`)."""
    out = []
    for p in metadata["packages"]:
        if p["name"] in PATCHED and p.get("source") is not None:
            out.append(("nested workspace", p["name"], p["version"], p["source"]))
    for p in packages(root_text):
        if p["name"] in PATCHED and p.get("source") is not None:
            out.append(("TN's Cargo.lock", p["name"], p["version"], p["source"]))
    return out


def report_unpatched(metadata, root_text):
    found = unpatched(metadata, root_text)
    for where, name, version, source in found:
        print(f"unpatched: {where} resolves {name} {version} from {source}, not "
              f"patches/{name}")
    if found:
        print("\nthe carried patch is not in effect: its version must satisfy the dependency "
              "requirement (patches/revm-inspectors/PATCH.md)")
    return len(found)


def write_seed():
    overlay_text = OVERLAY.read_text() if OVERLAY.exists() else ""
    NESTED_LOCK.write_text(seed(ROOT_LOCK.read_text(), overlay_text))


def resolve():
    """Resolve the nested workspace from the seed already written and return its metadata."""
    r = subprocess.run([str(HERE / "cargo.sh"), "metadata", "--format-version", "1",
                        "--color", "never"], stdout=subprocess.PIPE, text=True,
                       env={**os.environ, "TN_LOCKFILE_SEEDED": "1"})
    if r.returncode:
        sys.exit("lockfile.py: cargo metadata failed in the nested workspace")
    return json.loads(r.stdout)


def report_drift(metadata, root_text):
    found = drift(metadata, root_text)
    for name, version, tn in found:
        print(f"drift: the vendored crates resolve {name} {version}; TN ships {', '.join(tn)}")
    if found:
        print("\na test-only package needs a newer shared dependency: find it with `cargo tree "
              "-i <name>@<version>` in vendor/reth, pin it lower with `vendor/reth/tn/cargo.sh "
              "update -p <pkg>@<version> --precise <older>`, then run `lockfile.py update`")
    return len(found)


def cmd_update():
    root_text = ROOT_LOCK.read_text()
    start = (overlay(NESTED_LOCK.read_text(), root_text) if NESTED_LOCK.exists()
             else OVERLAY.read_text() if OVERLAY.exists() else "")
    NESTED_LOCK.write_text(seed(root_text, start))
    metadata = resolve()
    OVERLAY.write_text(overlay(NESTED_LOCK.read_text(), root_text))
    print(f"wrote {OVERLAY.relative_to(REPO)}: "
          f"{len(packages(OVERLAY.read_text()))} test-only packages")
    return 1 if report_drift(metadata, root_text) else 0


def cmd_check():
    write_seed()
    metadata = resolve()
    root_text = ROOT_LOCK.read_text()
    loose = unpinned(NESTED_LOCK.read_text(), root_text, OVERLAY.read_text())
    for name, version in loose:
        print(f"unpinned: {name} {version}")
    if loose:
        print("\nthe nested workspace resolved packages tn/test-only.lock does not pin; review "
              "them and run `python3 -I vendor/reth/tn/lockfile.py update`")
    drifted = report_drift(metadata, root_text)
    unpatched_count = report_unpatched(metadata, root_text)
    if loose or drifted or unpatched_count:
        return 1
    print(f"lockfile: every nested package is pinned, the vendored crates resolve TN's "
          f"versions ({len(packages(OVERLAY.read_text()))} test-only packages), and both "
          f"workspaces resolve the carried {', '.join(PATCHED)}")
    return 0


def main():
    cmds = {"seed": lambda: write_seed() or 0, "update": cmd_update, "check": cmd_check}
    if len(sys.argv) != 2 or sys.argv[1] not in cmds:
        sys.exit(f"usage: lockfile.py {{{'|'.join(cmds)}}}")
    sys.exit(cmds[sys.argv[1]]())


if __name__ == "__main__":
    main()
