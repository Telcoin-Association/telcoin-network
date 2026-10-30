#!/usr/bin/env python3
"""Snapshot and compare the feature-resolved dependency graph of TN's node binary.

Removing a vendored crate, or a dependency edge, can silently drop a cargo feature that another
crate relied on through feature unification. Some of those features are consensus-critical,
for example the revm precompile backends `secp256k1`, `blst` and `c-kzg`. Snapshot before and
after the change and diff:

  python3 -I vendor/reth/tn/graph.py snapshot /tmp/before.txt [-- extra cargo tree args]
  ... make the change ...
  python3 -I vendor/reth/tn/graph.py snapshot /tmp/after.txt
  python3 -I vendor/reth/tn/graph.py diff /tmp/before.txt /tmp/after.txt

A snapshot is one line per package: `name version feature,feature,...`. It covers the
production graph (normal and build edges, all targets) of the `telcoin-network` package with
its default features. Pass `--features telcoin-network/tndb-b`, `-p <pkg>` and the like after
`--` to snapshot other configurations.

`diff` lists packages that left or entered the graph and per-package feature changes. A reth
crate leaving is the intended effect of a prune. A third-party crate losing a feature needs an
explanation before the change lands.
"""

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]


def snapshot(out, extra):
    args = ["cargo", "tree", "--locked", "-e", "normal,build", "--target", "all", "--prefix",
            "none", "-f", "{p}|{f}"]
    if not any(a in ("-p", "--package") for a in extra):
        args += ["-p", "telcoin-network"]
    r = subprocess.run(args + extra, cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       text=True)
    if r.returncode != 0:
        sys.exit(f"cargo tree failed:\n{r.stderr}")
    pkgs = {}
    for line in r.stdout.splitlines():
        line = line.removesuffix(" (*)").strip()
        if "|" not in line:
            continue
        p, f = line.split("|", 1)
        parts = p.split()  # name vX.Y.Z [(source)] [(proc-macro)]
        key = f"{parts[0]} {parts[1]}"
        feats = set(x for x in f.split(",") if x)
        pkgs.setdefault(key, set()).update(feats)
    Path(out).write_text("".join(f"{k} {','.join(sorted(v))}\n" for k, v in sorted(pkgs.items())))
    print(f"{len(pkgs)} packages -> {out}")


def load(path):
    pkgs = {}
    for line in Path(path).read_text().splitlines():
        parts = line.split(" ")
        pkgs[f"{parts[0]} {parts[1]}"] = set(x for x in (parts[2] if len(parts) > 2 else "").split(",") if x)
    return pkgs


def diff(a_path, b_path):
    a, b = load(a_path), load(b_path)
    gone, new = sorted(set(a) - set(b)), sorted(set(b) - set(a))
    reth_gone = [k for k in gone if k.startswith("reth")]
    other_gone = [k for k in gone if not k.startswith("reth")]
    print(f"packages: {len(a)} -> {len(b)}")
    print(f"left the graph: {len(reth_gone)} reth, {len(other_gone)} other")
    for k in reth_gone:
        print(f"  - {k}")
    for k in other_gone:
        print(f"  - {k}")
    if new:
        print(f"entered the graph: {len(new)}")
        for k in new:
            print(f"  + {k}")
    changed = 0
    for k in sorted(set(a) & set(b)):
        lost, gained = a[k] - b[k], b[k] - a[k]
        if lost or gained:
            changed += 1
            print(f"  ~ {k}: " + " ".join([f"-{x}" for x in sorted(lost)] + [f"+{x}" for x in sorted(gained)]))
    print(f"feature changes on surviving packages: {changed}")
    return 1 if changed or new else 0


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "snapshot":
        extra = sys.argv[3:]
        if extra and extra[0] == "--":
            extra = extra[1:]
        snapshot(sys.argv[2], extra)
    elif len(sys.argv) == 4 and sys.argv[1] == "diff":
        sys.exit(diff(sys.argv[2], sys.argv[3]))
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
