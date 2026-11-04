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

A snapshot covers the production graph (normal and build edges) of the `telcoin-network`
package with its default features, one compilation unit per line:
`<view> <target> <name> <version> <feature,feature,...>`. Cargo resolves features separately for
the code that ends up in the binary and for the code that runs while building it, so a package
can have two units with different features. The views keep them apart:

  run    units linked into the binary for <target>: normal dependencies, not through a proc macro
  build  units compiled for the build host: build-dependencies, proc macros, and what they use

The targets are x86_64-unknown-linux-gnu (the release target) and aarch64-apple-darwin.
`--target <triple>` after `--` replaces them (repeat it for several). Other arguments after `--`
go to each `cargo tree` call: `--features telcoin-network/tndb-b`, `-p <pkg>`, `--offline`, or
`--manifest-path <checkout>/Cargo.toml` to snapshot another checkout, such as the commit before
a change, with this script.

`diff` compares view by view: packages that left or entered and per-package feature changes.
A reth crate leaving is the intended effect of a prune. A third-party crate losing a run-time
feature needs an explanation before the change lands. A build-time change alters how a build
script or proc macro is compiled, which rarely changes the binary, but say why it happened.
Snapshots taken before views existed (one line per package, every unit and target merged) still
diff against each other, but not against new ones.
"""

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
TARGETS = ["x86_64-unknown-linux-gnu", "aarch64-apple-darwin"]
HEADER = "# graph.py snapshot: <view> <target> <name> <version> <features>"
VIEWS = {"run": "run-time units", "build": "build-time units"}
# One `cargo tree --charset ascii` line: a prefix of 4-character columns, then the node.
TREE_LINE = re.compile(r"((?:\|-- |`-- |\|   |    )*)(.*)")


def units(tree):
    """{(view, "name version"): "feature,..."} from `cargo tree --charset ascii -f '{p}|{f}'`.

    A node is a build-time unit if it is a proc macro, sits under a [build-dependencies] label,
    or hangs below a build-time unit. Cargo prints each unit once per position, with `(*)` where
    it does not repeat the subtree, so every unit's features are on each of its lines.
    """
    out = {}
    path = []  # path[d] = [is_build_unit, below_build_label] for the node at depth d
    for line in tree.splitlines():
        if not line.strip():
            path = []  # the next root's tree follows
            continue
        prefix, body = TREE_LINE.fullmatch(line).groups()
        depth = len(prefix) // 4
        if body.startswith("["):  # e.g. "[build-dependencies]": its children follow at depth + 1
            del path[depth + 1:]
            path[depth][1] = body == "[build-dependencies]"
            continue
        del path[depth:]
        pkg, feats = body.removesuffix(" (*)").rsplit("|", 1)
        name, version = pkg.split()[:2]
        build = " (proc-macro)" in pkg or bool(path) and (path[-1][0] or path[-1][1])
        key = ("build" if build else "run", f"{name} {version}")
        feats = ",".join(sorted(f for f in feats.split(",") if f))
        if out.setdefault(key, feats) != feats:
            sys.exit(f"cargo tree shows two {key[0]} units of {key[1]} with different features "
                     f"({out[key]!r} and {feats!r}); graph.py keys units by name and version")
        path.append([build, False])
    return out


def snapshot(out, extra):
    targets, rest, args = [], [], iter(extra)
    for a in args:
        if a == "--target":
            targets.append(next(args, ""))
        elif a.startswith("--target="):
            targets.append(a.split("=", 1)[1])
        else:
            rest.append(a)
    if not any(a in ("-p", "--package") for a in rest):
        rest += ["-p", "telcoin-network"]
    targets = targets or TARGETS
    rows = []
    for target in targets:
        r = subprocess.run(["cargo", "tree", "--locked", "-e", "normal,build", "--target", target,
                            "--charset", "ascii", "-f", "{p}|{f}", *rest], cwd=REPO,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if r.returncode != 0:
            sys.exit(f"cargo tree failed:\n{r.stderr}")
        rows += [(view, target, key, feats) for (view, key), feats in units(r.stdout).items()]
    rows.sort(key=lambda r: (list(VIEWS).index(r[0]), targets.index(r[1]), r[2]))
    Path(out).write_text(HEADER + "\n" + "".join(f"{v} {t} {k} {f}\n" for v, t, k, f in rows))
    counts = {}
    for v, t, _, _ in rows:
        counts[(v, t)] = counts.get((v, t), 0) + 1
    print(", ".join(f"{n} {v} {t}" for (v, t), n in counts.items()) + f" -> {out}")


def load(path):
    """(per_unit, {(view, target): {"name version": {features}}}). A snapshot without the
    header is the old format: one merged view, ("all", "all")."""
    lines = Path(path).read_text().splitlines()
    per_unit = bool(lines) and lines[0] == HEADER
    views = {}
    for line in lines[per_unit:]:
        parts = line.split(" ")
        if per_unit:
            view, target, parts = parts[0], parts[1], parts[2:]
        else:
            view, target = "all", "all"
        feats = parts[2] if len(parts) > 2 else ""
        views.setdefault((view, target), {})[f"{parts[0]} {parts[1]}"] = {
            f for f in feats.split(",") if f}
    return per_unit, views


def diff_view(a, b):
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


def diff(a_path, b_path):
    (a_unit, a), (b_unit, b) = load(a_path), load(b_path)
    if a_unit != b_unit:
        print("one snapshot has per-unit views and the other does not (taken by an older "
              "graph.py); snapshot both with this one", file=sys.stderr)
        return 2
    if set(a) != set(b):
        print(f"the snapshots cover different views: {sorted(a)} and {sorted(b)}; take both with "
              "the same --target arguments", file=sys.stderr)
        return 2
    if not a_unit:
        return diff_view(a[("all", "all")], b[("all", "all")])
    rc = 0
    for i, (view, target) in enumerate(a):  # the snapshot's order: run views first
        if i:
            print()
        print(f"{VIEWS[view]}, {target}")
        rc |= diff_view(a[(view, target)], b[(view, target)])
    return rc


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
