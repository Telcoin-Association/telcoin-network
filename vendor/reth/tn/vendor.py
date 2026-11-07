#!/usr/bin/env python3
"""Maintain TN's vendored copy of reth under vendor/reth.

The vendored tree mirrors upstream paths: upstream `crates/storage/db` lives at
`vendor/reth/crates/storage/db`. Only the paths in `tn/crates.txt` are vendored, plus a few root
files (ROOT_FILES) and the workspace manifest. `tn/upstream.toml` pins the upstream commit the
copy was taken from; that commit is the merge base for every update.

The nested workspace manifest (`vendor/reth/Cargo.toml`) is upstream's root manifest with the
members and path dependencies of non-vendored crates filtered out, and the entries of its
[patch] sections dropped (`trim_manifest`; Cargo would apply those to reth's tests but never to
TN's build). Its "upstream version" is always that filtered text, so upstream edits to crates TN
does not vendor never show up as changes or conflicts.

Every operation that brings upstream content in (sync, pick, add, remove) is a per-file 3-way
merge: base = upstream at the old point, theirs = upstream at the new point, ours = the file in
vendor/reth. A file TN never touched is replaced outright; a file TN changed goes through
`git merge-file`, which leaves conflict markers when both sides touched the same lines. A file
upstream renamed (git's rename detection between the two upstream trees) is merged at its new
path against the old one, so TN's changes move with it. Each of these commands ends with the
same warning as `status` when a .gitignore rule hides files in the vendored scope.

Commands (run from anywhere; python3 -I vendor/reth/tn/vendor.py <cmd>):
  status            list files that differ from the pinned upstream (modified/added/deleted)
  diff [PATH...]    unified diff of TN's changes against the pinned upstream
  check             report workspace dependencies the vendored manifests use but the nested
                    workspace does not define (a crate upstream added that is not vendored)
  ledger            check CHANGES.md against `status`: every changed file named by an entry,
                    every `TN(TN-NNNN)` id in the code defined by one
  sync REF          move the copy to upstream REF (tag or commit), 3-way, and repin
  pick COMMIT       apply one upstream commit (its diff against its first parent), 3-way;
                    the pin does not move
  add PATH...       vendor more upstream paths at the pinned commit
  remove PATH...    drop vendored paths
  import            initial import of every listed path (refuses if any already exists)
  trim-manifest     print the filtered upstream manifest at the pin (or --ref)

The upstream clone defaults to $RETH_UPSTREAM, else ~/.cache/tn-vendor/reth (cloned on first
use). A blobless clone (--filter=blob:none) is fine: trees are read with `git archive`, which
fetches missing blobs in one batch.
"""

import argparse
import difflib
import io
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent  # vendor/reth/tn
VENDOR = HERE.parent  # vendor/reth
REPO = VENDOR.parent.parent  # TN repository root
CRATES_TXT = HERE / "crates.txt"
CHANGES_MD = VENDOR / "CHANGES.md"
UPSTREAM_TOML = HERE / "upstream.toml"
MANIFEST = "Cargo.toml"
# Upstream root files carried verbatim besides the manifest: the formatter and clippy configs
# keep `cargo fmt --all` and clippy reading reth's settings for reth's code, and the licenses
# travel with the code.
ROOT_FILES = ["rustfmt.toml", "clippy.toml", "LICENSE-APACHE", "LICENSE-MIT"]
DEFAULT_CLONE = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "tn-vendor" / "reth"


def die(msg):
    print(f"vendor.py: error: {msg}", file=sys.stderr)
    sys.exit(2)


def run(cmd, cwd=None, check=True, capture=True, input=None):
    r = subprocess.run(cmd, cwd=cwd, input=input, stdout=subprocess.PIPE if capture else None,
                       stderr=subprocess.PIPE if capture else None)
    if check and r.returncode != 0:
        err = r.stderr.decode(errors="replace") if capture else ""
        die(f"command failed ({r.returncode}): {' '.join(map(str, cmd))}\n{err}")
    return r


# ---------------------------------------------------------------------------------------------
# Pin and crate list


def read_pin():
    text = UPSTREAM_TOML.read_text()
    pin = dict(re.findall(r'^(\w+)\s*=\s*"([^"]*)"', text, re.M))
    for k in ("repo", "tag", "commit"):
        if k not in pin:
            die(f"{UPSTREAM_TOML} lacks `{k}`")
    return pin


def write_pin(tag, commit):
    text = UPSTREAM_TOML.read_text()
    text = re.sub(r'^tag\s*=\s*"[^"]*"', f'tag = "{tag}"', text, flags=re.M)
    text = re.sub(r'^commit\s*=\s*"[^"]*"', f'commit = "{commit}"', text, flags=re.M)
    UPSTREAM_TOML.write_text(text)


def read_crates():
    paths = []
    for line in CRATES_TXT.read_text().splitlines():
        p = line.split("#", 1)[0].strip().rstrip("/")
        if p:
            paths.append(p)
    if len(set(paths)) != len(paths):
        die(f"{CRATES_TXT} lists a path twice")
    return paths


def write_crates_add(paths):
    with CRATES_TXT.open("a") as f:
        for p in paths:
            f.write(f"{p}\n")


def write_crates_remove(paths):
    drop = set(paths)
    lines = CRATES_TXT.read_text().splitlines(keepends=True)
    kept = [l for l in lines if l.split("#", 1)[0].strip().rstrip("/") not in drop]
    CRATES_TXT.write_text("".join(kept))


# ---------------------------------------------------------------------------------------------
# Upstream clone


class Upstream:
    def __init__(self, path, repo_url):
        self.path = Path(path)
        self.url = repo_url
        self.fetched = False

    def ensure_clone(self):
        if (self.path / ".git").exists() or (self.path / "HEAD").exists():
            return
        print(f"cloning {self.url} into {self.path} (blobless)", file=sys.stderr)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        run(["git", "clone", "--filter=blob:none", "--no-checkout", self.url, str(self.path)],
            capture=False)

    def git(self, *args, check=True):
        return run(["git", "-C", str(self.path), *args], check=check)

    def resolve(self, ref):
        """Commit id for a tag/branch/commit, fetching tags once if it is not known yet."""
        self.ensure_clone()
        r = self.git("rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}", check=False)
        if r.returncode != 0 and not self.fetched:
            print(f"fetching {self.url} (looking for {ref})", file=sys.stderr)
            self.git("fetch", "--tags", "--force", "origin")
            self.fetched = True
            r = self.git("rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}", check=False)
        if r.returncode != 0:
            die(f"upstream ref {ref!r} not found in {self.path}")
        return r.stdout.decode().strip()

    def exists(self, commit, path):
        r = self.git("ls-tree", "--name-only", commit, "--", path)
        return bool(r.stdout.strip())

    def export(self, commit, crate_paths, dest):
        """Write the upstream view of the vendored scope at `commit` into `dest`.

        Crate paths that do not exist at `commit` are skipped (a crate added or removed between
        two upstream points). The manifest is written filtered to `crate_paths`.
        """
        dest = Path(dest)
        dest.mkdir(parents=True, exist_ok=True)
        present = [p for p in crate_paths + ROOT_FILES if self.exists(commit, p)]
        if present:
            tar = self.git("archive", "--format=tar", commit, "--", *present).stdout
            with tarfile.open(fileobj=io.BytesIO(tar)) as tf:
                tf.extractall(dest, filter="data")
        manifest = self.git("show", f"{commit}:{MANIFEST}").stdout.decode()
        (dest / MANIFEST).write_text(trim_manifest(manifest, crate_paths))
        return dest


def upstream_from(args, pin):
    path = args.upstream or os.environ.get("RETH_UPSTREAM") or DEFAULT_CLONE
    return Upstream(path, pin["repo"])


# ---------------------------------------------------------------------------------------------
# Manifest filter


# A first-level [patch.<source>] header. Anything deeper ([patch.<source>.<crate>]) is one entry.
PATCH_TABLE = re.compile(r"""\[patch\.(?:"[^"]*"|'[^']*'|[A-Za-z0-9_-]+)\]""")


def trim_manifest(text, keep_paths, dropped_patches=None):
    """Upstream root manifest restricted to `keep_paths`.

    Drops [workspace] members and default-members entries outside the list, and
    [workspace.dependencies] path entries pointing outside it. Line based on purpose: every
    kept line is byte-identical to upstream, so a diff between two filtered manifests is
    exactly upstream's change to the kept part.

    Also drops every entry of the [patch.*] sections, keeping their headers and comments. Cargo
    applies [patch] only from the root manifest of the workspace it builds, so TN's build never
    sees upstream's patches; carried here, they would make reth's own tests (tn/cargo.sh) build
    other sources than TN ships. The dropped lines go to `dropped_patches` when it is a list.
    """
    keep = {p.rstrip("/") for p in keep_paths}
    out = []
    section = ""
    in_members = False
    for line in text.splitlines(keepends=True):
        s = line.strip()
        if in_members:
            if s.startswith("]"):
                in_members = False
                out.append(line)
                continue
            m = re.match(r'"([^"]+)"', s)
            if m and m.group(1).rstrip("/") not in keep:
                continue
            out.append(line)
            continue
        if s.startswith("[") and not s.startswith("[["):
            section = s.split("#", 1)[0].strip()
        if section.startswith("[patch."):
            # A [patch.<source>] table keeps its header and comments and loses its entries; a
            # [patch.<source>.<crate>] table goes entirely.
            sub = not PATCH_TABLE.fullmatch(section)
            if s and (sub or s[0] not in "#["):
                if dropped_patches is not None and s[0] != "#":
                    dropped_patches.append(s if s[0] == "[" else f"{section} {s}")
                continue
        if section == "[workspace]":
            if re.match(r"members\s*=\s*\[\s*$", s):
                in_members = True
                out.append(line)
                continue
            if re.match(r"default-members\s*=", s):
                entries = re.findall(r'"([^"]+)"', s)
                kept = [e for e in entries if e.rstrip("/") in keep]
                if kept:
                    out.append("default-members = [" + ", ".join(f'"{e}"' for e in kept) + "]\n")
                continue
        if section == "[workspace.dependencies]":
            m = re.search(r'\bpath\s*=\s*"([^"]+)"', line)
            if m and m.group(1).rstrip("/") not in keep:
                continue
        out.append(line)
    return "".join(out)


def report_patches(old_manifest, new_manifest):
    """List the [patch] entries upstream added or removed between two of its root manifests.
    trim_manifest leaves them all out of vendor/reth/Cargo.toml; TN mirrors one only on purpose."""
    old, new = [], []
    trim_manifest(old_manifest, [], old)
    trim_manifest(new_manifest, [], new)
    added, removed = [e for e in new if e not in old], [e for e in old if e not in new]
    if not added and not removed:
        return 0
    print("\nupstream changed its [patch] entries. vendor/reth/Cargo.toml leaves them out: Cargo "
          "applies [patch] only from the root manifest, so TN's build never sees them, and reth's "
          "tests (tn/cargo.sh) should build what TN ships. If TN needs one, add it to the root "
          "Cargo.toml and to vendor/reth/Cargo.toml, with a CHANGES.md entry:")
    for e in added:
        print(f"  + {e}")
    for e in removed:
        print(f"  - {e}")
    return 1


# ---------------------------------------------------------------------------------------------
# File sets


def scope_files(root, crate_paths, include_manifest=True):
    """Relative paths of every file in the vendored scope under `root`."""
    root = Path(root)
    files = set()
    for p in crate_paths:
        d = root / p
        if not d.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(d):
            dirnames[:] = [n for n in dirnames if n != "target"]
            for n in filenames:
                if n == ".DS_Store":
                    continue
                files.add(str((Path(dirpath) / n).relative_to(root)))
    for f in ROOT_FILES + ([MANIFEST] if include_manifest else []):
        if (root / f).is_file():
            files.add(f)
    return files


def read(root, rel):
    p = Path(root) / rel
    return p.read_bytes() if p.is_file() else None


def is_binary(*blobs):
    return any(b is not None and b"\0" in b for b in blobs)


# ---------------------------------------------------------------------------------------------
# 3-way engine


def upstream_renames(base_root, theirs_root):
    """{old: new} for the files upstream renamed between two exported trees.

    Uses git's rename detection (`git diff --no-index -M`, 50% similarity), the rule `git merge`
    applies. Only renames count; a file git reports as a copy keeps its own history.
    """
    base_root, theirs_root = Path(base_root).resolve(), Path(theirs_root).resolve()
    top = Path(os.path.commonpath([base_root, theirs_root]))
    b, t = base_root.relative_to(top).as_posix(), theirs_root.relative_to(top).as_posix()
    r = run(["git", "diff", "--no-index", "--no-ext-diff", "-M", "-l0", "--name-status", "-z",
             "--", b, t], cwd=top, check=False)
    if r.returncode not in (0, 1):
        die(f"git diff --no-index {base_root} {theirs_root} failed: "
            f"{r.stderr.decode(errors='replace')}")
    fields = r.stdout.decode(errors="surrogateescape").split("\0")
    renames, i = {}, 0
    while i + 1 < len(fields):
        status = fields[i]
        if status[:1] in ("R", "C"):
            old, new = fields[i + 1], fields[i + 2]
            if status[0] == "R" and old.startswith(b + "/") and new.startswith(t + "/"):
                renames[old[len(b) + 1:]] = new[len(t) + 1:]
            i += 3
        else:
            i += 2
    return renames


def three_way(base_root, theirs_root, crate_paths, label_base, label_theirs, keep=()):
    """Merge upstream's base->theirs change into vendor/reth. Returns (changes, conflicts).

    Files under the paths in `keep` are left alone: a vendored crate whose directory vanished
    upstream (moved to its own repository, or deleted) stays TN's until someone decides.

    A file upstream renamed is merged at its new path, against the old path as base, so TN's
    changes follow the rename (`follow_rename`) instead of staying behind in a file the crate no
    longer compiles.
    """
    base_files = scope_files(base_root, crate_paths)
    theirs_files = scope_files(theirs_root, crate_paths)
    files = base_files | theirs_files | scope_files(VENDOR, crate_paths)
    changes, conflicts, done = [], [], set()
    for old, new in sorted(upstream_renames(base_root, theirs_root).items()):
        if old not in base_files or new not in theirs_files:
            continue
        if any(old.startswith(k + "/") for k in keep):
            o = read(VENDOR, old)
            if o is not None and o != read(base_root, old):
                conflicts.append((f"changed by TN in a path gone upstream; upstream moved the "
                                  f"file to {new}, which gets upstream's version without TN's "
                                  "change", old))
            continue  # `new` is merged below as an ordinary file
        follow_rename(old, new, base_root, theirs_root, label_base, label_theirs, changes,
                      conflicts)
        done |= {old, new}
    for rel in sorted(files - done):
        if any(rel.startswith(k + "/") for k in keep):
            continue
        kind, conflict = merge_one(rel, read(base_root, rel), read(theirs_root, rel),
                                   read(VENDOR, rel), label_base, label_theirs)
        if kind:
            changes.append((kind, rel))
        if conflict:
            conflicts.append((conflict, rel))
    return changes, conflicts


def merge_one(rel, b, t, o, label_base, label_theirs):
    """3-way merge of one file into vendor/reth/`rel`, given its base, theirs and ours bytes
    (None = absent). Returns (change kind, None), (None, conflict) or (None, None)."""
    if b == t or o == t:
        return None, None  # upstream did not touch it, or TN already has upstream's result
    dest = VENDOR / rel
    if o == b:  # TN never touched it: take upstream's version outright
        if t is None:
            dest.unlink()
            return "deleted", None
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(t)
        return ("added" if b is None else "updated"), None
    if o is None:  # TN deleted it; upstream changed it
        return None, "deleted by TN, changed upstream (left deleted)"
    if t is None:  # upstream deleted it; TN changed it
        return None, "changed by TN, deleted upstream (left in place)"
    if is_binary(b, t, o):
        return None, "binary changed on both sides (kept TN's)"
    with tempfile.TemporaryDirectory() as td:
        bp, tp = Path(td) / "base", Path(td) / "theirs"
        bp.write_bytes(b or b"")
        tp.write_bytes(t)
        r = run(["git", "merge-file", "--diff3", "-L", "TN", "-L", label_base, "-L", label_theirs,
                 str(dest), str(bp), str(tp)], check=False)
    if r.returncode < 0 or r.returncode > 127:
        die(f"git merge-file failed on {rel}: {r.stderr.decode(errors='replace')}")
    if r.returncode == 0:
        return "merged", None
    return None, f"{r.returncode} conflict(s), markers left in file"


def follow_rename(old, new, base_root, theirs_root, label_base, label_theirs, changes, conflicts):
    """Upstream renamed `old` to `new`: merge with base = upstream's `old`, theirs = upstream's
    `new`, ours = TN's file at whichever of the two paths it has."""
    b, t = read(base_root, old), read(theirs_root, new)
    o_old, o_new = read(VENDOR, old), read(VENDOR, new)
    moved = f"renamed upstream from {old}"
    if o_old is not None and o_new is not None:
        conflicts.append((f"renamed upstream to {new}, where TN has a file too (both left as "
                          "they are)", old))
        return
    if o_old is None and o_new is None:  # TN deleted it
        if b != t:
            conflicts.append((f"deleted by TN, renamed upstream to {new} and changed (left "
                              "deleted)", old))
        return
    if o_new is not None:  # TN already has it at the new path (an earlier pick of the rename)
        kind, conflict = merge_one(new, b, t, o_new, label_base, label_theirs)
        if kind:
            changes.append((kind, new))
        if conflict:
            conflicts.append((f"{conflict}; {moved}", new))
        return
    dest = VENDOR / new
    dest.parent.mkdir(parents=True, exist_ok=True)
    if o_old == b:  # TN never touched it
        dest.write_bytes(t)
        (VENDOR / old).unlink()
        changes.append(("renamed", f"{old} -> {new}"))
        return
    # TN changed it: move TN's version to the new path, then merge upstream's change into it.
    dest.write_bytes(o_old)
    (VENDOR / old).unlink()
    kind, conflict = merge_one(new, b, t, o_old, label_base, label_theirs)
    if conflict:
        conflicts.append((f"{conflict}; {moved}, TN's version moved here", new))
    else:
        changes.append(("merged", f"{new} ({moved})"))


def report(changes, conflicts):
    kinds = {}
    for k, _ in changes:
        kinds[k] = kinds.get(k, 0) + 1
    print("changes: " + (", ".join(f"{v} {k}" for k, v in sorted(kinds.items())) or "none"))
    for k, rel in changes:
        if k == "merged":
            print(f"  merged (TN + upstream)  {rel}")
    if conflicts:
        print(f"\n{len(conflicts)} CONFLICT(S) — resolve before committing:")
        for k, rel in conflicts:
            print(f"  {rel}: {k}")
    return 1 if conflicts else 0


def require_clean():
    r = run(["git", "-C", str(REPO), "status", "--porcelain", "--", str(VENDOR)])
    if r.stdout.strip():
        die("vendor/reth has uncommitted changes; commit or stash them first so the merge "
            "result can be reviewed (and reverted) on its own")


# ---------------------------------------------------------------------------------------------
# Commands


def cmd_status(args):
    pin = read_pin()
    crates = read_crates()
    up = upstream_from(args, pin)
    commit = up.resolve(pin["commit"])
    with tempfile.TemporaryDirectory() as td:
        base = up.export(commit, crates, td)
        rows = classify(base, crates)
    counts = {}
    for k, _ in rows:
        counts[k] = counts.get(k, 0) + 1
    print(f"vendor/reth vs upstream {pin['tag']} ({commit[:12]}): "
          + (", ".join(f"{v} {k}" for k, v in sorted(counts.items())) or "identical"))
    for k, rel in rows:
        print(f"  {k:9} {rel}")
    warn_ignored(crates)
    return 0


def classify(base, crates):
    rows = []
    for rel in sorted(scope_files(base, crates) | scope_files(VENDOR, crates)):
        b, o = read(base, rel), read(VENDOR, rel)
        if b == o:
            continue
        rows.append(("added" if b is None else "deleted" if o is None else "modified", rel))
    return rows


def warn_ignored(crates):
    """Files in the vendored scope that a .gitignore rule hides: `git add` would silently skip
    them and the committed copy would no longer match upstream."""
    paths = [str(VENDOR / p) for p in crates if (VENDOR / p).exists()]
    r = run(["git", "-C", str(REPO), "ls-files", "--others", "--ignored", "--exclude-standard",
             "-z", "--", *paths])
    names = sorted({n for n in r.stdout.decode().split("\0")
                    if n and "/target/" not in n and not n.endswith(".DS_Store")})
    if names:
        print(f"\nwarning: {len(names)} file(s) in the vendored scope are git-ignored (add a "
              "negation to vendor/reth/.gitignore):")
        for n in names[:50]:
            print(f"  {n}")
    return len(names)


def cmd_diff(args):
    pin = read_pin()
    crates = read_crates()
    up = upstream_from(args, pin)
    commit = up.resolve(pin["commit"])
    want = [p.rstrip("/") for p in args.paths]
    with tempfile.TemporaryDirectory() as td:
        base = up.export(commit, crates, td)
        for k, rel in classify(base, crates):
            if want and not any(rel == w or rel.startswith(w + "/") for w in want):
                continue
            b, o = read(base, rel), read(VENDOR, rel)
            if is_binary(b, o):
                print(f"Binary file {rel} {k}")
                continue
            a = (b or b"").decode(errors="replace").splitlines(keepends=True)
            c = (o or b"").decode(errors="replace").splitlines(keepends=True)
            sys.stdout.writelines(difflib.unified_diff(
                a, c, f"upstream/{rel}" if b is not None else "/dev/null",
                f"vendor/reth/{rel}" if o is not None else "/dev/null"))
    return 0


LEDGER_ENTRY = re.compile(r"^- \*\*(TN-\d{4})\*\*(.*?)(?=^- \*\*TN-\d{4}\*\*|^#|\Z)", re.S | re.M)
CODE_ID = re.compile(r"TN\(([^)]*)\)")


def expand_braces(text):
    """`a/{b,c/{d,e}}.rs` -> `a/b.rs`, `a/c/d.rs`, `a/c/e.rs`."""
    m = re.search(r"\{([^{}]*)\}", text)
    if not m:
        return [text]
    return [x for alt in m.group(1).split(",")
            for x in expand_braces(text[:m.start()] + alt.strip() + text[m.end():])]


def ledger_paths(text, crates):
    """Map every path the ledger's entries name to the ids of the entries that name it.

    A path is a backticked token, which may wrap across lines and use `{a,b}` alternatives. It
    is written from vendor/reth (`crates/...`, `testing/...`), or from a crate the same entry
    named earlier (`tests/it/startup.rs` after `crates/rpc/rpc-builder/src/lib.rs`). A directory
    names every file under it. The words "workspace manifest" name the root `Cargo.toml`."""
    named = {}
    by_len = sorted(crates, key=len, reverse=True)
    for m in LEDGER_ENTRY.finditer(text):
        tn, body = m.groups()
        if "workspace manifest" in re.sub(r"\s+", " ", body):
            named.setdefault(MANIFEST, set()).add(tn)
        bases = []
        for token in re.findall(r"`([^`]+)`", body):
            for raw in expand_braces(re.sub(r"\s*\n\s*", "", token).strip()):
                path = raw.rstrip("/")
                crate = next((c for c in by_len if path == c or path.startswith(c + "/")), None)
                if crate:
                    bases = [crate] + [b for b in bases if b != crate]
                    named.setdefault(path, set()).add(tn)
                elif path == MANIFEST or path.startswith(("crates/", "testing/")):
                    named.setdefault(path, set()).add(tn)
                elif "/" in raw or "." in raw:
                    for b in bases:
                        named.setdefault(f"{b}/{path}", set()).add(tn)
    return named


def ledger_problems(rows, text, crates, code_files):
    """What `ledger` reports, from `classify` rows, the ledger's text and {path: text} of the
    vendored files: changed files no entry names, and ids the code uses that no entry defines."""
    named = ledger_paths(text, crates)
    unnamed = [(k, rel) for k, rel in rows
               if not any(rel == p or rel.startswith(p + "/") for p in named)]
    defined = {m.group(1) for m in LEDGER_ENTRY.finditer(text)}
    undefined = {}
    for rel, body in sorted(code_files.items()):
        for m in CODE_ID.finditer(body):
            for tn in re.findall(r"TN-\d{4}", m.group(1)):
                if tn not in defined:
                    undefined.setdefault(tn, []).append(rel)
    return unnamed, undefined


def workspace_dep_names(manifest_text):
    names, section = set(), None
    for line in manifest_text.splitlines():
        s = line.strip()
        if s.startswith("["):
            section = s
            continue
        if section == "[workspace.dependencies]":
            m = re.match(r"([A-Za-z0-9_-]+)\s*=", s)
            if m:
                names.add(m.group(1))
    return names


def used_workspace_deps(crate_manifest):
    used = set()
    for line in crate_manifest.splitlines():
        s = line.strip()
        m = re.match(r"([A-Za-z0-9_-]+)\.workspace\s*=\s*true", s) or \
            re.match(r"([A-Za-z0-9_-]+)\s*=\s*\{[^}]*\bworkspace\s*=\s*true", s)
        if m and m.group(1) not in ("version", "edition", "rust-version", "license", "homepage",
                                    "repository", "exclude", "lints", "authors", "description",
                                    "readme", "keywords", "categories", "publish"):
            used.add(m.group(1))
    return used


def missing_deps(crates, upstream_manifest=None):
    defined = workspace_dep_names((VENDOR / MANIFEST).read_text())
    missing = {}
    for p in crates:
        m = VENDOR / p / "Cargo.toml"
        if not m.is_file():
            continue
        for name in used_workspace_deps(m.read_text()) - defined:
            missing.setdefault(name, []).append(p)
    hints = {}
    if upstream_manifest:
        for line in upstream_manifest.splitlines():
            m = re.match(r'([A-Za-z0-9_-]+)\s*=\s*\{[^}]*\bpath\s*=\s*"([^"]+)"', line.strip())
            if m:
                hints[m.group(1)] = m.group(2).rstrip("/")
    return missing, hints


def print_missing(missing, hints):
    if not missing:
        print("check: every workspace dependency the vendored crates use is defined")
        return 0
    print("check: workspace dependencies used but not defined in vendor/reth/Cargo.toml:")
    for name, users in sorted(missing.items()):
        hint = f" -> vendor it: vendor.py add {hints[name]}" if name in hints else ""
        print(f"  {name} (used by {', '.join(users)}){hint}")
    return 1


def cmd_check(args):
    pin = read_pin()
    crates = read_crates()
    up = upstream_from(args, pin)
    commit = up.resolve(pin["commit"])
    manifest = up.git("show", f"{commit}:{MANIFEST}").stdout.decode()
    return print_missing(*missing_deps(crates, manifest))


def cmd_ledger(args):
    pin = read_pin()
    crates = read_crates()
    up = upstream_from(args, pin)
    commit = up.resolve(pin["commit"])
    with tempfile.TemporaryDirectory() as td:
        base = up.export(commit, crates, td)
        rows = classify(base, crates)
    code = {rel: (read(VENDOR, rel) or b"").decode(errors="replace")
            for rel in scope_files(VENDOR, crates)}
    unnamed, undefined = ledger_problems(rows, CHANGES_MD.read_text(), crates, code)
    for k, rel in unnamed:
        print(f"not in CHANGES.md: {k:9} {rel}")
    for tn, rels in sorted(undefined.items()):
        print(f"no CHANGES.md entry for {tn}: {', '.join(rels)}")
    if unnamed or undefined:
        print("\nname each file in an entry's *Files* line (from vendor/reth, or from a crate the "
              "entry named before it), and give each id an entry")
        return 1
    print(f"ledger: CHANGES.md names all {len(rows)} changed files and defines every TN id")
    return 0


def cmd_sync(args):
    require_clean()
    pin = read_pin()
    crates = read_crates()
    up = upstream_from(args, pin)
    old = up.resolve(pin["commit"])
    new = up.resolve(args.ref)
    if old == new:
        print(f"already at {args.ref} ({new[:12]})")
        return 0
    gone = [p for p in crates if not up.exists(new, p)]
    with tempfile.TemporaryDirectory() as td:
        base = up.export(old, crates, Path(td) / "base")
        theirs = up.export(new, crates, Path(td) / "theirs")
        changes, conflicts = three_way(base, theirs, crates, f"upstream {pin['tag']}",
                                       f"upstream {args.ref}", keep=gone)
    write_pin(args.ref, new)
    print(f"synced vendor/reth {pin['tag']} ({old[:12]}) -> {args.ref} ({new[:12]})")
    rc = report(changes, conflicts)
    if gone:
        print("\nvendored paths gone upstream at the new point (moved to another repository or "
              "deleted). Their files were left as they were. Either keep one as a TN-owned fork, "
              "or `vendor.py remove` it and depend on its new home:")
        for p in gone:
            print(f"  {p}")
        rc = 1
    manifest = up.git("show", f"{new}:{MANIFEST}").stdout.decode()
    if print_missing(*missing_deps(crates, manifest)):
        rc = 1
    if report_patches(up.git("show", f"{old}:{MANIFEST}").stdout.decode(), manifest):
        rc = 1
    if warn_ignored(crates):
        rc = 1
    print("\nnext: resolve anything above, build and test (see vendor/reth/README.md), add a "
          "CHANGES.md entry for the sync, commit.")
    return rc


def cmd_pick(args):
    require_clean()
    pin = read_pin()
    crates = read_crates()
    up = upstream_from(args, pin)
    commit = up.resolve(args.commit)
    parent = up.resolve(f"{commit}^1")
    with tempfile.TemporaryDirectory() as td:
        base = up.export(parent, crates, Path(td) / "base")
        theirs = up.export(commit, crates, Path(td) / "theirs")
        changes, conflicts = three_way(base, theirs, crates, f"upstream {parent[:12]}",
                                       f"upstream {commit[:12]}")
    print(f"picked upstream {commit[:12]} onto vendor/reth (pin stays {pin['tag']})")
    rc = report(changes, conflicts)
    if not changes and not conflicts:
        print("(the commit touches nothing in the vendored scope)")
    if report_patches(up.git("show", f"{parent}:{MANIFEST}").stdout.decode(),
                      up.git("show", f"{commit}:{MANIFEST}").stdout.decode()):
        rc = 1
    if warn_ignored(crates):
        rc = 1
    print("\nnext: build and test, record the backport in CHANGES.md, commit.")
    return rc


def reshape(args, new_crates, verb):
    """Shared by add/remove: bring the vendored scope from read_crates() to new_crates."""
    pin = read_pin()
    old_crates = read_crates()
    up = upstream_from(args, pin)
    commit = up.resolve(pin["commit"])
    with tempfile.TemporaryDirectory() as td:
        base = up.export(commit, old_crates, Path(td) / "base")
        theirs = up.export(commit, new_crates, Path(td) / "theirs")
        # Files of paths leaving the scope are invisible to the scoped walk; remove them here.
        for p in set(old_crates) - set(new_crates):
            shutil.rmtree(VENDOR / p, ignore_errors=True)
        changes, conflicts = three_way(base, theirs, sorted(set(old_crates) | set(new_crates)),
                                       f"upstream {pin['tag']}", f"upstream {pin['tag']} ({verb})")
    rc = report(changes, conflicts)
    return 1 if warn_ignored(new_crates) else rc


def cmd_add(args):
    require_clean()
    pin = read_pin()
    crates = read_crates()
    up = upstream_from(args, pin)
    commit = up.resolve(pin["commit"])
    new = []
    for p in (p.rstrip("/") for p in args.paths):
        if p in crates:
            die(f"{p} is already vendored")
        if not up.exists(commit, p):
            die(f"{p} does not exist upstream at {pin['tag']}")
        if (VENDOR / p).exists() and not any(p.startswith(c + "/") for c in crates):
            die(f"vendor/reth/{p} exists on disk but is not listed; remove it first")
        new.append(p)
    rc = reshape(args, crates + new, "add")
    write_crates_add(new)
    manifest = up.git("show", f"{commit}:{MANIFEST}").stdout.decode()
    return print_missing(*missing_deps(read_crates(), manifest)) or rc


def cmd_remove(args):
    require_clean()
    crates = read_crates()
    drop = [p.rstrip("/") for p in args.paths]
    for p in drop:
        if p not in crates:
            die(f"{p} is not vendored")
        inner = [c for c in crates if c.startswith(p + "/") and c not in drop]
        if inner:
            die(f"{p} contains vendored {', '.join(inner)}; remove those too or keep {p}")
    rc = reshape(args, [c for c in crates if c not in drop], "remove")
    write_crates_remove(drop)
    return rc


def cmd_import(args):
    pin = read_pin()
    crates = read_crates()
    for p in crates + ROOT_FILES + [MANIFEST]:
        if (VENDOR / p).exists():
            die(f"vendor/reth/{p} already exists; import is for an empty vendor/reth only")
    up = upstream_from(args, pin)
    commit = up.resolve(pin["commit"])
    missing = [p for p in crates if not up.exists(commit, p)]
    if missing:
        die(f"not found upstream at {pin['tag']}: {', '.join(missing)}")
    up.export(commit, crates, VENDOR)
    n = len(scope_files(VENDOR, crates))
    print(f"imported {len(crates)} paths ({n} files) from {pin['tag']} ({commit[:12]})")
    return 1 if warn_ignored(crates) else 0


def cmd_trim_manifest(args):
    pin = read_pin()
    up = upstream_from(args, pin)
    commit = up.resolve(args.ref or pin["commit"])
    sys.stdout.write(trim_manifest(up.git("show", f"{commit}:{MANIFEST}").stdout.decode(),
                                   read_crates()))
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--upstream", help="upstream reth clone (default $RETH_UPSTREAM or "
                    f"{DEFAULT_CLONE})")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status").set_defaults(fn=cmd_status)
    p = sub.add_parser("diff")
    p.add_argument("paths", nargs="*")
    p.set_defaults(fn=cmd_diff)
    sub.add_parser("check").set_defaults(fn=cmd_check)
    sub.add_parser("ledger").set_defaults(fn=cmd_ledger)
    p = sub.add_parser("sync")
    p.add_argument("ref")
    p.set_defaults(fn=cmd_sync)
    p = sub.add_parser("pick")
    p.add_argument("commit")
    p.set_defaults(fn=cmd_pick)
    p = sub.add_parser("add")
    p.add_argument("paths", nargs="+")
    p.set_defaults(fn=cmd_add)
    p = sub.add_parser("remove")
    p.add_argument("paths", nargs="+")
    p.set_defaults(fn=cmd_remove)
    sub.add_parser("import").set_defaults(fn=cmd_import)
    p = sub.add_parser("trim-manifest")
    p.add_argument("--ref")
    p.set_defaults(fn=cmd_trim_manifest)
    args = ap.parse_args()
    sys.exit(args.fn(args))


if __name__ == "__main__":
    main()
