"""The two halves of etc/tn-lmdb-bench-compare.sh that need a parser: the standalone bench
manifest, and the comparison and its report. Standard library only (tomllib: Python 3.11 or
later); the script runs it isolated (`python3 -I`), so nothing beside it is imported.

    python3 -I etc/tn-lmdb-bench-compare.py manifest <bench Cargo.toml> <root Cargo.toml> <out>
    python3 -I etc/tn-lmdb-bench-compare.py report <bench dir> [--threshold PCT]
                                            [--size-threshold PCT] [--gate-durable]

`manifest` writes the Cargo.toml of one side's standalone build of the bench: the bench crate's
own manifest with every workspace-inherited field and dependency resolved against the head's
root manifest (so the package needs no workspace), `tn-lmdb` taken from `engine/crates/tn-lmdb`
(the side's checkout, linked into the build dir), the root's `[profile.release]` copied, and an
empty `[workspace]` that excludes `engine` (a path dependency inside a workspace's directory is
otherwise made a member of it). Exits 2 when the bench depends on a workspace crate other than
tn-lmdb.

`report` compares the bench dir's `json/base-<round>.json` against its `json/head-<round>.json`
(each one bench run's JSON, schema 1), writes `report.md` there and prints it. The run settings
come from the dir's `meta.tsv` when there is one (key, tab, value per line); a threshold given
on the command line wins over it. Exits 0 with no gated regression, 1 with one, 2 when the
JSON is missing, of another schema or shape, or of runs with different configs.

Any other failure (an unreadable file, a manifest of a shape `manifest` does not expect) exits 2
too, never 1: 1 means a gated regression to the script's callers.
"""

import json
import math
import os
import re
import statistics
import sys

ENGINE_PATH = "engine/crates/tn-lmdb"
ENGINE_CRATE = "tn-lmdb"
SCHEMA = 1


def fail(message):
    print(f"tn-lmdb-bench-compare: {message}", file=sys.stderr)
    sys.exit(2)


# ---------------------------------------------------------------------------------------------
# manifest
# ---------------------------------------------------------------------------------------------

def toml_key(key):
    return key if re.fullmatch(r"[A-Za-z0-9_-]+", key) else json.dumps(key)


def toml_value(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "[" + ", ".join(toml_value(item) for item in value) + "]"
    if isinstance(value, dict):
        items = ", ".join(f"{toml_key(k)} = {toml_value(v)}" for k, v in value.items())
        return "{ " + items + " }" if items else "{}"
    fail(f"cannot write {value!r} as TOML")


def emit_section(lines, header, table):
    """`[header]` with the table's values, then each nested table as its own section (a table
    that holds only tables gets no header of its own)."""
    nested = [(key, value) for key, value in table.items() if isinstance(value, dict)]
    values = [(key, value) for key, value in table.items() if not isinstance(value, dict)]
    if values or not nested:
        lines.append(f"[{header}]")
        lines += [f"{toml_key(key)} = {toml_value(value)}" for key, value in values]
        lines.append("")
    for key, value in nested:
        emit_section(lines, f"{header}.{toml_key(key)}", value)


def load_toml(path):
    import tomllib

    with open(path, "rb") as f:
        return tomllib.load(f)


def manifest(bench_path, root_path, out_path):
    bench = load_toml(bench_path)
    root = load_toml(root_path)
    workspace = root.get("workspace", {})
    workspace_package = workspace.get("package", {})
    workspace_deps = workspace.get("dependencies", {})

    def inherited(table, key):
        value = table.get(key)
        if isinstance(value, dict) and value.get("workspace") is True:
            if key not in workspace_package:
                fail(f"{bench_path}: package.{key} inherits from the workspace, which has none")
            return workspace_package[key]
        return value

    package = bench.get("package", {})
    out_package = {}
    # The fields that shape the build; the rest (description, license, readme, ...) are
    # metadata a local build never reads, and some name files the build dir does not have.
    for key in ("name", "version", "edition", "rust-version", "autobins", "autolib",
                "autoexamples", "autotests", "autobenches", "build", "default-run", "links"):
        value = inherited(package, key)
        if value is not None:
            out_package[key] = value
    if "name" not in out_package:
        fail(f"{bench_path} has no package.name")
    out_package.setdefault("version", "0.0.0")
    out_package["publish"] = False

    def resolve(name, spec, where):
        if isinstance(spec, str):
            spec = {"version": spec}
        spec = dict(spec)
        if spec.pop("workspace", False) is True:
            if name not in workspace_deps:
                fail(f"{bench_path}: {where}.{name} inherits from the workspace, which has no {name}")
            base = workspace_deps[name]
            base = {"version": base} if isinstance(base, str) else dict(base)
            features = list(base.get("features", []))
            features += [f for f in spec.pop("features", []) if f not in features]
            base.update(spec)
            if features:
                base["features"] = features
            spec = base
        if spec.get("package", name) == ENGINE_CRATE:
            # The side's engine, whatever the bench manifest says about where it lives.
            for key in ("path", "version", "git", "branch", "tag", "rev", "registry"):
                spec.pop(key, None)
            return {"path": ENGINE_PATH, **spec}
        if "path" in spec:
            fail(f"{bench_path}: {where}.{name} is a path dependency ({spec['path']}): the bench "
                 f"may depend on {ENGINE_CRATE} and published crates only, so that both sides "
                 "build the same code around their engine")
        return spec

    def resolve_table(table, where):
        return {name: resolve(name, spec, where) for name, spec in table.items()}

    lines = ["# Generated by etc/tn-lmdb-bench-compare.sh: one side's standalone bench build.", ""]
    emit_section(lines, "package", out_package)
    if "lib" in bench:
        emit_section(lines, "lib", bench["lib"])
    for target in bench.get("bin", []):
        lines.append("[[bin]]")
        for key, value in target.items():
            lines.append(f"{toml_key(key)} = {toml_value(value)}")
        lines.append("")
    if "features" in bench:
        emit_section(lines, "features", bench["features"])
    for section in ("dependencies", "build-dependencies"):
        if section in bench:
            resolved = resolve_table(bench[section], section)
            lines.append(f"[{section}]")
            lines += [f"{toml_key(k)} = {toml_value(v)}" for k, v in resolved.items()]
            lines.append("")
    for cfg, table in bench.get("target", {}).items():
        for section in ("dependencies", "build-dependencies"):
            if section in table:
                where = f"target.{cfg}.{section}"
                resolved = resolve_table(table[section], where)
                lines.append(f"[target.{toml_key(cfg)}.{section}]")
                lines += [f"{toml_key(k)} = {toml_value(v)}" for k, v in resolved.items()]
                lines.append("")
    release = root.get("profile", {}).get("release")
    if release:
        emit_section(lines, "profile.release", release)
    overlay_workspace = {"exclude": ["engine"]}
    if "resolver" in workspace:
        overlay_workspace["resolver"] = workspace["resolver"]
    emit_section(lines, "workspace", overlay_workspace)
    with open(out_path, "w") as f:
        f.write("\n".join(lines))


# ---------------------------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------------------------

def read_meta(bench_dir):
    meta = {}
    path = os.path.join(bench_dir, "meta.tsv")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.rstrip("\n")
                if line:
                    key, _, value = line.partition("\t")
                    meta[key] = value
    return meta


def valid_row(row):
    """Whether `row` has the shape `pool` and `report` read: a string name and numeric samples."""
    return (isinstance(row, dict) and isinstance(row.get("name"), str)
            and isinstance(row.get("samples"), list)
            and all(isinstance(s, (int, float)) and not isinstance(s, bool)
                    for s in row["samples"]))


def load_runs(bench_dir, side):
    json_dir = os.path.join(bench_dir, "json")
    runs = []
    if os.path.isdir(json_dir):
        for name in os.listdir(json_dir):
            match = re.fullmatch(rf"{side}-([0-9]+)\.json", name)
            if match:
                runs.append((int(match.group(1)), os.path.join(json_dir, name)))
    if not runs:
        fail(f"no {side} runs in {json_dir} (json/{side}-<round>.json)")
    docs = []
    for _, path in sorted(runs):
        try:
            with open(path) as f:
                doc = json.load(f)
        except (OSError, ValueError) as err:
            fail(f"{path}: {err}")
        if not isinstance(doc, dict):
            fail(f"{path}: not a tn-lmdb-bench JSON (the top level is not an object)")
        if doc.get("schema") != SCHEMA or doc.get("bench") != "tn-lmdb-bench":
            fail(f"{path}: not a tn-lmdb-bench JSON of schema {SCHEMA} (schema "
                 f"{doc.get('schema')!r}, bench {doc.get('bench')!r})")
        rows = doc.get("rows")
        if not isinstance(rows, list) or not all(valid_row(row) for row in rows):
            fail(f"{path}: `rows` must be a list of rows, each with a string `name` and a list "
                 "of numbers `samples`")
        doc["_path"] = path
        docs.append(doc)
    return docs


CONFIG_KEYS = ("tier", "n", "reps", "page_size", "os", "arch")


def pool(docs):
    """Each row's samples over a side's runs, in the order of first appearance. A row is its
    name and its sync mode: the rows a sync mode can change run once per selected mode."""
    rows = {}
    for doc in docs:
        for row in doc.get("rows", []):
            key = (row["name"], row.get("sync", ""))
            entry = rows.setdefault(key, {"row": row, "samples": []})
            entry["samples"] += [float(s) for s in row.get("samples", [])]
    for (name, sync), entry in rows.items():
        if not entry["samples"]:
            fail(f"row {name} ({sync}) has no samples")
    return rows


def ratio(head, base):
    if base == 0:
        return 1.0 if head == 0 else math.inf
    return head / base


def fmt_value(value, unit, exact=False):
    if exact or unit == "B":
        # A size or count: in full, so that a change in the last digit shows.
        text = f"{int(value):,}" if value == int(value) else f"{value:,.6g}"
        return f"{text} {unit}".rstrip()
    if unit == "ns":
        for scale, name in ((1e9, "s"), (1e6, "ms"), (1e3, "us")):
            if value >= scale:
                return f"{value / scale:.4g} {name}"
        return f"{value:.4g} ns"
    return f"{value:.4g} {unit}".rstrip()


def fmt_ratio(value):
    return "inf" if math.isinf(value) else f"{value:.3f}"


def spread(samples):
    middle = statistics.median(samples)
    if middle == 0:
        return 0.0 if max(samples) == min(samples) else math.inf
    return (max(samples) - min(samples)) / middle


def fmt_pct(value):
    return "inf" if math.isinf(value) else f"{value * 100:.1f}%"


def report(bench_dir, args):
    meta = read_meta(bench_dir)
    settings = {
        "threshold": meta.get("threshold", "10"),
        "size_threshold": meta.get("size_threshold", "1"),
        "gate_durable": meta.get("gate_durable", "0"),
    }
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in ("--threshold", "--size-threshold") and i + 1 < len(args):
            settings[arg[2:].replace("-", "_")] = args[i + 1]
            i += 2
        elif arg == "--gate-durable":
            settings["gate_durable"] = "1"
            i += 1
        else:
            fail(f"report: unknown argument {arg}")
    try:
        threshold = float(settings["threshold"]) / 100
        size_threshold = float(settings["size_threshold"]) / 100
    except ValueError:
        fail(f"thresholds must be numbers (time {settings['threshold']}, "
             f"size {settings['size_threshold']})")
    if threshold < 0 or size_threshold < 0:
        fail("thresholds must not be negative")
    gate_durable = settings["gate_durable"] == "1"

    base_docs = load_runs(bench_dir, "base")
    head_docs = load_runs(bench_dir, "head")
    first = base_docs[0]
    for doc in base_docs + head_docs:
        for key in CONFIG_KEYS:
            if doc.get(key) != first.get(key):
                fail(f"{doc['_path']} ran {key} {doc.get(key)!r}, {first['_path']} "
                     f"{first.get(key)!r}: the runs must share one config")
    base_rows = pool(base_docs)
    head_rows = pool(head_docs)
    keys = list(head_rows) + [key for key in base_rows if key not in head_rows]

    table = []
    gated_regressions = []
    ungated_over = []
    counts = {}
    for key in keys:
        name = key[0]
        label = f"{name} ({key[1]})"
        source = head_rows.get(key, base_rows.get(key))["row"]
        group, sync, unit = source.get("group", ""), source.get("sync", ""), source.get("unit", "")
        exact = bool(source.get("exact", False))
        if key not in base_rows or key not in head_rows:
            verdict = "new" if key not in base_rows else "removed"
            samples = (head_rows if key in head_rows else base_rows)[key]["samples"]
            value = fmt_value(statistics.median(samples), unit, exact)
            base_text, head_text = ("-", value) if verdict == "new" else (value, "-")
            table.append([name, group, sync, base_text, head_text, "-", "-", "-", verdict])
            counts[verdict] = counts.get(verdict, 0) + 1
            continue
        base = base_rows[key]["samples"]
        head = head_rows[key]["samples"]
        base_median, head_median = statistics.median(base), statistics.median(head)
        median_ratio = ratio(head_median, base_median)
        min_ratio = ratio(min(head), min(base))
        if exact:
            if head_median > base_median * (1 + size_threshold):
                verdict = "REGRESSION"
            elif head_median < base_median:
                verdict = "improved"
            else:
                verdict = "ok"
        else:
            # Both ratios must clear their bar, so one noisy run on either side cannot decide;
            # `improved` is the mirror image (the same test with the sides swapped).
            if median_ratio > 1 + threshold and min_ratio > 1 + threshold / 2:
                verdict = "REGRESSION"
            elif (ratio(base_median, head_median) > 1 + threshold
                  and ratio(min(base), min(head)) > 1 + threshold / 2):
                verdict = "improved"
            else:
                verdict = "ok"
        # A row the bench marks `gate: false` (a commit row of a small run) is reported only, an
        # exact one too; a durable time row is gated only with --gate-durable.
        gated = bool(source.get("gate", True)) and (exact or sync != "durable" or gate_durable)
        if not gated:
            if verdict == "REGRESSION":
                ungated_over.append(label)
            verdict = "not gated"
        elif verdict == "REGRESSION":
            gated_regressions.append(label)
        counts[verdict] = counts.get(verdict, 0) + 1
        table.append([
            name, group, sync, fmt_value(base_median, unit, exact),
            fmt_value(head_median, unit, exact),
            fmt_ratio(median_ratio), "-" if exact else fmt_ratio(min_ratio),
            f"{fmt_pct(spread(base))} / {fmt_pct(spread(head))}", verdict,
        ])

    def cell(key, default="unknown"):
        return meta.get(key) or default

    # The sync modes the runs ran, from their rows: without --sync (an empty `sync` in meta.tsv)
    # the bench runs its tier's.
    modes = ", ".join(dict.fromkeys(key[1] for key in keys)) or "none"
    if "sync" in meta and not meta["sync"]:
        modes += " (the tier's)"

    out = ["# tn-lmdb bench: base vs head", ""]
    header = [
        ("base", f"{cell('base')} {meta.get('base_subject', '')}".strip()),
        ("head", f"{cell('head')} {meta.get('head_subject', '')}".strip()
                 + (f"; {meta['head_changes']}" if meta.get("head_changes") else "")),
        ("host", f"{cell('host')} ({first.get('os')}/{first.get('arch')})"),
        ("rustc", cell("rustc")),
        ("date", cell("date")),
        ("tier", f"{first.get('tier')}, n {first.get('n')}, reps {first.get('reps')}, "
                 f"page size {first.get('page_size')}"),
        ("filters", f"rows {cell('rows', 'all')}; sync {modes}"),
        ("rounds", f"{cell('rounds', '?')} ({cell('order', '?')}): {len(base_docs)} base and "
                   f"{len(head_docs)} head runs, each row's samples pooled per side"),
        ("thresholds", f"time rows: head/base median > {1 + threshold:g} and min > "
                       f"{1 + threshold / 2:g}; exact rows: head > base x {1 + size_threshold:g}; "
                       + ("durable rows gated" if gate_durable
                          else "durable time rows reported, not gated")
                       + "; rows the bench marks `gate: false` reported, not gated"),
    ]
    if meta.get("busy"):
        header.append(("WARNING", f"other builds were running when the runs started: {meta['busy']}"))
    if not meta:
        header.append(("note", "no meta.tsv in the bench dir: commits and host unknown"))
    out += ["| | |", "|---|---|"] + [f"| {k} | {v} |" for k, v in header] + [""]
    columns = ["row", "group", "sync", "base median", "head median", "ratio", "min ratio",
               "spread base / head", "verdict"]
    out.append("| " + " | ".join(columns) + " |")
    out.append("|" + "|".join("---" for _ in columns) + "|")
    out += ["| " + " | ".join(row) + " |" for row in table]
    out.append("")
    out.append("ratio: head median / base median; min ratio: head min / base min; spread: "
               "(max - min) / median of a side's pooled samples. Lower is better for every row.")
    out.append("")
    summary = ", ".join(f"{counts[k]} {k}" for k in
                        ("ok", "improved", "REGRESSION", "not gated", "new", "removed") if k in counts)
    out.append(f"rows: {len(table)} ({summary})")
    if ungated_over:
        out.append("past the threshold but not gated (durable, or `gate: false`): "
                   f"{', '.join(ungated_over)}")
    if gated_regressions:
        out.append(f"result: REGRESSION in {len(gated_regressions)} gated row"
                   f"{'s' if len(gated_regressions) != 1 else ''}: {', '.join(gated_regressions)}")
    else:
        out.append("result: no gated regression")
    text = "\n".join(out) + "\n"
    with open(os.path.join(bench_dir, "report.md"), "w") as f:
        f.write(text)
    sys.stdout.write(text)
    return 1 if gated_regressions else 0


def main():
    try:
        if len(sys.argv) >= 2 and sys.argv[1] == "manifest" and len(sys.argv) == 5:
            manifest(*sys.argv[2:5])
            return 0
        if len(sys.argv) >= 3 and sys.argv[1] == "report":
            return report(sys.argv[2], sys.argv[3:])
    except Exception as err:
        # A failure not anticipated exits 2, never 1 (a gated regression). `fail`'s exit is a
        # SystemExit, not an Exception, so it passes through as it is.
        fail(f"unexpected failure: {type(err).__name__}: {err}")
    fail("usage: manifest <bench Cargo.toml> <root Cargo.toml> <out> | report <bench dir> [...]")


if __name__ == "__main__":
    sys.exit(main())
