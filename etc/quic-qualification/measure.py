#!/usr/bin/env python3
"""Validator-side metric scrape for the inbound QUIC capacity qualification (issue #1741).

Purpose: sample the node's Prometheus endpoint at a fixed interval and derive
the received rate, consensus progress, process cost, honest dial outcomes and
latency distributions, with every formula named in the output `definitions`.
Generator summaries are copied under `offered`, so one file carries the
offered and the received side without mixing them.

Requirements: Python 3.10 or newer and HTTP access to the node's `--metrics`
endpoint. No root. `--from-files DIR` replays saved `scrape-*.txt` captures
instead of HTTP (tests and offline re-derivation).

This tool does NOT generate traffic, does NOT read host counters (that is
`collect.py`), and never records a failed scrape as zeros: a failed scrape is
a gap. Quantiles are bucket-interpolated; the bucket bounds used are reported.
"""

import argparse
import json
import math
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 1
QUANTILES = (("p50", 0.5), ("p99", 0.99), ("p99_9", 0.999))

RECEIVED = "tn_network_quic_incoming_outcomes_total"
ROUND = "tn_primary_round"
COMMITTED_ROUND = "tn_primary_committed_round"
SUBDAGS = "tn_primary_subdags_committed_total"
COMMIT_LATENCY = "tn_primary_commit_latency_seconds"
CPU = "reth_process_cpu_seconds_total"
RSS = "reth_process_resident_memory_bytes"
ESTABLISHED = "tn_network_connections_established_total"
DIAL_FAILURES = "tn_network_dial_failures_total"
HANDSHAKE = "tn_network_connection_established_seconds"
OUTBOUND_REQUEST = "tn_network_outbound_request_duration_seconds"
INBOUND_SERVICE = "tn_network_inbound_request_service_seconds"
COUNTERS_AND_GAUGES = (RECEIVED, ROUND, COMMITTED_ROUND, SUBDAGS, CPU, RSS, ESTABLISHED, DIAL_FAILURES)
HISTOGRAMS = (COMMIT_LATENCY, HANDSHAKE, OUTBOUND_REQUEST, INBOUND_SERVICE)


def utc_now():
    """ISO-8601 UTC timestamp with second resolution."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_labels(body):
    """`a="x",b="y"` to a sorted tuple of (name, value); handles escaped quotes."""
    labels = []
    position = 0
    while position < len(body):
        if body[position] in ", ":
            position += 1
            continue
        equals = body.index("=", position)
        name = body[position:equals].strip()
        if body[equals + 1] != '"':
            raise ValueError(f"label {name!r} is not quoted")
        position = equals + 2
        value = []
        while body[position] != '"':
            if body[position] == "\\":
                value.append({"n": "\n", "\\": "\\", '"': '"'}.get(body[position + 1], body[position + 1]))
                position += 2
            else:
                value.append(body[position])
                position += 1
        position += 1
        labels.append((name, "".join(value)))
    return tuple(sorted(labels))


def parse_sample(line):
    """One exposition sample line to (name, labels, value)."""
    brace = line.find("{")
    if brace >= 0:
        close = line.rfind("}")
        if close < brace:
            raise ValueError(f"unterminated labels: {line!r}")
        name = line[:brace].strip()
        labels = parse_labels(line[brace + 1:close])
        rest = line[close + 1:].split()
    else:
        fields = line.split()
        name, labels, rest = fields[0], (), fields[1:]
    if not rest:
        raise ValueError(f"sample without a value: {line!r}")
    return name, labels, float(rest[0])


def parse_exposition(text):
    """Prometheus text format to `({name: {labels: value}}, {name: type})`."""
    samples = {}
    types = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            parts = line.split(None, 3)
            if len(parts) >= 4 and parts[1] == "TYPE":
                types[parts[2]] = parts[3].strip()
            continue
        try:
            name, labels, value = parse_sample(line)
        except IndexError:
            raise ValueError(f"malformed sample: {line!r}") from None
        samples.setdefault(name, {})[labels] = value
    return samples, types


def matches(labels, where):
    """True when every `where` label is present with that value."""
    present = dict(labels)
    return all(present.get(name) == value for name, value in where.items())


def group_reduce(table, keys, combine, where=None):
    """Reduce a metric's label sets down to `keys`, in that order, keeping only rows matching `where`."""
    grouped = {}
    for labels, value in table.items():
        if where and not matches(labels, where):
            continue
        present = dict(labels)
        key = tuple((name, present[name]) for name in keys if name in present)
        grouped[key] = combine(grouped[key], value) if key in grouped else value
    return grouped


def key_text(key):
    """Stable label text for a group key; the empty key is `all`."""
    return ",".join(f"{name}={value}" for name, value in key) or "all"


def add(left, right):
    """Sum combinator for counters."""
    return left + right


def interval_rates(before, after, name, keys, where=None):
    """Per-interval rate per group; a reset, a missing side or a zero interval is null."""
    seconds = after["monotonic"] - before["monotonic"]
    start = group_reduce(before["samples"].get(name, {}), keys, add, where)
    end = group_reduce(after["samples"].get(name, {}), keys, add, where)
    rates = {}
    for key in sorted(set(start) | set(end)):
        usable = key in start and key in end and end[key] >= start[key] and seconds > 0
        rates[key_text(key)] = (end[key] - start[key]) / seconds if usable else None
    return rates


def run_delta(first, last, name, keys, where=None):
    """Whole-run delta per group; null on a reset or a missing side."""
    start = group_reduce(first["samples"].get(name, {}), keys, add, where)
    end = group_reduce(last["samples"].get(name, {}), keys, add, where)
    deltas = {}
    for key in sorted(set(start) | set(end)):
        usable = key in start and key in end and end[key] >= start[key]
        deltas[key_text(key)] = end[key] - start[key] if usable else None
    return deltas


def gauge_value(scrape, name, keys=(), where=None):
    """A gauge reduced to `keys` by max, null when absent."""
    grouped = group_reduce(scrape["samples"].get(name, {}), keys, max, where)
    return {key_text(key): value for key, value in grouped.items()} or None


def distribution(values):
    """min, median, p99 (nearest rank), max and count over the non-null values."""
    present = sorted(value for value in values if value is not None)
    if not present:
        return None
    rank = lambda q: present[min(len(present) - 1, max(0, math.ceil(q * len(present)) - 1))]  # noqa: E731
    return {"min": present[0], "median": rank(0.5), "p99": rank(0.99), "max": present[-1], "count": len(present)}


def histogram_delta(first, last, name, keys, where=None):
    """Run-delta cumulative buckets per group: `{label: [(upper_bound, count)]}`; null on reset."""
    bucket_name = f"{name}_bucket"
    start = group_reduce(first["samples"].get(bucket_name, {}), tuple(keys) + ("le",), add, where)
    end = group_reduce(last["samples"].get(bucket_name, {}), tuple(keys) + ("le",), add, where)
    groups = {}
    for key in set(start) | set(end):
        bounds = dict(key)
        upper = float(bounds.pop("le"))
        group = tuple(sorted(bounds.items()))
        usable = key in start and key in end and end[key] >= start[key]
        count = end[key] - start[key] if usable else None
        groups.setdefault(key_text(group), []).append((upper, count))
    result = {}
    for label, buckets in groups.items():
        ordered = sorted(buckets)
        result[label] = None if any(count is None for _, count in ordered) else ordered
    return result


def histogram_quantile(q, buckets):
    """PromQL-style linear interpolation inside the bucket; `+Inf` reports the last finite bound."""
    if not buckets or buckets[-1][1] <= 0:
        return None
    rank = q * buckets[-1][1]
    lower, previous = 0.0, 0.0
    for upper, count in buckets:
        if count >= rank:
            if math.isinf(upper):
                return {"value": lower, "lower_bound": lower, "upper_bound": None, "in_overflow_bucket": True}
            width = count - previous
            value = lower + (upper - lower) * ((rank - previous) / width) if width > 0 else upper
            return {"value": value, "lower_bound": lower, "upper_bound": upper, "in_overflow_bucket": False}
        lower, previous = upper, count
    return None


def quantiles(buckets):
    """The three reported quantiles of one run-delta histogram, or null."""
    if buckets is None:
        return None
    return {name: histogram_quantile(q, buckets) for name, q in QUANTILES}


def bucket_bounds(scrapes, name):
    """The `le` bounds the endpoint exposes for a histogram, sorted."""
    bounds = set()
    for scrape in scrapes:
        for labels in scrape["samples"].get(f"{name}_bucket", {}):
            bounds.update(float(value) for key, value in labels if key == "le")
    return sorted(bounds)


def success_fraction(established, failures):
    """established / (established + failures), null when either is null or the denominator is 0."""
    if established is None or failures is None or established + failures == 0:
        return None
    return established / (established + failures)


def seen_metrics(scrapes):
    """Metric base names present in any scrape (histogram suffixes stripped)."""
    names = set()
    for scrape in scrapes:
        for name in scrape["samples"]:
            names.add(name)
            for suffix in ("_bucket", "_sum", "_count"):
                if name.endswith(suffix):
                    names.add(name[:-len(suffix)])
    return names


def derive(scrapes):
    """All derivations from an ordered list of scrapes (at least two)."""
    first, last = scrapes[0], scrapes[-1]
    elapsed = last["monotonic"] - first["monotonic"]
    rows = []
    for before, after in zip(scrapes, scrapes[1:]):
        rows.append({
            "timestamp_utc": after["timestamp_utc"],
            "interval_seconds": after["monotonic"] - before["monotonic"],
            "received_by_outcome_pps": interval_rates(before, after, RECEIVED, ("network", "stage", "decision")),
            "received_by_stage_pps": interval_rates(before, after, RECEIVED, ("network", "stage")),
            "received_total_pps": interval_rates(before, after, RECEIVED, ()).get("all"),
            "cpu_cores": interval_rates(before, after, CPU, ()).get("all"),
            "resident_memory_bytes": (gauge_value(after, RSS) or {}).get("all"),
            "round": (gauge_value(after, ROUND) or {}).get("all"),
            "committed_round": (gauge_value(after, COMMITTED_ROUND) or {}).get("all"),
        })
    per_label = lambda field: {  # noqa: E731
        label: distribution([row[field].get(label) for row in rows])
        for label in sorted({label for row in rows for label in row[field]})
    }
    series = lambda field: [row[field] for row in rows]  # noqa: E731
    received_delta = run_delta(first, last, RECEIVED, ()).get("all")
    subdags = run_delta(first, last, SUBDAGS, ()).get("all")
    established = run_delta(first, last, ESTABLISHED, (), {"direction": "out"}).get("all")
    failures = run_delta(first, last, DIAL_FAILURES, ()).get("all")
    rss = [value for value in series("resident_memory_bytes") if value is not None]
    gauge_span = lambda name: {  # noqa: E731
        "first": (gauge_value(first, name) or {}).get("all"),
        "last": (gauge_value(last, name) or {}).get("all"),
    }
    rounds = gauge_span(ROUND)
    committed = gauge_span(COMMITTED_ROUND)
    advance = lambda span: (span["last"] - span["first"]  # noqa: E731
                            if span["first"] is not None and span["last"] is not None else None)
    summary = {
        "elapsed_seconds": elapsed,
        "received": {
            "run_rate_pps": received_delta / elapsed if received_delta is not None and elapsed > 0 else None,
            "total": distribution(series("received_total_pps")),
            "by_stage": per_label("received_by_stage_pps"),
            "by_outcome": per_label("received_by_outcome_pps"),
        },
        "consensus": {
            "round": {**rounds, "advance": advance(rounds)},
            "committed_round": {**committed, "advance": advance(committed)},
            "subdags_committed": {"delta": subdags,
                                  "rate_per_second": subdags / elapsed if subdags is not None and elapsed > 0 else None},
            "commit_latency_seconds": quantiles(histogram_delta(first, last, COMMIT_LATENCY, ()).get("all")),
        },
        "process": {
            "cpu_cores": distribution(series("cpu_cores")),
            "resident_memory_bytes": ({"min": min(rss), "median": distribution(rss)["median"], "max": max(rss),
                                       "last": rss[-1]} if rss else None),
        },
        "honest_dials": {
            "established_outbound": established,
            "dial_failures": failures,
            "success_fraction": success_fraction(established, failures),
        },
        "handshake_time_seconds": {label: quantiles(buckets) for label, buckets in
                                   histogram_delta(first, last, HANDSHAKE, ("direction",)).items()},
        "established_latency": {
            "outbound_request_response": quantiles(
                histogram_delta(first, last, OUTBOUND_REQUEST, (), {"outcome": "response"}).get("all")),
            "inbound_request_service": quantiles(histogram_delta(first, last, INBOUND_SERVICE, ()).get("all")),
        },
    }
    present = seen_metrics(scrapes)
    unavailable = sorted(name for name in COUNTERS_AND_GAUGES + HISTOGRAMS if name not in present)
    bounds = {name: bucket_bounds(scrapes, name) for name in HISTOGRAMS if name in present}
    return rows, summary, unavailable, bounds


def definitions():
    """Exact metric names and formulas behind every derived figure."""
    rate = "per-interval (after - before) / interval_seconds, null on a counter reset or a gap"
    delta = "whole-run last - first, null on a counter reset"
    quantile = ("PromQL-style linear interpolation inside the bucket of the run-delta histogram; "
                "`in_overflow_bucket` means the quantile fell into +Inf and the value is the last finite bound")
    return {
        "received_by_outcome_pps": {"metrics": [RECEIVED], "formula": f"{rate}, grouped by network, stage, decision"},
        "received_by_stage_pps": {"metrics": [RECEIVED], "formula": f"{rate}, summed over decision and reason"},
        "received_total_pps": {"metrics": [RECEIVED], "formula": f"{rate}, summed over every label"},
        "received.run_rate_pps": {"metrics": [RECEIVED], "formula": f"{delta} / elapsed_seconds"},
        "consensus.round": {"metrics": [ROUND], "formula": "gauge first and last sample, advance = last - first"},
        "consensus.committed_round": {"metrics": [COMMITTED_ROUND],
                                      "formula": "gauge first and last sample, advance = last - first"},
        "consensus.subdags_committed": {"metrics": [SUBDAGS], "formula": f"{delta}; rate = delta / elapsed_seconds"},
        "consensus.commit_latency_seconds": {"metrics": [COMMIT_LATENCY], "formula": quantile},
        "cpu_cores": {"metrics": [CPU], "formula": f"{rate} (CPU seconds per second = cores)"},
        "resident_memory_bytes": {"metrics": [RSS], "formula": "gauge per sample; min, median, max, last"},
        "honest_dials.established_outbound": {"metrics": [ESTABLISHED], "formula": f'{delta} where direction="out"'},
        "honest_dials.dial_failures": {"metrics": [DIAL_FAILURES], "formula": f"{delta}, summed over every label"},
        "honest_dials.success_fraction": {"metrics": [ESTABLISHED, DIAL_FAILURES],
                                          "formula": "established / (established + failures), null when the sum is 0"},
        "handshake_time_seconds": {"metrics": [HANDSHAKE], "formula": f"{quantile}, grouped by direction"},
        "established_latency.outbound_request_response": {"metrics": [OUTBOUND_REQUEST],
                                                          "formula": f'{quantile}, outcome="response" only'},
        "established_latency.inbound_request_service": {"metrics": [INBOUND_SERVICE],
                                                        "formula": f"{quantile}, summed over class (companion figure)"},
    }


def fetch_http(url, timeout):
    """One scrape over HTTP; errors propagate as OSError subclasses."""
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8", "replace")


def collect(fetch, count, interval, clock, sleep, now):
    """`count` scrapes spaced `interval` seconds apart; a failed scrape becomes a gap."""
    scrapes, gaps = [], []
    start = clock()
    for index in range(count):
        target = start + index * interval
        remaining = target - clock()
        if remaining > 0:
            sleep(remaining)
        taken = clock()
        try:
            samples, types = parse_exposition(fetch())
            scrapes.append({"timestamp_utc": now(), "monotonic": taken, "samples": samples, "types": types})
        except (OSError, ValueError) as error:
            gaps.append({"timestamp_utc": now(), "error": f"{type(error).__name__}: {error}"})
    return scrapes, gaps


def scrapes_from_files(directory, interval):
    """Replay `scrape-*.txt` in name order with synthetic timestamps `interval` apart."""
    scrapes, gaps = [], []
    for index, path in enumerate(sorted(Path(directory).glob("scrape-*.txt"))):
        try:
            samples, types = parse_exposition(path.read_text())
            scrapes.append({"timestamp_utc": f"file:{path.name}", "monotonic": index * interval,
                            "samples": samples, "types": types})
        except (OSError, ValueError) as error:
            gaps.append({"timestamp_utc": f"file:{path.name}", "error": f"{type(error).__name__}: {error}"})
    return scrapes, gaps


def load_generators(paths, notes):
    """Generator summaries copied verbatim; an unreadable one is noted, not invented."""
    offered = []
    for path in paths:
        try:
            offered.append({"path": str(path), "summary": json.loads(Path(path).read_text())})
        except (OSError, ValueError) as error:
            notes.append(f"generator summary {path} not copied: {error}")
    return offered


def build_parser():
    """Command line."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--metrics", help="node metrics endpoint, e.g. http://127.0.0.1:9101/metrics")
    source.add_argument("--from-files", type=Path, help="directory of saved scrape-*.txt captures (no HTTP)")
    parser.add_argument("--duration", type=float, default=60.0, help="seconds to sample (default 60)")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between scrapes (default 1)")
    parser.add_argument("--timeout", type=float, default=5.0, help="HTTP timeout per scrape (default 5)")
    parser.add_argument("--generator", type=Path, action="append", default=[],
                        help="generator summary to copy as offered traffic (repeatable)")
    parser.add_argument("--output", required=True, type=Path, help="measurement JSON path")
    return parser


def main(argv=None):
    """Exit 0 on a complete run, 2 on a usage failure, 3 when scrapes are missing."""
    args = build_parser().parse_args(argv)
    if args.interval <= 0 or args.duration <= 0 or args.timeout <= 0:
        print("measure.py: --duration, --interval and --timeout must be positive", file=sys.stderr)
        return 2
    notes = []
    start_utc = utc_now()
    if args.from_files is not None:
        if not args.from_files.is_dir():
            print(f"measure.py: {args.from_files} is not a directory", file=sys.stderr)
            return 2
        scrapes, gaps = scrapes_from_files(args.from_files, args.interval)
        notes.append("replayed from files; timestamps are synthetic and interval spaced")
    else:
        count = max(2, int(args.duration // args.interval) + 1)
        try:
            scrapes, gaps = collect(lambda: fetch_http(args.metrics, args.timeout), count, args.interval,
                                    time.monotonic, time.sleep, utc_now)
        except KeyboardInterrupt:
            scrapes, gaps = [], [{"timestamp_utc": utc_now(), "error": "KeyboardInterrupt"}]
            notes.append("interrupted before the sampling loop completed")
    complete = len(scrapes) >= 2
    rows, summary, unavailable, bounds = derive(scrapes) if complete else ([], None, [], {})
    if not complete:
        notes.append("fewer than two successful scrapes; nothing derived")
    document = {
        "schema_version": SCHEMA_VERSION,
        "metrics_url": args.metrics,
        "source_directory": str(args.from_files) if args.from_files is not None else None,
        "start_utc": start_utc,
        "end_utc": utc_now(),
        "interval_seconds": args.interval,
        "scrape_count": len(scrapes),
        "samples": rows,
        "summary": summary,
        "definitions": definitions(),
        "bucket_bounds": bounds,
        "unavailable": unavailable,
        "gaps": gaps,
        "offered": load_generators(args.generator, notes),
        "notes": notes,
    }
    try:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    except OSError as error:
        print(f"measure.py: cannot write {args.output}: {error}", file=sys.stderr)
        return 3
    return 0 if complete and not gaps else 3


if __name__ == "__main__":
    sys.exit(main())
