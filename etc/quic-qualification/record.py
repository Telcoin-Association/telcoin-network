#!/usr/bin/env python3
"""Evidence record and capacity figure for the inbound QUIC capacity qualification (issue #1741).

Purpose: assemble one evidence record per run from the run manifest, the
collector output (`collect.py`), the validator-side measurement (`measure.py`)
and the generator summaries (`generate.py`), evaluate the predeclared
thresholds, and pick the capacity figure per node configuration.

Requirements: Python 3.10 or newer. No root, no network. Any host.

This tool does NOT measure anything and never fabricates a value: a missing
input or figure is `null` plus an `open_limitations` entry. The capacity
figure is for the related security advisory, not for a public issue.
"""

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 1
CONFIGURATIONS = ("plain", "receive_buffer", "accept_task")
THRESHOLD_NAMES = (
    "maximum_honest_p99_latency_ms",
    "maximum_honest_connection_failure_fraction",
    "maximum_socket_drops",
    "minimum_commit_rate_per_second",
    "maximum_process_cpu_cores",
)
ADVISORY_NOTE = ("The capacity figure sizes uplinks and belongs in the related security advisory; "
                 "do not post it on a public issue.")


def utc_now():
    """ISO-8601 UTC timestamp with second resolution."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_file(path):
    """Hex digest of a file."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        digest.update(handle.read())
    return digest.hexdigest()


def dig(value, *keys):
    """Nested lookup that yields null instead of raising for any missing step."""
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def load_input(path, label, limitations, artifacts):
    """Parse one JSON input; a missing or malformed one is a limitation, not an error."""
    if path is None:
        limitations.append(f"{label}: not supplied")
        return None
    try:
        document = json.loads(Path(path).read_text())
        artifacts.append({"label": label, "path": str(path), "sha256": sha256_file(path)})
        return document
    except (OSError, ValueError) as error:
        limitations.append(f"{label}: {path} unusable: {type(error).__name__}: {error}")
        return None


def ss_rows(stdout):
    """`ss` output rows with their indented continuation lines joined."""
    rows = []
    for line in stdout.splitlines():
        if line[:1].isspace() and rows:
            rows[-1] += " " + line.strip()
        elif line.strip():
            rows.append(line.strip())
    return rows


def parse_receive_buffers(stdout, pid=None):
    """`rb` (receive buffer bytes) of every `skmem` row, restricted to `pid` when given."""
    buffers = []
    for row in ss_rows(stdout):
        found = re.search(r"skmem:\([^)]*\brb(\d+)", row)
        if found is None:
            continue
        if pid is not None and f"pid={pid}," not in row and f"pid={pid})" not in row:
            continue
        buffers.append({"row": row.split("skmem:")[0].strip(), "rb": int(found.group(1))})
    return buffers


def ss_capture(host_evidence):
    """The collector's `ss` command record, when it ran."""
    for record in (dig(host_evidence, "commands") or []):
        argv = record.get("argv") or []
        if argv[:1] == ["ss"] and record.get("status") == "ok":
            return record.get("stdout", "")
    return None


def sysctl_int(host_evidence, name):
    """A `/proc/sys/net/core/*` value captured by the collector, as int."""
    observed = dig(host_evidence, "proc", f"sys/net/core/{name}")
    if dig(observed, "status") != "ok":
        return None
    try:
        return int(str(observed["value"]).strip())
    except ValueError:
        return None


def socket_drop_total(summary):
    """Total per-socket drop delta over both families; null unless every family is usable."""
    deltas = dig(summary, "socket_drop_deltas")
    if not isinstance(deltas, dict) or not deltas:
        return None, "collector summary has no socket_drop_deltas"
    total = 0
    for family, family_deltas in deltas.items():
        if dig(family_deltas, "status") != "ok":
            return None, f"socket_drop_deltas.{family} is {dig(family_deltas, 'status') or 'missing'}"
        for inode, entry in (family_deltas.get("value") or {}).items():
            if dig(entry, "status") != "ok":
                return None, f"socket_drop_deltas.{family}.{inode} is {dig(entry, 'status') or 'missing'}"
            total += int(dig(entry, "value", "drops") or 0)
    return total, None


def threshold_entry(raw):
    """Normalise a threshold to `{value, owner, date}`; a bare number has no owner or date."""
    if isinstance(raw, dict):
        return {"value": raw.get("value"), "owner": raw.get("owner"), "date": raw.get("date")}
    return {"value": raw, "owner": None, "date": None}


def evaluate_threshold(name, entry, observed):
    """pass, fail or unknown for one threshold against one observation."""
    result = {"name": name, **entry, "observed": observed, "result": "unknown", "reason": None}
    if name not in THRESHOLD_NAMES:
        result["reason"] = "no evaluator for this threshold name"
    elif entry["value"] is None:
        result["reason"] = "threshold value not declared"
    elif observed is None:
        result["reason"] = "observation unavailable"
    else:
        passed = observed >= entry["value"] if name.startswith("minimum_") else observed <= entry["value"]
        result["result"] = "pass" if passed else "fail"
    return result


def observations(measurement, collector_summary):
    """The five observed figures the thresholds compare against (null when absent)."""
    p99 = dig(measurement, "summary", "established_latency", "outbound_request_response", "p99", "value")
    success = dig(measurement, "summary", "honest_dials", "success_fraction")
    drops, _reason = socket_drop_total(collector_summary)
    return {
        "maximum_honest_p99_latency_ms": p99 * 1000 if p99 is not None else None,
        "maximum_honest_connection_failure_fraction": 1 - success if success is not None else None,
        "maximum_socket_drops": drops,
        "minimum_commit_rate_per_second": dig(measurement, "summary", "consensus", "subdags_committed",
                                              "rate_per_second"),
        "maximum_process_cpu_cores": dig(measurement, "summary", "process", "cpu_cores", "p99"),
    }


def evaluate_thresholds(declared, observed):
    """Every declared threshold evaluated; the five known names are always listed."""
    declared = declared if isinstance(declared, dict) else {}
    names = list(THRESHOLD_NAMES) + sorted(name for name in declared if name not in THRESHOLD_NAMES)
    return [evaluate_threshold(name, threshold_entry(declared.get(name)), observed.get(name)) for name in names]


def build_record(manifest, metadata, collector_summary, measurement, generators, configuration, limitations, artifacts):
    """The five evidence groups plus artifacts, from whatever inputs exist."""
    host_before = dig(metadata, "host_before")
    pid = dig(metadata, "identity", "pid")
    capture = ss_capture(host_before)
    buffers = parse_receive_buffers(capture, pid) if capture is not None else []
    if capture is None:
        limitations.append("effective receive buffer: the collector has no successful ss capture")
    elif not buffers:
        limitations.append("effective receive buffer: no skmem row for the node process in the ss capture")
    effective = max(entry["rb"] for entry in buffers) if buffers else None
    drops, drop_reason = socket_drop_total(collector_summary)
    if drop_reason is not None:
        limitations.append(f"socket drops: {drop_reason}")
    if measurement is None:
        limitations.append("measurement: no validator-side figures (received rate, latency, consensus, process)")
    if not generators:
        limitations.append("generators: no offered-traffic summaries")
    received = dig(measurement, "summary", "received")
    return {
        "build": {
            "node_revision": dig(manifest, "build", "node_commit"),
            "lockfile_sha256": dig(manifest, "build", "lockfile_sha256"),
            "carried_patch_revision": dig(manifest, "build", "carried_patch_revision"),
            "features": dig(manifest, "build", "features"),
            "rust_toolchain": dig(manifest, "build", "rust_toolchain"),
            "binary_sha256": dig(host_before, "executable_sha256", "value"),
            "configuration": {
                "name": configuration,
                "receive_buffer_bytes_requested": dig(manifest, "build", "configuration",
                                                      "receive_buffer_bytes_requested"),
                "receive_buffer_bytes_effective": effective,
                "receive_buffer_rows": buffers,
                "host_sysctls": {name: sysctl_int(host_before, name) for name in ("rmem_default", "rmem_max")},
                "inbound_accept_task": dig(manifest, "build", "configuration", "inbound_accept_task"),
            },
        },
        "topology": {
            "roles": dig(manifest, "deployment", "roles"),
            "swarm_count": len(generators),
            "worker_count": dig(manifest, "deployment", "worker_count"),
            "honest_peers": dig(manifest, "deployment", "honest_peers"),
            "honest_workload": dig(manifest, "workload", "honest_workload"),
            "shared_addresses": dig(manifest, "deployment", "shared_addresses"),
            "admission_policy_state": dig(manifest, "deployment", "admission_policy_state"),
        },
        "host_and_traffic": {
            "host": {
                "uname": dig(host_before, "uname"),
                "interface": dig(metadata, "interface"),
                "cpu_allocation": dig(manifest, "deployment", "node_cpu_affinity"),
            },
            "nic": {"link_speed_bps": dig(manifest, "deployment", "link_speed_bps"),
                    "mtu": dig(manifest, "deployment", "mtu")},
            "network": dig(manifest, "deployment", "topology_artifact"),
            "firewall_state": dig(manifest, "deployment", "firewall_disabled_evidence"),
            "offered": [{"path": gen["path"], "swarm": dig(gen["summary"], "swarm"),
                         "requested_rate_pps": dig(gen["summary"], "requested_rate_pps"),
                         "offered_rate_pps": dig(gen["summary"], "offered_rate_pps"),
                         "offered_rate_bps": dig(gen["summary"], "offered_rate_bps"),
                         "payload_bytes": dig(gen["summary"], "payload_bytes"),
                         "source_population": dig(gen["summary"], "source_population")} for gen in generators],
            "received": {
                "total_rate_pps": dig(received, "run_rate_pps"),
                "distribution": dig(received, "total"),
                "by_stage": dig(received, "by_stage"),
                "socket_drops": drops,
            },
            "generator_limits": [{"path": gen["path"], "send_errors": dig(gen["summary"], "send_errors"),
                                  "achieved_fraction": dig(gen["summary"], "achieved_fraction")}
                                 for gen in generators],
        },
        "method": {
            "metric_definitions": dig(measurement, "definitions"),
            "bucket_bounds": dig(measurement, "bucket_bounds"),
            "sampling": {"collector_interval_seconds": dig(metadata, "requested_interval_seconds"),
                         "measurement_interval_seconds": dig(measurement, "interval_seconds"),
                         "measurement_scrapes": dig(measurement, "scrape_count"),
                         "measurement_gaps": dig(measurement, "gaps"),
                         "manifest": dig(manifest, "method", "sampling")},
            "trial_count": dig(manifest, "method", "trial_count"),
            "baseline_run_id": dig(manifest, "method", "baseline_run_id"),
            "instrumentation_overhead": dig(manifest, "method", "instrumentation_overhead"),
            "result_distributions": {
                "consensus": dig(measurement, "summary", "consensus"),
                "process": dig(measurement, "summary", "process"),
                "honest_dials": dig(measurement, "summary", "honest_dials"),
                "handshake_time_seconds": dig(measurement, "summary", "handshake_time_seconds"),
                "established_latency": dig(measurement, "summary", "established_latency"),
                "unavailable_metrics": dig(measurement, "unavailable"),
            },
        },
        "thresholds": evaluate_thresholds(dig(manifest, "predeclared_acceptance"),
                                          observations(measurement, collector_summary)),
    }


def capacity(records):
    """Highest received total rate among fully passing records of one configuration."""
    names = sorted({dig(record, "build", "configuration", "name") for record in records})
    rows = []
    for record in records:
        results = [entry.get("result") for entry in (record.get("thresholds") or [])]
        rows.append({
            "run_id": record.get("run_id"),
            "received_total_rate_pps": dig(record, "host_and_traffic", "received", "total_rate_pps"),
            "result": ("fail" if "fail" in results
                       else "unknown" if not results or "unknown" in results else "pass"),
        })
    figure = {"schema_version": SCHEMA_VERSION, "configuration": names[0] if len(names) == 1 else None,
              "capacity_received_rate_pps": None, "run_id": None, "records": rows, "reason": None,
              "notes": [ADVISORY_NOTE]}
    if len(names) != 1:
        figure["reason"] = f"records span {len(names)} configurations; pass one configuration at a time"
        return figure
    if any(row["result"] == "unknown" for row in rows):
        figure["reason"] = "at least one record has an unknown threshold result"
        return figure
    passing = [row for row in rows if row["result"] == "pass" and row["received_total_rate_pps"] is not None]
    if not passing:
        figure["reason"] = "no record passes every threshold with a received rate"
        return figure
    best = max(passing, key=lambda row: row["received_total_rate_pps"])
    figure["capacity_received_rate_pps"] = best["received_total_rate_pps"]
    figure["run_id"] = best["run_id"]
    return figure


def build_parser():
    """Command line: `build` one record, `capacity` across records."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    subcommands = parser.add_subparsers(dest="command", required=True)
    build = subcommands.add_parser("build", help="assemble one evidence record")
    build.add_argument("--manifest", required=True, type=Path, help="run manifest (run.example.json shape)")
    build.add_argument("--collector", type=Path, help="collect.py output directory")
    build.add_argument("--measurement", type=Path, help="measure.py output")
    build.add_argument("--generator", type=Path, action="append", default=[], help="generate.py summary (repeatable)")
    build.add_argument("--configuration", required=True, choices=CONFIGURATIONS, help="node configuration under test")
    build.add_argument("--output", required=True, type=Path, help="evidence record path")
    cap = subcommands.add_parser("capacity", help="capacity figure for one configuration")
    cap.add_argument("--records", required=True, type=Path, nargs="+", help="evidence records of one configuration")
    cap.add_argument("--output", required=True, type=Path, help="capacity JSON path")
    return parser


def write_json(path, value):
    """Write a JSON document."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def main(argv=None):
    """Exit 0 when the output was written, 2 on a usage failure, 3 when inputs were missing."""
    args = build_parser().parse_args(argv)
    limitations, artifacts = [], []
    if args.command == "build":
        manifest = load_input(args.manifest, "manifest", limitations, artifacts)
        if manifest is None:
            print(f"record.py: {limitations[-1]}", file=sys.stderr)
            return 2
        metadata = load_input(args.collector / "metadata.json" if args.collector else None,
                              "collector metadata", limitations, artifacts)
        collector_summary = load_input(args.collector / "summary.json" if args.collector else None,
                                       "collector summary", limitations, artifacts)
        measurement = load_input(args.measurement, "measurement", limitations, artifacts)
        generators = [{"path": str(path), "summary": summary} for path in args.generator
                      for summary in [load_input(path, f"generator {path}", limitations, artifacts)]
                      if summary is not None]
        record = build_record(manifest, metadata, collector_summary, measurement, generators,
                              args.configuration, limitations, artifacts)
        document = {"schema_version": SCHEMA_VERSION, "run_id": dig(manifest, "run_id"),
                    "generated_utc": utc_now(), **record,
                    "open_limitations": (dig(manifest, "open_limitations") or []) + limitations,
                    "artifacts": artifacts}
    else:
        records = [load_input(path, f"record {path}", limitations, artifacts) for path in args.records]
        usable = [record for record in records if record is not None]
        document = {**capacity(usable), "generated_utc": utc_now(), "open_limitations": limitations,
                    "artifacts": artifacts}
    try:
        write_json(args.output, document)
    except OSError as error:
        print(f"record.py: cannot write {args.output}: {error}", file=sys.stderr)
        return 3
    return 3 if limitations else 0


if __name__ == "__main__":
    sys.exit(main())
