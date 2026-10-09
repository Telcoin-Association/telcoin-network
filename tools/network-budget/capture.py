#!/usr/bin/env python3
"""Capture bounded resource observations for an externally driven calibration workload."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import time
from urllib.request import Request, urlopen


NETWORK_METRICS = frozenset({
    "tn_network_established_connections",
    "tn_network_established_connection_limit",
    "tn_network_inbound_streams_per_connection_limit",
    "tn_network_receive_credit_per_connection_bytes",
    "tn_network_connection_limit_rejections_total",
    "tn_network_outbound_requests_pending",
    "tn_network_outbound_request_failures_total",
})
# The exporter adds the `reth` prefix to process metrics, see crates/tn-metrics/src/recorder.rs.
PROCESS_METRICS = frozenset({"reth_process_resident_memory_bytes", "reth_process_cpu_seconds_total"})
PENDING = "tn_network_inbound_requests_pending_by_class"
SHED = "tn_network_inbound_requests_shed_total"
SERVICE = "tn_network_inbound_request_service_seconds"
FAILED = "tn_network_inbound_requests_failed_total"
REJECTIONS = "tn_network_connection_limit_rejections_total"
DENIALS = "tn_network_inbound_connections_denied_total"
# The service histogram renders as a summary (quantile) or as buckets (le), with _sum and _count.
CLASS_METRICS = {
    PENDING: frozenset({"network", "class"}),
    SHED: frozenset({"network", "class", "reason"}),
    FAILED: frozenset({"network", "class", "outcome"}),
    SERVICE: frozenset({"network", "class", "quantile"}),
    f"{SERVICE}_bucket": frozenset({"network", "class", "le"}),
    f"{SERVICE}_sum": frozenset({"network", "class"}),
    f"{SERVICE}_count": frozenset({"network", "class"}),
}
SERVICE_CLASSES = frozenset({"vote", "epoch_record", "certificate_sync", "batch", "gossip", "other"})
# batch carries ReportBatch, the 2f+1 quorum-ack request, so it is critical on worker swarms.
CRITICAL_CLASSES = frozenset({"vote", "epoch_record", "batch"})
SHED_REASONS = frozenset({"queue_full", "unsubscribed", "admission"})
FAILURE_OUTCOMES = frozenset({"timeout", "omitted", "closed", "io", "unsupported"})
REJECTION_REASONS = frozenset({"pending_incoming", "pending_outgoing", "established_incoming", "established_outgoing",
                               "established_per_peer", "established_total", "unknown"})
DENIAL_REASONS = frozenset({"pending_incoming_limit", "established_per_peer_limit", "established_total_limit", "other_limit"})
SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)(?:\s+\S+)?$')
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"\\]*)"(?:,|$)')
FAILURE_KINDS = frozenset({"dial", "timeout", "connection", "unsupported", "io"})
MAX_RESPONSE_BYTES = 8 * 1024 * 1024


def bounded(text, upper):
    """Accept a finite float in [0, upper], or +Inf when upper is infinite."""
    try:
        value = float(text)
    except ValueError:
        return False
    return (text == "+Inf" and math.isinf(upper)) or (math.isfinite(value) and 0 <= value <= upper)


LABEL_SETS = {
    **{name: frozenset({"network"}) for name in NETWORK_METRICS},
    "tn_network_inbound_requests_pending": frozenset({"network"}),
    "tn_network_outbound_request_failures_total": frozenset({"network", "kind"}),
    REJECTIONS: frozenset({"network", "reason"}),
    DENIALS: frozenset({"network", "reason"}),
    **CLASS_METRICS,
    **{name: frozenset() for name in PROCESS_METRICS},
}
LABEL_VALUES = {
    "kind": FAILURE_KINDS.__contains__,
    "class": SERVICE_CLASSES.__contains__,
    "outcome": FAILURE_OUTCOMES.__contains__,
    "quantile": lambda text: bounded(text, 1),
    "le": lambda text: bounded(text, math.inf),
}
# The reason label is a different closed set on each metric that carries it.
REASON_VALUES = {SHED: SHED_REASONS, REJECTIONS: REJECTION_REASONS, DENIALS: DENIAL_REASONS}
REQUIRED_METRICS = NETWORK_METRICS | PROCESS_METRICS | {PENDING, SHED, FAILED, DENIALS, SERVICE, f"{SERVICE}_sum", f"{SERVICE}_count"}


def label_allowed(name, key, text):
    """Check one label value against its closed set. The reason set depends on the metric."""
    if key == "reason":
        return text in REASON_VALUES.get(name, frozenset())
    return key not in LABEL_VALUES or LABEL_VALUES[key](text)


def expected_down(entry, windows):
    """True when a scrape of entry["node"] overlaps an expected-down window recorded for that node."""
    started = entry["started_unix_seconds"]
    finished = entry.get("finished_unix_seconds", started)
    return any(window["node"] == entry["node"] and started <= window["end_unix_seconds"]
               and finished >= window["start_unix_seconds"] for window in windows)


def missing_metrics(selected):
    """List absent metric families; service quantiles and service buckets satisfy the same family."""
    present = {item["metric"] for item in selected}
    distribution = {SERVICE} if present & {SERVICE, f"{SERVICE}_bucket"} else set()
    return sorted(REQUIRED_METRICS - present - distribution)


def observations(text, workers):
    """Select only fixed metric names and topology-bounded labels; missing is never zero."""
    networks = {"primary", *(f"worker-{worker}" for worker in range(workers))}
    selected = []
    identities = set()
    for line in text.splitlines():
        match = SAMPLE.fullmatch(line)
        if match is None:
            continue
        name, raw_labels, raw_value = match.groups()
        if name not in LABEL_SETS:
            continue
        raw_labels = raw_labels or ""
        labels = list(LABEL.finditer(raw_labels))
        if "".join(label.group() for label in labels) != raw_labels:
            raise ValueError(f"unsupported labels on {name}")
        pairs = [(label.group(1), label.group(2)) for label in labels]
        values = dict(pairs)
        if len(values) != len(pairs):
            raise ValueError(f"duplicate labels on {name}")
        if set(values) != LABEL_SETS[name] or ("network" in values and values["network"] not in networks):
            raise ValueError(f"unexpected label set or swarm on {name}")
        rejected = sorted(key for key, text in values.items() if not label_allowed(name, key, text))
        if rejected:
            raise ValueError(f"unexpected {rejected[0]} label value on {name}")
        value = float(raw_value)
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"invalid measurement for {name}")
        identity = (name, tuple(sorted(values.items())))
        if identity in identities:
            raise ValueError(f"duplicate sample for {name}")
        identities.add(identity)
        selected.append({"metric": name, "labels": values, "value": value})
    if not selected:
        raise ValueError("exporter contains no supported resource observations")
    return selected


def file_record(path):
    """Pin the exact build/configuration/decision artifact without copying its contents."""
    path = path.resolve(strict=True)
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": str(path), "sha256": digest.hexdigest()}


def block_number(url):
    """Poll eth_blockNumber once; a failure is recorded as missing, never as block zero."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}).encode()
    request = Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=10) as response:
            result = json.loads(response.read(MAX_RESPONSE_BYTES + 1).decode("utf-8")).get("result")
        if not isinstance(result, str) or not re.fullmatch(r"0x[0-9a-fA-F]{1,16}", result):
            return {"missing": "eth_blockNumber returned no quantity"}
        return {"number": int(result, 16)}
    except (OSError, ValueError, AttributeError) as error:
        return {"missing": str(error)}


def positive(value):
    """Accept finite positive polling intervals."""
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return parsed


def capture(args):
    """Record provenance and each scrape independently, including failed observations."""
    manifest = json.loads(args.manifest.read_text())
    required = {"revision", "build_command", "topology", "nodes", "artifacts", "workload", "decisions"}
    if set(manifest) != required or not re.fullmatch(r"[0-9a-f]{40}", manifest["revision"]):
        raise ValueError("manifest requires revision, build_command, topology, nodes, artifacts, workload, decisions")
    topology = manifest["topology"]
    for field in ("validators", "workers_per_node", "cpus_per_node", "ram_bytes_per_node"):
        if type(topology.get(field)) is not int or topology[field] <= 0:
            raise ValueError(f"topology.{field} must be a positive integer")
    if topology["validators"] > 256 or topology["workers_per_node"] > 256:
        raise ValueError("capture supports at most 256 validators and 256 workers per node")
    nodes = manifest["nodes"]
    if len(nodes) != topology["validators"] or len({node["name"] for node in nodes}) != len(nodes):
        raise ValueError("provide one uniquely named metrics endpoint for every validator")
    if not manifest["artifacts"] or not manifest["decisions"] or not manifest["workload"] or not manifest["build_command"]:
        raise ValueError("build, artifacts, workload and threshold decisions must be recorded")
    for node in nodes:
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", node["name"]) or not node["metrics_url"].startswith(("http://", "https://")):
            raise ValueError("invalid node name or metrics URL")
        if set(node) - {"name", "metrics_url", "rpc_url"} or not node.get("rpc_url", "http://").startswith(("http://", "https://")):
            raise ValueError("node accepts only name, metrics_url and an optional http(s) rpc_url")
    artifacts = [file_record(args.manifest.parent / path) for path in manifest["artifacts"]]
    args.output.mkdir(parents=True, exist_ok=False)
    record = {"manifest": manifest, "artifacts": artifacts, "phase": args.phase,
              "samples": args.samples, "interval_seconds": args.interval,
              "collector_sha256": file_record(Path(__file__))["sha256"],
              "acceptance": "pending: requires workload outcomes and maintainer threshold review"}
    (args.output / "manifest.json").write_text(json.dumps(record, indent=2) + "\n")
    failures = []
    with (args.output / "observations.jsonl").open("w") as output:
        for sample in range(args.samples):
            started = time.monotonic()
            for node in nodes:
                entry = {"sample": sample, "node": node["name"], "started_unix_seconds": time.time()}
                try:
                    with urlopen(node["metrics_url"], timeout=10) as response:
                        data = response.read(MAX_RESPONSE_BYTES + 1)
                    if len(data) > MAX_RESPONSE_BYTES:
                        raise ValueError("metrics response exceeds 8 MiB")
                    entry["observations"] = observations(data.decode("utf-8"), topology["workers_per_node"])
                    entry["missing_metrics"] = missing_metrics(entry["observations"])
                    entry["missing_networks"] = sorted({"primary", *(f"worker-{i}" for i in range(topology["workers_per_node"]))} - {item["labels"].get("network") for item in entry["observations"]})
                except (OSError, ValueError) as error:
                    entry["error"] = str(error)
                entry["block"] = block_number(node["rpc_url"]) if "rpc_url" in node else {"missing": "no rpc_url"}
                entry["finished_unix_seconds"] = time.time()
                if "error" in entry:
                    failures.append({key: entry[key] for key in ("sample", "node", "started_unix_seconds", "finished_unix_seconds", "error")})
                output.write(json.dumps(entry, sort_keys=True) + "\n")
                output.flush()
            if sample + 1 < args.samples:
                time.sleep(max(0, args.interval - (time.monotonic() - started)))
    # Each failure names its node and scrape interval, so evaluate.py can drop the ones inside expected-down windows.
    result = {"failed_scrapes": len(failures), "failures": failures, "acceptance": "pending"}
    (args.output / "result.json").write_text(json.dumps(result) + "\n")
    return int(len(failures) != 0)


def main():
    """Parse the explicit workload identity and bounded capture duration."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--phase", required=True, choices=("baseline", "steady", "catch-up", "reconnect", "hostile", "mixed"))
    parser.add_argument("--samples", type=int, default=60)
    parser.add_argument("--interval", type=positive, default=1)
    args = parser.parse_args()
    if not 1 <= args.samples <= 3600:
        parser.error("samples must be between 1 and 3600")
    try:
        return capture(args)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"capture failed: {error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
