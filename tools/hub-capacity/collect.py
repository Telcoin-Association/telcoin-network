#!/usr/bin/env python3
"""Collect Linux process and Prometheus evidence for a frozen hub qualification plan."""

import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shlex
import subprocess
import time
import urllib.request


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("hub_qualify", ROOT / "qualify.py")
QUALIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(QUALIFY)
SWARMS = ("primary", "worker-0", "worker-1")
CLASSES = {
    "primary": ("epoch_stream", "epoch_record", "primary_shed"),
    "worker-0": ("batch_stream", "worker_shed", "prefetch"),
    "worker-1": ("batch_stream", "worker_shed", "prefetch"),
}
SAMPLE = re.compile(r'([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*\})?\s+(\S+)(?:\s+\S+)?')
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"(?:,|$)')


def parse_metrics(raw):
    """Parse finite Prometheus samples, rejecting duplicates and malformed label sets."""
    result = {}
    for line in raw.splitlines():
        if not line or line.startswith("#"):
            continue
        match = SAMPLE.fullmatch(line)
        if match is None:
            raise ValueError("malformed Prometheus sample")
        name, labels, value = match.groups()
        parsed = {}
        if labels:
            text = labels[1:-1]
            cursor = 0
            while cursor < len(text):
                label = LABEL.match(text, cursor)
                if label is None or label[1] in parsed:
                    raise ValueError("malformed or duplicate Prometheus label")
                parsed[label[1]] = json.loads('"' + label[2] + '"')
                cursor = label.end()
        key = (name, tuple(sorted(parsed.items())))
        if key in result:
            raise ValueError("duplicate Prometheus sample")
        # Histograms can legitimately expose infinity in bucket labels, but not in values.
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("nonfinite Prometheus value")
        result[key] = number
    return result


def select(metrics, name, labels=None):
    """Require exactly one series for the full selected label set."""
    key = (name, tuple(sorted((labels or {}).items())))
    if key not in metrics:
        raise ValueError(f"missing metric {name} {labels or {}}")
    value = metrics[key]
    if value < 0 or value != int(value):
        raise ValueError(f"metric {name} must be a nonnegative integer")
    return int(value)


def capacity_metrics(raw, progress_name):
    """Parse capacity and progress series while preserving the full response in raw telemetry."""
    selected = "\n".join(line for line in raw.splitlines()
                         if line.startswith(("tn_network_", progress_name)))
    return parse_metrics(selected)


def process_sample(pid, proc=Path("/proc"), ticks=None, page_size=None):
    """Read whole-process CPU and RSS, preserving process identity across a run."""
    ticks = ticks or os.sysconf("SC_CLK_TCK")
    page_size = page_size or os.sysconf("SC_PAGE_SIZE")
    directory = proc / str(pid)
    raw = (directory / "stat").read_text()
    # comm can contain spaces and closing parentheses. Fields after its final ')' are fixed.
    fields = raw[raw.rfind(")") + 2:].split()
    if len(fields) < 22:
        raise ValueError("incomplete process stat")
    cpu = (int(fields[11]) + int(fields[12])) / ticks
    identity = int(fields[19])
    rss = int(fields[21]) * page_size
    if rss <= 0 or cpu < 0:
        raise ValueError("invalid whole-process sample")
    return {"rss_bytes": rss, "cpu_seconds": cpu}, identity, raw


def observations(metrics, binding, phase):
    """Map all primary/worker allocations and service occupancies without defaulting omissions."""
    swarms = {}
    tasks = dict.fromkeys(QUALIFY.SERVICES, 0)
    for network in SWARMS:
        labels = {"network": network}
        swarm = {
            field: select(metrics, "tn_network_" + metric, labels)
            for field, metric in {
                "connections": "established_connections",
                "connection_limit": "established_connection_limit",
                "streams_per_connection_limit": "inbound_streams_per_connection_limit",
                "receive_credit_per_connection_bytes": "receive_credit_per_connection_bytes",
            }.items()
        }
        queues = ("command_queue_occupancy", "inbound_requests_pending", "record_queries_pending",
                  "outbound_requests_pending", "px_disconnects_pending")
        swarm["queue_occupancy"] = sum(select(metrics, "tn_network_" + name, labels) for name in queues)
        swarm["rejections"] = {}
        swarm["ordinary_peers"] = select(metrics, "tn_network_ordinary_peers_connected", labels)
        swarm["dao_connected"] = select(metrics, "tn_network_dao_observers_connected", labels)
        for (name, series_labels), value in metrics.items():
            series = dict(series_labels)
            if series.get("network") == network and (
                name.endswith("_denied_total") or name.endswith("_rejections_total")
                or name.endswith("_shed_total") or name.endswith("_refused_total")
            ):
                if value < 0 or value != int(value):
                    raise ValueError("invalid rejection counter")
                reason = name + ":" + json.dumps(series, sort_keys=True, separators=(",", ":"))
                swarm["rejections"][reason] = int(value)
        for service in CLASSES[network]:
            tasks[service] += select(metrics, "tn_network_serve_tasks_active", {**labels, "class": service})
        swarm["tasks"] = {service: select(metrics, "tn_network_serve_tasks_active", {**labels, "class": service})
                          for service in CLASSES[network]}
        swarm["task_limits"] = {service: select(metrics, "tn_network_serve_tasks_limit", {**labels, "class": service})
                                for service in CLASSES[network]}
        swarms[network] = swarm
    source = select(metrics, "tn_network_source_address_rows")
    accounting = select(metrics, "tn_network_source_accounting_enabled")
    if accounting not in (0, 1) or (phase == "candidate" and accounting != 1):
        raise ValueError("candidate must measure enabled process-wide source accounting")
    # These extra tables share the connection bound, and must also be retained in raw telemetry.
    for name in ("source_connections", "source_peer_rows", "source_prefix_rows"):
        select(metrics, "tn_network_" + name)
    return {
        "progress": select(metrics, **binding["progress"]),
        "dao_connected": min(swarm["dao_connected"] for swarm in swarms.values()),
        "source_rows": source, "swarms": swarms, "tasks": tasks,
    }


def file_hash(path):
    hasher = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def retain_file(source, destination):
    """Copy a finite raw declaration or topology artifact without reading it into memory."""
    size = 0
    with Path(source).open("rb") as incoming, destination.open("xb") as outgoing:
        for chunk in iter(lambda: incoming.read(1024 * 1024), b""):
            size += len(chunk)
            if size > 64 * 1024**2:
                raise ValueError("raw input exceeds 64 MiB")
            outgoing.write(chunk)
    if size == 0:
        raise ValueError("raw input must not be empty")


class RawLog:
    """Retain bounded raw JSONL segments, with hashes generated only after close."""

    def __init__(self, directory, maximum_segments=60):
        self.directory = directory
        self.paths = []
        self.stream = None
        self.size = 0
        self.maximum_segments = maximum_segments

    def append(self, entry):
        data = (json.dumps(entry, allow_nan=False, separators=(",", ":")) + "\n").encode()
        if len(data) > 8 * 1024**2:
            raise ValueError("raw entry exceeds 8 MiB")
        if self.stream is None or self.size + len(data) > 32 * 1024**2:
            self.close()
            if len(self.paths) >= self.maximum_segments:
                raise ValueError("raw telemetry exceeds artifact budget")
            path = self.directory / f"telemetry-{len(self.paths):03}.jsonl"
            self.stream = path.open("xb")
            self.paths.append(path)
            self.size = 0
        self.stream.write(data)
        self.stream.flush()
        self.size += len(data)

    def close(self):
        if self.stream is not None:
            self.stream.close()
            self.stream = None

    def artifacts(self):
        self.close()
        return [{"path": path.name, "sha256": file_hash(path)} for path in self.paths]


def read_operations(path):
    """Keep workload failures and successful operations for every required scenario."""
    if path.stat().st_size > 64 * 1024**2:
        raise ValueError("operation log exceeds 64 MiB")
    result = {scenario: [] for scenario in QUALIFY.SCENARIOS}
    with path.open() as stream:
        for line in stream:
            entry = json.loads(line)
            scenario = entry.pop("scenario")
            if scenario not in result:
                raise ValueError("unknown workload scenario")
            # Full command output and protocol traces remain in the hashed operations artifact.
            result[scenario].append({key: value for key, value in entry.items() if key in {
                "id", "success", "rejection_reason", "latency_ms", "elapsed_seconds", "hops", "cancelled",
            }})
    return result


def validate_process(binding, envelope, proc=Path("/proc"), affinity=None):
    """Reject a deployment whose argv, CPU assignment or cgroup limits differ from its plan."""
    pid = QUALIFY.integer(binding["pid"], "hub pid", 1)
    directory = proc / str(pid)
    argv = [part.decode() for part in (directory / "cmdline").read_bytes().split(b"\0") if part]
    if argv != binding["argv"]:
        raise ValueError("running command does not match deployment")
    if sorted((affinity or os.sched_getaffinity)(pid)) != binding["cpu_affinity"]:
        raise ValueError("CPU affinity does not match deployment")
    cgroup = directory / "root/sys/fs/cgroup"
    if int((cgroup / "memory.max").read_text()) != envelope["ram_bytes_per_hub"]:
        raise ValueError("container memory limit does not match envelope")
    quota, period = (cgroup / "cpu.max").read_text().split()
    if int(quota) / int(period) != envelope["cpus_per_hub"]:
        raise ValueError("container CPU quota does not match envelope")


def collect(frozen, bindings, phase, output):
    plan = frozen["plan"]
    if frozen["plan_sha256"] != QUALIFY.digest(plan):
        raise ValueError("frozen plan hash mismatch")
    QUALIFY.validate_plan(plan)
    if set(bindings["hubs"]) != set(plan["hubs"]):
        raise ValueError("bindings must cover every declared hub")
    if not isinstance(bindings["workload"], list) or not all(isinstance(arg, str) for arg in bindings["workload"]):
        raise ValueError("workload must be an executable argument list")
    if not bindings["workload"] or shlex.join(bindings["workload"]) != plan["adapter_command"]:
        raise ValueError("workload must match the frozen adapter declaration")
    output.mkdir(parents=True, exist_ok=False)
    topology = output / "topology.json"
    retain_file(bindings["topology_artifact"], topology)
    protocol_logs = bindings.get("protocol_logs", [])
    if not isinstance(protocol_logs, list) or len(protocol_logs) != plan["envelope"]["committee_peers"] or len(set(protocol_logs)) != len(protocol_logs):
        raise ValueError("retain a distinct production log for every declared committee validator")
    raw = RawLog(output, maximum_segments=64 - 3 - len(protocol_logs))
    operations = output / "operations.jsonl"
    workload_log = output / "workload.log"
    identities = {}
    for hub, binding in bindings["hubs"].items():
        if binding["revision"] != plan[phase]["revision"]:
            raise ValueError(f"{hub}: source revision mismatch")
        if QUALIFY.digest(QUALIFY.read_json(Path(binding["profile_path"]))) != QUALIFY.digest(plan[phase]["profile"]):
            raise ValueError(f"{hub}: deployment profile mismatch")
        pid = QUALIFY.integer(binding["pid"], "hub pid", 1)
        validate_process(binding, plan["envelope"])
        binary = Path("/proc") / str(pid) / "exe"
        if file_hash(binary) != plan[phase]["binary_sha256"]["telcoin-network"]:
            raise ValueError(f"{hub}: running executable digest mismatch")
        _, identities[hub], _ = process_sample(pid)
    started = time.monotonic()
    samples = []
    child = None
    try:
        with workload_log.open("xb") as log:
            # The driver writes operation observations with timestamps relative to this origin.
            environment = {**os.environ, "HUB_CAPACITY_ORIGIN": str(started),
                           "HUB_CAPACITY_OPERATIONS": str(operations.resolve()),
                           "HUB_CAPACITY_PHASE": phase, "HUB_CAPACITY_PLAN_SHA256": QUALIFY.digest(plan)}
            child = subprocess.Popen(bindings["workload"], stdout=log, stderr=log, env=environment)
            while True:
                completed_before_sample = child.poll() == 0
                elapsed = 0.0 if not samples else time.monotonic() - started
                hubs = {}
                for hub, binding in bindings["hubs"].items():
                    process, identity, stat = process_sample(binding["pid"])
                    if identity != identities[hub]:
                        raise ValueError(f"{hub}: process restarted during qualification")
                    with urllib.request.urlopen(binding["metrics_url"], timeout=2) as response:
                        body = response.read(4 * 1024**2 + 1)
                    if len(body) > 4 * 1024**2:
                        raise ValueError("metrics response exceeds 4 MiB")
                    text = body.decode()
                    raw.append({"hub": hub, "elapsed_seconds": elapsed,
                                "pid": binding["pid"], "stat": stat, "metrics": text})
                    hubs[hub] = {**process, **observations(capacity_metrics(text, binding["progress"]["name"]), binding, phase)}
                samples.append({"elapsed_seconds": elapsed, "hubs": hubs})
                if elapsed >= plan["envelope"]["duration_seconds"] and completed_before_sample:
                    break
                if elapsed >= plan["envelope"]["duration_seconds"] + 30:
                    raise ValueError("workload did not finish within the measured drain interval")
                if child.poll() not in (None, 0):
                    raise ValueError("workload driver failed")
                if workload_log.stat().st_size > 64 * 1024**2:
                    raise ValueError("workload log exceeds artifact budget")
                time.sleep(max(0, 2 - (time.monotonic() - started - elapsed)))
            if child.wait(timeout=30) != 0:
                raise ValueError("workload driver failed")
        protocol_artifacts = []
        for index, path in enumerate(protocol_logs):
            retained = output / f"protocol-{index:02}.jsonl"
            retain_file(path, retained)
            protocol_artifacts.append({"path": retained.name, "sha256": file_hash(retained)})
        result = {
            "phase": phase, "plan_sha256": QUALIFY.digest(plan), "revision": plan[phase]["revision"],
            "profile_sha256": QUALIFY.digest(plan[phase]["profile"]),
            "binary_sha256": plan[phase]["binary_sha256"], "envelope": plan["envelope"],
            "samples": samples, "operations": read_operations(operations),
            "artifacts": raw.artifacts() + protocol_artifacts + [
                {"path": path.name, "sha256": file_hash(path)} for path in (operations, workload_log, topology)
            ],
        }
        QUALIFY.validate_evidence(plan, result, phase)
        with (output / "evidence.json").open("x") as stream:
            json.dump(result, stream, allow_nan=False)
        return result
    finally:
        raw.close()
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan", type=Path)
    parser.add_argument("bindings", type=Path)
    parser.add_argument("--phase", choices=("baseline", "candidate"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    collect(QUALIFY.read_json(args.plan), QUALIFY.read_json(args.bindings), args.phase, args.output)


if __name__ == "__main__":
    main()
