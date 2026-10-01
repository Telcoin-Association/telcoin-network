#!/usr/bin/env python3
"""Freeze acceptance criteria and score complete public hub qualification evidence.

Input measurements must come from a live workload adapter. This tool neither
generates population measurements nor turns a unit test into capacity evidence.
"""

import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parent
MAX_BYTES = 16 * 1024 * 1024
SCENARIOS = {
    "public_join", "shared_nat_reconnect", "gossip_two_hops", "record_lookup",
    "submit_url_lookup", "concurrent_sync", "committee_progress", "dao_connectivity",
}
SERVICES = {"epoch_stream", "epoch_record", "primary_shed", "batch_stream", "worker_shed", "prefetch"}
TASK_LIMITS = {
    "primary": {"epoch_stream": 5, "epoch_record": 5, "primary_shed": 8},
    "worker-0": {"batch_stream": 5, "worker_shed": 8, "prefetch": 8},
    "worker-1": {"batch_stream": 5, "worker_shed": 8, "prefetch": 8},
}


def read_json(path):
    """Bound input size and reject duplicate keys and nonfinite JSON numbers."""
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key: {key}")
            result[key] = value
        return result

    with path.open("rb") as source:
        raw = source.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("input exceeds 16 MiB")
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda value: fail(f"nonfinite number: {value}"))


def fail(message):
    raise ValueError(message)


def digest(value):
    """Hash semantic JSON, independent of formatting or object key order."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def number(value, name, minimum=0):
    if type(value) not in (int, float) or abs(value) > 2**64 - 1 or not math.isfinite(value) or value < minimum:
        fail(f"{name} must be a finite number >= {minimum}")
    return value


def integer(value, name, minimum=1):
    if type(value) is not int or value < minimum or value > 2**64 - 1:
        fail(f"{name} must be an integer >= {minimum}")
    return value


def validate_plan(plan):
    """Check the complete declaration before either workload is scored."""
    if type(plan.get("version")) is not int or plan["version"] != 1:
        fail("unsupported plan version")
    for phase in ("baseline", "candidate"):
        revision = plan[phase]["revision"]
        if not isinstance(revision, str) or re.fullmatch(r"[0-9a-f]{40}", revision) is None:
            fail(f"{phase}.revision must be a full source SHA")
        if not plan[phase]["build_command"] or not plan[phase]["binary_sha256"]:
            fail(f"{phase} requires build command and binary digests")
        for value in plan[phase]["binary_sha256"].values():
            if re.fullmatch(r"[0-9a-f]{64}", value) is None:
                fail("binary digest must be SHA-256")
    shipped = read_json(ROOT / "profile-v1.json")
    candidate_profile = plan["candidate"]["profile"]
    selected = {key: candidate_profile.get(key) for key in shipped}
    if digest(selected) != digest(shipped):
        fail("candidate configuration differs from the shipped profile")
    if set(candidate_profile) - set(shipped) - {"bootstrap_peers", "hostname", "dao_observers"}:
        fail("v1 permits deployment bootstrap/hostname settings alongside the exact profile")
    observers = candidate_profile.get("dao_observers", [])
    if not isinstance(observers, list) or len(observers) != 8 or any(not isinstance(key, str) or not key for key in observers) or len(set(observers)) != 8:
        fail("declare eight distinct DAO observer identities before qualification")
    if any(key not in candidate_profile.get("bootstrap_peers", {}) for key in observers):
        fail("every DAO observer must be provisioned in the trusted bootstrap set")
    if plan["baseline"]["profile"].get("dao_observers") != observers:
        fail("baseline and candidate must measure the same DAO identities")
    envelope = plan["envelope"]
    for field in ("cpus_per_hub", "ram_bytes_per_hub", "link_mbps", "rtt_ms",
                  "public_peers", "shared_nat_peers", "dao_observers", "committee_peers", "workers_per_hub",
                  "duration_seconds"):
        integer(envelope[field], field)
    number(envelope["loss_percent"], "loss_percent")
    if envelope["loss_percent"] > 100:
        fail("loss_percent exceeds 100")
    if envelope["public_peers"] != 64 or envelope["shared_nat_peers"] != 16:
        fail("v1 qualifies 64 public peers, including 16 sharing one NAT")
    if envelope["dao_observers"] != 8 or envelope["committee_peers"] != 4 or envelope["workers_per_hub"] != 2:
        fail("v1 requires 8 DAO observers, 12 committee peers across rotation, and 2 workers per hub")
    if envelope["duration_seconds"] < 600:
        fail("each phase must last at least 600 seconds")
    if not envelope["hardware"] or not envelope["network_setup"]:
        fail("hardware and exact network setup must be recorded")
    if not plan["hubs"] or len(set(plan["hubs"])) != len(plan["hubs"]):
        fail("declare distinct hub IDs")
    thresholds = plan["thresholds"]
    for field in ("max_rss_bytes", "max_cpu_cores", "max_queue_occupancy", "max_progress_stall_seconds"):
        number(thresholds[field], field, 1)
    if thresholds["max_rss_bytes"] > envelope["ram_bytes_per_hub"]:
        fail("RSS threshold exceeds available RAM")
    if thresholds["max_cpu_cores"] >= envelope["cpus_per_hub"]:
        fail("CPU threshold must leave aggregate headroom")
    if set(thresholds["scenarios"]) != SCENARIOS:
        fail("declare acceptance criteria for all eight workload scenarios")
    for scenario, bounds in thresholds["scenarios"].items():
        integer(bounds["minimum_attempts"], scenario)
        number(bounds["max_p99_ms"], scenario, 1)
        rate = number(bounds["minimum_success_rate"], scenario)
        if not 0 < rate <= 1:
            fail(f"invalid success rate for {scenario}")
        if scenario == "committee_progress" and not 0 <= number(bounds["max_cancelled_fraction"], "committee cancellation bound") < 1:
            fail("invalid committee cancellation bound")
    if not plan["threshold_owner"] or not plan["adapter_command"]:
        fail("record the threshold decision and exact workload adapter command")


def validate_evidence(plan, evidence, phase):
    """Reject missing, stale, inconsistent, or incomplete telemetry."""
    if evidence.get("phase") != phase or evidence.get("plan_sha256") != digest(plan):
        fail("evidence is not bound to this predeclared plan and phase")
    if evidence.get("revision") != plan[phase]["revision"]:
        fail("source revision mismatch")
    if evidence.get("profile_sha256") != digest(plan[phase]["profile"]):
        fail("deployed profile mismatch")
    if evidence.get("binary_sha256") != plan[phase]["binary_sha256"]:
        fail("deployed binary mismatch")
    if evidence.get("envelope") != plan["envelope"]:
        fail("baseline and candidate must use the declared envelope")
    if not evidence.get("artifacts"):
        fail("raw telemetry and workload logs must be retained")
    for artifact in evidence["artifacts"]:
        if not artifact["path"] or re.fullmatch(r"[0-9a-f]{64}", artifact["sha256"]) is None:
            fail("every raw artifact requires a path and SHA-256")
    swarms = {"primary", "worker-0", "worker-1"}
    previous = None
    for sample in evidence["samples"]:
        timestamp = number(sample["elapsed_seconds"], "sample time")
        if previous is not None and not 0 < timestamp - previous <= 5:
            fail("samples must be monotonic, no more than five seconds apart")
        previous = timestamp
        if set(sample["hubs"]) != set(plan["hubs"]):
            fail("every sample must cover every hub process")
        for hub in sample["hubs"].values():
            number(hub["rss_bytes"], "whole-process RSS", 1)
            number(hub["cpu_seconds"], "whole-process CPU")
            integer(hub["progress"], "application progress", 0)
            integer(hub["dao_connected"], "DAO connectivity", 0)
            integer(hub["source_rows"], "source accounting occupancy", 0)
            if set(hub["swarms"]) != swarms:
                fail("primary and all configured workers must be measured")
            totals = dict.fromkeys(SERVICES, 0)
            for network, swarm in hub["swarms"].items():
                for field in ("connections", "connection_limit", "streams_per_connection_limit",
                              "receive_credit_per_connection_bytes", "queue_occupancy",
                              "ordinary_peers", "dao_connected"):
                    integer(swarm[field], field, 0)
                if not isinstance(swarm["rejections"], dict):
                    fail("record rejection counts by reason, including an empty map")
                for count in swarm["rejections"].values():
                    integer(count, "rejections", 0)
                if set(swarm["tasks"]) != set(TASK_LIMITS[network]) or swarm["task_limits"] != TASK_LIMITS[network]:
                    fail("every swarm must declare its independent serve-class allocations")
                for limit in swarm["task_limits"].values():
                    integer(limit, "per-swarm serve allocation", 1)
                for service, count in swarm["tasks"].items():
                    totals[service] += integer(count, "per-swarm serve occupancy", 0)
            if set(hub["tasks"]) != SERVICES:
                fail("all serve-class task occupancies must be measured")
            for count in hub["tasks"].values():
                integer(count, "task occupancy", 0)
            if hub["tasks"] != totals:
                fail("process serve occupancy must equal the measured primary and worker totals")
    samples = evidence["samples"]
    if len(samples) < 2 or samples[0]["elapsed_seconds"] != 0:
        fail("capture must start at zero and contain multiple samples")
    if samples[-1]["elapsed_seconds"] < plan["envelope"]["duration_seconds"]:
        fail("incomplete measurement interval")
    if set(evidence["operations"]) != SCENARIOS:
        fail("missing workload scenarios")
    for scenario, operations in evidence["operations"].items():
        if not operations:
            fail(f"empty {scenario} workload")
        identities = set()
        for operation in operations:
            if operation["id"] in identities:
                fail("duplicate operation ID")
            identities.add(operation["id"])
            if type(operation["success"]) is not bool:
                fail("operation success must be boolean")
            reason = operation["rejection_reason"]
            if not operation["success"] and (not isinstance(reason, str) or not reason):
                fail("rejected operations require a reason")
            if type(operation.get("cancelled", False)) is not bool or (operation.get("cancelled", False) and (scenario != "committee_progress" or operation["success"])):
                fail("only unsuccessful committee requests may be classified as cancelled")
            number(operation["latency_ms"], "operation latency")
            at = number(operation["elapsed_seconds"], "operation timestamp")
            if at > samples[-1]["elapsed_seconds"]:
                fail("operation lies outside captured interval")
            if scenario == "gossip_two_hops" and operation["success"]:
                if integer(operation["hops"], "gossip hops") < 2:
                    fail("direct hub delivery is insufficient gossip evidence")


def score(plan, evidence):
    """Apply absolute candidate limits while reporting measured baseline results separately."""
    bounds = plan["thresholds"]
    failures = []
    summary = {}
    profile = plan["candidate"]["profile"]
    process_budget = profile["process_budget"]
    swarm_limit = process_budget["max_established_connections"] // process_budget["swarm_count"]
    total_connections = swarm_limit * process_budget["swarm_count"]
    stream_limit = process_budget["max_inbound_streams"] // total_connections
    credit_limit = process_budget["max_receive_credit_bytes"] // total_connections
    for scenario, operations in evidence["operations"].items():
        rule = bounds["scenarios"][scenario]
        completed = [operation for operation in operations if not operation.get("cancelled", False)]
        cancellations = len(operations) - len(completed)
        latencies = sorted(operation["latency_ms"] for operation in completed)
        p99 = latencies[math.ceil(len(latencies) * 0.99) - 1] if latencies else 0
        rate = sum(operation["success"] for operation in completed) / len(completed) if completed else 0
        summary[scenario] = {"attempts": len(completed), "cancelled": cancellations, "success_rate": rate, "p99_ms": p99}
        if scenario == "committee_progress" and cancellations / len(operations) > rule["max_cancelled_fraction"]:
            failures.append("committee_progress: cancellation threshold exceeded")
        if len(completed) < rule["minimum_attempts"] or rate < rule["minimum_success_rate"] or p99 > rule["max_p99_ms"]:
            failures.append(f"{scenario}: workload threshold exceeded")
    task_limits = {"epoch_stream": 5, "epoch_record": 5, "primary_shed": 8,
                   "batch_stream": 10, "worker_shed": 16, "prefetch": 16}
    first = evidence["samples"][0]
    last = evidence["samples"][-1]
    for hub_id in plan["hubs"]:
        initial = first["hubs"][hub_id]
        final = last["hubs"][hub_id]
        for network in TASK_LIMITS:
            if max(sample["hubs"][hub_id]["swarms"][network]["ordinary_peers"]
                   for sample in evidence["samples"]) < plan["envelope"]["public_peers"]:
                failures.append(f"{hub_id}/{network}: declared public population was never measured")
        if final["progress"] <= initial["progress"]:
            failures.append(f"{hub_id}: application made no progress")
        previous_cpu = initial["cpu_seconds"]
        previous_progress = initial["progress"]
        last_advanced_at = first["elapsed_seconds"]
        for previous, sample in zip(evidence["samples"], evidence["samples"][1:]):
            hub = sample["hubs"][hub_id]
            delta = hub["cpu_seconds"] - previous_cpu
            if delta < 0 or delta / (sample["elapsed_seconds"] - previous["elapsed_seconds"]) > bounds["max_cpu_cores"]:
                failures.append(f"{hub_id}: CPU headroom exhausted or process restarted")
            previous_cpu = hub["cpu_seconds"]
            if hub["progress"] < previous_progress:
                failures.append(f"{hub_id}: application progress regressed")
            if hub["progress"] > previous_progress:
                last_advanced_at = sample["elapsed_seconds"]
            elif sample["elapsed_seconds"] - last_advanced_at > bounds["max_progress_stall_seconds"]:
                failures.append(f"{hub_id}: application progress stalled")
            previous_progress = hub["progress"]
        for sample in evidence["samples"]:
            hub = sample["hubs"][hub_id]
            if hub["rss_bytes"] > bounds["max_rss_bytes"] or hub["source_rows"] > profile["source_admission"]["max_sources"]:
                failures.append(f"{hub_id}: RSS or accounting table exceeded")
            if hub["dao_connected"] < plan["envelope"]["dao_observers"]:
                failures.append(f"{hub_id}: DAO observer reservation lost")
            allocations = list(hub["swarms"].values())
            if any(swarm["ordinary_peers"] > profile["public_peer_limit"] or
                   swarm["dao_connected"] < plan["envelope"]["dao_observers"] for swarm in allocations):
                failures.append(f"{hub_id}: public population or per-swarm DAO reservation exceeded")
            if any(count > swarm["task_limits"][service]
                   for swarm in allocations for service, count in swarm["tasks"].items()):
                failures.append(f"{hub_id}: independent swarm task budget exceeded")
            if any(swarm["connection_limit"] != swarm_limit or swarm["connections"] > swarm_limit or
                   swarm["streams_per_connection_limit"] != stream_limit or
                   swarm["receive_credit_per_connection_bytes"] != credit_limit or
                   swarm["queue_occupancy"] > bounds["max_queue_occupancy"] for swarm in allocations):
                failures.append(f"{hub_id}: swarm allocation or queue threshold exceeded")
            if sum(swarm["connection_limit"] for swarm in allocations) > process_budget["max_established_connections"]:
                failures.append(f"{hub_id}: aggregate connection allocation exceeded")
            if any(count > task_limits[service] for service, count in hub["tasks"].items()):
                failures.append(f"{hub_id}: serve-class task budget exceeded")
    return {"passed": not failures, "failures": sorted(set(failures)), "scenarios": summary}


def verify_artifacts(evidence, directory):
    """Verify retained raw files without loading whole logs into memory."""
    if len(evidence["artifacts"]) > 64:
        fail("at most 64 raw artifacts per phase")
    for artifact in evidence["artifacts"]:
        path = directory / artifact["path"]
        hasher = hashlib.sha256()
        size = 0
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                size += len(chunk)
                if size > 64 * 1024 * 1024:
                    fail("split raw artifacts larger than 64 MiB before scoring")
                hasher.update(chunk)
        if hasher.hexdigest() != artifact["sha256"]:
            fail(f"raw artifact digest mismatch: {artifact['path']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    template = commands.add_parser("template", help="write an unfilled declaration for a real run")
    template.add_argument("--output", required=True, type=Path)
    freeze = commands.add_parser("freeze", help="validate and freeze criteria before workload execution")
    freeze.add_argument("declaration", type=Path)
    freeze.add_argument("--output", required=True, type=Path)
    qualify = commands.add_parser("score", help="score complete baseline and candidate evidence")
    qualify.add_argument("plan", type=Path)
    qualify.add_argument("baseline", type=Path)
    qualify.add_argument("candidate", type=Path)
    qualify.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "template":
        phase = {"revision": "REPLACE_WITH_SOURCE_SHA", "build_command": "REPLACE_WITH_EXACT_BUILD_COMMAND",
                 "binary_sha256": {"telcoin-network": "REPLACE_WITH_BINARY_SHA256"}, "profile": {}}
        scenario_bounds = {
            "public_join": (64, 8000), "shared_nat_reconnect": (64, 20000),
            "gossip_two_hops": (4096, 3000), "record_lookup": (256, 3000),
            "submit_url_lookup": (256, 3000), "concurrent_sync": (256, 30000),
            "committee_progress": (512, 1500), "dao_connectivity": (512, 1000),
        }
        result = {
            "version": 1, "baseline": phase,
            "candidate": {**phase, "profile": read_json(ROOT / "profile-v1.json")},
            "envelope": {"cpus_per_hub": 4, "ram_bytes_per_hub": 8 * 1024**3,
                         "link_mbps": 25, "rtt_ms": 50, "loss_percent": 0.1,
                         "public_peers": 64, "shared_nat_peers": 16, "dao_observers": 8, "committee_peers": 4,
                         "workers_per_hub": 2, "duration_seconds": 600,
                         "hardware": "REPLACE_WITH_HARDWARE_DESCRIPTION",
                         "network_setup": "REPLACE_WITH_REPRODUCIBLE_NETWORK_COMMANDS"},
            "hubs": ["hub-0", "hub-1"],
            "threshold_owner": "PR author, using requester-authorized engineering judgment",
            "adapter_command": "REPLACE_WITH_EXACT_WORKLOAD_ADAPTER_COMMAND",
            "thresholds": {"max_rss_bytes": 4 * 1024**3, "max_cpu_cores": 3,
                           "max_queue_occupancy": 100, "max_progress_stall_seconds": 15,
                           "scenarios": {scenario: {"minimum_attempts": attempts,
                               "minimum_success_rate": 0.99, "max_p99_ms": latency,
                               **({"max_cancelled_fraction": 0.35} if scenario == "committee_progress" else {})}
                               for scenario, (attempts, latency) in scenario_bounds.items()}},
        }
        with args.output.open("x") as output:
            json.dump(result, output, indent=2, allow_nan=False)
            output.write("\n")
        return 0
    document = read_json(args.declaration if args.command == "freeze" else args.plan)
    plan = document if args.command == "freeze" else document["plan"]
    validate_plan(plan)
    if args.command == "freeze":
        result = {"plan": plan, "plan_sha256": digest(plan)}
    else:
        frozen = document
        # A frozen plan cannot silently acquire new thresholds after measurements exist.
        if frozen["plan_sha256"] != digest(frozen["plan"]):
            fail("frozen plan hash mismatch")
        plan = frozen["plan"]
        baseline, candidate = read_json(args.baseline), read_json(args.candidate)
        validate_evidence(plan, baseline, "baseline")
        validate_evidence(plan, candidate, "candidate")
        verify_artifacts(baseline, args.baseline.parent)
        verify_artifacts(candidate, args.candidate.parent)
        result = {"plan_sha256": digest(plan), "baseline": score(plan, baseline),
                  "candidate": score(plan, candidate)}
    with args.output.open("x") as output:
        json.dump(result, output, indent=2, allow_nan=False)
        output.write("\n")
    return 0 if args.command == "freeze" or result["candidate"]["passed"] else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, KeyError, TypeError, OSError, AttributeError, IndexError, RecursionError) as error:
        print(f"qualification rejected: {error}", file=sys.stderr)
        sys.exit(2)
