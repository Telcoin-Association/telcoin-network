"""Validate declared hardware acceptance against a pinned measurement report."""

from datetime import datetime
import hashlib
import json
import math
from pathlib import Path


# Every bound must be selected before the measurement, with no guessed defaults.
METRICS = {
    "cpu_percent": "max", "rss_bytes": "max", "socket_drops": "max",
    "queue_peak": "max", "established_p99_ms": "max",
    "established_throughput_bps": "min", "honest_reconnect_success": "min",
    "command_progress": "min", "timer_progress": "min", "consensus_progress": "min",
}


def require(condition, message):
    """Reject incomplete evidence instead of filling missing observations with defaults."""
    if not condition:
        raise ValueError(message)


def number(value, name):
    """Metrics and bounds must be real finite numbers, not booleans or nulls."""
    require(type(value) in (int, float) and math.isfinite(value) and value >= 0,
            f"invalid or missing numeric evidence: {name}")
    return value


def validate(path, commit):
    """Check the candidate, complete swarm coverage, measured envelope and every bound."""
    report = json.loads(path.read_text())
    require(report.get("version") == 1, "unsupported qualification report version")
    require(report.get("candidate") == commit, "hardware report is for a different candidate")
    environment = report["environment"]
    require(environment.get("representative_nic") is True, "loopback is not NIC qualification")
    require(environment.get("firewall_disabled") is True, "host firewall condition is unverified")
    for field in ("host", "kernel", "interface", "driver", "offloads", "representativeness"):
        require(bool(environment.get(field)), f"missing hardware environment: {field}")
    require(environment["interface"] != "lo", "loopback is not a representative NIC")
    acceptance = report["acceptance"]
    selected = datetime.fromisoformat(acceptance["selected_at"])
    started = datetime.fromisoformat(report["measurement_started_at"])
    require(selected.tzinfo is not None and started.tzinfo is not None and selected <= started,
            "acceptance thresholds must be timestamped before measurement")
    uplink = number(environment["uplink_bps"], "uplink_bps")
    lower = number(acceptance["ingress_min_bps"], "ingress_min_bps")
    upper = number(acceptance["ingress_max_bps"], "ingress_max_bps")
    require(0 < lower <= upper < uplink, "declared ingress must remain below uplink saturation")
    require(number(report["real_sources"], "real_sources") >= 2, "diverse real sources are missing")
    require(number(report["retry_completed_sources"], "retry_completed_sources") >= 2,
            "clients completing Retry are missing")
    roles = report["swarm_roles"]
    require(isinstance(roles, list) and roles and len(set(roles)) == len(roles),
            "swarm roles must be explicit and unique")
    require("primary" in roles and "worker-0" in roles, "primary and worker coverage are missing")
    expected = set(roles) | {"process"}
    cases = report["cases"]
    require({case["role"] for case in cases} == expected and len(cases) == len(expected),
            "measure each swarm separately and then the complete node")
    for case in cases:
        received = number(case["received_bps"], "received_bps")
        generator = number(case["generator_capacity_bps"], "generator_capacity_bps")
        require(lower <= received <= upper and generator >= received,
                f"received traffic or generator capacity outside declared envelope: {case['role']}")
        for metric, direction in METRICS.items():
            measured = number(case["metrics"].get(metric), f"{case['role']}.{metric}")
            bound = number(acceptance["metrics"].get(metric), f"bound.{metric}")
            if direction == "max":
                require(measured <= bound, f"failed maximum: {case['role']}.{metric}")
            else:
                require(bound > 0, f"progress and throughput bounds must be positive: {metric}")
                require(measured >= bound, f"failed minimum: {case['role']}.{metric}")
            if metric == "honest_reconnect_success":
                require(measured <= 1 and bound <= 1, "reconnect success must be a ratio")
    artifacts = report["artifacts"]
    categories = {artifact["kind"] for artifact in artifacts}
    require({"node-binary", "node-config", "host-collector", "traffic", "application-telemetry",
             "firewall"} <= categories, "qualification provenance artifacts are incomplete")
    for artifact in artifacts:
        source = (path.parent / artifact["path"]).resolve(strict=True)
        require(source.is_relative_to(path.parent.resolve()) and source.is_file(),
                "qualification artifacts must stay inside their evidence directory")
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        require(digest == artifact["sha256"], f"changed qualification artifact: {artifact['path']}")
    return {"status": "passed", "candidate": commit, "roles": sorted(expected),
            "report_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "acceptance": acceptance, "environment": environment}
