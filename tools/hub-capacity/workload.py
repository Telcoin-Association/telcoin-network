#!/usr/bin/env python3
"""Schedule concurrent real peer-agent commands for every frozen qualification scenario."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("hub_qualify", ROOT / "qualify.py")
QUALIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(QUALIFY)

# Leave eight control-server slots for handlers finishing timed-out commands.
MAX_ACTIVE_COMMANDS = 24


def validate_manifest(plan, manifest):
    """Bind scenario commands and peer populations before launching any workload."""
    if set(manifest["scenarios"]) != QUALIFY.SCENARIOS:
        raise ValueError("manifest must implement every required scenario")
    for scenario, definition in manifest["scenarios"].items():
        if QUALIFY.integer(definition["concurrency"], "driver concurrency", 1) > 16:
            raise ValueError("driver concurrency must not exceed sixteen")
        if not 0 <= QUALIFY.number(definition.get("offset_fraction", 0), "schedule offset") < 1:
            raise ValueError("schedule offset must be between zero and one")
        if QUALIFY.integer(definition.get("burst_size", 1), "burst size", 1) > definition["concurrency"]:
            raise ValueError("burst size must not exceed bounded driver concurrency")
        if not definition["agents"] or len(definition["agents"]) > 128:
            raise ValueError("every scenario requires a bounded peer-agent population")
        identities = set()
        for agent in definition["agents"]:
            if not isinstance(agent["identity"], str) or not agent["identity"] or agent["identity"] in identities:
                raise ValueError("peer-agent identities must be unique within each scenario")
            identities.add(agent["identity"])
            if not agent["argv"] or not all(isinstance(arg, str) and arg for arg in agent["argv"]):
                raise ValueError("every agent requires a real executable argument vector")
        required = {"public_join": plan["envelope"]["public_peers"],
                    "shared_nat_reconnect": plan["envelope"]["shared_nat_peers"],
                    "dao_connectivity": plan["envelope"]["dao_observers"]}.get(scenario, 1)
        if len(identities) < required:
            raise ValueError(f"{scenario}: insufficient distinct peer agents")
    if not manifest.get("topology_artifact"):
        raise ValueError("retain network namespaces, links, NAT rules, and peer topology")


def execute(agent, scenario, operation_id, origin, timeout):
    """Measure one command, keeping timeouts and refusals in the operation population."""
    started = time.monotonic()
    result = {"scenario": scenario, "id": operation_id, "success": False,
              "rejection_reason": None, "agent": agent["identity"], "argv": agent["argv"]}
    environment = {**os.environ, "HUB_CAPACITY_OPERATION_ID": operation_id,
                   "HUB_CAPACITY_SCENARIO": scenario,
                   "HUB_CAPACITY_MEASUREMENT_UNIX_US": str(time.time_ns() // 1000 - int((started - origin) * 1_000_000))}
    child = None
    try:
        with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
            child = subprocess.Popen(agent["argv"], stdout=output, stderr=errors, env=environment)
            while child.poll() is None:
                if time.monotonic() - started > timeout:
                    result["rejection_reason"] = "timeout"
                    child.kill()
                    break
                if os.fstat(output.fileno()).st_size + os.fstat(errors.fileno()).st_size > 65536:
                    result["rejection_reason"] = "agent_output_limit"
                    child.kill()
                    break
                time.sleep(0.01)
            status = child.wait()
            output.seek(0)
            errors.seek(0)
            stdout = output.read(65536).decode(errors="replace")
            result["stderr"] = errors.read(65536).decode(errors="replace")
            result["stdout"] = stdout
            if result["rejection_reason"] is None:
                response = json.loads(stdout)
                if response["operation_id"] != operation_id or response["scenario"] != scenario:
                    raise ValueError("agent acknowledgement does not identify the requested operation")
                if type(response["success"]) is not bool:
                    raise ValueError("agent success must be boolean")
                if response["success"] and response.get("identity") != agent["identity"]:
                    raise ValueError("agent acknowledgement does not identify the declared peer")
                result["success"] = status == 0 and response["success"]
                result["rejection_reason"] = None if result["success"] else response.get("rejection_reason") or f"agent_exit_{status}"
                if scenario == "gossip_two_hops" and result["success"]:
                    route = response["route"]
                    if not isinstance(route, list) or len(route) < 3 or any(not isinstance(peer, str) or not peer for peer in route) or len(set(route)) != len(route):
                        raise ValueError("gossip requires a distinct sender, relay, and receiver trace")
                    result["hops"] = len(route) - 1
                    result["route"] = route
                if result["success"] and not response.get("trace"):
                    raise ValueError("successful agent observations require a raw protocol trace")
                result["trace"] = response.get("trace")
                if result["success"] and scenario == "concurrent_sync":
                    validate_bulk_trace(result["trace"])
                if result["success"] and scenario == "gossip_two_hops":
                    receipt = result["trace"]["receipt"]
                    publication = result["trace"]["publication"]["record"]["fields"]
                    published = int(publication["unix_us"])
                    received = int(receipt["received_unix_us"])
                    if publication["event"] != "gossip_publish" or publication["message_id"] != receipt["message_id"] or publication["source"] != result["route"][0] or receipt["propagation_source"] != result["route"][1] or published < int(environment["HUB_CAPACITY_MEASUREMENT_UNIX_US"]) or received < published:
                        raise ValueError("gossip receipt does not match its measured production publication")
                    result["latency_ms"] = (received - published) / 1000
                if scenario == "committee_progress" and result["trace"]:
                    entries = result["trace"]["observations"]
                    if not isinstance(entries, list) or not 1 <= len(entries) <= 1024:
                        raise ValueError("committee observation batches must be bounded and nonempty")
                    measured = []
                    for index, observation in enumerate(entries):
                        fields = observation["record"]["fields"]
                        latency = int(fields["latency_us"])
                        ended = int(fields["unix_us"])
                        measurement = int(environment["HUB_CAPACITY_MEASUREMENT_UNIX_US"])
                        if fields["event"] != "committee_request" or observation["source"] != agent["identity"] or type(fields["success"]) is not bool or type(fields["completed"]) is not bool or (fields["success"] and not fields["completed"]) or latency < 0 or ended - latency < measurement or ended > time.time_ns() // 1000:
                            raise ValueError("committee observation does not belong to the measured request population")
                        measured.append({"scenario": scenario, "id": f"{operation_id}-{index}",
                                         "agent": agent["identity"], "argv": agent["argv"],
                                         "success": fields["success"], "latency_ms": latency / 1000,
                                         "cancelled": not fields["completed"],
                                         "rejection_reason": None if fields["success"] else "committee request failed or was cancelled",
                                         "elapsed_seconds": (ended - measurement) / 1_000_000,
                                         "trace": {"observation": observation}})
                    result["committee_observations"] = measured
    except (OSError, ValueError, KeyError, TypeError) as error:
        result["success"] = False
        result["rejection_reason"] = f"driver_validation: {error}"
    finally:
        if child is not None and child.poll() is None:
            child.kill()
            child.wait()
    result["command_latency_ms"] = (time.monotonic() - started) * 1000
    result.setdefault("latency_ms", result["command_latency_ms"])
    result["elapsed_seconds"] = time.monotonic() - origin
    return result


def validate_bulk_trace(trace):
    """Require completed primary and independent worker transfers with four matching real batches."""
    transfers = trace["transfers"]
    if trace.get("completed") is not True or len(transfers) != 3:
        raise ValueError("bulk sync must complete on all three swarms")
    roles = {transfer["swarm"] for transfer in transfers}
    if roles != {"primary", "worker-0", "worker-1"}:
        raise ValueError("bulk sync is missing an independent worker swarm")
    expected = None
    for transfer in transfers:
        if transfer.get("completed") is not True or not 0 < QUALIFY.integer(transfer["bytes"], "transfer bytes", 1) <= 64 * 1024**2:
            raise ValueError("bulk sync requires bounded nonempty completed transfers")
        if transfer["swarm"] != "primary":
            digests = transfer["batch_digests"]
            if len(digests) != 4 or len(set(digests)) != 4 or transfer["bytes"] < 4 * 32768:
                raise ValueError("worker sync requires four distinct fixture batches and real transaction bytes")
            if any(not isinstance(digest, str) or len(digest) != 66 or not digest.startswith("0x")
                   or any(character not in "0123456789abcdef" for character in digest[2:]) for digest in digests):
                raise ValueError("worker sync batch digest is malformed")
            if expected is not None and set(digests) != expected:
                raise ValueError("worker sync digest observations disagree")
            expected = set(digests)


def run(plan, manifest, output, origin):
    validate_manifest(plan, manifest)
    duration = plan["envelope"]["duration_seconds"]
    write_lock = threading.Lock()
    command_slots = threading.BoundedSemaphore(MAX_ACTIVE_COMMANDS)
    with output.open("x") as stream:
        def record(entry):
            with write_lock:
                entries = entry.pop("committee_observations", None) if entry["success"] else None
                for measured in entries or [entry]:
                    stream.write(json.dumps(measured, allow_nan=False, separators=(",", ":")) + "\n")
                stream.flush()

        def scenario_run(scenario):
            definition = manifest["scenarios"][scenario]
            target = plan["thresholds"]["scenarios"][scenario]["minimum_attempts"]
            if scenario == "committee_progress":
                target = max(target, duration * 4)
            # Commands are not queued when their bounded execution slots are all occupied.
            slots = threading.BoundedSemaphore(definition["concurrency"])
            with ThreadPoolExecutor(max_workers=definition["concurrency"]) as executor:
                futures = []
                for index in range(target):
                    burst = definition.get("burst_size", 1)
                    due = origin + (index - index % burst + definition.get("offset_fraction", 0)) * duration / target
                    time.sleep(max(0, due - time.monotonic()))
                    operation_id = f"{scenario}-{index}"
                    agent = definition["agents"][index % len(definition["agents"])]
                    if not slots.acquire(blocking=False):
                        record({"scenario": scenario, "id": operation_id, "success": False,
                                "rejection_reason": "driver_concurrency", "latency_ms": 0,
                                "elapsed_seconds": time.monotonic() - origin})
                        continue
                    if not command_slots.acquire(blocking=False):
                        slots.release()
                        record({"scenario": scenario, "id": operation_id, "success": False,
                                "rejection_reason": "driver_capacity", "latency_ms": 0,
                                "elapsed_seconds": time.monotonic() - origin})
                        continue

                    def attempt(agent=agent, operation_id=operation_id):
                        try:
                            record(execute(agent, scenario, operation_id, origin, 30))
                        finally:
                            command_slots.release()
                            slots.release()

                    futures.append(executor.submit(attempt))
                for future in futures:
                    future.result()
                if scenario == "committee_progress":
                    time.sleep(max(0, origin + duration - time.monotonic()))
                    for index, agent in enumerate(definition["agents"]):
                        record(execute(agent, scenario, f"committee_progress-final-{index}", origin, 30))

        with ThreadPoolExecutor(max_workers=len(QUALIFY.SCENARIOS)) as executor:
            list(executor.map(scenario_run, sorted(QUALIFY.SCENARIOS)))
    # A topology file is retained beside operations, independently of agent success claims.
    topology = Path(manifest["topology_artifact"])
    if not topology.is_file() or topology.stat().st_size == 0:
        raise ValueError("missing topology artifact")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan", type=Path)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    args = parser.parse_args()
    raw_manifest = args.manifest.read_bytes()
    if hashlib.sha256(raw_manifest).hexdigest() != args.manifest_sha256:
        raise ValueError("workload manifest differs from the frozen command digest")
    frozen = QUALIFY.read_json(args.plan)
    plan = frozen["plan"]
    if frozen["plan_sha256"] != QUALIFY.digest(plan) or os.environ["HUB_CAPACITY_PLAN_SHA256"] != QUALIFY.digest(plan):
        raise ValueError("workload plan identity mismatch")
    QUALIFY.validate_plan(plan)
    run(plan, QUALIFY.read_json(args.manifest), Path(os.environ["HUB_CAPACITY_OPERATIONS"]),
        float(os.environ["HUB_CAPACITY_ORIGIN"]))


if __name__ == "__main__":
    main()
