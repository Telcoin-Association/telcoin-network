#!/usr/bin/env python3
"""Schedule concurrent real peer-agent commands for every frozen qualification scenario."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading
import time


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("hub_qualify", ROOT / "qualify.py")
QUALIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(QUALIFY)
CONTROL_SPEC = importlib.util.spec_from_file_location("hub_control", ROOT / "control.py")
CONTROL = importlib.util.module_from_spec(CONTROL_SPEC)
CONTROL_SPEC.loader.exec_module(CONTROL)

# Leave eight control-server slots for handlers finishing timed-out commands.
MAX_ACTIVE_COMMANDS = 64


def validate_manifest(plan, manifest):
    """Bind scenario commands and peer populations before launching any workload."""
    if set(manifest["scenarios"]) != QUALIFY.SCENARIOS:
        raise ValueError("manifest must implement every required scenario")
    for scenario, definition in manifest["scenarios"].items():
        if QUALIFY.integer(definition["concurrency"], "driver concurrency", 1) > 32:
            raise ValueError("driver concurrency must not exceed thirty-two")
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


def control_arguments(argv):
    """Recognize only the source-owned adapter and its exact declared CLI form."""
    if (len(argv) < 4 or argv[0] not in {"python3", sys.executable} or
            argv[1:3] != ["-B", "-I"] or argv[3] != str(ROOT / "control.py")):
        return None
    values = {"url": None, "identity": None, "observations": None, "bulk_root": None}
    options = argv[4:]
    if len(options) % 2:
        return None
    seen = set()
    for option, value in zip(options[::2], options[1::2]):
        name = option.removeprefix("--").replace("-", "_")
        if option not in {"--url", "--identity", "--observations", "--bulk-root"} or name in seen:
            return None
        seen.add(name)
        values[name] = value
    if any(url is not None and not CONTROL.direct_http_url(url)
           for url in (values["url"], values["observations"])):
        return None
    return argparse.Namespace(**values)


def execute(agent, scenario, operation_id, origin, timeout, measurement_unix_us=None):
    """Measure one command, keeping timeouts and refusals in the operation population."""
    if scenario == "committee_progress":
        raise ValueError("committee observations require the dedicated bounded consumer")
    started = time.monotonic()
    result = {"scenario": scenario, "id": operation_id, "success": False,
              "rejection_reason": None, "agent": agent["identity"], "argv": agent["argv"],
              "driver_started_unix_us": time.time_ns() // 1000,
              "driver_execution_mode": "subprocess"}
    environment = {**os.environ, "HUB_CAPACITY_OPERATION_ID": operation_id,
                   "HUB_CAPACITY_SCENARIO": scenario,
                    "HUB_CAPACITY_MEASUREMENT_UNIX_US": str(measurement_unix_us if measurement_unix_us is not None else time.time_ns() // 1000 - int((started - origin) * 1_000_000))}
    child = None
    try:
        with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
            args = control_arguments(agent["argv"])
            if args is not None:
                result["driver_execution_mode"] = "in_process_control"
                result["driver_adapter_started_unix_us"] = time.time_ns() // 1000
                try:
                    response = CONTROL.invoke(args, environment, deadline=started + timeout)
                    encoded = json.dumps(response, allow_nan=False, separators=(",", ":")).encode()
                    CONTROL.remaining_timeout(started + timeout)
                    if len(encoded) > 65536:
                        result["rejection_reason"] = "agent_output_limit"
                    output.write(encoded[:65536])
                    status = 0
                finally:
                    result["driver_adapter_completed_unix_us"] = time.time_ns() // 1000
            else:
                child = subprocess.Popen(agent["argv"], stdout=output, stderr=errors, env=environment)
                result["driver_spawn_completed_unix_us"] = time.time_ns() // 1000
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
                result["driver_child_completed_unix_us"] = time.time_ns() // 1000
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
    except TimeoutError:
        result["success"] = False
        result["rejection_reason"] = "timeout"
    except (OSError, ValueError, KeyError, TypeError, http.client.HTTPException) as error:
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


def committee_fence(path, source, measurement, duration):
    """Read an independent bounded fence, never an observed-log maximum."""
    try:
        with path.open("rb") as stream:
            body = stream.read(32 * 1024 + 1)
    except FileNotFoundError:
        return None
    if len(body) > 32 * 1024:
        raise ValueError("committee producer fences exceed bounded storage")
    document = json.loads(body)
    cutoff = measurement + duration * 1_000_000
    if (type(document.get("version")) is not int or document.get("version") != 1 or
            document.get("measurement_start_unix_us") != measurement or
            document.get("window_end_unix_us") != cutoff):
        raise ValueError("committee producer fence measurement binding mismatch")
    fences = document["fences"]
    if not isinstance(fences, dict) or len(fences) > 2:
        raise ValueError("committee producer fences exceed bounded storage")
    fence = fences.get(source)
    if fence is None:
        return None
    if fence["source"] != source:
        raise ValueError("committee producer fence source mismatch")
    generation = fence["generation"]
    if not isinstance(generation, str) or re.fullmatch(r"[0-9a-f]{32}", generation) is None:
        raise ValueError("invalid committee producer fence generation")
    count = QUALIFY.integer(fence["allocated_request_count"], "producer fence allocation count", 0)
    if count > 2**53:
        raise ValueError("producer watermark exceeds exact native gauge integer range")
    QUALIFY.integer(fence["process_id"], "producer fence process", 1)
    QUALIFY.integer(fence["process_identity"], "producer fence process identity", 1)
    if not CONTROL.direct_http_url(fence["metrics_url"]) or not isinstance(fence["hub"], str):
        raise ValueError("invalid committee producer fence binding")
    scrape_start = fence["scrape_started_elapsed_seconds"]
    scrape_end = fence["scrape_completed_elapsed_seconds"]
    if (type(scrape_start) not in (int, float) or type(scrape_end) not in (int, float) or
            not math.isfinite(scrape_start) or not math.isfinite(scrape_end) or
            not duration <= scrape_start <= scrape_end < duration + 30 or
            QUALIFY.integer(fence["scrape_started_unix_us"], "producer fence scrape time", 1) < cutoff):
        raise ValueError("committee producer fence was not sampled within the post-cutoff drain")
    line = f'tn_primary_vote_observation_allocated{{generation="{generation}"}} {count}'
    if fence["metric_line"] != line or fence["metric_line_sha256"] != hashlib.sha256(line.encode()).hexdigest():
        raise ValueError("committee producer fence metric provenance mismatch")
    return fence


def consume_committee(agent, origin, measurement_unix_us, duration, record):
    """One consumer owns each hub queue, with bounded pending state and the existing tail budget."""
    args = control_arguments(agent["argv"])
    if args is None or args.identity != agent["identity"] or not args.observations:
        raise ValueError("committee consumer must bind the native observation adapter and hub")
    end = origin + duration
    deadline = end + 30
    pending = {}
    fence_path = Path(os.environ["HUB_CAPACITY_COMMITTEE_FENCES"])
    fence = None
    started_count = terminal_count = 0
    poll = 0
    while time.monotonic() < deadline:
        response = CONTROL.post(args.observations, {
            "scenario": "committee_progress", "identity": agent["identity"],
            "not_before_unix_us": measurement_unix_us,
        }, deadline=min(deadline, time.monotonic() + 30))
        if response.get("success") is not True or response.get("collector_status") not in {"empty", "batch"}:
            raise ValueError(f"committee collector failed: {response.get('rejection_reason', 'invalid acknowledgement')}")
        trace = response["trace"]
        entries = trace["observations"]
        if not isinstance(entries, list) or len(entries) > 32 or bool(entries) != (response["collector_status"] == "batch"):
            raise ValueError("invalid bounded committee batch")
        for observation in entries:
            if observation["source"] != agent["identity"]:
                raise ValueError("committee observation source mismatch")
            fields = observation["record"]["fields"]
            identity = QUALIFY.committee_identity(fields)
            started = QUALIFY.native_integer(fields, "started_unix_us", 1)
            if not measurement_unix_us <= started < measurement_unix_us + duration * 1_000_000:
                continue
            if fields["event"] == "committee_request_start":
                if identity in pending or len(pending) >= 1024:
                    raise ValueError("committee pending start duplicate or capacity exhausted")
                pending[identity] = (started, fields["process_id"], fields["header"], fields["peer"])
                started_count += 1
            elif fields["event"] == "committee_request":
                expected = (started, fields["process_id"], fields["header"], fields["peer"])
                if pending.pop(identity, None) != expected:
                    raise ValueError("committee terminal has no matching native start")
                record(QUALIFY.committee_operation(observation, measurement_unix_us, duration))
                terminal_count += 1
            else:
                raise ValueError("unknown committee observation event")
        QUALIFY.integer(trace["offset"], "follower offset", 0)
        QUALIFY.integer(trace["size"], "follower size", 0)
        QUALIFY.integer(trace["queued"], "follower queue", 0)
        if type(trace["caught_up"]) is not bool or trace["caught_up"] != (trace["offset"] == trace["size"]):
            raise ValueError("invalid follower position")
        allocation = trace["allocation"]
        through = QUALIFY.integer(allocation["started_through"], "native start coverage", 0)
        holes = QUALIFY.integer(trace["allocation_holes"], "native out-of-order start count", 0)
        if holes > 1024:
            raise ValueError("committee start coverage allocation exhausted")
        if allocation["generation"] is None:
            if allocation["process_id"] is not None or through or holes:
                raise ValueError("unbound committee start coverage")
        elif (not isinstance(allocation["generation"], str) or
              re.fullmatch(r"[0-9a-f]{32}", allocation["generation"]) is None):
            raise ValueError("invalid committee coverage generation")
        else:
            QUALIFY.integer(allocation["process_id"], "native coverage process", 1)
        candidate = committee_fence(fence_path, agent["identity"], measurement_unix_us, duration)
        if fence is not None and candidate != fence:
            raise ValueError("frozen committee producer fence changed or disappeared")
        if candidate is not None:
            fence = candidate
            if allocation["generation"] is not None and (allocation["generation"], allocation["process_id"]) != (fence["generation"], fence["process_id"]):
                raise ValueError("committee producer fence generation or process mismatch")
        now = time.monotonic()
        if now >= deadline:
            break
        covered = fence is not None and through >= fence["allocated_request_count"]
        complete = now >= end and covered and not pending and trace["queued"] == 0 and trace["caught_up"]
        record({"kind": "collector_telemetry", "scenario": "committee_progress",
                "source": agent["identity"], "poll": poll,
                "state": "complete" if complete else response["collector_status"],
                "elapsed_seconds": time.monotonic() - origin,
                "measurement_start_unix_us": measurement_unix_us,
                "started": started_count, "terminals": terminal_count,
                "pending": len(pending), "follower": {key: trace[key] for key in ("offset", "size", "queued", "caught_up")},
                "allocation": allocation, "allocation_holes": holes, "producer_fence": fence})
        poll += 1
        if complete:
            return
    raise ValueError("committee observation drain incomplete within thirty seconds")


def run(plan, manifest, output, origin, measurement_unix_us=None):
    validate_manifest(plan, manifest)
    duration = plan["envelope"]["duration_seconds"]
    if measurement_unix_us is None:
        measurement_unix_us = time.time_ns() // 1000 - int((time.monotonic() - origin) * 1_000_000)
    write_lock = threading.Lock()
    command_slots = threading.BoundedSemaphore(MAX_ACTIVE_COMMANDS)
    with output.open("x") as stream:
        def record(entry):
            with write_lock:
                stream.write(json.dumps(entry, allow_nan=False, separators=(",", ":")) + "\n")
                stream.flush()

        def scenario_run(scenario):
            definition = manifest["scenarios"][scenario]
            if scenario == "committee_progress":
                agents = definition["agents"]
                if len(agents) != 2 or len({agent["identity"] for agent in agents}) != 2 or not 2 <= definition["concurrency"] <= 4:
                    raise ValueError("committee collection requires two distinct hub consumers under cap four")
                def consume(agent):
                    with command_slots:
                        consume_committee(agent, origin, measurement_unix_us, duration, record)
                with ThreadPoolExecutor(max_workers=definition["concurrency"]) as executor:
                    consumers = [executor.submit(consume, agent)
                                 for agent in agents]
                    for future in consumers:
                        future.result()
                return
            target = plan["thresholds"]["scenarios"][scenario]["minimum_attempts"]
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
                            record(execute(agent, scenario, operation_id, origin, 30, measurement_unix_us))
                        finally:
                            command_slots.release()
                            slots.release()

                    futures.append(executor.submit(attempt))
                for future in futures:
                    future.result()

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
        float(os.environ["HUB_CAPACITY_ORIGIN"]), int(os.environ["HUB_CAPACITY_MEASUREMENT_UNIX_US"]))


if __name__ == "__main__":
    main()
