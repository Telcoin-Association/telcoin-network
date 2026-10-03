#!/usr/bin/env python3
"""Run and retain a frozen baseline/candidate qualification on an isolated Linux Docker network."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import shlex
import signal
import subprocess
import sys
import time
import uuid


ROOT = Path(__file__).resolve().parent


def runner_resources(name):
    """Declare disjoint CPU sets and memory budgets before measuring either phase."""
    if name == "github-actions":
        return {"hub_cpus": 1, "hub_memory": 3 * 1024**3, "coordinator_cpus": "2-3",
                "coordinator_cpu_count": 2, "coordinator_memory": 8 * 1024**3,
                "minimum_cpus": 4, "minimum_memory": 14 * 1024**3,
                "max_cpu_cores": 0.75, "max_rss_bytes": 2 * 1024**3}
    if name == "workstation":
        return {"hub_cpus": 4, "hub_memory": 8 * 1024**3, "coordinator_cpus": "8-11",
                "coordinator_cpu_count": 4, "coordinator_memory": 8 * 1024**3,
                "minimum_cpus": 12, "minimum_memory": 24 * 1024**3,
                "max_cpu_cores": 3, "max_rss_bytes": 4 * 1024**3}
    raise ValueError("unknown runner envelope")


def hub_cpu_set(resources, index):
    """Use the same disjoint affinity in Docker and the process binding attestation."""
    count = resources["hub_cpus"]
    return list(range(index * count, (index + 1) * count))


def digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def write_json(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, allow_nan=False, sort_keys=True, indent=2)
        stream.write("\n")


class Docker:
    def __init__(self, output, binaries, image, resources=None):
        self.output = output
        self.binaries = binaries
        self.image = image
        self.resources = resources or runner_resources("workstation")
        self.names = []
        self.network = None
        self.commands = []
        self.background = []

    def run(self, *arguments):
        self.commands.append(["docker", *arguments])
        result = subprocess.run(["docker", *arguments], check=False, capture_output=True, text=True)
        if len(result.stdout.encode()) + len(result.stderr.encode()) > 8 * 1024**2:
            raise ValueError("Docker command exceeds the finite result budget")
        if result.returncode:
            write_json(self.output / f"command-failure-{len(self.commands)}.json", {
                "argv": ["docker", *arguments], "exit_code": result.returncode,
                "stdout": result.stdout, "stderr": result.stderr})
        result.check_returncode()
        return result.stdout.strip()

    def container(self, name, ip, cpus, anchor=None, coordinator=False):
        kind = "coordinator" if coordinator else "hub"
        cpu_count = self.resources["coordinator_cpu_count" if coordinator else "hub_cpus"]
        memory = str(self.resources[kind + "_memory"])
        arguments = ["run", "-d", "--name", name, "--label", "tn.capacity.issue=1476",
                     "--network", self.network, "--ip", ip, "--cpuset-cpus", cpus,
                     "--cpus", str(cpu_count), "--memory", memory, "--memory-swap", memory,
                     "--cap-add", "NET_ADMIN", "--hostname", name,
                     "--mount", f"type=bind,source={self.output},target=/qualification",
                     "--mount", f"type=bind,source={self.output / 'source'},target=/tools,readonly",
                     "--mount", f"type=bind,source={self.binaries},target=/binaries,readonly"]
        if anchor:
            arguments += ["--pid", f"container:{anchor}"]
        if coordinator:
            # Creating network namespaces requires mount permission in this isolated coordinator.
            arguments += ["--cap-add", "SYS_ADMIN", "--security-opt", "apparmor=unconfined",
                          "--sysctl", "net.ipv4.ip_forward=1"]
        identifier = self.run(*arguments, self.image, "sleep", "infinity")
        self.names.append(identifier)
        return identifier

    def execute(self, container, *arguments):
        return self.run("exec", container, *arguments)

    def background_execute(self, container, name, *arguments):
        argv = ["docker", "exec", container, *arguments]
        self.commands.append(argv)
        log = (self.output / f"{name}-docker-exec.log").open("xb")
        process = subprocess.Popen(argv, stdout=log, stderr=log)
        self.background.append((process, log))
        return process

    def close(self):
        for identifier in reversed(self.names):
            subprocess.run(["docker", "rm", "-f", identifier], check=False, capture_output=True)
        for process, log in self.background:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            log.close()
        if self.network:
            subprocess.run(["docker", "network", "rm", self.network], check=False, capture_output=True)
        write_json(self.output / "executed-docker-commands.json", self.commands)


def wait_file(path, processes=(), timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists() and path.stat().st_size:
            return
        if any(process.poll() is not None for process in processes):
            raise ValueError(f"qualification process exited before creating {path.name}")
        time.sleep(0.1)
    raise TimeoutError(f"qualification did not create {path.name}")


def workload_manifest(population):
    """Bind each operation class to actual process identities and private control endpoints."""
    def agents(peers, gossip=False, bulk=False):
        return [{"identity": peer["identity"], "argv": ["python3", "-B", "-I", "/tools/control.py",
                 "--url", f"http://10.147.0.20:9401/peer/{peer['name']}",
                 *(["--observations", "http://127.0.0.1:9400"] if gossip else []),
                 *(["--bulk-root", "/qualification/deployment"] if bulk else [])]} for peer in peers]
    ordinary, dao = population["ordinary"], population["dao"]
    scenarios = {name: {"concurrency": concurrency, "agents": agents(peers, name == "gossip_two_hops", name == "concurrent_sync")}
                 for name, concurrency, peers in [
                     ("public_join", 2, ordinary),
                     ("shared_nat_reconnect", 2, [peer for peer in ordinary if peer["nat"]]),
                     ("gossip_two_hops", 16, ordinary), ("record_lookup", 4, ordinary),
                     ("submit_url_lookup", 4, ordinary), ("concurrent_sync", 8, ordinary),
                     ("dao_connectivity", 4, dao) ]}
    scenarios["shared_nat_reconnect"]["offset_fraction"] = 0.5
    scenarios["concurrent_sync"]["burst_size"] = 8
    scenarios["committee_progress"] = {"concurrency": 4, "agents": [
        {"identity": node["bls_key"], "argv": ["python3", "-B", "-I", "/tools/control.py",
         "--identity", node["bls_key"], "--observations", "http://127.0.0.1:9400"]}
        for node in population["validators"] if node["hub"]]}
    return {"scenarios": scenarios, "topology_artifact": "/qualification/links-initial.json"}


def stop_process(docker, container, pid_file, command_token):
    """Signal only an owned PID whose current argv still belongs to this qualification."""
    docker.execute(container, "python3", "-B", "-I", "-c",
                   "import os,pathlib,signal,sys; p=int(pathlib.Path(sys.argv[1]).read_text()); "
                   "a=pathlib.Path('/proc',str(p),'cmdline').read_bytes().split(bytes([0])); "
                   "assert sys.argv[2].encode() in a, 'owned process argv changed'; os.kill(p,signal.SIGINT)",
                   pid_file, command_token)


def stage_binaries(source, destination):
    """Verify the CI artifact's recorded digests before copying executable permissions locally."""
    source = source.resolve()
    revision = (source / "hub-capacity-revision.txt").read_text().strip()
    if len(revision) != 40 or any(char not in "0123456789abcdef" for char in revision):
        raise ValueError("CI artifact must record a full source revision")
    hashes = {}
    for line in (source / "hub-capacity-binaries.sha256").read_text().splitlines():
        expected, name = line.split(None, 1)
        relative = Path(name.strip())
        selected = Path("examples/hub-capacity-peer") if relative.name == "hub-capacity-peer" else Path(relative.name)
        if selected.name not in {"telcoin-network", "node-record-api", "hub-capacity-peer"} or selected.name in hashes:
            raise ValueError("unexpected or duplicate CI binary")
        incoming = source / selected
        if digest(incoming) != expected:
            raise ValueError("CI binary digest mismatch")
        outgoing = destination / selected
        outgoing.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(incoming, outgoing)
        outgoing.chmod(0o755)
        hashes[selected.name] = expected
    if set(hashes) != {"telcoin-network", "node-record-api", "hub-capacity-peer"}:
        raise ValueError("CI artifact is missing a qualification binary")
    return revision, hashes


def verify_source_provenance(binary_revision):
    """Record separate committed revisions and reject changes outside the qualification harness."""
    repository = Path(subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=ROOT,
        check=True, capture_output=True, text=True).stdout.strip())

    def git(*arguments):
        return subprocess.run(["git", *arguments], cwd=repository, check=True,
                              capture_output=True, text=True).stdout

    current = git("rev-parse", "HEAD").strip()
    if git("status", "--porcelain").strip():
        raise ValueError("qualification requires a clean committed worktree")
    ancestor = subprocess.run(["git", "merge-base", "--is-ancestor", binary_revision, current],
                              cwd=repository, capture_output=True)
    if ancestor.returncode:
        raise ValueError("CI binary revision must be an ancestor of the qualification revision")
    changed = sorted(filter(None, git("diff", "--name-only", "-z", binary_revision, current).split("\0")))

    def harness_only(name):
        path = Path(name)
        return (path.parent == Path("tools/hub-capacity") and path.suffix == ".py"
                or name == "docs/src/network/hub-capacity.md")

    incompatible = [name for name in changed if not harness_only(name)]
    if incompatible:
        raise ValueError("CI binaries have different source inputs: " + ", ".join(incompatible))
    return {"binary_revision": binary_revision, "qualification_revision": current,
            "qualification_only_changes": changed}


def warmup(docker, coordinator, processes):
    """Allow completed epochs to become available, retaining failures in the owned logs."""
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if any(process.poll() is not None for process in processes):
            raise ValueError("qualification process exited during warmup")
        time.sleep(1)
    docker.execute(coordinator, "curl", "--fail", "--silent", "--max-time", "5",
                   "http://10.147.0.10:9000")
    docker.execute(coordinator, "curl", "--fail", "--silent", "--max-time", "5",
                   "http://10.147.0.11:9000")


def wait_for_phase_exit(processes, peer_supervisor, timeout=30):
    """Require every recorded actor to exit and the supervisor to confirm child cleanup."""
    deadline = time.monotonic() + timeout
    while any(process.poll() is None for process in processes) and time.monotonic() < deadline:
        time.sleep(0.1)
    if any(process.poll() is None for process in processes):
        raise ValueError("owned phase processes did not stop, refusing to reuse the topology")
    if peer_supervisor is not None and peer_supervisor.poll() != 0:
        raise ValueError("peer supervisor exited unsuccessfully; inspect retained startup and cleanup logs")


def run_phase(docker, coordinator, hubs, population, phase, plan, revision):
    phase_dir = docker.output / "deployment" / phase
    container_dir = f"/qualification/deployment/{phase}"
    processes, stops = [], []
    peer_supervisor = None
    try:
        for index, node in enumerate(population["validators"]):
            command = f"{container_dir}/{node['name']}-command.json"
            container = hubs[index] if node["hub"] else coordinator
            prefix = ["ip", "netns", "exec", node["namespace"]] if node["namespace"] else []
            process = docker.background_execute(container, f"{phase}-{node['name']}", *prefix,
                         "python3", "-B", "-I", "/tools/launch.py", command)
            processes.append(process)
            wait_file(phase_dir / f"{node['name']}.pid", processes)
            wait_file(phase_dir / f"{node['name']}.jsonl", processes)
            stops.append((container, f"{container_dir}/{node['name']}.pid", "/binaries/telcoin-network"))
        peer_supervisor = docker.background_execute(coordinator, f"{phase}-peers", "python3", "-B", "-I",
            "/tools/supervise.py", "/qualification/deployment", phase, "/binaries/examples/hub-capacity-peer",
            "--ready", f"{container_dir}/peers-ready.json", "--pid-file", f"{container_dir}/supervisor.pid")
        processes.append(peer_supervisor)
        wait_file(phase_dir / "supervisor.pid", processes)
        stops.append((coordinator, f"{container_dir}/supervisor.pid", "/tools/supervise.py"))
        wait_file(phase_dir / "peers-ready.json", processes, timeout=180)
        print(f"{phase}: peer readiness reports retained; seeding the local chain", flush=True)
        docker.execute(coordinator, "python3", "-B", "-I", "/tools/traffic.py", "initial",
                       "--fixture", "/qualification/transactions.json", "--output", f"{container_dir}/initial-transactions.jsonl")
        arguments = ["python3", "-B", "-I", "/tools/observations.py", "--pid-file", f"{container_dir}/observations.pid"]
        for node in population["validators"]:
            arguments += ["--source", f"{node['bls_key']}={container_dir}/{node['name']}.jsonl"]
            if node["hub"]:
                arguments += ["--hub", node["bls_key"]]
        processes.append(docker.background_execute(coordinator, f"{phase}-observations", *arguments))
        wait_file(phase_dir / "observations.pid", processes)
        stops.append((coordinator, f"{container_dir}/observations.pid", "/tools/observations.py"))
        warmup(docker, coordinator, processes)
        print(f"{phase}: warmup completed; binding canonical batch targets", flush=True)
        docker.execute(coordinator, "python3", "-B", "-I", "/tools/traffic.py", "targets",
                       "--output", f"{container_dir}/bulk-targets.json", "--observations", f"{container_dir}/canonical-batch-observations.json")
        docker.execute(coordinator, "python3", "-B", "-I", "/tools/netns.py", "snapshot",
                       "--output", f"{container_dir}/links-before.json")
        topology = {"population": population, "network": json.loads((phase_dir / "links-before.json").read_text()),
                    "docker": [json.loads(docker.run("inspect", container))[0] for container in [coordinator, *hubs]],
                    "image": docker.image, "source": json.loads((docker.output / "source-hashes.json").read_text()),
                    "source_provenance": json.loads((docker.output / "source-provenance.json").read_text()),
                    "deployment_hashes": {str(path.relative_to(phase_dir)): digest(path)
                        for pattern in ("*-profile.json", "*-command.json", "peers/*.json", "*/node-info.yaml", "*/network-config", "ceremony/parameters.yaml", "ceremony/genesis/validators/*.yaml")
                        for path in phase_dir.glob(pattern)},
                    "peer_readiness": json.loads((phase_dir / "peers-ready.json").read_text()),
                    "transaction_fixture_sha256": digest(docker.output / "transactions.json"),
                    "initial_transactions_sha256": digest(phase_dir / "initial-transactions.jsonl"),
                    "bulk_targets": json.loads((phase_dir / "bulk-targets.json").read_text()),
                    "canonical_batch_observations_sha256": digest(phase_dir / "canonical-batch-observations.json"),
                    "linux_cpuinfo": docker.execute(coordinator, "cat", "/proc/cpuinfo"),
                    "linux_version": docker.execute(coordinator, "uname", "-a")}
        write_json(phase_dir / "topology.json", topology)
        bindings = {"topology_artifact": f"{container_dir}/topology.json",
                    "protocol_logs": [f"{container_dir}/{node['name']}.jsonl" for node in population["validators"]],
                    "workload": shlex.split(plan["adapter_command"]), "hubs": {}}
        for index, node in enumerate(population["validators"][:2]):
            command = json.loads((phase_dir / f"{node['name']}-command.json").read_text())
            bindings["hubs"][plan["hubs"][index]] = {
                "pid": int((phase_dir / f"{node['name']}.pid").read_text()), "revision": revision,
                "argv": command["argv"], "cpu_affinity": hub_cpu_set(docker.resources, index),
                "profile_path": f"{container_dir}/{node['name']}/network-config",
                "metrics_url": f"http://{node['ip']}:9000", "progress": {"name": "tn_engine_canonical_height"}}
        write_json(phase_dir / "bindings.json", bindings)
        processes.append(docker.background_execute(coordinator, f"{phase}-transactions", "python3", "-B", "-I",
            "/tools/traffic.py", "stream", "--fixture", "/qualification/transactions.json",
            "--output", f"{container_dir}/stream-transactions.jsonl", "--pid-file", f"{container_dir}/transactions.pid"))
        wait_file(phase_dir / "transactions.pid", processes)
        stops.append((coordinator, f"{container_dir}/transactions.pid", "/tools/traffic.py"))
        print(f"{phase}: starting the frozen 600-second concurrent measurement", flush=True)
        docker.execute(coordinator, "python3", "-B", "-I", "/tools/collect.py", "/qualification/plan.json",
                       f"{container_dir}/bindings.json", "--phase", phase, "--output", f"/qualification/{phase}-evidence")
        if any(process.poll() not in (None, 0) for process in processes):
            raise ValueError("qualification traffic process failed during measurement")
        verify_transactions(docker.output / "transactions.json", phase_dir / "stream-transactions.jsonl")
        docker.execute(coordinator, "python3", "-B", "-I", "/tools/netns.py", "snapshot",
                       "--output", f"{container_dir}/links-after.json")
    finally:
        for container, pid_file, token in reversed(stops):
            try:
                stop_process(docker, container, pid_file, token)
            except (subprocess.CalledProcessError, ValueError):
                pass
        wait_for_phase_exit(processes, peer_supervisor)


def verify_transactions(fixture, observations):
    """Require the complete signed workload, including exact acknowledgements, before scoring."""
    inputs = json.loads(fixture.read_text())
    rows = [json.loads(line) for line in observations.read_text().splitlines()]
    if len(rows) != 384 or any(
            row.get("nonce") != nonce or row.get("success") is not True
            or row.get("transaction_hash") != inputs["transaction_hashes"][nonce]
            or row.get("raw_sha256") != hashlib.sha256(inputs["transactions"][nonce].encode()).hexdigest()
            for nonce, row in zip(range(128, 512), rows)):
        raise ValueError("qualification did not acknowledge all declared measurement transactions")


def execute_qualification(args):
    """Freeze inputs before startup and remove only resources created by this invocation."""
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    source = output / "source"
    source.mkdir()
    for path in ROOT.iterdir():
        if path.is_file() and path.suffix in {".py", ".json"}:
            shutil.copyfile(path, source / path.name)
    write_json(output / "source-hashes.json", {path.name: digest(path) for path in source.iterdir()})
    revision, hashes = stage_binaries(args.binaries, output / "bin")
    provenance = verify_source_provenance(revision)
    write_json(output / "source-provenance.json", provenance)
    resources = runner_resources(args.runner_envelope)
    docker = Docker(output, output / "bin", args.image, resources)
    try:
        information = json.loads(docker.run("info", "--format", "{{json .}}"))
        if information["NCPU"] < resources["minimum_cpus"] or information["MemTotal"] < resources["minimum_memory"]:
            raise ValueError("Docker host is smaller than the declared runner envelope")
        image = json.loads(docker.run("image", "inspect", args.image))[0]
        if image["Architecture"] != "arm64":
            raise ValueError("qualification CI binaries require an arm64 Linux runtime")
        docker.image = image["Id"]
        write_json(output / "docker-runtime.json", {"information": information, "image": image})
        network = "tn-capacity-1476-" + uuid.uuid4().hex[:12]
        docker.run("network", "create", "--internal", "--subnet", "10.147.0.0/16", "--gateway", "10.147.0.254",
                   "--label", "tn.capacity.issue=1476", network)
        docker.network = network
        coordinator = docker.container(network + "-coordinator", "10.147.0.20", resources["coordinator_cpus"], coordinator=True)
        hubs = [docker.container(network + f"-hub-{index}", f"10.147.0.{10 + index}",
                    ",".join(map(str, hub_cpu_set(resources, index))), anchor=coordinator)
                for index in range(2)]
        for hub in hubs:
            for subnet in (1, 3, 4):
                docker.execute(hub, "ip", "route", "add", f"10.147.{subnet}.0/24", "via", "10.147.0.20")
            docker.execute(hub, "tc", "qdisc", "replace", "dev", "eth0", "root", "netem", "limit", "1024",
                           "rate", "25mbit", "delay", "25ms", "loss", "0.1%")
        docker.execute(coordinator, "python3", "-B", "-I", "/tools/netns.py", "setup", "--output", "/qualification/links-initial.json")
        docker.execute(coordinator, "python3", "-B", "-I", "/tools/prepare.py", "initialize", "/qualification/deployment",
                       "/binaries/telcoin-network", "/binaries/examples/hub-capacity-peer")
        docker.execute(coordinator, "python3", "-B", "-I", "/tools/prepare.py", "phase", "/qualification/deployment", "baseline",
                       "/binaries/telcoin-network", "/tools/profile-v1.json")
        population = json.loads((output / "deployment/population.json").read_text())
        cast = shutil.which("cast")
        if not cast:
            raise ValueError("qualification requires cast for offline chain-4476 transaction signing")
        subprocess.run([sys.executable, "-B", "-I", str(ROOT / "traffic.py"), "create", "--cast", cast,
                        "--output", str(output / "transactions.json")], check=True, timeout=300)
        manifest = workload_manifest(population)
        manifest["qualification_revision"] = provenance["qualification_revision"]
        manifest["transaction_fixture_sha256"] = digest(output / "transactions.json")
        manifest["transaction_workload"] = {"initial_count": 128, "initial_interval_seconds": 0.5,
                                            "measurement_count": 384, "measurement_seconds": 600}
        write_json(output / "manifest.json", manifest)
        docker.execute(coordinator, "python3", "-B", "-I", "/tools/qualify.py", "template", "--output", "/qualification/declaration-template.json")
        plan = json.loads((output / "declaration-template.json").read_text())
        for phase in ("baseline", "candidate"):
            plan[phase].update({"revision": revision, "binary_sha256": hashes,
                "qualification_revision": provenance["qualification_revision"],
                "build_command": "cargo +1.94 build --locked -p telcoin-network -p tn-node-record-api; cargo +1.94 build --locked -p tn-node-record-api --example hub-capacity-peer",
                "profile": json.loads((output / f"deployment/baseline/{phase}-profile.json").read_text())})
        plan["envelope"].update({"cpus_per_hub": resources["hub_cpus"], "ram_bytes_per_hub": resources["hub_memory"]})
        plan["thresholds"].update({"max_cpu_cores": resources["max_cpu_cores"], "max_rss_bytes": resources["max_rss_bytes"]})
        affinity = [hub_cpu_set(resources, index) for index in range(2)]
        plan["envelope"]["hardware"] = f"Docker Linux arm64, {information['NCPU']} CPUs, {information['MemTotal']} bytes RAM; image {docker.image}; preset {args.runner_envelope}; hub CPUs {affinity}; coordinator CPUs {resources['coordinator_cpus']}"
        plan["envelope"]["network_setup"] = "docker-run.py and netns.py at the recorded source revision; isolated internal bridge; 25 Mbit/s and 25 ms netem per participant egress, 0.1 percent loss; sixteen namespace peers share kernel SNAT at 10.147.0.20"
        plan["adapter_command"] = shlex.join(["python3", "-B", "-I", "/tools/workload.py", "/qualification/plan.json",
                                             "/qualification/manifest.json", "--manifest-sha256", digest(output / "manifest.json")])
        write_json(output / "declaration.json", plan)
        docker.execute(coordinator, "python3", "-B", "-I", "/tools/qualify.py", "freeze", "/qualification/declaration.json", "--output", "/qualification/plan.json")
        print(f"Frozen {args.runner_envelope} envelope and workload at {provenance['qualification_revision']}", flush=True)
        for phase in ("baseline", "candidate"):
            print(f"{phase}: starting the declared topology", flush=True)
            if phase == "candidate":
                docker.execute(coordinator, "python3", "-B", "-I", "/tools/prepare.py", "phase", "/qualification/deployment", phase,
                               "/binaries/telcoin-network", "/tools/profile-v1.json")
            # The host publishes runtime metadata here; key files keep their existing ownership.
            docker.execute(coordinator, "chown", f"{os.getuid()}:{os.getgid()}",
                           f"/qualification/deployment/{phase}")
            run_phase(docker, coordinator, hubs, population, phase, plan, revision)
        docker.execute(coordinator, "python3", "-B", "-I", "/tools/qualify.py", "score", "/qualification/plan.json",
                       "/qualification/baseline-evidence/evidence.json", "/qualification/candidate-evidence/evidence.json",
                       "--output", "/qualification/report.json")
    finally:
        docker.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--image", default="tn-capacity-1476-runtime:ubuntu24")
    parser.add_argument("--runner-envelope", choices=("workstation", "github-actions"), default="workstation")
    args = parser.parse_args()
    execute_qualification(args)


if __name__ == "__main__":
    main()
