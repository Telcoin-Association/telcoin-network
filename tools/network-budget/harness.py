#!/usr/bin/env python3
"""Run baseline and candidate calibration phases over ssh from a JSON inventory."""

import argparse
import itertools
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time
from urllib.request import urlopen

from capture import expected_down

BUILDS = ("baseline", "candidate")
PHASES = ("steady", "catch-up", "reconnect", "hostile", "mixed")
# These phases need an operator-supplied generator. No generator ships with this tool.
GENERATOR_PHASES = ("hostile", "mixed")
BUDGET_FIELDS = ("swarm_count", "max_established_connections", "max_established_connections_per_peer",
                 "max_inbound_streams", "max_receive_credit_bytes")
CAPTURE = Path(__file__).resolve().with_name("capture.py")


def spawn(argv):
    """Start one local command. Remote work goes through ssh and scp."""
    return subprocess.Popen(argv, stdout=subprocess.PIPE, text=True)


def metrics_ready(url):
    """True when the metrics endpoint answers HTTP 200 within two seconds."""
    try:
        with urlopen(url, timeout=2) as response:
            return response.status == 200
    except OSError:
        return False


class Planned:
    """A command that plan mode prints and does not run."""

    returncode = 0

    def communicate(self):
        return "{}", None


def printing(argv):
    print(shlex.join(argv))
    return Planned()


class Harness:
    """Build every command from the inventory. The runner, sleep, writer, reader, clock and probe are injectable for tests."""

    def __init__(self, inventory, path, runner=spawn, sleep=time.sleep, write=lambda target, text: Path(target).write_text(text),
                 read=lambda target: Path(target).read_text(), clock=time.time, probe=metrics_ready):
        names = [host["name"] for host in inventory["hosts"]]
        generator = inventory.get("generator")
        if (not names or len(set(names)) != len(names) or inventory["catch_up_node"] not in names
                or (generator is not None and ("{target}" not in generator or not inventory.get("hostile_targets")
                                               or not set(inventory["hostile_targets"]) <= set(names)))):
            raise ValueError("inventory needs unique hosts and a known catch_up_node; "
                             "a generator needs {target} and known hostile_targets")
        if any(type(inventory.get(field, 60)) is not int or inventory.get(field, 60) <= 0 for field in ("ready_timeout_secs", "stop_timeout_secs")):
            raise ValueError("ready_timeout_secs and stop_timeout_secs must be positive integers")
        self.inventory, self.path, self.runner, self.sleep, self.write = inventory, Path(path).resolve(), runner, sleep, write
        self.read, self.clock, self.probe = read, clock, probe
        self.base = self.path.parent
        self.hosts = {host["name"]: host for host in inventory["hosts"]}

    def call(self, argv):
        """Run a command to completion. A nonzero exit stops the harness."""
        process = self.runner(argv)
        stdout, _ = process.communicate()
        if process.returncode != 0:
            raise RuntimeError(f"command failed ({process.returncode}): {shlex.join(argv)}")
        return stdout

    def ssh(self, host, command):
        return self.call(["ssh", host["ssh"], command])

    def setup(self):
        """Generate keys on each host, make genesis on the first host, and copy genesis to every host."""
        binary, work, first = self.inventory["binary"]["baseline"], self.base / "work", self.inventory["hosts"][0]
        genesis = self.inventory["genesis_dir"]
        self.call(["mkdir", "-p", str(work / "validators")])
        for host in self.inventory["hosts"]:
            self.ssh(host, shlex.join([binary, "keytool", "generate", "validator", "--datadir", host["datadir"], "--address", host["address"]]))
            self.call(["scp", f"{host['ssh']}:{host['datadir']}/node-info.yaml", str(work / "validators" / f"{host['name']}.yaml")])
        self.ssh(first, shlex.join(["mkdir", "-p", f"{genesis}/genesis/validators"]))
        for host in self.inventory["hosts"]:
            self.call(["scp", str(work / "validators" / f"{host['name']}.yaml"), f"{first['ssh']}:{genesis}/genesis/validators/{host['name']}.yaml"])
        self.ssh(first, shlex.join([binary, "genesis", "--datadir", genesis, *self.inventory["genesis_args"]]))
        shared = (("genesis/genesis.yaml", "genesis"), ("genesis/committee.yaml", "genesis"), ("parameters.yaml", "."))
        for source, _ in shared:
            self.call(["scp", f"{first['ssh']}:{genesis}/{source}", str(work / Path(source).name)])
        for host in self.inventory["hosts"]:
            self.ssh(host, shlex.join(["mkdir", "-p", f"{host['datadir']}/genesis"]))
            for source, target in shared:
                self.call(["scp", str(work / Path(source).name), f"{host['ssh']}:{host['datadir']}/{target}/{Path(source).name}"])
        return 0

    def network_config(self, build):
        """The baseline keeps the template. The candidate appends process_budget to it."""
        text = (self.base / self.inventory["network_config"]).read_text()
        if "process_budget" in text:
            raise ValueError("the network_config template must not set process_budget")
        if build == "baseline":
            return text
        budget = self.inventory["process_budget"]
        if set(budget) != set(BUDGET_FIELDS) or any(type(budget[field]) is not int or budget[field] <= 0 for field in BUDGET_FIELDS):
            raise ValueError(f"process_budget needs positive integers {', '.join(BUDGET_FIELDS)}")
        if budget["swarm_count"] != 1 + self.inventory["workers_per_node"]:
            raise ValueError("process_budget.swarm_count must equal one primary plus workers_per_node")
        return text.rstrip("\n") + "\nprocess_budget:\n" + "".join(f"  {field}: {budget[field]}\n" for field in BUDGET_FIELDS)

    def begin(self, host, binary):
        """Start the node in the background. Refuse when the pid file names a live process. Append to node.log
        after a start marker, so the log of an earlier start in the same phase stays readable."""
        node = shlex.join([binary, "node", "--datadir", host["datadir"], *host.get("node_args", self.inventory["node_args"])])
        log, pid = shlex.quote(f"{host['datadir']}/node.log"), shlex.quote(f"{host['datadir']}/node.pid")
        self.ssh(host, f'if [ -f {pid} ] && kill -0 "$(cat {pid})" 2>/dev/null; then echo "node already running" >&2; exit 1; fi; '
                       f'echo "=== harness start $(date -u +%Y-%m-%dT%H:%M:%SZ) ===" >> {log}; '
                       f"nohup {node} >> {log} 2>&1 < /dev/null & echo $! > {pid}")

    def stop(self, host):
        """Stop the node: SIGTERM, wait up to stop_timeout_secs, then SIGKILL. Keep the pid file until the process
        is gone. A process that survives SIGKILL fails the harness."""
        pid = shlex.quote(f"{host['datadir']}/node.pid")
        wait = self.inventory.get("stop_timeout_secs", 60)
        self.ssh(host, f'if [ -f {pid} ]; then p="$(cat {pid})"; kill "$p" 2>/dev/null; '
                       f'for _ in $(seq {wait}); do kill -0 "$p" 2>/dev/null || break; sleep 1; done; '
                       f'if kill -0 "$p" 2>/dev/null; then kill -9 "$p"; sleep 1; fi; '
                       f'if kill -0 "$p" 2>/dev/null; then echo "node $p survived SIGKILL" >&2; exit 1; fi; rm -f {pid}; fi')

    def await_metrics(self, host):
        """Poll the metrics_url until it answers. Past ready_timeout_secs the harness stops."""
        timeout = self.inventory.get("ready_timeout_secs", 60)
        deadline = self.clock() + timeout
        polls = itertools.takewhile(lambda _: self.clock() < deadline, itertools.count())
        if not any(self.probe(host["metrics_url"]) or self.sleep(1) for _ in polls):
            raise RuntimeError(f"{host['name']} metrics not ready in {timeout} s: {host['metrics_url']}")

    def restart(self, host, binary):
        """Restart one node during the capture. Return the window in which its scrapes are expected to fail."""
        self.sleep(self.inventory["reconnect_gap_secs"])
        start = self.clock()
        self.stop(host)
        self.begin(host, binary)
        self.await_metrics(host)
        return {"node": host["name"], "start_unix_seconds": start, "end_unix_seconds": self.clock()}

    def generator(self, host):
        return shlex.split(self.inventory["generator"].format(target=host["multiaddr"]))

    def phases(self):
        """The phases this inventory can run. Without a generator, the hostile and mixed phases stay pending."""
        return tuple(phase for phase in PHASES if "generator" in self.inventory or phase not in GENERATOR_PHASES)

    def capture_manifest(self, build, phase):
        inventory = self.inventory
        return {
            "revision": inventory["revision"][build],
            "build_command": inventory["build_command"][build],
            "topology": {"validators": len(inventory["hosts"]), "workers_per_node": inventory["workers_per_node"],
                         "cpus_per_node": inventory["cpus_per_node"], "ram_bytes_per_node": inventory["ram_bytes_per_node"]},
            "nodes": [{key: host[key] for key in ("name", "metrics_url", "rpc_url") if key in host} for host in inventory["hosts"]],
            "artifacts": [str(self.base / path) for path in inventory["artifacts"]],
            "workload": f"harness.py run --build {build} --phase {phase}; inventory {self.path}; "
                        f"generator {inventory.get('generator', 'none')}",
            "decisions": inventory["decisions"],
        }

    def run(self, build, phase, output):
        """Run one phase for one build and capture it into output/build/phase."""
        if phase not in self.phases():
            raise ValueError(f"phase {phase} needs a generator in the inventory; the phase stays pending")
        target = Path(output).resolve() / build / phase
        binary, hosts = self.inventory["binary"][build], self.inventory["hosts"]
        config, staged = self.network_config(build), target.parent / f"{phase}.network-config.yaml"
        self.call(["mkdir", "-p", str(target.parent)])
        self.write(staged, config)
        for host in hosts:
            self.stop(host)
            self.call(["scp", str(staged), f"{host['ssh']}:{host['datadir']}/network-config"])
        lagging = self.hosts[self.inventory["catch_up_node"]] if phase in ("catch-up", "mixed") else None
        for host in hosts:
            if host is not lagging:
                self.begin(host, binary)
        extra = {"build": build, "phase": phase}
        if lagging is not None:
            self.sleep(self.inventory["catch_up_pause_secs"])
            self.begin(lagging, binary)
            extra["catch_up_node"] = lagging["name"]
        # Every endpoint answers before the capture starts, so no scrape failure is startup noise.
        for host in hosts:
            self.await_metrics(host)
        manifest = target.parent / f"{phase}.capture.json"
        self.write(manifest, json.dumps(self.capture_manifest(build, phase), indent=2) + "\n")
        capture = self.runner([sys.executable, str(CAPTURE), str(manifest), str(target), "--phase", phase,
                               "--samples", str(self.inventory["samples"]), "--interval", str(self.inventory["interval"])])
        if phase == "reconnect":
            extra["expected_down"] = [self.restart(host, binary) for host in hosts]
        if phase in GENERATOR_PHASES:
            extra["generator"] = [json.loads(self.call(self.generator(self.hosts[name]))) for name in self.inventory["hostile_targets"]]
        capture.communicate()
        extra["capture_exit"] = capture.returncode
        for host in hosts:
            self.stop(host)
        self.write(target / "phase.json", json.dumps(extra, indent=2, sort_keys=True) + "\n")
        # capture.py exits nonzero on any failed scrape. Failures inside an expected-down window are planned.
        return int(capture.returncode != 0 and len(self.unexpected_failures(target, extra.get("expected_down", []))) > 0)

    def unexpected_failures(self, target, windows):
        """The scrape failures in result.json that fall outside every expected-down window."""
        failures = json.loads(self.read(target / "result.json")).get("failures", [])
        return [failure for failure in failures if not expected_down(failure, windows)]


def main(argv=None):
    """Plan prints every command. Setup and run execute them."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inventory", type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan", help="print every setup and run command, run nothing")
    plan.add_argument("--output", type=Path, required=True)
    commands.add_parser("setup", help="generate keys and genesis on the hosts")
    run = commands.add_parser("run", help="run and capture one phase for one build")
    run.add_argument("--build", required=True, choices=BUILDS)
    run.add_argument("--phase", required=True, choices=PHASES)
    run.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        inventory = json.loads(args.inventory.read_text())
        if args.command == "plan":
            harness = Harness(inventory, args.inventory, runner=printing,
                              sleep=lambda seconds: print(f"sleep {seconds}"), write=lambda target, _: print(f"write {target}"),
                              probe=lambda url: print(f"probe {url}") is None)
            harness.setup()
            return max(harness.run(build, phase, args.output) for build in BUILDS for phase in harness.phases())
        harness = Harness(inventory, args.inventory)
        return harness.setup() if args.command == "setup" else harness.run(args.build, args.phase, args.output)
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        parser.exit(1, f"harness failed: {error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
