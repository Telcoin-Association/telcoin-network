"""Exercise binary attestation and deployment rejection without synthetic qualification results."""

import hashlib
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from unittest.mock import Mock


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


RUNNER = load("capacity_runner", "docker-run.py")
COLLECT = load("capacity_deployment_collect", "collect.py")


class DeploymentTests(unittest.TestCase):
    def test_measurement_requires_every_exact_transaction_acknowledgement(self):
        transactions = [f"signed-{nonce}" for nonce in range(512)]
        hashes = [f"0x{nonce:064x}" for nonce in range(512)]
        complete = [{"nonce": nonce, "success": True, "transaction_hash": hashes[nonce],
                     "raw_sha256": hashlib.sha256(transactions[nonce].encode()).hexdigest()}
                    for nonce in range(128, 512)]
        with tempfile.TemporaryDirectory() as directory:
            fixture, output = Path(directory) / "fixture.json", Path(directory) / "stream.jsonl"
            fixture.write_text(json.dumps({"transactions": transactions, "transaction_hashes": hashes}))
            output.write_text("".join(json.dumps(row) + "\n" for row in complete))
            RUNNER.verify_transactions(fixture, output)
            for field, invalid in (("success", False), ("nonce", 510),
                                   ("transaction_hash", hashes[510]), ("raw_sha256", "wrong")):
                with self.subTest(field=field):
                    rows = copy.deepcopy(complete)
                    rows[-1][field] = invalid
                    output.write_text("".join(json.dumps(row) + "\n" for row in rows))
                    with self.assertRaisesRegex(ValueError, "did not acknowledge"):
                        RUNNER.verify_transactions(fixture, output)
            output.write_text("".join(json.dumps(row) + "\n" for row in complete[:-1]))
            with self.assertRaisesRegex(ValueError, "did not acknowledge"):
                RUNNER.verify_transactions(fixture, output)

    def test_phase_cleanup_requires_stopped_actors_and_successful_peer_cleanup(self):
        actor = mock.Mock()
        supervisor = mock.Mock()
        actor.poll.return_value = 130
        supervisor.poll.return_value = 0
        RUNNER.wait_for_phase_exit([actor, supervisor], supervisor, timeout=0)
        supervisor.poll.return_value = 1
        with self.assertRaisesRegex(ValueError, "failed to reap"):
            RUNNER.wait_for_phase_exit([actor, supervisor], supervisor, timeout=0)
        supervisor.poll.return_value = 0
        actor.poll.return_value = None
        with self.assertRaisesRegex(ValueError, "refusing to reuse"):
            RUNNER.wait_for_phase_exit([actor, supervisor], supervisor, timeout=0)

    def test_runner_envelopes_reserve_disjoint_cpu_and_memory_budgets(self):
        for name in ("workstation", "github-actions"):
            with self.subTest(name=name):
                resources = RUNNER.runner_resources(name)
                first, second = (set(RUNNER.hub_cpu_set(resources, index)) for index in range(2))
                start, end = map(int, resources["coordinator_cpus"].split("-"))
                coordinator = set(range(start, end + 1))
                self.assertFalse(first & second or first & coordinator or second & coordinator)
                self.assertEqual(first | second | coordinator, set(range(resources["minimum_cpus"])))
                self.assertEqual(2 * resources["hub_memory"] + resources["coordinator_memory"], resources["minimum_memory"])
                self.assertLess(resources["max_cpu_cores"], resources["hub_cpus"])
                self.assertLess(resources["max_rss_bytes"], resources["hub_memory"])
        with self.assertRaisesRegex(ValueError, "unknown runner"):
            RUNNER.runner_resources("unmeasured")

    def test_ci_container_limits_match_the_declared_roles(self):
        resources = RUNNER.runner_resources("github-actions")
        docker = RUNNER.Docker(Path("/owned"), Path("/bin"), "runtime", resources)
        docker.run = Mock(return_value="owned-container")
        for coordinator, cpus, count, memory in ((False, "0", "1", 3 * 1024**3),
                                                 (True, "2-3", "2", 8 * 1024**3)):
            with self.subTest(coordinator=coordinator):
                docker.container("owned", "10.147.0.10", cpus, coordinator=coordinator)
                arguments = docker.run.call_args.args
                for flag, expected in (("--cpuset-cpus", cpus), ("--cpus", count),
                                       ("--memory", str(memory)), ("--memory-swap", str(memory))):
                    self.assertEqual(arguments[arguments.index(flag) + 1], expected)
                self.assertEqual("apparmor=unconfined" in arguments, coordinator)

    def test_ci_process_attestation_rejects_a_larger_quota(self):
        with tempfile.TemporaryDirectory() as temporary:
            proc = Path(temporary)
            directory = proc / "47"
            cgroup = directory / "root/sys/fs/cgroup"
            cgroup.mkdir(parents=True)
            (directory / "cmdline").write_bytes(b"/binary\0node\0")
            (cgroup / "memory.max").write_text(str(3 * 1024**3))
            (cgroup / "cpu.max").write_text("100000 100000")
            binding = {"pid": 47, "argv": ["/binary", "node"], "cpu_affinity": [1]}
            envelope = {"ram_bytes_per_hub": 3 * 1024**3, "cpus_per_hub": 1}
            COLLECT.validate_process(binding, envelope, proc, lambda _pid: {1})
            (cgroup / "cpu.max").write_text("200000 100000")
            with self.assertRaisesRegex(ValueError, "CPU quota"):
                COLLECT.validate_process(binding, envelope, proc, lambda _pid: {1})

    def test_changed_ci_binary_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "ci"
            source.mkdir()
            (source / "hub-capacity-revision.txt").write_text("a" * 40)
            records = []
            for name in ("telcoin-network", "node-record-api", "examples/hub-capacity-peer"):
                path = source / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(name.encode())
                records.append(hashlib.sha256(path.read_bytes()).hexdigest() + "  target/debug/" + name)
            (source / "hub-capacity-binaries.sha256").write_text("\n".join(records))
            revision, hashes = RUNNER.stage_binaries(source, Path(temporary) / "valid")
            self.assertEqual(revision, "a" * 40)
            self.assertEqual(len(hashes), 3)
            (source / "telcoin-network").write_bytes(b"different executable")
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                RUNNER.stage_binaries(source, Path(temporary) / "invalid")

    def test_changed_running_deployment_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            proc = Path(temporary)
            directory = proc / "47"
            cgroup = directory / "root/sys/fs/cgroup"
            cgroup.mkdir(parents=True)
            (directory / "cmdline").write_bytes(b"/binary\0node\0--datadir\0/owned\0")
            (cgroup / "memory.max").write_text(str(8 * 1024**3))
            (cgroup / "cpu.max").write_text("400000 100000")
            binding = {"pid": 47, "argv": ["/binary", "node", "--datadir", "/owned"], "cpu_affinity": [0, 1, 2, 3]}
            envelope = {"ram_bytes_per_hub": 8 * 1024**3, "cpus_per_hub": 4}
            COLLECT.validate_process(binding, envelope, proc, lambda _pid: {0, 1, 2, 3})
            with self.assertRaisesRegex(ValueError, "CPU affinity"):
                COLLECT.validate_process(binding, envelope, proc, lambda _pid: {0, 1})
            for filename, changed, message in (("memory.max", "max", "invalid literal"),
                                              ("memory.max", str(4 * 1024**3), "memory limit"),
                                              ("cpu.max", "200000 100000", "CPU quota")):
                path = cgroup / filename
                previous = path.read_text()
                path.write_text(changed)
                with self.assertRaisesRegex(ValueError, message):
                    COLLECT.validate_process(binding, envelope, proc, lambda _pid: {0, 1, 2, 3})
                path.write_text(previous)
            (directory / "cmdline").write_bytes(b"/binary\0node\0--datadir\0/unrelated\0")
            with self.assertRaisesRegex(ValueError, "running command"):
                COLLECT.validate_process(binding, envelope, proc, lambda _pid: {0, 1, 2, 3})


if __name__ == "__main__":
    unittest.main()
