"""Exercise binary attestation and deployment rejection without synthetic qualification results."""

import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


RUNNER = load("capacity_runner", "docker-run.py")
COLLECT = load("capacity_deployment_collect", "collect.py")


class DeploymentTests(unittest.TestCase):
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
