#!/usr/bin/env python3
"""Run the complete QUIC release evidence lane on the attest box."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib

ROOT = Path(__file__).resolve().parents[2]
RELEASES = ("0.13.1", "0.14.0", "candidate")
# Candidate source whose Retry argument the mutation step forces off.
MUTATION_SOURCE = "testing/quic-interop/src/candidate.rs"
# The listener's limit call, matched through any whitespace because rustfmt splits it across lines.
RETRY_ANCHOR = re.compile(r"(\.apply\(\s*config,\s*)retry(,\s*Arc::clone\(&stats\),?\s*\);)")
# One libtest summary line per test binary; the count is the number of tests that passed.
PASSED = re.compile(r"^test result: ok\. (\d+) passed", re.MULTILINE)


def disable_retry(source):
    """Force the listener's Retry argument off, or fail if the anchor no longer matches once."""
    mutated, count = RETRY_ANCHOR.subn(r"\1false\2", source)
    if count != 1:
        raise RuntimeError("mutation anchor changed; review the experiment")
    return mutated


def passed_tests(text):
    """Total the passing tests in a libtest log, so a filter that selects nothing counts zero."""
    return sum(int(count) for count in PASSED.findall(text))


def load_module(name):
    """Load an explicitly named sibling without adding the checkout to sys.path."""
    spec = importlib.util.spec_from_file_location(f"quic_{name}", Path(__file__).with_name(f"{name}.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(args):
    """Retain every command, failure and artifact before deciding release qualification."""
    output = args.output.resolve()
    if output.is_relative_to(ROOT) or output.exists():
        raise ValueError("use a fresh evidence directory outside the source tree")
    output.mkdir(parents=True)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)
    if dirty:
        raise ValueError("release qualification requires a clean committed candidate")
    # Check the anchor before the long build and matrix steps, so drift fails in seconds.
    mutated = disable_retry((ROOT / MUTATION_SOURCE).read_text())
    toolchain = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
    result = {"candidate": commit, "status": "running", "steps": [],
              "toolchain": toolchain, "cadence": "before feature release and on transport changes"}

    def step(name, command, timeout=3600, success=True, tests=0):
        """Capture a finite command once, preserving failures and exact argv.

        A nonzero `tests` is the fewest libtest cases the command must pass, so a filter that
        selects nothing fails instead of becoming a successful receipt.
        """
        started = time.monotonic()
        log = output / f"{name}.log"
        failure = None
        code = None
        try:
            with log.open("wb") as stream:
                code = subprocess.run(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT,
                                      timeout=timeout, check=False).returncode
        except (subprocess.TimeoutExpired, OSError) as error:
            failure = error
        receipt = {"name": name, "command": command, "exit_code": code,
                   "elapsed_seconds": time.monotonic() - started, "log": log.name,
                   "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest()}
        if failure is not None:
            receipt["error"] = repr(failure)
        if tests:
            receipt["tests_passed"] = passed_tests(log.read_text(errors="replace"))
        result["steps"].append(receipt)
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        if failure is not None:
            raise failure
        if success and code != 0:
            raise RuntimeError(f"{name} failed; see {log}")
        if tests and receipt["tests_passed"] < tests:
            raise RuntimeError(f"{name} passed {receipt['tests_passed']} tests, expected at least {tests}; see {log}")
        return code, log

    try:
        result["environment"] = load_module("run").environment()
        result["releases"] = load_module("run").check_locks()
        binaries = {}
        for release in RELEASES:
            manifest = ROOT / f"testing/quic-interop/releases/{release}/Cargo.toml"
            target = output / "builds" / release
            cargo = ["cargo", f"+{toolchain}", "--locked", "--manifest-path", str(manifest),
                     "--target-dir", str(target)]
            step(f"build-{release}", [*cargo[:2], "build", *cargo[2:], "--bins", "-j", "2"])
            step(f"test-{release}", [*cargo[:2], "test", *cargo[2:], "--all-targets", "-j", "2"])
            binaries[release] = target / "debug/quic-interop"
            step(f"features-{release}", ["cargo", f"+{toolchain}", "tree", "--locked",
                 "--manifest-path", str(manifest), "-e", "features"])
            result.setdefault("binaries", {})[release] = hashlib.sha256(binaries[release].read_bytes()).hexdigest()
        for listener in RELEASES:
            for dialer in RELEASES:
                step(f"matrix-{dialer}-to-{listener}", [sys.executable, "-I",
                     str(Path(__file__).with_name("run.py")), "--listener", str(binaries[listener]),
                     "--dialer", str(binaries[dialer]), "--listener-release", listener,
                     "--dialer-release", dialer, "--output", str(output / f"matrix-{dialer}-{listener}")],
                     timeout=600)
        # The test files sit under src/tests/ but are `#[path]` modules of `consensus`, so
        # libtest names them `consensus::<module>::<test>`.
        step("node-scheduling", ["cargo", f"+{toolchain}", "test", "--locked", "-p",
             "tn-network-libp2p", "--target-dir", str(output / "builds/node"),
             "--lib", "consensus::loop_budget_tests::", "-j", "2"], tests=4)
        step("node-reconnect", ["cargo", f"+{toolchain}", "test", "--locked", "-p",
             "tn-network-libp2p", "--target-dir", str(output / "builds/node"),
             "--lib", "-j", "2", "--", "--exact",
             "consensus::network_tests::test_score_decay_and_reconnection"], tests=1)
        privileged = [] if os.geteuid() == 0 else ["sudo", "-n"]
        step("isolated-source", [*privileged, sys.executable, "-I",
             str(Path(__file__).with_name("isolated.py")), "run",
             "--candidate", str(binaries["candidate"]), "--stock", str(binaries["0.14.0"]),
             "--initial", str(output / "builds/candidate/debug/quic-initial"),
             "--output", str(output / "isolated")], timeout=300)
        with tempfile.TemporaryDirectory(prefix="tn1432-mutation-") as temporary:
            copy = Path(temporary)
            for relative in ("patches/libp2p-quic", "testing/quic-interop"):
                shutil.copytree(ROOT / relative, copy / relative,
                                ignore=shutil.ignore_patterns("target", "__pycache__"))
            for relative in ("crates/config/src/network/quic.rs",
                             "crates/network-libp2p/src/quic_incoming.rs"):
                destination = copy / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, destination)
            (copy / MUTATION_SOURCE).write_text(mutated)
            code, log = step("mutation-no-retry", ["cargo", f"+{toolchain}", "test", "--locked",
                 "--manifest-path", str(copy / "testing/quic-interop/releases/candidate/Cargo.toml"),
                 "--target-dir", str(output / "builds/mutation"), "--lib", "-j", "2"], success=False)
            if code == 0 or "test result: FAILED" not in log.read_text():
                raise RuntimeError("Retry mutation did not produce an assertion failure")
        result["qualification"] = load_module("qualification").validate(args.qualification.resolve(), commit)
        report = args.qualification.resolve()
        hardware = output / "hardware"
        hardware.mkdir()
        shutil.copy2(report, hardware / "report.json")
        for artifact in json.loads(report.read_text())["artifacts"]:
            source = (report.parent / artifact["path"]).resolve()
            destination = hardware / source.relative_to(report.parent)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        result["binaries"] = {release: hashlib.sha256(binary.read_bytes()).hexdigest()
                              for release, binary in binaries.items()}
        result["status"] = "passed"
    except BaseException as error:
        result.update(status="failed", error=repr(error))
        raise
    finally:
        result["artifacts"] = {str(path.relative_to(output)): hashlib.sha256(path.read_bytes()).hexdigest()
                               for path in output.rglob("*") if path.is_file()
                               and not path.is_relative_to(output / "builds")
                               and path != output / "result.json"}
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")


def main():
    """Require explicit output and hardware evidence instead of a loopback pass claim."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--qualification", type=Path, required=True)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
