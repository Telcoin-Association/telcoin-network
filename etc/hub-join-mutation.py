"""Confirm that every new join qualification rejects broken authenticated-resolution evidence."""

import os
from pathlib import Path
import re
import subprocess


def rejected(command, log):
    """Require an actual test failure, rather than a compilation or runner failure."""
    with log.open("w") as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, check=False)
    if result.returncode != 100:
        raise RuntimeError(f"expected nextest test failure, got {result.returncode}; see {log}")


def qualify_mutation():
    """Mutate only the new window-resolution getter and restore its exact source on all exits."""
    source = Path("crates/network-libp2p/src/peers/admission.rs")
    original = source.read_text()
    mutant, count = re.subn(
        r"(pub fn resolved_window\(&self\) -> usize \{\s*)self\.resolved_window",
        r"\g<1>self.resolved_window.saturating_sub(self.resolved_window)",
        original,
    )
    if count != 1:
        raise RuntimeError("expected exactly one new resolved_window getter")
    evidence = Path("hub-join-evidence")
    try:
        source.write_text(mutant)
        command = ["cargo", "nextest", "run", "--locked", "-p", "tn-network-libp2p",
                   "-E", "test(hub_join_)", "--success-output", "immediate",
                   "--failure-output", "immediate"]
        swarm_log = evidence / "mutation-swarms.log"
        rejected(command, swarm_log)
        clean_log = re.sub(r"\x1b\[[0-9;]*m", "", swarm_log.read_text())
        if not re.search(r"7 tests run: 0 passed, 7 failed", clean_log):
            raise RuntimeError(f"every new swarm qualification must reject the mutant; see {swarm_log}")
        subprocess.run(["make", "build-e2e-bin"], check=True)
        os.environ["HUB_JOIN_QUALIFICATION_ATTEMPT"] = "6"
        rejected(["cargo", "nextest", "run", "--locked", "-p", "e2e-tests", "-E",
                  "test(hub_join_governance_two_workers)", "--run-ignored", "only",
                  "--success-output", "immediate", "--failure-output", "immediate"],
                 evidence / "mutation-governance.log")
    finally:
        source.write_text(original)
    print("All seven swarm qualifications and the governance qualification rejected the mutation.")


if __name__ == "__main__":
    qualify_mutation()
