"""Summarize complete, independently repeated hub join qualification evidence."""

import json
import math
from pathlib import Path
import re
import sys


def distribution(values):
    """Return nearest-rank percentiles without implying a large-sample confidence interval."""
    ordered = sorted(values)
    return {
        "samples": len(ordered),
        "p50_ms": ordered[math.ceil(len(ordered) * 0.50) - 1],
        "p95_ms": ordered[math.ceil(len(ordered) * 0.95) - 1],
        "max_ms": ordered[-1],
    }


def summarize(directory):
    """Reject incomplete runs and retain both raw samples and per-stage distributions."""
    pattern = re.compile(
        r"hub_join_timing role=(Primary|Worker\(\d+\)) delayed=(true|false) "
        r"publication_ms=(\d+) resolution_ms=(\d+) connection_ms=(\d+) activation_ms=(\d+)"
    )
    swarms = {}
    for match in pattern.finditer((directory / "swarms.log").read_text()):
        role, delayed, *timings = match.groups()
        swarms.setdefault(f"{role}/delayed={delayed}", []).append(
            dict(zip(("publication", "resolution", "connection", "activation"), map(int, timings)))
        )
    required = {f"{role}/delayed={delayed}" for role in ("Primary", "Worker(0)", "Worker(1)")
                for delayed in ("true", "false")}
    if set(swarms) != required or any(len(samples) != 5 for samples in swarms.values()):
        raise ValueError("expected five samples for every swarm and hub condition")
    governance = [json.loads(line.split("hub_join_governance_evidence=", 1)[1])
                  for line in (directory / "governance.log").read_text().splitlines()
                  if "hub_join_governance_evidence=" in line]
    if len(governance) != 5:
        raise ValueError("expected five governance activation samples")
    stages = ("publication_ms", "publication_to_resolution_ms", "resolution_to_connection_ms",
              "connection_to_consensus_readiness_ms")
    return {
        "environment": (directory / "environment.txt").read_text(),
        "swarm_samples": swarms,
        "swarm_distributions": {role: {stage: distribution([sample[stage] for sample in samples])
                                      for stage in samples[0]} for role, samples in swarms.items()},
        "governance_samples": governance,
        "governance_distributions": {stage: distribution([sample[stage] for sample in governance])
                                     for stage in stages},
    }


if __name__ == "__main__":
    print(json.dumps(summarize(Path(sys.argv[1])), indent=2))
