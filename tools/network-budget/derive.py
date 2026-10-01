#!/usr/bin/env python3
"""Derive a proposed process_budget from baseline honest phases. The output is a proposal only."""

import argparse
import json
import math
from pathlib import Path

from capture import file_record
from evaluate import ACCEPTANCE, ESTABLISHED, load_run, samples

HONEST_PHASES = ("steady", "catch-up", "reconnect")
TRANSPORT_FIELDS = ("peak_connections_per_peer", "peak_inbound_streams_per_connection", "peak_receive_credit_bytes_per_connection")
U32_MAX = 2**32 - 1


def allocate(budget):
    """Mirror NetworkProcessBudget::allocate in crates/config/src/network_budget.rs."""
    connections = budget["max_established_connections"] // budget["swarm_count"]
    admitted = connections * budget["swarm_count"]
    if connections == 0 or budget["max_inbound_streams"] // admitted == 0 or budget["max_receive_credit_bytes"] // admitted == 0:
        raise ValueError("budget cannot give each swarm one connection and each connection one stream and one byte")
    return {
        "connections": connections,
        "connections_per_peer": min(budget["max_established_connections_per_peer"], connections),
        "streams_per_connection": min(budget["max_inbound_streams"] // admitted, U32_MAX),
        "receive_credit_per_connection": min(budget["max_receive_credit_bytes"] // admitted, U32_MAX),
    }


def derive(root, transport_path, headroom):
    """Scale observed peaks by headroom and split them like allocate(). Refuse on missing inputs."""
    if not math.isfinite(headroom) or headroom < 1:
        raise ValueError("headroom must be finite and at least 1")
    transport = json.loads(Path(transport_path).read_text())
    if (set(transport) != {*TRANSPORT_FIELDS, "source"} or not transport["source"]
            or any(type(transport[field]) is not int or transport[field] <= 0 for field in TRANSPORT_FIELDS)):
        raise ValueError(f"transport peaks need positive integers {', '.join(TRANSPORT_FIELDS)} and a source")
    runs = {phase: load_run(root, "baseline", phase) for phase in HONEST_PHASES}
    missing = [phase for phase, run in runs.items() if run is None]
    if missing:
        raise ValueError(f"baseline phases not captured: {', '.join(missing)}")
    topology = runs["steady"]["topology"]
    swarms = 1 + topology["workers_per_node"]
    peaks = {}
    for run in runs.values():
        for _, labels, _, value in samples(run, ESTABLISHED):
            peaks[labels["network"]] = max(peaks.get(labels["network"], 0), value)
    expected = {"primary", *(f"worker-{worker}" for worker in range(topology["workers_per_node"]))}
    if set(peaks) != expected:
        raise ValueError(f"established connection peaks missing for: {', '.join(sorted(expected - set(peaks)))}")
    connections = max(math.ceil(max(peaks.values()) * headroom), 1)
    admitted = connections * swarms
    scaled = {field: math.ceil(transport[field] * headroom) for field in TRANSPORT_FIELDS}
    budget = {
        "swarm_count": swarms,
        "max_established_connections": admitted,
        "max_established_connections_per_peer": min(scaled["peak_connections_per_peer"], connections),
        "max_inbound_streams": scaled["peak_inbound_streams_per_connection"] * admitted,
        "max_receive_credit_bytes": scaled["peak_receive_credit_bytes_per_connection"] * admitted,
    }
    return {
        "status": "proposed",
        "acceptance": ACCEPTANCE,
        "inputs": {"baseline": [file_record(Path(root) / "baseline" / phase / "observations.jsonl") for phase in HONEST_PHASES],
                   "transport": file_record(Path(transport_path)), "transport_source": transport["source"]},
        "topology": topology,
        "headroom": headroom,
        "peak_established_per_swarm": peaks,
        "transport_peaks": {field: transport[field] for field in TRANSPORT_FIELDS},
        "process_budget": budget,
        "allocation": allocate(budget),
    }


def main(argv=None):
    """Write the derivation record to a new file."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path, help="directory with baseline/<phase> capture outputs")
    parser.add_argument("transport", type=Path, help="JSON with transport peaks measured by tracing")
    parser.add_argument("--headroom", type=float, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        record = derive(args.results, args.transport, args.headroom)
        with args.output.open("x") as output:
            output.write(json.dumps(record, indent=2) + "\n")
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, f"derive refused: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
