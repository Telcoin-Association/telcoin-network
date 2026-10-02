#!/usr/bin/env python3
"""Confirm new load-bearing Rust regressions with compiling, reverted production mutations."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parents[2]
CASES = [
    ("closed_connection_io", "crates/network-libp2p/src/consensus.rs",
     "ReqResOutboundFailure::Io(e) => match e.kind() {\n                        ErrorKind::NotConnected\n                        | ErrorKind::ConnectionReset",
     "ReqResOutboundFailure::Io(e) => match e.kind() {\n                        ErrorKind::ConnectionReset",
     "disconnected_request_io_does_not_score_peer"),
    ("cancelled_permit_occupancy", "crates/network-libp2p/src/capacity.rs",
     "active.decrement(1.0);", "active.decrement(0.0);", "cancellation_releases_reserved_occupancy"),
    ("reserved_permit_occupancy", "crates/network-libp2p/src/capacity.rs",
     "metrics.active.increment(1.0);", "metrics.active.increment(0.0);", "cancellation_releases_reserved_occupancy"),
    ("independent_worker_metrics", "crates/network-libp2p/src/capacity.rs",
     "let network = network_label(network);", 'let network = network_label(network).replace("worker-1", "worker-0");',
     "workers_have_independent_series"),
    ("application_query_cap", "crates/network-libp2p/src/consensus.rs",
     "\n                    >= 100\n", "\n                    >= 101\n", "application_record_queries_are_bounded_and_complete"),
    ("rotation_public_ceiling", "crates/network-libp2p/src/peers/manager.rs",
     ".max(public_excess)", ".min(public_excess)", "public_peer_limit_prunes_after_committee_rotation"),
    ("independent_serve_classes", "crates/config/src/network_serve.rs",
     "usize::from(self.prefetch.get())", "usize::from(self.worker_shed.get())", "operator_limits_are_finite_and_independent"),
    ("mesh_degree_order", "crates/config/src/gossip_mesh.rs",
     "self.low > self.target", "self.low > self.high", "rejects_invalid_mesh_relationships"),
    ("remote_periodic_replication", "crates/network-libp2p/src/kad.rs",
     "(self.retention.is_none() || record.publisher == Some(self.local_peer_id))", "true",
     "test_kad_record_jobs_publish_own_record_only"),
    ("duplicate_rotation_disconnect", "crates/network-libp2p/src/peers/manager.rs",
     "PeerAction::Disconnect | PeerAction::DisconnectWithPX => self.temporarily_ban(peer_id),",
     "PeerAction::Disconnect | PeerAction::DisconnectWithPX => self.apply_peer_action(peer_id, action),",
     "public_peer_limit_prunes_after_committee_rotation"),
]


def execute(argv, directory, label):
    """Retain finite compiler/test logs, without accepting a compiler failure as a killed mutant."""
    result = subprocess.run(argv, cwd=ROOT, capture_output=True, timeout=1800)
    raw = result.stdout + result.stderr
    if len(raw) > 64 * 1024**2:
        raise ValueError("mutation command log exceeds 64 MiB")
    path = directory / (label + ".log")
    path.write_bytes(raw)
    return result.returncode, re.sub(r"\x1b\[[0-9;]*m", "", raw.decode(errors="replace")), {
        "argv": argv, "exit_code": result.returncode,
        "log": path.name, "sha256": hashlib.sha256(raw).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    reports = []
    try:
        for name, relative, before, after, regression in CASES:
            path = ROOT / relative
            original = path.read_bytes()
            text = original.decode()
            if text.count(before) != 1:
                raise ValueError(f"{name}: mutation must select exactly one production expression")
            selector = f"test({regression})"
            test_argv = ["cargo", "+1.94", "nextest", "run", "--locked", "--workspace", "--exclude", "tn-faucet",
                         "-E", selector, "--no-tests", "fail", "--test-threads", "1"]
            control_code, _, control = execute(test_argv, args.output, name + "-control")
            if control_code:
                raise ValueError(f"{name}: original regression must pass")
            report = {"mutation": name, "path": relative, "regression": regression,
                      "source_sha256": hashlib.sha256(original).hexdigest(), "control": control}
            try:
                path.write_text(text.replace(before, after))
                compiler, _, compilation = execute(
                    ["cargo", "+1.94", "test", "--locked", "--workspace", "--no-run"], args.output, name + "-compile")
                report["compilation"] = compilation
                if compiler:
                    raise ValueError(f"{name}: mutant did not compile, no mutation confirmation")
                code, output, test = execute(test_argv, args.output, name + "-test")
                report["test"] = test
                report["detected"] = code == 100 and re.search(r"FAIL[^\n]*" + re.escape(regression), output) is not None
                if not report["detected"]:
                    raise ValueError(f"{name}: selected regression did not fail on the compiling mutant")
            finally:
                path.write_bytes(original)
                report["restored_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
                reports.append(report)
            print(json.dumps({"mutation": name, "detected": True}, sort_keys=True), flush=True)
    finally:
        with (args.output / "report.json").open("x") as output:
            json.dump(reports, output, allow_nan=False, sort_keys=True, indent=2)
            output.write("\n")


if __name__ == "__main__":
    main()
