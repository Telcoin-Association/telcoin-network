"""Confirm that the new Rust regression assertions reject mutations of observation code."""

import argparse
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def cases(suite):
    """Return mutations only on lines introduced for the incoming observations."""
    protocol = "patches/quinn-proto/src/endpoint.rs"
    counters = "patches/quinn-proto/src/incoming_stats.rs"
    metrics = "crates/network-libp2p/src/metrics.rs"
    if suite == "protocol":
        return [
            (protocol, "stats.enqueue()", "stats.dequeue(0)", ["incoming_observations_release_every_disposition"]),
            (protocol, "stats.dequeue(1)", "stats.dequeue(0)", ["incoming_observations_release_every_disposition"]),
            (protocol, "stats.add_server(server.max_incoming)", "stats.add_server(0)", ["incoming_observations_effective_configuration"]),
            (counters, "self.counter(event).fetch_add(1, Ordering::Relaxed)",
             "self.counter(event).fetch_add(0, Ordering::Relaxed)", ["incoming_observations"]),
        ]
    if suite == "candidate":
        return [(counters, "self.counter(event).fetch_add(1, Ordering::Relaxed)",
                 "self.counter(event).fetch_add(0, Ordering::Relaxed)",
                 ["tests::valid_and_replayed_validation_tokens", "tests::malformed_and_wrong_ip_validation_tokens",
                  "tests::expired_retry_has_no_accepted_event"])]
    return [
        (metrics, "f64::from(high).mul_add(f64::from(u32::MAX) + 1.0, f64::from(low))",
         "f64::from(low)", ["metrics::tests::quic_metrics_preserve_large_ceilings"]),
        (metrics, "self.counter.increment(current.saturating_sub(previous))",
         "self.counter.absolute(current)", ["metrics::tests::quic_metrics_accumulate_across_swarm_restarts"]),
        (metrics, '"tn_network.quic_incoming_outcomes_total"',
         '"tn_network.quic_incoming_outcomes_mutant_total"', ["metrics::tests::quic_metrics_have_bounded_per_swarm_series"]),
        (metrics, "self.0.quic_incoming_queued.set(0)", "self.0.quic_incoming_queued.increment(0)",
         ["metrics::tests::quic_metrics_clear_live_gauges_at_shutdown"]),
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("protocol", "candidate", "metrics"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    command = ["cargo", "+1.94", "test", "--locked"]
    if args.suite == "metrics":
        command += ["-p", "tn-network-libp2p", "--lib"]
    else:
        manifest = ("patches/quinn-proto/Cargo.toml" if args.suite == "protocol"
                    else "testing/quic-interop/releases/candidate/Cargo.toml")
        command += ["--manifest-path", manifest, "--lib"]
    environment = {**os.environ, "RUSTC_WRAPPER": ""}
    evidence = []
    try:
        for number, (relative, before, after, filters) in enumerate(cases(args.suite)):
            source = ROOT / relative
            original = source.read_bytes()
            text = original.decode()
            if before not in text:
                raise RuntimeError(f"mutation anchor drifted: {relative}: {before}")
            try:
                source.write_text(text.replace(before, after, 1))
                for test in filters:
                    result = subprocess.run([*command, test, "--", "--nocapture"], cwd=ROOT,
                                            env=environment, capture_output=True, timeout=1200)
                    label = f"{args.suite}-{number}-{test.replace('::', '-')}"
                    (args.output / f"{label}.stdout").write_bytes(result.stdout)
                    (args.output / f"{label}.stderr").write_bytes(result.stderr)
                    failed_assertion = b"test result: FAILED." in result.stdout and b"panicked at" in result.stderr
                    receipt = {"source": relative, "filter": test, "exit_code": result.returncode,
                               "assertion_failed": failed_assertion}
                    evidence.append(receipt)
                    print(json.dumps(receipt), flush=True)
                    if result.returncode != 101 or not failed_assertion:
                        raise RuntimeError(f"mutation did not fail a Rust assertion: {label}")
            finally:
                source.write_bytes(original)
    finally:
        (args.output / "summary.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    main()
