#!/usr/bin/env python3
"""Evaluate captured calibration runs against the proposed thresholds. Acceptance stays pending."""

import argparse
import json
import math
from pathlib import Path

from capture import CRITICAL_CLASSES, FAILED, REJECTIONS, SERVICE, SHED, expected_down

PHASES = ("steady", "catch-up", "reconnect", "hostile", "mixed")
ACCEPTANCE = "pending maintainer decision"
ESTABLISHED = "tn_network_established_connections"
LIMIT = "tn_network_established_connection_limit"
# Hostile pressure on established connections shows up under these rejection reasons only.
ESTABLISHED_REASONS = ("established_total", "established_per_peer", "established_incoming")
RSS = "reth_process_resident_memory_bytes"
CPU = "reth_process_cpu_seconds_total"


def load_run(root, build, phase):
    """Read one captured phase from root/build/phase, or return None when it was not captured."""
    directory = Path(root) / build / phase
    if not (directory / "observations.jsonl").is_file():
        return None
    extra, result = directory / "phase.json", directory / "result.json"
    manifest = json.loads((directory / "manifest.json").read_text())["manifest"]
    return {
        "records": [json.loads(line) for line in (directory / "observations.jsonl").read_text().splitlines() if line],
        "topology": manifest["topology"],
        "nodes": [node["name"] for node in manifest["nodes"]],
        "phase": json.loads(extra.read_text()) if extra.is_file() else {},
        "result": json.loads(result.read_text()) if result.is_file() else None,
    }


def gap(run, label):
    """Name the first evidence gap in one captured phase, or None. Gaps inside expected-down windows do not count.

    Every (node, network) pair from the manifest needs a sample. A scrape error, a missing metric or swarm,
    a failed scrape, or a nonzero capture_exit without reported failures makes the phase pending.
    """
    windows = run["phase"].get("expected_down", [])
    counted = [record for record in run["records"] if not expected_down(record, windows)]
    broken = [record for record in counted if record.get("error") or record.get("missing_metrics") or record.get("missing_networks")]
    networks = {"primary", *(f"worker-{worker}" for worker in range(run["topology"]["workers_per_node"]))}
    seen = {(record["node"], item["labels"].get("network")) for record in counted for item in record.get("observations", [])}
    absent = sorted({(node, network) for node in run["nodes"] for network in networks} - seen)
    result = run["result"]
    failures = [] if result is None else result.get("failures", [])
    unexpected = [failure for failure in failures if not expected_down(failure, windows)]
    checks = (
        (broken, lambda: f"incomplete scrape of {broken[0]['node']} at sample {broken[0]['sample']} in {label}"),
        (absent, lambda: f"no samples for {absent[0][0]} {absent[0][1]} in {label}"),
        (result is None, lambda: f"{label} capture did not finish"),
        (result is not None and (unexpected or result["failed_scrapes"] > len(failures)),
         lambda: f"failed scrapes outside expected-down windows in {label}"),
        (run["phase"].get("capture_exit", 0) != 0 and not failures,
         lambda: f"capture exited {run['phase']['capture_exit']} without reported scrape failures in {label}"),
    )
    return next((message() for failed, message in checks if failed), None)


def captured(root, build, phases):
    """Load every phase, and name the first phase that was not captured or has an evidence gap."""
    loaded = {phase: load_run(root, build, phase) for phase in phases}
    missing = [f"{build} {phase} not captured" for phase, run in loaded.items() if run is None]
    gaps = [found for found in (gap(run, f"{build} {phase}") for phase, run in loaded.items() if run is not None) if found]
    return loaded, next(iter([*missing, *gaps]), None)


def both_builds(root, phase):
    """Load one phase for the baseline and the candidate, or name the first gap in either."""
    runs = [captured(root, build, (phase,)) for build in ("baseline", "candidate")]
    return [loaded[phase] for loaded, _ in runs], next((missing for _, missing in runs if missing), None)


def process_runs(root, metric):
    """Every captured candidate phase, or a pending reason: an evidence gap or a node without this metric."""
    loaded, _ = captured(root, "candidate", PHASES)
    runs = {phase: run for phase, run in loaded.items() if run is not None}
    gaps = [found for found in (gap(run, f"candidate {phase}") for phase, run in runs.items()) if found]
    absent = [f"candidate {phase}: {missing}" for phase, run in runs.items()
              if (missing := metric_gap(run, metric, networked=False, minimum=2 if metric == CPU else 1))]
    empty = [] if runs else ["no candidate phase captured"]
    return list(runs.values()), next(iter([*gaps, *absent, *empty]), None)


def measured_records(run):
    """Exclude scrapes that overlap an explicitly recorded expected-down interval."""
    windows = run["phase"].get("expected_down", [])
    return (record for record in run["records"] if not expected_down(record, windows))


def metric_gap(run, metric, networked=True, minimum=1, **labels):
    """Require this metric and label selection on every expected node or (node, swarm) pair."""
    networks = {"primary", *(f"worker-{worker}" for worker in range(run["topology"]["workers_per_node"]))}
    expected = {(node, network) for node in run["nodes"] for network in networks} if networked else set(run["nodes"])
    seen, series = {}, {}
    for node, found, time, _ in samples(run, metric, **labels):
        key = (node, found.get("network")) if networked else node
        seen.setdefault(key, set()).add(time)
        series.setdefault((node, tuple(sorted(found.items()))), set()).add(time)
    missing = sorted(key for key in expected if len(seen.get(key, ())) < minimum)
    incomplete = sorted(key for key, times in series.items() if len(times) < minimum)
    problem = next(iter([*missing, *incomplete]), None)
    return f"fewer than {minimum} {metric} {labels} samples for {problem}" if problem is not None else None


def samples(run, metric, **labels):
    """Yield (node, labels, time, value) for each observation with this metric and these labels."""
    for record in measured_records(run):
        for item in record.get("observations", []):
            if item["metric"] == metric and all(item["labels"].get(key) == value for key, value in labels.items()):
                yield record["node"], item["labels"], record["started_unix_seconds"], item["value"]


def increases(run, metric, **labels):
    """Sum counter increases per node and label set. A decrease is a restart. None when absent."""
    previous, total = {}, {}
    for node, found, _, value in samples(run, metric, **labels):
        key = (node, tuple(sorted(found.items())))
        prior = previous.get(key, value)
        total[key] = total.get(key, 0) + (value - prior if value >= prior else value)
        previous[key] = value
    return total or None


def nearest_rank(values, quantile):
    """Return the nearest-rank quantile of a non-empty list."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def verdict(passed):
    return "pass" if passed else "fail"


def bucket_p99(run, service_class):
    """Estimate p99 per (node, network) pair as the upper bucket bound from bucket increases. Keep the worst pair."""
    counts = {}
    for (node, labels), value in (increases(run, f"{SERVICE}_bucket", **{"class": service_class}) or {}).items():
        found = dict(labels)
        pair = counts.setdefault((node, found["network"]), {})
        pair[float(found["le"])] = pair.get(float(found["le"]), 0) + value
    estimates = [min(bound for bound, count in pair.items() if count >= math.ceil(0.99 * pair[math.inf]))
                 for pair in counts.values() if pair.get(math.inf, 0) > 0]
    return max(estimates, default=None)


def service_p99(threshold, root):
    """Worst (node, network) pair p99 for the class over the phases, from summary quantiles or from buckets."""
    runs, missing = captured(root, "candidate", threshold["phases"])
    if missing:
        return "pending", missing
    estimates = []
    for phase, run in runs.items():
        service_class = {"class": threshold["class"]}
        count_gap = metric_gap(run, f"{SERVICE}_count", **service_class)
        summary_gap = metric_gap(run, SERVICE, quantile="0.99", **service_class)
        bucket_gap = metric_gap(run, f"{SERVICE}_bucket", minimum=2, le="+Inf", **service_class)
        if count_gap or (summary_gap and bucket_gap):
            return "pending", f"candidate {phase}: {count_gap or summary_gap}"
        # An empty summary renders its quantiles as 0, so a class with no requests is pending, not a pass.
        if max((value for *_, value in samples(run, f"{SERVICE}_count", **{"class": threshold["class"]})), default=0) <= 0:
            return "pending", f"no {threshold['class']} requests served in candidate {phase}"
        quantiles = [value for *_, value in samples(run, SERVICE, **{"class": threshold["class"], "quantile": "0.99"})]
        estimate = max(quantiles) if summary_gap is None else bucket_p99(run, threshold["class"])
        if estimate is None:
            return "pending", f"no {threshold['class']} service samples in candidate {phase}"
        estimates.append(estimate)
    measured = max(estimates)
    # A p99 in the +Inf bucket fails; report it as a string because JSON has no infinity.
    return verdict(measured <= threshold["limit_seconds"]), "+Inf" if math.isinf(measured) else measured


def critical_sheds(threshold, root):
    """Vote, epoch record and batch sheds summed over every shed reason: queue_full, unsubscribed and admission."""
    return critical_total(threshold, root, SHED, "shed")


def critical_failures(threshold, root):
    """Vote, epoch record and batch requests that failed after admission, summed over every outcome."""
    return critical_total(threshold, root, FAILED, "failure")


def critical_total(threshold, root, metric, noun):
    """Sum counter increases over the critical classes and phases. An absent counter is pending, not zero."""
    runs, missing = captured(root, "candidate", threshold["phases"])
    if missing:
        return "pending", missing
    total = 0
    for phase, run in runs.items():
        for service_class in sorted(CRITICAL_CLASSES):
            missing = metric_gap(run, metric, minimum=2, **{"class": service_class})
            if missing:
                return "pending", f"candidate {phase}: {missing}"
            found = increases(run, metric, **{"class": service_class})
            if found is None:
                return "pending", f"no {service_class} {noun} counter in candidate {phase}"
            total += sum(found.values())
    return verdict(total <= threshold["limit"]), total


def within_allocation(threshold, root):
    """Highest established/limit ratio. A zero limit means the candidate runs without a budget."""
    runs, missing = captured(root, "candidate", threshold["phases"])
    if missing:
        return "pending", missing
    worst = None
    for phase, run in runs.items():
        missing = metric_gap(run, LIMIT) or metric_gap(run, ESTABLISHED)
        if missing:
            return "pending", f"candidate {phase}: {missing}"
        limits = {(node, found["network"]): value for node, found, _, value in samples(run, LIMIT)}
        for node, found, _, value in samples(run, ESTABLISHED):
            limit = limits.get((node, found["network"]))
            if limit is None:
                return "pending", f"no allocation sample for {node} {found['network']} in {phase}"
            if limit == 0:
                return "fail", f"{node} {found['network']} runs without an allocation in {phase}"
            worst = max(worst or 0, value / limit)
    return ("pending", "no established connection samples") if worst is None else (verdict(worst <= 1), worst)


def hostile_recovery(threshold, root):
    """Hostile rejections must rise, and final occupancy must return to the steady peak."""
    runs, missing = captured(root, "candidate", ("steady", "hostile"))
    if missing:
        return "pending", missing
    # pending_* rejections are handshake state; only established-stage reasons show pressure on the allocation.
    missing = (metric_gap(runs["steady"], ESTABLISHED) or metric_gap(runs["hostile"], ESTABLISHED)
               or next((gap for reason in ESTABLISHED_REASONS
                        if (gap := metric_gap(runs["hostile"], REJECTIONS, minimum=2, reason=reason))), None))
    if missing:
        return "pending", missing
    found = [increases(runs["hostile"], REJECTIONS, reason=reason) for reason in ESTABLISHED_REASONS]
    rejections = None if None in found else {key: value for part in found for key, value in part.items()}
    ceiling, final = {}, {}
    for node, found, _, value in samples(runs["steady"], ESTABLISHED):
        ceiling[(node, found["network"])] = max(ceiling.get((node, found["network"]), 0), value)
    for node, found, _, value in samples(runs["hostile"], ESTABLISHED):
        final[(node, found["network"])] = value
    if rejections is None or not final or set(final) - set(ceiling):
        return "pending", "hostile rejections, steady peaks or final occupancy not captured"
    raised = sum(rejections.values())
    recovered = all(value <= ceiling[key] for key, value in final.items())
    return verdict(raised > 0 and recovered), {"rejections": raised, "recovered": recovered}


def heights(run):
    """Map each node to its (time, block) polls. Missing polls are skipped, never read as zero."""
    found = {}
    for record in measured_records(run):
        if "number" in record.get("block", {}):
            found.setdefault(record["node"], []).append((record["started_unix_seconds"], record["block"]["number"]))
    return found


def block_rate(run):
    """Mean blocks per second over the nodes with at least two polls."""
    polls = heights(run)
    if set(run["nodes"]) - set(polls) or any(len(points) < 2 for points in polls.values()):
        return None
    rates = [(points[-1][1] - points[0][1]) / (points[-1][0] - points[0][0])
             for points in polls.values() if points[-1][0] > points[0][0]]
    return sum(rates) / len(rates) if rates else None


def persistence_ratio(threshold, root):
    """Lowest candidate/baseline block rate ratio over the phases."""
    ratios = []
    for phase in threshold["phases"]:
        runs, problem = both_builds(root, phase)
        if problem:
            return "pending", problem
        rates = [block_rate(run) for run in runs]
        if None in rates or rates[0] <= 0:
            return "pending", f"block rate for {phase} not captured for both builds"
        ratios.append(rates[1] / rates[0])
    return verdict(min(ratios) >= threshold["limit"]), min(ratios)


def catch_up_seconds(run):
    """Seconds from the first poll until the lagging node reaches the lowest peer height in one sample."""
    node = run["phase"].get("catch_up_node")
    by_sample = {}
    for record in measured_records(run):
        if "number" in record.get("block", {}):
            by_sample.setdefault(record["sample"], {})[record["node"]] = (record["started_unix_seconds"], record["block"]["number"])
    if node is None or not by_sample:
        return None
    start = min(time for polls in by_sample.values() for time, _ in polls.values())
    reached = [polls[node][0] - start for _, polls in sorted(by_sample.items())
               if node in polls and len(polls) > 1 and set(run["nodes"]) <= set(polls)
               and polls[node][1] >= min(height for name, (_, height) in polls.items() if name != node)]
    return reached[0] if reached else None


def catch_up_ratio(threshold, root):
    """Candidate/baseline catch-up time ratio from the same starting state."""
    runs, problem = both_builds(root, "catch-up")
    if problem:
        return "pending", problem
    times = [catch_up_seconds(run) for run in runs]
    if None in times or times[0] <= 0:
        return "pending", "catch-up completion not captured for both builds"
    return verdict(times[1] / times[0] <= threshold["limit"]), times[1] / times[0]


def rss_fraction(threshold, root):
    """Peak RSS as a fraction of host RAM over every captured candidate phase."""
    runs, problem = process_runs(root, RSS)
    if problem:
        return "pending", problem
    peaks = [value / run["topology"]["ram_bytes_per_node"] for run in runs for *_, value in samples(run, RSS)]
    if not peaks:
        return "pending", "no candidate RSS samples"
    return verdict(max(peaks) <= threshold["limit"]), max(peaks)


def cpu_fraction(threshold, root):
    """Process CPU p95 as a fraction of host CPUs. The network share needs a profiler trace."""
    runs, problem = process_runs(root, CPU)
    if problem:
        return "pending", problem
    shares = []
    for run in runs:
        series = {}
        for node, _, time, value in samples(run, CPU):
            series.setdefault(node, []).append((time, value))
        shares += [(later[1] - earlier[1]) / (later[0] - earlier[0]) / run["topology"]["cpus_per_node"]
                   for points in series.values() for earlier, later in zip(points, points[1:])
                   if later[0] > earlier[0] and later[1] >= earlier[1]]
    if not shares:
        return "pending", "no candidate CPU samples"
    p95 = nearest_rank(shares, 0.95)
    return verdict(p95 <= threshold["limit"]), {"cpu_p95_fraction": p95, "network_share": "pending: record from a per-swarm profiler trace"}


CHECKS = {
    "service_p99": service_p99,
    "critical_sheds": critical_sheds,
    "critical_failures": critical_failures,
    "within_allocation": within_allocation,
    "hostile_recovery": hostile_recovery,
    "persistence_ratio": persistence_ratio,
    "catch_up_ratio": catch_up_ratio,
    "rss_fraction": rss_fraction,
    "cpu_fraction": cpu_fraction,
}


def evaluate(document, root):
    """Give pass, fail or pending for each threshold. Missing evidence is pending."""
    if document.get("acceptance") != ACCEPTANCE:
        raise ValueError(f"thresholds must keep acceptance as {ACCEPTANCE!r}")
    results = []
    for threshold in document["thresholds"]:
        status, measured = CHECKS[threshold["check"]](threshold, root)
        results.append({"id": threshold["id"], "status": status, "measured": measured, "threshold": threshold})
    return {"acceptance": ACCEPTANCE, "results": results}


def main(argv=None):
    """Print the report. Exit 1 when a threshold fails; acceptance is never granted here."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("thresholds", type=Path)
    parser.add_argument("results", type=Path, help="directory with <build>/<phase> capture outputs")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        report = evaluate(json.loads(args.thresholds.read_text()), args.results)
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, f"evaluate failed: {error}\n")
    text = json.dumps(report, indent=2) + "\n"
    print(text, end="")
    if args.output:
        args.output.write_text(text)
    return int(any(item["status"] == "fail" for item in report["results"]))


if __name__ == "__main__":
    raise SystemExit(main())
