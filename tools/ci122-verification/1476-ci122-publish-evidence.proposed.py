"""Package an already qualified run for durable PR review, excluding fixture keys and binaries."""

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import types


def digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def owned_file(root, name):
    if (not isinstance(name, str) or not name or "\\" in name or "\x00" in name
            or Path(name).is_absolute() or any(part in ("", ".", "..") for part in name.split("/"))):
        raise ValueError(f"publication path must be a contained relative path: {name!r}")
    source = root / name
    if source.is_symlink() or not source.is_file() or not source.resolve().is_relative_to(root.resolve()):
        raise ValueError(f"publication source is not a regular owned file: {name}")
    return source


def verify_source(root, plan, checkout, revision):
    """Bind the complete retained source manifest to independently supplied Git objects."""
    if not isinstance(revision, str) or re.fullmatch(r"[0-9a-f]{40}", revision) is None:
        raise ValueError("qualification revision must be an explicit full Git commit ID")
    for phase in ("baseline", "candidate"):
        if plan[phase]["qualification_revision"] != revision:
            raise ValueError(f"{phase} qualification revision differs from trusted revision")

    def git(*arguments):
        return subprocess.run(["git", "--no-replace-objects", "-C", str(checkout), *arguments], check=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout

    if git("rev-parse", "--verify", f"{revision}^{{commit}}").decode().strip() != revision:
        raise ValueError("trusted qualification revision is not a commit")
    entries = {}
    for record in git("ls-tree", "-z", f"{revision}:tools/hub-capacity").split(b"\0"):
        if not record:
            continue
        metadata, encoded_name = record.split(b"\t", 1)
        name = encoded_name.decode()
        if not (name.endswith(".py") or name == "profile-v1.json"):
            continue
        mode, kind, object_id = metadata.decode().split()
        if mode not in ("100644", "100755") or kind != "blob":
            raise ValueError(f"trusted qualification source is not a regular blob: {name}")
        entries[name] = object_id
    source_hashes = json.loads(owned_file(root, "source-hashes.json").read_text())
    if not isinstance(source_hashes, dict) or set(source_hashes) != set(entries) or "qualify.py" not in entries:
        raise ValueError("retained qualification source manifest is incomplete or differs from trusted Git tree")
    source_root = root / "source"
    if source_root.is_symlink() or not source_root.is_dir() or not source_root.resolve().is_relative_to(root):
        raise ValueError("retained qualification source directory is not owned")
    retained_names = {path.name for path in source_root.iterdir() if path.name != "__pycache__"}
    if retained_names != set(entries):
        raise ValueError("retained qualification source files differ from complete trusted manifest")
    verified = {}
    for name, object_id in sorted(entries.items()):
        retained = owned_file(source_root, name).read_bytes()
        trusted = git("cat-file", "blob", object_id)
        if retained != trusted or hashlib.sha256(trusted).hexdigest() != source_hashes[name]:
            raise ValueError(f"retained qualification source differs from trusted Git blob: {name}")
        verified[name] = retained
    return source_hashes, verified["qualify.py"]


def phase_artifacts(root, phase, document):
    directory = root / f"{phase}-evidence"
    if directory.is_symlink() or not directory.is_dir() or not directory.resolve().is_relative_to(root):
        raise ValueError(f"publication phase directory is not owned: {phase}")
    names = []
    for artifact in document["artifacts"]:
        owned_file(directory, artifact["path"])
        names.append(f"{phase}-evidence/{artifact['path']}")
    return names


def archive(root, names, destination):
    with destination.open("xb") as outgoing:
        with gzip.GzipFile(filename="", mode="wb", fileobj=outgoing, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as bundle:
                for name in sorted(names):
                    source = owned_file(root, name)
                    member = tarfile.TarInfo(name)
                    member.size, member.mode, member.mtime = source.stat().st_size, 0o644, 0
                    with source.open("rb") as incoming:
                        bundle.addfile(member, incoming)


def peak_cpu_cores(scorer, samples, hub):
    """Report the same CPU divisor as the independently authenticated scorer."""
    process_times = getattr(scorer, "process_sample_times", None)
    return max((end["hubs"][hub]["cpu_seconds"] - start["hubs"][hub]["cpu_seconds"]) /
               (process_times(end["hubs"][hub])[0] - process_times(start["hubs"][hub])[1]
                if process_times is not None else end["elapsed_seconds"] - start["elapsed_seconds"])
               for start, end in zip(samples, samples[1:]))


def positive_ci_run_id(value):
    """Accept only the canonical positive decimal form of a GitHub Actions run ID."""
    if re.fullmatch(r"[1-9][0-9]*", value) is None:
        raise argparse.ArgumentTypeError("CI run ID must be a positive decimal integer")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--source-checkout", type=Path, required=True,
                        help="independently trusted checkout containing qualification Git objects")
    parser.add_argument("--qualification-revision", required=True,
                        help="independently trusted full qualification source commit ID")
    parser.add_argument("--ci-run-id", required=True, type=positive_ci_run_id,
                        help="explicit GitHub Actions run ID for the recorded measurement")
    args = parser.parse_args()
    root = args.run.resolve()
    destination = args.destination.resolve()
    frozen = json.loads(owned_file(root, "plan.json").read_text())
    report = json.loads(owned_file(root, "report.json").read_text())
    plan = frozen["plan"]
    evidence = {phase: json.loads(owned_file(root, f"{phase}-evidence/evidence.json").read_text())
                for phase in ("baseline", "candidate")}
    artifact_names = {phase: phase_artifacts(root, phase, document) for phase, document in evidence.items()}
    source_hashes, scorer_source = verify_source(root, plan, args.source_checkout.resolve(),
                                                args.qualification_revision)
    scorer_path = owned_file(args.source_checkout.resolve() / "tools/hub-capacity", "qualify.py")
    cached_profile = owned_file(scorer_path.parent, "profile-v1.json")
    if any(parent.is_symlink() for parent in cached_profile.parents) or cached_profile.stat().st_size > 1024**2:
        raise ValueError("trusted checkout profile cache is symlinked or exceeds one MiB")
    with cached_profile.open("rb") as incoming:
        if incoming.read(1024**2 + 1) != owned_file(root, "source/profile-v1.json").read_bytes():
            raise ValueError("trusted checkout profile cache differs from Git-authenticated retained source")
    scorer = types.ModuleType("retained_scorer")
    scorer.__file__ = str(scorer_path)
    exec(compile(scorer_source, str(scorer_path), "exec"), scorer.__dict__)
    scorer.validate_plan(plan)
    if frozen["plan_sha256"] != scorer.digest(plan) or report["plan_sha256"] != frozen["plan_sha256"]:
        raise ValueError("publication plan digest mismatch")
    for phase, document in evidence.items():
        scorer.validate_evidence(plan, document, phase)
        scorer.verify_artifacts(document, root / f"{phase}-evidence")
        if scorer.score(plan, document) != report[phase]:
            raise ValueError(f"publication report differs from rescoring: {phase}")
    if not report["candidate"]["passed"]:
        raise ValueError("candidate does not qualify, publication cannot claim completion")
    for phase in evidence:
        topology = json.loads(owned_file(root, f"deployment/{phase}/topology.json").read_text())
        for field, name in (("initial_transactions_sha256", "initial-transactions.jsonl"),
                            ("canonical_batch_observations_sha256", "canonical-batch-observations.json")):
            if digest(owned_file(root, f"deployment/{phase}/{name}")) != topology[field]:
                raise ValueError(f"retained seed evidence digest mismatch: {phase} {name}")
    inputs = ["plan.json", "declaration.json", "manifest.json", "source-provenance.json", "source-hashes.json",
              "docker-runtime.json", "links-initial.json", "transactions.json",
              "executed-docker-commands.json",
              "deployment/population.json"]
    inputs += [f"source/{name}" for name in source_hashes]
    for phase, document in evidence.items():
        inputs += [f"deployment/{phase}/bindings.json", f"deployment/{phase}/peers-ready.json",
                   f"deployment/{phase}/{phase}-profile.json", f"deployment/{phase}/topology.json",
                   f"deployment/{phase}/initial-transactions.jsonl",
                   f"deployment/{phase}/canonical-batch-observations.json"]
    for name in inputs:
        owned_file(root, name)
    destination.mkdir(parents=True, exist_ok=False)
    for name in ("plan.json", "report.json"):
        shutil.copyfile(owned_file(root, name), destination / name)
    for phase in evidence:
        names = [f"{phase}-evidence/evidence.json", *artifact_names[phase]]
        archive(root, names, destination / f"{phase}.tar.gz")
    archive(root, inputs, destination / "inputs.tar.gz")
    lines = ["# Public hub capacity qualification", "",
             "The candidate passed every predeclared threshold on the recorded envelope.", "",
             f"Binary source: `{plan['candidate']['revision']}`.",
             f"Qualification source verified against independently supplied Git objects: "
              f"[`{args.qualification_revision}`](https://github.com/Telcoin-Association/telcoin-network/tree/{args.qualification_revision}/tools/hub-capacity).",
              f"Recorded measurement run: [GitHub Actions run {args.ci_run_id}]"
              f"(https://github.com/Telcoin-Association/telcoin-network/actions/runs/{args.ci_run_id}).",
             f"Frozen plan SHA-256: `{frozen['plan_sha256']}`.", "",
             "The plan records hardware, link conditions, population, exact profiles and thresholds.",
             "The report and compressed archives retain all scored observations and their original hashes.", "",
             "| Scenario | Baseline success | Baseline p99 ms | Candidate attempts | Candidate success | Candidate p99 / bound ms |",
             "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for scenario in sorted(report["candidate"]["scenarios"]):
        before, after = report["baseline"]["scenarios"][scenario], report["candidate"]["scenarios"][scenario]
        bound = plan["thresholds"]["scenarios"][scenario]
        lines.append(f"| {scenario} | {before['success_rate']:.2%} | {before['p99_ms']:.2f} | {after['attempts']} / {bound['minimum_attempts']} | {after['success_rate']:.2%} | {after['p99_ms']:.2f} / {bound['max_p99_ms']} |")
    lines += ["", "Candidate whole-process resources:", "",
              "| Hub | Peak sampled CPU cores / bound | Peak RSS MiB / bound | Maximum progress stall seconds / bound | Canonical progress |",
              "| --- | ---: | ---: | ---: | ---: |"]
    samples = evidence["candidate"]["samples"]
    for hub in plan["hubs"]:
        values = [sample["hubs"][hub] for sample in samples]
        cores = peak_cpu_cores(scorer, samples, hub)
        advanced = samples[0]["elapsed_seconds"]
        stalled = 0
        for previous, current, sample in zip(values, values[1:], samples[1:]):
            if current["progress"] > previous["progress"]:
                advanced = sample["elapsed_seconds"]
            else:
                stalled = max(stalled, sample["elapsed_seconds"] - advanced)
        rss = max(value["rss_bytes"] for value in values) / 1024**2
        lines.append(f"| {hub} | {cores:.4f} / {plan['thresholds']['max_cpu_cores']} | {rss:.2f} / {plan['thresholds']['max_rss_bytes'] / 1024**2:.0f} | {stalled:.2f} / {plan['thresholds']['max_progress_stall_seconds']} | {values[0]['progress']} to {values[-1]['progress']} |")
    lines += ["", "Candidate primary and worker allocations:", "",
              "| Hub / swarm | Peak connections / limit | Peak public population | Minimum DAO population | Peak queue occupancy | Peak class tasks / limits |",
              "| --- | ---: | ---: | ---: | ---: | --- |"]
    for hub in plan["hubs"]:
        for network in scorer.TASK_LIMITS:
            values = [sample["hubs"][hub]["swarms"][network] for sample in samples]
            tasks = ", ".join(f"{name}: {max(value['tasks'][name] for value in values)} / {limit}"
                              for name, limit in values[0]["task_limits"].items())
            lines.append(f"| {hub} / {network} | {max(value['connections'] for value in values)} / {values[0]['connection_limit']} | {max(value['ordinary_peers'] for value in values)} | {min(value['dao_connected'] for value in values)} | {max(value['queue_occupancy'] for value in values)} | {tasks} |")
    lines += ["", "Checksums detect bundle changes; they do not authenticate the measurements.", "",
              "Run these commands from this evidence directory:", "", "```sh",
              "sha256sum -c SHA256SUMS",
              "mkdir /tmp/tn-capacity-review",
              "tar -xzf inputs.tar.gz -C /tmp/tn-capacity-review",
              "tar -xzf baseline.tar.gz -C /tmp/tn-capacity-review",
              "tar -xzf candidate.tar.gz -C /tmp/tn-capacity-review",
              "python3 -B -I /tmp/tn-capacity-review/source/qualify.py score \\",
              "  /tmp/tn-capacity-review/plan.json \\",
              "  /tmp/tn-capacity-review/baseline-evidence/evidence.json \\",
              "  /tmp/tn-capacity-review/candidate-evidence/evidence.json \\",
              "  --output /tmp/tn-capacity-review/rescored.json",
              "cmp report.json /tmp/tn-capacity-review/rescored.json", "```", "",
              "Generated validator keys and executable binaries are excluded. This qualification covers the declared Hub envelope; validator Launch qualification remains separate.", ""]
    (destination / "README.md").write_text("\n".join(lines))
    checksums = {path.name: digest(path) for path in destination.iterdir()}
    (destination / "SHA256SUMS").write_text("".join(f"{value}  {name}\n" for name, value in sorted(checksums.items())))
    print(json.dumps({"destination": str(destination), "candidate_passed": True,
                      "files": [{"path": p.name, "bytes": p.stat().st_size} for p in sorted(destination.iterdir())]}))


if __name__ == "__main__":
    main()
