"""Verify the bounded QUIC CI artifact against official metadata and current source."""

import ast
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import re
import stat
import statistics
import subprocess
import tomllib
import zipfile

BASE = Path("/private/tmp")
EVIDENCE_DIR = BASE / "1476-ci105-quic-evidence-v1"
REPO = Path("/Users/oobi/Documents/gpt8/telcoin-network-1476")
HEAD = '7a92ffd5fd87e5eb94be29a682cb24717a081c41'
MERGE = '89f6e26672124b93fe9e31a297e74c488b99a71b'
RUN = 37770301605
JOB = 113287974339
ARTIFACT = 11547354300
PRIOR_VERIFIED_HEAD = '3ecca49161bbe97bf2e9c8c6fcaf41a839f01c99'
SOURCE_PARENTS = ('422ee20086e1ef15174af0a3d454615e81494c16',)
SOURCE_CHAIN = ('422ee20086e1ef15174af0a3d454615e81494c16', HEAD)
PRIOR_RESULT = BASE / "1476-ci104-quic-verification-result.json"
PRIOR_VERIFIER = BASE / "1476-ci104-quic-verify-v1.proposed.py"
PRIOR_RESULT_SHA256 = '3f066ab5116dbf24847d8903d9fa6a8bcd0cbfb254e79c5034142751e6a182ef'
PRIOR_VERIFIER_SHA256 = '42d74983387f8b53889d3179eddaac0c5eb93839756a77b3b0138d00a070a3c3'
EXPECTED_SOURCE_FILES = set(subprocess.check_output(
    ["git", "--no-replace-objects", "-C", str(REPO), "diff", "--name-only",
     f"{HEAD}^1", HEAD], text=True).splitlines())
EXPECTED_SOURCE_FILES_SHA256 = "72a73e057fd13aaeb5911cac49b9ea239c15a00bfbb75be096a1661eaeddaaab"
LOGICAL_PACKET_TASK = 'ci105_quic_binary_verification'
ACTUAL_EXECUTOR_THREAD = '/root/ci96_prefix_review_recovery'
PR = 1502
MAX_ARCHIVE = 1024 * 1024
MAX_EXPANDED = 16 * 1024 * 1024
SOURCE_PATHS = (
    "Cargo.toml", "Cargo.lock", "prepare.py", "profile.rs", "src/main.rs", "run.py"
)
EVIDENCE_NAMES = (
    "run.json", "job.json", "artifacts-list.json", "pr.json",
    "artifact-metadata.json", "merge-rest-commit.json", "merge-recursive-tree.json",
    "job.log", "artifact.zip",
    "jobs.json", "source-commit-official.json", "capture-attempts.json",
)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        check(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def parse(raw):
    return json.loads(raw, object_pairs_hook=unique_pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))


def git_blob_sha(data):
    payload = b"blob " + str(len(data)).encode() + bytes([0]) + data
    return hashlib.sha1(payload).hexdigest()


def distribution(values):
    ordered = sorted(value / 1000 for value in values)
    return {"median_us": statistics.median(ordered),
            "p95_us": ordered[math.ceil(len(ordered) * 0.95) - 1]}


def source_constants(source):
    parsed = ast.parse(source)
    constants = {}
    for statement in parsed.body:
        if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
            target = statement.targets[0]
            if isinstance(target, ast.Name) and target.id in ("SCENARIOS", "PINNED"):
                constants[target.id] = ast.literal_eval(statement.value)
    check(set(constants) == {"SCENARIOS", "PINNED"}, "source constants missing")
    return constants


def source_bytes_at_head(path):
    value = subprocess.check_output(
        ["git", "--no-replace-objects", "-C", str(REPO), "show", f"{HEAD}:{path}"]
    )
    check(value == (REPO / path).read_bytes(), f"worktree drift: {path}")
    return value


def main():
    prior_result_raw = PRIOR_RESULT.read_bytes()
    prior_verifier_raw = PRIOR_VERIFIER.read_bytes()
    check(digest(prior_result_raw) == PRIOR_RESULT_SHA256, "prior result hash")
    check(digest(prior_verifier_raw) == PRIOR_VERIFIER_SHA256, "prior verifier hash")
    prior_result = parse(prior_result_raw)
    check(prior_result["verified"] and prior_result["head_sha"] == PRIOR_VERIFIED_HEAD and
          prior_result["verifier_sha256"] == PRIOR_VERIFIER_SHA256, "prior verification binding")
    original_tree = ast.parse(prior_verifier_raw.decode().replace("1476-ci104-quic-", "1476-ci105-quic-"))
    current_tree = ast.parse(Path(__file__).read_text())
    def assertions(syntax):
        calls = []
        for node in ast.walk(syntax):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "check":
                if (len(node.args) > 1 and isinstance(node.args[1], ast.Constant) and
                        node.args[1].value == "source commit change scope"):
                    node.args[0].comparators[0] = ast.Name(id="EXPECTED_SOURCE_FILES", ctx=ast.Load())
                if (len(node.args) > 1 and isinstance(node.args[1], ast.Constant) and
                        node.args[1].value in {"prior verification binding",
                                               "official source identity/parent",
                                                "official source parent/tree bound to pinned local source and tested merge",
                                                "source first parent descends from authenticated prior head"}):
                    node.args[0] = ast.Name(id="PARENT_AWARE_GUARD", ctx=ast.Load())
                if (len(node.args) > 1 and isinstance(node.args[1], ast.Constant) and
                        node.args[1].value == "reviewed main workflow hash"):
                    node.args[0] = ast.Name(id="PINNED_WORKFLOW_HASH_GUARD", ctx=ast.Load())
                if (len(node.args) > 1 and isinstance(node.args[1], ast.Constant) and
                        node.args[1].value == "reviewed source change list"):
                    node.args[0].values[0].comparators[0] = ast.Name(
                        id="REVIEWED_SOURCE_CHANGE_COUNT", ctx=ast.Load())
                calls.append(ast.dump(node, include_attributes=False))
        return Counter(calls)
    original_assertions, current_assertions = assertions(original_tree), assertions(current_tree)
    check(all(current_assertions[node] >= count for node, count in original_assertions.items()),
          "all prior source/merge/archive/report assertions preserved")
    evidence = {}
    for name in EVIDENCE_NAMES:
        path = EVIDENCE_DIR / name
        raw = path.read_bytes()
        evidence[name] = {"path": str(path), "bytes": len(raw), "sha256": digest(raw)}
    def load(name):
        return parse((EVIDENCE_DIR / f"{name}.json").read_bytes())

    run, job, artifact_list = load("run"), load("job"), load("artifacts-list")
    pr, artifact, commit, tree = (load(name) for name in
                                  ("pr", "artifact-metadata", "merge-rest-commit", "merge-recursive-tree"))
    jobs, source = load("jobs"), load("source-commit-official")
    check(jobs["total_count"] == 1 and jobs["jobs"] == [job], "derived job bound to official jobs response")
    check(source["sha"] == HEAD and tuple(parent["sha"] for parent in source["parents"]) == SOURCE_PARENTS,
          "official source identity/parent")
    *local_parents, local_tree = subprocess.check_output(
        ["git", "--no-replace-objects", "-C", str(REPO), "rev-parse",
         f"{HEAD}^@", f"{HEAD}^{{tree}}"],
        text=True).splitlines()
    check(tuple(local_parents) == SOURCE_PARENTS and
          source["commit"]["tree"]["sha"] == local_tree == tree["sha"],
          "official source parent/tree bound to pinned local source and tested merge")
    local_chain = subprocess.check_output(
        ["git", "--no-replace-objects", "-C", str(REPO), "rev-list",
         "--first-parent", "--reverse", f"{PRIOR_VERIFIED_HEAD}..{HEAD}"],
        text=True).splitlines()
    check(tuple(local_chain) == SOURCE_CHAIN and SOURCE_CHAIN[-1] == HEAD and
          SOURCE_CHAIN[-2] == SOURCE_PARENTS[0],
          "source first parent descends from authenticated prior head")
    check(len(EXPECTED_SOURCE_FILES) == 3 and
          digest(("\n".join(sorted(EXPECTED_SOURCE_FILES)) + "\n").encode()) ==
          EXPECTED_SOURCE_FILES_SHA256, "reviewed source change list")
    check({file["filename"] for file in source["files"]} == EXPECTED_SOURCE_FILES,
          "source commit change scope")
    check(run["id"] == RUN and run["event"] == "pull_request", "run identity/event")
    check(run["path"] == ".github/workflows/quic-handshake-profile.yaml", "workflow path")
    check(run["name"] == "QUIC handshake profile", "workflow name")
    check(run["head_sha"] == HEAD and run["run_attempt"] == 1, "run head/attempt")
    check(run["status"] == "completed" and run["conclusion"] == "success", "run conclusion")
    check(len(run["pull_requests"]) == 1 and run["pull_requests"][0]["number"] == PR,
          "run pull request")
    check(run["pull_requests"][0]["head"]["sha"] == HEAD, "run PR head")
    base = run["pull_requests"][0]["base"]["sha"]
    check(pr["number"] == PR and pr["head"]["sha"] == HEAD,
          "current PR identity/head")
    check(pr["base"]["sha"] == base and pr["merge_commit_sha"] == MERGE,
          "PR base/merge revision")
    check(job["id"] == JOB and job["run_id"] == RUN and job["run_attempt"] == 1,
          "job run/attempt")
    check(job["head_sha"] == HEAD and job["workflow_name"] == run["name"],
          "job source/workflow")
    check(job["name"] == "profile" and job["status"] == "completed" and
          job["conclusion"] == "success", "job conclusion")
    steps = {step["name"]: step["conclusion"] for step in job["steps"]}
    for name in ("Run actions/checkout@v4", "Prepare disposable instrumentation",
                 "Check profiling harness", "Build profiling harness",
                 "Verify experiments and collect samples", "Run actions/upload-artifact@v4"):
        check(steps.get(name) == "success", f"job step: {name}")
    check(artifact_list["total_count"] == 1 and len(artifact_list["artifacts"]) == 1,
          "run artifact count")
    check(artifact_list["artifacts"][0] == artifact, "single artifact API mismatch")
    check(artifact["id"] == ARTIFACT and artifact["name"] == "quic-handshake-profile" and
          not artifact["expired"], "artifact identity")
    check(artifact["workflow_run"]["id"] == RUN and
          artifact["workflow_run"]["head_sha"] == HEAD, "artifact run/head binding")
    check(artifact["workflow_run"]["repository_id"] ==
          artifact["workflow_run"]["head_repository_id"] == 780459444,
          "artifact repository binding")
    check(commit["sha"] == MERGE and [p["sha"] for p in commit["parents"]] ==
          [base, HEAD], "merge commit parents")
    check(tree["sha"] == commit["commit"]["tree"]["sha"] and not tree["truncated"],
          "merge tree identity/completeness")

    log = (EVIDENCE_DIR / "job.log").read_text()
    check(f"+{MERGE}:refs/remotes/pull/{PR}/merge" in log, "fetched merge revision")
    check(f"git checkout --progress --force refs/remotes/pull/{PR}/merge" in log,
          "checked out PR merge ref")
    check(f"HEAD is now at {MERGE[:7]}" in log and
          re.search(r"Z " + MERGE + r"\s*$", log, re.M) is not None,
          "actual checked out revision")
    for command in ("cargo +1.94 clippy --locked", "-D warnings",
                    "cargo +1.94 build --release --locked",
                    "python3 -P testing/quic-handshake/run.py --samples 50"):
        check(command in log, f"log command: {command}")

    current_head = subprocess.check_output(
        ["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True
    ).strip()
    check(current_head == HEAD, "local source HEAD")
    tracked_status = subprocess.check_output(
        ["git", "-C", str(REPO), "status", "--porcelain", "--untracked-files=no"],
        text=True
    )
    check(tracked_status == "", "tracked worktree status")
    entries = {entry["path"]: entry for entry in tree["tree"]}
    paths = (".github/workflows/quic-handshake-profile.yaml", "Cargo.lock",
             ".github/workflows/pr.yaml", "tools/hub-capacity/docker-run.py") + tuple(sorted(
        EXPECTED_SOURCE_FILES - {".github/workflows/pr.yaml"})) + tuple(
        "testing/quic-handshake/" + path for path in SOURCE_PATHS
    )
    bound = {}
    for path in paths:
        data = source_bytes_at_head(path)
        check(entries[path]["type"] == "blob" and
              entries[path]["sha"] == git_blob_sha(data), f"tested merge source: {path}")
        bound[path] = {"sha256": digest(data), "git_blob_sha1": entries[path]["sha"]}
    check(bound[".github/workflows/pr.yaml"]["sha256"] ==
          "b8e3d0ce383c723702aed7becfe00e5bf99efa8ebb87db17e9f30dcb071bb2e5",
          "reviewed main workflow hash")
    workflow = source_bytes_at_head(paths[0]).decode()
    check("uses: actions/checkout@v4" in workflow and
          "name: quic-handshake-profile" in workflow and
          "testing/quic-handshake/results/ci" in workflow, "workflow commands/artifact")
    match = re.search(r"run: python3 -P testing/quic-handshake/run.py --samples (\d+) ", workflow)
    check(match is not None, "workflow sample count")
    sample_count = int(match.group(1))
    constants = source_constants(source_bytes_at_head("testing/quic-handshake/run.py").decode())
    scenarios = tuple(constants["SCENARIOS"])
    check(len(scenarios) == len(set(scenarios)) and sample_count >= 2,
          "source scenarios/sample count")
    check(log.count(f"{sample_count} verified samples") == len(scenarios),
          "observed per-scenario execution")

    archive = (EVIDENCE_DIR / "artifact.zip").read_bytes()
    check(len(archive) == artifact["size_in_bytes"] <= MAX_ARCHIVE, "archive size bound")
    check(artifact["digest"] == "sha256:" + digest(archive), "official artifact digest")
    check(f"SHA256 digest of uploaded artifact zip is {digest(archive)}" in log,
          "upload digest in log")
    check(f"Final size is {len(archive)} bytes" in log and
          str(ARTIFACT) in log, "artifact upload size/ID in log")
    expected_members = {"report.json"} | {scenario + ".jsonl" for scenario in scenarios}
    with zipfile.ZipFile(EVIDENCE_DIR / "artifact.zip") as zipped:
        infos = zipped.infolist()
        names = [info.filename for info in infos]
        check(len(infos) == len(expected_members) and len(set(names)) == len(infos) and
              set(names) == expected_members, "archive members/duplicates")
        expanded = sum(info.file_size for info in infos)
        check(expanded <= MAX_EXPANDED, "expanded archive bound")
        payloads = {}
        for info in infos:
            name = info.filename
            check(name == Path(name).name and name not in (".", "..") and
                  not info.is_dir(), f"archive path: {name}")
            check(stat.S_IFMT(info.external_attr >> 16) == stat.S_IFREG,
                  f"archive file type: {name}")
            check(info.file_size <= MAX_ARCHIVE and not info.flag_bits & 1,
                  f"archive member bound/encryption: {name}")
            data = zipped.read(info)
            check(len(data) == info.file_size, f"archive CRC/size: {name}")
            payloads[name] = data

    report_raw = payloads["report.json"]
    report = parse(report_raw)
    check(report["schema"] == 1 and report["source_revision"] == MERGE and
          report["source_status"] == "", "report schema/source revision/status")
    check(report["samples_per_scenario"] == sample_count, "report sample count")
    check(set(report["scenarios"]) == set(scenarios) and
          set(report["source_sha256"]) == set(SOURCE_PATHS), "report scenarios/sources")
    check(report["timing_scope"] ==
          "Instrumented in-memory QUIC TLS, Ed25519 identity, P-256 certificate. No UDP or packet protection.",
          "timing scope")
    check(report["cpu_scope"] ==
          "Child process user+system CPU, both peers, setup, sampling and JSON output included.",
          "CPU scope")
    check(all(isinstance(report[key], str) and report[key] for key in
              ("platform", "machine", "rustc")), "runner provenance")
    check(re.fullmatch(r"[0-9a-f]{64}", report["binary_sha256"]) is not None,
          "binary digest format")
    for path in SOURCE_PATHS:
        key = "testing/quic-handshake/" + path
        check(report["source_sha256"][path] == bound[key]["sha256"],
              f"report source SHA: {path}")
    check(report["node_lock_sha256"] == bound["Cargo.lock"]["sha256"],
          "node lock SHA")
    node = tomllib.loads(source_bytes_at_head("Cargo.lock").decode())
    fixture = tomllib.loads(source_bytes_at_head("testing/quic-handshake/Cargo.lock").decode())
    versions = {}
    for package in constants["PINNED"]:
        node_versions = {p["version"] for p in node["package"] if p["name"] == package}
        fixture_versions = {p["version"] for p in fixture["package"] if p["name"] == package}
        check(len(node_versions) == 1 and node_versions == fixture_versions,
              f"dependency version binding: {package}")
        versions[package] = next(iter(node_versions))
    check(report["versions"] == versions, "reported dependency versions")
    manifest = tomllib.loads(source_bytes_at_head("testing/quic-handshake/Cargo.toml").decode())
    for package, version in versions.items():
        requirement = manifest["dependencies"][package]
        if isinstance(requirement, dict):
            requirement = requirement.get("version")
        check(requirement == f"={version}", f"profile manifest version pin: {package}")

    raw_hashes = {}
    rows_total = 0
    for scenario in scenarios:
        raw = payloads[scenario + ".jsonl"]
        raw_hashes[scenario] = digest(raw)
        rows = [parse(line) for line in raw.splitlines()]
        check(len(rows) == sample_count, f"raw sample count: {scenario}")
        check(all(set(row) == {"scenario", "result"} and
                  row["scenario"] == scenario and isinstance(row["result"], dict)
                  for row in rows), f"raw sample schema: {scenario}")
        results = [row["result"] for row in rows]
        check(all(type(row.get("sample")) is int for row in results) and
              sorted(row["sample"] for row in results) == list(range(sample_count)),
              f"raw sample indices: {scenario}")
        warm = results[1:]
        expected = {"raw_sha256": digest(raw)}
        if scenario in ("incompatible", "wrong-peer"):
            check(all(isinstance(row.get("rejected"), str) and row["rejected"]
                      for row in results), f"negative scenario rejection: {scenario}")
            expected.update(rejections=sample_count, reason=results[0]["rejected"])
        elif scenario.startswith("kx-"):
            fields = ("client_start_ns", "server_exchange_ns", "client_complete_ns")
            check(all(isinstance(row.get("group"), str) and
                      row["group"] == warm[0]["group"] for row in warm),
                  f"key exchange group: {scenario}")
            check(all(all(type(row.get(field)) is int and row[field] >= 0
                          for field in fields) for row in results),
                  f"key exchange timings: {scenario}")
            expected.update(group=warm[0]["group"], warm_samples=len(warm),
                            timings={field: distribution(row[field] for row in warm)
                                     for field in fields})
        else:
            kind = "Resumed" if scenario == "reconnect" else "Full"
            check(results[0].get("kind") == "Full" and
                  all(row.get("kind") == kind for row in warm),
                  f"handshake kind: {scenario}")
            check(all(isinstance(row.get("group"), str) and
                      row["group"] == warm[0]["group"] for row in warm),
                  f"handshake group: {scenario}")
            counts = ({"parse": 1, "certificate_signature": 1,
                       "extension_signature": 1} if kind == "Resumed" else
                      {"parse": 3, "certificate_signature": 3,
                       "extension_signature": 3, "transcript_signature": 1})
            fields = ("wall_ns", "server_first_flight_ns", "server_read_ns",
                      "server_peer_id_ns")
            check(all(all(type(row.get(field)) is int and row[field] >= 0
                          for field in fields) for row in results),
                  f"handshake timings: {scenario}")
            for row in warm:
                phases = row.get("server_phases_count_ns")
                check(isinstance(phases, dict) and
                      {name: value[0] for name, value in phases.items()} == counts and
                      all(type(value[1]) is int and value[1] >= 0
                          for value in phases.values()),
                      f"verification phases: {scenario}")
            expected.update(
                group=warm[0]["group"], kind=kind, warm_samples=len(warm),
                timings={field: distribution(row[field] for row in warm)
                         for field in fields},
                verification_counts_per_handshake=counts,
                verification_timings={phase: distribution(
                    row["server_phases_count_ns"][phase][1] for row in warm)
                    for phase in counts},
            )
        observed = report["scenarios"][scenario]
        check(set(observed) == set(expected) |
              {"process_wall_seconds", "process_cpu_seconds"},
              f"scenario summary keys: {scenario}")
        check({key: value for key, value in observed.items()
               if key not in ("process_wall_seconds", "process_cpu_seconds")} == expected,
              f"scenario summary values: {scenario}")
        check(all(type(observed[key]) in (int, float) and
                  math.isfinite(observed[key]) and observed[key] >= 0
                  for key in ("process_wall_seconds", "process_cpu_seconds")),
              f"process timings: {scenario}")
        rows_total += len(rows)
    check(rows_total == len(scenarios) * sample_count, "total rows")
    check(len(scenarios) == 14 and rows_total == 700, "required scenario/row totals")

    result = {
        "outcome": "authenticated", "packet_id": "052c781ae19c89a28df3ad61", "final_result": "PASS",
        "logical_packet_task": LOGICAL_PACKET_TASK, "actual_executor_thread": ACTUAL_EXECUTOR_THREAD,
        "expected_source_files": sorted(EXPECTED_SOURCE_FILES),
        "metadata_parameter_normalizations": ["source commit change scope filenames",
                                               "single-parent correction after merge, first-parent ancestry, and local full-tree equality"],
        "source_parents": list(SOURCE_PARENTS), "prior_verified_head": PRIOR_VERIFIED_HEAD,
        "source_parent_and_tree_authenticated": True,
        "prior_result": {"path": str(PRIOR_RESULT), "sha256": PRIOR_RESULT_SHA256},
        "prior_verifier": {"path": str(PRIOR_VERIFIER), "sha256": PRIOR_VERIFIER_SHA256},
        "prior_check_calls_preserved": sum(original_assertions.values()),
        "current_check_calls": sum(current_assertions.values()),
        "capture_attempts": load("capture-attempts"),
        "verified": True, "capacity_qualified": False,
        "run_id": RUN, "run_attempt": 1, "job_id": JOB, "artifact_id": ARTIFACT,
        "pr_number": PR, "head_sha": HEAD, "merge_checkout": MERGE,
        "merge_parents": [base, HEAD], "merge_tree_sha": tree["sha"],
        "source_files": bound, "source_files_tested_merge_match_head": True,
        "main_workflow_sha256": bound[".github/workflows/pr.yaml"]["sha256"],
        "workflow_samples_per_scenario": sample_count,
        "scenarios": len(scenarios), "samples": rows_total,
        "zip_members": len(expected_members), "zip_bytes": len(archive),
        "zip_expanded_bytes": expanded, "zip_limit_bytes": MAX_ARCHIVE,
        "expanded_limit_bytes": MAX_EXPANDED, "zip_sha256": digest(archive),
        "report_sha256": digest(report_raw), "raw_sha256": raw_hashes,
        "all_zip_members_crc_paths_types_valid": True,
        "all_report_summaries_match_raw": True,
        "node_lock_matches_tested_merge": True,
        "evidence": evidence,
        "verifier_sha256": digest(Path(__file__).read_bytes()),
    }
    output = BASE / "1476-ci105-quic-verification-result.json"
    with output.open("x") as stream:
        stream.write(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print(json.dumps({"result_path": str(output), "result_sha256": digest(output.read_bytes()),
                      "run_id": RUN, "job_id": JOB, "artifact_id": ARTIFACT,
                      "head_sha": HEAD, "merge_checkout": MERGE,
                      "scenarios": len(scenarios), "samples": rows_total,
                      "zip_members": len(expected_members), "zip_bytes": len(archive),
                      "zip_expanded_bytes": expanded,
                      "verified": True, "capacity_qualified": False}, sort_keys=True))


if __name__ == "__main__":
    main()
