"""Verify the bounded QUIC CI artifact against official metadata and current source."""

import argparse
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

IDENTITY_PATH = Path(__file__).with_name("ci122_runner_identity.py")
if (IDENTITY_PATH.is_symlink() or not IDENTITY_PATH.is_file()
        or IDENTITY_PATH.stat().st_size > 4096):
    raise ValueError("runner identity helper is not a bounded regular sibling")
IDENTITY_RAW = IDENTITY_PATH.read_bytes()
if len(IDENTITY_RAW) > 4096 or hashlib.sha256(IDENTITY_RAW).hexdigest() != "649197fc31e9ecf5dadb5de5675c19c8ed28e0e0b6f947fb7ac5b3d985f97413":
    raise ValueError("runner identity helper bytes changed")
IDENTITY_NAMESPACE = {}
exec(compile(IDENTITY_RAW, str(IDENTITY_PATH), "exec"), IDENTITY_NAMESPACE)
runner_identity = IDENTITY_NAMESPACE["runner_identity"]

BASE = None
EVIDENCE_DIR = None
REPO = None
HEAD = '3914e53957fcc3ff7befe8b1a3f9a3284bfaba4b'
MERGE = HEAD  # Legacy checkout identity variable now binds the exact manual head.
HEAD_BRANCH = "feat/1476-public-hub-capacity"
REVIEWED_SOURCE_TREE = '6175a752c787eef23cfefe76324a735d5f2baef2'
RUN_BASE = '07d6feaa86475479993475fe3fdf368eabc695b7'
EXPECTED_ARTIFACT_SIZE = 23790
EXPECTED_ARTIFACT_SHA256 = '70a4f96919c6aaff2508c5100092c61442e2a077aa55f65d7a027dbc30218e88'
RUN = 37990580614
JOB = 114023405270
ARTIFACT = 11645521069
PRIOR_VERIFIED_HEAD = '7a92ffd5fd87e5eb94be29a682cb24717a081c41'
SOURCE_PARENTS = ('a45a21df90b60939f14ae1336bfa64585570043f',)
SOURCE_CHAIN = ('cd4433858bc0c3b85d753169c0a2284f0adcc005', '5d78548262678266509db061e6e799ea346b8d4b', '1e975450bac2bb8457e95fa5f0d89a8a92d757dc', 'c7ca6a658b3e2ee569f62664edf101098fe1d5aa', '125fc0e8d967297044736cb0d2fa3332f727a82e', '90e286cb2c3029263aea80c45c97dc75e79d6b1b', '3b20c3f39e099fb511fe85226496e46001a1050b', 'ee6bffdb1aec0c495c512cb2e1d9f8f08577c06f', '7eb2f2973a4e4af9438aa7f199af791a3c2a3d01', 'a9ec144cf9310929ec67f3604d70e0c5422a490f', '7987a08d48bbfa0eadf9b3ba72e35d4cca178433', '6bdf846ef83283251312da7a1a2f27c472db2b07', '49877c35f6ab8ff61ea3d41c802f9487e62a3670', 'ef552eb5be7dbb89b8a6a6e9dd88a3a67f5e4ebe', 'b72ce3d2351dc7d423ea6aa2c7ff3a65d07fbaea', '750df5a0d6563693c16b0560a213d59259008921', '68fdb4b955a1ddcf8587c4df371489711d84e7aa', '4556f835e3d4a6f803afc941f4cfcd9cfd7faad1', 'ca02e454b4f2fa5f5e1a47db8e346fb1bec00666', 'eeae47d83a935086fcda8a5da602b5867b89663f', 'c24b810047c1a3183ef5bc9ee7a423a2fd21cb2b', 'a45a21df90b60939f14ae1336bfa64585570043f', '3914e53957fcc3ff7befe8b1a3f9a3284bfaba4b')
PRIOR_RESULT = None
PRIOR_VERIFIER = None
PRIOR_RESULT_SHA256 = 'f67333ccabab62fdd1b23f7cffac282f9bafcfc5427d1dadd0bcaa869bade364'
PRIOR_VERIFIER_SHA256 = '9f9a87b1afe4652b054dabd6e695f14b27bfc255c52a76e18455edfb9834ed4b'
EXPECTED_SOURCE_FILES = set()
EXPECTED_SOURCE_FILES_SHA256 = '47025988ebdfcde9f1be8701becbfdbc34d89f87f5b9574f0ee77865f692de93'
FULL_SOURCE_FILES = set()
EXPECTED_FULL_SOURCE_FILES_SHA256 = '9811fd0e09ba9bb6b9f7d1c95b443153826962aa44e9f739945bb421b3e02260'
STAGING_BASE = 'b6805f8a53d9b2f5fa0f48a789a3cbe513d7a5ad'
INTEGRATION_COMMIT = '49877c35f6ab8ff61ea3d41c802f9487e62a3670'
INTEGRATION_FIRST_PARENT = '6bdf846ef83283251312da7a1a2f27c472db2b07'
REVIEWED_INTEGRATION_TREE = '1092ca53d5938977df2abba2ab7248fb59f194cd'
LOGICAL_PACKET_TASK = 'ci122_artifact_readers'
ACTUAL_EXECUTOR_THREAD = None
OUTPUT = None
PR = 1502
MAX_ARCHIVE = 1024 * 1024
MAX_EXPANDED = 16 * 1024 * 1024
SOURCE_PATHS = (
    "Cargo.toml", "Cargo.lock", "prepare.py", "profile.rs", "src/main.rs", "run.py"
)
EVIDENCE_NAMES = (
    "run.json", "job.json", "artifacts-list.json", "pr.json", "staging-ref.json",
    "artifact-metadata-official.json", "merge-rest-commit.json", "merge-recursive-tree-v2.json",
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


def configure(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--prior-result", type=Path, required=True)
    parser.add_argument("--prior-verifier", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    global REPO, EVIDENCE_DIR, PRIOR_RESULT, PRIOR_VERIFIER, OUTPUT, BASE
    global EXPECTED_SOURCE_FILES, FULL_SOURCE_FILES, ACTUAL_EXECUTOR_THREAD
    REPO = args.repo.resolve(strict=True)
    EVIDENCE_DIR = args.evidence_dir.resolve(strict=True)
    PRIOR_RESULT = args.prior_result.resolve(strict=True)
    PRIOR_VERIFIER = args.prior_verifier.resolve(strict=True)
    OUTPUT = args.output.absolute()
    BASE = OUTPUT.parent.resolve(strict=True)
    ACTUAL_EXECUTOR_THREAD = runner_identity()
    EXPECTED_SOURCE_FILES = set(subprocess.check_output(
        ["git", "--no-replace-objects", "-C", str(REPO), "diff", "--name-only",
         f"{HEAD}^1", HEAD], text=True).splitlines())
    FULL_SOURCE_FILES = set(subprocess.check_output(
        ["git", "--no-replace-objects", "-C", str(REPO), "diff", "--name-only",
         PRIOR_VERIFIED_HEAD, HEAD], text=True).splitlines())


def main():
    prior_result_raw = PRIOR_RESULT.read_bytes()
    prior_verifier_raw = PRIOR_VERIFIER.read_bytes()
    check(digest(prior_result_raw) == PRIOR_RESULT_SHA256, "prior result hash")
    check(digest(prior_verifier_raw) == PRIOR_VERIFIER_SHA256, "prior verifier hash")
    prior_result = parse(prior_result_raw)
    check(prior_result["verified"] and prior_result["head_sha"] == PRIOR_VERIFIED_HEAD and
          prior_result["verifier_sha256"] == PRIOR_VERIFIER_SHA256, "prior verification binding")
    original_tree = ast.parse(prior_verifier_raw.decode().replace("1476-ci105-quic-", "1476-ci122-quic-"))
    current_tree = ast.parse(Path(__file__).read_text())
    def assertions(syntax):
        # Recognize the reviewed baseline and current sides before normalizing guards.
        heads = [ast.literal_eval(statement.value) for statement in syntax.body
                 if isinstance(statement, ast.Assign) and len(statement.targets) == 1
                 and isinstance(statement.targets[0], ast.Name)
                 and statement.targets[0].id == "HEAD"]
        if heads == [PRIOR_VERIFIED_HEAD]:
            current = False
        elif heads == [HEAD]:
            current = True
        else:
            raise AssertionError("unreviewed assertion source identity")
        forms = {
            'official source parent/tree bound to pinned local source and tested merge': (
                "check(tuple(local_parents) == SOURCE_PARENTS and source['commit']['tree']['sha'] == local_tree == tree['sha'], 'official source parent/tree bound to pinned local source and tested merge')",
                "check(tuple(local_parents) == SOURCE_PARENTS and source['commit']['tree']['sha'] == local_tree == tree['sha'] == REVIEWED_SOURCE_TREE, 'official source parent/tree bound to pinned local source and tested merge')"),
            'reviewed source change list': (
                "check(len(EXPECTED_SOURCE_FILES) == 3 and digest(('\\n'.join(sorted(EXPECTED_SOURCE_FILES)) + '\\n').encode()) == EXPECTED_SOURCE_FILES_SHA256, 'reviewed source change list')",
                "check(len(EXPECTED_SOURCE_FILES) == 1 and digest(('\\n'.join(sorted(EXPECTED_SOURCE_FILES)) + '\\n').encode()) == EXPECTED_SOURCE_FILES_SHA256, 'reviewed source change list')"),
            'run identity/event': (
                "check(run['id'] == RUN and run['event'] == 'pull_request', 'run identity/event')",
                "check(run['id'] == RUN and run['event'] == 'workflow_dispatch', 'run identity/event')"),
            'PR base/merge revision': (
                "check(pr['base']['sha'] == base and pr['merge_commit_sha'] == MERGE, 'PR base/merge revision')",
                "check(pr['base']['sha'] == base and pr['head']['sha'] == MERGE == HEAD == commit['sha'], 'PR base/merge revision')"),
            'merge commit parents': (
                "check(commit['sha'] == MERGE and [p['sha'] for p in commit['parents']] == [base, HEAD], 'merge commit parents')",
                "check(commit['sha'] == MERGE and [p['sha'] for p in commit['parents']] == list(SOURCE_PARENTS), 'merge commit parents')"),
            'fetched merge revision': (
                "check(f'+{MERGE}:refs/remotes/pull/{PR}/merge' in log, 'fetched merge revision')",
                "check(f'+{HEAD}:refs/remotes/origin/{HEAD_BRANCH}' in log, 'fetched merge revision')"),
            'checked out PR merge ref': (
                "check(f'git checkout --progress --force refs/remotes/pull/{PR}/merge' in log, 'checked out PR merge ref')",
                "check(f'git checkout --progress --force -B {HEAD_BRANCH} refs/remotes/origin/{HEAD_BRANCH}' in log, 'checked out PR merge ref')"),
            'actual checked out revision': (
                "check(f'HEAD is now at {MERGE[:7]}' in log and re.search('Z ' + MERGE + '\\\\s*$', log, re.M) is not None, 'actual checked out revision')",
                'check(f"Switched to a new branch \'{HEAD_BRANCH}\'" in log and re.search(\'Z \' + MERGE + \'\\\\s*$\', log, re.M) is not None, \'actual checked out revision\')'),
            'reviewed main workflow hash': (
                "check(bound['.github/workflows/pr.yaml']['sha256'] == 'b8e3d0ce383c723702aed7becfe00e5bf99efa8ebb87db17e9f30dcb071bb2e5', 'reviewed main workflow hash')",
                "check(bound['.github/workflows/pr.yaml']['sha256'] == '9148a4a5a5927caa2c6016ffac91a20d97203ff321b59098ece66c79c36b1c65', 'reviewed main workflow hash')"),
            'archive size bound': (
                "check(len(archive) == artifact['size_in_bytes'] <= MAX_ARCHIVE, 'archive size bound')",
                "check(len(archive) == artifact['size_in_bytes'] == EXPECTED_ARTIFACT_SIZE <= MAX_ARCHIVE, 'archive size bound')"),
            'official artifact digest': (
                "check(artifact['digest'] == 'sha256:' + digest(archive), 'official artifact digest')",
                "check(artifact['digest'] == 'sha256:' + digest(archive) == 'sha256:' + EXPECTED_ARTIFACT_SHA256, 'official artifact digest')"),
        }
        additions = {
            'reviewed full source change list': "check(len(FULL_SOURCE_FILES) == 73 and digest(('\\n'.join(sorted(FULL_SOURCE_FILES)) + '\\n').encode()) == EXPECTED_FULL_SOURCE_FILES_SHA256 and (EXPECTED_SOURCE_FILES <= FULL_SOURCE_FILES), 'reviewed full source change list')",
            'reviewed staging integration parent/tree': "check(integration_parents == [INTEGRATION_FIRST_PARENT, STAGING_BASE, REVIEWED_INTEGRATION_TREE], 'reviewed staging integration parent/tree')",
            'actual staging ref': "check(base == RUN_BASE and staging_ref['ref'] == 'refs/heads/staging/mavenrain-2026-10-09' and (staging_ref['object']['sha'] == RUN_BASE), 'actual staging ref')",
        }
        calls, raw_calls = [], Counter()
        for node in ast.walk(syntax):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "check":
                raw = ast.dump(node, include_attributes=False)
                raw_calls[raw] += 1
                name = node.args[1].value if (len(node.args) > 1 and
                       isinstance(node.args[1], ast.Constant)) else None
                if name in forms:
                    old, new = forms[name]
                    expected = ast.dump(ast.parse(new if current else old, mode="eval").body,
                                        include_attributes=False)
                    if raw != expected:
                        raise AssertionError(f"unreviewed assertion form: {name}")
                    raw = ast.dump(ast.parse(old, mode="eval").body, include_attributes=False)
                calls.append(raw)
        if len(calls) != (85 if current else 82):
            raise AssertionError("reviewed assertion cardinality")
        if current:
            for name, form in additions.items():
                expected = ast.dump(ast.parse(form, mode="eval").body, include_attributes=False)
                if raw_calls[expected] != 1:
                    raise AssertionError(f"reviewed added assertion missing: {name}")
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
                                  ("pr", "artifact-metadata-official", "merge-rest-commit",
                                   "merge-recursive-tree-v2"))
    jobs, source = load("jobs"), load("source-commit-official")
    staging_ref = load("staging-ref")
    check(jobs["total_count"] == 1 and jobs["jobs"] == [job], "derived job bound to official jobs response")
    check(source["sha"] == HEAD and tuple(parent["sha"] for parent in source["parents"]) == SOURCE_PARENTS,
          "official source identity/parent")
    *local_parents, local_tree = subprocess.check_output(
        ["git", "--no-replace-objects", "-C", str(REPO), "rev-parse",
         f"{HEAD}^@", f"{HEAD}^{{tree}}"],
        text=True).splitlines()
    check(tuple(local_parents) == SOURCE_PARENTS and
          source["commit"]["tree"]["sha"] == local_tree == tree["sha"] == REVIEWED_SOURCE_TREE,
          "official source parent/tree bound to pinned local source and tested merge")
    local_chain = subprocess.check_output(
        ["git", "--no-replace-objects", "-C", str(REPO), "rev-list",
         "--first-parent", "--reverse", f"{PRIOR_VERIFIED_HEAD}..{HEAD}"],
        text=True).splitlines()
    check(tuple(local_chain) == SOURCE_CHAIN and SOURCE_CHAIN[-1] == HEAD and
          SOURCE_CHAIN[-2] == SOURCE_PARENTS[0],
          "source first parent descends from authenticated prior head")
    check(len(EXPECTED_SOURCE_FILES) == 1 and
          digest(("\n".join(sorted(EXPECTED_SOURCE_FILES)) + "\n").encode()) ==
          EXPECTED_SOURCE_FILES_SHA256, "reviewed source change list")
    check(len(FULL_SOURCE_FILES) == 73 and
          digest(("\n".join(sorted(FULL_SOURCE_FILES)) + "\n").encode()) ==
          EXPECTED_FULL_SOURCE_FILES_SHA256 and
          EXPECTED_SOURCE_FILES <= FULL_SOURCE_FILES, "reviewed full source change list")
    integration_parents = subprocess.check_output(
        ["git", "--no-replace-objects", "-C", str(REPO), "rev-parse",
         f"{INTEGRATION_COMMIT}^1", f"{INTEGRATION_COMMIT}^2",
         f"{INTEGRATION_COMMIT}^{{tree}}"], text=True).splitlines()
    check(integration_parents == [INTEGRATION_FIRST_PARENT, STAGING_BASE, REVIEWED_INTEGRATION_TREE],
          "reviewed staging integration parent/tree")
    check({file["filename"] for file in source["files"]} == EXPECTED_SOURCE_FILES,
          "source commit change scope")
    check(run["id"] == RUN and run["event"] == "workflow_dispatch", "run identity/event")
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
    check(base == RUN_BASE and staging_ref["ref"] ==
          "refs/heads/staging/mavenrain-2026-10-09" and
          staging_ref["object"]["sha"] == RUN_BASE, "actual staging ref")
    check(pr["base"]["sha"] == base and pr["head"]["sha"] == MERGE == HEAD == commit["sha"],
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
          list(SOURCE_PARENTS), "merge commit parents")
    check(tree["sha"] == commit["commit"]["tree"]["sha"] and not tree["truncated"],
          "merge tree identity/completeness")

    log = (EVIDENCE_DIR / "job.log").read_text()
    check(f"+{HEAD}:refs/remotes/origin/{HEAD_BRANCH}" in log, "fetched merge revision")
    check(f"git checkout --progress --force -B {HEAD_BRANCH} refs/remotes/origin/{HEAD_BRANCH}" in log,
          "checked out PR merge ref")
    check(f"Switched to a new branch '{HEAD_BRANCH}'" in log and
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
    paths = tuple(sorted({".github/workflows/quic-handshake-profile.yaml", "Cargo.lock",
                          ".github/workflows/pr.yaml", "tools/hub-capacity/docker-run.py"}
                         | FULL_SOURCE_FILES
                         | {"testing/quic-handshake/" + path for path in SOURCE_PATHS}))
    bound = {}
    for path in paths:
        data = source_bytes_at_head(path)
        check(entries[path]["type"] == "blob" and
              entries[path]["sha"] == git_blob_sha(data), f"tested merge source: {path}")
        bound[path] = {"sha256": digest(data), "git_blob_sha1": entries[path]["sha"]}
    check(bound[".github/workflows/pr.yaml"]["sha256"] ==
          "9148a4a5a5927caa2c6016ffac91a20d97203ff321b59098ece66c79c36b1c65",
          "reviewed main workflow hash")
    workflow = source_bytes_at_head(".github/workflows/quic-handshake-profile.yaml").decode()
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
    check(len(archive) == artifact["size_in_bytes"] == EXPECTED_ARTIFACT_SIZE <= MAX_ARCHIVE, "archive size bound")
    check(artifact["digest"] == "sha256:" + digest(archive) == "sha256:" + EXPECTED_ARTIFACT_SHA256, "official artifact digest")
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
        "outcome": "authenticated", "packet_id": "9d860d1dc4964c7a2796601d", "final_result": "PASS",
        "logical_packet_task": LOGICAL_PACKET_TASK, "actual_executor_thread": ACTUAL_EXECUTOR_THREAD,
        "expected_source_files": sorted(EXPECTED_SOURCE_FILES),
        "full_source_files_since_prior_verified_head": sorted(FULL_SOURCE_FILES),
        "actual_staging_ref": RUN_BASE, "integration_commit": INTEGRATION_COMMIT,
        "integration_tree_sha": REVIEWED_INTEGRATION_TREE,
        "metadata_parameter_normalizations": ["source commit change scope filenames",
                                               "single-parent correction after merge, first-parent ancestry, and local full-tree equality",
                                               "manual event, exact head checkout, and current run PR base",
                                               "reviewed full-source count; fixed artifact size and digest strengthen original guards"],
        "source_parents": list(SOURCE_PARENTS), "prior_verified_head": PRIOR_VERIFIED_HEAD,
        "source_parent_and_tree_authenticated": True,
        "prior_result": {"path": str(PRIOR_RESULT), "sha256": PRIOR_RESULT_SHA256},
        "prior_verifier": {"path": str(PRIOR_VERIFIER), "sha256": PRIOR_VERIFIER_SHA256},
        "prior_check_calls_preserved": sum(original_assertions.values()),
        "current_check_calls": sum(current_assertions.values()),
        "capture_attempts": load("capture-attempts"),
        "verified": True, "capacity_qualified": False,
        "run_id": RUN, "run_attempt": 1, "job_id": JOB, "artifact_id": ARTIFACT,
        "pr_number": PR, "head_sha": HEAD, "checkout_sha": MERGE,
        "workflow_event": "workflow_dispatch", "checkout_kind": "manual branch head",
        "checkout_parents": list(SOURCE_PARENTS), "checkout_tree_sha": tree["sha"],
        "source_files": bound, "source_files_tested_checkout_match_head": True,
        "main_workflow_sha256": bound[".github/workflows/pr.yaml"]["sha256"],
        "workflow_samples_per_scenario": sample_count,
        "scenarios": len(scenarios), "samples": rows_total,
        "zip_members": len(expected_members), "zip_bytes": len(archive),
        "zip_expanded_bytes": expanded, "zip_limit_bytes": MAX_ARCHIVE,
        "expanded_limit_bytes": MAX_EXPANDED, "zip_sha256": digest(archive),
        "report_sha256": digest(report_raw), "raw_sha256": raw_hashes,
        "all_zip_members_crc_paths_types_valid": True,
        "all_report_summaries_match_raw": True,
        "node_lock_matches_tested_checkout": True,
        "evidence": evidence,
        "verifier_sha256": digest(Path(__file__).read_bytes()),
    }
    output = OUTPUT
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
    configure()
    main()
