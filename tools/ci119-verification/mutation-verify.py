"""Verify retained mutation results and logs against an explicit trusted Git revision."""

import argparse
import ast
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import tomllib
from zipfile import ZipFile


REGISTRY_SHA256 = "d8c30535ca16b127e4cf840849641e79b9ecfcc373bd9e254aba8fbdbd92c1e1"
ACCEPTED70_REVISION = "750df5a0d6563693c16b0560a213d59259008921"
ACCEPTED70_SHA256 = "38668e3df257d1e39da4f9cd231c5c7d924947b7edfc150b788ddb63952977d8"
EXPECTED_INGRESS_CASES = [
    ("worker_ingress_lane_fairness", "crates/consensus/worker/src/network/ingress.rs",
     "self.turn = second;", "self.turn = first;",
     "ready_lanes_alternate_without_starving_epoch_events"),
    ("worker_ingress_epoch_gap_retention", "crates/consensus/worker/src/network/ingress.rs",
     "pending.push_back(stream);", "pending.push_back(stream);\n                drop(pending.pop_back());",
     "epoch_receiver_gap_preserves_stream_permit_and_source_epoch"),
    ("worker_ingress_shared_peer_admission", "crates/consensus/worker/src/network/ingress.rs",
     "let decision = try_admit_sync(&pool.stream_semaphore, &pool.peers, peer)",
     "let decision = try_admit_sync(&pool.stream_semaphore, &Default::default(), peer)",
     "pending_and_active_streams_share_admission_bounds"),
    ("worker_ingress_epoch_gap_expiry", "crates/consensus/worker/src/network/ingress.rs",
     ".map(|stream| stream.deadline)", ".map(|stream| stream.deadline + SYNC_REQUEST_READ_TIMEOUT)",
     "receiver_gap_expiry_releases_permits_without_a_consumer"),
    ("worker_ingress_unpolled_expiry_owner", "crates/consensus/worker/src/network/ingress.rs",
     "let owner = ExpiryOwner(self.clone());", "let owner = std::mem::ManuallyDrop::new(ExpiryOwner(self.clone()));",
     "unpolled_expiry_owner_releases_pending_and_rejects_late_streams"),
]


def exact_command(result, exit_code, argv):
    if (not isinstance(result, dict) or set(result) != {"exit_code", "argv", "log", "sha256"}
            or type(result["exit_code"]) is not int
            or not isinstance(result["log"], str)
            or not isinstance(result["sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", result["sha256"])
            or result["exit_code"] != exit_code or result["argv"] != argv):
        raise ValueError("mutation result is not from the exact required command")


def strict_json(raw):
    """Reject duplicate fields and non-finite constants in already bounded bytes."""
    def unique(pairs):
        result = {}
        for name, value in pairs:
            if name in result:
                raise ValueError("duplicate mutation JSON field")
            result[name] = value
        return result

    def nonfinite(value):
        raise ValueError("non-finite mutation JSON constant")

    return json.loads(raw, object_pairs_hook=unique, parse_constant=nonfinite)


def retained_bytes(root, relative, limit):
    path = root / relative
    if (path.is_symlink() or not path.is_file()
            or not path.resolve(strict=True).is_relative_to(root)
            or path.stat().st_size > limit):
        raise ValueError("mutation evidence is nonregular, outside root or exceeds its bound")
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("mutation evidence exceeds its byte bound while reading")
    return raw


def check_manifest(manifest, provenance, names, report):
    fields = {"version", "provenance", "cases", "case_count", "log_count", "complete", "shards"}
    if (not isinstance(manifest, dict) or set(manifest) != fields
            or type(manifest["version"]) is not int or manifest["version"] != 1
            or manifest["provenance"] != provenance or manifest["cases"] != names
            or type(manifest["case_count"]) is not int or manifest["case_count"] != 75
            or type(manifest["log_count"]) is not int or manifest["log_count"] != 225
            or manifest["complete"] is not True
            or not isinstance(manifest["shards"], list) or len(manifest["shards"]) != 2):
        raise ValueError("aggregate manifest schema, provenance or completeness mismatch")
    for index, summary in enumerate(manifest["shards"]):
        if (not isinstance(summary, dict)
                or set(summary) != {"shard_index", "manifest_sha256", "report_sha256"}
                or type(summary["shard_index"]) is not int or summary["shard_index"] != index):
            raise ValueError("aggregate shard summaries are not exact ordered indices")
        shard = {"version": 1, "provenance": provenance, "shard_index": index,
                 "shard_count": 2, "cases": names[index::2], "complete": True}
        manifest_raw = (json.dumps(shard, sort_keys=True) + "\n").encode()
        report_raw = (json.dumps(report[index::2], allow_nan=False,
                                 sort_keys=True, indent=2) + "\n").encode()
        if (summary["manifest_sha256"] != hashlib.sha256(manifest_raw).hexdigest()
                or summary["report_sha256"] != hashlib.sha256(report_raw).hexdigest()):
            raise ValueError("canonical shard digest consistency mismatch")


def expected_commands(relative, regression, blob, trusted_paths, workspace):
    """Derive exact Cargo argv from files and manifests at the pinned commit."""
    if not isinstance(relative, str) or not relative or "\\" in relative:
        raise ValueError("mutation source path is not canonical")
    path = PurePosixPath(relative)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != relative:
        raise ValueError("mutation source path is not canonical")
    if relative not in trusted_paths:
        raise ValueError("mutation source is not tracked at the trusted revision")
    manifests = [(parent / "Cargo.toml").as_posix() for parent in path.parents
                 if (parent / "Cargo.toml").as_posix() in trusted_paths]
    if not manifests:
        raise ValueError("mutation source has no trusted owning manifest")
    manifest = manifests[0]
    owner = PurePosixPath(manifest).parent.as_posix()
    document = tomllib.loads(blob(manifest).decode())
    package = document.get("package", {}).get("name")
    if not isinstance(package, str) or not package:
        raise ValueError("trusted owning manifest has no package name")
    members = workspace.get("members", [])
    exclusions = workspace.get("exclude", [])
    if not isinstance(members, list) or not isinstance(exclusions, list):
        raise ValueError("trusted workspace membership is malformed")
    member = owner in members
    excluded = owner in exclusions
    if member == excluded:
        raise ValueError("owning package is absent from or ambiguous in workspace")
    # Resolve a standalone patch only when its lockfile is tracked at this commit.
    standalone = excluded and (PurePosixPath(owner) / "Cargo.lock").as_posix() in trusted_paths
    inside = path.relative_to(PurePosixPath(owner))
    target = []
    if len(inside.parts) == 2 and inside.parts[0] == "examples" and inside.suffix == ".rs":
        name = inside.stem
        examples = document.get("example", [])
        if not isinstance(examples, list):
            raise ValueError("trusted example targets are malformed")
        named = [item for item in examples if item.get("name") == name or
                 item.get("path") == inside.as_posix()]
        if named:
            if (len(named) != 1 or named[0].get("name") != name or
                    named[0].get("path", inside.as_posix()) != inside.as_posix()):
                raise ValueError("ambiguous trusted example target")
        elif document["package"].get("autoexamples", True) is not True:
            raise ValueError("example target is not enabled by trusted manifest")
        target = ["--example", name]
    elif inside.parts and inside.parts[0] == "src" and inside.suffix == ".rs":
        if excluded:
            library = document.get("lib")
            if library is None and document["package"].get("autolib", True) is True:
                library = {"path": "src/lib.rs"}
            if not isinstance(library, dict):
                raise ValueError("excluded source has no trusted library target")
            libpath = library.get("path")
            if (libpath != "src/lib.rs" or
                    (PurePosixPath(owner) / libpath).as_posix() not in trusted_paths):
                raise ValueError("excluded source has unsupported trusted library target")
            if inside.as_posix() == "src/main.rs":
                raise ValueError("excluded binary source cannot be selected as a library")
            target = ["--lib"]
    else:
        raise ValueError("mutation source has unsupported Cargo target layout")
    selection = ["--manifest-path", manifest] if standalone else ["-p", package]
    compile_argv = ["cargo", "+1.94", "test", "--locked", *selection, "--no-run", *target]
    test_argv = ["cargo", "+1.94", "nextest", "run", "--locked", *selection,
                 "-E", f"test({regression})", "--no-tests", "fail", "--test-threads", "1", *target]
    return package, compile_argv, test_argv


def verify(root, source, revision, archive, archive_sha256, run_id, run_attempt):
    """Bind the complete registry, commands, source restoration and selected test logs."""
    root = root.resolve(strict=True)
    source = source.resolve(strict=True)
    archive = archive.resolve(strict=True)
    if (not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision)
            or any(not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]{0,19}", value)
                   for value in (run_id, run_attempt))):
        raise ValueError("revision and official run identity must be explicit canonical values")
    if not isinstance(archive_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", archive_sha256):
        raise ValueError("expected archive SHA256 must come from independent CI metadata")
    if not archive.is_file() or archive.stat().st_size > 1 * 1024**2:
        raise ValueError("mutation archive exceeds the existing compressed bound")
    with archive.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != archive_sha256:
            raise ValueError("mutation archive differs from its independent CI SHA256")

    def archived(relative, raw, limit):
        with ZipFile(archive) as retained:
            members = [info for info in retained.infolist() if info.filename == relative]
            if len(members) != 1 or members[0].file_size > limit:
                raise ValueError("mutation archive entry is missing, duplicated or too large")
            if retained.read(members[0]) != raw:
                raise ValueError("mutation evidence differs from the checksum-bound CI archive")

    commit = subprocess.check_output(
        ["git", "rev-parse", "--verify", revision + "^{commit}"], cwd=source, text=True).strip()
    if commit != revision:
        raise ValueError("qualification revision must be a full commit hash")

    def blob(relative):
        return subprocess.check_output(["git", "show", commit + ":" + relative], cwd=source)

    tracked_raw = subprocess.check_output(
        ["git", "ls-tree", "-rz", "--name-only", commit], cwd=source)
    trusted_paths = {entry.decode() for entry in tracked_raw.split(b"\0") if entry}
    workspace = tomllib.loads(blob("Cargo.toml").decode())["workspace"]
    registry_raw = blob("tools/hub-capacity/mutate-rust.py")
    if hashlib.sha256(registry_raw).hexdigest() != REGISTRY_SHA256:
        raise ValueError("trusted registry bytes differ from the exact 75-case registry")
    registry = ast.parse(registry_raw)
    declarations = [node for node in registry.body if isinstance(node, ast.Assign)
                    and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
                    and node.targets[0].id in {"CASES", "PUBLIC_ADMISSION_CASES", "INGRESS_CASES"}]
    base = [node for node in declarations if node.targets[0].id == "CASES"]
    admission = [node for node in declarations
                 if node.targets[0].id == "PUBLIC_ADMISSION_CASES"]
    ingress = [node for node in declarations if node.targets[0].id == "INGRESS_CASES"]
    append = [node for node in registry.body if isinstance(node, ast.AugAssign)
              and isinstance(node.target, ast.Name) and node.target.id == "CASES"]
    if (len(base) != 1 or len(admission) != 1 or len(append) != 2
            or not base[0].lineno < admission[0].lineno < append[0].lineno
            or not isinstance(append[0].op, ast.Add)
            or not isinstance(append[0].value, ast.Name)
            or append[0].value.id != "PUBLIC_ADMISSION_CASES"
            or len(ingress) != 1
            or not append[0].lineno < ingress[0].lineno < append[1].lineno
            or not isinstance(append[1].op, ast.Add)
            or not isinstance(append[1].value, ast.Name)
            or append[1].value.id != "INGRESS_CASES"):
        raise ValueError("trusted source must declare the exact base, admission and ingress registries")
    original_cases = ast.literal_eval(base[0].value)
    admission_cases = ast.literal_eval(admission[0].value)
    ingress_cases = ast.literal_eval(ingress[0].value)
    if (not isinstance(original_cases, (list, tuple)) or len(original_cases) != 58
            or not isinstance(admission_cases, (list, tuple)) or len(admission_cases) != 12
            or not isinstance(ingress_cases, (list, tuple)) or len(ingress_cases) != 5):
        raise ValueError("trusted mutation registry size or append layout differs")
    previous_raw = subprocess.check_output(
        ["git", "show", "768fc0b04812a7e9be9a1ed53564d05e42021300:tools/hub-capacity/mutate-rust.py"],
        cwd=source)
    if hashlib.sha256(previous_raw).hexdigest() != "4e394bb1c274537e718514914d1552326b5e05a8c83891683139549c40e23d58":
        raise ValueError("previous trusted registry differs")
    previous_registry = ast.parse(previous_raw)
    previous_base = [node for node in previous_registry.body if isinstance(node, ast.Assign)
                     and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
                     and node.targets[0].id == "CASES"]
    if len(previous_base) != 1:
        raise ValueError("previous trusted registry declaration differs")
    previous_cases = ast.literal_eval(previous_base[0].value)
    if [case[0] for case in original_cases] != [case[0] for case in previous_cases]:
        raise ValueError("original 58 mutation IDs changed or were reordered")
    accepted70_raw = subprocess.check_output(
        ["git", "show", ACCEPTED70_REVISION + ":tools/hub-capacity/mutate-rust.py"], cwd=source)
    if hashlib.sha256(accepted70_raw).hexdigest() != ACCEPTED70_SHA256:
        raise ValueError("accepted 70-case registry bytes differ")
    accepted70_registry = ast.parse(accepted70_raw)
    accepted70_base = [node for node in accepted70_registry.body if isinstance(node, ast.Assign)
                       and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
                       and node.targets[0].id == "CASES"]
    accepted70_admission = [node for node in accepted70_registry.body if isinstance(node, ast.Assign)
                            and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
                            and node.targets[0].id == "PUBLIC_ADMISSION_CASES"]
    if len(accepted70_base) != 1 or len(accepted70_admission) != 1:
        raise ValueError("accepted 70-case registry declarations differ")
    accepted70_cases = (list(ast.literal_eval(accepted70_base[0].value))
                        + list(ast.literal_eval(accepted70_admission[0].value)))
    if list(original_cases) + list(admission_cases) != accepted70_cases:
        raise ValueError("original 70 mutation tuples changed or were reordered")
    if ingress_cases != EXPECTED_INGRESS_CASES:
        raise ValueError("five ingress mutation tuples differ or were reordered")
    cases = list(original_cases) + list(admission_cases) + list(ingress_cases)
    if (len(cases) != 75
            or any(not isinstance(case, (list, tuple)) or len(case) != 5
                   or any(not isinstance(value, str) or not value for value in case)
                   or not re.fullmatch(r"[a-z][a-z0-9_]*", case[0]) for case in cases)):
        raise ValueError("trusted mutation registry is malformed")
    expected = {case[0]: case for case in cases}
    if not cases or len(expected) != len(cases) or any(len(case) != 5 for case in cases):
        raise ValueError("trusted mutation registry is malformed")
    names = [case[0] for case in cases]
    members = {"report.json", "manifest.json"} | {
        name + "-" + phase + ".log" for name in names for phase in ("control", "compile", "test")}
    with ZipFile(archive) as retained:
        inventory = retained.infolist()
        if (len(inventory) != 227 or len({info.filename for info in inventory}) != 227
                or {info.filename for info in inventory} != members):
            raise ValueError("mutation archive does not contain the exact 227 members")
        if sum(info.file_size for info in inventory) > 32 * 1024**2:
            raise ValueError("mutation archive exceeds the existing expanded bound")
        if any(stat.S_IFMT(info.external_attr >> 16) != stat.S_IFREG
               or info.flag_bits & 1 or info.file_size > 32 * 1024**2 for info in inventory):
            raise ValueError("mutation archive entries must be bounded regular unencrypted files")
    if {path.name for path in root.iterdir()} != members:
        raise ValueError("retained mutation directory does not contain the exact member set")
    report_raw = retained_bytes(root, "report.json", 16 * 1024**2)
    archived("report.json", report_raw, 16 * 1024**2)
    report = strict_json(report_raw)
    row_fields = {"mutation", "path", "package", "regression", "source_sha256",
                  "mutated_sha256", "restored_sha256", "control", "compilation", "test", "detected"}
    if (not isinstance(report, list) or len(report) != len(cases)
            or any(not isinstance(case, dict) or set(case) != row_fields for case in report)
            or [case["mutation"] for case in report] != names):
        raise ValueError("retained report does not cover the exact trusted mutation registry")
    verified_logs = 0
    for case in report:
        _, relative, before, after, regression = expected[case["mutation"]]
        if case["path"] != relative or case["regression"] != regression:
            raise ValueError("mutation source or regression differs from trusted registry")
        original = blob(relative)
        text = original.decode()
        if text.count(before) != 1 or before == after:
            raise ValueError("trusted mutation must replace exactly one different expression")
        mutated_digest = hashlib.sha256(text.replace(before, after, 1).encode()).hexdigest()
        if case["mutated_sha256"] != mutated_digest:
            raise ValueError("mutation hash differs from exact trusted mutant bytes")
        digest = hashlib.sha256(original).hexdigest()
        if digest != case["source_sha256"] or digest != case["restored_sha256"]:
            raise ValueError("mutation source restoration differs from trusted Git bytes")
        package, compile_argv, test_argv = expected_commands(
            relative, regression, blob, trusted_paths, workspace)
        if case["package"] != package or case["detected"] is not True:
            raise ValueError("mutation package or detection result is incorrect")
        for phase, exit_code, argv in (("compilation", 0, compile_argv),
                                      ("control", 0, test_argv), ("test", 100, test_argv)):
            result = case[phase]
            exact_command(result, exit_code, argv)
            relative_log = Path(result["log"])
            label = {"compilation": "compile", "control": "control", "test": "test"}[phase]
            if result["log"] != case["mutation"] + "-" + label + ".log":
                raise ValueError("mutation log does not belong to its exact case and phase")
            if relative_log.is_absolute() or ".." in relative_log.parts:
                raise ValueError("mutation log path is outside the retained artifact")
            log = root / relative_log
            if log.is_symlink() or not log.resolve(strict=True).is_relative_to(root):
                raise ValueError("mutation log resolves outside the retained artifact")
            if log.stat().st_size > 64 * 1024**2:
                raise ValueError("mutation log exceeds its declared bound")
            raw = retained_bytes(root, relative_log, 64 * 1024**2)
            archived(relative_log.as_posix(), raw, 64 * 1024**2)
            if hashlib.sha256(raw).hexdigest() != result["sha256"]:
                raise ValueError("mutation log digest mismatch")
            output = re.sub(r"\x1b\[[0-9;]*m", "", raw.decode(errors="replace"))
            status = {"control": "PASS", "test": "FAIL"}.get(phase)
            selected_result = (r"(?m)^\s*" + str(status) + r"\s+[^\n]*(?:\s|::)"
                               + re.escape(regression) + r"\s*$")
            if status and re.search(selected_result, output) is None:
                raise ValueError("selected regression result is absent from the retained log")
            verified_logs += 1
    manifest_raw = retained_bytes(root, "manifest.json", 4 * 1024**2)
    archived("manifest.json", manifest_raw, 4 * 1024**2)
    tree = subprocess.check_output(
        ["git", "rev-parse", commit + "^{tree}"], cwd=source, text=True).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", tree):
        raise ValueError("trusted Git tree is malformed")
    provenance = {"source": commit, "tree": tree, "run_id": run_id, "run_attempt": run_attempt,
                  "registry_sha256": hashlib.sha256(blob("tools/hub-capacity/mutate-rust.py")).hexdigest(),
                  "case_set_sha256": hashlib.sha256(json.dumps(cases, separators=(",", ":")).encode()).hexdigest()}
    check_manifest(strict_json(manifest_raw), provenance, names, report)
    return {"head": commit, "cases": len(cases), "verified_logs": verified_logs,
            "reported_restored_hashes_match_source": True,
            "archive_sha256": archive_sha256,
            "report_sha256": hashlib.sha256(report_raw).hexdigest(),
            "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
            "provenance": provenance, "exact_members": 227,
            "canonical_shard_digests_consistent": True,
            "shard_digest_scope": "Reconstructed canonical bytes are consistency checks only; unacquired shard ZIPs are not authenticated here.",
            "official_job_artifact_authentication": "Required separately before use; supplied run identity and archive digest do not authenticate job conclusions."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("source", type=Path)
    parser.add_argument("revision")
    parser.add_argument("--run-id", required=True, help="independently authenticated official run ID")
    parser.add_argument("--run-attempt", required=True, help="independently authenticated official run attempt")
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--archive-sha256", required=True,
                        help="Expected SHA256 from independent GitHub artifact metadata")
    args = parser.parse_args()
    print(json.dumps(verify(args.artifact, args.source, args.revision,
                            args.archive, args.archive_sha256, args.run_id,
                            args.run_attempt), sort_keys=True))
