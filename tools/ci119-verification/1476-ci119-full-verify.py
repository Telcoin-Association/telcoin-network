"""Run the pinned readers and full independent scorer on a fresh ARM runner."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sys

HEAD = "ca02e454b4f2fa5f5e1a47db8e346fb1bec00666"
TREE = "33c406e60bcd5b6eb8327155758939c26ebe495c"
RUN = 37940162416
QUIC_RUN = 37940184188
REPOSITORY = "Telcoin-Association/telcoin-network"
JOB_NAMES = {"Manual hub capacity qualification / Manual hub capacity code checks",
             "Manual hub capacity qualification / Manual hub capacity mutation shard 0",
             "Manual hub capacity qualification / Manual hub capacity mutation shard 1",
             "Manual hub capacity qualification / Manual hub capacity mutation reconciliation",
             "Manual hub capacity qualification / Manual hub capacity live qualification"}


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def load_json(path, limit=1024**2):
    if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= limit:
        raise ValueError("invalid bounded JSON file: " + path.name)
    return json.loads(path.read_bytes(), object_pairs_hook=unique_object)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def checked_manifest(root, manifest_path, expected):
    manifest = load_json(manifest_path)
    if not re.fullmatch("[0-9a-f]{64}", expected) or sha256(manifest_path) != expected:
        raise ValueError("reviewed manifest hash differs")
    if (manifest.get("ready") is not True or manifest.get("qualification_head") != HEAD
            or manifest.get("qualification_tree") != TREE or manifest.get("capacity_run_id") != RUN
            or manifest.get("quic_run_id") != QUIC_RUN):
        raise ValueError("official bindings are pending or source identity differs")
    paths = {}
    for role, pin in manifest["roles"].items():
        name = pin["name"]
        if Path(name).name != name or name in ("", ".", "..") or not re.fullmatch("[0-9a-f]{64}", pin["sha256"]):
            raise ValueError("invalid dependency pin")
        path = root / name
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 16 * 1024**2 or sha256(path) != pin["sha256"]:
            raise ValueError("reviewed dependency bytes differ: " + role)
        paths[role] = path
    return manifest, paths


def official_capacity(run, jobs):
    if (run.get("id") != RUN or run.get("head_sha") != HEAD or run.get("run_attempt") != 1
            or run.get("event") != "workflow_dispatch" or run.get("status") != "completed"
            or run.get("conclusion") != "success" or run.get("repository", {}).get("id") != 780459444):
        raise ValueError("official capacity run is not the exact successful qualification run")
    observed = {}
    for job in jobs["jobs"]:
        if job.get("name") in JOB_NAMES:
            if job["name"] in observed or job.get("run_id") != RUN or job.get("head_sha") != HEAD or job.get("status") != "completed" or job.get("conclusion") != "success":
                raise ValueError("official required capacity job failed or differs")
            observed[job["name"]] = job["id"]
    if set(observed) != JOB_NAMES:
        raise ValueError("official required capacity jobs are incomplete")
    return observed


def reader_results(quic, binaries, mutations, binary_map, scored):
    if (quic.get("verified") is not True or quic.get("final_result") != "PASS"
            or quic.get("head_sha") != HEAD or quic.get("run_id") != QUIC_RUN
            or binaries.get("outcome") != "pass" or binaries.get("head_sha") != HEAD or binaries.get("run_id") != RUN
            or mutations.get("head") != HEAD or mutations.get("cases") != 75 or mutations.get("verified_logs") != 225
            or mutations.get("exact_members") != 227 or mutations.get("canonical_shard_digests_consistent") is not True
            or scored.get("report_equal") is not True or scored.get("candidate_passed") is not True):
        raise ValueError("independent reader results are incomplete or non-PASS")
    if (set(binary_map) != {"telcoin-network", "node-record-api", "hub-capacity-peer"}
            or any(not isinstance(value, str) or not re.fullmatch("[0-9a-f]{64}", value) for value in binary_map.values())
            or binaries.get("archive", {}).get("actual_binary_sha256") != binary_map):
        raise ValueError("actual binary map differs from authenticated binary proof")


def write_json(path, value):
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    if len(raw) > 1024**2:
        raise ValueError("verification receipt exceeds its bound")
    with path.open("xb") as output:
        if output.write(raw) != len(raw):
            raise OSError("short verification receipt write")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--source-checkout", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    manifest, paths = checked_manifest(root, args.manifest, args.manifest_sha256)
    jobs = official_capacity(load_json(paths["capacity_run"]), load_json(paths["capacity_jobs"]))
    loader = importlib.util.spec_from_file_location("publication", paths["publication"])
    publication = importlib.util.module_from_spec(loader)
    loader.loader.exec_module(publication)
    temp = Path(os.environ["RUNNER_TEMP"])
    if not temp.is_absolute() or not temp.is_dir():
        raise ValueError("invalid runner temporary directory")
    publication.disk_floor(temp)
    work = temp / "ci119"
    work.mkdir()
    env = os.environ.copy()
    def run(role, *arguments):
        return publication.command([sys.executable, "-B", "-I", str(paths[role]), *map(str, arguments)], root, env, timeout=2400)
    quic_dir = work / "quic"
    quic_dir.mkdir()
    quic_api = work / "quic-api-inputs"
    quic_api.mkdir()
    for name, role in manifest["quic_api_inputs"].items():
        if Path(name).name != name:
            raise ValueError("invalid official QUIC API input name")
        with (quic_api / name).open("xb") as output:
            raw = paths[role].read_bytes()
            if output.write(raw) != len(raw):
                raise OSError("short official QUIC API input write")
    quic_metadata = work / "quic-metadata.json"
    run("quic_metadata_assembler", "--repo", args.source_checkout, "--evidence-dir", quic_dir, "--api-inputs", quic_api,
        "--run-metadata", paths["quic_run"], "--jobs-metadata", paths["quic_jobs"],
        "--artifacts-metadata", paths["quic_artifacts"], "--artifact-metadata", paths["quic_artifact"],
        "--source-pins", paths["quic_source_pins"], "--verifier", paths["quic_verifier"],
        "--prior-preparation", paths["quic_prior_preparation"], "--output", quic_metadata)
    run("quic_acquirer", "--evidence-dir", quic_dir, "--metadata", quic_metadata)
    quic_proof = work / "quic-proof.json"
    run("quic_verifier", "--repo", args.source_checkout, "--evidence-dir", quic_dir,
        "--prior-result", paths["quic_prior_result"], "--prior-verifier", paths["quic_prior_verifier"], "--output", quic_proof)
    binary_map = work / "binary-sha256.json"
    binary_proof = work / "binary-proof.json"
    run("binary_verifier", "--metadata", paths["binary_metadata"], "--quic-proof", quic_proof,
        "--quic-proof-sha256", sha256(quic_proof), "--map-output", binary_map, "--result-output", binary_proof)
    mutation_archive = work / "mutations.zip"
    mutation_metadata = load_json(paths["mutation_metadata"])
    mutation_metadata_sha = manifest["roles"]["mutation_metadata"]["sha256"]
    run("mutation_acquirer", "--metadata", paths["mutation_metadata"], "--metadata-sha256", mutation_metadata_sha, "--destination", mutation_archive)
    mutation_dir = work / "mutations"
    run("mutation_extractor", "--metadata", paths["mutation_metadata"], "--metadata-sha256", mutation_metadata_sha,
        "--archive", mutation_archive, "--dest", mutation_dir, "--destination-parent", work,
        "--name", "hub-capacity-mutations-" + HEAD, "--run", RUN, "--head", HEAD)
    mutation_raw = run("mutation_verifier", mutation_dir, args.source_checkout, HEAD, "--run-id", RUN, "--run-attempt", 1,
                       "--archive", mutation_archive, "--archive-sha256", mutation_metadata["digest"].removeprefix("sha256:"))
    mutations = json.loads(mutation_raw, object_pairs_hook=unique_object)
    write_json(work / "mutation-proof.json", mutations)
    capacity_metadata = paths["capacity_metadata"].read_bytes()
    with (temp / "1476-ci119-capacity-artifact-metadata.json").open("xb") as output:
        if output.write(capacity_metadata) != len(capacity_metadata):
            raise OSError("short capacity metadata write")
    run("capacity_acquirer")
    bundle = work / "public-bundle"
    scored = json.loads(run("full_verifier", temp / "1476-ci119-evidence.zip", "--evidence-metadata", paths["capacity_metadata"],
        "--binary-metadata", paths["binary_metadata"], "--actual-binary-hashes", binary_map,
        "--source-checkout", args.source_checkout, "--qualification-revision", HEAD, "--binary-revision", HEAD,
        "--ci-run-id", RUN, "--repository-id", 780459444, "--repository", REPOSITORY, "--destination", bundle), object_pairs_hook=unique_object)
    quic = load_json(quic_proof)
    binaries = load_json(binary_proof)
    actual_map = load_json(binary_map)
    reader_results(quic, binaries, mutations, actual_map, scored)
    if scored.get("published") != str(bundle):
        raise ValueError("full verifier did not finish the exact fresh public bundle")
    receipt = {"qualification_head": HEAD, "qualification_tree": TREE, "capacity_run_id": RUN,
               "helper_sha": env["EXPECTED_HELPER_SHA"], "helper_run_id": int(env["GITHUB_RUN_ID"]),
               "helper_run_attempt": int(env["GITHUB_RUN_ATTEMPT"]), "helper_job": env["GITHUB_JOB"],
               "candidate_passed": True, "report_equal": True, "independently_rescored": True,
               "quic_authenticated": True, "binaries_authenticated": True, "mutation_controls_authenticated": True,
               "mutation_case_count": 75, "official_capacity_job_ids": jobs, "input_manifest_sha256": args.manifest_sha256,
               "reader_proofs": {"quic": quic, "binaries": binaries, "mutations": mutations, "full_rescore": scored},
               "actual_binary_sha256": actual_map,
               "public_files": {name: publication.file_digest(publication.regular_file(bundle / name, publication.LIMITS[name])) for name in publication.FILES}}
    publication.verify_bundle(bundle, receipt, env["EXPECTED_HELPER_SHA"], int(env["GITHUB_RUN_ID"]), int(env["GITHUB_RUN_ATTEMPT"]))
    write_json(work / "proof.json", receipt)
    with Path(env["GITHUB_OUTPUT"]).open("a") as output:
        output.write("qualified=true\n")
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    main()
