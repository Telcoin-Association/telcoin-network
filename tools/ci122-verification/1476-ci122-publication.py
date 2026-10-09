"""Publish only the independently qualified seven-file CI119 public bundle."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import subprocess
import time
import urllib.request

HEAD = "3914e53957fcc3ff7befe8b1a3f9a3284bfaba4b"
RUN = 37990571919
REPOSITORY = "Telcoin-Association/telcoin-network"
BRANCH = "evidence/hub-capacity-1476-37990571919"
FILES = ("plan.json", "report.json", "baseline.tar.gz", "candidate.tar.gz", "inputs.tar.gz", "README.md", "SHA256SUMS")
LIMITS = {name: 100 * 1024**2 - 1 for name in FILES if name.endswith(".tar.gz")}
LIMITS.update({"plan.json": 16 * 1024**2, "report.json": 16 * 1024**2, "README.md": 1024**2, "SHA256SUMS": 475})
RESERVE = 30 * 1024**3
SLACK = 256 * 1024**2
IDENTITY = "Onyeka Obi"
EMAIL = "softwareengineerasaservant@isurvivable.cv"


def regular_file(path, limit):
    if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= limit:
        raise ValueError("invalid bounded regular file: " + path.name)
    return path


def file_digest(path):
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024**2), b""):
            size += len(chunk)
            digest.update(chunk)
    return {"bytes": size, "sha256": digest.hexdigest()}


def verify_bundle(bundle, proof, helper_sha, helper_run, helper_attempt):
    if not re.fullmatch("[0-9a-f]{40}", helper_sha):
        raise ValueError("invalid helper SHA")
    if bundle.is_symlink() or not bundle.is_dir() or set(path.name for path in bundle.iterdir()) != set(FILES):
        raise ValueError("public bundle must contain exactly seven files")
    if (proof.get("qualification_head") != HEAD or proof.get("capacity_run_id") != RUN
            or proof.get("helper_sha") != helper_sha or proof.get("helper_run_id") != helper_run
            or proof.get("helper_run_attempt") != helper_attempt or proof.get("helper_job") != "verify"
            or proof.get("candidate_passed") is not True or proof.get("report_equal") is not True
            or proof.get("independently_rescored") is not True
            or proof.get("quic_authenticated") is not True or proof.get("binaries_authenticated") is not True
            or proof.get("mutation_controls_authenticated") is not True or proof.get("mutation_case_count") != 77):
        raise ValueError("qualification or helper provenance is incomplete")
    actual = {name: file_digest(regular_file(bundle / name, LIMITS[name])) for name in FILES}
    if proof.get("public_files") != actual:
        raise ValueError("public bundle bytes differ from independently qualified proof")
    report = json.loads((bundle / "report.json").read_bytes())
    if report.get("candidate", {}).get("passed") is not True or report["candidate"].get("failures") != []:
        raise ValueError("public candidate report is not PASS")
    checksums = "".join(actual[name]["sha256"] + "  " + name + "\n" for name in sorted(FILES) if name != "SHA256SUMS")
    if (bundle / "SHA256SUMS").read_bytes() != checksums.encode():
        raise ValueError("public SHA256SUMS differs from bundle")
    return actual


def disk_floor(path, total=0):
    free = shutil.disk_usage(path).free
    if free < RESERVE + 2 * total + (SLACK if total else 0):
        raise ValueError("30 GiB publication reserve or Git staging allowance failed")
    return free


def command(args, cwd, env, payload=None, timeout=600, limit=1024**2):
    process = subprocess.Popen(args, cwd=cwd, env=env, stdin=subprocess.PIPE if payload is not None else subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        if payload is not None:
            process.stdin.write(payload)
            process.stdin.close()
        outputs = {"stdout": bytearray(), "stderr": bytearray()}
        deadline = time.monotonic() + timeout
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("remote publication command timed out")
                for key, _ in selector.select(min(1, remaining)):
                    chunk = os.read(key.fd, min(65536, limit + 1 - len(outputs[key.data])))
                    if not chunk:
                        selector.unregister(key.fileobj)
                    else:
                        outputs[key.data].extend(chunk)
                        if len(outputs[key.data]) > limit:
                            raise ValueError("remote publication command output limit exceeded")
        status = process.wait(timeout=5)
        if status != 0:
            error = RuntimeError("remote publication command failed: " + args[0])
            error.returncode = status
            error.stderr = bytes(outputs["stderr"])
            raise error
        return bytes(outputs["stdout"]).decode("utf-8")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None and not stream.closed:
                stream.close()


def verify_public(commit, actual, opener=urllib.request.urlopen):
    if not re.fullmatch("[0-9a-f]{40}", commit):
        raise ValueError("invalid immutable publication commit")
    observed = {}
    for name in FILES:
        url = f"https://raw.githubusercontent.com/{REPOSITORY}/{commit}/{name}"
        digest = hashlib.sha256()
        size = 0
        deadline = time.monotonic() + 600
        with opener(url, timeout=60) as response:
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError("immutable public verification timed out")
                chunk = response.read(min(1024**2, actual[name]["bytes"] + 1 - size))
                if not chunk:
                    break
                size += len(chunk)
                if size > actual[name]["bytes"]:
                    raise ValueError("immutable public file exceeds authenticated byte count")
                digest.update(chunk)
        if {"bytes": size, "sha256": digest.hexdigest()} != actual[name]:
            raise ValueError("immutable public bytes differ: " + name)
        observed[name] = dict(actual[name], url=url)
    return observed


def publish(bundle, actual, temp):
    total = sum(item["bytes"] for item in actual.values())
    before = disk_floor(temp, total)
    git_dir = temp / "ci122-publication-git"
    git_dir.mkdir()
    env = os.environ.copy()
    env.update({"GIT_AUTHOR_NAME": IDENTITY, "GIT_COMMITTER_NAME": IDENTITY,
                "GIT_AUTHOR_EMAIL": EMAIL, "GIT_COMMITTER_EMAIL": EMAIL,
                "GIT_TERMINAL_PROMPT": "0", "GIT_INDEX_FILE": str(git_dir / "publication.index"),
                "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull})
    token = env.pop("GH_TOKEN")
    encoded = base64.b64encode(("x-access-token:" + token).encode()).decode()
    env.update({"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "http.https://github.com/.extraheader",
                "GIT_CONFIG_VALUE_0": "AUTHORIZATION: basic " + encoded})
    def git(*args, payload=None):
        return command(["git", "-c", "core.autocrlf=false", *args], git_dir, env, payload)
    git("init", "--quiet")
    blobs = {}
    for name in FILES:
        oid = git("hash-object", "-w", "--no-filters", "--", str(bundle / name)).strip()
        if not re.fullmatch("[0-9a-f]{40}", oid):
            raise ValueError("invalid public Git blob")
        blobs[name] = oid
        git("update-index", "--add", "--cacheinfo", "100644," + oid + "," + name)
    tree = git("write-tree").strip()
    message = f"Publish independently qualified hub capacity evidence for CI run {RUN}\n\nSigned-off-by: {IDENTITY} <{EMAIL}>\n"
    commit = git("commit-tree", tree, payload=message.encode()).strip()
    content = git("cat-file", "commit", commit)
    if content.splitlines()[0] != "tree " + tree or any(line.startswith("parent ") for line in content.split("\n\n", 1)[0].splitlines()):
        raise ValueError("evidence commit must be an orphan root")
    expected_tree = "".join("100644 blob " + blobs[name] + "\t" + name + "\n" for name in sorted(FILES))
    if git("ls-tree", commit) != expected_tree:
        raise ValueError("evidence tree differs from exact seven-file set")
    after_staging = disk_floor(temp, total)
    remote = "https://github.com/" + REPOSITORY + ".git"
    if git("ls-remote", "--heads", remote, "refs/heads/" + BRANCH).strip():
        raise ValueError("evidence publication branch already exists")
    git("push", remote, commit + ":refs/heads/" + BRANCH)
    print(json.dumps({"push_completed": True, "commit": commit, "branch": BRANCH}), flush=True)
    if git("ls-remote", "--heads", remote, "refs/heads/" + BRANCH).strip() != commit + "\trefs/heads/" + BRANCH:
        raise ValueError("remote evidence branch differs from orphan commit")
    after_push = disk_floor(temp)
    public = verify_public(commit, actual)
    print(json.dumps({"published": True, "public_bytes_verified": True, "commit": commit, "branch": BRANCH,
                      "public_files": public, "disk_free_bytes": [before, after_staging, after_push]}, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--proof", type=Path, required=True)
    args = parser.parse_args()
    env = os.environ
    proof = json.loads(regular_file(args.proof, 1024**2).read_bytes())
    actual = verify_bundle(args.bundle, proof, env["EXPECTED_HELPER_SHA"], int(env["GITHUB_RUN_ID"]), int(env["GITHUB_RUN_ATTEMPT"]))
    temp = Path(env["RUNNER_TEMP"])
    if not temp.is_absolute() or not temp.is_dir():
        raise ValueError("invalid runner temporary directory")
    publish(args.bundle, actual, temp)


if __name__ == "__main__":
    main()
