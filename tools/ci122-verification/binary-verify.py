"""Verify one official CI107 binary ZIP entirely in bounded memory.

This script is a proposal until reviewed. It intentionally retains no raw ZIP.
It never executes a downloaded member.
"""

import argparse
import datetime
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import selectors
import stat
import subprocess
import time
import zipfile
import zlib

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


HEAD = "3914e53957fcc3ff7befe8b1a3f9a3284bfaba4b"
RUN = 37990571919
REPOSITORY = 780459444
ARTIFACT = 11646185897
ARCHIVE_BYTES = 68561066
ARCHIVE_SHA256 = '4a656fc80d44c6b12e1bcb18bddff2b7a5d8401be1d3d9edcf60bf9d4f6e7793'
METADATA_SHA256 = '90e4e59ecb50c4d0fba838e8ea9a24dabceb2bbb7c59a6a709abbd125e8b4d47'
METADATA = None
QUIC_RESULT = None
QUIC_RESULT_SHA256 = None  # Pending actual root CI118 QUIC proof.
TESTED_CHECKOUT = "3914e53957fcc3ff7befe8b1a3f9a3284bfaba4b"
TESTED_TREE = "6175a752c787eef23cfefe76324a735d5f2baef2"
WORKFLOW_SHA256 = '1d117f9d67e1eaa5549b8e241d26fd142e992d3e757c40087421b3bc110758eb'
MANUAL_WORKFLOW_SHA256 = 'acb17364ed627366f398ca84301cf5dac8fbedca6a8c072d8b7943626cd2e0ae'
MAP_OUTPUT = None
RESULT_OUTPUT = None
API_PATH = None
CHUNK = 64 * 1024
MAX_STDERR = 64 * 1024
MAX_MEMBER = 512 * 1024 * 1024
MAX_EXPANDED = 6 * 1024 * 1024 * 1024
TIMEOUT_SECONDS = 240
BINARIES = (
    ("telcoin-network", "telcoin-network", "target/debug/telcoin-network"),
    ("node-record-api", "node-record-api", "target/debug/node-record-api"),
    ("hub-capacity-peer", "examples/hub-capacity-peer", "target/debug/examples/hub-capacity-peer"),
)
EXPECTED_MEMBERS = {entry[1] for entry in BINARIES} | {
    "hub-capacity-revision.txt", "hub-capacity-binaries.sha256"
}


class VerificationError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise VerificationError(message)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def reject_nonfinite(value):
    raise VerificationError(f"nonfinite JSON value: {value}")


def parse_json(data):
    return json.loads(data, object_pairs_hook=unique_object, parse_constant=reject_nonfinite)


def validated_metadata():
    raw = METADATA.read_bytes()
    require(sha256(raw) == METADATA_SHA256, "official metadata bytes changed")
    meta = parse_json(raw)
    require(meta["id"] == ARTIFACT, "artifact ID mismatch")
    require(meta["name"] == "hub-capacity-linux-arm64-" + HEAD, "artifact name mismatch")
    require(meta["size_in_bytes"] == ARCHIVE_BYTES, "API archive size mismatch")
    require(meta["digest"] == "sha256:" + ARCHIVE_SHA256, "API archive digest mismatch")
    require(meta["expired"] is False, "artifact expired")
    expiry = datetime.datetime.fromisoformat(meta["expires_at"].replace("Z", "+00:00"))
    require(expiry > datetime.datetime.now(datetime.timezone.utc), "artifact expiry passed")
    run = meta["workflow_run"]
    require(run["id"] == RUN and run["head_sha"] == HEAD, "run or head mismatch")
    require(run["repository_id"] == run["head_repository_id"] == REPOSITORY,
            "repository mismatch")
    require(run["head_branch"] == "feat/1476-public-hub-capacity", "branch mismatch")
    require(meta["archive_download_url"] == "https://api.github.com/" + API_PATH,
            "archive endpoint mismatch")
    return meta, raw


def capture_command(command, expected_size, timeout_seconds=TIMEOUT_SECONDS):
    """Read one child response with bounded stdout and stderr, then require exit zero."""
    require(0 < expected_size <= 128 * 1024 * 1024, "unbounded archive size")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ, "stdout")
    selector.register(process.stderr, selectors.EVENT_READ, "stderr")
    archive = bytearray()
    stderr = bytearray()
    digest = hashlib.sha256()
    deadline = time.monotonic() + timeout_seconds
    try:
        while selector.get_map():
            remaining = deadline - time.monotonic()
            require(remaining > 0, "artifact download timed out")
            for key, _ in selector.select(timeout=min(remaining, 5)):
                chunk = os.read(key.fileobj.fileno(), CHUNK)
                if not chunk:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                elif key.data == "stdout":
                    require(len(archive) + len(chunk) <= expected_size,
                            "artifact response exceeds API size")
                    archive.extend(chunk)
                    digest.update(chunk)
                else:
                    require(len(stderr) + len(chunk) <= MAX_STDERR,
                            "artifact command stderr exceeds bound")
                    stderr.extend(chunk)
        remaining = deadline - time.monotonic()
        require(remaining > 0, "artifact command deadline reached")
        status = process.wait(timeout=remaining)
        require(status == 0, f"artifact command exited {status}: " +
                stderr.decode("utf-8", errors="replace")[-1024:])
        require(len(archive) == expected_size, "artifact response size differs from API")
        return bytes(archive), digest.hexdigest()
    except BaseException:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        raise
    finally:
        selector.close()
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()


def verify_archive_bytes(raw, expected_size, expected_sha256, head=HEAD):
    """Check ZIP structure, CRC and SHA while streaming each member, without extraction."""
    require(len(raw) == expected_size, "archive byte count mismatch")
    actual_archive_sha = sha256(raw)
    require(actual_archive_sha == expected_sha256, "archive digest mismatch")
    member_evidence = {}
    binary_sha = {}
    small_contents = {}
    expanded = 0
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        infos = archive.infolist()
        names = [info.filename for info in infos]
        require(len(names) == len(set(names)) == len(EXPECTED_MEMBERS) == 5,
                "duplicate or missing ZIP member")
        require(set(names) == EXPECTED_MEMBERS, "unexpected ZIP member set")
        for info in infos:
            path = PurePosixPath(info.filename)
            require(not path.is_absolute() and ".." not in path.parts and
                    "\\" not in info.filename, "unsafe ZIP path")
            require(not info.is_dir() and not (info.flag_bits & 1),
                    "directory or encrypted ZIP member")
            mode = info.external_attr >> 16
            require(stat.S_IFMT(mode) in (0, stat.S_IFREG), "non-regular ZIP member")
            require(info.file_size <= MAX_MEMBER, "oversized ZIP member")
            expanded += info.file_size
            require(expanded <= MAX_EXPANDED, "oversized expanded archive")
        for info in infos:
            size = 0
            crc = 0
            digest = hashlib.sha256()
            kept = bytearray() if info.filename in (
                "hub-capacity-revision.txt", "hub-capacity-binaries.sha256") else None
            with archive.open(info) as stream:
                while chunk := stream.read(CHUNK):
                    size += len(chunk)
                    require(size <= info.file_size and size <= MAX_MEMBER,
                            "member exceeds declared size")
                    digest.update(chunk)
                    crc = zlib.crc32(chunk, crc)
                    if kept is not None:
                        require(len(kept) + len(chunk) <= 4096,
                                "revision or manifest too large")
                        kept.extend(chunk)
            require(size == info.file_size, "member shorter than declared size")
            require((crc & 0xFFFFFFFF) == info.CRC, "member CRC mismatch")
            sha = digest.hexdigest()
            member_evidence[info.filename] = {
                "size_bytes": size,
                "compressed_bytes": info.compress_size,
                "mode_octal": oct(info.external_attr >> 16),
                "crc32": f"{info.CRC:08x}",
                "sha256": sha,
            }
            if kept is not None:
                small_contents[info.filename] = bytes(kept)
    for logical, member, _ in BINARIES:
        binary_sha[logical] = member_evidence[member]["sha256"]
    require(small_contents["hub-capacity-revision.txt"] == (head + "\n").encode(),
            "revision does not equal exact head")
    expected_manifest = "".join(
        f"{binary_sha[logical]}  {manifest_path}\n"
        for logical, _, manifest_path in BINARIES
    ).encode()
    require(small_contents["hub-capacity-binaries.sha256"] == expected_manifest,
            "manifest does not equal actual binary hashes and paths")
    return {
        "archive_sha256": actual_archive_sha,
        "archive_size_bytes": len(raw),
        "expanded_bytes": expanded,
        "members": member_evidence,
        "revision_sha256": member_evidence["hub-capacity-revision.txt"]["sha256"],
        "manifest_sha256": member_evidence["hub-capacity-binaries.sha256"]["sha256"],
        "actual_binary_sha256": binary_sha,
    }


def configure(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--quic-proof", type=Path, required=True)
    parser.add_argument("--quic-proof-sha256", required=True)
    parser.add_argument("--map-output", type=Path, required=True)
    parser.add_argument("--result-output", type=Path, required=True)
    args = parser.parse_args(argv)
    global METADATA, QUIC_RESULT, QUIC_RESULT_SHA256, MAP_OUTPUT, RESULT_OUTPUT, API_PATH
    if type(ARTIFACT) is not int or ARTIFACT <= 0:
        raise ValueError("binary artifact identity is pending")
    if type(ARCHIVE_BYTES) is not int or ARCHIVE_BYTES <= 0:
        raise ValueError("binary artifact size is pending")
    if any(not isinstance(value, str) or len(value) != 64 or
           any(char not in "0123456789abcdef" for char in value)
           for value in (ARCHIVE_SHA256, METADATA_SHA256, args.quic_proof_sha256)):
        raise ValueError("binary artifact or QUIC proof digest is pending or malformed")
    METADATA = args.metadata.resolve(strict=True)
    QUIC_RESULT = args.quic_proof.resolve(strict=True)
    QUIC_RESULT_SHA256 = args.quic_proof_sha256
    MAP_OUTPUT = args.map_output.absolute()
    RESULT_OUTPUT = args.result_output.absolute()
    API_PATH = f"repos/Telcoin-Association/telcoin-network/actions/artifacts/{ARTIFACT}/zip"
    runner_identity()


def main():
    quic_raw = QUIC_RESULT.read_bytes()
    require(sha256(quic_raw) == QUIC_RESULT_SHA256, "QUIC proof bytes changed")
    quic = parse_json(quic_raw)
    require(quic["verified"] is True and quic["head_sha"] == HEAD and
            quic["checkout_kind"] == "manual branch head" and
            quic["workflow_event"] == "workflow_dispatch" and
            quic["checkout_sha"] == TESTED_CHECKOUT and
            quic["checkout_tree_sha"] == TESTED_TREE and
            quic["source_files_tested_checkout_match_head"] is True and
            quic["main_workflow_sha256"] == WORKFLOW_SHA256 and
            quic["source_files"][".github/workflows/pr.yaml"]["sha256"] == WORKFLOW_SHA256 and
            quic["source_files"][".github/workflows/hub-capacity-manual.yaml"]["sha256"] == MANUAL_WORKFLOW_SHA256,
            "QUIC source and manual checkout proof differs")
    meta, meta_raw = validated_metadata()
    require(not MAP_OUTPUT.exists() and not RESULT_OUTPUT.exists(),
            "verification outputs already exist")
    raw, downloaded_sha = capture_command(["gh", "api", "--allow-escape-sequences", API_PATH], meta["size_in_bytes"])
    require(downloaded_sha == ARCHIVE_SHA256, "download stream digest mismatch")
    proof = verify_archive_bytes(raw, meta["size_in_bytes"], ARCHIVE_SHA256)
    del raw
    result = {
        "executor": runner_identity(),
        "proof_origin": "ci119-binary-verifier-derived-accepted-ci118",
        "outcome": "pass",
        "packet_id": "9d860d1dc4964c7a2796601d",
        "run_id": RUN,
        "head_sha": HEAD,
        "repository_id": REPOSITORY,
        "artifact_id": ARTIFACT,
        "artifact_name": meta["name"],
        "official_metadata_path": str(METADATA),
        "official_metadata_sha256": sha256(meta_raw),
        "artifact_expires_at": meta["expires_at"],
        "source_workflow_path": ".github/workflows/hub-capacity-manual.yaml",
        "source_workflow_sha256": MANUAL_WORKFLOW_SHA256,
        "pr_workflow_sha256": WORKFLOW_SHA256,
        "checkout_kind": "manual branch head",
        "workflow_event": "workflow_dispatch",
        "checkout_sha": TESTED_CHECKOUT,
        "checkout_tree_sha": TESTED_TREE,
        "quic_verification_result": {"path": str(QUIC_RESULT),
                                     "sha256": QUIC_RESULT_SHA256},
        "archive": proof,
        "actual_hash_map_path": str(MAP_OUTPUT),
        "raw_zip_retained": False,
        "extraction_performed": False,
        "binary_execution": False,
        "build_mode": "debug",
        "qualification_scored": False,
        "limits": "Raw ZIP was verified in memory and not retained; independent offline replay needs a later download. This proves current artifact bytes and member hashes, not reproducible builds or passing qualification.",
        "verifier_path": str(Path(__file__).resolve()),
        "verifier_sha256": sha256(Path(__file__).read_bytes()),
    }
    with MAP_OUTPUT.open("x") as stream:
        stream.write(json.dumps(proof["actual_binary_sha256"], indent=2,
                                     sort_keys=True) + "\n")
    with RESULT_OUTPUT.open("x") as stream:
        stream.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "verified": True,
        "artifact_id": ARTIFACT,
        "archive_sha256": proof["archive_sha256"],
        "actual_binary_sha256": proof["actual_binary_sha256"],
        "result_path": str(RESULT_OUTPUT),
        "result_sha256": sha256(RESULT_OUTPUT.read_bytes()),
    }, sort_keys=True))


if __name__ == "__main__":
    configure()
    main()
