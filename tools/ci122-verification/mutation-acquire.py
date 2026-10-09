"""Acquire one CI118 mutation ZIP only after authenticating official artifact metadata."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import subprocess
import sys
import time

HEAD = "3914e53957fcc3ff7befe8b1a3f9a3284bfaba4b"
RUN_ID = 37990571919
REPOSITORY_ID = 780459444
ARTIFACT_ID: int | None = 11649838509
METADATA = None
DESTINATION = None
MAX_ZIP_BYTES = 1 * 1024 * 1024
MAX_META_BYTES = 8192
MAX_STDERR_BYTES = 8192
DEADLINE_SECONDS = 180
MIN_REMAINING_DISK_BYTES = 30 * 1024**3


def configure(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--metadata-sha256", required=True)
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args(argv)
    global METADATA, DESTINATION
    METADATA = args.metadata.resolve(strict=True)
    DESTINATION = args.destination.absolute()
    return args.metadata_sha256


def main(expected_metadata_sha256):
    if type(ARTIFACT_ID) is not int or ARTIFACT_ID <= 0:
        raise ValueError("aggregate artifact identity is pending")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_metadata_sha256):
        raise ValueError("invalid expected metadata digest")
    if METADATA.is_symlink() or not METADATA.is_file() or METADATA.stat().st_size > MAX_META_BYTES:
        raise ValueError("metadata is not an allowed regular file")
    metadata = METADATA.read_bytes()
    if hashlib.sha256(metadata).hexdigest() != expected_metadata_sha256:
        raise ValueError("metadata digest mismatch")
    meta = json.loads(metadata)
    workflow = meta["workflow_run"]
    if (meta["name"] != "hub-capacity-mutations-" + HEAD
            or workflow["id"] != RUN_ID or workflow["head_sha"] != HEAD
            or workflow["repository_id"] != REPOSITORY_ID
            or workflow["head_repository_id"] != REPOSITORY_ID
            or meta["expired"] is not False):
        raise ValueError("artifact provenance mismatch")
    artifact_id = meta["id"]
    expected = meta["size_in_bytes"]
    digest = meta["digest"]
    if (type(artifact_id) is not int or artifact_id <= 0 or artifact_id != ARTIFACT_ID
            or type(expected) is not int or not 0 < expected <= MAX_ZIP_BYTES
            or not isinstance(digest, str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
            or DESTINATION.exists()):
        raise ValueError("artifact metadata or destination is invalid")
    if shutil.disk_usage(DESTINATION.parent).free - expected < MIN_REMAINING_DISK_BYTES:
        raise ValueError("mutation ZIP would breach the 30 GiB free-disk floor")
    process = subprocess.Popen(
        ["gh", "api", "--allow-escape-sequences",
         f"repos/Telcoin-Association/telcoin-network/actions/artifacts/{artifact_id}/zip"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    streams = {"stdout": bytearray(), "stderr": bytearray()}
    deadline = time.monotonic() + DEADLINE_SECONDS
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("mutation ZIP request exceeded deadline")
                for key, _ in selector.select(min(1, remaining)):
                    kind = key.data
                    limit = expected if kind == "stdout" else MAX_STDERR_BYTES
                    chunk = os.read(key.fd, min(65536, limit + 1 - len(streams[kind])))
                    if not chunk:
                        selector.unregister(key.fileobj)
                    else:
                        streams[kind].extend(chunk)
                        if len(streams[kind]) > limit:
                            raise ValueError(f"mutation ZIP {kind} exceeded byte bound")
        if process.wait(timeout=5) != 0:
            raise RuntimeError("artifact request failed")
        raw = bytes(streams["stdout"])
        if len(raw) != expected or hashlib.sha256(raw).hexdigest() != digest[7:]:
            raise ValueError("artifact byte count or digest mismatch")
        with DESTINATION.open("xb") as output:
            output.write(raw)
        print(json.dumps({"artifact_id": artifact_id, "actual_bytes": len(raw),
                          "actual_sha256": digest[7:], "path": str(DESTINATION),
                          "downloaded_once": True, "stderr_bytes": len(streams["stderr"])}))
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stdout.close()
        process.stderr.close()


if __name__ == "__main__":
    main(configure())
