"""Bounded acquisition of the one reviewed CI119 QUIC Actions artifact."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import selectors
import subprocess
import time


BASE = None
META = None
ZIP = None
RECEIPT = None
META_SHA256 = '701de1996a6272b0ba9a52bba3da517b9b02876da3d38c3f8c75467a02f26088'
ARTIFACT = 11645521069
RUN = 37990580614
HEAD = '3914e53957fcc3ff7befe8b1a3f9a3284bfaba4b'
EXPECTED_SIZE = 23790
EXPECTED_SHA256 = '70a4f96919c6aaff2508c5100092c61442e2a077aa55f65d7a027dbc30218e88'
MAX_ARCHIVE = 1024 * 1024
MAX_STDERR = 64 * 1024
DISK_FLOOR = 30 * 1024**3
DEADLINE = 120
API_PATH = f"repos/Telcoin-Association/telcoin-network/actions/artifacts/{ARTIFACT}/zip"


def digest(value):
    return hashlib.sha256(value).hexdigest()


def free_bytes():
    stat = os.statvfs(BASE)
    return stat.f_bavail * stat.f_frsize


def configure(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    args = parser.parse_args(argv)
    global BASE, META, ZIP, RECEIPT
    BASE = args.evidence_dir.resolve(strict=True)
    if not BASE.is_dir():
        raise ValueError("evidence directory is not a directory")
    META = args.metadata.resolve(strict=True)
    ZIP = BASE / "artifact.zip"
    RECEIPT = BASE / "acquisition-attempt1.json"


def main():
    assert not ZIP.exists() and not RECEIPT.exists(), "output already exists"
    raw_meta = META.read_bytes()
    assert digest(raw_meta) == META_SHA256, "metadata changed"
    meta = json.loads(raw_meta)
    assert meta["id"] == ARTIFACT and meta["size_in_bytes"] == EXPECTED_SIZE
    assert meta["name"] == "quic-handshake-profile" and meta["expired"] is False
    assert meta["digest"] == "sha256:" + EXPECTED_SHA256
    assert meta["workflow_run"]["id"] == RUN
    assert meta["workflow_run"]["head_sha"] == HEAD
    assert meta["workflow_run"]["repository_id"] == meta["workflow_run"]["head_repository_id"] == 780459444
    assert meta["archive_download_url"] == "https://api.github.com/" + API_PATH
    assert 0 < EXPECTED_SIZE <= MAX_ARCHIVE
    assert datetime.datetime.fromisoformat(meta["expires_at"].replace("Z", "+00:00")) > datetime.datetime.now(datetime.timezone.utc)
    disk_before = free_bytes()
    assert disk_before - MAX_ARCHIVE >= DISK_FLOOR, "30 GiB disk reserve failed"

    cmd = ["gh", "api", "--allow-escape-sequences", API_PATH]
    receipt = {
        "attempt": 1,
        "command": cmd,
        "limit_bytes": MAX_ARCHIVE,
        "deadline_seconds": DEADLINE,
        "disk_floor_after_limit_bytes": DISK_FLOOR,
        "disk_free_before_bytes": disk_before,
        "official_size_bytes": EXPECTED_SIZE,
        "official_digest": "sha256:" + EXPECTED_SHA256,
        "status": "started",
        "exit_code": None,
        "error": None,
        "started_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "retained_artifact_path": str(ZIP),
    }
    chunks = []
    stderr = bytearray()
    size = 0
    proc = None
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        selector = selectors.DefaultSelector()
        selector.register(proc.stdout, selectors.EVENT_READ, "stdout")
        selector.register(proc.stderr, selectors.EVENT_READ, "stderr")
        end = time.monotonic() + DEADLINE
        while selector.get_map():
            remaining = end - time.monotonic()
            assert remaining > 0, "artifact command deadline reached"
            for key, _ in selector.select(min(remaining, 1)):
                chunk = os.read(key.fileobj.fileno(), 64 * 1024)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                if key.data == "stdout":
                    assert size + len(chunk) <= MAX_ARCHIVE, "response exceeds 1 MiB"
                    size += len(chunk)
                    chunks.append(chunk)
                else:
                    assert len(stderr) + len(chunk) <= MAX_STDERR, "stderr exceeds 64 KiB"
                    stderr.extend(chunk)
        selector.close()
        remaining = end - time.monotonic()
        assert remaining > 0, "artifact command deadline reached"
        receipt["exit_code"] = proc.wait(timeout=remaining)
        assert receipt["exit_code"] == 0, "artifact command failed"
        data = b"".join(chunks)
        assert len(data) == EXPECTED_SIZE, "response differs from official size"
        assert digest(data) == EXPECTED_SHA256, "response differs from official digest"
        assert free_bytes() >= DISK_FLOOR, "post-download disk reserve failed"
        fd = os.open(ZIP, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as output:
            output.write(data)
        receipt["status"] = "verified_download"
        receipt["stdout_bytes"] = len(data)
        receipt["stdout_sha256"] = digest(data)
        receipt["stderr_bytes"] = len(stderr)
        receipt["stderr_sha256"] = digest(stderr)
    except Exception as exc:
        receipt["status"] = "failed"
        receipt["error"] = str(exc)
        if proc is not None:
            proc.kill()
            receipt["exit_code"] = proc.wait()
        raise
    finally:
        receipt["finished_at_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with RECEIPT.open("x") as stream:
            stream.write(json.dumps(receipt, sort_keys=True, indent=2) + "\n")
        print(json.dumps({"receipt": str(RECEIPT), "status": receipt["status"],
                          "exit_code": receipt["exit_code"], "bytes": size,
                          "sha256": receipt.get("stdout_sha256")}, sort_keys=True))


if __name__ == "__main__":
    configure()
    main()
