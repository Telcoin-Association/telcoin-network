"""Download one authenticated qualification evidence ZIP with strict bounds."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import selectors
import subprocess
import time

expected: int | None = 141052907  # Pending official CI122 artifact byte count.
digest: str | None = '9e3e5b528e0704a0126b26940641bf0133f40d93dd68e592ac597ff2125d2848'
ARTIFACT_ID: int | None = 11651175795
METADATA_SHA256: str | None = 'a7732546a16d368098ce30ba30ed48280b2995084722799f9a6d9f677d84844c'
if (type(ARTIFACT_ID) is not int or ARTIFACT_ID <= 0
        or type(expected) is not int
        or not isinstance(METADATA_SHA256, str) or len(METADATA_SHA256) != 64
        or any(character not in "0123456789abcdef" for character in METADATA_SHA256)
        or not isinstance(digest, str) or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)):
    raise ValueError("capacity artifact bindings are pending or invalid")
assert isinstance(expected, int) and 0 < expected <= 384 * 1024**2
runner_temp = Path(os.environ["RUNNER_TEMP"])
assert runner_temp.is_absolute() and runner_temp.is_dir(), "RUNNER_TEMP must be an existing absolute directory"
metadata = (runner_temp / "1476-ci122-capacity-artifact-metadata.json").read_bytes()
assert hashlib.sha256(metadata).hexdigest() == METADATA_SHA256
meta = json.loads(metadata)
assert meta["id"] == ARTIFACT_ID and meta["size_in_bytes"] == expected
assert meta["digest"] == "sha256:" + digest and meta["expired"] is False
assert datetime.datetime.fromisoformat(meta["expires_at"].replace("Z", "+00:00")) > datetime.datetime.now(datetime.timezone.utc)
assert meta["workflow_run"]["id"] == 37990571919
assert meta["workflow_run"]["head_sha"] == "3914e53957fcc3ff7befe8b1a3f9a3284bfaba4b"
assert meta["workflow_run"]["repository_id"] == meta["workflow_run"]["head_repository_id"] == 780459444
assert meta["workflow_run"]["head_branch"] == "feat/1476-public-hub-capacity"
assert meta["name"] == "hub-capacity-evidence-3914e53957fcc3ff7befe8b1a3f9a3284bfaba4b-attempt-1"
assert meta["archive_download_url"] == "https://api.github.com/repos/Telcoin-Association/telcoin-network/actions/artifacts/" + str(meta["id"]) + "/zip"
destination = runner_temp / "1476-ci122-evidence.zip"
assert not destination.exists()
stat = os.statvfs(destination.parent)
free_bytes = stat.f_bavail * stat.f_frsize
assert free_bytes >= expected and free_bytes - expected >= 30 * 1024**3, "30 GiB acquisition reserve failed"
process = subprocess.Popen(
    ["gh", "api", f"repos/Telcoin-Association/telcoin-network/actions/artifacts/{ARTIFACT_ID}/zip"],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
streams = {"stdout": bytearray(), "stderr": bytearray()}
deadline = time.monotonic() + 1200
try:
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ, "stdout")
        selector.register(process.stderr, selectors.EVENT_READ, "stderr")
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("qualification evidence download exceeded 1200 seconds")
            for key, _ in selector.select(min(1, remaining)):
                kind = key.data
                limit = expected if kind == "stdout" else 8192
                chunk = os.read(key.fd, min(4096, limit + 1 - len(streams[kind])))
                if not chunk:
                    selector.unregister(key.fileobj)
                else:
                    streams[kind].extend(chunk)
                    if len(streams[kind]) > limit:
                        raise ValueError(f"qualification evidence download {kind} exceeded its byte bound")
    status = process.wait(timeout=5)
    if status != 0:
        raise RuntimeError("GitHub archive request failed")
    raw = bytes(streams["stdout"])
    assert len(raw) == expected and hashlib.sha256(raw).hexdigest() == digest
    output = destination.open("xb")
    try:
        with output:
            assert output.write(raw) == expected
    except BaseException:
        destination.unlink(missing_ok=True)
        raise
    print(json.dumps({"artifact_id": meta["id"], "actual_bytes": len(raw),
                      "actual_sha256": digest, "path": str(destination),
                      "downloaded_once": True, "stderr_bytes": len(streams["stderr"]),
                      "download_exit_code": 0, "transport_timeout_seconds": 1200}))
except Exception as exc:
    print(json.dumps({"artifact_id": meta["id"], "downloaded_once": False,
                      "download_exit_code": process.poll(),
                      "received_stdout_bytes": len(streams["stdout"]),
                      "received_stderr_bytes": len(streams["stderr"]),
                      "stderr_tail": bytes(streams["stderr"][-1024:]).decode("utf-8", "replace"),
                      "failure_type": type(exc).__name__,
                      "failure": str(exc)[:256],
                      "path": str(destination), "destination_exists": destination.exists()}),
          flush=True)
    raise
finally:
    if process.poll() is None:
        process.kill()
        process.wait(timeout=5)
    process.stdout.close()
    process.stderr.close()
