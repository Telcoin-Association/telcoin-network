"""Authenticate and safely unpack one bounded CI122 mutation artifact."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import zlib
from zipfile import ZipFile


MAX_ARCHIVE = 1024 * 1024
MAX_EXPANDED = 32 * 1024 * 1024
RESERVE = 30 * 1024**3
REPOSITORY_ID = 780459444
ARTIFACT_ID: int | None = 11649838509


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--metadata-sha256", required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--dest", type=Path, required=True)
    parser.add_argument("--destination-parent", type=Path, required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--run", type=int, required=True)
    parser.add_argument("--head", required=True)
    args = parser.parse_args()

    if type(ARTIFACT_ID) is not int or ARTIFACT_ID <= 0:
        raise ValueError("aggregate artifact identity is pending")
    if not re.fullmatch(r"[0-9a-f]{64}", args.metadata_sha256):
        raise ValueError("expected metadata digest is malformed")
    if args.metadata.is_symlink() or not args.metadata.is_file() or args.metadata.stat().st_size > 8192:
        raise ValueError("metadata is not a bounded regular file")
    metadata_raw = args.metadata.read_bytes()
    if hashlib.sha256(metadata_raw).hexdigest() != args.metadata_sha256:
        raise ValueError("official metadata digest differs from reviewed pin")
    entry = json.loads(metadata_raw)
    workflow = entry["workflow_run"]
    if (entry["id"] != ARTIFACT_ID or entry["name"] != args.name
            or entry["expired"] is not False
            or workflow["id"] != args.run or workflow["head_sha"] != args.head
            or workflow["repository_id"] != REPOSITORY_ID
            or workflow["head_repository_id"] != REPOSITORY_ID):
        raise ValueError("artifact metadata differs from the expected run and source")
    digest = entry["digest"]
    if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError("official artifact digest is missing or malformed")

    if args.archive.is_symlink() or not args.archive.is_file():
        raise ValueError("archive is not a regular file")
    size = args.archive.stat().st_size
    if size != entry["size_in_bytes"] or size > MAX_ARCHIVE:
        raise ValueError("archive size differs from metadata or exceeds 1 MiB")
    with args.archive.open("rb") as stream:
        actual = hashlib.file_digest(stream, "sha256").hexdigest()
    if actual != digest.removeprefix("sha256:"):
        raise ValueError("archive SHA256 differs from official metadata")

    if args.dest.parent.resolve(strict=True) != args.destination_parent.resolve(strict=True):
        raise ValueError("destination parent differs from the explicitly authorized directory")
    with ZipFile(args.archive) as zipped:
        infos = zipped.infolist()
        if len(infos) != 233 or len({info.filename.casefold() for info in infos}) != 233:
            raise ValueError("archive must contain 233 unique members")
        expanded = 0
        for info in infos:
            name = info.filename
            if (not re.fullmatch(
                    r"(?:manifest|report)\.json|[a-z][a-z0-9_]*-(?:control|compile|test)\.log",
                    name)
                    or stat.S_IFMT(info.external_attr >> 16) != stat.S_IFREG
                    or info.flag_bits & 1):
                raise ValueError("archive contains an unsafe, special or encrypted member")
            expanded += info.file_size
            if expanded > MAX_EXPANDED:
                raise ValueError("archive expands beyond 32 MiB")

        free = os.statvfs(args.dest.parent)
        if free.f_bavail * free.f_frsize - expanded < RESERVE:
            raise ValueError("less than 30 GiB would remain after extraction")
        args.dest.mkdir(parents=False, exist_ok=False)
        copied = 0
        for info in infos:
            remaining = info.file_size
            crc = 0
            path = args.dest / info.filename
            with zipped.open(info) as src, path.open("xb") as dst:
                while remaining:
                    chunk = src.read(min(65536, remaining))
                    if not chunk:
                        raise ValueError("archive member ended before its declared size")
                    remaining -= len(chunk)
                    copied += len(chunk)
                    if copied > MAX_EXPANDED:
                        raise ValueError("actual expansion exceeds 32 MiB")
                    crc = zlib.crc32(chunk, crc)
                    dst.write(chunk)
            if crc & 0xffffffff != info.CRC:
                raise ValueError("archive member CRC differs from ZIP metadata")
        if copied != expanded:
            raise ValueError("decompressed size differs from ZIP metadata")

    print(json.dumps({"artifact_id": entry["id"], "archive_sha256": actual,
                      "archive_bytes": size, "entries": len(infos),
                      "expanded_bytes": expanded, "crc_checked": True,
                      "safe_paths_and_types": True, "destination": str(args.dest),
                      "metadata_sha256": args.metadata_sha256}, sort_keys=True))


if __name__ == "__main__":
    main()
