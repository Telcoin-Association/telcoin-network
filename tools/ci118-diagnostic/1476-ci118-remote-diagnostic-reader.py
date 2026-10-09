"""Authenticate CI118 evidence and print only the CI-produced scoring report."""
import datetime
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import stat
import struct
import sys
import zipfile
import zlib

ARTIFACT_ID = 11615103682
EXPECTED_SIZE = 139764345
ARCHIVE_SHA256 = "a07070c2cdce86e631acdfaf04ae02a2ee52fdb3fcd9b1999b6ebf692702dd38"
METADATA_SHA256 = "ad1ae221ede54300de425b5a99e5fde495c25e6cc5ea0db3ea6c83cb838000ba"
SOURCE_HEAD = "4556f835e3d4a6f803afc941f4cfcd9cfd7faad1"
SOURCE_RUN = 37914665470
REPOSITORY = "Telcoin-Association/telcoin-network"
HELPER_REF = "refs/heads/work/1476-ci118-verification"
MAX_ARCHIVE = 384 * 1024**2
MAX_CENTRAL = 8 * 1024**2
MAX_MEMBERS = 4096
MAX_MEMBER = 512 * 1024**2
MAX_EXPANDED = 8 * 1024**3
MAX_REPORT = 256 * 1024
MAX_SUMMARY = 128 * 1024


class Invalid(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise Invalid(message)


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def constant(_):
        raise Invalid("non-finite JSON number")

    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise Invalid("invalid JSON: " + str(exc)[:160]) from exc


def authenticate(metadata, archive):
    require(len(metadata) <= 8192 and hashlib.sha256(metadata).hexdigest() == METADATA_SHA256,
            "official metadata byte hash mismatch")
    meta = strict_json(metadata)
    require(type(meta) is dict and type(meta.get("workflow_run")) is dict, "metadata schema")
    run = meta.get("workflow_run", {})
    require(meta.get("id") == ARTIFACT_ID and meta.get("size_in_bytes") == EXPECTED_SIZE
            and meta.get("digest") == "sha256:" + ARCHIVE_SHA256 and meta.get("expired") is False,
            "official artifact binding mismatch")
    require(run.get("id") == SOURCE_RUN and run.get("head_sha") == SOURCE_HEAD
            and run.get("repository_id") == run.get("head_repository_id") == 780459444
            and run.get("head_branch") == "feat/1476-public-hub-capacity",
            "official source binding mismatch")
    require(meta.get("name") == "hub-capacity-evidence-" + SOURCE_HEAD + "-attempt-1"
            and meta.get("archive_download_url") == "https://api.github.com/repos/" + REPOSITORY
            + "/actions/artifacts/" + str(ARTIFACT_ID) + "/zip", "official artifact identity mismatch")
    require(datetime.datetime.fromisoformat(meta["expires_at"].replace("Z", "+00:00"))
            > datetime.datetime.now(datetime.timezone.utc), "official artifact expired")
    require(type(EXPECTED_SIZE) is int and 0 < EXPECTED_SIZE <= MAX_ARCHIVE, "archive size bound")
    archive.seek(0, 2)
    require(archive.tell() == EXPECTED_SIZE, "archive byte count mismatch")
    archive.seek(0)
    digest = hashlib.sha256()
    count = 0
    while chunk := archive.read(64 * 1024):
        count += len(chunk)
        require(count <= EXPECTED_SIZE, "archive grew during authentication")
        digest.update(chunk)
    require(count == EXPECTED_SIZE and digest.hexdigest() == ARCHIVE_SHA256,
            "whole archive SHA256 mismatch")
    return meta


def inventory(archive):
    archive.seek(max(0, EXPECTED_SIZE - 65557))
    tail = archive.read(65557)
    offset = tail.rfind(b"PK\x05\x06")
    require(offset >= 0 and len(tail) - offset >= 22, "missing ZIP end record")
    end = struct.unpack("<4s4H2IH", tail[offset:offset + 22])
    _, disk, central_disk, disk_count, count, central_size, central_offset, comment = end
    end_offset = EXPECTED_SIZE - len(tail) + offset
    require(disk == central_disk == 0 and disk_count == count and 0 < count <= MAX_MEMBERS,
            "multi-disk, ZIP64 or member count bound")
    require(central_size <= MAX_CENTRAL and central_offset + central_size == end_offset
            and offset + 22 + comment == len(tail), "central directory or trailing byte bound")
    archive.seek(0)
    require(archive.read(4) == b"PK\x03\x04", "ZIP prefix is not a local header")
    zipped = zipfile.ZipFile(archive)
    try:
        infos = zipped.infolist()
        require(len(infos) == count, "central member count mismatch")
        names, spans, reports = set(), [], []
        expanded = 0
        for member in infos:
            name = member.orig_filename
            trimmed = name[:-1] if name.endswith("/") else name
            require(0 < len(name.encode("utf-8")) <= 1024 and "\x00" not in name
                    and "\\" not in name and ":" not in name
                    and not name.startswith("/") and all(part not in ("", ".", "..")
                                                          for part in trimmed.split("/")),
                    "unsafe archive member name")
            require(name not in names, "duplicate archive member")
            names.add(name)
            mode = stat.S_IFMT(member.external_attr >> 16)
            require(mode in (0, stat.S_IFREG, stat.S_IFDIR), "non-regular archive member")
            require(member.flag_bits & ~0x808 == 0 and member.compress_type in (0, 8),
                    "encrypted or unsupported archive member")
            require(0 <= member.compress_size <= MAX_ARCHIVE and 0 <= member.file_size <= MAX_MEMBER,
                    "member byte bound")
            expanded += member.file_size
            require(expanded <= MAX_EXPANDED, "expanded archive byte bound")
            require(not member.is_dir() or member.file_size == 0, "nonempty directory member")
            require(0 <= member.header_offset < central_offset, "local header offset bound")
            archive.seek(member.header_offset)
            raw = archive.read(30)
            require(len(raw) == 30, "truncated local header")
            header = struct.unpack("<4s5H3I2H", raw)
            signature, _, flags, method, _, _, crc, compressed, size, name_size, extra_size = header
            require(signature == b"PK\x03\x04" and flags == member.flag_bits
                    and method == member.compress_type and name_size <= 1024 and extra_size <= 4096,
                    "local header mismatch or bound")
            local_name = archive.read(name_size).decode("utf-8" if flags & 0x800 else "cp437")
            require(local_name == name, "local and central member name mismatch")
            if not flags & 8:
                require((crc, compressed, size) == (member.CRC, member.compress_size, member.file_size),
                        "local and central size or CRC mismatch")
            start = member.header_offset
            finish = start + 30 + name_size + extra_size + member.compress_size
            require(finish <= central_offset, "member data overlaps central directory")
            spans.append((start, finish))
            if PurePosixPath(name).name == "report.json" and not member.is_dir():
                reports.append(member)
        require(all(left[1] <= right[0] for left, right in zip(sorted(spans), sorted(spans)[1:])),
                "overlapping archive members")
        require(len(reports) <= 1, "multiple report.json members")
        return zipped, reports
    except BaseException:
        zipped.close()
        raise


def validate_report(report):
    require(type(report) is dict and set(report) == {"plan_sha256", "baseline", "candidate"},
            "report root schema")
    require(type(report["plan_sha256"]) is str and re.fullmatch("[0-9a-f]{64}", report["plan_sha256"]),
            "report plan hash schema")
    for label in ("baseline", "candidate"):
        score = report[label]
        require(type(score) is dict and set(score) == {"passed", "failures", "scenarios"},
                label + " score schema")
        failures, scenarios = score["failures"], score["scenarios"]
        require(type(score["passed"]) is bool and type(failures) is list and len(failures) <= 64
                and all(type(failure) is str and 0 < len(failure.encode("utf-8")) <= 512
                        for failure in failures), label + " failure schema or bound")
        require(failures == sorted(set(failures)) and score["passed"] == (not failures),
                label + " passed/failures consistency")
        require(type(scenarios) is dict and 0 < len(scenarios) <= 32, label + " scenario count bound")
        for name, measured in scenarios.items():
            require(0 < len(name.encode("utf-8")) <= 128 and type(measured) is dict
                    and set(measured) == {"attempts", "cancelled", "success_rate", "p99_ms"},
                    label + " scenario schema")
            require(all(type(measured[key]) is int and 0 <= measured[key] <= 2**53
                        for key in ("attempts", "cancelled")), label + " scenario count schema")
            require(all(type(measured[key]) in (int, float) and 0 <= measured[key] <= 2**53
                        and math.isfinite(measured[key]) for key in ("success_rate", "p99_ms"))
                    and measured["success_rate"] <= 1, label + " scenario measurement schema")
    return report


def diagnose(metadata, archive):
    summary = {"diagnostic_only": True, "capacity_qualified": False, "independently_rescored": False,
               "source_run_id": SOURCE_RUN, "source_run_attempt": 1, "source_head": SOURCE_HEAD,
               "source_workflow": ".github/workflows/durable-e2e.yaml", "artifact_id": ARTIFACT_ID,
               "archive_bytes": EXPECTED_SIZE, "archive_sha256": ARCHIVE_SHA256,
               "metadata_sha256": METADATA_SHA256, "archive_authenticated": False}
    try:
        authenticate(metadata, archive)
        summary["archive_authenticated"] = True
        zipped, reports = inventory(archive)
    except (Invalid, ValueError, OSError, zipfile.BadZipFile, UnicodeError, struct.error) as exc:
        return summary | {"report_status": "archive_invalid", "reason": str(exc)[:256]}
    with zipped:
        if not reports:
            return summary | {"report_status": "missing", "reason": "authenticated ZIP contains no report.json"}
        member = reports[0]
        try:
            require(member.file_size <= MAX_REPORT and member.compress_size <= MAX_REPORT,
                    "report byte bound")
            raw = bytearray()
            with zipped.open(member) as stream:
                while chunk := stream.read(min(4096, MAX_REPORT + 1 - len(raw))):
                    raw.extend(chunk)
                    require(len(raw) <= MAX_REPORT, "expanded report byte bound")
            require(len(raw) == member.file_size, "report byte count mismatch")
            report = validate_report(strict_json(bytes(raw)))
            result = summary | {"report_status": "present", "report_member": member.filename,
                                "report_bytes": len(raw), "report_sha256": hashlib.sha256(raw).hexdigest(),
                                "ci_report": report}
            require(len(json.dumps(result, allow_nan=False).encode("utf-8")) <= MAX_SUMMARY - 4096,
                    "diagnostic summary byte bound")
            return result
        except (Invalid, ValueError, OSError, UnicodeError, zipfile.BadZipFile, EOFError, RuntimeError, zlib.error) as exc:
            return summary | {"report_status": "invalid", "reason": str(exc)[:256]}


def main():
    env = os.environ
    helper_sha = env["EXPECTED_HELPER_SHA"]
    require(re.fullmatch("[0-9a-f]{40}", helper_sha) and env["ACTUAL_WORKFLOW_COMMIT"] == helper_sha
            and env["ACTUAL_REPOSITORY"] == REPOSITORY and env["ACTUAL_REF"] == HELPER_REF
            and env["ACTUAL_EVENT"] == "workflow_dispatch"
            and env["ACTUAL_WORKFLOW_REF"] == REPOSITORY + "/.github/workflows/durable-e2e.yaml@" + HELPER_REF,
            "diagnostic workflow provenance mismatch")
    require(all(re.fullmatch("[1-9][0-9]{0,19}", env[key]) for key in ("ACTUAL_RUN_ID", "ACTUAL_RUN_ATTEMPT")),
            "diagnostic Actions run provenance invalid")
    temp = Path(env["RUNNER_TEMP"])
    require(temp.is_absolute() and temp.is_dir(), "RUNNER_TEMP is not an existing absolute directory")
    metadata_path = temp / "1476-ci118-capacity-artifact-metadata.json"
    with metadata_path.open("rb") as stream:
        metadata = stream.read(8193)
    with (temp / "1476-ci118-evidence.zip").open("rb") as archive:
        result = diagnose(metadata, archive)
    result["diagnostic_actions"] = {"repository": env["ACTUAL_REPOSITORY"], "ref": env["ACTUAL_REF"],
                                    "workflow_ref": env["ACTUAL_WORKFLOW_REF"], "head_sha": helper_sha,
                                    "run_id": int(env["ACTUAL_RUN_ID"]),
                                    "run_attempt": int(env["ACTUAL_RUN_ATTEMPT"])}
    output = json.dumps(result, sort_keys=True, allow_nan=False)
    require(len(output.encode("utf-8")) <= MAX_SUMMARY, "final diagnostic output byte bound")
    print(output)
    return 0 if result["report_status"] in ("present", "missing") else 1


if __name__ == "__main__":
    sys.exit(main())
