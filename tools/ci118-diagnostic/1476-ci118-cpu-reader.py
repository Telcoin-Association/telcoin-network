"""Diagnose the frozen CI118 CPU criterion from authenticated candidate samples."""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
from types import ModuleType
import zipfile
import zlib

BASE_SHA256 = "9407dcb7890ef678872486eda61ab6a16e0e2a65f8ae60e0ecfe58e34f9113ff"
base_path = Path(__file__).with_name("1476-ci118-remote-diagnostic-reader.py")
base_source = base_path.read_bytes()
if hashlib.sha256(base_source).hexdigest() != BASE_SHA256:
    raise ValueError("reviewed authentication reader source hash mismatch")
base = ModuleType("ci118_reviewed_report_reader")
exec(compile(base_source, str(base_path), "exec"), base.__dict__)
require = base.require
CPU_THRESHOLD = 0.75
MAX_PHASE = 24 * 1024**2
MAX_RAW_MEMBER = 32 * 1024**2
MAX_RAW_TOTAL = 2 * 1024**3
MAX_RAW_LINE = 8 * 1024**2
MAX_SAMPLES = 2048
MAX_HUBS = 8
MAX_BREACHES = 64
CHUNK = 64 * 1024


class BoundedArchive:
    """Preserve ZIP seek/read semantics while bounding each underlying read."""
    def __init__(self, archive):
        self.archive = archive

    def seek(self, *args):
        return self.archive.seek(*args)

    def tell(self):
        return self.archive.tell()

    def seekable(self):
        return self.archive.seekable()

    def read(self, size=-1):
        if size < 0:
            size = base.EXPECTED_SIZE - self.tell()
        require(0 <= size <= base.MAX_CENTRAL + 65557, "ZIP library read allocation bound")
        data = bytearray()
        while len(data) < size:
            chunk = self.archive.read(min(CHUNK, size - len(data)))
            if not chunk:
                break
            data.extend(chunk)
        return bytes(data)


def number(value, label, minimum=0):
    require(type(value) in (int, float) and minimum <= value <= 2**53 and math.isfinite(value),
            label + " must be a finite bounded number")
    return value


def integer(value, label, minimum=0):
    require(type(value) is int and minimum <= value <= 2**53, label + " must be a bounded integer")
    return value


def counts(value, label):
    require(type(value) is dict and len(value) <= 16, label + " map bound")
    for name, count in value.items():
        require(type(name) is str and 0 < len(name.encode("utf-8")) <= 64, label + " name bound")
        integer(count, label)
    return value


def read_member(zipped, member, maximum):
    require(member.file_size <= maximum and member.compress_size <= maximum, "selected member byte bound")
    raw = bytearray()
    with zipped.open(member) as stream:
        while chunk := stream.read(min(CHUNK, maximum + 1 - len(raw))):
            raw.extend(chunk)
            require(len(raw) <= maximum, "selected member expanded byte bound")
    require(len(raw) == member.file_size, "selected member byte count mismatch")
    return bytes(raw)


def analyze_phase(phase, plan_sha256=None):
    require(type(phase) is dict and phase.get("phase") == "candidate"
            and phase.get("revision") == base.SOURCE_HEAD, "candidate phase or source binding")
    require(type(phase.get("plan_sha256")) is str and re.fullmatch("[0-9a-f]{64}", phase["plan_sha256"])
            and (plan_sha256 is None or phase["plan_sha256"] == plan_sha256), "candidate plan hash binding")
    samples = phase.get("samples")
    require(type(samples) is list and 2 <= len(samples) <= MAX_SAMPLES, "candidate sample count bound")
    require(type(samples[0]) is dict and type(samples[0].get("hubs")) is dict, "candidate first sample schema")
    hubs = list(samples[0]["hubs"])
    require(1 <= len(hubs) <= MAX_HUBS and all(re.fullmatch("hub-[0-9]{1,2}", hub) for hub in hubs),
            "candidate hub schema or bound")
    contexts, elapsed = [], []
    for index, sample in enumerate(samples):
        require(type(sample) is dict and type(sample.get("hubs")) is dict and set(sample["hubs"]) == set(hubs),
                "candidate sample hub coverage")
        at = number(sample.get("elapsed_seconds"), "sample elapsed time")
        require((index == 0 and at == 0) or (index > 0 and 0 < at - elapsed[-1] <= 5),
                "candidate sample timing must start at zero and advance by at most five seconds")
        elapsed.append(at)
        measured = {}
        for hub in hubs:
            value = sample["hubs"][hub]
            require(type(value) is dict and type(value.get("swarms")) is dict
                    and 1 <= len(value["swarms"]) <= 8, "candidate process or swarm schema")
            measured[hub] = {"cpu_seconds": number(value.get("cpu_seconds"), "cumulative CPU"),
                             "rss_bytes": integer(value.get("rss_bytes"), "RSS", 1),
                             "progress": integer(value.get("progress"), "progress"),
                             "tasks": counts(value.get("tasks"), "process tasks"), "swarms": {}}
            for swarm, row in value["swarms"].items():
                require(type(swarm) is str and 0 < len(swarm) <= 64 and type(row) is dict, "swarm schema")
                measured[hub]["swarms"][swarm] = {"queue_occupancy": integer(row.get("queue_occupancy"), "queue"),
                                                "tasks": counts(row.get("tasks"), "swarm tasks")}
        contexts.append({"sample_index": index, "elapsed_seconds": at, "hubs": measured, "raw": {}})
    summaries, breaches, selected = {}, [], set()
    for hub in hubs:
        rates, deltas, duration, above, negative = [], [], elapsed[-1] - elapsed[0], 0, 0
        for index in range(1, len(samples)):
            before = contexts[index - 1]["hubs"][hub]["cpu_seconds"]
            after = contexts[index]["hubs"][hub]["cpu_seconds"]
            delta, seconds = after - before, elapsed[index] - elapsed[index - 1]
            rate = delta / seconds
            require(math.isfinite(rate), "CPU interval rate is not finite")
            rates.append(rate)
            deltas.append(delta)
            above += rate > CPU_THRESHOLD
            negative += delta < 0
            if delta < 0 or rate > CPU_THRESHOLD:
                require(len(breaches) < MAX_BREACHES, "all-breach output count exceeds diagnostic bound")
                nearby = list(range(max(0, index - 2), min(len(samples), index + 2)))
                selected.update(nearby)
                breaches.append({"hub": hub, "before_sample": index - 1, "after_sample": index,
                                 "nearby_context_indices": nearby, "from_elapsed_seconds": elapsed[index - 1],
                                 "to_elapsed_seconds": elapsed[index], "loop_start_interval_seconds": seconds,
                                 "cpu_before_seconds": before, "cpu_after_seconds": after,
                                 "delta_cpu_seconds": delta, "cpu_cores_using_scored_interval": rate,
                                 "above_frozen_threshold": rate > CPU_THRESHOLD, "negative_cpu_delta": delta < 0})
        summaries[hub] = {"interval_count": len(rates), "max_cpu_cores": max(rates),
                          "mean_interval_cpu_cores": sum(rates) / len(rates),
                          "duration_weighted_mean_cpu_cores": sum(deltas) / duration,
                          "positive_cpu_seconds_total": sum(max(0, delta) for delta in deltas),
                          "negative_cpu_delta_count": negative, "above_threshold_count": above,
                          "cpu_first_seconds": contexts[0]["hubs"][hub]["cpu_seconds"],
                          "cpu_last_seconds": contexts[-1]["hubs"][hub]["cpu_seconds"]}
    return {"frozen_max_cpu_cores": CPU_THRESHOLD, "sample_count": len(samples),
            "elapsed_first_seconds": elapsed[0], "elapsed_last_seconds": elapsed[-1],
            "per_hub": summaries, "breach_count": len(breaches), "all_breaches": breaches,
            "nearby_contexts": {str(index): contexts[index] for index in sorted(selected)}}, contexts, hubs, elapsed


def process_stat(raw, pid):
    require(type(raw) is str and len(raw.encode("utf-8")) <= 8192, "raw process stat bound")
    closing = raw.rfind(")")
    require(closing >= 0 and raw.split(" ", 1)[0] == str(pid), "raw process stat PID or format")
    fields = raw[closing + 2:].split()
    require(len(fields) >= 22, "raw process stat fields")
    try:
        utime, stime, identity = (int(fields[index]) for index in (11, 12, 19))
    except ValueError as exc:
        raise base.Invalid("raw process stat integer fields") from exc
    for value in (utime, stime, identity):
        integer(value, "raw process tick or identity")
    return {"utime_ticks": utime, "stime_ticks": stime, "total_cpu_ticks": utime + stime,
            "starttime_identity": identity}


def attach_raw(zipped, members, prefix, phase, analysis, contexts, hubs, elapsed):
    artifacts = phase.get("artifacts")
    require(type(artifacts) is list and 0 < len(artifacts) <= 64, "candidate artifact list bound")
    references = {}
    for item in artifacts:
        require(type(item) is dict and type(item.get("path")) is str
                and re.fullmatch("[A-Za-z0-9_.-]{1,128}", item["path"])
                and item["path"] not in references and type(item.get("sha256")) is str
                and re.fullmatch("[0-9a-f]{64}", item["sha256"]), "candidate artifact path/hash schema")
        references[item["path"]] = item["sha256"]
    segments = sorted(name for name in references if re.fullmatch("telemetry-[0-9]{3}\\.jsonl", name))
    require(len(segments) <= 60, "raw telemetry segment count bound")
    if not segments or any(prefix + name not in members for name in segments):
        return {"status": "missing", "reason": "referenced raw telemetry is absent", "segments": segments}
    require(segments == [f"telemetry-{index:03}.jsonl" for index in range(len(segments))],
            "raw telemetry segment sequence")
    require(sum(members[prefix + name].file_size for name in segments) <= MAX_RAW_TOTAL,
            "raw telemetry total byte bound")
    indices = {at: index for index, at in enumerate(elapsed)}
    seen, identities, pids, retained, hashes = set(), {hub: set() for hub in hubs}, {hub: set() for hub in hubs}, {}, []
    raw_count, expanded = 0, 0
    inferred_tick_rate = None

    def consume(line, segment, line_number):
        nonlocal raw_count, inferred_tick_rate
        row = base.strict_json(line)
        require(type(row) is dict and row.get("hub") in hubs, "raw telemetry hub schema")
        hub = row["hub"]
        at = number(row.get("elapsed_seconds"), "raw elapsed time")
        require(at in indices, "raw telemetry has no matching scored sample")
        index = indices[at]
        key = index, hub
        require(key not in seen, "duplicate raw telemetry sample")
        seen.add(key)
        raw_count += 1
        require(raw_count <= MAX_SAMPLES * MAX_HUBS, "raw telemetry row count bound")
        pid = integer(row.get("pid"), "raw PID", 1)
        parsed = process_stat(row.get("stat"), pid)
        ticks = integer(parsed["total_cpu_ticks"], "raw total CPU ticks")
        cpu_seconds = contexts[index]["hubs"][hub]["cpu_seconds"]
        if cpu_seconds == 0:
            require(ticks == 0, "raw ticks disagree with zero scored CPU")
        else:
            ratio = ticks / cpu_seconds
            require(math.isfinite(ratio) and 1 <= ratio <= 1_000_000, "inferred CPU tick rate bound")
            rate = round(ratio)
            require(math.isclose(ratio, rate, rel_tol=1e-12, abs_tol=1e-9), "inferred CPU tick rate is not integral")
            if inferred_tick_rate is None:
                inferred_tick_rate = rate
            require(rate == inferred_tick_rate
                    and math.isclose(ticks, cpu_seconds * inferred_tick_rate, rel_tol=1e-12, abs_tol=1e-6),
                    "raw CPU ticks disagree with scored CPU or common inferred tick rate")
        start = number(row.get("scrape_started_elapsed_seconds"), "scrape start")
        complete = number(row.get("scrape_completed_elapsed_seconds"), "scrape completion")
        require(at <= start <= complete, "raw scrape timing order")
        unix = integer(row.get("scrape_started_unix_us"), "scrape unix timestamp", 1)
        identities[hub].add(parsed["starttime_identity"])
        pids[hub].add(pid)
        require(len(identities[hub]) <= 32 and len(pids[hub]) <= 32, "process identity output bound")
        fence = row.get("committee_fence")
        fence_identity = None
        if fence is not None:
            require(type(fence) is dict, "raw committee fence schema")
            fence_identity = integer(fence.get("process_identity"), "fence process identity")
        if str(index) in analysis["nearby_contexts"]:
            context = {"segment": segment, "line": line_number, "row_sha256": hashlib.sha256(line).hexdigest(),
                       "pid": pid, **parsed, "scrape_started_elapsed_seconds": start,
                       "scrape_completed_elapsed_seconds": complete, "scrape_duration_seconds": complete - start,
                       "scrape_start_offset_from_loop_seconds": start - at, "scrape_started_unix_us": unix,
                       "scored_cpu_seconds": cpu_seconds,
                       "fence_process_identity": fence_identity,
                       "fence_identity_matches_stat": None if fence_identity is None else fence_identity == parsed["starttime_identity"]}
            contexts[index]["raw"][hub] = context
            retained[key] = context

    for name in segments:
        member = members[prefix + name]
        require(member.file_size <= MAX_RAW_MEMBER and member.compress_size <= MAX_RAW_MEMBER,
                "raw telemetry member byte bound")
        digest, buffer, count, line_number = hashlib.sha256(), bytearray(), 0, 0
        with zipped.open(member) as stream:
            while chunk := stream.read(CHUNK):
                count += len(chunk)
                expanded += len(chunk)
                require(count <= MAX_RAW_MEMBER and expanded <= MAX_RAW_TOTAL, "raw expanded byte bound")
                digest.update(chunk)
                buffer.extend(chunk)
                while (end := buffer.find(b"\n")) >= 0:
                    require(end <= MAX_RAW_LINE, "raw telemetry line byte bound")
                    line_number += 1
                    consume(bytes(buffer[:end]), name, line_number)
                    del buffer[:end + 1]
                require(len(buffer) <= MAX_RAW_LINE, "raw telemetry line byte bound")
        require(not buffer and count == member.file_size, "raw telemetry incomplete line or byte count")
        require(digest.hexdigest() == references[name], "raw telemetry artifact SHA256 mismatch")
        hashes.append({"member": member.filename, "bytes": count, "sha256": digest.hexdigest()})
    require(len(seen) == len(elapsed) * len(hubs), "raw telemetry scored-sample coverage incomplete")
    for context in retained.values():
        context["inferred_clock_ticks_per_second"] = inferred_tick_rate
        context["ticks_consistent_with_scored_cpu"] = True
    for breach in analysis["all_breaches"]:
        before, after, hub = breach["before_sample"], breach["after_sample"], breach["hub"]
        left, right = retained[(before, hub)], retained[(after, hub)]
        scrape_interval = right["scrape_started_elapsed_seconds"] - left["scrape_started_elapsed_seconds"]
        require(scrape_interval > 0, "raw scrape-start intervals must advance")
        breach["raw_comparison"] = {"delta_cpu_ticks": right["total_cpu_ticks"] - left["total_cpu_ticks"],
                                     "starttime_changed": left["starttime_identity"] != right["starttime_identity"],
                                     "pid_changed": left["pid"] != right["pid"],
                                     "scrape_start_interval_seconds": scrape_interval}
    return {"status": "present", "rows": raw_count, "expanded_bytes": expanded, "segments": hashes,
            "inferred_clock_ticks_per_second": inferred_tick_rate,
            "tick_rate_note": "Inferred common integer ratio of raw CPU ticks to scored CPU seconds; not retained kernel clock metadata.",
            "process_identities": {hub: {"pids": sorted(pids[hub]), "starttime_identities": sorted(identities[hub])}
                                   for hub in hubs}}


def diagnose(metadata, archive):
    archive = BoundedArchive(archive)
    previous = base.diagnose(metadata, archive)
    result = {key: value for key, value in previous.items() if key != "ci_report"}
    if previous["report_status"] == "present":
        result["reported_candidate_failures"] = previous["ci_report"]["candidate"]["failures"]
    result["cpu_diagnostic_status"] = "unavailable"
    if previous["report_status"] == "archive_invalid":
        return result
    try:
        zipped, _ = base.inventory(archive)
        with zipped:
            members = {member.filename: member for member in zipped.infolist()}
            candidates = [name for name in members if name == "candidate-evidence/evidence.json" or name.endswith("/candidate-evidence/evidence.json")]
            require(len(candidates) <= 1, "multiple candidate-evidence/evidence.json members")
            if not candidates:
                return result | {"cpu_diagnostic_status": "missing", "reason": "candidate-evidence/evidence.json is absent"}
            member = members[candidates[0]]
            raw = read_member(zipped, member, MAX_PHASE)
            phase = base.strict_json(raw)
            plan = previous.get("ci_report", {}).get("plan_sha256")
            analysis, contexts, hubs, elapsed = analyze_phase(phase, plan)
            prefix = candidates[0][:-len("evidence.json")]
            raw_summary = attach_raw(zipped, members, prefix, phase, analysis, contexts, hubs, elapsed)
            observed_hubs = sorted(hub for hub, row in analysis["per_hub"].items()
                                   if row["above_threshold_count"] or row["negative_cpu_delta_count"])
            reported_hubs = sorted(failure.split(":", 1)[0] for failure in result.get("reported_candidate_failures", [])
                                   if re.fullmatch("hub-[0-9]{1,2}: CPU headroom exhausted or process restarted", failure))
            consistency = {"reported_cpu_failure_hubs": reported_hubs, "observed_cpu_breach_hubs": observed_hubs,
                           "status": "unavailable" if previous["report_status"] != "present"
                           else "consistent" if reported_hubs == observed_hubs else "inconsistent"}
            status = "incomplete" if raw_summary["status"] != "present" or consistency["status"] == "unavailable" \
                else "inconsistent" if consistency["status"] == "inconsistent" else "present"
            result.update(cpu_diagnostic_status=status, report_cpu_attribution=consistency, candidate_member=member.filename,
                          candidate_bytes=len(raw), candidate_sha256=hashlib.sha256(raw).hexdigest(),
                          cpu_analysis=analysis, raw_telemetry=raw_summary,
                          timing_note="Scored rates use adjacent loop-start timestamps. Scrape start occurs after the /proc read and is not its exact timestamp. No alternate timing is used to qualify or rescore.")
            require(len(json.dumps(result, allow_nan=False).encode("utf-8")) <= base.MAX_SUMMARY - 4096,
                    "complete CPU diagnostic exceeds output byte bound")
            return result
    except (base.Invalid, ValueError, KeyError, TypeError, UnicodeError, OSError, EOFError,
            RuntimeError, zipfile.BadZipFile, zlib.error) as exc:
        return {key: value for key, value in result.items() if key not in ("cpu_analysis", "raw_telemetry")} | {
            "cpu_diagnostic_status": "invalid", "reason": str(exc)[:256]}


def main():
    env = os.environ
    helper_sha = env["EXPECTED_HELPER_SHA"]
    require(re.fullmatch("[0-9a-f]{40}", helper_sha) and env["ACTUAL_WORKFLOW_COMMIT"] == helper_sha
            and env["ACTUAL_REPOSITORY"] == base.REPOSITORY and env["ACTUAL_REF"] == base.HELPER_REF
            and env["ACTUAL_EVENT"] == "workflow_dispatch"
            and env["ACTUAL_WORKFLOW_REF"] == base.REPOSITORY + "/.github/workflows/durable-e2e.yaml@" + base.HELPER_REF,
            "diagnostic workflow provenance mismatch")
    require(all(re.fullmatch("[1-9][0-9]{0,19}", env[key]) for key in ("ACTUAL_RUN_ID", "ACTUAL_RUN_ATTEMPT")),
            "diagnostic Actions run provenance invalid")
    temp = Path(env["RUNNER_TEMP"])
    require(temp.is_absolute() and temp.is_dir(), "RUNNER_TEMP is not an existing absolute directory")
    with (temp / "1476-ci118-capacity-artifact-metadata.json").open("rb") as stream:
        metadata = stream.read(8193)
    with (temp / "1476-ci118-evidence.zip").open("rb") as archive:
        result = diagnose(metadata, archive)
    result["diagnostic_actions"] = {"repository": env["ACTUAL_REPOSITORY"], "ref": env["ACTUAL_REF"],
                                    "workflow_ref": env["ACTUAL_WORKFLOW_REF"], "head_sha": helper_sha,
                                    "run_id": int(env["ACTUAL_RUN_ID"]), "run_attempt": int(env["ACTUAL_RUN_ATTEMPT"])}
    output = json.dumps(result, sort_keys=True, allow_nan=False)
    require(len(output.encode("utf-8")) <= base.MAX_SUMMARY, "final diagnostic output byte bound")
    print(output)
    return 0 if result["cpu_diagnostic_status"] == "present" else 1


if __name__ == "__main__":
    sys.exit(main())
