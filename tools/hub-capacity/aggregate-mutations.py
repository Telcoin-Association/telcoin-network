#!/usr/bin/env python3
"""Require complete, current-source mutation proofs before qualification or PR acceptance."""

import argparse
import hashlib
import importlib.util
import itertools
import json
from pathlib import Path
import re
import shutil


SPEC = importlib.util.spec_from_file_location("capacity_mutations", Path(__file__).with_name("mutate-rust.py"))
MUTATIONS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MUTATIONS)
SHARD_COUNT = 2
MAX_JSON_BYTES = 4 * 1024**2


def read_file(path, limit):
    """Read bounded regular evidence files, rejecting links and truncated or oversized input."""
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"missing or nonregular mutation evidence: {path.name}")
    with path.open("rb") as source:
        raw = source.read(limit + 1)
    if len(raw) > limit:
        raise ValueError(f"mutation evidence exceeds its byte bound: {path.name}")
    return raw


def unique_object(pairs):
    """Reject duplicate JSON fields rather than interpreting only their last value."""
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate mutation evidence JSON field")
        result[name] = value
    return result


def read_json(path):
    """Parse bounded strict JSON; nonfinite numeric claims are invalid evidence."""
    return json.loads(read_file(path, MAX_JSON_BYTES), object_pairs_hook=unique_object,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError("nonfinite mutation evidence")))


def command_receipt(directory, receipt, argv, name, stage, exit_code, regression):
    """Validate one expected command and its retained raw output, not a summary claim alone."""
    if not isinstance(receipt, dict) or set(receipt) != {"argv", "exit_code", "log", "sha256"}:
        raise ValueError("invalid mutation command receipt")
    log = f"{name}-{stage}.log"
    if receipt["argv"] != argv or receipt["log"] != log or type(receipt["exit_code"]) is not int or receipt["exit_code"] != exit_code:
        raise ValueError("mutation command identity or outcome mismatch")
    path = directory / log
    raw = read_file(path, MUTATIONS.MAX_LOG_BYTES)
    if hashlib.sha256(raw).hexdigest() != receipt["sha256"]:
        raise ValueError("mutation command log hash mismatch")
    text = re.sub(r"\x1b\[[0-9;]*m", "", raw.decode(errors="replace"))
    marker = "PASS" if stage == "control" else "FAIL"
    if stage == "compile":
        if "Finished" not in text:
            raise ValueError("mutation compiler log has no completed compilation")
    elif not re.search(r"(?:^|\n)\s*" + marker + r"[^\n]*\b" + re.escape(regression) + r"(?:\s|$)", text):
        raise ValueError("mutation log lacks the selected regression outcome")
    return path


def aggregate(inputs, output, expected, shard_result):
    """Reconcile both exact partitions, source preimages and all three proofs per case."""
    if shard_result != "success" or len(inputs) != SHARD_COUNT:
        raise ValueError("all mutation shards must succeed and be present")
    expected_names = [case[0] for case in MUTATIONS.CASES]
    if len(set(expected_names)) != len(expected_names):
        raise ValueError("duplicate mutation registry identity")
    verified = {}
    logs = []
    seen_shards = set()
    manifests = []
    for directory in inputs:
        if directory.is_symlink() or not directory.is_dir():
            raise ValueError("missing or nonregular mutation shard directory")
        manifest = read_json(directory / "manifest.json")
        if not isinstance(manifest, dict) or set(manifest) != {"version", "provenance", "shard_index", "shard_count", "cases", "complete"}:
            raise ValueError("invalid mutation shard manifest")
        index = manifest["shard_index"]
        if type(index) is not int or not 0 <= index < SHARD_COUNT or index in seen_shards:
            raise ValueError("duplicate or invalid mutation shard identity")
        selected = MUTATIONS.select_cases(index, SHARD_COUNT)
        names = [case[0] for case in selected]
        if manifest["version"] != 1 or type(manifest["version"]) is not int or manifest["shard_count"] != SHARD_COUNT or type(manifest["shard_count"]) is not int or manifest["complete"] is not True or manifest["provenance"] != expected or manifest["cases"] != names:
            raise ValueError("mutation shard provenance, partition or completeness mismatch")
        seen_shards.add(index)
        expected_files = {"manifest.json", "report.json"} | {f"{name}-{stage}.log" for name in names for stage in ("control", "compile", "test")}
        files = list(itertools.islice(directory.iterdir(), len(expected_files) + 1))
        if {path.name for path in files} != expected_files:
            raise ValueError("mutation shard log set is incomplete or contains extra evidence")
        rows = read_json(directory / "report.json")
        if not isinstance(rows, list) or len(rows) != len(selected):
            raise ValueError("mutation shard report is incomplete")
        for case, row in zip(selected, rows):
            name, relative, before, after, regression = case
            row_fields = {"mutation", "path", "package", "regression", "source_sha256",
                          "mutated_sha256", "restored_sha256", "control", "compilation", "test", "detected"}
            if not isinstance(row, dict) or set(row) != row_fields or row.get("mutation") != name or row.get("path") != relative or row.get("regression") != regression or name in verified:
                raise ValueError("mutation report case identity mismatch")
            original = read_file(MUTATIONS.ROOT / relative, MUTATIONS.MAX_LOG_BYTES)
            text = original.decode()
            if text.count(before) != 1:
                raise ValueError("mutation registry must select exactly one source expression")
            source_hash = hashlib.sha256(original).hexdigest()
            mutant_hash = hashlib.sha256(text.replace(before, after).encode()).hexdigest()
            if row.get("source_sha256") != source_hash or row.get("restored_sha256") != source_hash or row.get("mutated_sha256") != mutant_hash or row.get("detected") is not True:
                raise ValueError("mutation source, restoration or detection mismatch")
            package, compilation, test = MUTATIONS.mutation_commands(relative, regression)
            if row.get("package") != package:
                raise ValueError("mutation source owner mismatch")
            logs.extend(command_receipt(directory, row.get(stage), argv, name, label, code, regression)
                        for stage, argv, label, code in (("control", test, "control", 0),
                                                       ("compilation", compilation, "compile", 0),
                                                       ("test", test, "test", 100)))
            verified[name] = row
        manifests.append({"shard_index": index,
                          "manifest_sha256": hashlib.sha256(read_file(directory / "manifest.json", MAX_JSON_BYTES)).hexdigest(),
                          "report_sha256": hashlib.sha256(read_file(directory / "report.json", MAX_JSON_BYTES)).hexdigest()})
    if seen_shards != set(range(SHARD_COUNT)) or set(verified) != set(expected_names) or len(logs) != 3 * len(expected_names):
        raise ValueError("mutation aggregate is not the complete expected proof set")
    output.mkdir(parents=True, exist_ok=False)
    for log in logs:
        shutil.copyfile(log, output / log.name)
        name, stage = log.name.rsplit("-", 1)
        key = "compilation" if stage == "compile.log" else stage.removesuffix(".log")
        if hashlib.sha256(read_file(output / log.name, MUTATIONS.MAX_LOG_BYTES)).hexdigest() != verified[name][key]["sha256"]:
            raise ValueError("mutation log changed during aggregation")
    (output / "report.json").write_text(json.dumps([verified[name] for name in expected_names], sort_keys=True, indent=2) + "\n")
    summary = {"version": 1, "provenance": expected, "cases": expected_names,
               "case_count": len(verified), "log_count": len(logs), "complete": True,
               "shards": sorted(manifests, key=lambda value: value["shard_index"])}
    (output / "manifest.json").write_text(json.dumps(summary, sort_keys=True, indent=2) + "\n")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True)
    parser.add_argument("--shards-result", choices=("success",), required=True)
    args = parser.parse_args()
    expected = MUTATIONS.provenance(args.source, args.run_id, args.run_attempt)
    summary = aggregate(args.input, args.output, expected, args.shards_result)
    if MUTATIONS.provenance(args.source, args.run_id, args.run_attempt) != expected:
        raise ValueError("mutation aggregate source provenance changed")
    print(json.dumps({"cases": summary["case_count"], "logs": summary["log_count"], "complete": True}))


if __name__ == "__main__":
    main()
