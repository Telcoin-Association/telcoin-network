#!/usr/bin/env python3
"""Check data bindings in an already authenticated hub capacity evidence tree.

This does not authenticate a GitHub artifact, execute retained code, or score a
candidate. The caller must independently authenticate both artifact metadata
records and derive the three actual hashes from authenticated binary bytes.
The trusted publication helper must still verify source bytes and rescore.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import stat


MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_EVIDENCE_BYTES = 24 * 1024 * 1024
MAX_SOURCE_BYTES = 1024 * 1024
MAX_TRANSACTION_BYTES = 64 * 1024 * 1024
BINARIES = {"telcoin-network", "node-record-api", "hub-capacity-peer"}
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
PHASES = ("baseline", "candidate")


class BindingError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise BindingError(message)


def object_pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def reject_constant(value):
    raise BindingError(f"nonfinite JSON value: {value}")


def read_bytes(path, maximum=MAX_JSON_BYTES):
    require(type(maximum) is int and maximum in (MAX_JSON_BYTES, MAX_EVIDENCE_BYTES, MAX_SOURCE_BYTES),
            "invalid JSON input cap")
    require(not any(parent.is_symlink() for parent in (path, *path.parents)),
            f"symlink input: {path}")
    info = path.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_size <= maximum,
            f"missing, nonregular, or oversized JSON input: {path}")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    require(len(raw) <= maximum, f"oversized JSON input: {path}")
    return raw


def read_json(path, maximum=MAX_JSON_BYTES):
    raw = read_bytes(path, maximum)
    try:
        return json.loads(raw, object_pairs_hook=object_pairs, parse_constant=reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BindingError(f"malformed JSON input: {path}") from error


def hash_file(path, maximum):
    require(not any(parent.is_symlink() for parent in (path, *path.parents)),
            f"symlink input: {path}")
    info = path.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_size <= maximum,
            f"missing, nonregular, or oversized input: {path}")
    sha = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            size += len(chunk)
            require(size <= maximum, f"oversized input: {path}")
            sha.update(chunk)
    return sha.hexdigest()


def owned_file(root, name):
    require(root.is_dir() and not root.is_symlink(), f"unsafe root: {root}")
    parts = name.split("/")
    require(all(part not in ("", ".", "..") for part in parts), f"unsafe relative path: {name}")
    path = root
    for part in parts:
        path = path / part
        require(not path.is_symlink(), f"symlink input: {path}")
    require(path.resolve().is_relative_to(root.resolve()), f"path escaped root: {path}")
    return path


def owned_json(root, name, maximum=MAX_JSON_BYTES):
    return read_json(owned_file(root, name), maximum)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def revision(value, label):
    require(isinstance(value, str) and HEX40.fullmatch(value) is not None,
            f"{label} must be a full lowercase commit ID")
    return value


def hash_map(value, label):
    require(isinstance(value, dict) and set(value) == BINARIES,
            f"{label} must contain exactly the three CI binaries")
    require(all(isinstance(item, str) and HEX64.fullmatch(item) for item in value.values()),
            f"{label} contains malformed SHA-256")
    return value


def metadata_matches(document, *, name_pattern, label, source_revision, run_id):
    require(isinstance(document, dict), f"{label}: metadata object missing")
    workflow = document.get("workflow_run")
    require(isinstance(workflow, dict), f"{label}: workflow_run missing")
    require(type(workflow.get("id")) is int and workflow["id"] == run_id,
            f"{label}: CI run ID mismatch")
    require(workflow.get("head_sha") == source_revision,
            f"{label}: CI head mismatch")
    require(isinstance(document.get("name"), str)
            and re.fullmatch(name_pattern, document["name"]),
            f"{label}: artifact name mismatch")
    value = document.get("digest")
    require(isinstance(value, str) and value.startswith("sha256:")
            and HEX64.fullmatch(value[7:]), f"{label}: artifact digest missing")
    require(document.get("expired") is False, f"{label}: artifact expired or status missing")


def load_documents(root, checkout, actual_hashes_path, evidence_metadata_path, binary_metadata_path):
    require(checkout.is_dir() and not checkout.is_symlink(), "unsafe trusted checkout")
    return {
        "frozen": owned_json(root, "plan.json"),
        "provenance": owned_json(root, "source-provenance.json"),
        "manifest": owned_json(root, "manifest.json"),
        "manifest_sha256": hash_file(owned_file(root, "manifest.json"), MAX_JSON_BYTES),
        "transactions_sha256": hash_file(owned_file(root, "transactions.json"), MAX_TRANSACTION_BYTES),
        "source_hashes": owned_json(root, "source-hashes.json"),
        "source_profile": owned_json(root, "source/profile-v1.json", maximum=MAX_SOURCE_BYTES),
        "source_profile_bytes": read_bytes(owned_file(root, "source/profile-v1.json"), maximum=MAX_SOURCE_BYTES),
        "trusted_profile": owned_json(checkout, "tools/hub-capacity/profile-v1.json"),
        "trusted_profile_bytes": read_bytes(owned_file(checkout, "tools/hub-capacity/profile-v1.json")),
        "actual_hashes": read_json(actual_hashes_path),
        "evidence_metadata": read_json(evidence_metadata_path),
        "binary_metadata": read_json(binary_metadata_path),
        "phases": {phase: {
            "profile": owned_json(root, f"deployment/{phase}/{phase}-profile.json"),
            "profile_bytes": read_bytes(owned_file(root, f"deployment/{phase}/{phase}-profile.json")),
            "topology": owned_json(root, f"deployment/{phase}/topology.json"),
            "topology_bytes": read_bytes(owned_file(root, f"deployment/{phase}/topology.json")),
            "measurement_topology": owned_json(root, f"{phase}-evidence/topology.json"),
            "measurement_topology_bytes": read_bytes(owned_file(root, f"{phase}-evidence/topology.json")),
            "evidence": owned_json(root, f"{phase}-evidence/evidence.json",
                                   maximum=MAX_EVIDENCE_BYTES),
        } for phase in PHASES},
    }


def verify(documents, source_revision, binary_revision, run_id):
    revision(source_revision, "expected source revision")
    revision(binary_revision, "expected binary revision")
    require(type(run_id) is int and run_id > 0, "CI run ID must be positive")
    actual = hash_map(documents["actual_hashes"], "externally authenticated actual hashes")
    metadata_matches(documents["evidence_metadata"],
                     name_pattern=rf"hub-capacity-evidence-{source_revision}-attempt-[1-9][0-9]*",
                     label="evidence artifact",
                     source_revision=source_revision, run_id=run_id)
    metadata_matches(documents["binary_metadata"],
                     name_pattern=rf"hub-capacity-linux-arm64-{source_revision}",
                     label="binary artifact",
                     source_revision=source_revision, run_id=run_id)

    provenance = documents["provenance"]
    require(isinstance(provenance, dict), "source provenance missing")
    require(provenance.get("binary_revision") == binary_revision,
            "source provenance binary revision mismatch")
    require(provenance.get("qualification_revision") == source_revision,
            "source provenance qualification revision mismatch")
    changes = provenance.get("qualification_only_changes")
    require(isinstance(changes, list) and all(isinstance(name, str) for name in changes),
            "source provenance change list missing or malformed")
    if binary_revision == source_revision:
        require(changes == [], "same-head provenance claims qualification-only changes")
    manifest = documents["manifest"]
    require(isinstance(manifest, dict) and manifest.get("qualification_revision") == source_revision,
            "manifest qualification revision mismatch")
    require(manifest.get("transaction_fixture_sha256") == documents["transactions_sha256"],
            "manifest transaction fixture digest mismatch")

    frozen = documents["frozen"]
    require(isinstance(frozen, dict) and isinstance(frozen.get("plan"), dict),
            "frozen plan missing")
    plan = frozen["plan"]
    plan_hash = digest(plan)
    require(isinstance(frozen.get("plan_sha256"), str)
            and frozen["plan_sha256"] == plan_hash, "frozen plan digest mismatch")
    require(documents["source_profile_bytes"] == documents["trusted_profile_bytes"],
            "retained profile differs from trusted checkout profile")
    shipped = documents["trusted_profile"]
    require(isinstance(shipped, dict), "trusted profile must be an object")
    require(isinstance(plan.get("envelope"), dict), "frozen plan envelope missing")
    command = plan.get("adapter_command")
    require(isinstance(command, str), "frozen adapter command missing")
    try:
        argv = shlex.split(command)
    except ValueError as error:
        raise BindingError("malformed adapter command") from error
    require(argv.count("--manifest-sha256") == 1
            and argv[argv.index("--manifest-sha256") + 1:argv.index("--manifest-sha256") + 2]
            == [documents["manifest_sha256"]],
            "adapter command manifest digest mismatch")
    require(isinstance(documents["source_hashes"], dict), "source hash map missing")

    for phase in PHASES:
        entry = plan.get(phase)
        require(isinstance(entry, dict), f"{phase} plan missing")
        require(entry.get("qualification_revision") == source_revision,
                f"{phase} plan qualification revision mismatch")
        require(entry.get("revision") == binary_revision,
                f"{phase} plan binary revision mismatch")
        require(entry.get("binary_sha256") == actual,
                f"{phase} plan actual binary hashes mismatch")
        require(isinstance(entry.get("profile"), dict), f"{phase} plan profile missing")
        phase_docs = documents["phases"].get(phase)
        require(isinstance(phase_docs, dict), f"{phase} documents missing")
        require(phase_docs.get("profile") == entry["profile"],
                f"{phase} retained deployment profile mismatch")
        topology = phase_docs.get("topology")
        require(isinstance(topology, dict)
                and topology.get("source_provenance") == provenance,
                f"{phase} topology source provenance mismatch")
        require(topology.get("source") == documents["source_hashes"],
                f"{phase} topology source hash map mismatch")
        require(topology.get("transaction_fixture_sha256") == documents["transactions_sha256"],
                f"{phase} topology transaction fixture digest mismatch")
        deployment_hashes = topology.get("deployment_hashes")
        require(isinstance(deployment_hashes, dict)
                and deployment_hashes.get(f"{phase}-profile.json")
                == hashlib.sha256(phase_docs["profile_bytes"]).hexdigest(),
                f"{phase} topology deployment profile digest mismatch")
        require(phase_docs.get("measurement_topology") == topology
                and phase_docs.get("measurement_topology_bytes") == phase_docs.get("topology_bytes"),
                f"{phase} measured topology differs from retained deployment topology")
        evidence = phase_docs.get("evidence")
        require(isinstance(evidence, dict), f"{phase} evidence missing")
        artifacts = evidence.get("artifacts")
        require(isinstance(artifacts, list), f"{phase} evidence artifacts missing")
        topology_records = [item for item in artifacts
                            if isinstance(item, dict) and item.get("path") == "topology.json"]
        require(len(topology_records) == 1 and topology_records[0].get("sha256")
                == hashlib.sha256(phase_docs["measurement_topology_bytes"]).hexdigest(),
                f"{phase} measured topology artifact digest mismatch")
        expected = {
            "phase": phase,
            "revision": binary_revision,
            "plan_sha256": plan_hash,
            "profile_sha256": digest(entry["profile"]),
            "binary_sha256": actual,
            "envelope": plan.get("envelope"),
        }
        for field, value in expected.items():
            require(field in evidence and evidence[field] == value,
                    f"{phase} evidence {field} mismatch or missing")

    candidate = plan["candidate"]["profile"]
    require(all(key in candidate and candidate[key] == value for key, value in shipped.items()),
            "candidate shipped profile settings mismatch")
    allowed_extra = {"bootstrap_peers", "hostname", "dao_observers", "libp2p_config"}
    require(not (set(candidate) - set(shipped) - allowed_extra),
            "candidate profile has unsupported extra settings")
    baseline = plan["baseline"]["profile"]
    identity_fields = {"dao_observers", "libp2p_config", "bootstrap_peers"}
    require(identity_fields <= set(baseline) and identity_fields <= set(candidate),
            "baseline or candidate topology identity settings missing")
    require(baseline.get("dao_observers") == candidate.get("dao_observers")
            and baseline.get("libp2p_config") == candidate.get("libp2p_config")
            and baseline.get("bootstrap_peers") == candidate.get("bootstrap_peers"),
            "baseline and candidate topology identity settings differ")
    return {"binding_passed": True, "source_revision": source_revision,
            "binary_revision": binary_revision, "ci_run_id": run_id,
            "plan_sha256": plan_hash, "binary_sha256": actual,
            "qualification_scored": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-root", required=True, type=Path)
    parser.add_argument("--trusted-checkout", required=True, type=Path)
    parser.add_argument("--expected-source-revision", required=True)
    parser.add_argument("--expected-binary-revision", required=True)
    parser.add_argument("--ci-run-id", required=True, type=int)
    parser.add_argument("--actual-binary-hashes", required=True, type=Path)
    parser.add_argument("--evidence-artifact-metadata", required=True, type=Path)
    parser.add_argument("--binary-artifact-metadata", required=True, type=Path)
    args = parser.parse_args()
    try:
        documents = load_documents(args.evidence_root, args.trusted_checkout,
                                   args.actual_binary_hashes, args.evidence_artifact_metadata,
                                   args.binary_artifact_metadata)
        result = verify(documents, args.expected_source_revision,
                        args.expected_binary_revision, args.ci_run_id)
    except (BindingError, OSError, TypeError, ValueError) as error:
        parser.exit(1, f"binding verification failed: {error}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
