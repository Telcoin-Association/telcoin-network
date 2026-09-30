#!/usr/bin/env python3
"""Capture node release feature trees and prove the vendored source's published provenance."""

import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import tarfile
import tomllib


NAMES = {"libp2p-quic", "libp2p-tls", "libp2p-core", "libp2p-identity", "quinn",
         "quinn-proto", "rustls", "rustls-webpki", "aws-lc-rs", "aws-lc-sys", "ring"}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(argv, cwd, output, name):
    env = os.environ.copy()
    env["RUSTC_WRAPPER"] = ""
    result = subprocess.run(argv, cwd=cwd, env=env, capture_output=True)
    stdout, stderr = output / f"{name}.stdout", output / f"{name}.stderr"
    stdout.write_bytes(result.stdout)
    stderr.write_bytes(result.stderr)
    receipt = {"command": argv, "cwd": str(cwd), "exit_code": result.returncode,
               "stdout_sha256": sha256(stdout), "stderr_sha256": sha256(stderr)}
    (output / f"{name}.json").write_text(json.dumps(receipt, indent=2) + "\n")
    if result.returncode:
        raise RuntimeError(f"{name} failed; see {stderr}")
    return result.stdout


def node(root, output, label):
    """Capture default and Adiri release graphs for the production Linux target without building."""
    result = {}
    for mode, features in (("default", []), ("adiri", ["--features", "telcoin-network/adiri"])):
        raw = run(["cargo", "+1.94", "metadata", "--locked", "--format-version", "1",
                   "--filter-platform", "x86_64-unknown-linux-gnu"] + features,
                  root, output, f"{label}-{mode}-metadata")
        metadata = json.loads(raw)
        active = next(package for package in metadata["packages"] if package["name"] == "libp2p-quic")
        if active["version"] != "0.14.0":
            raise RuntimeError("QUIC version changed; extend the source record and peer recipe")
        if label == "node":
            if active["source"] is not None or Path(active["manifest_path"]).resolve() != root / "patches/libp2p-quic/Cargo.toml":
                raise RuntimeError("Cargo does not select the recorded vendored QUIC source")
        elif not (active["source"] or "").startswith("registry+"):
            raise RuntimeError("comparison does not select the registry QUIC source")
        nodes = {item["id"]: item for item in metadata["resolve"]["nodes"]}
        packages = [{"name": package["name"], "version": package["version"], "source": package["source"],
                     "id": package["id"].replace(str(root), "$NODE"),
                     "dependencies": sorted(dependency["pkg"].replace(str(root), "$NODE")
                                            for dependency in nodes[package["id"]]["deps"])}
                    for package in metadata["packages"] if package["name"] in NAMES]
        if any(package["name"] != "libp2p-quic"
               and not (package["source"] or "").startswith("registry+") for package in packages):
            raise RuntimeError("another transport/crypto source is overridden; extend its record and peer recipe")
        tree = run(["cargo", "+1.94", "tree", "--locked", "-p", "telcoin-network",
                    "--target", "x86_64-unknown-linux-gnu", "--edges", "normal,build",
                    "--prefix", "none", "--format", "{p}|{f}"] + features,
                   root, output, f"{label}-{mode}-features").decode()
        selected = sorted(set(line.replace(str(root), "$NODE") for line in tree.splitlines()
                              if line.split(" ", 1)[0] in NAMES and " (*)" not in line))
        result[mode] = {"packages": packages, "release_feature_tree": selected,
                        "lockfile_sha256": sha256(root / "Cargo.lock")}
    return result


def provenance(root, output):
    """Reproduce the committed diff from the checksum-identified registry archive."""
    archive = next((Path.home() / ".cargo/registry/cache").glob("*/libp2p-quic-0.14.0.crate"))
    checksum = sha256(archive)
    if checksum != "4f78ca359466657b380e469fe8c04df2f4447d1430838e6c3cef4a1c51ccb2ee":
        raise RuntimeError("upstream registry checksum differs from the recorded base")
    extracted = output / "registry"
    with tarfile.open(archive) as source:
        source.extractall(extracted, filter="data")
    original = extracted / "libp2p-quic-0.14.0"
    revision = json.loads((original / ".cargo_vcs_info.json").read_text())["git"]["sha1"]
    upstream = output / "upstream"
    patched = output / "patched"
    shutil.copytree(original, upstream)
    shutil.copytree(root / "patches/libp2p-quic", patched)
    (upstream / "Cargo.toml.orig").replace(upstream / "Cargo.toml")
    for name in ("Cargo.lock", ".cargo-ok", ".cargo_vcs_info.json"):
        (upstream / name).unlink(missing_ok=True)
    for name in ("PATCH.md", "upstream.diff"):
        (patched / name).unlink()
    diff = subprocess.run(["git", "diff", "--no-index", "--no-color", "--no-ext-diff",
                           "--no-prefix", "upstream", "patched"], cwd=output, capture_output=True)
    if diff.returncode != 1 or diff.stdout != (root / "patches/libp2p-quic/upstream.diff").read_bytes():
        (output / "observed.diff").write_bytes(diff.stdout)
        raise RuntimeError("committed upstream.diff does not reproduce from the registry archive")
    files = {str(path.relative_to(patched)): sha256(path) for path in sorted(patched.rglob("*")) if path.is_file()}
    return {"registry_archive_sha256": checksum, "upstream_base_revision": revision,
            "upstream_diff_sha256": sha256(root / "patches/libp2p-quic/upstream.diff"),
            "vendored_files": files,
            "vendored_tree_sha256": hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--compare", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    result = {"status": "failed"}
    try:
        result["node"] = node(args.root.resolve(), output, "node")
        if args.compare:
            result["comparison"] = node(args.compare.resolve(), output, "comparison")
            for mode in ("default", "adiri"):
                carried = result["node"][mode]
                stock = result["comparison"][mode]
                def normalized_packages(packages):
                    return [{**package, "source": "source-control", "id": "libp2p-quic-source-control"}
                            if package["name"] == "libp2p-quic" else package for package in packages]
                def normalized_features(lines):
                    return [re.sub(r" \(\$NODE/patches/libp2p-quic\)", "", line) for line in lines]
                if (normalized_packages(carried["packages"]) != normalized_packages(stock["packages"])
                        or normalized_features(carried["release_feature_tree"]) != normalized_features(stock["release_feature_tree"])):
                    raise RuntimeError(f"{mode} graph differs beyond the intended QUIC source override")
            result["source_override_comparison"] = "only libp2p-quic source identity differs"
        result["provenance"] = provenance(args.root.resolve(), output)
        result["status"] = "passed"
    finally:
        (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({"status": result["status"], "evidence": str(output / "summary.json")}))


if __name__ == "__main__":
    main()
