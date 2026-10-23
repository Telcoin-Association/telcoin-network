#!/usr/bin/env python3
"""Record actual registry/path advisory behavior with an independently affected control."""

import argparse
import hashlib
import json
import os
import runpy
from pathlib import Path
import subprocess
import tarfile
import tomllib
import urllib.request


ROOT = Path(__file__).resolve().parents[2]
ADVISORY = "RUSTSEC-2024-0373"
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(argv, cwd, output, name, env):
    """Retain a command's output and exit status, including positive advisory reports."""
    result = subprocess.run(argv, cwd=cwd, env=env, capture_output=True)
    stdout, stderr = output / f"{name}.stdout", output / f"{name}.stderr"
    stdout.write_bytes(result.stdout)
    stderr.write_bytes(result.stderr)
    receipt = {"command": argv, "exit_code": result.returncode,
               "stdout": stdout.name, "stdout_sha256": sha256(stdout),
               "stderr": stderr.name, "stderr_sha256": sha256(stderr)}
    (output / f"{name}.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return result, receipt


def require(argv, cwd, output, name, env):
    result, receipt = run(argv, cwd, output, name, env)
    if result.returncode:
        raise RuntimeError(f"{name} failed; see {output / receipt['stderr']}")
    return result.stdout


def crate(name, version, output):
    """Download a real published crate, preserve its registry checksum, and extract as data."""
    url = f"https://crates.io/api/v1/crates/{name}/{version}/download"
    archive = output / f"{name}-{version}.crate"
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "telcoin-transport-qualification"}), timeout=60) as response:
        archive.write_bytes(response.read())
    directory = output / "sources"
    directory.mkdir(exist_ok=True)
    with tarfile.open(archive) as source:
        source.extractall(directory, filter="data")
    return directory / f"{name}-{version}", {"url": url, "sha256": sha256(archive)}


def fixture(name, source, output, env):
    """Resolve the same vulnerable package identity once from the registry and once by path."""
    directory = output / name
    directory.mkdir()
    dependency = '"=0.11.6"' if source is None else '{ path = ' + json.dumps(str(source)) + ' }'
    manifest = f'''[package]
name = "advisory-control-{name}"
version = "0.0.0"
edition = "2024"
publish = false
[workspace]
[lib]
path = {json.dumps(str(ROOT / "etc/transport-patches/control.rs"))}
[dependencies]
quinn-proto = {dependency}
'''
    (directory / "Cargo.toml").write_text(manifest)
    metadata = json.loads(require(["cargo", "+1.94", "metadata", "--format-version", "1"],
                                 directory, output, f"{name}-metadata", env))
    package = next(package for package in metadata["packages"] if package["name"] == "quinn-proto")
    if package["version"] != "0.11.6" or (package["source"] is None) != (source is not None):
        raise RuntimeError("control did not resolve the intended affected package/source")
    return directory, {"id": package["id"], "name": package["name"],
                       "version": package["version"], "source": package["source"]}


def registry_form(lockfile, identities):
    """Copy a lockfile with the recorded path packages re-sourced to the registry.

    Both pinned scanners skip a package without a registry source, so a path override hides
    every advisory against the carried version itself. The copy keeps every other entry
    verbatim, is audit input only, and never replaces the node lockfile.
    """
    def resource(block):
        entry = tomllib.loads(block)["package"][0] if block.startswith("[[package]]") else {}
        identity = (entry.get("name"), entry.get("version"))
        if entry.get("source") is not None or identity not in identities:
            return block, None
        lines = block.split("\n")
        index = lines.index(f'version = "{entry["version"]}"') + 1
        return "\n".join(lines[:index] + [f'source = "{REGISTRY}"'] + lines[index:]), identity
    blocks, resourced = zip(*(resource(block) for block in lockfile.split("\n\n")))
    rewritten = sorted(identity for identity in resourced if identity is not None)
    if rewritten != sorted(identities):
        raise RuntimeError("registry-form copy did not re-source every recorded path package")
    return "\n\n".join(blocks), [{"name": name, "version": version} for name, version in rewritten]


def audit_copy(args, output, name, lockfile, identities, env):
    """Audit a registry-form copy of `lockfile`; the copy lives under the evidence directory only."""
    directory = output / name
    directory.mkdir()
    text, rewritten = registry_form(lockfile.read_text(), identities)
    copy = directory / "Cargo.lock"
    copy.write_text(text)
    result, receipt = run([args.audit, "audit", "--no-fetch", "--db", str(args.database),
                           "--file", str(copy), "--json"], directory, output, name, env)
    if result.returncode not in (0, 1):
        raise RuntimeError(f"{name}: scanner error cannot count as a registry-form result")
    return result, {**receipt, "lockfile_sha256": sha256(lockfile), "copy_sha256": sha256(copy),
                    "rewritten": rewritten}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--audit", required=True)
    parser.add_argument("--deny", required=True)
    parser.add_argument("--sources", required=True, type=Path,
                        help="completed source evidence from sources.py for the current node lockfile")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env["RUSTC_WRAPPER"] = ""
    env["CARGO_TARGET_DIR"] = str(output / "target")
    summary = {"status": "failed", "controls": {}, "scanners": {}}
    try:
        database_revision = require(["git", "rev-parse", "HEAD"], args.database, output,
                                    "database-revision", env).decode().strip()
        advisory_file = args.database / "crates/quinn-proto" / f"{ADVISORY}.md"
        text = advisory_file.read_text()
        advisory = tomllib.loads(text.split("```toml\n", 1)[1].split("```", 1)[0])
        if advisory["versions"] != {"patched": [">= 0.11.7"], "unaffected": ["< 0.11.0"]}:
            raise RuntimeError("advisory range changed; review the control before proceeding")
        summary.update({"database_revision": database_revision, "advisory": ADVISORY,
                        "advisory_sha256": sha256(advisory_file), "affected_control": "0.11.6",
                        "carried_quinn_proto": "0.11.18", "carried_applicability": "not affected: >= 0.11.7"})
        source, download = crate("quinn-proto", "0.11.6", output)
        if download["sha256"] != "ba92fb39ec7ad06ca2582c0ca834dfeadcaf06ddfc8e635c80aa7e1c05315fdd":
            raise RuntimeError("affected control archive differs from the recorded published source")
        summary["control_source"] = download
        config = output / "deny.toml"
        config.write_text('[advisories]\ndb-path = ' + json.dumps(str(output / "deny-db")) + '\n')
        for tool, binary in (("cargo-audit", args.audit), ("cargo-deny", args.deny)):
            summary["scanners"][tool] = require([binary, "--version"], ROOT, output,
                                               f"{tool}-version", env).decode().strip()
        fixtures = {}
        for name, path in (("registry", None), ("path", source)):
            directory, identity = fixture(name, path, output, env)
            fixtures[name] = directory
            observations = {"resolved": identity, "results": {}}
            audit = [args.audit, "audit", "--no-fetch", "--db", str(args.database),
                     "--file", str(directory / "Cargo.lock"), "--json"]
            deny = [args.deny, "--format", "json", "--manifest-path", str(directory / "Cargo.toml"),
                    "--config", str(config), "--locked", "check", "advisories"]
            for tool, argv in (("cargo-audit", audit), ("cargo-deny", deny)):
                result, receipt = run(argv, directory, output, f"{name}-{tool}", env)
                reported = ADVISORY.encode() in result.stdout + result.stderr
                observations["results"][tool] = {**receipt, "known_advisory_reported": reported}
                if result.returncode not in (0, 1) or (name == "registry" and not reported):
                    raise RuntimeError(f"{name} {tool} failed its affected control")
                if result.returncode != (1 if reported else 0):
                    raise RuntimeError(f"{name} {tool}: scanner error cannot count as a coverage gap")
            summary["controls"][name] = observations
        # The path control shows what both scanners miss. The same lockfile in registry form
        # must report the advisory, or the rewrite proves nothing about the node scan below.
        result, control = audit_copy(args, output, "registry-form-control",
                                     fixtures["path"] / "Cargo.lock", {("quinn-proto", "0.11.6")}, env)
        control["known_advisory_reported"] = ADVISORY.encode() in result.stdout + result.stderr
        summary["controls"]["registry-form"] = control
        if result.returncode != 1 or not control["known_advisory_reported"]:
            raise RuntimeError("registry-form control did not restore path advisory coverage")
        deny_databases = list((output / "deny-db").glob("advisory-db-*"))
        if len(deny_databases) != 1:
            raise RuntimeError("cannot identify cargo-deny's actual advisory database")
        summary["deny_database_revision"] = require(["git", "rev-parse", "HEAD"],
            deny_databases[0], output, "deny-database-revision", env).decode().strip()
        lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
        stack = {package["name"]: package["version"] for package in lock["package"]
                 if package["name"] in {"libp2p-quic", "libp2p-tls", "quinn", "quinn-proto", "rustls"}}
        if stack.get("quinn-proto") != "0.11.18":
            raise RuntimeError("carried quinn-proto changed; reassess applicability")
        summary["carried_stack"] = stack
        result, receipt = run([args.audit, "audit", "--no-fetch", "--db", str(args.database),
                               "--file", str(ROOT / "Cargo.lock"), "--json"], ROOT, output, "carried-audit", env)
        report = json.loads(result.stdout)
        if result.returncode not in (0, 1):
            raise RuntimeError("workspace audit failed before producing a usable advisory result")
        findings = report["vulnerabilities"]["list"]
        source_summary = json.loads((args.sources / "summary.json").read_text())
        if source_summary["status"] != "passed":
            raise RuntimeError("source evidence must pass before advisory qualification")
        closure = runpy.run_path(str(ROOT / "etc/transport-patches/sources.py"))["transport_closure"]
        identities = set()
        paths = set()
        summary["transport_closure_counts"] = {}
        for mode in ("default", "adiri"):
            if source_summary["node"][mode]["lockfile_sha256"] != sha256(ROOT / "Cargo.lock"):
                raise RuntimeError("source evidence does not identify the current node lockfile")
            raw = args.sources / f"node-{mode}-metadata.stdout"
            source_receipt = json.loads((args.sources / f"node-{mode}-metadata.json").read_text())
            if source_receipt["stdout_sha256"] != sha256(raw):
                raise RuntimeError("resolved source evidence changed after capture")
            packages = closure(json.loads(raw.read_text()))
            identities.update((package["name"], package["version"]) for package in packages)
            paths.update((package["name"], package["version"]) for package in packages
                         if package["source"] is None)
            summary["transport_closure_counts"][mode] = len(packages)
        summary["carried_scan"] = {**receipt, "workspace_finding_count": len(findings),
            "transport_findings": [finding for finding in findings
                                   if (finding["package"]["name"], finding["package"]["version"]) in identities]}
        if summary["carried_scan"]["transport_findings"]:
            raise RuntimeError("carried transport has advisory findings requiring review")
        # The scan above cannot see the path packages themselves. Scan the node lockfile again
        # with those packages in registry form, filtered by the same closure identities.
        result, carried = audit_copy(args, output, "registry-form-carried", ROOT / "Cargo.lock", paths, env)
        findings = json.loads(result.stdout)["vulnerabilities"]["list"]
        summary["carried_registry_form_scan"] = {**carried, "workspace_finding_count": len(findings),
            "transport_findings": [finding for finding in findings
                                   if (finding["package"]["name"], finding["package"]["version"]) in identities]}
        if summary["carried_registry_form_scan"]["transport_findings"]:
            raise RuntimeError("carried path packages have advisory findings requiring review")
        summary["status"] = "passed"
    finally:
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps({"status": summary["status"], "evidence": str(output / "summary.json")}))


if __name__ == "__main__":
    main()
