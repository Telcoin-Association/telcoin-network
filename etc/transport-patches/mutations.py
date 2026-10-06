#!/usr/bin/env python3
"""Prove that real transport failures kill the traffic and identity qualification checks."""

import argparse
import asyncio
import json
import os
from pathlib import Path
import runpy
import shutil


ROOT = Path(__file__).resolve().parents[2]


def main():
    """Build only mutated peer binaries while reusing the independently compiled dependencies."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--peers", required=True, type=Path)
    args = parser.parse_args()
    output = args.peers.resolve()
    qualified = json.loads((output / "summary.json").read_text())
    if qualified["status"] != "passed":
        raise RuntimeError("qualification must pass before evaluating mutations")
    recipe = runpy.run_path(str(ROOT / "etc/transport-patches/qualify.py"))
    source = (ROOT / "etc/transport-patches/peer.rs").read_text()
    env = os.environ.copy()
    env.pop("SSLKEYLOGFILE", None)
    env.update({"CARGO_TARGET_DIR": str(output / "target"), "CARGO_BUILD_JOBS": "2", "RUSTC_WRAPPER": ""})
    proof = {"status": "failed", "mutations": {}}
    changes = {
        "traffic": ('inbound.write_all(&frame).await?;', 'inbound.write_all(&[0_u8; 32]).await?;'),
        "identity": ('if peer != expected {', 'if false {'),
    }
    try:
        for name, (original, replacement) in changes.items():
            if source.count(original) != 1:
                raise RuntimeError(f"{name}: mutation site changed; review the mutation")
            directory = output / f"mutation-{name}"
            directory.mkdir()
            peer = directory / "peer.rs"
            peer.write_text(source.replace(original, replacement, 1))
            manifest = (output / "carried/Cargo.toml").read_text()
            manifest = manifest.replace(json.dumps(str(ROOT / "etc/transport-patches/peer.rs")), json.dumps(str(peer)))
            manifest = manifest.replace('name = "peer"', 'name = "peer-mutant"')
            (directory / "Cargo.toml").write_text(manifest)
            shutil.copyfile(output / "carried/Cargo.lock", directory / "Cargo.lock")
            recipe["command"](["cargo", "+1.94", "build", "--locked", "--features", "carried"],
                              directory, output, f"mutation-{name}-build", env)
            mutant = str(output / "target/debug/peer-mutant")
            try:
                if name == "traffic":
                    asyncio.run(recipe["exercise"](mutant, str(output / "peer-stock-current"),
                                                   "/ip4/127.0.0.1/udp/0/quic-v1", env))
                else:
                    asyncio.run(recipe["identity_rejection"](str(output / "peer-carried"), mutant, env))
            except RuntimeError as error:
                expected = "echo differs" if name == "traffic" else "wrong expected identity was not rejected"
                if expected not in str(error):
                    raise RuntimeError(f"{name}: unrelated failure cannot kill the mutation: {error}") from error
                proof["mutations"][name] = {"status": "killed", "failure": str(error),
                    "source_sha256": recipe["digest"](peer), "binary_sha256": recipe["digest"](Path(mutant))}
            else:
                raise RuntimeError(f"{name}: mutation survived its qualification check")
        proof["status"] = "passed"
    finally:
        (output / "mutations.json").write_text(json.dumps(proof, indent=2) + "\n")
        print(json.dumps(proof))


if __name__ == "__main__":
    main()
