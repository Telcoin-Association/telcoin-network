#!/usr/bin/env python3
"""Build independent locked QUIC peers and retain source, crypto, and runtime evidence."""

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import tomllib


ROOT = Path(__file__).resolve().parents[2]
PEERS = {
    "carried": ("0.14.0", "0.44.0", "0.3.0"),
    "stock-current": ("0.14.0", "0.44.0", "0.3.0"),
    "stock-previous": ("0.13.1", "0.43.2", "0.2.13"),
}


def digest(path):
    """Identify retained evidence independently of its location."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def command(arguments, cwd, output, name, env):
    """Retain each finite command and require success before continuing."""
    result = subprocess.run(arguments, cwd=cwd, env=env, capture_output=True)
    if (output / f"{name}.json").exists():
        name = f"{name}-{time.time_ns()}"
    stdout = output / f"{name}.stdout"
    stderr = output / f"{name}.stderr"
    stdout.write_bytes(result.stdout)
    stderr.write_bytes(result.stderr)
    receipt = {"command": arguments, "cwd": str(cwd), "exit_code": result.returncode,
               "stdout": stdout.name, "stdout_sha256": digest(stdout),
               "stderr": stderr.name, "stderr_sha256": digest(stderr)}
    (output / f"{name}.json").write_text(json.dumps(receipt, indent=2) + "\n")
    if result.returncode:
        raise RuntimeError(f"{name} failed ({result.returncode}); see {stderr}")
    return result.stdout


def manifest(name, versions, stack):
    """Pin the node's transport stack and select the actual path source only for the patch."""
    quic, core, identity = versions
    rustls = stack["rustls"]
    text = f'''[package]
name = "transport-peer-{name}"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]

[[bin]]
name = "peer"
path = {json.dumps(str(ROOT / "etc/transport-patches/peer.rs"))}

[features]
carried = []

[profile.dev]
debug = 0
incremental = false

[dependencies]
futures = "0.3"
libp2p-quic = {{ version = "={quic}", features = ["tokio"] }}
libp2p-core = "={core}"
libp2p-identity = {{ version = "={identity}", features = ["ed25519", "rand"] }}
tokio = {{ version = "=1.53.0", features = ["macros", "rt-multi-thread", "time"] }}
quinn = {{ version = "=0.11.9", default-features = false }}
quinn-proto = {{ version = "=0.11.18", default-features = false }}
rustls = {{ version = "={rustls}", default-features = false, features = ["aws_lc_rs", "logging", "prefer-post-quantum", "ring", "std", "tls12"] }}
aws-lc-rs = {{ version = "={stack['aws-lc-rs']}", default-features = false, features = ["aws-lc-sys", "prebuilt-nasm"] }}
aws-lc-sys = {{ version = "={stack['aws-lc-sys']}", default-features = false, features = ["prebuilt-nasm"] }}
rustls-webpki = {{ version = "={stack['rustls-webpki']}", features = ["aws-lc-rs", "ring"] }}
'''
    if name == "carried":
        text += '\n[patch.crates-io]\nlibp2p-quic = { path = ' + json.dumps(str(ROOT / "patches/libp2p-quic")) + ' }\n'
    return text


def resolution(metadata):
    """Record actual package identities and resolved crypto features, including path sources."""
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    names = {"libp2p-quic", "libp2p-core", "libp2p-identity", "libp2p-tls", "quinn",
             "quinn-proto", "rustls", "aws-lc-rs", "ring"}
    return [{"name": package["name"], "version": package["version"],
             "id": package["id"], "source": package["source"],
             "features": nodes[package["id"]]["features"]}
            for package in metadata["packages"] if package["name"] in names]


async def listener(binary, address, env):
    """Wait for an event from a real listener instead of assuming a startup delay."""
    process = await asyncio.create_subprocess_exec(binary, "listen", address, env=env,
                                                   stdout=asyncio.subprocess.PIPE,
                                                   stderr=asyncio.subprocess.PIPE)
    try:
        line = (await asyncio.wait_for(process.stdout.readline(), 20)).decode().strip()
        if not line.startswith("READY "):
            raise RuntimeError(f"listener did not become ready: {line}")
        address, peer = line.removeprefix("READY ").rsplit("/p2p/", 1)
        return process, address, peer
    except BaseException:
        await stop(process)
        raise


async def stop(process):
    """Always reap the peer on failure or intentional rejection."""
    if process.returncode is None:
        process.kill()
    await process.communicate()


async def exercise(server, client, address, env):
    """Validate identities and traffic twice against the same listener, covering reconnect."""
    process, bound, peer = await listener(server, address, env)
    exchanges = []
    try:
        for exchange in range(2):
            dialer = await asyncio.create_subprocess_exec(client, "dial", bound, peer, env=env,
                                                          stdout=asyncio.subprocess.PIPE,
                                                          stderr=asyncio.subprocess.PIPE)
            try:
                stdout, stderr = await asyncio.wait_for(dialer.communicate(), 45)
            finally:
                if dialer.returncode is None:
                    await stop(dialer)
            if dialer.returncode or stdout.decode().strip() != f"VERIFIED {peer}":
                raise RuntimeError(f"{Path(server).name} <- {Path(client).name}, {address}, exchange {exchange + 1}: {stderr.decode()}")
            exchanges.append({"exit_code": dialer.returncode, "authenticated": True,
                              "echo_bytes": 32})
        stdout, stderr = await asyncio.wait_for(process.communicate(), 45)
        if process.returncode or stdout.decode().count("ECHO ") != 2:
            raise RuntimeError(f"listener failed: {stderr.decode()}")
        return {"listener": server, "dialer": client, "address": address,
                "listener_exit_code": process.returncode, "exchanges": exchanges,
                "reconnect": "passed"}
    finally:
        if process.returncode is None:
            await stop(process)


async def identity_rejection(server, client, env):
    """A separately obtained wrong expected identity must fail before application traffic."""
    processes = []
    try:
        first, bound, _ = await listener(server, "/ip4/127.0.0.1/udp/0/quic-v1", env)
        processes.append(first)
        second, _, wrong = await listener(server, "/ip4/127.0.0.1/udp/0/quic-v1", env)
        processes.append(second)
        dialer = await asyncio.create_subprocess_exec(client, "dial", bound, wrong, env=env,
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stderr=asyncio.subprocess.PIPE)
        processes.append(dialer)
        _, stderr = await asyncio.wait_for(dialer.communicate(), 45)
        if dialer.returncode == 0 or b"authenticated peer identity differs" not in stderr:
            raise RuntimeError("wrong expected identity was not rejected")
        return {"dialer_exit_code": dialer.returncode, "wrong_expected_identity": "rejected",
                "boundary": "authenticated transport output before application I/O"}
    finally:
        for process in processes:
            if process.returncode is None:
                await stop(process)


async def runtime(binaries, env, output):
    """Exercise every required release pair in both directions and both address families."""
    results = []
    for stock in ("stock-current", "stock-previous"):
        for server, client in (("carried", stock), (stock, "carried")):
            for address in ("/ip4/127.0.0.1/udp/0/quic-v1", "/ip6/::1/udp/0/quic-v1"):
                print(json.dumps({"listener": server, "dialer": client, "address": address}), flush=True)
                result = await exercise(binaries[server], binaries[client], address, env)
                result.update({"listener_source": server, "dialer_source": client})
                results.append(result)
    keylog = output / "temporary-keylog-control"
    keylog.unlink(missing_ok=True)
    logged_env = {**env, "SSLKEYLOGFILE": str(keylog)}
    try:
        await exercise(binaries["carried"], binaries["stock-current"], "/ip4/127.0.0.1/udp/0/quic-v1", logged_env)
        labels = sorted({line.split()[0] for line in keylog.read_text().splitlines()
                         if line and not line.startswith("#")})
        if "CLIENT_HANDSHAKE_TRAFFIC_SECRET" not in labels:
            raise RuntimeError("key logging positive control did not record a handshake")
    finally:
        keylog.unlink(missing_ok=True)
    return {"matrix": results, "identity_rejection": await identity_rejection(
        binaries["carried"], binaries["stock-current"], env),
        "key_logging_control": {"setting_SSLKEYLOGFILE": "writes secrets", "labels": labels,
                                "secret_file_removed": not keylog.exists()}}


def main():
    """Keep failed and successful evidence together in a new, explicitly selected directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--toolchain", default="1.94")
    parser.add_argument("--resume-build", action="store_true")
    args = parser.parse_args()
    output = args.output.resolve()
    if args.resume_build:
        if not output.is_dir():
            parser.error("resuming needs an existing build evidence directory")
    else:
        output.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env.pop("SSLKEYLOGFILE", None)
    env["CARGO_TARGET_DIR"] = str(output / "target")
    env["CARGO_BUILD_JOBS"] = "2"
    env["RUSTC_WRAPPER"] = ""
    binaries = {}
    stack = {package["name"]: package["version"]
             for package in tomllib.loads((ROOT / "Cargo.lock").read_text())["package"]
             if package["name"] in {"rustls", "aws-lc-rs", "aws-lc-sys", "rustls-webpki"}}
    summary = {"status": "failed", "toolchain": args.toolchain,
               "platform": platform.platform(), "peer_source_sha256": digest(ROOT / "etc/transport-patches/peer.rs"),
               "production_key_logging": "SSLKEYLOGFILE unset", "peers": {}}
    try:
        for name, versions in PEERS.items():
            directory = output / name
            binary = output / f"peer-{name}"
            binaries[name] = str(binary)
            directory.mkdir(exist_ok=args.resume_build)
            (directory / "Cargo.toml").write_text(manifest(name, versions, stack))
            cargo = ["cargo", f"+{args.toolchain}"]
            command(cargo + ["generate-lockfile"], directory, output, f"{name}-lock", env)
            raw = command(cargo + ["metadata", "--locked", "--format-version", "1"],
                          directory, output, f"{name}-metadata", env)
            build = cargo + ["build", "--locked"]
            if name == "carried":
                build += ["--features", "carried"]
                raw = command(cargo + ["metadata", "--locked", "--format-version", "1",
                                        "--features", "carried"], directory, output,
                              f"{name}-metadata", env)
            command(build, directory, output, f"{name}-build", env)
            binary.write_bytes((output / "target/debug/peer").read_bytes())
            binary.chmod(0o755)
            provider = command([str(binary), "crypto", "/ip4/127.0.0.1/udp/0/quic-v1"],
                               directory, output, f"{name}-crypto", env).decode().strip()
            summary["peers"][name] = {"binary_sha256": digest(binary),
                "lockfile_sha256": digest(directory / "Cargo.lock"),
                "resolved": resolution(json.loads(raw)), "default_provider": provider}
            baseline = ROOT / "docs/transport-patches/libp2p-quic/evidence/crypto-before.txt"
            if provider != baseline.read_text().strip():
                raise RuntimeError(f"{name}: provider groups or cipher suite order changed; review baseline")
        summary.update(asyncio.run(runtime(binaries, env, output)))
        summary["status"] = "passed"
    except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as error:
        summary["error"] = str(error)
        raise
    finally:
        document = json.dumps(summary, indent=2) + "\n"
        (output / f"summary-{time.time_ns()}.json").write_text(document)
        (output / "summary.json").write_text(document)
        print(json.dumps({"status": summary["status"], "evidence": str(output / "summary.json")}))


if __name__ == "__main__":
    main()
