#!/usr/bin/env python3
"""Exercise independently built QUIC processes on unprivileged loopback sockets."""

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import time
import tomllib

ROOT = Path(__file__).resolve().parents[2]
TRANSPORT_PACKAGES = ("libp2p", "libp2p-quic", "libp2p-tls", "quinn", "quinn-proto", "rustls")


def require(condition, message):
    """Keep acceptance checks active even when Python assertions are disabled."""
    if not condition:
        raise RuntimeError(message)


def versions(path):
    """Select the release evidence from a Cargo lockfile."""
    packages = tomllib.loads(path.read_text())["package"]
    return {name: sorted(p["version"] for p in packages if p["name"] == name)
            for name in TRANSPORT_PACKAGES}


def check_locks():
    """Fail if the current fixture drifts from the node's resolved transport stack."""
    releases = {
        release: versions(ROOT / "testing/quic-interop/releases" / release / "Cargo.lock")
        for release in ("0.13.1", "0.14.0")
    }
    for release, resolved in releases.items():
        require(resolved["libp2p-quic"] == [release], f"wrong QUIC resolution: {resolved}")
    require(releases["0.14.0"] == versions(ROOT / "Cargo.lock"),
            "current fixture transport versions differ from the node; refresh its lockfile")
    return releases


def environment():
    """Record observed runner facts without changing host networking."""
    status = Path("/proc/self/status")
    capabilities = ([line for line in status.read_text().splitlines() if line.startswith("Cap")]
                    if status.exists() else [])
    probe = {"available": False}
    if shutil.which("unshare"):
        result = subprocess.run(["unshare", "--net", "true"], capture_output=True,
                                text=True, timeout=5, check=False)
        probe = {"available": True, "exit_code": result.returncode, "stderr": result.stderr}
    return {
        "platform": platform.platform(), "python": platform.python_version(),
        "uid": os.geteuid(), "interfaces": socket.if_nameindex(),
        "capabilities": capabilities, "network_namespace_probe": probe,
        "tools": {name: shutil.which(name) for name in ("ip", "tc", "ethtool")},
        "runner": os.environ.get("RUNNER_NAME"), "run_id": os.environ.get("GITHUB_RUN_ID"),
        "candidate": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "dirty": bool(subprocess.check_output(["git", "status", "--porcelain",
                                              "--untracked-files=no"], cwd=ROOT)),
        "network": "loopback", "representative_nic": False,
    }


class Fixture:
    """A process with a continuously drained JSON event stream and retained stderr."""

    def __init__(self, binary, role, settings, output, address=None):
        self.command = [str(binary), role, str(settings)] + ([address] if address else [])
        self.output = output
        self.events = []
        self.queue = asyncio.Queue()

    async def __aenter__(self):
        self.stderr = self.output.with_suffix(".stderr").open("wb")
        self.process = await asyncio.create_subprocess_exec(
            *self.command, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=self.stderr)
        self.reader = asyncio.create_task(self.read_events())
        return self

    async def read_events(self):
        with self.output.with_suffix(".jsonl").open("wb") as log:
            async for line in self.process.stdout:
                log.write(line)
                log.flush()
                event = json.loads(line)
                self.events.append(event)
                await self.queue.put(event)
        await self.queue.put(None)

    async def event(self, name, timeout=15):
        async def receive():
            while True:
                event = await self.queue.get()
                require(event is not None, f"{self.command[1]} exited before {name}")
                require(event["event"] != "failed" or name == "failed",
                        f"unexpected fixture failure: {event}")
                if event["event"] == name:
                    return event
        return await asyncio.wait_for(receive(), timeout)

    async def finish(self, timeout, success=True):
        code = await asyncio.wait_for(self.process.wait(), timeout)
        await self.reader
        require((code == 0) == success, f"unexpected process exit {code}: {self.command}")
        return self.events

    async def __aexit__(self, *_exc):
        if self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), 5)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()
        await self.reader
        self.stderr.close()


def select(events, name):
    """Select typed observations without discarding the raw log."""
    return [event for event in events if event["event"] == name]


def configuration(event, release):
    """Verify the role's release and that every configured field reached the transport."""
    require(event["release"] == release, f"wrong binary: {event}")
    require(event["settings"] == event["applied"], "production configuration mapping drifted")
    return event


async def honest(args, settings):
    """Verify identity, fresh connections, payloads and clean reconnects in both roles."""
    async with Fixture(args.listener, "listen", settings, args.output / "honest-listener") as listener:
        server = configuration(await listener.event("configuration"), args.listener_release)
        address = (await listener.event("listening"))["address"]
        async with Fixture(args.dialer, "dial", settings, args.output / "honest-dialer", address) as dialer:
            client = configuration(await dialer.event("configuration"), args.dialer_release)
            await dialer.event("complete", 90)
            await listener.event("closed")
            await listener.event("closed")
            await listener.event("closed")
            dialer.process.stdin.write(b"!")
            await dialer.process.stdin.drain()
            await dialer.finish(5)
            connected = select(dialer.events, "connected")
            accepted = select(listener.events, "accepted")
            require([event["round"] for event in connected] == [0, 1, 2], "missing fresh connections")
            require(len(accepted) == 3, "listener did not establish three connections")
            require(all(event["peer"] == server["peer"] for event in connected), "wrong listener identity")
            require(all(event["peer"] == client["peer"] for event in accepted), "dialer identity changed")
            verified = select(dialer.events, "verified")
            require([(e["round"], e["sample"]) for e in verified] ==
                    [(round_, sample) for round_ in range(3) for sample in range(3)],
                    "missing or duplicated verified stream")
            require(len(select(listener.events, "echo")) == 9, "missing listener echoes")
            require(all(e["bytes"] == 32768 for e in verified), "wrong traffic size")
            require(len(select(dialer.events, "complete")) == 1, "dialer did not finish")
            require(not select(listener.events, "failed"), "listener failed during reconnects")
            return {"listener": server, "dialer": client, "connections": connected, "streams": verified}


class Blackhole(asyncio.DatagramProtocol):
    """Forward client datagrams but drop server replies on a bound loopback UDP socket."""

    def __init__(self, server):
        self.server = server
        self.forwarded = 0
        self.dropped = 0

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, address):
        if address == self.server:
            self.dropped += 1
        else:
            self.forwarded += 1
            self.transport.sendto(data, self.server)


def timed_out(event):
    """Require a QUIC timeout, rather than treating arbitrary handshake failure as a pass."""
    require(event["error"] in {"HandshakeTimedOut", "Quic(HandshakeTimedOut)",
                               "Connection(ConnectionError(TimedOut))",
                               "Quic(Connection(ConnectionError(TimedOut)))"},
            f"expected resolved QUIC timeout, got {event}")
    return event


async def deadlines(args, settings):
    """Record actual stalled-handshake deadlines without asserting wall-clock timings."""
    async with Fixture(args.listener, "listen", settings, args.output / "deadline-listener") as listener:
        config = configuration(await listener.event("configuration"), args.listener_release)
        address = (await listener.event("listening"))["address"]
        parts = address.split("/")
        server = (parts[2], int(parts[4]))
        relay, protocol = await asyncio.get_running_loop().create_datagram_endpoint(
            lambda: Blackhole(server), local_addr=("127.0.0.1", 0))
        parts[4] = str(relay.get_extra_info("sockname")[1])
        stalled_address = "/".join(parts)
        # This is a harness watchdog, not an assertion about network scheduling.
        watchdog = config["settings"]["handshake_timeout"]["secs"] + 20
        try:
            async with Fixture(args.dialer, "dial", settings, args.output / "deadline-dialer",
                               stalled_address) as dialer:
                configuration(await dialer.event("configuration"), args.dialer_release)
                await dialer.finish(watchdog, success=False)
                failures = select(dialer.events, "failed")
                require(len(failures) == 1, "missing outbound timeout observation")
                outbound = timed_out(failures[0])
                inbound = {"event": "not_observed", "reason": "no Incoming transport event"}
                if select(listener.events, "incoming"):
                    inbound = timed_out(await listener.event("failed", watchdog))
                require(not select(listener.events, "accepted"), "blackholed handshake was accepted")
                require(protocol.forwarded > 0 and protocol.dropped > 0, "loss relay saw no traffic")
                return {"outbound": outbound, "inbound": inbound,
                        "forwarded": protocol.forwarded, "dropped": protocol.dropped}
        finally:
            relay.close()


async def run(args):
    """Retain metadata and partial results even when an acceptance check fails."""
    args.output.mkdir(parents=True, exist_ok=True)
    settings = args.output / "settings.json"
    settings.write_text("{}\n")
    result = {"status": "running"}
    started = time.monotonic()
    try:
        result["environment"] = environment()
        result["releases"] = check_locks()
        result["binaries"] = {role: hashlib.sha256(path.read_bytes()).hexdigest()
                              for role, path in (("listener", args.listener), ("dialer", args.dialer))}
        result["honest"] = await honest(args, settings)
        result["deadlines"] = await deadlines(args, settings)
        result["status"] = "passed"
    except Exception as error:
        result.update(status="failed", error=repr(error))
        raise
    finally:
        result["elapsed_seconds"] = time.monotonic() - started
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")


def main():
    """Parse explicit binaries and release labels for one matrix direction."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listener", type=Path, required=True)
    parser.add_argument("--dialer", type=Path, required=True)
    parser.add_argument("--listener-release", choices=("0.13.1", "0.14.0"), required=True)
    parser.add_argument("--dialer-release", choices=("0.13.1", "0.14.0"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
