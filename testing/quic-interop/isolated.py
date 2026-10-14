#!/usr/bin/env python3
"""Address-validation regression in disconnected Linux network namespaces."""

import argparse
import asyncio
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import sys
import uuid


def command(argv, **kwargs):
    """Run a bounded command without a shell or an inherited network route."""
    return subprocess.run(argv, check=True, timeout=30, **kwargs)


def send(args):
    """Send one genuine Initial with a controlled source inside the named namespace."""
    identified = command(["ip", "netns", "identify"], capture_output=True, text=True).stdout.strip()
    if identified != args.namespace:
        raise RuntimeError("raw packet generation is restricted to the created namespace")
    source = ipaddress.IPv4Address("192.0.2.99").packed
    destination = ipaddress.IPv4Address("192.0.2.1").packed
    payload = args.packet.read_bytes()
    if not 1200 <= len(payload) <= 65535 - 28:
        raise RuntimeError("invalid QUIC Initial datagram length")
    udp = struct.pack("!HHHH", 41000, args.port, len(payload) + 8, 0) + payload
    header = struct.pack("!BBHHHBBH4s4s", 0x45, 0, len(udp) + 20, 1, 0, 64, 17, 0,
                         source, destination)
    total = sum(struct.unpack("!10H", header))
    total = (total & 0xffff) + (total >> 16)
    total = (total & 0xffff) + (total >> 16)
    checksum = (~total) & 0xffff
    header = header[:10] + struct.pack("!H", checksum) + header[12:]
    with socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_RAW) as raw:
        raw.sendto(header + udp, ("192.0.2.1", args.port))


def fixtures():
    """Load the same event collector used by the ordinary compatibility matrix."""
    spec = importlib.util.spec_from_file_location("quic_fixtures", Path(__file__).with_name("run.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def exercise(args, namespaces):
    """Compare stock acceptance with candidate Retry, then verify honest reconnects."""
    fixture = fixtures()
    settings = args.output / "settings.json"
    settings.write_text("{}\n")
    initial = args.output / "initial.bin"
    command(["ip", "netns", "exec", namespaces[1], str(args.initial), str(initial)])
    result = {}
    for release, binary in (("0.14.0", args.stock), ("candidate", args.candidate)):
        listener = fixture.Fixture(binary, "listen", settings, args.output / f"{release}-listener")
        listener.command = ["ip", "netns", "exec", namespaces[0], "env",
                            "TN_QUIC_LISTEN_IP=192.0.2.1", *listener.command]
        async with listener:
            configuration = fixture.configuration(await listener.event("configuration"), release)
            address = (await listener.event("listening"))["address"]
            port = int(address.split("/udp/", 1)[1].split("/", 1)[0])
            command(["ip", "netns", "exec", namespaces[1], sys.executable, "-I",
                     str(Path(__file__).resolve()), "send", "--namespace", namespaces[1],
                     "--packet", str(initial), "--port", str(port)])
            if release == "0.14.0":
                await listener.event("incoming")
                result[release] = {"unvalidated_incoming_event": True,
                                   "configuration": configuration}
            else:
                challenged = await listener.event("outcomes")
                fixture.require(challenged["retried"] >= 1 and challenged["accepted"] == 0,
                                "unvalidated source reached candidate acceptance")
                fixture.require(not fixture.select(listener.events, "incoming"),
                                "candidate returned an Incoming event before address validation")
                dialer = fixture.Fixture(args.candidate, "dial", settings,
                                         args.output / "honest-dialer", address)
                dialer.command = ["ip", "netns", "exec", namespaces[1], *dialer.command]
                async with dialer:
                    await dialer.event("configuration")
                    await dialer.event("complete", 90)
                    for _connection in range(3):
                        await listener.event("closed")
                    dialer.process.stdin.write(b"!")
                    await dialer.process.stdin.drain()
                    await dialer.finish(5)
                    fixture.require(len(fixture.select(dialer.events, "verified")) == 9,
                                    "honest reconnect traffic failed after the forged attempt")
                outcomes = fixture.select(listener.events, "outcomes")
                fixture.require(outcomes[-1]["accepted"] == 3, "unexpected accepted work")
                result[release] = {"unvalidated": challenged, "honest_connections": 3,
                                   "verified_streams": 9, "configuration": configuration}
    return result


def run(args):
    """Create only a disconnected veth pair and always remove the owned namespaces."""
    if sys.platform != "linux" or os.geteuid() != 0:
        raise RuntimeError("isolated lane requires Linux and namespace/raw-socket privileges")
    for tool in ("ip", "tcpdump"):
        if shutil.which(tool) is None:
            raise RuntimeError(f"missing isolated-lane tool: {tool}")
    args.output.mkdir(parents=True, exist_ok=False)
    suffix = uuid.uuid4().hex[:8]
    names = [f"tn1432-l-{suffix}", f"tn1432-c-{suffix}"]
    links = [f"ql{suffix}", f"qc{suffix}"]
    owned = []
    capture = None
    capture_log = (args.output / "tcpdump.stderr").open("wb")
    result = {"status": "running", "namespaces": names, "network": "disconnected-veth",
              "forged_source": "192.0.2.99", "host_firewall_modified": False}
    try:
        for name in names:
            command(["ip", "netns", "add", name])
            owned.append(name)
        command(["ip", "link", "add", links[0], "type", "veth", "peer", "name", links[1]])
        for index, (name, link) in enumerate(zip(names, links, strict=True), start=1):
            command(["ip", "link", "set", link, "netns", name])
            prefix = ["ip", "netns", "exec", name, "ip"]
            command([*prefix, "link", "set", "lo", "up"])
            command([*prefix, "addr", "add", f"192.0.2.{index}/24", "dev", link])
            command([*prefix, "link", "set", link, "up"])
            routes = command([*prefix, "-json", "route"], capture_output=True, text=True).stdout
            if any(route.get("dst") == "default" for route in json.loads(routes)):
                raise RuntimeError("isolated namespace unexpectedly has a default route")
            (args.output / f"routes-{index}.json").write_text(routes)
        capture = subprocess.Popen(["ip", "netns", "exec", names[0], "tcpdump", "-U", "-n",
                                    "-i", links[0], "-w", str(args.output / "traffic.pcap")],
                                   stdout=subprocess.DEVNULL, stderr=capture_log)
        result["cases"] = asyncio.run(exercise(args, names))
        capture.terminate()
        capture.wait(timeout=5)
        capture = None
        recorded = command(["tcpdump", "-n", "-r", str(args.output / "traffic.pcap"),
                            "src host 192.0.2.99 and udp src port 41000"],
                           capture_output=True, text=True).stdout.splitlines()
        if len(recorded) != 2:
            raise RuntimeError("packet capture did not retain both controlled forged Initials")
        result["captured_forged_initials"] = len(recorded)
        result["status"] = "passed"
    except BaseException as error:
        result.update(status="failed", error=repr(error))
        raise
    finally:
        if capture is not None:
            capture.terminate()
            try:
                capture.wait(timeout=5)
            except subprocess.TimeoutExpired:
                capture.kill()
                capture.wait(timeout=5)
        capture_log.close()
        for name in reversed(owned):
            subprocess.run(["ip", "netns", "delete", name], check=False, timeout=10,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        for artifact in args.output.iterdir():
            if artifact.is_file():
                artifact.chmod(0o644)


def main():
    """Parse either the qualification lane or its namespace-confined raw sender."""
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    lane = modes.add_parser("run")
    for flag in ("candidate", "stock", "initial", "output"):
        lane.add_argument(f"--{flag}", type=Path, required=True)
    sender = modes.add_parser("send")
    sender.add_argument("--namespace", required=True)
    sender.add_argument("--packet", type=Path, required=True)
    sender.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    if args.mode == "send":
        send(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
