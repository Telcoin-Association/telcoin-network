#!/usr/bin/env python3
"""Traffic swarms for the inbound QUIC capacity qualification (issue #1741).

Purpose: emit the two forged-source swarms the qualification runs need, paced
at a requested rate that stays below link saturation, and write an
offered-traffic summary for the evidence record. Received figures come from
the validator side (`measure.py` and `collect.py`), never from this tool.

Requirements: Linux, root (or CAP_NET_RAW) for the raw IPv4 socket, an
approved isolated test range, and Python 3.10 or newer. IPv4 only: forged
IPv6 sources need a link-layer socket, which this tool does not open.

This tool does NOT measure anything on the validator, does NOT drive honest
traffic, does NOT open a socket under `--dry-run`, and refuses any rate whose
wire cost exceeds the declared fraction of the link.
"""

import argparse
import errno
import hashlib
import ipaddress
import json
import os
import platform
import random
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 1
IPV4_UDP_OVERHEAD_BYTES = 28
GENERATOR_LIMIT_ERRNOS = ("EPERM", "ENOBUFS", "EAGAIN")
MINIMUM_INITIAL_DATAGRAM_BYTES = 1200
DRY_RUN_PACKETS = 8
DEFAULT_SOURCE_PORTS = "1024-65535"


def utc_now():
    """ISO-8601 UTC timestamp with second resolution."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ipv4_checksum(header):
    """RFC 1071 ones-complement sum over 16-bit words; an odd tail is zero padded."""
    padded = header + (b"\x00" if len(header) % 2 else b"")
    total = sum(int.from_bytes(padded[i:i + 2], "big") for i in range(0, len(padded), 2))
    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)
    return (~total) & 0xFFFF


def build_ipv4_udp(src, sport, dst, dport, payload, ident):
    """One IPv4/UDP datagram: IHL 5, DF set, TTL 64, UDP checksum 0 (IPv4 permits it)."""
    udp_length = 8 + len(payload)
    total_length = 20 + udp_length
    if total_length > 0xFFFF:
        raise ValueError(f"datagram of {total_length} bytes exceeds the IPv4 total length")
    header = bytearray(20)
    header[0] = 0x45
    header[2:4] = total_length.to_bytes(2, "big")
    header[4:6] = (ident & 0xFFFF).to_bytes(2, "big")
    header[6:8] = (0x4000).to_bytes(2, "big")
    header[8] = 64
    header[9] = 17
    header[12:16] = ipaddress.IPv4Address(src).packed
    header[16:20] = ipaddress.IPv4Address(dst).packed
    header[10:12] = ipv4_checksum(bytes(header)).to_bytes(2, "big")
    udp = (sport.to_bytes(2, "big") + dport.to_bytes(2, "big")
           + udp_length.to_bytes(2, "big") + b"\x00\x00")
    return bytes(header) + udp + payload


def check_initial_template(data):
    """Problems with a raw UDP payload claimed to be one genuine QUIC Initial."""
    problems = []
    if len(data) < MINIMUM_INITIAL_DATAGRAM_BYTES:
        problems.append(f"template is {len(data)} bytes; a QUIC Initial datagram "
                        f"carries at least {MINIMUM_INITIAL_DATAGRAM_BYTES}")
    if len(data) < 5:
        return problems + ["template is too short for a QUIC long header"]
    if data[0] & 0xC0 != 0xC0:
        problems.append("first byte does not carry the long-header and fixed bits")
    if data[1:5] == b"\x00\x00\x00\x00":
        problems.append("version field is zero (a version negotiation packet, not an Initial)")
    return problems


def source_pool(cidr, count, seed):
    """`count` pseudo-random usable hosts of `cidr`; network and broadcast excluded."""
    network = ipaddress.IPv4Network(cidr, strict=False)
    if network.num_addresses < 4:
        raise ValueError(f"{cidr} has no usable host range once network and broadcast are excluded")
    rng = random.Random(seed)
    base = int(network.network_address)
    last_offset = network.num_addresses - 2
    return [str(ipaddress.IPv4Address(base + rng.randint(1, last_offset))) for _ in range(count)]


def parse_sources(text):
    """`IPv4` or `IPv4:port` per line; blank lines and `#` comments are skipped."""
    entries = []
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        host, separator, port = line.partition(":")
        try:
            address = str(ipaddress.IPv4Address(host))
        except ValueError as error:
            raise ValueError(f"line {number}: {line!r} is not an IPv4 address: {error}") from None
        if separator and not (port.isdigit() and 0 < int(port) < 65536):
            raise ValueError(f"line {number}: {line!r} has an invalid port")
        entries.append((address, int(port) if separator else None))
    if not entries:
        raise ValueError("no source addresses")
    return entries


def link_budget(rate_pps, payload_len, link_mbps, fraction):
    """Offered wire rate in bit/s and whether it stays within `fraction` of the link."""
    offered_bps = rate_pps * (payload_len + IPV4_UDP_OVERHEAD_BYTES) * 8
    return (offered_bps <= link_mbps * 1_000_000 * fraction, offered_bps)


def schedule(rate_pps, duration_s):
    """Pacing plan `(packets_per_tick, tick_s, ticks)`: 1 ms ticks above 1000 pps."""
    if rate_pps <= 0 or duration_s <= 0:
        raise ValueError("rate and duration must be positive")
    if rate_pps > 1000:
        tick_s = 0.001
        packets_per_tick = rate_pps / 1000
    else:
        tick_s = 1 / rate_pps
        packets_per_tick = 1
    ticks = max(1, round(duration_s / tick_s))
    return (packets_per_tick, tick_s, ticks)


def pace_state():
    """Mutable progress the pacer keeps current, so an interrupt still leaves counts."""
    return {"attempted_packets": 0, "offered_packets": 0, "send_errors": {}, "elapsed_seconds": 0.0}


def run_pace(send, plan, clock, sleep, state=None):
    """Send on the plan; count sends and errno failures per name without aborting."""
    packets_per_tick, tick_s, ticks = plan
    state = pace_state() if state is None else state
    start = clock()
    errors = state["send_errors"]
    for tick in range(ticks):
        target = round((tick + 1) * packets_per_tick)
        while state["attempted_packets"] < target:
            try:
                send(state["attempted_packets"])
            except OSError as error:
                name = errno.errorcode.get(error.errno, f"errno_{error.errno}")
                errors[name] = errors.get(name, 0) + 1
                if error.errno == errno.ENOBUFS:
                    sleep(tick_s)
            state["attempted_packets"] += 1
            state["offered_packets"] = state["attempted_packets"] - sum(errors.values())
            state["elapsed_seconds"] = clock() - start
        remaining = start + (tick + 1) * tick_s - clock()
        if remaining > 0:
            sleep(remaining)
    state["elapsed_seconds"] = clock() - start
    return state


def parse_target(text):
    """`IP:PORT`; an IPv6 target is refused with the reason."""
    host, separator, port = text.rpartition(":")
    if not separator or not port.isdigit() or not 0 < int(port) < 65536:
        raise ValueError(f"target {text!r} is not IP:PORT")
    address = ipaddress.ip_address(host.strip("[]"))
    if address.version != 4:
        raise ValueError("IPv6 targets are not supported: forged IPv6 sources need a "
                         "link-layer socket; use an IPv4 listener address")
    return (str(address), int(port))


def parse_port_range(text):
    """`A-B` inclusive port range."""
    low, separator, high = text.partition("-")
    if not (separator and low.isdigit() and high.isdigit()):
        raise ValueError(f"port range {text!r} is not A-B")
    first, last = int(low), int(high)
    if not (0 < first <= last < 65536):
        raise ValueError(f"port range {text!r} is outside 1-65535 or reversed")
    return (first, last)


def packet_factory(sources, ports, dst, dport, payload_for, rng):
    """Callable building packet `index` with a rotating forged source address."""
    def build(index):
        address, port = sources[index % len(sources)]
        sport = port if port is not None else rng.randint(ports[0], ports[1])
        return build_ipv4_udp(address, sport, dst, dport, payload_for(index), index + 1)
    return build


def sha256_bytes(data):
    """Hex digest of in-memory bytes."""
    return hashlib.sha256(data).hexdigest()


def preflight_live():
    """Reasons a live run cannot start on this host."""
    reasons = []
    if not sys.platform.startswith("linux"):
        reasons.append("raw IPv4 sockets with forged sources require Linux")
    if hasattr(os, "geteuid") and os.geteuid() != 0:
        reasons.append("a raw socket needs root or CAP_NET_RAW")
    return reasons


def build_parser():
    """Command line: `initial` and `peerlist` share the pacing and output flags."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    subcommands = parser.add_subparsers(dest="swarm", required=True)
    initial = subcommands.add_parser("initial", help="replay one genuine QUIC Initial from forged sources")
    initial.add_argument("--template", required=True, type=Path,
                         help="raw UDP payload bytes of one genuine QUIC Initial")
    initial.add_argument("--source-range", required=True,
                         help="CIDR the forged sources are drawn from (network and broadcast excluded)")
    initial.add_argument("--source-count", type=int, default=4096,
                         help="distinct forged sources drawn from the range (default 4096)")
    peerlist = subcommands.add_parser("peerlist", help="small datagrams from forged peer-list addresses")
    peerlist.add_argument("--sources", required=True, type=Path,
                          help="one IPv4 or IPv4:port per line: the committee, trusted and bootstrap addresses")
    peerlist.add_argument("--size", type=int, default=64, help="payload bytes per datagram (default 64)")
    for sub in (initial, peerlist):
        sub.add_argument("--target", required=True, help="validator listener IP:PORT (IPv4)")
        sub.add_argument("--rate", required=True, type=float, help="requested packets per second")
        sub.add_argument("--duration", required=True, type=float, help="seconds to run")
        sub.add_argument("--seed", type=int, default=0, help="pseudo-random seed for sources, ports and payloads")
        sub.add_argument("--source-ports", default=DEFAULT_SOURCE_PORTS, help="A-B source port range")
        sub.add_argument("--link-mbps", required=True, type=float, help="link speed the budget is checked against")
        sub.add_argument("--max-link-fraction", type=float, default=0.8,
                         help="largest fraction of the link the offered rate may use (default 0.8)")
        sub.add_argument("--output", required=True, type=Path, help="summary JSON path")
        sub.add_argument("--dry-run", action="store_true",
                         help="build the schedule and the first packets without a socket")
    return parser


def usage_failure(message):
    """Report a preflight failure and return the usage exit code."""
    print(f"generate.py: {message}", file=sys.stderr)
    return 2


def write_json(path, value):
    """Write a JSON document atomically enough for a single run."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def summary_document(args, target, population, payload_bytes, template_sha256, pace, notes, started, ended):
    """The offered-traffic summary; received figures come from the validator side."""
    elapsed = pace["elapsed_seconds"]
    offered_rate = pace["offered_packets"] / elapsed if elapsed > 0 else None
    return {
        "schema_version": SCHEMA_VERSION,
        "swarm": args.swarm,
        "target": f"{target[0]}:{target[1]}",
        "ip_family": 4,
        "requested_rate_pps": args.rate,
        "requested_duration_seconds": args.duration,
        "offered_packets": pace["offered_packets"],
        "offered_bytes": pace["offered_packets"] * (payload_bytes + IPV4_UDP_OVERHEAD_BYTES),
        "elapsed_seconds": elapsed,
        "offered_rate_pps": offered_rate,
        "offered_rate_bps": (offered_rate * (payload_bytes + IPV4_UDP_OVERHEAD_BYTES) * 8
                             if offered_rate is not None else None),
        "achieved_fraction": offered_rate / args.rate if offered_rate is not None else None,
        "send_errors": pace["send_errors"],
        "source_population": population,
        "template_sha256": template_sha256,
        "payload_bytes": payload_bytes,
        "start_utc": started,
        "end_utc": ended,
        "generator_host": {"node": platform.node(), "release": platform.release()},
        "link_mbps": args.link_mbps,
        "max_link_fraction": args.max_link_fraction,
        "seed": args.seed,
        "notes": notes,
    }


def main(argv=None):
    """Exit 0 on a complete run, 2 on a preflight failure, 3 on an incomplete run."""
    args = build_parser().parse_args(argv)
    try:
        target = parse_target(args.target)
        ports = parse_port_range(args.source_ports)
    except ValueError as error:
        return usage_failure(str(error))
    rng = random.Random(args.seed)
    template_sha256 = None
    if args.swarm == "initial":
        try:
            template = args.template.read_bytes()
        except OSError as error:
            return usage_failure(f"cannot read template: {error}")
        problems = check_initial_template(template)
        if problems:
            return usage_failure("; ".join(problems))
        if args.source_count <= 0:
            return usage_failure("--source-count must be positive")
        try:
            sources = [(address, None) for address in source_pool(args.source_range, args.source_count, args.seed)]
        except ValueError as error:
            return usage_failure(str(error))
        population = {"kind": "range", "count": len(set(sources)), "range": args.source_range}
        payload_bytes = len(template)
        template_sha256 = sha256_bytes(template)
        payload_for = lambda _index: template  # noqa: E731 (one template, replayed verbatim)
    else:
        if args.size <= 0:
            return usage_failure("--size must be positive")
        try:
            sources = parse_sources(args.sources.read_text())
        except (OSError, ValueError) as error:
            return usage_failure(f"cannot use --sources: {error}")
        population = {"kind": "file", "count": len(sources), "file": str(args.sources)}
        payload_bytes = args.size
        payload_for = lambda _index: rng.randbytes(args.size)  # noqa: E731
    within_budget, offered_bps = link_budget(args.rate, payload_bytes, args.link_mbps, args.max_link_fraction)
    if not within_budget:
        return usage_failure(f"{args.rate} pps of {payload_bytes}-byte payloads offers {offered_bps:.0f} bit/s, "
                             f"above {args.max_link_fraction} of a {args.link_mbps} Mbit/s link")
    try:
        plan = schedule(args.rate, args.duration)
    except ValueError as error:
        return usage_failure(str(error))
    build = packet_factory(sources, ports, target[0], target[1], payload_for, rng)
    notes = [f"offered wire rate {offered_bps:.0f} bit/s within {args.max_link_fraction} of the link",
             "offered figures only; received figures come from the validator side"]
    if args.dry_run:
        first_packets = [build(index) for index in range(DRY_RUN_PACKETS)]
        notes.append(f"dry_run: {len(first_packets)} packets built, none sent, no socket opened")
        pace = pace_state()
        stamp = utc_now()
        document = summary_document(args, target, population, payload_bytes, template_sha256, pace, notes, stamp, stamp)
        document["schedule"] = {"packets_per_tick": plan[0], "tick_seconds": plan[1], "ticks": plan[2]}
        write_json(args.output, document)
        return 0
    reasons = preflight_live()
    if reasons:
        return usage_failure("; ".join(reasons))
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_RAW)
    except OSError as error:
        return usage_failure(f"cannot open a raw socket: {error}")
    destination = (target[0], 0)
    started = utc_now()
    status = 0
    pace = pace_state()
    try:
        run_pace(lambda index: sock.sendto(build(index), destination), plan, time.monotonic, time.sleep, pace)
    except KeyboardInterrupt:
        status = 3
        notes.append("interrupted: counts cover the packets attempted before the interrupt")
    finally:
        sock.close()
    ended = utc_now()
    limits = {name: count for name, count in pace["send_errors"].items() if name in GENERATOR_LIMIT_ERRNOS}
    if limits:
        notes.append(f"generator limits reached: {json.dumps(limits, sort_keys=True)}")
    document = summary_document(args, target, population, payload_bytes, template_sha256, pace, notes, started, ended)
    document["schedule"] = {"packets_per_tick": plan[0], "tick_seconds": plan[1], "ticks": plan[2]}
    try:
        write_json(args.output, document)
    except OSError as error:
        print(f"generate.py: cannot write summary: {error}", file=sys.stderr)
        return 3
    return status


if __name__ == "__main__":
    sys.exit(main())
