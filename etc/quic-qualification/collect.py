#!/usr/bin/env python3
"""Collect passive Linux evidence for the shared QUIC ingress investigation.

This tool neither generates traffic nor changes socket or host configuration.
A completed capture is not a deployment qualification verdict.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time


def observe(action):
    """Keep unavailable evidence distinct from a successfully observed zero."""
    try:
        return {"status": "ok", "value": action()}
    except (OSError, ValueError, IndexError) as error:
        return {"status": "error", "error": f"{type(error).__name__}: {error}"}


def command(argv):
    """Record a bounded, read-only command, including failure diagnostics."""
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=10,
                                env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"})
        return {
            "argv": argv,
            "status": "ok" if result.returncode == 0 else "error",
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    except (OSError, subprocess.TimeoutExpired, UnicodeError) as error:
        return {"argv": argv, "status": "error", "error": str(error)}


def process_identity(proc, pid):
    """Detect exits, PID reuse, namespace changes and executable replacement."""
    process = proc / str(pid)
    # comm can contain spaces and parentheses; the final ')' ends field 2.
    fields = (process / "stat").read_text().rsplit(") ", 1)[1].split()
    executable = os.readlink(process / "exe")
    executable_stat = os.stat(process / "exe")
    return {
        "pid": pid,
        "start_ticks": int(fields[19]),
        "boot_id": (proc / "sys/kernel/random/boot_id").read_text().strip(),
        "net_namespace": os.readlink(process / "ns/net"),
        "executable": executable,
        "executable_device": executable_stat.st_dev,
        "executable_inode": executable_stat.st_ino,
    }


def socket_inodes(process):
    """Select the target process's sockets, rejecting an incomplete FD scan."""
    # Keep our directory FD alive when inspecting the collector's own process.
    with os.scandir(process / "fd") as descriptors:
        targets = (os.readlink(fd.path) for fd in descriptors)
        return {
            int(match.group(1))
            for target in targets
            if (match := re.fullmatch(r"socket:\[(\d+)\]", target))
        }


def parse_udp(text, inodes):
    """Read Linux UDP queue memory and drops for only the selected sockets."""
    lines = text.splitlines()
    if not lines or "local_address" not in lines[0] or "inode" not in lines[0]:
        raise ValueError("missing UDP table header")
    sockets = []
    for line in lines[1:]:
        fields = line.split()
        if not fields:
            continue
        inode = int(fields[9])
        if inode in inodes:
            tx_queue, rx_queue = fields[4].split(":")
            sockets.append({
                "inode": inode,
                "local_hex": fields[1],
                "remote_hex": fields[2],
                "tx_queue_bytes": int(tx_queue, 16),
                "rx_queue_bytes": int(rx_queue, 16),
                "drops": int(fields[12]),
            })
    return sockets


def udp_counters(text):
    """Decode named IPv4 UDP counters without depending on their order."""
    rows = [line.split()[1:] for line in text.splitlines() if line.startswith("Udp:")]
    if len(rows) != 2 or len(rows[0]) != len(rows[1]):
        raise ValueError("missing or mismatched UDP counter headers")
    return dict(zip(rows[0], map(int, rows[1])))


def udp6_counters(text):
    """Decode IPv6 UDP counters from the separate SNMP6 table."""
    counters = {
        name: int(value)
        for name, value in (line.split() for line in text.splitlines())
        if name.startswith("Udp6")
    }
    if not counters:
        raise ValueError("missing IPv6 UDP counters")
    return counters


def nic_counters(interface):
    """Retain interface-wide receive and transmit counters at each sample."""
    names = ("rx_bytes", "rx_packets", "rx_dropped", "rx_errors",
             "tx_bytes", "tx_packets", "tx_dropped", "tx_errors")
    return {name: int((interface / "statistics" / name).read_text()) for name in names}


def counter_delta(before, after):
    """Never turn a missing counter or a reset into a numeric delta."""
    if before["status"] != "ok" or after["status"] != "ok":
        return {"status": "unavailable"}
    start, end = before["value"], after["value"]
    if start.keys() != end.keys():
        return {"status": "unavailable", "reason": "counter set changed"}
    reset = sorted(name for name in start if end[name] < start[name])
    if reset:
        return {"status": "reset", "counters": reset}
    return {"status": "ok", "value": {name: end[name] - start[name] for name in start}}


def accumulate_delta(total, before, after):
    """Preserve a reset or missing observation even if a later sample recovers."""
    if total["status"] != "ok":
        return total
    delta = counter_delta(before, after)
    if delta["status"] != "ok":
        return delta
    return {"status": "ok", "value": {
        name: value + delta["value"][name] for name, value in total["value"].items()
    }}


def snapshot(proc, pid, interface):
    """Capture process, socket, namespace and NIC evidence with bounded timestamps."""
    process = proc / str(pid)
    start = time.monotonic_ns()
    identity = observe(lambda: process_identity(proc, pid))
    inodes = observe(lambda: sorted(socket_inodes(process)))
    tables = {name: observe(lambda name=name: (process / "net" / name).read_text())
              for name in ("udp", "udp6")}
    sockets = {}
    for name, table in tables.items():
        if table["status"] == "ok" and inodes["status"] == "ok":
            sockets[name] = observe(lambda: parse_udp(table["value"], set(inodes["value"])))
        else:
            sockets[name] = {"status": "unavailable", "reason": "FD scan or UDP table failed"}
    sample = {
        "started_monotonic_ns": start,
        "wall_time_ns": time.time_ns(),
        "identity_before": identity,
        "socket_inodes": inodes,
        "socket_tables": tables,
        "sockets": sockets,
        "udp": observe(lambda: udp_counters((process / "net/snmp").read_text())),
        "udp6": observe(lambda: udp6_counters((process / "net/snmp6").read_text())),
        "nic": observe(lambda: nic_counters(interface)),
        "process_stat": observe(lambda: (process / "stat").read_text()),
        "process_schedstat": observe(lambda: (process / "schedstat").read_text()),
        "host_stat": observe(lambda: (proc / "stat").read_text()),
        "host_softirqs": observe(lambda: (proc / "softirqs").read_text()),
        "host_softnet": observe(lambda: (proc / "net/softnet_stat").read_text()),
    }
    sample["identity_after"] = observe(lambda: process_identity(proc, pid))
    sample["finished_monotonic_ns"] = time.monotonic_ns()
    return sample


def sha256(path):
    """Hash the actual running executable using bounded memory."""
    digest = hashlib.sha256()
    with path.open("rb") as binary:
        for chunk in iter(lambda: binary.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def host_evidence(proc, pid, interface):
    """Record effective socket buffers, NIC configuration and firewall evidence."""
    commands = [
        ["ss", "-u", "-a", "-n", "-m", "-e", "-p"],
        ["ip", "-details", "-statistics", "link", "show", "dev", interface],
        ["ethtool", interface], ["ethtool", "-i", interface],
        ["ethtool", "-k", interface], ["ethtool", "-S", interface],
        ["nft", "list", "ruleset"], ["iptables-save", "-c"], ["ip6tables-save", "-c"],
    ]
    paths = ("sys/net/core/rmem_default", "sys/net/core/rmem_max",
             "sys/net/core/netdev_max_backlog", "sys/net/core/netdev_budget",
             "sys/net/core/netdev_budget_usecs", "sys/net/ipv4/udp_rmem_min",
             "sys/net/ipv4/udp_mem", "cpuinfo", "meminfo")
    return {
        "uname": list(platform.uname()),
        "proc": {name: observe(lambda name=name: (proc / name).read_text()) for name in paths},
        "process": {name: observe(lambda name=name: (proc / str(pid) / name).read_text())
                    for name in ("status", "limits", "cgroup")},
        "executable": observe(lambda: os.readlink(proc / str(pid) / "exe")),
        "executable_sha256": observe(lambda: sha256(proc / str(pid) / "exe")),
        "commands": [command(argv) for argv in commands],
    }


def write_json(path, value):
    """Create an artifact without overwriting any earlier capture."""
    with path.open("x") as output:
        json.dump(value, output, indent=2, allow_nan=False)
        output.write("\n")


def positive_seconds(value):
    """Reject unbounded, non-finite and non-positive sampling arguments."""
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be finite and greater than zero")
    return number


def accumulate_socket_drops(total, before, after):
    """Summarize only socket inodes and addresses observed in every sample."""
    if total is not None and total["status"] != "ok":
        return total
    if before["status"] != "ok" or after["status"] != "ok":
        return {"status": "unavailable"}
    start = {str(row["inode"]): row for row in before["value"]}
    end = {str(row["inode"]): row for row in after["value"]}
    if total is None:
        total = {"status": "ok", "value": {
            inode: {"status": "ok", "value": {"drops": 0}} for inode in start
        }}
    values = {}
    for inode, previous in total["value"].items():
        if inode not in start or inode not in end:
            continue
        if any(start[inode][key] != end[inode][key] for key in ("local_hex", "remote_hex")):
            continue
        values[inode] = accumulate_delta(
            previous, {"status": "ok", "value": {"drops": start[inode]["drops"]}},
            {"status": "ok", "value": {"drops": end[inode]["drops"]}})
    return {"status": "ok", "value": values}


def capture(args, proc=Path("/proc"), sysfs=Path("/sys")):
    """Write a reusable capture; return failure if process identity becomes invalid."""
    if platform.system() != "Linux":
        raise ValueError("collection requires Linux; parser tests can run on other systems")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,14}", args.interface):
        raise ValueError("invalid interface name")
    interface = sysfs / "class/net" / args.interface
    if not (interface / "statistics").is_dir():
        raise ValueError("network interface does not exist")
    identity = process_identity(proc, args.pid)
    if identity["net_namespace"] != os.readlink(proc / "self/ns/net"):
        raise ValueError("collector and target must share a network namespace")
    manifest = json.loads(args.manifest.read_text())
    if not isinstance(manifest, dict):
        raise ValueError("run manifest must be a JSON object")
    first = last = None
    count = 0
    status = "incomplete"
    deltas = {}
    socket_deltas = {}
    error = None
    args.output.mkdir()
    try:
        write_json(args.output / "metadata.json", {
            "schema_version": 1,
            "qualification": "not_evaluated",
            "manifest": manifest,
            "identity": identity,
            "interface": args.interface,
            "requested_duration_seconds": args.duration,
            "requested_interval_seconds": args.interval,
            "host_before": host_evidence(proc, args.pid, args.interface),
        })
        with (args.output / "samples.jsonl").open("x") as output:
            deadline = time.monotonic() + args.duration
            while True:
                sample = snapshot(proc, args.pid, interface)
                output.write(json.dumps(sample, allow_nan=False) + "\n")
                output.flush()
                for key in ("udp", "udp6", "nic"):
                    deltas[key] = (accumulate_delta(deltas[key], last[key], sample[key])
                                   if last else counter_delta(sample[key], sample[key]))
                for family in ("udp", "udp6"):
                    socket_deltas[family] = accumulate_socket_drops(
                        socket_deltas.get(family), (last or sample)["sockets"][family],
                        sample["sockets"][family])
                first = first or sample
                last = sample
                count += 1
                if any(sample[key] != {"status": "ok", "value": identity}
                       for key in ("identity_before", "identity_after")):
                    status = "invalid_process_identity"
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0 and count >= 2:
                    status = "complete"
                    break
                time.sleep(min(args.interval, max(remaining, 0)))
    except KeyboardInterrupt:
        status = "interrupted"
    except (OSError, ValueError) as failure:
        status = "incomplete"
        error = f"{type(failure).__name__}: {failure}"
    finally:
        try:
            host_after = host_evidence(proc, args.pid, args.interface)
        except KeyboardInterrupt:
            status = "interrupted"
            host_after = {"status": "unavailable", "reason": "interrupted"}
        except (OSError, ValueError) as failure:
            error = f"{type(failure).__name__}: {failure}"
            host_after = {"status": "error", "error": error}
            if status == "complete":
                status = "incomplete"
        elapsed = ((last["started_monotonic_ns"] - first["started_monotonic_ns"])
                   / 1e9) if first and last else None
        summary = {
            "capture_status": status,
            "capture_error": error,
            "qualification": "not_evaluated",
            "samples": count,
            "elapsed_seconds": elapsed,
            "mean_sample_interval_seconds": elapsed / (count - 1) if count > 1 else None,
            "counter_deltas": deltas if status == "complete" else {},
            "socket_drop_deltas": socket_deltas if status == "complete" else {},
            "host_after": host_after,
        }
        try:
            write_json(args.output / "summary.json", summary)
        except KeyboardInterrupt:
            status = "interrupted"
            error = "summary write interrupted"
        except (OSError, ValueError) as failure:
            status = "incomplete"
            error = f"summary write failed: {failure}"
        if error is not None:
            print(f"Partial capture: {args.output}: {error}", file=sys.stderr)
    return 0 if status == "complete" else 3


def main():
    """Parse a single controlled run and print its artifact location."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--interface", required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration", type=positive_seconds, default=60.0)
    parser.add_argument("--interval", type=positive_seconds, default=1.0)
    args = parser.parse_args()
    try:
        result = capture(args)
    except (OSError, ValueError) as error:
        parser.exit(2, f"capture failed: {error}\n")
    except KeyboardInterrupt:
        parser.exit(2, "capture interrupted before collection started\n")
    print(f"Capture: {args.output}; deployment qualification not evaluated")
    return result


if __name__ == "__main__":
    sys.exit(main())
