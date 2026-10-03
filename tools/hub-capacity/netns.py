#!/usr/bin/env python3
"""Create and inspect qualification links inside the dedicated coordinator container."""

import argparse
import importlib.util
import json
from pathlib import Path
import subprocess


SPEC = importlib.util.spec_from_file_location("capacity_prepare", Path(__file__).with_name("prepare.py"))
PREPARE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREPARE)


def run(*argv):
    return subprocess.run(argv, check=True, capture_output=True, text=True).stdout


def participants():
    ordinary, dao = PREPARE.peer_population()
    return ordinary + dao + [node for node in PREPARE.validators() if not node["hub"]]


def setup():
    if run("ip", "netns", "list").strip():
        raise ValueError("qualification requires a fresh private container namespace")
    for subnet in range(1, 5):
        bridge = f"capacity-b{subnet}"
        run("ip", "link", "add", bridge, "type", "bridge")
        run("ip", "addr", "add", f"10.147.{subnet}.254/24", "dev", bridge)
        run("ip", "link", "set", bridge, "up")
    for index, peer in enumerate(participants()):
        namespace = peer["namespace"]
        subnet = int(peer["ip"].split(".")[2])
        host, child = f"cv{index}", f"cp{index}"
        run("ip", "netns", "add", namespace)
        run("ip", "link", "add", host, "type", "veth", "peer", "name", child)
        run("ip", "link", "set", child, "netns", namespace)
        run("ip", "link", "set", host, "master", f"capacity-b{subnet}")
        run("ip", "link", "set", host, "up")
        run("ip", "-n", namespace, "link", "set", child, "name", "eth0")
        run("ip", "-n", namespace, "addr", "add", peer["ip"] + "/24", "dev", "eth0")
        run("ip", "-n", namespace, "link", "set", "lo", "up")
        run("ip", "-n", namespace, "link", "set", "eth0", "up")
        run("ip", "-n", namespace, "route", "add", "default", "via", f"10.147.{subnet}.254")
        run("ip", "netns", "exec", namespace, "tc", "qdisc", "replace", "dev", "eth0", "root",
            "netem", "limit", "1024", "rate", "25mbit", "delay", "25ms", "loss", "0.1%")
    run("iptables", "-t", "nat", "-A", "POSTROUTING", "-s", "10.147.2.0/24", "-o", "eth0",
        "-j", "SNAT", "--to-source", "10.147.0.20")
    run("iptables", "-A", "FORWARD", "-i", "eth0", "-o", "capacity-b2", "-m", "conntrack",
        "!", "--ctstate", "ESTABLISHED,RELATED", "-j", "DROP")


def snapshot():
    names = [peer["namespace"] for peer in participants()]
    return {
        "links": json.loads(run("ip", "-j", "link")),
        "addresses": json.loads(run("ip", "-j", "addr")),
        "routes": json.loads(run("ip", "-j", "route")),
        "nat_and_filter_counters": run("iptables-save", "-c"),
        "namespaces": {name: {
            "links": json.loads(run("ip", "-n", name, "-j", "link")),
            "addresses": json.loads(run("ip", "-n", name, "-j", "addr")),
            "routes": json.loads(run("ip", "-n", name, "-j", "route")),
            "qdisc": json.loads(run("ip", "netns", "exec", name, "tc", "-j", "qdisc", "show", "dev", "eth0")),
        } for name in names},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("setup", "snapshot"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.command == "setup":
        setup()
    if args.output:
        with args.output.open("x") as output:
            json.dump(snapshot(), output, allow_nan=False, sort_keys=True)


if __name__ == "__main__":
    main()
