#!/usr/bin/env python3
"""Invoke one persistent production-protocol peer with the frozen workload nonce."""

import argparse
import http.client
import io
import ipaddress
import json
import os
from pathlib import Path
import socket
import time
import urllib.error
import urllib.parse
import urllib.request


# The reusable client inherits proxy configuration once, before workload admission.
HTTP_PROXIES = urllib.request.getproxies()


def direct_http_url(url):
    """Only declared numeric HTTP endpoints support the bounded local transport."""
    try:
        parsed = urllib.parse.urlsplit(url)
        ipaddress.ip_address(parsed.hostname)
        return (parsed.scheme == "http" and parsed.port != 0 and
                parsed.username is None and parsed.password is None and not parsed.fragment and
                (not HTTP_PROXIES.get("http") or
                 urllib.request.proxy_bypass_environment(parsed.hostname, HTTP_PROXIES)))
    except (ValueError, TypeError):
        return False


def remaining_timeout(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("control operation deadline exceeded")
    return min(29, remaining)


class DeadlineReader(io.RawIOBase):
    """Recheck the operation deadline even when headers or bodies arrive in a drip."""

    def __init__(self, connection, deadline):
        self.connection = connection
        self.deadline = deadline

    def readable(self):
        return True

    def readinto(self, buffer):
        self.connection.settimeout(remaining_timeout(self.deadline))
        return self.connection.recv_into(buffer)

    def close(self):
        try:
            self.connection.close()
        finally:
            super().close()


class DeadlineSocket:
    def __init__(self, connection, deadline):
        self.connection = connection
        self.deadline = deadline

    def sendall(self, data):
        self.connection.settimeout(remaining_timeout(self.deadline))
        self.connection.sendall(data)

    def makefile(self, mode):
        if mode != "rb":
            raise ValueError("control response requires a binary reader")
        return io.BufferedReader(DeadlineReader(self.connection, self.deadline))

    def close(self):
        self.connection.close()


def deadline_post(url, body, deadline):
    """Make one direct HTTP request without DNS, redirects, or background tasks."""
    if not direct_http_url(url):
        raise ValueError("bounded control requires a declared numeric HTTP endpoint")
    parsed = urllib.parse.urlsplit(url)
    address = ipaddress.ip_address(parsed.hostname)
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port)
    raw = socket.socket(socket.AF_INET6 if address.version == 6 else socket.AF_INET,
                        socket.SOCK_STREAM)
    try:
        raw.settimeout(remaining_timeout(deadline))
        raw.connect((str(address), parsed.port or 80))
        connection.sock = DeadlineSocket(raw, deadline)
        path = urllib.parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
        connection.request("POST", path, body, {"Content-Type": "application/json"})
        with connection.getresponse() as response:
            if not 200 <= response.status < 300:
                error = urllib.error.HTTPError(url, response.status, response.reason,
                                               response.headers, None)
                error.close()
                raise error
            return response.read(64 * 1024 + 1)
    finally:
        connection.close()
        raw.close()


def post(url, payload, timings=None, deadline=None):
    request = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
    started = time.time_ns() // 1000
    if deadline is None:
        with urllib.request.urlopen(request, timeout=29) as response:
            body = response.read(64 * 1024 + 1)
    else:
        body = deadline_post(url, request.data, deadline)
    completed = time.time_ns() // 1000
    if len(body) > 64 * 1024:
        raise ValueError("protocol acknowledgement exceeds 64 KiB")
    if timings is not None:
        timings.append({"request_started_unix_us": started,
                        "response_completed_unix_us": completed})
    result = json.loads(body)
    if not isinstance(result, dict):
        raise ValueError("protocol acknowledgement must be an object")
    if deadline is not None:
        remaining_timeout(deadline)
    return result


def invoke(args, environment, deadline=None, started=None):
    """Share the native adapter payload and witness logic with its standalone CLI."""
    started = time.time_ns() // 1000 if started is None else started
    timings = []
    payload = {
        "operation_id": environment["HUB_CAPACITY_OPERATION_ID"],
        "scenario": environment["HUB_CAPACITY_SCENARIO"],
        "not_before_unix_us": int(environment["HUB_CAPACITY_MEASUREMENT_UNIX_US"]),
    }
    if payload["scenario"] == "concurrent_sync":
        if not args.bulk_root:
            raise ValueError("bulk sync requires retained executed-batch observations")
        fixture = Path(args.bulk_root) / environment["HUB_CAPACITY_PHASE"] / "bulk-targets.json"
        if fixture.stat().st_size > 8192:
            raise ValueError("bulk target observations exceed 8 KiB")
        targets = json.loads(fixture.read_text())
        payload.update({key: targets[key] for key in ("sync_epoch", "batch_digests")})
    if payload["scenario"] == "committee_progress":
        if not args.identity or not args.observations:
            raise ValueError("committee observations require a declared hub and production-log service")
        result = {"operation_id": payload["operation_id"], "scenario": payload["scenario"],
                  "identity": args.identity, **post(args.observations, {**payload, "identity": args.identity}, timings, deadline)}
    else:
        if not args.url:
            raise ValueError("peer control URL is required")
        result = post(args.url, payload, timings, deadline)
        if payload["scenario"] == "gossip_two_hops" and result.get("success"):
            if not args.observations:
                raise ValueError("gossip latency requires its production publisher observation")
            result.update(post(args.observations, {**payload, "trace": result["trace"]}, timings, deadline))
    if result.get("operation_id") != payload["operation_id"] or result.get("scenario") != payload["scenario"]:
        raise ValueError("peer acknowledgement does not match the workload command")
    result["control_timing"] = {"started_unix_us": started, "requests": timings,
                                "completed_unix_us": time.time_ns() // 1000}
    return result


def main():
    started = time.time_ns() // 1000
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url")
    parser.add_argument("--identity", help="measured hub identity for committee log observations")
    parser.add_argument("--observations", help="local production-log correlation service")
    parser.add_argument("--bulk-root", help="retained completed-epoch and executed-batch observations")
    args = parser.parse_args()
    result = invoke(args, os.environ, started=started)
    print(json.dumps(result, allow_nan=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
