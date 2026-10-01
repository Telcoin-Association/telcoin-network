#!/usr/bin/env python3
"""Invoke one persistent production-protocol peer with the frozen workload nonce."""

import argparse
import json
import os
import urllib.request


def post(url, payload):
    request = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=29) as response:
        body = response.read(64 * 1024 + 1)
    if len(body) > 64 * 1024:
        raise ValueError("protocol acknowledgement exceeds 64 KiB")
    return json.loads(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url")
    parser.add_argument("--identity", help="measured hub identity for committee log observations")
    parser.add_argument("--observations", help="local production-log correlation service")
    args = parser.parse_args()
    payload = {
        "operation_id": os.environ["HUB_CAPACITY_OPERATION_ID"],
        "scenario": os.environ["HUB_CAPACITY_SCENARIO"],
        "not_before_unix_us": int(os.environ["HUB_CAPACITY_MEASUREMENT_UNIX_US"]),
    }
    if payload["scenario"] == "committee_progress":
        if not args.identity or not args.observations:
            raise ValueError("committee observations require a declared hub and production-log service")
        result = {"operation_id": payload["operation_id"], "scenario": payload["scenario"],
                  "identity": args.identity, **post(args.observations, {**payload, "identity": args.identity})}
    else:
        if not args.url:
            raise ValueError("peer control URL is required")
        result = post(args.url, payload)
        if payload["scenario"] == "gossip_two_hops" and result.get("success"):
            if not args.observations:
                raise ValueError("gossip latency requires its production publisher observation")
            result.update(post(args.observations, {**payload, "trace": result["trace"]}))
    if result.get("operation_id") != payload["operation_id"] or result.get("scenario") != payload["scenario"]:
        raise ValueError("peer acknowledgement does not match the workload command")
    print(json.dumps(result, allow_nan=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
