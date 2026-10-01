#!/usr/bin/env python3
"""Invoke one persistent production-protocol peer with the frozen workload nonce."""

import argparse
import json
import os
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    args = parser.parse_args()
    payload = {
        "operation_id": os.environ["HUB_CAPACITY_OPERATION_ID"],
        "scenario": os.environ["HUB_CAPACITY_SCENARIO"],
    }
    request = urllib.request.Request(args.url, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=29) as response:
        body = response.read(64 * 1024 + 1)
    if len(body) > 64 * 1024:
        raise ValueError("peer acknowledgement exceeds 64 KiB")
    result = json.loads(body)
    if result.get("operation_id") != payload["operation_id"] or result.get("scenario") != payload["scenario"]:
        raise ValueError("peer acknowledgement does not match the workload command")
    print(json.dumps(result, allow_nan=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
