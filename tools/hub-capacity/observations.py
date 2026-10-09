#!/usr/bin/env python3
"""Correlate bounded production JSON logs with real gossip and committee observations."""

import argparse
import importlib.util
import hashlib
import os
from collections import OrderedDict, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import time


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("hub_qualify", ROOT / "qualify.py")
QUALIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(QUALIFY)


class Observations:
    def __init__(self, hubs):
        self.condition = threading.Condition()
        self.publications = OrderedDict()
        self.committee = {hub: deque() for hub in hubs}
        self.measuring = set()
        self.positions = {}
        # Track all starts, including warmup rows evicted from the bounded delivery queue.
        self.allocations = {hub: {"generation": None, "process_id": None,
                                  "started_through": 0, "ahead": set()} for hub in hubs}
        self.error = None

    def ingest(self, source, record, *, offset=0, line_sha256=None):
        if record.get("target") != "network::capacity":
            return
        fields = record["fields"]
        with self.condition:
            observation = {"source": source, "record": record, "offset": offset,
                           "line_sha256": line_sha256}
            if fields["event"] == "committee_observation_error":
                raise ValueError("native committee observation identity failed")
            if fields["event"] == "gossip_publish":
                key = (fields["message_id"], fields["source"])
                self.publications[key] = observation
                self.publications.move_to_end(key)
                if len(self.publications) > 8192:
                    self.publications.popitem(last=False)
            elif fields["event"] in {"committee_request_start", "committee_request"} and source in self.committee:
                generation, request_id = QUALIFY.committee_identity(fields)
                allocation = self.allocations[source]
                process_id = QUALIFY.native_integer(fields, "process_id", 1)
                if allocation["generation"] is None:
                    allocation["generation"], allocation["process_id"] = generation, process_id
                if (generation, process_id) != (allocation["generation"], allocation["process_id"]):
                    raise ValueError("committee producer generation or process changed")
                if fields["event"] == "committee_request_start":
                    through, ahead = allocation["started_through"], allocation["ahead"]
                    if request_id <= through or request_id in ahead:
                        raise ValueError("duplicate native committee start")
                    if request_id == through + 1:
                        through = request_id
                        while through + 1 in ahead:
                            ahead.remove(through + 1)
                            through += 1
                        allocation["started_through"] = through
                    else:
                        # Bound stored out-of-order IDs, never allocate a range from a watermark.
                        if len(ahead) >= 1024:
                            raise ValueError("committee start coverage allocation exhausted")
                        ahead.add(request_id)
                queue = self.committee[source]
                if len(queue) >= 1024:
                    if source in self.measuring:
                        raise ValueError("committee observation allocation exhausted")
                    queue.popleft()
                queue.append(observation)
            self.condition.notify_all()

    def query(self, request, timeout=2):
        deadline = time.monotonic() + timeout
        with self.condition:
            while True:
                if self.error:
                    raise ValueError(self.error)
                if request["scenario"] == "gossip_two_hops":
                    trace = request["trace"]
                    receipt = trace["receipt"]
                    publication = self.publications.get((receipt["message_id"], trace["route"][0]))
                    if publication is not None:
                        trace = {**trace, "publication": publication}
                        return {"success": True, "trace": trace, "route": trace["route"]}
                elif request["scenario"] == "committee_progress":
                    self.measuring.add(request["identity"])
                    queue = self.committee[request["identity"]]
                    entries = []
                    while queue and len(entries) < 32:
                        observation = queue.popleft()
                        fields = observation["record"]["fields"]
                        if int(fields["started_unix_us"]) >= request["not_before_unix_us"]:
                            entries.append(observation)
                    if entries:
                        return {"success": True, "collector_status": "batch",
                                "trace": {"observations": entries, **self.position(request["identity"])}}
                else:
                    raise ValueError("unsupported observation scenario")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if request["scenario"] == "committee_progress":
                        return {"success": True, "collector_status": "empty",
                                "trace": {"observations": [], **self.position(request["identity"])}}
                    raise TimeoutError("no matching production observation before deadline")
                self.condition.wait(remaining)

    def position(self, source):
        offset, size = self.positions.get(source, (0, -1))
        allocation = self.allocations[source]
        return {"offset": offset, "size": size, "caught_up": offset == size,
                "queued": len(self.committee[source]),
                "allocation": {key: allocation[key] for key in
                               ("generation", "process_id", "started_through")},
                "allocation_holes": len(allocation["ahead"])}


def follow(observations, source, path):
    try:
        with path.open("rb") as stream:
            while True:
                if path.stat().st_size > QUALIFY.MAX_PROTOCOL_LOG_BYTES:
                    raise ValueError(f"production log exceeds {QUALIFY.MAX_PROTOCOL_LOG_BYTES // 1024**2} MiB")
                position = stream.tell()
                line = stream.readline(65537)
                if len(line) > 65536:
                    raise ValueError("production log line exceeds 64 KiB")
                if not line.endswith(b"\n"):
                    stream.seek(position)
                    time.sleep(0.02)
                else:
                    try:
                        record = json.loads(line)
                    except (UnicodeError, json.JSONDecodeError):
                        # Other process output remains in the retained raw log.
                        if b"committee_" in line:
                            raise ValueError("invalid native committee log record")
                        record = None
                    if record is not None:
                        observations.ingest(source, record, offset=position,
                                            line_sha256=hashlib.sha256(line).hexdigest())
                with observations.condition:
                    observations.positions[source] = (stream.tell(), path.stat().st_size)
                    observations.condition.notify_all()
    except (OSError, ValueError, KeyError, TypeError) as error:
        with observations.condition:
            observations.error = f"{source}: {error}"
            observations.condition.notify_all()


def handler_for(observations):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 65536:
                    raise ValueError("observation request must be bounded")
                request = json.loads(self.rfile.read(length))
                response = observations.query(request)
            except (OSError, ValueError, KeyError, TypeError, TimeoutError) as error:
                response = {"success": False, "collector_status": "error", "rejection_reason": str(error)}
            data = json.dumps(response, allow_nan=False, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *_args):
            pass

    return Handler


class BoundedServer(ThreadingHTTPServer):
    """At most 72 active private control handlers, with no queued application tasks."""
    daemon_threads = True

    def __init__(self, *args):
        self.slots = threading.BoundedSemaphore(72)
        super().__init__(*args)

    def process_request(self, request, client_address):
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", action="append", required=True, help="public identity=production JSON log")
    parser.add_argument("--hub", action="append", required=True)
    parser.add_argument("--listen", default="127.0.0.1:9400")
    parser.add_argument("--pid-file", type=Path, required=True)
    args = parser.parse_args()
    sources = dict(entry.split("=", 1) for entry in args.source)
    if len(sources) != len(args.source) or not set(args.hub) <= set(sources) or len(args.hub) != 2:
        raise ValueError("declare distinct sources and exactly two measured hubs")
    observations = Observations(args.hub)
    for source, path in sources.items():
        threading.Thread(target=follow, args=(observations, source, Path(path)), daemon=True).start()
    host, port = args.listen.rsplit(":", 1)
    server = BoundedServer((host, int(port)), handler_for(observations))
    with args.pid_file.open("x") as output:
        output.write(str(os.getpid()) + "\n")
    server.serve_forever()


if __name__ == "__main__":
    main()
