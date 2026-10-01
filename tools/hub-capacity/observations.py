#!/usr/bin/env python3
"""Correlate bounded production JSON logs with real gossip and committee observations."""

import argparse
from collections import OrderedDict, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import time


class Observations:
    def __init__(self, hubs):
        self.condition = threading.Condition()
        self.publications = OrderedDict()
        self.committee = {hub: deque() for hub in hubs}
        self.error = None

    def ingest(self, source, record):
        if record.get("target") != "network::capacity":
            return
        fields = record["fields"]
        with self.condition:
            observation = {"source": source, "record": record}
            if fields["event"] == "gossip_publish":
                key = (fields["message_id"], fields["source"])
                self.publications[key] = observation
                self.publications.move_to_end(key)
                if len(self.publications) > 8192:
                    self.publications.popitem(last=False)
            elif fields["event"] == "committee_request" and source in self.committee:
                queue = self.committee[source]
                if len(queue) >= 1024:
                    raise ValueError("committee observation allocation exhausted")
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
                    queue = self.committee[request["identity"]]
                    entries = []
                    while queue and len(entries) < 32:
                        observation = queue.popleft()
                        fields = observation["record"]["fields"]
                        if int(fields["unix_us"]) - int(fields["latency_us"]) >= request["not_before_unix_us"]:
                            entries.append(observation)
                    if entries:
                        return {"success": True, "trace": {"observations": entries}}
                else:
                    raise ValueError("unsupported observation scenario")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("no matching production observation before deadline")
                self.condition.wait(remaining)


def follow(observations, source, path):
    try:
        with path.open("rb") as stream:
            while True:
                if path.stat().st_size > 64 * 1024**2:
                    raise ValueError("production log exceeds 64 MiB")
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
                        continue
                    observations.ingest(source, record)
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
                response = {"success": False, "rejection_reason": str(error)}
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
    """At most 32 active private control handlers, with no queued application tasks."""
    def __init__(self, *args):
        self.slots = threading.BoundedSemaphore(32)
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
    args = parser.parse_args()
    sources = dict(entry.split("=", 1) for entry in args.source)
    if len(sources) != len(args.source) or not set(args.hub) <= set(sources) or len(args.hub) != 2:
        raise ValueError("declare distinct sources and exactly two measured hubs")
    observations = Observations(args.hub)
    for source, path in sources.items():
        threading.Thread(target=follow, args=(observations, source, Path(path)), daemon=True).start()
    host, port = args.listen.rsplit(":", 1)
    server = BoundedServer((host, int(port)), handler_for(observations))
    server.serve_forever()


if __name__ == "__main__":
    main()
