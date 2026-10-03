#!/usr/bin/env python3
"""Run persistent real peers and restart the same shared-NAT identities under bounded control."""

import argparse
import os
import importlib.util
import json
from pathlib import Path
import signal
import subprocess
import threading
import time
import urllib.request


SPEC = importlib.util.spec_from_file_location("capacity_observations", Path(__file__).with_name("observations.py"))
OBSERVATIONS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(OBSERVATIONS)


class Peer:
    def __init__(self, declaration, phase, binary):
        self.declaration = declaration
        self.phase = phase
        self.binary = binary
        self.lock = threading.Lock()
        self.lifecycle_lock = threading.RLock()
        self.stopping = False
        self.generation = -1
        self.process = None
        self.log = None
        self.ready = None

    def start(self):
        with self.lifecycle_lock:
            if self.stopping:
                raise ValueError("peer supervisor is shutting down")
            self.generation += 1
            if self.generation > 64:
                raise ValueError("peer restart allocation exhausted")
            name = self.declaration["name"]
            self.ready = self.phase / f"{name}-ready-{self.generation:02}.json"
            self.log = (self.phase / f"{name}-process-{self.generation:02}.log").open("xb")
            self.process = subprocess.Popen(["ip", "netns", "exec", self.declaration["namespace"],
                                            str(self.binary), "run", "--config", str(self.phase / "peers" / f"{name}.json"),
                                            "--ready", str(self.ready)], stdout=self.log, stderr=self.log)

    def wait_ready(self, timeout=12):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise ValueError(f"peer process exited with {self.process.returncode}")
            if self.ready.exists() and self.ready.stat().st_size:
                try:
                    public = json.loads(self.ready.read_text())
                except json.JSONDecodeError:
                    time.sleep(0.02)
                    continue
                if public["identity"] != self.declaration["identity"] or public["bls_key"] != self.declaration["bls_key"]:
                    raise ValueError("restarted process changed its declared identity")
                return public
            time.sleep(0.02)
        raise TimeoutError("peer process did not publish its public ready report")

    def stop(self):
        with self.lifecycle_lock:
            if self.process is not None and self.process.poll() is None:
                self.process.send_signal(signal.SIGINT)
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
            if self.log is not None:
                self.log.close()

    def command(self, request):
        if request["scenario"] != "shared_nat_reconnect":
            if self.stopping:
                raise ValueError("peer supervisor is shutting down")
            return self.forward(request, request, None)
        with self.lock:
            if self.stopping:
                raise ValueError("peer supervisor is shutting down")
            restart = None
            if request["scenario"] == "shared_nat_reconnect":
                if not self.declaration["nat"] or self.process.poll() is not None:
                    raise ValueError("shared-NAT restart requires the existing declared NAT peer")
                old_pid = self.process.pid
                self.stop()
                self.start()
                public = self.wait_ready()
                restart = {"old_pid": old_pid, "new_pid": self.process.pid,
                           "generation": self.generation, "public_ready": public}
                # The old process is gone. Check both live connections in the replacement process.
                forwarded = {**request, "scenario": "dao_connectivity"}
            else:
                forwarded = request
            return self.forward(request, forwarded, restart)

    def forward(self, request, forwarded, restart):
        url = f"http://{self.declaration['ip']}:9500/"
        control = urllib.request.Request(url, data=json.dumps(forwarded).encode(),
                                         headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(control, timeout=29) as response:
            data = response.read(65537)
        if len(data) > 65536:
            raise ValueError("peer acknowledgement exceeds 64 KiB")
        result = json.loads(data)
        if result["operation_id"] != request["operation_id"] or result["scenario"] != forwarded["scenario"] or result["identity"] != self.declaration["identity"]:
            raise ValueError("protocol peer did not acknowledge the declared request and identity")
        result["scenario"] = request["scenario"]
        if restart is not None:
            result["trace"] = {"restart": restart, "connections": result.get("trace")}
        return result


def stop_peers(peers, grace_seconds=5, kill_seconds=5):
    """Stop only recorded children, with shared deadlines and no restart during shutdown."""
    peers = tuple(peers)
    for peer in peers:
        peer.stopping = True
    owned = []
    for peer in peers:
        with peer.lifecycle_lock:
            peer.stopping = True
            owned.append((peer.process, peer.log))
    for process, _log in owned:
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGINT)
    deadline = time.monotonic() + grace_seconds
    for process, _log in owned:
        if process is not None and process.poll() is None:
            try:
                process.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                pass
    for process, _log in owned:
        if process is not None and process.poll() is None:
            process.kill()
    deadline = time.monotonic() + kill_seconds
    for process, log in owned:
        if process is not None:
            process.wait(timeout=max(0, deadline - time.monotonic()))
        if log is not None:
            log.close()


def handler_for(peers):
    class Handler(OBSERVATIONS.handler_for(None)):
        def do_POST(self):
            request = {}
            try:
                name = self.path.removeprefix("/peer/")
                length = int(self.headers.get("Content-Length", "0"))
                if name not in peers or not 0 < length <= 16384:
                    raise ValueError("unknown peer or excessive control request")
                request = json.loads(self.rfile.read(length))
                result = peers[name].command(request)
            except (OSError, ValueError, KeyError, TypeError, TimeoutError) as error:
                result = {"operation_id": request.get("operation_id"), "scenario": request.get("scenario"),
                          "identity": peers[name].declaration["identity"] if name in peers else None,
                          "success": False, "rejection_reason": str(error)}
            data = json.dumps(result, allow_nan=False, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("phase", choices=("baseline", "candidate"))
    parser.add_argument("binary", type=Path)
    parser.add_argument("--ready", type=Path, required=True)
    parser.add_argument("--pid-file", type=Path, required=True)
    args = parser.parse_args()
    population = json.loads((args.root / "population.json").read_text())
    declarations = population["ordinary"] + population["dao"]
    if len(declarations) != 72:
        raise ValueError("exactly 64 ordinary and eight DAO peers are required")
    peers = {declaration["name"]: Peer(declaration, args.root / args.phase, args.binary) for declaration in declarations}
    server = OBSERVATIONS.BoundedServer(("10.147.0.20", 9401), handler_for(peers))
    def stop(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with args.pid_file.open("x") as output:
        output.write(str(os.getpid()) + "\n")
    try:
        for peer in peers.values():
            peer.start()
        reports = [peer.wait_ready() for peer in peers.values()]
        with args.ready.open("x") as output:
            json.dump(reports, output, allow_nan=False)
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop_peers(peers.values())
        server.server_close()


if __name__ == "__main__":
    main()
