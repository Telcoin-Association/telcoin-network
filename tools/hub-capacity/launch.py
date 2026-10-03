#!/usr/bin/env python3
"""Launch one exact declared hub argv inside its CPU and memory limited container."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", type=Path)
    args = parser.parse_args()
    if args.command.stat().st_size > 128 * 1024:
        raise ValueError("hub launch declaration exceeds 128 KiB")
    command = json.loads(args.command.read_text())
    if not command["argv"] or not all(isinstance(argument, str) for argument in command["argv"]):
        raise ValueError("hub launch requires its complete argument vector")
    with Path(command["log"]).open("xb") as log:
        child = subprocess.Popen(command["argv"], env={**os.environ, **command["environment"]},
                                 stdout=log, stderr=log)
        Path(command["pid_file"]).write_text(str(child.pid))
        def stop(_signum, _frame):
            if child.poll() is None:
                child.terminate()
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        try:
            status = child.wait()
            if status != 0:
                raise SystemExit(status)
        finally:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()


if __name__ == "__main__":
    main()
