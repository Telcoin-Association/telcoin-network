"""Exercise metrics transport with synthetic HTTP responses, never qualification evidence."""

from contextlib import contextmanager
import gzip
import importlib.util
from pathlib import Path
import socketserver
import threading
import time
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("hub_collect_metrics", ROOT / "collect.py")
COLLECT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COLLECT)
LIMIT = 4 * 1024**2


@contextmanager
def endpoint(body, encoding=None, drip=False, length_extra=0):
    requests = []
    stop = threading.Event()

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            self.connection.settimeout(2)
            request = bytearray()
            while len(request) < 8192:
                line = self.rfile.readline(1024)
                request.extend(line)
                if line in (b"\r\n", b""):
                    break
            requests.append(bytes(request))
            headers = (f"HTTP/1.1 200 OK\r\nContent-Length: {len(body) + length_extra}\r\n"
                       "Connection: close\r\n")
            if encoding is not None:
                headers += f"Content-Encoding: {encoding}\r\n"
            try:
                self.connection.sendall(headers.encode() + b"\r\n")
                if drip:
                    for byte in body:
                        self.connection.sendall(bytes([byte]))
                        if stop.wait(0.02):
                            break
                else:
                    self.connection.sendall(body)
            except OSError:
                pass

    class Server(socketserver.ThreadingTCPServer):
        daemon_threads = True

    with Server(("127.0.0.1", 0), Handler) as server:
        worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        worker.start()
        try:
            yield f"http://127.0.0.1:{server.server_address[1]}/metrics", requests
        finally:
            stop.set()
            server.shutdown()
            worker.join(timeout=2)


class MetricsHttpTests(unittest.TestCase):
    def setUp(self):
        # A developer's HTTP proxy must not intercept a loopback test fixture.
        self.proxies = mock.patch.dict(COLLECT.CONTROL.HTTP_PROXIES, {}, clear=True)
        self.proxies.start()
        self.addCleanup(self.proxies.stop)

    def test_identity_and_negotiated_gzip_preserve_all_metrics_bytes(self):
        metrics = (b'# HELP example synthetic metric\nexample{label="value"} 1\n' * 16000)
        for encoding in (None, "identity", "gzip", "GZip"):
            with self.subTest(encoding=encoding):
                body = gzip.compress(metrics, compresslevel=1, mtime=0) if encoding and encoding.lower() == "gzip" else metrics
                with endpoint(body, encoding) as (url, requests):
                    self.assertEqual(COLLECT.metrics_get(url, time.monotonic() + 2), metrics)
                    self.assertEqual(len(requests), 1)
                    self.assertIn(b"Accept-Encoding: gzip\r\n", requests[0])

    def test_exact_decoded_limit_is_accepted(self):
        metrics = b"a" * LIMIT
        for encoding in (None, "gzip"):
            with self.subTest(encoding=encoding):
                body = gzip.compress(metrics, compresslevel=1, mtime=0) if encoding else metrics
                with endpoint(body, encoding) as (url, _):
                    self.assertEqual(COLLECT.metrics_get(url, time.monotonic() + 2), metrics)

    def test_wire_limit_applies_before_identity_or_gzip_decoding(self):
        for encoding in (None, "gzip"):
            with self.subTest(encoding=encoding), endpoint(b"a" * (LIMIT + 1), encoding) as (url, _):
                with self.assertRaisesRegex(ValueError, "wire response exceeds 4 MiB"):
                    COLLECT.metrics_get(url, time.monotonic() + 2)

    def test_compressed_expansion_cannot_exceed_decoded_limit(self):
        body = gzip.compress(b"a" * (LIMIT + 1), compresslevel=1, mtime=0)
        with endpoint(body, "gzip") as (url, _):
            with self.assertRaisesRegex(ValueError, "decoded metrics response exceeds 4 MiB"):
                COLLECT.metrics_get(url, time.monotonic() + 2)

    def test_gzip_requires_complete_single_valid_member(self):
        valid = gzip.compress(b"example 1\n", compresslevel=1, mtime=0)
        corrupt = bytearray(valid)
        corrupt[-8] ^= 1
        cases = [valid[:-1], bytes(corrupt), valid + b"trailing", valid + valid, b"invalid"]
        for body in cases:
            with self.subTest(body=body), endpoint(body, "gzip") as (url, _):
                with self.assertRaisesRegex(ValueError, "metrics gzip response"):
                    COLLECT.metrics_get(url, time.monotonic() + 2)

    def test_complete_payload_cannot_hide_truncated_http_framing(self):
        metrics = b"example 1\n"
        for encoding in (None, "gzip"):
            body = gzip.compress(metrics, mtime=0) if encoding else metrics
            with self.subTest(encoding=encoding), endpoint(body, encoding, length_extra=1) as (url, _):
                with self.assertRaisesRegex(ValueError, "incomplete metrics HTTP response"):
                    COLLECT.metrics_get(url, time.monotonic() + 2)

    def test_unknown_or_stacked_content_encodings_fail_closed(self):
        for encoding in ("br", "gzip, gzip", "gzip, identity"):
            with self.subTest(encoding=encoding), endpoint(b"example 1\n", encoding) as (url, _):
                with self.assertRaisesRegex(ValueError, "unsupported metrics content encoding"):
                    COLLECT.metrics_get(url, time.monotonic() + 2)

    def test_dripping_body_cannot_renew_absolute_deadline(self):
        for encoding in (None, "gzip"):
            body = gzip.compress(b"example 1\n" * 100, mtime=0) if encoding else b"a" * 100
            with self.subTest(encoding=encoding), endpoint(body, encoding, drip=True) as (url, requests):
                start = time.monotonic()
                with self.assertRaises(TimeoutError) as raised:
                    COLLECT.metrics_get(url, start + 0.12)
                self.assertLess(time.monotonic() - start, 0.8)
                self.assertIn("metrics HTTP stage=body", raised.exception.__notes__)
                self.assertEqual(len(requests), 1)

    def test_decode_is_part_of_the_original_deadline(self):
        metrics = b"example 1\n"
        body = gzip.compress(metrics, mtime=0)
        original_remaining = COLLECT.CONTROL.remaining_timeout
        original_decoder = COLLECT.zlib.decompressobj
        decoded = False

        class Decoder:
            eof = True
            unused_data = b""
            unconsumed_tail = b""

            def decompress(self, data, limit):
                nonlocal decoded
                result = original_decoder(16 + COLLECT.zlib.MAX_WBITS).decompress(data, limit)
                decoded = True
                return result

        def remaining(deadline):
            if decoded:
                raise TimeoutError("synthetic decode exhausted deadline")
            return original_remaining(deadline)

        with endpoint(body, "gzip") as (url, _), \
             mock.patch.object(COLLECT.zlib, "decompressobj", return_value=Decoder()), \
             mock.patch.object(COLLECT.CONTROL, "remaining_timeout", side_effect=remaining):
            with self.assertRaisesRegex(TimeoutError, "decode exhausted") as raised:
                COLLECT.metrics_get(url, time.monotonic() + 2)
            self.assertIn("metrics HTTP stage=decode", raised.exception.__notes__)


if __name__ == "__main__":
    unittest.main()
