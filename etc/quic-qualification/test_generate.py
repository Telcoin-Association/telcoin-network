"""Deterministic generator regressions; no socket, no root, no network."""

import errno
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("generator", Path(__file__).with_name("generate.py"))
generator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(generator)

# RFC 791 header from the usual worked example: checksum field zeroed, expected 0xB861.
WIKIPEDIA_HEADER = bytes.fromhex("45000073000040004011" "0000" "c0a80001c0a800c7")
TEMPLATE = bytes([0xC3]) + b"\x00\x00\x00\x01" + b"\x00" * 1195


class FakeClock:
    """Monotonic clock advanced only by sleeps."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class PacketTests(unittest.TestCase):
    def test_checksum_matches_known_header(self):
        self.assertEqual(generator.ipv4_checksum(WIKIPEDIA_HEADER), 0xB861)

    def test_checksum_of_checksummed_header_is_zero(self):
        checksummed = WIKIPEDIA_HEADER[:10] + (0xB861).to_bytes(2, "big") + WIKIPEDIA_HEADER[12:]
        self.assertEqual(generator.ipv4_checksum(checksummed), 0)

    def test_odd_length_is_zero_padded(self):
        self.assertEqual(generator.ipv4_checksum(b"\x01"), (~0x0100) & 0xFFFF)

    def test_packet_layout(self):
        packet = generator.build_ipv4_udp("192.168.0.1", 1234, "192.168.0.199", 9000, b"x" * 8, 7)
        self.assertEqual(len(packet), 36)
        self.assertEqual(packet[0], 0x45)
        self.assertEqual(int.from_bytes(packet[2:4], "big"), 36)
        self.assertEqual(int.from_bytes(packet[4:6], "big"), 7)
        self.assertEqual(int.from_bytes(packet[6:8], "big"), 0x4000)
        self.assertEqual(packet[8], 64)
        self.assertEqual(packet[9], 17)
        self.assertEqual(generator.ipv4_checksum(packet[:20]), 0)
        self.assertEqual(packet[12:16], bytes([192, 168, 0, 1]))
        self.assertEqual(packet[16:20], bytes([192, 168, 0, 199]))
        self.assertEqual(int.from_bytes(packet[20:22], "big"), 1234)
        self.assertEqual(int.from_bytes(packet[22:24], "big"), 9000)
        self.assertEqual(int.from_bytes(packet[24:26], "big"), 16)
        self.assertEqual(packet[26:28], b"\x00\x00")
        self.assertEqual(packet[28:], b"x" * 8)

    def test_ident_wraps_to_sixteen_bits(self):
        packet = generator.build_ipv4_udp("10.0.0.1", 1, "10.0.0.2", 2, b"", 0x1_0005)
        self.assertEqual(int.from_bytes(packet[4:6], "big"), 5)

    def test_oversized_payload_rejected(self):
        with self.assertRaises(ValueError):
            generator.build_ipv4_udp("10.0.0.1", 1, "10.0.0.2", 2, b"\x00" * 65508, 1)

    def test_initial_template_checks(self):
        self.assertEqual(generator.check_initial_template(TEMPLATE), [])
        self.assertTrue(generator.check_initial_template(TEMPLATE[:100]))
        self.assertTrue(generator.check_initial_template(b"\x40" + TEMPLATE[1:]))
        self.assertTrue(generator.check_initial_template(bytes([0xC3]) + b"\x00" * 1199))


class SourceTests(unittest.TestCase):
    def test_pool_stays_inside_range_and_excludes_edges(self):
        pool = generator.source_pool("10.1.2.0/24", 500, seed=7)
        self.assertEqual(len(pool), 500)
        self.assertNotIn("10.1.2.0", pool)
        self.assertNotIn("10.1.2.255", pool)
        self.assertTrue(all(address.startswith("10.1.2.") for address in pool))
        self.assertEqual(pool, generator.source_pool("10.1.2.0/24", 500, seed=7))
        self.assertNotEqual(pool, generator.source_pool("10.1.2.0/24", 500, seed=8))

    def test_small_slash_30_uses_both_hosts_only(self):
        self.assertEqual(set(generator.source_pool("10.0.0.0/30", 64, seed=1)), {"10.0.0.1", "10.0.0.2"})

    def test_pool_rejects_ranges_without_hosts(self):
        with self.assertRaises(ValueError):
            generator.source_pool("10.0.0.0/31", 4, seed=1)

    def test_parse_sources_accepts_ip_and_ip_port(self):
        text = "10.0.0.1\n10.0.0.2:4001  # committee\n\n# trusted\n10.0.0.3\n"
        self.assertEqual(generator.parse_sources(text),
                         [("10.0.0.1", None), ("10.0.0.2", 4001), ("10.0.0.3", None)])

    def test_parse_sources_rejects_garbage(self):
        for text in ("garbage\n", "10.0.0.1:70000\n", "10.0.0.1:abc\n", "::1\n", "\n"):
            with self.assertRaises(ValueError, msg=text):
                generator.parse_sources(text)

    def test_parse_target(self):
        self.assertEqual(generator.parse_target("10.0.0.9:9000"), ("10.0.0.9", 9000))
        for text in ("[::1]:9000", "::1:9000", "10.0.0.9", "10.0.0.9:0", "host:9000"):
            with self.assertRaises(ValueError, msg=text):
                generator.parse_target(text)

    def test_parse_port_range(self):
        self.assertEqual(generator.parse_port_range("1024-65535"), (1024, 65535))
        for text in ("5000-4000", "0-10", "abc", "10"):
            with self.assertRaises(ValueError, msg=text):
                generator.parse_port_range(text)


class PacingTests(unittest.TestCase):
    def test_link_budget_boundary(self):
        self.assertEqual(generator.link_budget(100_000, 972, 1000, 0.8), (True, 800_000_000))
        self.assertEqual(generator.link_budget(100_001, 972, 1000, 0.8)[0], False)
        self.assertEqual(generator.link_budget(10, 1200, 1000, 0.8), (True, 98_240))

    def test_schedule_arithmetic(self):
        self.assertEqual(generator.schedule(100, 2), (1, 0.01, 200))
        self.assertEqual(generator.schedule(5000, 1.5), (5.0, 0.001, 1500))
        self.assertEqual(generator.schedule(1000, 1), (1, 0.001, 1000))
        for rate, duration in ((0, 1), (10, 0), (-1, 1)):
            with self.assertRaises(ValueError):
                generator.schedule(rate, duration)

    def test_run_pace_counts_sends_and_errno_failures(self):
        clock = FakeClock()
        sent = []

        def send(index):
            if index == 0:
                raise OSError(errno.EPERM, "denied")
            if index == 1:
                raise OSError(errno.ENOBUFS, "full")
            sent.append(index)

        state = generator.run_pace(send, (2, 0.001, 3), clock, clock.sleep)
        self.assertEqual(state["attempted_packets"], 6)
        self.assertEqual(state["offered_packets"], 4)
        self.assertEqual(state["send_errors"], {"EPERM": 1, "ENOBUFS": 1})
        self.assertEqual(sent, [2, 3, 4, 5])
        self.assertIn(0.001, clock.sleeps)
        self.assertAlmostEqual(state["elapsed_seconds"], 0.003)

    def test_run_pace_fractional_packets_per_tick(self):
        clock = FakeClock()
        count = []
        generator.run_pace(lambda index: count.append(index), (2.5, 0.001, 4), clock, clock.sleep)
        self.assertEqual(len(count), 10)

    def test_run_pace_keeps_partial_state_on_interrupt(self):
        clock = FakeClock()
        state = generator.pace_state()

        def send(index):
            if index == 3:
                raise KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            generator.run_pace(send, (1, 0.001, 10), clock, clock.sleep, state)
        self.assertEqual(state["attempted_packets"], 3)
        self.assertEqual(state["offered_packets"], 3)


class CommandLineTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.template = self.root / "initial.bin"
        self.template.write_bytes(TEMPLATE)
        self.sources = self.root / "sources.txt"
        self.sources.write_text("10.0.0.1\n10.0.0.2:4001\n")

    def tearDown(self):
        self.directory.cleanup()

    def common(self, output):
        return ["--target", "10.0.1.1:9000", "--rate", "100", "--duration", "1", "--link-mbps", "1000",
                "--seed", "3", "--dry-run", "--output", str(output)]

    def test_initial_dry_run(self):
        output = self.root / "out" / "initial.json"
        code = generator.main(["initial", "--template", str(self.template), "--source-range", "10.0.0.0/24",
                               "--source-count", "16"] + self.common(output))
        self.assertEqual(code, 0)
        summary = json.loads(output.read_text())
        self.assertEqual(summary["schema_version"], generator.SCHEMA_VERSION)
        self.assertEqual(summary["swarm"], "initial")
        self.assertEqual(summary["offered_packets"], 0)
        self.assertEqual(summary["ip_family"], 4)
        self.assertEqual(summary["payload_bytes"], 1200)
        self.assertEqual(summary["template_sha256"], hashlib.sha256(TEMPLATE).hexdigest())
        self.assertEqual(summary["source_population"]["kind"], "range")
        self.assertEqual(summary["schedule"], {"packets_per_tick": 1, "tick_seconds": 0.01, "ticks": 100})
        self.assertTrue(any(note.startswith("dry_run") for note in summary["notes"]))
        self.assertIsNone(summary["offered_rate_pps"])

    def test_peerlist_dry_run(self):
        output = self.root / "peerlist.json"
        code = generator.main(["peerlist", "--sources", str(self.sources), "--size", "64"] + self.common(output))
        self.assertEqual(code, 0)
        summary = json.loads(output.read_text())
        self.assertEqual(summary["swarm"], "peerlist")
        self.assertEqual(summary["payload_bytes"], 64)
        self.assertEqual(summary["source_population"], {"kind": "file", "count": 2, "file": str(self.sources)})
        self.assertIsNone(summary["template_sha256"])

    def test_ipv6_target_exits_2(self):
        output = self.root / "v6.json"
        argv = ["peerlist", "--sources", str(self.sources)] + self.common(output)
        argv[argv.index("10.0.1.1:9000")] = "[::1]:9000"
        self.assertEqual(generator.main(argv), 2)
        self.assertFalse(output.exists())

    def test_link_budget_exits_2(self):
        output = self.root / "budget.json"
        argv = ["peerlist", "--sources", str(self.sources), "--size", "1400"] + self.common(output)
        argv[argv.index("--rate") + 1] = "1000000"
        self.assertEqual(generator.main(argv), 2)
        self.assertFalse(output.exists())

    def test_short_template_exits_2(self):
        short = self.root / "short.bin"
        short.write_bytes(TEMPLATE[:400])
        output = self.root / "short.json"
        argv = ["initial", "--template", str(short), "--source-range", "10.0.0.0/24"] + self.common(output)
        self.assertEqual(generator.main(argv), 2)

    def test_missing_sources_file_exits_2(self):
        output = self.root / "missing.json"
        argv = ["peerlist", "--sources", str(self.root / "absent.txt")] + self.common(output)
        self.assertEqual(generator.main(argv), 2)


if __name__ == "__main__":
    unittest.main()
