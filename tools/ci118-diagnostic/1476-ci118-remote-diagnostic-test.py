"""In-memory rejection checks for the bounded CI118 report reader."""
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import struct
import unittest
from unittest.mock import patch
import warnings
import zipfile

spec = importlib.util.spec_from_file_location("diagnostic", Path(__file__).with_name("1476-ci118-remote-diagnostic-reader.py"))
reader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reader)


def report():
    scenario = {"attempts": 100, "cancelled": 2, "success_rate": 0.98, "p99_ms": 23.5}
    return {"plan_sha256": "b" * 64,
            "baseline": {"passed": True, "failures": [], "scenarios": {"sync": scenario}},
            "candidate": {"passed": False, "failures": ["sync: workload threshold exceeded"],
                          "scenarios": {"sync": dict(scenario)}}}


def archive(entries=None):
    buffer = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zipped:
            for name, data in entries or [("report.json", json.dumps(report()).encode())]:
                zipped.writestr(name, data)
    return buffer.getvalue()


def official_metadata(raw):
    value = {"id": reader.ARTIFACT_ID, "size_in_bytes": len(raw),
             "digest": "sha256:" + hashlib.sha256(raw).hexdigest(), "expired": False,
             "expires_at": "2027-01-07T09:58:18Z",
             "name": "hub-capacity-evidence-" + reader.SOURCE_HEAD + "-attempt-1",
             "archive_download_url": "https://api.github.com/repos/" + reader.REPOSITORY
             + "/actions/artifacts/" + str(reader.ARTIFACT_ID) + "/zip",
             "workflow_run": {"id": reader.SOURCE_RUN, "head_sha": reader.SOURCE_HEAD,
                              "repository_id": 780459444, "head_repository_id": 780459444,
                              "head_branch": "feat/1476-public-hub-capacity"}}
    return json.dumps(value).encode()


class DiagnosticTests(unittest.TestCase):
    def diagnose(self, raw, metadata=None, modified_raw=None, limits=None):
        metadata = official_metadata(raw) if metadata is None else metadata
        bindings = {"EXPECTED_SIZE": len(raw), "ARCHIVE_SHA256": hashlib.sha256(raw).hexdigest(),
                    "METADATA_SHA256": hashlib.sha256(metadata).hexdigest()}
        bindings.update(limits or {})
        with patch.multiple(reader, **bindings):
            return reader.diagnose(metadata, io.BytesIO(raw if modified_raw is None else modified_raw))

    def assert_status(self, result, status, reason):
        self.assertEqual(result["report_status"], status)
        self.assertIn(reason, result["reason"])
        self.assertFalse(result["capacity_qualified"])
        self.assertFalse(result["independently_rescored"])
        self.assertNotIn("ci_report", result)

    def test_preserves_full_report_without_rescoring(self):
        result = self.diagnose(archive())
        self.assertEqual(result["report_status"], "present")
        self.assertEqual(result["ci_report"], report())
        self.assertTrue(result["archive_authenticated"])
        self.assertTrue(result["diagnostic_only"])
        self.assertFalse(result["capacity_qualified"])
        self.assertFalse(result["independently_rescored"])

    def test_missing_report_does_not_invent_threshold_failure(self):
        self.assert_status(self.diagnose(archive([("candidate/log.txt", b"log")])), "missing", "no report.json")

    def test_metadata_byte_tamper(self):
        raw = archive()
        metadata = official_metadata(raw)
        with patch.multiple(reader, EXPECTED_SIZE=len(raw), ARCHIVE_SHA256=hashlib.sha256(raw).hexdigest(),
                            METADATA_SHA256=hashlib.sha256(metadata).hexdigest()):
            self.assert_status(reader.diagnose(metadata + b" ", io.BytesIO(raw)), "archive_invalid", "metadata byte hash")

    def test_wrong_source_binding(self):
        raw = archive()
        metadata = json.loads(official_metadata(raw))
        metadata["workflow_run"]["head_sha"] = "0" * 40
        self.assert_status(self.diagnose(raw, json.dumps(metadata).encode()), "archive_invalid", "source binding")

    def test_whole_archive_tamper(self):
        raw = archive()
        changed = bytearray(raw)
        changed[40] ^= 1
        self.assert_status(self.diagnose(raw, modified_raw=bytes(changed)), "archive_invalid", "SHA256")

    def test_archive_size_tamper(self):
        raw = archive()
        self.assert_status(self.diagnose(raw, modified_raw=raw + b"x"), "archive_invalid", "byte count")

    def test_duplicate_members(self):
        raw = archive([("report.json", b"{}"), ("report.json", b"{}")])
        self.assert_status(self.diagnose(raw), "archive_invalid", "duplicate archive member")

    def test_multiple_reports_in_different_directories(self):
        raw = archive([("baseline/report.json", b"{}"), ("candidate/report.json", b"{}")])
        self.assert_status(self.diagnose(raw), "archive_invalid", "multiple report.json")

    def test_unsafe_member_names(self):
        for name in ("../log.txt", "/log.txt", "a\\log.txt", "a//log.txt", "a/./log.txt", "C:log.txt"):
            with self.subTest(name=name):
                self.assert_status(self.diagnose(archive([(name, b"x")])), "archive_invalid", "unsafe archive member")

    def test_member_count_and_expanded_bounds(self):
        for limits, reason in (({"MAX_MEMBERS": 1}, "member count"),
                               ({"MAX_MEMBER": 1}, "member byte"),
                               ({"MAX_EXPANDED": 1}, "expanded archive"),
                               ({"MAX_CENTRAL": 1}, "central directory")):
            with self.subTest(limits=limits):
                self.assert_status(self.diagnose(archive([("log.txt", b"xx"), ("report.json", b"{}")]), limits=limits),
                                   "archive_invalid", reason)

    def test_report_bound(self):
        self.assert_status(self.diagnose(archive(), limits={"MAX_REPORT": 10}), "invalid", "report byte bound")

    def test_crc_rejection_after_authentication(self):
        raw = bytearray(archive())
        central = raw.index(b"PK\x01\x02")
        crc = struct.unpack_from("<I", raw, central + 16)[0] ^ 1
        struct.pack_into("<I", raw, 14, crc)
        struct.pack_into("<I", raw, central + 16, crc)
        self.assert_status(self.diagnose(bytes(raw)), "invalid", "CRC")

    def test_local_header_mismatch(self):
        raw = bytearray(archive())
        raw[30] ^= 1
        self.assert_status(self.diagnose(bytes(raw)), "archive_invalid", "member name mismatch")

    def test_symlink_member(self):
        buffer = io.BytesIO()
        member = zipfile.ZipInfo("report.json")
        member.create_system = 3
        member.external_attr = 0o120777 << 16
        with zipfile.ZipFile(buffer, "w") as zipped:
            zipped.writestr(member, b"target")
        self.assert_status(self.diagnose(buffer.getvalue()), "archive_invalid", "non-regular archive member")

    def test_invalid_json_and_duplicate_keys(self):
        for raw in (b"\xff", b"{", b'{"plan_sha256":"a","plan_sha256":"b"}', b'{"x":NaN}'):
            with self.subTest(raw=raw):
                self.assert_status(self.diagnose(archive([("report.json", raw)])), "invalid", "JSON")

    def test_schema_rejection(self):
        variants = []
        for edit in (lambda value: value.update(extra=1),
                     lambda value: value["candidate"].update(passed=True),
                     lambda value: value["candidate"].update(failures=[1]),
                     lambda value: value["candidate"]["scenarios"]["sync"].update(attempts=True),
                     lambda value: value["candidate"]["scenarios"]["sync"].update(success_rate=1.1)):
            value = copy.deepcopy(report())
            edit(value)
            variants.append(value)
        for value in variants:
            with self.subTest(value=value):
                result = self.diagnose(archive([("report.json", json.dumps(value).encode())]))
                self.assertEqual(result["report_status"], "invalid")
                self.assertNotIn("ci_report", result)

    def test_output_bound(self):
        self.assert_status(self.diagnose(archive(), limits={"MAX_SUMMARY": 4100}), "invalid", "summary byte bound")

    def test_no_member_other_than_report_is_opened(self):
        original = zipfile.ZipFile.open
        opened = []
        raw = archive([("log.txt", b"unread"), ("report.json", json.dumps(report()).encode())])

        def observe(zipped, member, *args, **kwargs):
            opened.append(member.filename)
            return original(zipped, member, *args, **kwargs)

        with patch.object(zipfile.ZipFile, "open", observe):
            result = self.diagnose(raw)
        self.assertEqual(result["report_status"], "present")
        self.assertEqual(opened, ["report.json"])


if __name__ == "__main__":
    unittest.main()
