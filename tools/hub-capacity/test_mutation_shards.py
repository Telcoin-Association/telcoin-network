"""Exercise real shard/aggregate control flow with mocked Cargo outcomes and hostile evidence."""

from contextlib import redirect_stdout
import ast
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("capacity_mutation_aggregate", Path(__file__).with_name("aggregate-mutations.py"))
AGGREGATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AGGREGATE)
MUTATIONS = AGGREGATE.MUTATIONS
ROOT = Path(__file__).resolve().parents[2]


class ShardTests(unittest.TestCase):
    def test_two_shards_are_the_exact_disjoint_75_case_partition(self):
        first = MUTATIONS.select_cases(0, 2)
        second = MUTATIONS.select_cases(1, 2)
        self.assertEqual((len(first), len(second)), (38, 37))
        self.assertEqual(first, MUTATIONS.CASES[::2])
        self.assertEqual(second, MUTATIONS.CASES[1::2])
        self.assertEqual(len(set(case[0] for case in first + second)), 75)
        self.assertEqual(MUTATIONS.select_cases(), MUTATIONS.CASES)
        with patch.object(MUTATIONS, "CASES", MUTATIONS.CASES[:5]):
            self.assertEqual([len(MUTATIONS.select_cases(i, 2)) for i in range(2)], [3, 2])

    def test_invalid_partition_or_duplicate_registry_is_rejected(self):
        for index, count in ((0, 0), (-1, 2), (2, 2), (0, 76), (0, -1)):
            with self.subTest(index=index, count=count), self.assertRaisesRegex(ValueError, "shard"):
                MUTATIONS.select_cases(index, count)
        with patch.object(MUTATIONS, "CASES", [MUTATIONS.CASES[0]] * 2):
            with self.assertRaisesRegex(ValueError, "duplicate"):
                MUTATIONS.select_cases()

    def test_provenance_rejects_wrong_checkout_dirty_source_and_bad_run_identity(self):
        source = "a" * 40
        with patch.object(MUTATIONS.subprocess, "check_output", side_effect=[source.encode(), b"b" * 40]), \
                patch.object(MUTATIONS.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)):
            proof = MUTATIONS.provenance(source, "123", "2")
        self.assertEqual(proof["source"], source)
        self.assertEqual(proof["tree"], "b" * 40)
        for head, status in (("c" * 40, 0), (source, 1)):
            with patch.object(MUTATIONS.subprocess, "check_output", side_effect=[head.encode(), b"b" * 40]), \
                    patch.object(MUTATIONS.subprocess, "run", return_value=subprocess.CompletedProcess([], status)):
                with self.assertRaisesRegex(ValueError, "clean expected"):
                    MUTATIONS.provenance(source, "123", "2")
        for values in ((source, "0", "1"), (source, "1", "0"), (source, "x", "1"), ("bad", "1", "1")):
            with self.subTest(values=values), self.assertRaisesRegex(ValueError, "provenance"):
                MUTATIONS.provenance(*values)

    def test_sharding_cannot_start_without_provenance(self):
        with patch.object(sys, "argv", ["mutate-rust.py", "--output", "unused", "--shard-count", "2"]), \
                patch.object(MUTATIONS, "execute", side_effect=AssertionError("Cargo must not run without provenance")):
            with self.assertRaisesRegex(ValueError, "requires source/run"):
                MUTATIONS.main()


class AggregateTests(unittest.TestCase):
    def fixture(self, temporary):
        root = Path(temporary) / "checkout"
        paths = {Path(case[1]) for case in MUTATIONS.CASES}
        paths.add(Path("Cargo.toml"))
        for case in MUTATIONS.CASES:
            source = ROOT / case[1]
            paths.update(parent.relative_to(ROOT) / "Cargo.toml" for parent in source.parents
                         if parent.is_relative_to(ROOT) and (parent / "Cargo.toml").is_file())
        for relative in paths:
            destination = root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes((ROOT / relative).read_bytes())
        expected = {"source": "a" * 40, "tree": "b" * 40, "run_id": "123", "run_attempt": "2",
                    "registry_sha256": hashlib.sha256(Path(MUTATIONS.__file__).read_bytes()).hexdigest(),
                    "case_set_sha256": hashlib.sha256(json.dumps(MUTATIONS.CASES, separators=(",", ":")).encode()).hexdigest()}
        directories = [Path(temporary) / f"shard-{index}" for index in range(2)]
        calls = []
        with patch.object(MUTATIONS, "ROOT", root), patch.object(MUTATIONS, "provenance", return_value=expected):
            for index, directory in enumerate(directories):
                selected = MUTATIONS.select_cases(index, 2)
                shard_calls = []

                def cargo(argv, *, cwd, capture_output, timeout):
                    case = selected[len(shard_calls) // 3]
                    stage = len(shard_calls) % 3
                    name, relative, before, after, regression = case
                    package, compilation, test = MUTATIONS.mutation_commands(relative, regression)
                    self.assertEqual(argv, compilation if stage == 1 else test)
                    self.assertEqual((cwd, capture_output, timeout), (root, True, 1800))
                    content = (root / relative).read_text()
                    original = (ROOT / relative).read_text()
                    self.assertEqual(content, original if stage == 0 else original.replace(before, after))
                    raw = f"Finished test profile\n{'PASS' if stage == 0 else 'FAIL'} tests::{regression}\n".encode()
                    shard_calls.append((name, stage))
                    return subprocess.CompletedProcess(argv, 100 if stage == 2 else 0, raw, b"")

                argv = ["mutate-rust.py", "--output", str(directory), "--shard-index", str(index),
                        "--shard-count", "2", "--source", expected["source"], "--run-id", "123", "--run-attempt", "2"]
                with patch.object(sys, "argv", argv), patch.object(MUTATIONS.subprocess, "run", side_effect=cargo), redirect_stdout(io.StringIO()):
                    MUTATIONS.main()
                self.assertEqual(len(shard_calls), (114, 111)[index])
                calls.extend(shard_calls)
        self.assertEqual(len(calls), 225)
        self.assertEqual(len(set(name for name, stage in calls)), 75)
        return root, directories, expected

    def rewrite(self, path, change):
        value = json.loads(path.read_text())
        change(value)
        path.write_text(json.dumps(value))

    def reject(self, change, match=None):
        with tempfile.TemporaryDirectory() as temporary:
            root, directories, expected = self.fixture(temporary)
            change(directories, expected)
            output = Path(temporary) / "aggregate"
            with patch.object(MUTATIONS, "ROOT", root):
                with self.assertRaisesRegex(ValueError, match or "."):
                    AGGREGATE.aggregate(directories, output, expected, "success")
            self.assertFalse(output.exists(), "unverified proofs must not be published")

    def test_real_producer_loop_reconciles_all_75_cases_and_225_logs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, directories, expected = self.fixture(temporary)
            output = Path(temporary) / "aggregate"
            with patch.object(MUTATIONS, "ROOT", root):
                summary = AGGREGATE.aggregate(list(reversed(directories)), output, expected, "success")
            self.assertEqual((summary["case_count"], summary["log_count"], summary["complete"]), (75, 225, True))
            self.assertEqual(len(list(output.glob("*.log"))), 225)
            rows = json.loads((output / "report.json").read_text())
            self.assertEqual([row["mutation"] for row in rows], [case[0] for case in MUTATIONS.CASES])
            self.assertTrue(all(row["source_sha256"] == row["restored_sha256"] for row in rows))

    def test_stale_provenance_and_wrong_partition_are_rejected(self):
        for key in ("source", "tree", "registry_sha256", "case_set_sha256", "run_id", "run_attempt"):
            with self.subTest(key=key):
                self.reject(lambda dirs, expected: self.rewrite(dirs[0] / "manifest.json",
                            lambda value: value["provenance"].update({key: "stale"})), "provenance")
        for key, value in (("complete", False), ("complete", 1), ("shard_count", 1), ("shard_index", 1),
                           ("version", True), ("cases", [])):
            with self.subTest(key=key, value=value):
                self.reject(lambda dirs, expected: self.rewrite(dirs[0] / "manifest.json",
                            lambda current: current.update({key: value})))

    def test_missing_duplicate_extra_or_skipped_cases_and_logs_are_rejected(self):
        edits = [lambda rows: rows.pop(), lambda rows: rows.append(rows[0]),
                 lambda rows: rows.__setitem__(1, rows[0]),
                 lambda rows: rows[0].update({"skipped": True}),
                 lambda rows: rows[0].update({"mutation": "extra"}),
                 lambda rows: rows[0].update({"detected": False})]
        for edit in edits:
            with self.subTest(edit=edit):
                self.reject(lambda dirs, expected: self.rewrite(dirs[0] / "report.json", edit))
        self.reject(lambda dirs, expected: next(dirs[0].glob("*.log")).unlink(), "log set")
        self.reject(lambda dirs, expected: (dirs[0] / "extra.log").write_text("orphan"), "log set")

    def test_command_identity_exit_codes_and_source_restoration_are_required(self):
        for key in ("source_sha256", "restored_sha256", "mutated_sha256", "package", "path", "regression"):
            with self.subTest(key=key):
                self.reject(lambda dirs, expected: self.rewrite(dirs[0] / "report.json",
                            lambda rows: rows[0].update({key: "wrong"})))
        for stage in ("control", "compilation", "test"):
            for key, value in (("exit_code", -1), ("exit_code", False), ("argv", []), ("log", "../outside"),
                               ("sha256", "wrong")):
                with self.subTest(stage=stage, key=key):
                    self.reject(lambda dirs, expected: self.rewrite(dirs[0] / "report.json",
                                lambda rows: rows[0][stage].update({key: value})))

    def test_raw_log_tampering_and_hash_consistent_false_outcomes_are_rejected(self):
        def change(dirs, expected, stage, raw, update_hash):
            report = dirs[0] / "report.json"
            rows = json.loads(report.read_text())
            receipt = rows[0][stage]
            (dirs[0] / receipt["log"]).write_bytes(raw)
            if update_hash:
                receipt["sha256"] = hashlib.sha256(raw).hexdigest()
                report.write_text(json.dumps(rows))
        def stale_hash(dirs, expected):
            rows = json.loads((dirs[0] / "report.json").read_text())
            path = dirs[0] / rows[0]["test"]["log"]
            path.write_bytes(path.read_bytes() + b"\nadditional diagnostic\n")
        self.reject(stale_hash, "hash")
        for stage, raw in (("control", b"PASS unrelated\n"), ("test", b"FAIL unrelated\n"),
                           ("compilation", b"compiler error\n")):
            with self.subTest(stage=stage):
                self.reject(lambda dirs, expected: change(dirs, expected, stage, raw, True))

    def test_missing_failed_cancelled_or_skipped_shards_are_not_proofs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, directories, expected = self.fixture(temporary)
            with patch.object(MUTATIONS, "ROOT", root):
                for status in ("failure", "cancelled", "skipped", ""):
                    with self.subTest(status=status), self.assertRaisesRegex(ValueError, "succeed"):
                        AGGREGATE.aggregate(directories, Path(temporary) / "aggregate", expected, status)
                for inputs in (directories[:1], directories + directories[:1], [directories[0]] * 2):
                    with self.subTest(count=len(inputs)), self.assertRaises(ValueError):
                        AGGREGATE.aggregate(inputs, Path(temporary) / "aggregate", expected, "success")

    def test_duplicate_json_fields_and_nonregular_or_oversized_evidence_are_rejected(self):
        def duplicate_field(dirs, expected):
            path = dirs[0] / "manifest.json"
            path.write_text(path.read_text().rstrip().removesuffix("}") + ',"version":1}')
        self.reject(duplicate_field, "duplicate")
        self.reject(lambda dirs, expected: (dirs[0] / "manifest.json").write_text('{"version":NaN}'), "nonfinite")
        def symlink(dirs, expected):
            path = next(dirs[0].glob("*.log"))
            saved = path.read_bytes()
            path.unlink()
            target = dirs[0].parent / "outside.log"
            target.write_bytes(saved)
            path.symlink_to(target)
        self.reject(symlink, "nonregular")
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bounded"
            path.write_bytes(b"12345")
            with self.assertRaisesRegex(ValueError, "byte bound"):
                AGGREGATE.read_file(path, 4)


class WorkflowTests(unittest.TestCase):
    def job(self, name):
        workflow = (ROOT / ".github/workflows/pr.yaml").read_text()
        return re.search(r"^  " + re.escape(name) + r":\n(.*?)(?=^  [a-z][\w-]*:\n|\Z)", workflow, re.M | re.S).group(1)

    def test_full_code_lane_and_existing_limits_remain(self):
        body = self.job("hub-capacity")
        for command in ("cargo +1.94 test --locked --workspace --no-run", "cargo +1.94 build --locked -p telcoin-network -p tn-node-record-api",
                        "cargo +1.94 nextest run --locked --workspace --exclude tn-faucet",
                        "cargo +1.94 clippy --locked --workspace --all-targets --keep-going -- -D warnings"):
            self.assertIn(command, body)
        self.assertIn("timeout-minutes: 120", body)
        self.assertNotIn("mutate-rust.py", body)
        shard = self.job("hub-capacity-mutation-shards")
        for text in ("needs: hub-capacity", "qualification_required == 'true'", "shard: [0, 1]", "fail-fast: false",
                     "timeout-minutes: 120", "CARGO_BUILD_JOBS: 2", "CARGO_INCREMENTAL: 0", "--shard-count 2",
                     "--source", "--run-id", "--run-attempt", "if: always()", 'save-if: "false"'):
            self.assertIn(text, shard)

    def test_qualification_and_exact_pr_gate_require_aggregate_success(self):
        self.assertIn("needs: [hub-capacity, hub-capacity-mutations]", self.job("hub-capacity-qualification"))
        gate = self.job("ci-success")
        self.assertIn("hub-capacity-mutations", gate.split("    steps:", 1)[0])
        self.assertIn("hub-capacity-mutations", re.search(r'expected="([^"]+)"', gate).group(1))
        self.assertIn("*:hub-capacity-mutations:success", gate)
        self.assertIn("*:hub-capacity-mutations:*", gate)
        body = self.job("hub-capacity-mutations")
        self.assertIn("needs: [hub-capacity, hub-capacity-mutation-shards]", body)
        self.assertIn('test "$CODE_RESULT" = success', body)
        self.assertIn('true) test "$SHARDS_RESULT" = success', body)
        self.assertIn('false) test "$SHARDS_RESULT" = skipped', body)
        self.assertIn("--shards-result", body)
        self.assertEqual(body.count("actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"), 2)

    def test_real_scope_guard_rejects_partial_shards_and_preserves_unchanged_scope(self):
        body = self.job("hub-capacity-mutations")
        script = textwrap.dedent(body.split("        run: |\n", 1)[1].split("      - name:", 1)[0])
        for code, required, shards, expected in (("success", "true", "success", 0),
                                                ("success", "false", "skipped", 0),
                                                ("failure", "false", "skipped", 1),
                                                ("success", "", "success", 1)):
            with self.subTest(code=code, required=required, shards=shards):
                result = subprocess.run(["bash", "-c", script], capture_output=True,
                                        env={"CODE_RESULT": code, "REQUIRED": required, "SHARDS_RESULT": shards})
                self.assertEqual(result.returncode, expected)
        for shards in ("failure", "cancelled", "skipped", ""):
            with self.subTest(shards=shards):
                result = subprocess.run(["bash", "-c", script], capture_output=True,
                                        env={"CODE_RESULT": "success", "REQUIRED": "true", "SHARDS_RESULT": shards})
                self.assertNotEqual(result.returncode, 0)


class ManualWorkflowTests(unittest.TestCase):
    JOBS = ("hub-capacity", "hub-capacity-mutation-shards", "hub-capacity-mutations", "hub-capacity-qualification")

    def workflow(self, name):
        return (ROOT / ".github/workflows" / name).read_text()

    def job(self, workflow, name):
        return re.search(r"^  " + re.escape(name) + r":\n(.*?)(?=^  [a-z][\w-]*:\n|\Z)", workflow, re.M | re.S).group(1)

    def condition(self, body, event, capacity):
        expression = re.search(r"^    if: \$\{\{ (.*?) \}\}$", body, re.M).group(1)
        expression = expression.replace("github.event_name", repr(event)).replace("inputs.hub_capacity", repr(capacity))
        expression = re.sub(r"!(?!=)", "not ", expression.replace("&&", " and ").replace("||", " or "))
        tree = ast.parse(expression, mode="eval")

        def value(node):
            if isinstance(node, ast.Constant):
                return node.value
            if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
                return not value(node.operand)
            if isinstance(node, ast.BoolOp):
                values = [value(item) for item in node.values]
                return all(values) if isinstance(node.op, ast.And) else any(values)
            if isinstance(node, ast.Compare) and len(node.ops) == 1:
                equal = value(node.left) == value(node.comparators[0])
                return equal if isinstance(node.ops[0], ast.Eq) else not equal
            raise AssertionError("unsupported workflow routing expression")

        return value(tree.body)

    def test_dispatch_is_opt_in_and_schedules_keep_durable_job(self):
        caller = self.workflow("durable-e2e.yaml")
        self.assertRegex(caller, r"hub_capacity:\n        description: [^\n]+\n        type: boolean\n        default: false\n")
        manual = self.job(caller, "hub-capacity-manual")
        durable = self.job(caller, "durable-e2e")
        self.assertIn("uses: ./.github/workflows/hub-capacity-manual.yaml", manual)
        self.assertIn("permissions:\n      contents: read", manual)
        self.assertNotIn("secrets:", caller)
        for event, capacity, expected in (("schedule", False, False), ("schedule", True, False),
                                          ("workflow_dispatch", False, False), ("workflow_dispatch", True, True)):
            with self.subTest(event=event, capacity=capacity):
                self.assertEqual(self.condition(manual, event, capacity), expected)
                self.assertEqual(self.condition(durable, event, capacity), not expected)

    def test_every_manual_job_runs_and_uses_distinct_check_names(self):
        workflow = self.workflow("hub-capacity-manual.yaml")
        self.assertIn("on:\n  workflow_call:\n  workflow_dispatch:\n", workflow)
        self.assertIn("permissions:\n  contents: read", workflow)
        self.assertNotIn("inputs:", workflow)
        self.assertNotIn("CI Success", workflow)
        self.assertEqual(tuple(re.findall(r"^  ([a-z][\w-]*):$", workflow.split("jobs:\n", 1)[1], re.M)), self.JOBS)
        names = []
        for job in self.JOBS:
            body = self.job(workflow, job)
            names.append(re.search(r"^    name: (.+)$", body, re.M).group(1))
            for condition in re.findall(r"^\s+if: (.+)$", body, re.M):
                self.assertEqual(condition, "always()")
        self.assertEqual(len(set(names)), len(self.JOBS))
        self.assertTrue(all(name.startswith("Manual hub capacity ") for name in names))
        self.assertIn('qualification_required: "true"', workflow)
        self.assertNotIn("steps.scope", workflow)
        self.assertNotIn("Detect capacity source changes", workflow)
        self.assertEqual(workflow.count("ref: ${{ github.sha }}"), 4)
        self.assertNotIn("github.event.pull_request", workflow)

    def test_manual_retains_required_commands_pins_resources_and_artifacts(self):
        original = self.workflow("pr.yaml")
        manual = self.workflow("hub-capacity-manual.yaml")
        self.assertEqual(original.split("\nenv:\n", 1)[1].split("\njobs:\n", 1)[0].rstrip(),
                         manual.split("\nenv:\n", 1)[1].split("\njobs:\n", 1)[0].rstrip())
        for job in self.JOBS:
            with self.subTest(job=job):
                source = self.job(original, job)
                copied = self.job(manual, job)
                source = re.sub(r"      - name: Detect capacity source changes\n.*?(?=      - name:)", "", source, flags=re.S)
                source = source.replace("${{ steps.scope.outputs.changed }}", '"true"')
                source = source.replace("${{ github.event.pull_request.head.sha || github.sha }}", "${{ github.sha }}")
                source = source.replace("always() && needs.hub-capacity.outputs.qualification_required == 'true'", "always()")
                source = re.sub(r"^\s+if: (?:steps.scope.outputs.changed|needs.hub-capacity.outputs.qualification_required) == 'true'\n", "\n", source, flags=re.M)
                copied = re.sub(r"^    name: [^\n]+\n", "", copied, count=1, flags=re.M)

                def meaningful_lines(body):
                    return [line for line in body.splitlines() if line.strip() and not line.lstrip().startswith("#")]

                self.assertEqual(meaningful_lines(copied), meaningful_lines(source))

    def test_manual_reconciliation_rejects_every_incomplete_shard_outcome(self):
        body = self.job(self.workflow("hub-capacity-manual.yaml"), "hub-capacity-mutations")
        script = textwrap.dedent(body.split("        run: |\n", 1)[1].split("      - name:", 1)[0])
        for code, shards, expected in (("success", "success", 0), ("failure", "success", 1),
                                      ("success", "failure", 1), ("success", "cancelled", 1),
                                      ("success", "skipped", 1), ("success", "", 1)):
            with self.subTest(code=code, shards=shards):
                result = subprocess.run(["bash", "-c", script], capture_output=True,
                                        env={"CODE_RESULT": code, "REQUIRED": "true", "SHARDS_RESULT": shards})
                self.assertEqual(result.returncode, expected)


if __name__ == "__main__":
    unittest.main()
