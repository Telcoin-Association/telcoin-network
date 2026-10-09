#!/usr/bin/env python3
"""Verify authenticated qualification ZIPs without extracting raw observations.

Artifact API records and the actual three-binary hash map are independently
authenticated caller inputs. This tool has no network access and executes only
pinned local helpers and qualification source independently verified in Git.
Verification of a failed run is not qualification or permission to publish it.
"""

import argparse
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import types
import zipfile


BUFFER_BYTES = 1024 * 1024
MAX_STDIN_BYTES = 128 * 1024**2
MAX_JSON_BYTES = 16 * 1024**2
MAX_EVIDENCE_BYTES = 24 * 1024**2
MAX_SOURCE_BYTES = 1024 * 1024
MAX_CENTRAL_BYTES = 16 * 1024**2
MAX_MEMBERS = 4096
MAX_EXPANDED_BYTES = 8 * 1024**3
MAX_MEMBER_BYTES = 512 * 1024**2
MIN_FREE_BYTES = 30 * 1024**3
PUBLICATION_NAMES = ("plan.json", "report.json", "baseline.tar.gz", "candidate.tar.gz",
                     "inputs.tar.gz", "README.md", "SHA256SUMS")
CHECKSUM_NAMES = tuple(sorted(PUBLICATION_NAMES[:-1]))
MAX_PUBLIC_GZIP_BYTES = 100 * 1024**2 - 1
MAX_PUBLIC_README_BYTES = 1024**2
PUBLIC_CHECKSUM_BYTES = 475
PUBLIC_ALLOCATION_SLACK = 16 * 1024**2
REPOSITORY = "Telcoin-Association/telcoin-network"
ROOT_TOKEN = "@1476-verified-zip-root"
PUBLISHER_PATH = Path(__file__).with_name("1476-ci119-publish-evidence.proposed.py")
PUBLISHER_SHA256 = "a9cad66e1b86a2d0bd0e465e23a150f5790214b0daafaf643ed55bb4f57c8348"
BINDINGS_PATH = Path(__file__).with_name("1476-ci110-verify-qualified-bindings-v2.proposed.py")
BINDINGS_SHA256 = "75d9a1285b78384b5db0e7aac1f344a527b6c7206f61949f82b8144c55b77a20"
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def regular_path(path):
    require(not any(item.is_symlink() for item in (path, *path.parents)),
            f"symlink input: {path}")
    require(stat.S_ISREG(path.stat().st_mode), f"input is not a regular file: {path}")


def load_helper(path, expected, name):
    regular_path(path)
    require(path.stat().st_size <= MAX_SOURCE_BYTES, "trusted helper exceeds source cap")
    with path.open("rb") as source:
        raw = source.read(MAX_SOURCE_BYTES + 1)
    require(len(raw) <= MAX_SOURCE_BYTES, "trusted helper exceeds source cap")
    require(hashlib.sha256(raw).hexdigest() == expected, f"trusted helper pin mismatch: {path}")
    module = types.ModuleType(name)
    module.__file__ = str(path)
    exec(compile(raw, str(path), "exec"), module.__dict__)
    return module


def trusted_helpers():
    return (load_helper(PUBLISHER_PATH, PUBLISHER_SHA256, "trusted_publisher"),
            load_helper(BINDINGS_PATH, BINDINGS_SHA256, "trusted_bindings"))


def stream_digest(stream, maximum=MAX_EXPANDED_BYTES):
    require(type(maximum) is int and 0 <= maximum <= MAX_EXPANDED_BYTES,
            "invalid compressed read cap")
    stream.seek(0)
    digest = hashlib.sha256()
    size = 0
    for chunk in iter(lambda: stream.read(min(BUFFER_BYTES, maximum - size + 1)), b""):
        size += len(chunk)
        require(size <= maximum, "compressed input exceeds authenticated read cap")
        digest.update(chunk)
    stream.seek(0)
    return size, digest.hexdigest()


def bounded_stdin(source, maximum=MAX_STDIN_BYTES):
    output = io.BytesIO()
    size = 0
    for chunk in iter(lambda: source.read(min(BUFFER_BYTES, maximum - size + 1)), b""):
        size += len(chunk)
        require(size <= maximum, "compressed stdin exceeds 128 MiB")
        output.write(chunk)
    output.seek(0)
    return output


def metadata_cap(name):
    if name.startswith("source/") and name.count("/") == 1:
        return MAX_SOURCE_BYTES
    if name in ("baseline-evidence/evidence.json", "candidate-evidence/evidence.json"):
        return MAX_EVIDENCE_BYTES
    fixed = {"plan.json", "report.json", "manifest.json", "source-hashes.json",
             "source-provenance.json"}
    phases = ("baseline", "candidate")
    fixed.update(f"{phase}-evidence/{leaf}" for phase in phases
                 for leaf in ("topology.json",))
    fixed.update(f"deployment/{phase}/{leaf}" for phase in phases
                 for leaf in ("topology.json", f"{phase}-profile.json"))
    return MAX_JSON_BYTES if name in fixed else None


def safe_name(name, directory=False):
    require(isinstance(name, str) and name and "\\" not in name and "\x00" not in name,
            "unsafe archive member path")
    plain = name[:-1] if directory and name.endswith("/") else name
    require(plain and not PurePosixPath(plain).is_absolute()
            and all(part not in ("", ".", "..") for part in plain.split("/"))
            and PurePosixPath(plain).as_posix() == plain,
            f"unsafe archive member path: {name!r}")
    return plain


class MemberReader:
    """Sequential CRC-checked ZIP reads with bounded physical buffers and hashes."""

    def __init__(self, tree, name):
        self.tree, self.name = tree, name
        cap = metadata_cap(name)
        require(cap is None or tree.members[name].file_size <= cap,
                f"metadata/source cap exceeded: {name}")
        self.source = tree.zip.open(tree.members[name], "r")
        self.digest = hashlib.sha256()
        self.size = 0
        self.closed = False

    def read(self, amount):
        require(type(amount) is int and amount >= 0, "whole-member raw reads are forbidden")
        if amount > BUFFER_BYTES:
            cap = metadata_cap(self.name)
            require(cap is not None and amount <= cap + 1,
                    "raw read exceeds bounded metadata cap")
        parts = bytearray()
        while len(parts) < amount:
            requested = min(BUFFER_BYTES, amount - len(parts))
            self.tree.max_physical_read = max(self.tree.max_physical_read, requested)
            chunk = self.source.read(requested)
            if not chunk:
                break
            self.size += len(chunk)
            self.tree.decompressed_bytes += len(chunk)
            require(self.size <= self.tree.members[self.name].file_size,
                    "member expanded beyond its indexed size")
            self.digest.update(chunk)
            parts.extend(chunk)
        return bytes(parts)

    def readline(self, amount):
        require(type(amount) is int and 0 <= amount <= 64 * 1024**2 + 1,
                "unbounded or oversized raw line")
        parts = bytearray()
        while len(parts) < amount:
            requested = min(BUFFER_BYTES, amount - len(parts))
            self.tree.max_physical_read = max(self.tree.max_physical_read, requested)
            chunk = self.source.readline(requested)
            if not chunk:
                break
            self.size += len(chunk)
            self.tree.decompressed_bytes += len(chunk)
            require(self.size <= self.tree.members[self.name].file_size,
                    "member expanded beyond its indexed size")
            self.digest.update(chunk)
            parts.extend(chunk)
            if chunk.endswith(b"\n"):
                break
        return bytes(parts)

    def close(self, validate=True):
        if not self.closed:
            self.closed = True
            try:
                if validate:
                    require(self.size == self.tree.members[self.name].file_size,
                            f"incomplete streamed member read: {self.name}")
                    value = self.digest.hexdigest()
                    expected = self.tree.expected_hashes.get(self.name)
                    require(expected is None or value == expected,
                            f"streamed member digest mismatch: {self.name}")
                    if self.name in {"plan.json", "report.json"}:
                        self.tree.expected_hashes.setdefault(self.name, value)
            finally:
                self.source.close()

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        self.close(validate=kind is None)


class ZipPath:
    """Contained read-only Path operations needed by the unchanged trusted helpers."""

    def __init__(self, tree, name=""):
        self.tree, self.member_name = tree, name

    def __truediv__(self, leaf):
        require(isinstance(leaf, str), "ZIP path component must be text")
        name = f"{self.member_name}/{leaf}" if self.member_name else leaf
        return ZipPath(self.tree, safe_name(name))

    @property
    def name(self):
        return self.member_name.rsplit("/", 1)[-1]

    @property
    def parents(self):
        parts = self.member_name.split("/") if self.member_name else []
        return tuple(ZipPath(self.tree, "/".join(parts[:count]))
                     for count in range(len(parts) - 1, -1, -1))

    def resolve(self):
        return self

    def is_relative_to(self, root):
        return (isinstance(root, ZipPath) and root.tree is self.tree
                and (not root.member_name or self.member_name == root.member_name
                     or self.member_name.startswith(root.member_name + "/")))

    def is_symlink(self):
        return False

    def is_file(self):
        return self.member_name in self.tree.members

    def is_dir(self):
        return self.member_name in self.tree.directories

    def is_absolute(self):
        return True

    def stat(self):
        if self.is_file():
            return types.SimpleNamespace(st_mode=stat.S_IFREG | 0o644,
                                         st_size=self.tree.members[self.member_name].file_size)
        require(self.is_dir(), f"ZIP member missing: {self.member_name}")
        return types.SimpleNamespace(st_mode=stat.S_IFDIR | 0o755, st_size=0)

    def open(self, mode="rb"):
        require(mode == "rb" and self.is_file(), f"unsupported or missing ZIP input: {self}")
        return MemberReader(self.tree, self.member_name)

    def read_bytes(self):
        cap = metadata_cap(self.member_name)
        require(cap is not None and self.stat().st_size <= cap,
                f"unbounded or oversized metadata/source input: {self.member_name}")
        with self.open("rb") as source:
            raw = source.read(cap + 1)
        require(len(raw) <= cap, f"metadata/source cap exceeded: {self.member_name}")
        return raw

    def read_text(self):
        return self.read_bytes().decode("utf-8")

    def iterdir(self):
        require(self.is_dir(), f"ZIP directory missing: {self}")
        prefix = self.member_name + "/" if self.member_name else ""
        children = {name[len(prefix):].split("/", 1)[0]
                    for name in self.tree.members.keys() | self.tree.directories
                    if name.startswith(prefix) and name != self.member_name}
        return iter(self / name for name in sorted(children) if name)

    def __str__(self):
        return f"verified-zip:/{self.member_name}"


class ZipTree:
    """Authenticated ZIP index; raw bytes are never expanded to a disk cache."""

    def __init__(self, stream, expected_size, expected_digest, path=None):
        require(type(expected_size) is int and 0 < expected_size <= MAX_EXPANDED_BYTES,
                "invalid authenticated compressed size")
        require(isinstance(expected_digest, str) and HEX64.fullmatch(expected_digest),
                "invalid authenticated compressed digest")
        self.stream, self.path = stream, path
        self.expected_size, self.expected_digest = expected_size, expected_digest
        self.expected_hashes = {}
        self.max_physical_read = self.decompressed_bytes = 0
        self.identity = os.fstat(stream.fileno()) if path is not None else None
        if self.identity is not None:
            require(stat.S_ISREG(self.identity.st_mode) and self.identity.st_size == expected_size,
                    "archive fstat size differs from API before hashing")
        self.reverify()
        end = zipfile._EndRecData(stream)
        require(end is not None, "ZIP end record missing")
        require(end[zipfile._ECD_DISK_NUMBER] == 0 and end[zipfile._ECD_DISK_START] == 0
                and end[zipfile._ECD_ENTRIES_THIS_DISK] == end[zipfile._ECD_ENTRIES_TOTAL],
                "multi-disk ZIP input is unsupported")
        require(0 < end[zipfile._ECD_ENTRIES_TOTAL] <= MAX_MEMBERS, "ZIP member count exceeds cap")
        require(0 <= end[zipfile._ECD_SIZE] <= MAX_CENTRAL_BYTES,
                "ZIP central directory exceeds sixteen MiB")
        self.zip = zipfile.ZipFile(stream)
        self.members, self.directories = {}, {""}
        seen, expanded = set(), 0
        try:
            infos = self.zip.infolist()
            require(len(infos) == end[zipfile._ECD_ENTRIES_TOTAL], "ZIP entry count mismatch")
            for info in infos:
                require(info.filename == info.orig_filename, "ZIP member name was normalized")
                directory = info.is_dir()
                name = safe_name(info.filename, directory)
                require(name not in seen, "duplicate ZIP member name")
                seen.add(name)
                mode = stat.S_IFMT(info.external_attr >> 16)
                require(mode in (0, stat.S_IFDIR if directory else stat.S_IFREG),
                        "ZIP member is not a regular file or directory")
                require(not info.flag_bits & 1, "encrypted ZIP member unsupported")
                require(info.compress_type in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED),
                        "unsupported ZIP compression")
                require(0 <= info.file_size <= MAX_MEMBER_BYTES and info.compress_size >= 0,
                        "archive member exceeds 512 MiB")
                require(0 <= info.header_offset < expected_size and info.compress_size <= expected_size,
                        "invalid ZIP member offset or compressed size")
                expanded += info.file_size
                require(expanded <= MAX_EXPANDED_BYTES, "expanded archive exceeds eight GiB")
                if directory:
                    require(info.file_size == 0, "ZIP directory contains data")
                    self.directories.add(name)
                else:
                    self.members[name] = info
                parts = name.split("/")
                self.directories.update("/".join(parts[:count]) for count in range(1, len(parts)))
            require(not self.members.keys() & self.directories, "ZIP file/directory path collision")
            self.expanded_bytes = expanded
            self.root = ZipPath(self)
        except BaseException:
            self.zip.close()
            raise

    def reverify(self):
        if self.path is not None:
            regular_path(self.path)
            now = self.path.stat()
            require((now.st_dev, now.st_ino) == (self.identity.st_dev, self.identity.st_ino),
                    "archive path changed during use")
            require(now.st_size == self.expected_size, "archive path size differs from API before hashing")
        self.stream.seek(0, io.SEEK_END)
        require(self.stream.tell() == self.expected_size, "archive stream size differs from API before hashing")
        size, digest = stream_digest(self.stream, self.expected_size)
        require(size == self.expected_size, "archive API size mismatch")
        require(digest == self.expected_digest, "archive API SHA-256 mismatch")

    def close(self):
        self.zip.close()


def verify_api(bindings, metadata, source_revision, run_id, repository_id, binary=False):
    pattern = (rf"hub-capacity-linux-arm64-{source_revision}" if binary else
               rf"hub-capacity-evidence-{source_revision}-attempt-[1-9][0-9]*")
    bindings.metadata_matches(metadata, name_pattern=pattern, label="binary" if binary else "evidence",
                              source_revision=source_revision, run_id=run_id)
    require(type(repository_id) is int and repository_id > 0, "explicit repository ID required")
    workflow = metadata["workflow_run"]
    require(type(workflow.get("repository_id")) is int
            and type(workflow.get("head_repository_id")) is int
            and workflow["repository_id"] == repository_id
            and workflow["head_repository_id"] == repository_id,
            "artifact API repository ID mismatch")
    artifact_id = metadata.get("id")
    require(type(artifact_id) is int and artifact_id > 0, "artifact API ID missing")
    prefix = f"https://api.github.com/repos/{REPOSITORY}/actions/artifacts/{artifact_id}"
    require(metadata.get("url") == prefix and metadata.get("archive_download_url") == prefix + "/zip",
            "artifact API repository URL mismatch")
    size = metadata.get("size_in_bytes")
    require(type(size) is int and 0 < size <= MAX_EXPANDED_BYTES, "artifact API size missing or oversized")
    return size, metadata["digest"][7:]


def git_blob(checkout, revision, path):
    base = ["git", "--no-replace-objects", "-C", str(checkout)]
    object_name = f"{revision}:{path}"
    size = int(subprocess.check_output([*base, "cat-file", "-s", object_name]))
    require(0 <= size <= MAX_SOURCE_BYTES, "trusted Git source exceeds one MiB")
    return subprocess.check_output([*base, "cat-file", "blob", object_name])


def verify_source_manifest(verify_source, root, plan, checkout, revision):
    """Use the pinned publisher's exact trusted Git source set and blob checks.

    Its verified manifest defines completeness, including newly selected sources.
    Retained manifest names or a fixed file count cannot establish that binding.
    """
    return verify_source(root, plan, checkout, revision)


def verify_sources(publisher, root, plan, checkout, revision):
    hashes, retained = verify_source_manifest(
        publisher.verify_source, root, plan, checkout, revision)
    trusted = git_blob(checkout, revision, "tools/hub-capacity/qualify.py")
    require(retained == trusted, "trusted scorer bytes differ from retained source")
    profile = git_blob(checkout, revision, "tools/hub-capacity/profile-v1.json")
    cached_profile = checkout / "tools/hub-capacity/profile-v1.json"
    regular_path(cached_profile)
    require(cached_profile.stat().st_size <= MAX_SOURCE_BYTES, "profile cache exceeds source cap")
    with cached_profile.open("rb") as incoming:
        cached = incoming.read(MAX_SOURCE_BYTES + 1)
    require(cached == profile,
            "trusted checkout profile cache differs from Git")
    for name, digest in hashes.items():
        root.tree.expected_hashes[f"source/{name}"] = digest
    return hashes, trusted


def verify_evidence(tree, publisher, bindings, args, evidence_metadata, binary_metadata, actual_hashes):
    root = tree.root
    frozen = bindings.owned_json(root, "plan.json")
    report = bindings.owned_json(root, "report.json")
    require(isinstance(frozen, dict) and isinstance(frozen.get("plan"), dict), "frozen plan missing")
    plan = frozen["plan"]
    evidence = {phase: bindings.owned_json(root, f"{phase}-evidence/evidence.json",
                                          maximum=bindings.MAX_EVIDENCE_BYTES)
                for phase in bindings.PHASES}
    for phase, document in evidence.items():
        publisher.phase_artifacts(root, phase, document)
        for artifact in document["artifacts"]:
            tree.expected_hashes[f"{phase}-evidence/{artifact['path']}"] = artifact["sha256"]
    source_hashes, trusted_source = verify_sources(publisher, root, plan,
                                                  args.source_checkout, args.qualification_revision)
    documents = bindings.load_documents(root, args.source_checkout, args.actual_binary_hashes,
                                        args.evidence_metadata, args.binary_metadata)
    require(documents["evidence_metadata"] == evidence_metadata
            and documents["binary_metadata"] == binary_metadata
            and documents["actual_hashes"] == actual_hashes, "external inputs changed during use")
    binding_result = bindings.verify(documents, args.qualification_revision,
                                     args.binary_revision, args.ci_run_id)
    scorer = types.ModuleType("trusted_git_scorer")
    scorer.__file__ = str(args.source_checkout / "tools/hub-capacity/qualify.py")
    exec(compile(trusted_source, scorer.__file__, "exec"), scorer.__dict__)
    scorer.validate_plan(plan)
    require(frozen["plan_sha256"] == scorer.digest(plan), "publication plan digest mismatch")
    computed = {"plan_sha256": frozen["plan_sha256"]}
    for phase, document in evidence.items():
        scorer.validate_evidence(plan, document, phase)
        scorer.verify_artifacts(document, root / f"{phase}-evidence")
        computed[phase] = scorer.score(plan, document)
        topology = documents["phases"][phase]["topology"]
        for field, leaf in (("initial_transactions_sha256", "initial-transactions.jsonl"),
                            ("canonical_batch_observations_sha256", "canonical-batch-observations.json")):
            path = publisher.owned_file(root, f"deployment/{phase}/{leaf}")
            tree.expected_hashes[path.member_name] = topology[field]
            require(publisher.digest(path) == topology[field], "retained seed evidence digest mismatch")
    require(computed == report, "publication report differs from exact rescoring")
    tree.reverify()
    return {"report": report, "bindings": binding_result,
            "source_hashes": source_hashes, "scorer_sha256": hashlib.sha256(trusted_source).hexdigest()}


def public_member(name):
    parts = PurePosixPath(name).parts
    forbidden = {"telcoin-network", "node-record-api", "hub-capacity-peer", "validator-keys", "keys"}
    require(not forbidden.intersection(parts) and not name.endswith((".key", ".pem", ".keystore")),
            f"private key or executable cannot enter public bundle: {name}")


def stream_archive(publisher, root, names, destination):
    require(len(names) == len(set(names)), "duplicate publication member")
    for name in names:
        public_member(name)
        path = publisher.owned_file(root, name)
        if name not in root.tree.expected_hashes:
            root.tree.expected_hashes[name] = publisher.digest(path)
    publisher.archive(root, names, destination)


def copy_pinned_member(source, target, publisher):
    expected = source.tree.expected_hashes.get(source.member_name)
    require(isinstance(expected, str) and HEX64.fullmatch(expected),
            f"publication copy lacks an authenticated member hash: {source.member_name}")
    with source.open("rb") as incoming, target.open("xb") as outgoing:
        shutil.copyfileobj(incoming, outgoing, length=BUFFER_BYTES)
    require(publisher.digest(target) == expected,
            f"published copy digest mismatch: {source.member_name}")


class OwnedDestination:
    """Record exclusive output-directory ownership for failure cleanup."""

    def __init__(self, path):
        self.path = path
        self.identity = None

    def resolve(self):
        return self

    def mkdir(self, **kwargs):
        self.path.mkdir(**kwargs)
        self.identity = self.path.stat()

    def __truediv__(self, name):
        return self.path / name

    def iterdir(self):
        return self.path.iterdir()

    def __str__(self):
        return str(self.path)

    def cleanup(self):
        if self.identity is not None and self.path.exists() and not self.path.is_symlink():
            current = self.path.stat()
            require((current.st_dev, current.st_ino) == (self.identity.st_dev, self.identity.st_ino),
                    "publication destination ownership changed")
            shutil.rmtree(self.path)


def publication_limits(tree):
    """Derive copy quotas from the authenticated index, without reading a member."""
    limits = {name: MAX_PUBLIC_GZIP_BYTES for name in PUBLICATION_NAMES if name.endswith(".gz")}
    for name in ("plan.json", "report.json"):
        size = tree.members[name].file_size
        require(type(size) is int and 0 <= size <= MAX_JSON_BYTES,
                f"publication copy size exceeds metadata cap: {name}")
        limits[name] = size
    limits.update({"README.md": MAX_PUBLIC_README_BYTES, "SHA256SUMS": PUBLIC_CHECKSUM_BYTES})
    require(sum(64 + 2 + len(name.encode("ascii")) + 1 for name in CHECKSUM_NAMES)
            == PUBLIC_CHECKSUM_BYTES, "checksum inventory size assumption changed")
    return limits


def publication_slack(parent, destination):
    """Reserve block rounding plus conservative directory and filesystem metadata slack.

    This is not an atomic reservation against unrelated filesystem writers.
    Per-write and final floor checks reject observed concurrent space consumption.
    """
    filesystem = os.statvfs(parent)
    block = max(filesystem.f_bsize, filesystem.f_frsize)
    require(0 < block <= 1024**2, "unsupported publication filesystem allocation unit")
    missing = sum(not item.exists() for item in destination.parents)
    require(missing <= 64, "publication output parent depth exceeds slack bound")
    return PUBLIC_ALLOCATION_SLACK + block * (len(PUBLICATION_NAMES) + missing + 1)


class QuotaWriter:
    """Unbuffered physical writes reserve quota before the syscall, including gzip trailers."""

    def __init__(self, owner, name, raw):
        self.owner, self.output_name, self.raw = owner, name, raw
        self.identity = os.fstat(raw.fileno())
        self.size, self.hasher, self.failed = 0, hashlib.sha256(), False

    @property
    def name(self):
        return str(self.owner.path / self.output_name)

    @property
    def closed(self):
        return self.raw.closed

    def tell(self):
        return self.size

    def write(self, data):
        try:
            require(not self.closed and not self.failed, "publication writer is closed or failed")
            data = memoryview(data).cast("B")
            count = len(data)
            require(count <= self.owner.limits[self.output_name] - self.size,
                    f"publication file quota exceeded: {self.output_name}")
            require(count <= self.owner.maximum - self.owner.reserved,
                    "publication aggregate write quota exceeded")
            self.owner.assert_identity()
            require(shutil.disk_usage(self.owner.parent).free >= MIN_FREE_BYTES + count + self.owner.slack,
                    "publication write would consume the thirty GiB floor")
            self.owner.reserved += count
            written = self.raw.write(data)
            require(type(written) is int and written == count, "publication short or failed write")
            self.size += written
            self.hasher.update(data)
            return written
        except BaseException:
            self.failed = True
            raise

    def flush(self):
        try:
            self.raw.flush()
        except BaseException:
            self.failed = True
            raise

    def close(self):
        if not self.closed:
            try:
                self.flush()
            finally:
                self.raw.close()

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        self.close()


class QuotaPath:
    """The fixed output-path operations used by the byte-identical publisher."""

    def __init__(self, owner, name):
        require(name in owner.limits, f"unknown publication output: {name}")
        self.owner, self.name = owner, name

    def __str__(self):
        return str(self.owner.path / self.name)

    def __lt__(self, other):
        require(isinstance(other, QuotaPath) and other.owner is self.owner,
                "publication output comparison changed ownership")
        return self.name < other.name

    def stat(self):
        return self.owner.check_file(self.name)

    def open(self, mode="rb"):
        require(mode in ("rb", "xb"), "unsupported publication output mode")
        if mode == "xb":
            return self.owner.create_file(self.name)
        identity = self.owner.check_file(self.name)
        descriptor = self.owner.open_descriptor(self.name, os.O_RDONLY)
        try:
            raw = os.fdopen(descriptor, "rb", buffering=0)
        except BaseException:
            os.close(descriptor)
            raise
        try:
            current = os.fstat(raw.fileno())
            require((current.st_dev, current.st_ino, current.st_size)
                    == (identity.st_dev, identity.st_ino, identity.st_size),
                    "publication read identity changed")
        except BaseException:
            raw.close()
            raise
        return raw

    def write_text(self, text):
        require(self.name in ("README.md", "SHA256SUMS") and isinstance(text, str),
                "unexpected publication text output")
        encoded = text.encode("utf-8")
        require(len(encoded) <= self.owner.limits[self.name], "publication text quota exceeded")
        if self.name == "SHA256SUMS":
            self.owner.check_inventory(CHECKSUM_NAMES)
            expected = "".join(f"{self.owner.writers[name].hasher.hexdigest()}  {name}\n"
                               for name in CHECKSUM_NAMES).encode("ascii")
            require(encoded == expected and len(encoded) == PUBLIC_CHECKSUM_BYTES,
                    "publication checksums differ from the exact six-file inventory")
        with self.open("xb") as outgoing:
            outgoing.write(encoded)
        return len(text)


class QuotaDestination(OwnedDestination):
    """Owned publication with exact inventory and bounded physical output writes."""

    def __init__(self, path, limits, parent, slack):
        super().__init__(path)
        require(set(limits) == set(PUBLICATION_NAMES), "publication quota inventory differs")
        self.limits, self.parent, self.slack = dict(limits), parent, slack
        self.maximum, self.reserved, self.writers = sum(limits.values()), 0, {}

    def assert_identity(self):
        require(self.identity is not None, "publication output has not been exclusively created")
        current = self.path.lstat()
        require(stat.S_ISDIR(current.st_mode)
                and (current.st_dev, current.st_ino) == (self.identity.st_dev, self.identity.st_ino),
                "publication destination ownership changed")

    def __truediv__(self, name):
        return QuotaPath(self, name)

    def create_file(self, name):
        self.assert_identity()
        require(name in self.limits and name not in self.writers,
                "unknown or duplicate publication output")
        descriptor = self.open_descriptor(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        try:
            raw = os.fdopen(descriptor, "wb", buffering=0)
        except BaseException:
            os.close(descriptor)
            raise
        try:
            writer = QuotaWriter(self, name, raw)
        except BaseException:
            raw.close()
            raise
        self.writers[name] = writer
        return writer

    def open_descriptor(self, name, flags):
        self.assert_identity()
        directory = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            current = os.fstat(directory)
            require((current.st_dev, current.st_ino) == (self.identity.st_dev, self.identity.st_ino),
                    "publication directory identity changed before file creation")
            return os.open(name, flags | os.O_NOFOLLOW, 0o666, dir_fd=directory)
        finally:
            os.close(directory)

    def check_file(self, name):
        self.assert_identity()
        require(name in self.writers, "publication file was not created by its quota writer")
        writer = self.writers[name]
        require(writer.closed and not writer.failed, "publication writer did not finish successfully")
        current = (self.path / name).lstat()
        require(stat.S_ISREG(current.st_mode) and current.st_nlink == 1
                and (current.st_dev, current.st_ino) == (writer.identity.st_dev, writer.identity.st_ino),
                "publication output is not its exclusively owned regular file")
        require(current.st_size == writer.size <= self.limits[name], "publication final file size differs")
        if name in ("plan.json", "report.json", "SHA256SUMS"):
            require(current.st_size == self.limits[name], "publication exact-size output differs")
        return current

    def check_inventory(self, expected):
        self.assert_identity()
        require(set(self.writers) == set(expected)
                and {path.name for path in self.path.iterdir()} == set(expected),
                "publication final output inventory differs")
        for name in expected:
            self.check_file(name)
            with (self / name).open("rb") as incoming:
                hasher, size = hashlib.sha256(), 0
                for chunk in iter(lambda: incoming.read(min(BUFFER_BYTES,
                                 self.writers[name].size - size + 1)), b""):
                    size += len(chunk)
                    require(size <= self.writers[name].size, "publication output grew during hashing")
                    hasher.update(chunk)
            self.check_file(name)
            require(hasher.hexdigest() == self.writers[name].hasher.hexdigest(),
                    "publication output hash differs from physically written bytes")

    def iterdir(self):
        expected = PUBLICATION_NAMES if "SHA256SUMS" in self.writers else CHECKSUM_NAMES
        self.check_inventory(expected)
        return iter(self / name for name in sorted(expected))

    def finish(self):
        self.check_inventory(PUBLICATION_NAMES)
        require(self.reserved <= self.maximum, "publication aggregate final quota exceeded")
        require(shutil.disk_usage(self.parent).free >= MIN_FREE_BYTES,
                "publication leaves less than the thirty GiB free-space floor")

    def cleanup(self):
        failures = []
        for writer in self.writers.values():
            try:
                writer.close()
            except BaseException as error:
                failures.append(error)
        super().cleanup()
        if failures:
            raise failures[0]


class PublicationTransaction:
    """Keep exclusive directory ownership through final checks, output and close."""

    def __init__(self):
        self.owner = None

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        if kind is not None and self.owner is not None:
            self.owner.cleanup()


def publish(tree, publisher, verified, args):
    require(verified["report"]["candidate"]["passed"] is True,
            "candidate does not qualify, publication cannot claim completion")
    destination = args.destination
    require(destination is not None and not destination.exists() and not destination.is_symlink(),
            "publication destination already exists or is missing")
    require(not any(parent.is_symlink() for parent in destination.parents), "symlink output parent")
    parent = next((item for item in destination.parents if item.exists()), None)
    require(parent is not None, "output parent missing")
    limits = publication_limits(tree)
    slack = publication_slack(parent, destination)
    reserve = sum(limits.values()) + slack
    require(shutil.disk_usage(parent).free - reserve >= MIN_FREE_BYTES,
            "public bundle requires thirty GiB free after conservative output reserve")
    output = QuotaDestination(destination, limits, parent, slack)
    original_path, original_archive, original_copy, original_verify = (
        publisher.Path, publisher.archive, publisher.shutil, publisher.verify_source)
    original_argv = sys.argv

    def paths(value):
        if str(value) == ROOT_TOKEN:
            return tree.root
        if str(value) == str(destination):
            return output
        return Path(value)

    def copied(source, target):
        copy_pinned_member(source, target, publisher)

    def trusted_source(root, plan, checkout, revision):
        hashes, retained = verify_source_manifest(
            original_verify, root, plan, checkout, revision)
        trusted = git_blob(checkout, revision, "tools/hub-capacity/qualify.py")
        require(retained == trusted, "trusted publication scorer mismatch")
        return hashes, trusted

    try:
        publisher.Path = paths
        publisher.archive = lambda root, names, target: stream_archive(
            types.SimpleNamespace(archive=original_archive, owned_file=publisher.owned_file,
                                  digest=publisher.digest), root, names, target)
        publisher.shutil = types.SimpleNamespace(copyfile=copied)
        publisher.verify_source = trusted_source
        sys.argv = [str(PUBLISHER_PATH), ROOT_TOKEN, str(destination),
                    "--source-checkout", str(args.source_checkout),
                    "--qualification-revision", args.qualification_revision,
                    "--ci-run-id", str(args.ci_run_id)]
        with contextlib.redirect_stdout(io.StringIO()):
            publisher.main()
        tree.reverify()
        output.finish()
    except BaseException:
        output.cleanup()
        raise
    finally:
        publisher.Path, publisher.archive, publisher.shutil, publisher.verify_source = (
            original_path, original_archive, original_copy, original_verify)
        sys.argv = original_argv
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("zip", help="authenticated retained ZIP path, or - for bounded stdin")
    parser.add_argument("--evidence-metadata", type=Path, required=True)
    parser.add_argument("--binary-metadata", type=Path, required=True)
    parser.add_argument("--actual-binary-hashes", type=Path, required=True)
    parser.add_argument("--source-checkout", type=Path, required=True)
    parser.add_argument("--qualification-revision", required=True)
    parser.add_argument("--binary-revision", required=True)
    parser.add_argument("--ci-run-id", type=int, required=True)
    parser.add_argument("--repository-id", type=int, required=True)
    parser.add_argument("--repository", choices=(REPOSITORY,), required=True)
    parser.add_argument("--destination", type=Path,
                        help="publish only an independently reproduced passing report")
    args = parser.parse_args()
    require(HEX40.fullmatch(args.qualification_revision) and HEX40.fullmatch(args.binary_revision),
            "explicit full lowercase source and binary revisions required")
    require(args.ci_run_id > 0, "explicit positive CI run ID required")
    publisher, bindings = trusted_helpers()
    evidence_metadata = bindings.read_json(args.evidence_metadata)
    binary_metadata = bindings.read_json(args.binary_metadata)
    actual_hashes = bindings.hash_map(bindings.read_json(args.actual_binary_hashes), "actual binary hashes")
    size, digest = verify_api(bindings, evidence_metadata, args.qualification_revision,
                              args.ci_run_id, args.repository_id)
    verify_api(bindings, binary_metadata, args.qualification_revision,
               args.ci_run_id, args.repository_id, binary=True)
    path = None
    if args.zip == "-":
        source = bounded_stdin(sys.stdin.buffer)
    else:
        path = Path(args.zip)
        regular_path(path)
        source = path.open("rb")
    tree = None
    with PublicationTransaction() as publication:
        try:
            tree = ZipTree(source, size, digest, path)
            verified = verify_evidence(tree, publisher, bindings, args,
                                       evidence_metadata, binary_metadata, actual_hashes)
            if args.destination is not None:
                publication.owner = publish(tree, publisher, verified, args)
            tree.reverify()
            if publication.owner is not None:
                publication.owner.finish()
            print(json.dumps({"authenticated_zip_sha256": digest, "compressed_bytes": size,
                              "members": len(tree.members), "expanded_bytes": tree.expanded_bytes,
                              "source_count": len(verified["source_hashes"]),
                              "trusted_scorer_sha256": verified["scorer_sha256"],
                              "report_equal": True, "candidate_passed": verified["report"]["candidate"]["passed"],
                              "report": verified["report"], "bindings": verified["bindings"],
                              "max_physical_read_bytes": tree.max_physical_read,
                              "decompressed_bytes": tree.decompressed_bytes,
                              "published": str(args.destination) if args.destination is not None else None},
                             sort_keys=True, allow_nan=False))
        finally:
            if tree is not None:
                tree.close()
            source.close()


if __name__ == "__main__":
    main()
