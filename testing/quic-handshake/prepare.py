#!/usr/bin/env python3
"""Prepare a version-checked, disposable libp2p-tls profiling copy."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import tarfile
import tempfile
import tomllib
import urllib.request

ROOT = Path(__file__).resolve().parent
HASHES = {
    "lib.rs": "218c83e5c7f9f93e9ceecfddec999ded50039315588129397925645402842977",
    "certificate.rs": "cc2235283eaffd86d75b302f568d08045e32b218a8fb9f7da418cc636bec5c0d",
    "verifier.rs": "18f4313e6de34a84153ef9b064dee3df6e5f116663bf2bc32d73d499a63ae860",
}


def replace_once(text, old, new):
    """Reject source drift instead of silently instrumenting a different path."""
    if text.count(old) != 1:
        raise ValueError(f"expected one instrumentation site: {old!r}")
    return text.replace(old, new, 1)


def prepare(source):
    """Copy only verified published source, leaving the registry untouched."""
    manifest = tomllib.loads((source / "Cargo.toml").read_text())
    if (manifest["package"]["name"], manifest["package"]["version"]) != ("libp2p-tls", "0.7.0"):
        raise ValueError("expected published libp2p-tls 0.7.0")
    originals = {}
    for name, digest in HASHES.items():
        data = (source / "src" / name).read_bytes()
        if hashlib.sha256(data).hexdigest() != digest:
            raise ValueError(f"upstream source hash mismatch: {name}")
        originals[name] = data.decode()

    lib = replace_once(originals["lib.rs"], "pub mod certificate;", "pub mod certificate;\npub mod profile;")
    provider = "rustls::crypto::aws_lc_rs::default_provider()"
    if lib.count(provider) != 2:
        raise ValueError("expected exactly two provider construction sites")
    lib = lib.replace(provider, "crate::profile::provider()")

    cert = replace_once(
        originals["certificate.rs"],
        "    let x509 = X509Certificate::from_der(der_input)",
        '    let _span = crate::profile::Span::new("parse");\n    let x509 = X509Certificate::from_der(der_input)',
    )
    cert = replace_once(
        cert,
        "self.verify_signature(signature_scheme, raw_certificate, signature)",
        'crate::profile::measure("certificate_signature", || self.verify_signature(signature_scheme, raw_certificate, signature))',
    )
    cert = replace_once(
        cert,
        "let user_owns_sk = self\n            .extension\n            .public_key\n            .verify(&msg, &self.extension.signature);",
        'let user_owns_sk = crate::profile::measure("extension_signature", || self.extension.public_key.verify(&msg, &self.extension.signature));',
    )
    verifier = replace_once(
        originals["verifier.rs"],
        "certificate::parse(cert)?.verify_signature(signature_scheme, message, signature)?;",
        'let certificate = certificate::parse(cert)?;\n    crate::profile::measure("transcript_signature", || certificate.verify_signature(signature_scheme, message, signature))?;',
    )

    destination = ROOT / "generated" / "libp2p-tls"
    # Only this script's ignored output directory may be refreshed.
    if destination.is_symlink() or destination.parent.is_symlink():
        raise ValueError("generated source directory must not be a symlink")
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(source, destination)
    for name, text in {"lib.rs": lib, "certificate.rs": cert, "verifier.rs": verifier}.items():
        (destination / "src" / name).write_text(text)
    shutil.copyfile(ROOT / "profile.rs", destination / "src" / "profile.rs")
    print(json.dumps({"instrumentation": 1, "source_sha256": HASHES, "output": str(destination)}))


def download():
    """Verify the published archive against the node lockfile before extraction."""
    lock = tomllib.loads((ROOT.parent.parent / "Cargo.lock").read_text())
    packages = [p for p in lock["package"] if p["name"] == "libp2p-tls"]
    if len(packages) != 1 or packages[0]["version"] != "0.7.0":
        raise ValueError("node no longer resolves libp2p-tls 0.7.0; rebase the instrumentation")
    url = "https://static.crates.io/crates/libp2p-tls/libp2p-tls-0.7.0.crate"
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != packages[0]["checksum"]:
        raise ValueError("published crate checksum does not match the node lockfile")
    with tempfile.TemporaryDirectory(prefix="quic-handshake-") as temporary:
        archive = Path(temporary) / "tls.crate"
        archive.write_bytes(data)
        with tarfile.open(archive) as crate:
            crate.extractall(temporary, filter="data")
        prepare(Path(temporary) / "libp2p-tls-0.7.0")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tls-source", type=Path, help="use cached published source instead of downloading")
    args = parser.parse_args()
    if args.tls_source is None:
        download()
    else:
        prepare(args.tls_source.resolve())
