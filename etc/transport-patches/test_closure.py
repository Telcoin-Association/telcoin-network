"""Verify that advisory scope follows Cargo edges and retains exact dependency versions."""

from pathlib import Path
import runpy
import unittest


CLOSURE = runpy.run_path(str(Path(__file__).with_name("sources.py")))["transport_closure"]
SOURCES = runpy.run_path(str(Path(__file__).with_name("sources.py")))


class AdvisoryScope(unittest.TestCase):
    def test_only_recorded_proto_source_is_allowed(self):
        root = Path("/recorded-node")
        package = {"name": "quinn-proto", "version": "0.11.18", "source": None,
                   "manifest_path": str(root / "patches/quinn-proto/Cargo.toml")}
        SOURCES["validate_sources"]([package], root, True)
        for changed in ({**package, "version": "0.11.6"},
                        {**package, "manifest_path": "/unreviewed/quinn-proto/Cargo.toml"},
                        {**package, "name": "rustls"}):
            with self.assertRaises(RuntimeError):
                SOURCES["validate_sources"]([changed], root, True)
        with self.assertRaises(RuntimeError):
            SOURCES["validate_sources"]([package], root, False)

    def test_normalization_retains_proto_versions_and_edges(self):
        normalize = SOURCES["normalized_packages"]
        packages = [{"name": "libp2p-quic", "version": "0.14.0", "source": None,
                     "id": "path-quic", "dependencies": ["path-proto"]},
                    {"name": "quinn-proto", "version": "0.11.18", "source": None,
                     "id": "path-proto", "dependencies": ["crypto"]}]
        stock = [{**packages[0], "id": "registry-quic", "source": "registry+test",
                  "dependencies": ["registry-proto"]},
                 {**packages[1], "id": "registry-proto", "source": "registry+test"}]
        self.assertEqual(normalize(packages), normalize(stock))
        self.assertNotEqual(normalize(packages), normalize([stock[0], {**stock[1], "version": "0.11.6"}]))
        self.assertNotEqual(normalize(packages), normalize([stock[0], {**stock[1], "dependencies": []}]))

    def test_transitive_runtime_and_build_dependencies_keep_versions(self):
        packages = [
            {"id": "quic", "name": "libp2p-quic", "version": "0.14.0"},
            {"id": "tls", "name": "libp2p-tls", "version": "0.7.0"},
            {"id": "parser-1", "name": "certificate-parser", "version": "1.0.0"},
            {"id": "parser-2", "name": "certificate-parser", "version": "2.0.0"},
            {"id": "compiler", "name": "crypto-build-tool", "version": "1.0.0"},
            {"id": "dev", "name": "test-fixture", "version": "1.0.0"},
        ]
        edges = {"quic": [("tls", None), ("compiler", "build"), ("dev", "dev")],
                 "tls": [("parser-1", None)], "compiler": [("parser-1", None)]}
        nodes = [{"id": package["id"], "deps": [
            {"pkg": target, "dep_kinds": [{"kind": kind}]} for target, kind in edges.get(package["id"], [])
        ]} for package in packages]
        selected = {(package["name"], package["version"])
                    for package in CLOSURE({"packages": packages, "resolve": {"nodes": nodes}})}
        self.assertIn(("certificate-parser", "1.0.0"), selected)
        self.assertNotIn(("certificate-parser", "2.0.0"), selected)
        self.assertIn(("crypto-build-tool", "1.0.0"), selected)
        self.assertNotIn(("test-fixture", "1.0.0"), selected)
        self.assertEqual(len(selected), 4)


if __name__ == "__main__":
    unittest.main()
