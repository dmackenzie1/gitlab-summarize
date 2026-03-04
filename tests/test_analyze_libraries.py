import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

with mock.patch.dict("sys.modules", {"toml": types.SimpleNamespace(load=lambda *_a, **_k: {})}):
    from analyze_libraries import find_manifests, is_manifest_filename, parse_manifest


class AnalyzeLibrariesTests(unittest.TestCase):
    def test_manifest_name_detection_handles_docker_variants(self):
        self.assertTrue(is_manifest_filename("Dockerfile"))
        self.assertTrue(is_manifest_filename("Dockerfile.prod"))
        self.assertTrue(is_manifest_filename("api.Dockerfile"))
        self.assertTrue(is_manifest_filename("docker-compose.ci.yaml"))
        self.assertTrue(is_manifest_filename("compose.yaml"))
        self.assertTrue(is_manifest_filename("requirements-dev.txt"))

    def test_find_manifests_includes_nested_docker_and_lockfiles(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            (repo / "docker" / "services").mkdir(parents=True)

            (repo / "docker" / "services" / "Dockerfile.prod").write_text("FROM node:20\n", encoding="utf-8")
            (repo / "docker" / "services" / "package-lock.json").write_text("{}", encoding="utf-8")
            (repo / "docker" / "services" / "docker-compose.ci.yaml").write_text(
                "services:\n  app:\n    image: node:22\n", encoding="utf-8"
            )

            manifests = {p.relative_to(repo).as_posix() for p in find_manifests(repo)}

            self.assertIn("docker/services/Dockerfile.prod", manifests)
            self.assertIn("docker/services/package-lock.json", manifests)
            self.assertIn("docker/services/docker-compose.ci.yaml", manifests)

    def test_parse_manifest_reads_base_image_from_dockerfile_variant(self):
        with tempfile.TemporaryDirectory() as tmp:
            dockerfile = Path(tmp) / "Dockerfile.base"
            dockerfile.write_text("FROM node:18-alpine\n", encoding="utf-8")

            kind, libs = parse_manifest(dockerfile)

            self.assertEqual(kind, "Dockerfile")
            self.assertEqual(libs.get("runtime:node"), "18-alpine")


if __name__ == "__main__":
    unittest.main()
