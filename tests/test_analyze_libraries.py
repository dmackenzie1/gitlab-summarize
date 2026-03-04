import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

with mock.patch.dict("sys.modules", {"toml": types.SimpleNamespace(load=lambda *_a, **_k: {})}):
    from analyze_libraries import (
        build_matrix,
        clean_version,
        find_manifests,
        is_manifest_filename,
        parse_manifest,
    )


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

    def test_parse_manifest_skips_internal_docker_stages_and_reads_registry_images(self):
        with tempfile.TemporaryDirectory() as tmp:
            dockerfile = Path(tmp) / "Dockerfile"
            dockerfile.write_text(
                "\n".join(
                    [
                        "FROM eegitlabregistry.fit.nasa.gov/emss/docker-images/nginx:1.28.0-alpine AS base",
                        "FROM node:24.13.1-alpine3.23 AS web",
                        "FROM base AS release",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            _, libs = parse_manifest(dockerfile)

            self.assertEqual(libs.get("runtime:nginx"), "1.28.0-alpine")
            self.assertEqual(libs.get("runtime:node"), "24.13.1-alpine3.23")
            self.assertNotIn("runtime:base", libs)

    def test_build_matrix_contains_app_count_and_latest_version_column(self):
        usage = {
            "runtime:node": [
                types.SimpleNamespace(repo="a", relpath="Dockerfile", manifest_type="Dockerfile", version_or_spec="22.12.0"),
                types.SimpleNamespace(repo="b", relpath="Dockerfile", manifest_type="Dockerfile", version_or_spec="24.13.1-alpine3.23"),
            ]
        }

        header, rows = build_matrix(usage, ["a", "b"], min_repos=1)

        self.assertEqual(header, ["Library", "AppsCount", "LatestVersionInUse", "a", "b"])
        self.assertEqual(rows[0]["AppsCount"], "2")
        self.assertEqual(rows[0]["LatestVersionInUse"], "24.13.1")

    def test_clean_version_rejects_variables_and_extracts_numeric_exacts(self):
        self.assertEqual(clean_version("${version_number}"), "")
        self.assertEqual(clean_version("package-name==1.2.3."), "1.2.3")


if __name__ == "__main__":
    unittest.main()
