"""The release/packaging files stay consistent: workflow gates, compose image, installer switches, docs."""

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _text(*parts: str) -> str:
    return ROOT.joinpath(*parts).read_text(encoding="utf-8")


class ReleaseWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = _text(".github", "workflows", "release.yml")

    def test_publishing_waits_for_the_tests(self):
        self.assertRegex(self.workflow, r"(?s)build-installer:\s+needs: verify")
        self.assertRegex(self.workflow, r"(?s)docker-image:\s+needs: verify")
        self.assertIn("unittest discover", self.workflow)

    def test_prerelease_flag_follows_the_tag_suffix(self):
        self.assertIn("prerelease: ${{ contains(github.ref_name, '-') }}", self.workflow)

    def test_notes_come_from_the_changelog_and_checksums_are_published(self):
        self.assertIn("tools/changelog.py notes", self.workflow)
        self.assertIn("release-assets/SHA256SUMS", self.workflow)

    def test_image_is_amd64_with_provenance_and_sbom(self):
        self.assertIn("platforms: linux/amd64", self.workflow)
        self.assertIn("provenance: mode=max", self.workflow)
        self.assertIn("sbom: true", self.workflow)


class ComposeAndInstallerTests(unittest.TestCase):
    def test_compose_uses_the_published_image_with_a_build_fallback_and_healthcheck(self):
        compose = _text("docker-compose.yml")
        self.assertRegex(compose, r"(?m)^\s+image: \$\{JELLYBALL_IMAGE:-ghcr\.io/darthbitbeard/jellyball:latest\}")
        self.assertRegex(compose, r"(?m)^\s+build:")
        self.assertRegex(compose, r"(?m)^\s+healthcheck:")
        self.assertIn("/healthz", compose)

    def test_installer_reads_each_switch_and_never_writes_an_empty_password(self):
        script = _text("Jellyball", "installer", "jellyball.iss")
        for switch in ("PORT", "USER", "LAN", "DATADIR"):
            self.assertIn("{param:%s|}" % switch, script)
        # With no password the line is left out (so Jellyball generates one), not written empty.
        self.assertIn("# DASHBOARD_PASSWORD is not set", script)
        self.assertEqual(script.count("'DASHBOARD_PASSWORD=\"'"), 1)
        self.assertIn("dashboard-password.txt", script)

    def test_build_script_writes_sha256sums(self):
        self.assertIn("SHA256SUMS", _text("Jellyball", "build-installer.ps1"))


class DocsTests(unittest.TestCase):
    def test_every_guide_exists_and_relative_links_resolve(self):
        guides = ["ARCHITECTURE", "PROVIDERS", "JELLYFIN", "TROUBLESHOOTING", "DOCKER", "UPGRADING"]
        for name in guides:
            path = ROOT / "docs" / f"{name}.md"
            self.assertTrue(path.is_file(), name)
            for target in re.findall(r"\]\(((?!https?:|#)[^)#\s]+)", path.read_text(encoding="utf-8")):
                self.assertTrue((path.parent / target).exists(), f"{name}.md links to missing {target}")

    def test_jellyfin_matrix_claims_nothing_verified(self):
        text = _text("docs", "JELLYFIN.md")
        self.assertIn("unverified", text)
        matrix_rows = [line for line in text.splitlines() if line.startswith("| ") and "unverified" in line]
        self.assertTrue(matrix_rows)
        self.assertFalse([row for row in matrix_rows if re.search(r"\|\s*(works|verified|ok)\s*\|", row, re.I)])


if __name__ == "__main__":
    unittest.main()
