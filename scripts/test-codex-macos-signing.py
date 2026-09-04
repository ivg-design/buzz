#!/usr/bin/env python3
"""Fixture tests for the deterministic bundled-Codex macOS signing gate."""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import codex_macos_signing_core as GATE


class FakeAppleTools:
    def __init__(
        self,
        *,
        architecture: str = "arm64",
        team: str = "2DC432GLL2",
        timestamp: str = "Sep 3, 2026 at 19:38:16",
        runtime: bool = True,
        verify_failure: bool = False,
    ) -> None:
        self.architecture = architecture
        self.team = team
        self.timestamp = timestamp
        self.runtime = runtime
        self.verify_failure = verify_failure
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, arguments: list[str]) -> str:
        self.calls.append(tuple(arguments))
        if arguments[0] == GATE.LIPO:
            return self.architecture
        if arguments[0] != GATE.CODESIGN:
            raise AssertionError(arguments)
        if "--verify" in arguments:
            if self.verify_failure:
                raise GATE.VerificationError("fixture strict signature failure")
            return "valid on disk\nexplicit requirement satisfied"
        flags = "runtime" if self.runtime else "none"
        leaf = (
            "Developer ID Application: OpenAI OpCo, LLC (2DC432GLL2)"
            if self.team == "2DC432GLL2"
            else "Developer ID Application: Unexpected Signer (BADTEAM123)"
        )
        return "\n".join(
            [
                f"CodeDirectory v=20500 size=123 flags=0x10000({flags}) hashes=1+2",
                f"Authority={leaf}",
                "Authority=Developer ID Certification Authority",
                "Authority=Apple Root CA",
                f"Timestamp={self.timestamp}",
                f"TeamIdentifier={self.team}",
            ]
        )


class SigningGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.policy = GATE.load_policy()
        self.pins = json.loads(GATE.PINS_PATH.read_text(encoding="utf-8"))["codex"]

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def write(path: Path, value: bytes, mode: int) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
        path.chmod(mode)

    @staticmethod
    def digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def make_bundle(self, root: Path) -> None:
        self.write(root / ".keep", b"", 0o644)
        license_bytes = (
            GATE.REPO_ROOT / "third_party/codex-runtime/LICENSE.openai-codex"
        ).read_bytes()
        payload_data = {
            "licenses/openai-codex-LICENSE": (license_bytes, 0o644),
            "runtime/bin/codex": (b"\xcf\xfa\xed\xfeCODEX", 0o755),
            "runtime/bin/codex-code-mode-host": (b"\xcf\xfa\xed\xfeHOST", 0o755),
            "runtime/codex-package.json": (b"{}\n", 0o644),
            "runtime/codex-path/rg": (b"\xcf\xfa\xed\xfeRG", 0o755),
            "runtime/codex-resources/zsh/bin/zsh": (b"\xcf\xfa\xed\xfeZSH", 0o755),
        }
        for relative, (contents, mode) in payload_data.items():
            self.write(root / relative, contents, mode)
        platform = self.pins["platforms"]["aarch64-apple-darwin"]
        payload_paths = list(payload_data)
        payloads = [
            {
                "path": relative,
                "bytes": (root / relative).stat().st_size,
                "sha256": self.digest(root / relative),
            }
            for relative in payload_paths
        ]
        provenance = {
            "schemaVersion": 1,
            "target": "aarch64-apple-darwin",
            "codexPath": "runtime/bin/codex",
            "codex": {
                "version": self.pins["version"],
                "upstreamCommit": self.pins["upstreamCommit"],
                "packageUrl": (
                    "https://registry.npmjs.org/@openai/codex/-/"
                    f"codex-{self.pins['version']}-{platform['packageSuffix']}.tgz"
                ),
                "integrity": platform["integrity"],
                "requiredPayloads": [
                    "bin/codex",
                    "bin/codex-code-mode-host",
                    "codex-path/rg",
                    "codex-package.json",
                    "codex-resources/zsh/bin/zsh",
                ],
                "licenseSha256": self.pins["licenseSha256"],
            },
            "payloads": payloads,
        }
        (root / "PROVENANCE.json").write_text(
            f"{json.dumps(provenance, indent=2)}\n", encoding="utf-8"
        )
        checksum_paths = [*payload_paths, "PROVENANCE.json"]
        (root / "SHA256SUMS.txt").write_text(
            "".join(
                f"{self.digest(root / relative)}  {relative}\n"
                for relative in checksum_paths
            ),
            encoding="utf-8",
        )

    def verify(self, root: Path, tools: FakeAppleTools | None = None) -> list[str]:
        return GATE.verify_root(
            root,
            "aarch64-apple-darwin",
            self.policy,
            tools or FakeAppleTools(),
        )

    def rewrite_checksums(self, root: Path, provenance: dict) -> None:
        (root / "PROVENANCE.json").write_text(
            f"{json.dumps(provenance, indent=2)}\n", encoding="utf-8"
        )
        checksum_paths = [
            item["path"] for item in provenance["payloads"]
        ] + ["PROVENANCE.json"]
        (root / "SHA256SUMS.txt").write_text(
            "".join(
                f"{self.digest(root / relative)}  {relative}\n"
                for relative in checksum_paths
            ),
            encoding="utf-8",
        )

    def test_valid_bundle_binds_strict_apple_requirement(self) -> None:
        root = self.base / "codex-cli"
        self.make_bundle(root)
        tools = FakeAppleTools()

        self.assertEqual(
            self.verify(root, tools),
            [
                "runtime/bin/codex",
                "runtime/bin/codex-code-mode-host",
                "runtime/codex-path/rg",
                "runtime/codex-resources/zsh/bin/zsh",
            ],
        )
        verify_call = next(call for call in tools.calls if "--verify" in call)
        self.assertIn("--strict=all", verify_call)
        self.assertTrue(any("anchor apple generic" in item for item in verify_call))
        self.assertTrue(any("2DC432GLL2" in item for item in verify_call))

    def test_rejects_missing_or_tampered_payload(self) -> None:
        root = self.base / "codex-cli"
        self.make_bundle(root)
        (root / "runtime/bin/codex").unlink()
        with self.assertRaisesRegex(GATE.VerificationError, "missing Codex resource"):
            self.verify(root)

        self.make_bundle(root)
        with (root / "runtime/bin/codex").open("ab") as handle:
            handle.write(b"tampered")
        with self.assertRaisesRegex(GATE.VerificationError, "does not match provenance"):
            self.verify(root)

    def test_rejects_unlisted_executable(self) -> None:
        root = self.base / "codex-cli"
        self.make_bundle(root)
        self.write(root / "runtime/bin/rogue", b"#!/bin/sh\n", 0o755)
        with self.assertRaisesRegex(GATE.VerificationError, "unlisted executable"):
            self.verify(root)

    def test_rejects_extra_executable_even_when_added_to_provenance(self) -> None:
        root = self.base / "codex-cli"
        self.make_bundle(root)
        rogue = root / "runtime/bin/rogue"
        self.write(rogue, b"\xcf\xfa\xed\xfeROGUE", 0o755)
        provenance = json.loads((root / "PROVENANCE.json").read_text(encoding="utf-8"))
        provenance["payloads"].append(
            {
                "path": "runtime/bin/rogue",
                "bytes": rogue.stat().st_size,
                "sha256": self.digest(rogue),
            }
        )
        self.rewrite_checksums(root, provenance)
        with self.assertRaisesRegex(GATE.VerificationError, "payload inventory"):
            self.verify(root)

    def test_rejects_wrong_architecture(self) -> None:
        root = self.base / "codex-cli"
        self.make_bundle(root)
        with self.assertRaisesRegex(GATE.VerificationError, "wrong architecture"):
            self.verify(root, FakeAppleTools(architecture="x86_64"))

    def test_rejects_unexpected_signer(self) -> None:
        root = self.base / "codex-cli"
        self.make_bundle(root)
        with self.assertRaisesRegex(GATE.VerificationError, "unexpected Developer ID signer"):
            self.verify(root, FakeAppleTools(team="BADTEAM123"))

    def test_rejects_missing_timestamp_or_hardened_runtime(self) -> None:
        root = self.base / "codex-cli"
        self.make_bundle(root)
        with self.assertRaisesRegex(GATE.VerificationError, "timestamp is missing"):
            self.verify(root, FakeAppleTools(timestamp="none"))
        with self.assertRaisesRegex(GATE.VerificationError, "hardened runtime"):
            self.verify(root, FakeAppleTools(runtime=False))

    def test_rejects_failed_strict_signature_validation(self) -> None:
        root = self.base / "codex-cli"
        self.make_bundle(root)
        with self.assertRaisesRegex(GATE.VerificationError, "strict signature failure"):
            self.verify(root, FakeAppleTools(verify_failure=True))

    def test_app_phase_requires_exact_unique_resource_placement(self) -> None:
        staged = self.base / "staged/codex-cli"
        self.make_bundle(staged)
        app = self.base / "Buzz.app"
        packaged = app / "Contents/Resources/codex-cli"
        shutil.copytree(staged, packaged)

        root = GATE.validate_app_placement(app, self.policy, staged)
        self.assertEqual(root, packaged)
        self.assertEqual(len(self.verify(root)), 4)

        duplicate = app / "Contents/Other/codex-cli"
        shutil.copytree(staged, duplicate)
        with self.assertRaisesRegex(GATE.VerificationError, "not uniquely placed"):
            GATE.validate_app_placement(app, self.policy, staged)

    def test_rejects_executable_non_macho_payload(self) -> None:
        root = self.base / "codex-cli"
        self.make_bundle(root)
        codex = root / "runtime/bin/codex"
        codex.write_bytes(b"#!/bin/sh\n")
        provenance = json.loads((root / "PROVENANCE.json").read_text(encoding="utf-8"))
        codex_record = next(
            item for item in provenance["payloads"] if item["path"] == "runtime/bin/codex"
        )
        codex_record.update(bytes=codex.stat().st_size, sha256=self.digest(codex))
        self.rewrite_checksums(root, provenance)
        with self.assertRaisesRegex(GATE.VerificationError, "not Mach-O"):
            self.verify(root)


if __name__ == "__main__":
    unittest.main()
