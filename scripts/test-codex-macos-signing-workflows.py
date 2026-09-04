#!/usr/bin/env python3
"""Contract tests that keep the Codex signing gate on every macOS build lane."""

from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VERIFY = "python3 scripts/verify-codex-macos-signing.py"


def section(text: str, start: str, end: str | None = None) -> str:
    _, separator, remainder = text.partition(start)
    if not separator:
        raise AssertionError(f"missing workflow section {start!r}")
    return remainder.partition(end)[0] if end is not None else remainder


class WorkflowContractTests(unittest.TestCase):
    @staticmethod
    def assert_ordered(text: str, *needles: str) -> None:
        cursor = -1
        for needle in needles:
            position = text.find(needle, cursor + 1)
            if position < 0:
                raise AssertionError(f"missing or out-of-order workflow contract: {needle}")
            cursor = position

    def test_release_checks_both_macos_architectures_before_and_after_signing(self) -> None:
        workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
        arm64 = section(workflow, "  release:\n", "  release-macos-x64:\n")
        x64 = section(workflow, "  release-macos-x64:\n", "  release-linux:\n")
        for job in (arm64, x64):
            self.assertEqual(job.count(f"{VERIFY} staged"), 1)
            self.assertEqual(job.count(f"{VERIFY} app"), 1)
            self.assert_ordered(
                job,
                "node scripts/bundle-codex-runtime.mjs",
                f"{VERIFY} staged",
                "- name: Build unsigned Tauri app",
                "block/apple-codesign-action@",
                "ditto -x -k",
                "cp -R \"${EXTRACT_DIR}/Buzz.app\"",
                "- name: Verify code signature",
                f"{VERIFY} app",
            )

    def test_signed_canary_checks_staged_and_extracted_app(self) -> None:
        workflow = (ROOT / ".github/workflows/signed-macos-canary.yml").read_text(
            encoding="utf-8"
        )
        self.assertEqual(workflow.count(f"{VERIFY} staged"), 1)
        self.assertEqual(workflow.count(f"{VERIFY} app"), 1)
        self.assert_ordered(
            workflow,
            "node scripts/bundle-codex-runtime.mjs",
            f"{VERIFY} staged",
            "- name: Build unsigned Tauri app",
            "block/apple-codesign-action@",
            "ditto -x -k",
            f"{VERIFY} app",
        )

    def test_unsigned_intel_canary_checks_staged_payload_before_build(self) -> None:
        workflow = (ROOT / ".github/workflows/macos-intel-canary.yml").read_text(
            encoding="utf-8"
        )
        self.assertEqual(workflow.count(f"{VERIFY} staged"), 1)
        self.assertNotIn(f"{VERIFY} app", workflow)
        self.assert_ordered(
            workflow,
            "node scripts/bundle-codex-runtime.mjs",
            f"{VERIFY} staged",
            "- name: Build unsigned Intel DMG",
        )


if __name__ == "__main__":
    unittest.main()
