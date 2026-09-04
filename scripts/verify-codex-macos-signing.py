#!/usr/bin/env python3
"""Verify staged or packaged native Codex payloads for macOS releases."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from codex_macos_signing_core import (
    POLICY_PATH,
    STAGED_ROOT,
    VerificationError,
    fail,
    load_policy,
    validate_app_placement,
    verify_root,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("staged", "app"))
    parser.add_argument("--target", required=True)
    parser.add_argument("--app", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        policy = load_policy(POLICY_PATH)
        if args.target not in policy.architectures:
            fail(f"unsupported macOS signing target: {args.target}")
        if args.phase == "staged":
            if args.app is not None:
                fail("--app is only valid for the app phase")
            root = STAGED_ROOT
        else:
            if args.app is None:
                fail("--app is required for the app phase")
            root = validate_app_placement(args.app, policy, STAGED_ROOT)
        verified = verify_root(root, args.target, policy)
    except VerificationError as error:
        print(f"Codex macOS signing verification failed: {error}", file=sys.stderr)
        return 1
    print(
        f"Verified {len(verified)} Codex Mach-O payloads for {args.target} "
        f"at {root}:"
    )
    for relative in verified:
        print(f"  {relative}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
