#!/usr/bin/env python3
"""Fail-closed signing and placement verification for bundled macOS Codex."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
STAGED_ROOT = REPO_ROOT / "desktop/src-tauri/bundle-resources/codex-cli"
PINS_PATH = REPO_ROOT / "third_party/codex-runtime/manifest.json"
POLICY_PATH = Path(__file__).with_name("codex-macos-signing-policy.json")
CODESIGN = "/usr/bin/codesign"
LIPO = "/usr/bin/lipo"
MAX_TOOL_OUTPUT = 128 * 1024
MAX_CONTROL_BYTES = 1024 * 1024
MAX_RESOURCE_FILES = 32
MACHO_MAGICS = {
    bytes.fromhex(value)
    for value in (
        "feedface",
        "cefaedfe",
        "feedfacf",
        "cffaedfe",
        "cafebabe",
        "bebafeca",
        "cafebabf",
        "bfbafeca",
    )
}
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class VerificationError(RuntimeError):
    """The staged or packaged Codex payload violated the signing contract."""


@dataclass(frozen=True)
class Signer:
    common_name: str
    team_identifier: str
    authority_chain: tuple[str, ...]


@dataclass(frozen=True)
class Policy:
    resource_path: PurePosixPath
    required_controls: frozenset[str]
    optional_controls: frozenset[str]
    required_payloads: frozenset[str]
    architectures: dict[str, str]
    signers: tuple[Signer, ...]


@dataclass(frozen=True)
class Payload:
    relative_path: str
    size: int
    sha256: str


CommandRunner = Callable[[Sequence[str]], str]


def fail(message: str) -> None:
    raise VerificationError(message)


def read_text(path: Path, label: str) -> str:
    try:
        size = path.stat().st_size
        if size > MAX_CONTROL_BYTES:
            fail(f"{label} exceeds {MAX_CONTROL_BYTES} bytes: {path}")
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        fail(f"cannot read {label} {path}: {error}")


def read_json(path: Path, label: str) -> dict:
    try:
        value = json.loads(read_text(path, label))
    except json.JSONDecodeError as error:
        fail(f"cannot read {label} {path}: {error}")
    if not isinstance(value, dict):
        fail(f"{label} must be a JSON object: {path}")
    return value


def relative_path(raw: object, label: str) -> str:
    if not isinstance(raw, str) or not raw or "\\" in raw:
        fail(f"{label} must be a non-empty POSIX relative path")
    path = PurePosixPath(raw)
    if (
        path.is_absolute()
        or path.as_posix() != raw
        or any(part in ("", ".", "..") for part in path.parts)
    ):
        fail(f"unsafe {label}: {raw!r}")
    return path.as_posix()


def load_policy(path: Path = POLICY_PATH) -> Policy:
    raw = read_json(path, "signing policy")
    if raw.get("schemaVersion") != 1:
        fail("signing policy schemaVersion must be 1")
    resource = PurePosixPath(relative_path(raw.get("resourcePath"), "resourcePath"))
    required = frozenset(
        relative_path(item, "required control file")
        for item in raw.get("requiredControlFiles", [])
    )
    optional = frozenset(
        relative_path(item, "optional control file")
        for item in raw.get("optionalControlFiles", [])
    )
    required_payloads = frozenset(
        relative_path(item, "required payload policy path")
        for item in raw.get("requiredPayloads", [])
    )
    if required != {"PROVENANCE.json", "SHA256SUMS.txt"} or required & optional:
        fail("signing policy control-file contract is invalid")
    if not required_payloads or any(
        not path.startswith(("licenses/", "runtime/")) for path in required_payloads
    ):
        fail("signing policy requiredPayloads contract is invalid")
    architectures = raw.get("targets")
    if not isinstance(architectures, dict) or architectures != {
        "aarch64-apple-darwin": "arm64",
        "x86_64-apple-darwin": "x86_64",
    }:
        fail("signing policy must define the two supported thin macOS targets")
    signers = []
    for item in raw.get("allowedSigners", []):
        if not isinstance(item, dict):
            fail("each allowed signer must be an object")
        common_name = item.get("commonName")
        team = item.get("teamIdentifier")
        chain = item.get("authorityChain")
        if (
            not isinstance(common_name, str)
            or not re.fullmatch(r"Developer ID Application: [^\"\\]+", common_name)
            or not isinstance(team, str)
            or not re.fullmatch(r"[A-Z0-9]{10}", team)
            or not isinstance(chain, list)
            or not chain
            or any(not isinstance(authority, str) for authority in chain)
            or chain[0] != common_name
            or chain[-1] != "Apple Root CA"
        ):
            fail("allowed signer must name an exact Developer ID leaf, team, and Apple chain")
        signers.append(Signer(common_name, team, tuple(chain)))
    if not signers:
        fail("signing policy must allow at least one signer")
    return Policy(
        resource, required, optional, required_payloads, architectures, tuple(signers)
    )


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def walk_regular_files(root: Path) -> dict[str, Path]:
    if not root.is_dir() or root.is_symlink():
        fail(f"Codex resource root must be a real directory: {root}")
    found: dict[str, Path] = {}
    for current, directories, filenames in os.walk(root, followlinks=False):
        current_path = Path(current)
        for name in directories:
            path = current_path / name
            if path.is_symlink():
                fail(f"symlinked directory is forbidden in Codex resources: {path}")
        for name in filenames:
            path = current_path / name
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                fail(f"Codex resource must be a regular file: {path}")
            relative = path.relative_to(root).as_posix()
            relative_path(relative, "resource path")
            found[relative] = path
            if len(found) > MAX_RESOURCE_FILES:
                fail(f"Codex resource tree exceeds {MAX_RESOURCE_FILES} files")
    return found


def validate_pins(provenance: dict, pins: dict, target: str) -> None:
    codex = provenance.get("codex")
    pinned_codex = pins.get("codex")
    if not isinstance(codex, dict) or not isinstance(pinned_codex, dict):
        fail("provenance and pin manifests must contain codex objects")
    platforms = pinned_codex.get("platforms")
    pinned_platform = platforms.get(target) if isinstance(platforms, dict) else None
    if not isinstance(pinned_platform, dict):
        fail(f"target is absent from the pinned Codex manifest: {target}")
    version = pinned_codex.get("version")
    suffix = pinned_platform.get("packageSuffix")
    expected = {
        "version": version,
        "upstreamCommit": pinned_codex.get("upstreamCommit"),
        "packageUrl": f"https://registry.npmjs.org/@openai/codex/-/codex-{version}-{suffix}.tgz",
        "integrity": pinned_platform.get("integrity"),
        "licenseSha256": pinned_codex.get("licenseSha256"),
    }
    for key, value in expected.items():
        if not isinstance(value, str) or codex.get(key) != value:
            fail(f"Codex provenance {key} does not match the tracked pin")
    required = codex.get("requiredPayloads")
    if not isinstance(required, list) or not required:
        fail("Codex provenance requiredPayloads must be a non-empty list")
    required_paths = [relative_path(item, "required payload") for item in required]
    if len(required_paths) != len(set(required_paths)):
        fail("Codex provenance contains duplicate required payloads")


def parse_payloads(provenance: dict) -> dict[str, Payload]:
    raw_payloads = provenance.get("payloads")
    if not isinstance(raw_payloads, list) or not raw_payloads:
        fail("Codex provenance payloads must be a non-empty list")
    payloads: dict[str, Payload] = {}
    for item in raw_payloads:
        if not isinstance(item, dict):
            fail("each Codex provenance payload must be an object")
        path = relative_path(item.get("path"), "payload path")
        size = item.get("bytes")
        digest = item.get("sha256")
        if path in payloads:
            fail(f"duplicate payload path in provenance: {path}")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            fail(f"invalid byte count for payload: {path}")
        if not isinstance(digest, str) or not SHA256_RE.fullmatch(digest):
            fail(f"invalid SHA-256 for payload: {path}")
        payloads[path] = Payload(path, size, digest)
    return payloads


def validate_checksums(root: Path, payloads: dict[str, Payload]) -> None:
    path = root / "SHA256SUMS.txt"
    checksums: dict[str, str] = {}
    lines = read_text(path, "checksum ledger").splitlines()
    for line in lines:
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if not match:
            fail(f"malformed checksum ledger line: {line!r}")
        relative = relative_path(match.group(2), "checksum path")
        if relative in checksums:
            fail(f"duplicate checksum ledger path: {relative}")
        checksums[relative] = match.group(1)
    expected = set(payloads) | {"PROVENANCE.json"}
    if set(checksums) != expected:
        fail("checksum ledger paths do not exactly match provenance payloads")
    for relative, digest in checksums.items():
        if sha256(root / relative) != digest:
            fail(f"checksum ledger mismatch: {relative}")


def validate_manifest_and_files(
    root: Path, target: str, policy: Policy, pins_path: Path = PINS_PATH
) -> tuple[dict, dict[str, Payload], dict[str, Path]]:
    files = walk_regular_files(root)
    provenance = read_json(root / "PROVENANCE.json", "Codex provenance")
    if provenance.get("schemaVersion") != 1 or provenance.get("target") != target:
        fail(f"Codex provenance does not describe target {target}")
    validate_pins(provenance, read_json(pins_path, "Codex pin manifest"), target)
    payloads = parse_payloads(provenance)
    codex_path = relative_path(provenance.get("codexPath"), "codexPath")
    required = {
        f"runtime/{relative_path(path, 'required payload')}"
        for path in provenance["codex"]["requiredPayloads"]
    }
    expected_runtime = {
        path.removeprefix("runtime/")
        for path in policy.required_payloads
        if path.startswith("runtime/")
    }
    if {
        relative_path(path, "required payload")
        for path in provenance["codex"]["requiredPayloads"]
    } != expected_runtime:
        fail("provenance requiredPayloads do not match the reviewed macOS policy")
    if not policy.required_payloads <= set(payloads) or codex_path not in required:
        fail("reviewed payloads or codexPath are absent from the provenance payload list")
    if set(payloads) != policy.required_payloads:
        fail(
            "provenance payload inventory differs from the reviewed macOS policy: "
            f"missing={sorted(policy.required_payloads - set(payloads))}, "
            f"extra={sorted(set(payloads) - policy.required_payloads)}"
        )
    license_payload = payloads["licenses/openai-codex-LICENSE"]
    if license_payload.sha256 != provenance["codex"]["licenseSha256"]:
        fail("bundled OpenAI license does not match the tracked license pin")
    expected_files = set(payloads) | set(policy.required_controls)
    allowed_files = expected_files | set(policy.optional_controls)
    if not expected_files <= set(files):
        fail(f"missing Codex resource files: {sorted(expected_files - set(files))}")
    if not set(files) <= allowed_files:
        unlisted = set(files) - allowed_files
        executable = [
            relative for relative in unlisted if files[relative].stat().st_mode & 0o111
        ]
        if executable:
            fail(f"unlisted executable Codex payloads: {sorted(executable)}")
        fail(f"unlisted Codex resource files: {sorted(unlisted)}")
    for relative, payload in payloads.items():
        path = files[relative]
        if path.stat().st_size != payload.size or sha256(path) != payload.sha256:
            fail(f"payload does not match provenance: {relative}")
    validate_checksums(root, payloads)
    return provenance, payloads, files


def is_macho(path: Path) -> bool:
    with path.open("rb") as handle:
        return handle.read(4) in MACHO_MAGICS


def run_command(arguments: Sequence[str]) -> str:
    try:
        result = subprocess.run(
            list(arguments),
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            env={**os.environ, "LC_ALL": "C", "LANG": "C"},
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        fail(f"failed to run {arguments[0]}: {error}")
    output = f"{result.stdout}{result.stderr}"
    if len(output.encode("utf-8", errors="replace")) > MAX_TOOL_OUTPUT:
        fail(f"tool output exceeded {MAX_TOOL_OUTPUT} bytes: {arguments[0]}")
    if result.returncode != 0:
        detail = output.strip()[-4096:]
        fail(f"command failed ({result.returncode}): {' '.join(arguments)}\n{detail}")
    return output.strip()


def codesign_details(output: str) -> tuple[str, str, tuple[str, ...]]:
    teams = re.findall(r"(?m)^TeamIdentifier=(.+)$", output)
    timestamps = re.findall(r"(?m)^Timestamp=(.+)$", output)
    authorities = tuple(re.findall(r"(?m)^Authority=(.+)$", output))
    directory = next(
        (line for line in output.splitlines() if line.startswith("CodeDirectory ")), ""
    )
    if len(teams) != 1 or len(timestamps) != 1 or not timestamps[0].strip():
        fail("code signature must contain one team identifier and one secure timestamp")
    if timestamps[0].strip().lower() in {"none", "n/a"}:
        fail("code signature secure timestamp is missing")
    flags = re.search(r"\bflags=\S+\(([^)]*)\)", directory)
    if not flags or "runtime" not in {part.strip() for part in flags.group(1).split(",")}:
        fail("code signature does not enable hardened runtime")
    return teams[0].strip(), timestamps[0].strip(), authorities


def verify_macho_payloads(
    payloads: dict[str, Payload],
    files: dict[str, Path],
    target: str,
    policy: Policy,
    runner: CommandRunner = run_command,
) -> list[str]:
    executable = {
        relative for relative, path in files.items() if path.stat().st_mode & 0o111
    }
    unlisted_executable = executable - set(payloads)
    if unlisted_executable:
        fail(f"unlisted executable Codex payloads: {sorted(unlisted_executable)}")
    macho = {relative for relative in payloads if is_macho(files[relative])}
    if macho - executable:
        fail(f"Mach-O payloads are not executable: {sorted(macho - executable)}")
    if executable - macho:
        fail(f"executable payloads are not Mach-O: {sorted(executable - macho)}")
    if not macho:
        fail("Codex provenance does not enumerate any Mach-O payloads")

    expected_architecture = policy.architectures.get(target)
    if expected_architecture is None:
        fail(f"unsupported macOS signing target: {target}")
    verified: list[str] = []
    for relative in sorted(macho):
        path = files[relative]
        architecture = runner([LIPO, "-archs", str(path)]).strip()
        if architecture != expected_architecture:
            fail(
                f"wrong architecture for {relative}: expected {expected_architecture}, "
                f"got {architecture!r}"
            )
        details = runner([CODESIGN, "--display", "--verbose=4", str(path)])
        team, _timestamp, authorities = codesign_details(details)
        signer = next(
            (
                allowed
                for allowed in policy.signers
                if allowed.team_identifier == team
                and allowed.authority_chain == authorities
            ),
            None,
        )
        if signer is None:
            fail(f"unexpected Developer ID signer for {relative}: team={team!r}")
        requirement = (
            "anchor apple generic and "
            f'certificate leaf[subject.OU] = "{signer.team_identifier}" and '
            f'certificate leaf[subject.CN] = "{signer.common_name}"'
        )
        runner(
            [
                CODESIGN,
                "--verify",
                "--strict=all",
                "--verbose=4",
                f"-R={requirement}",
                str(path),
            ]
        )
        verified.append(relative)
    return verified


def validate_app_placement(app: Path, policy: Policy, staged_root: Path) -> Path:
    if app.suffix != ".app" or not app.is_dir() or app.is_symlink():
        fail(f"signed app must be a real .app directory: {app}")
    current = app
    for part in policy.resource_path.parts:
        current /= part
        if current.is_symlink():
            fail(f"symlinked app resource path is forbidden: {current}")
    root = current
    if not root.is_dir() or root.is_symlink():
        fail(f"Codex resources are not at the required app path: {root}")
    for control in ("PROVENANCE.json", "SHA256SUMS.txt"):
        try:
            if (root / control).read_bytes() != (staged_root / control).read_bytes():
                fail(f"packaged {control} differs from the staged release input")
        except OSError as error:
            fail(f"cannot compare packaged {control}: {error}")
    matches = []
    for current, _directories, filenames in os.walk(app / "Contents", followlinks=False):
        if "PROVENANCE.json" in filenames:
            matches.append(Path(current) / "PROVENANCE.json")
    if matches != [root / "PROVENANCE.json"]:
        fail(f"Codex provenance is not uniquely placed at {root}: {matches}")
    return root


def verify_root(
    root: Path,
    target: str,
    policy: Policy,
    runner: CommandRunner = run_command,
    pins_path: Path = PINS_PATH,
) -> list[str]:
    _provenance, payloads, files = validate_manifest_and_files(
        root, target, policy, pins_path
    )
    return verify_macho_payloads(payloads, files, target, policy, runner)
