//! Checksum-pinned Claude ACP adapter with Buzz JobPolicyV1 enforcement.

use sha2::{Digest, Sha256};
use std::io::Write as _;
use std::path::{Path, PathBuf};

pub(crate) const BUNDLED_CLAUDE_ACP_VERSION: &str = "0.73.0";
pub(crate) const BUNDLED_CLAUDE_ACP_TARBALL_SHA256: &str =
    "01c5d58734d77fcfc3779bd86e0fed5575fe1d8168e03e63a4ef138f1f2150e4";
const DARWIN_ARM64_RUNTIME_SHA256: &str =
    "a2040fe41ef0fd64789801a73165280594339194966d1bdbf8b874b006efc831";
const DARWIN_X64_RUNTIME_SHA256: &str =
    "d9a97f0eab8a57d20f3d1f8d1f9b84cb843a438b5309396e90db8ab17fe054e4";
const WINDOWS_X64_RUNTIME_SHA256: &str =
    "88586945dfd3353ca49659af7593d1a256addeb71e6d31bcea04e34640b7a619";
const WINDOWS_ARM64_RUNTIME_SHA256: &str =
    "959224a2d434d25c352510aae19eb7db4be5496c4d6019d336cabd86c3fe01f1";

const TARBALL_NAME: &str = "agentclientprotocol-claude-agent-acp-0.73.0-nemo.tgz";
const TARBALL: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/claude-agent-acp/agentclientprotocol-claude-agent-acp-0.73.0-nemo.tgz"
));
const PATCH: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/claude-agent-acp/nemo-job-policy.patch"
));
const PROVENANCE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/claude-agent-acp/PROVENANCE.json"
));
const LICENSE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/claude-agent-acp/APACHE-2.0.txt"
));

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn write_bundled_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if std::fs::read(path).is_ok_and(|existing| existing == bytes) {
        return Ok(());
    }
    let parent = path.parent().ok_or_else(|| {
        format!(
            "bundled Claude adapter path has no parent: {}",
            path.display()
        )
    })?;
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("create bundled Claude adapter directory: {error}"))?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("file"),
        uuid::Uuid::new_v4()
    ));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(|error| format!("create bundled Claude adapter temp file: {error}"))?;
        file.write_all(bytes)
            .map_err(|error| format!("write bundled Claude adapter temp file: {error}"))?;
        file.sync_all()
            .map_err(|error| format!("sync bundled Claude adapter temp file: {error}"))?;
        if path.exists() {
            std::fs::remove_file(path)
                .map_err(|error| format!("replace bundled Claude adapter file: {error}"))?;
        }
        std::fs::rename(&temp, path)
            .map_err(|error| format!("install bundled Claude adapter file: {error}"))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp);
    }
    result
}

/// Materialize the reviewed tarball and compliance sidecars beneath Buzz's
/// private npm prefix. npm never receives bytes that fail the embedded digest.
pub(crate) fn materialize_bundled_claude_acp() -> Result<PathBuf, String> {
    let actual = sha256(TARBALL);
    if actual != BUNDLED_CLAUDE_ACP_TARBALL_SHA256 {
        return Err(format!(
            "bundled Claude adapter integrity mismatch: expected {BUNDLED_CLAUDE_ACP_TARBALL_SHA256}, got {actual}"
        ));
    }
    let prefix = super::buzz_managed_npm_prefix().ok_or_else(|| {
        "failed to resolve Buzz app-data directory for bundled Claude adapter".to_string()
    })?;
    let directory = prefix.join(".bundled").join(format!(
        "claude-agent-acp-{BUNDLED_CLAUDE_ACP_VERSION}-nemo"
    ));
    let tarball = directory.join(TARBALL_NAME);
    let patch = directory.join("nemo-job-policy.patch");
    let provenance = directory.join("PROVENANCE.json");
    let license = directory.join("APACHE-2.0.txt");
    for (path, bytes) in [
        (tarball.as_path(), TARBALL),
        (patch.as_path(), PATCH),
        (provenance.as_path(), PROVENANCE),
        (license.as_path(), LICENSE),
    ] {
        write_bundled_file(path, bytes)?;
    }
    let written = std::fs::read(&tarball)
        .map_err(|error| format!("read materialized Claude adapter tarball: {error}"))?;
    if sha256(&written) != BUNDLED_CLAUDE_ACP_TARBALL_SHA256 {
        let _ = std::fs::remove_file(&tarball);
        return Err("materialized Claude adapter integrity mismatch".to_string());
    }
    Ok(tarball)
}

pub(crate) fn buzz_managed_claude_npm_prefix() -> Option<PathBuf> {
    dirs::data_dir().map(|directory| {
        directory
            .join("Buzz")
            .join("claude-job-runtime")
            .join(BUNDLED_CLAUDE_ACP_VERSION)
    })
}

pub(crate) fn buzz_managed_claude_npm_bin_dir() -> Option<PathBuf> {
    buzz_managed_claude_npm_prefix().map(|prefix| installed_bin(&prefix))
}

fn installed_dist(prefix: &Path) -> PathBuf {
    #[cfg(windows)]
    let modules = prefix.join("node_modules");
    #[cfg(not(windows))]
    let modules = prefix.join("lib/node_modules");
    modules
        .join("@agentclientprotocol")
        .join("claude-agent-acp")
        .join("dist")
}

fn installed_bin(prefix: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        prefix.to_path_buf()
    }
    #[cfg(not(windows))]
    {
        prefix.join("bin")
    }
}

fn collect_files(root: &Path, directory: &Path, files: &mut Vec<(String, Vec<u8>)>) -> Option<()> {
    let mut entries = std::fs::read_dir(directory)
        .ok()?
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        // npm generates platform-specific command wrappers here. Buzz never
        // executes them: it launches Node with the verified index.js directly.
        if entry.file_name() == ".bin" {
            continue;
        }
        let path = entry.path();
        let metadata = path.symlink_metadata().ok()?;
        if metadata.file_type().is_symlink() {
            return None;
        }
        if metadata.is_dir() {
            collect_files(root, &path, files)?;
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .ok()?
                .components()
                .map(|component| component.as_os_str().to_str())
                .collect::<Option<Vec<_>>>()?
                .join("/");
            files.push((relative, std::fs::read(path).ok()?));
        } else {
            return None;
        }
    }
    Some(())
}

fn runtime_tree_sha256(root: &Path) -> Option<String> {
    if !root.symlink_metadata().ok()?.is_dir() {
        return None;
    }
    let mut files = Vec::new();
    collect_files(root, root, &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    let mut digest = Sha256::new();
    digest.update(b"buzz-claude-acp-runtime-v1\0");
    for (path, bytes) in files {
        digest.update((path.len() as u64).to_be_bytes());
        digest.update(path.as_bytes());
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    Some(hex::encode(digest.finalize()))
}

fn runtime_sha256_for_target(os: &str, arch: &str) -> Option<&'static str> {
    match (os, arch) {
        ("macos", "aarch64") => Some(DARWIN_ARM64_RUNTIME_SHA256),
        ("macos", "x86_64") => Some(DARWIN_X64_RUNTIME_SHA256),
        ("windows", "x86_64") => Some(WINDOWS_X64_RUNTIME_SHA256),
        ("windows", "aarch64") => Some(WINDOWS_ARM64_RUNTIME_SHA256),
        _ => None,
    }
}

fn expected_runtime_sha256() -> Option<&'static str> {
    runtime_sha256_for_target(std::env::consts::OS, std::env::consts::ARCH)
}

fn adapter_verified_at(prefix: &Path, adapter_path: &Path, expected: &str) -> bool {
    if !adapter_path.is_file() {
        return false;
    }
    if adapter_path.parent() != Some(installed_bin(prefix).as_path()) {
        return false;
    }
    let expected_name = if cfg!(windows) {
        "claude-agent-acp.cmd"
    } else {
        "claude-agent-acp"
    };
    if adapter_path.file_name().and_then(|name| name.to_str()) != Some(expected_name) {
        return false;
    }
    let dist = installed_dist(prefix);
    let Some(package) = dist.parent() else {
        return false;
    };
    runtime_tree_sha256(package).as_deref() == Some(expected)
}

/// Only the Buzz-private launcher whose complete installed package tree is the
/// reviewed build is eligible for managed Claude sessions.
pub(crate) fn bundled_claude_acp_is_verified(adapter_path: &Path) -> bool {
    buzz_managed_claude_npm_prefix()
        .zip(expected_runtime_sha256())
        .is_some_and(|(prefix, expected)| adapter_verified_at(&prefix, adapter_path, expected))
}

pub(crate) fn resolve_bundled_claude_acp_command() -> Option<PathBuf> {
    let name = if cfg!(windows) {
        "claude-agent-acp.cmd"
    } else {
        "claude-agent-acp"
    };
    let candidate = buzz_managed_claude_npm_bin_dir()?.join(name);
    bundled_claude_acp_is_verified(&candidate).then_some(candidate)
}

/// Return a cross-platform direct Node launch for the verified adapter. The
/// npm shim is used only as an installation marker and is never executed.
pub(crate) fn verified_claude_acp_launch(
    adapter_path: &Path,
) -> Result<(PathBuf, PathBuf), String> {
    if !bundled_claude_acp_is_verified(adapter_path) {
        return Err("Claude ACP adapter is missing or failed checksum verification".to_string());
    }
    let node = super::buzz_managed_node_bin_path()
        .filter(|path| path.is_file())
        .ok_or_else(|| "Buzz's verified managed Node runtime is unavailable".to_string())?;
    let prefix = buzz_managed_claude_npm_prefix()
        .ok_or_else(|| "Buzz private Claude npm prefix is unavailable".to_string())?;
    let entrypoint = installed_dist(&prefix).join("index.js");
    Ok((node, entrypoint))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_artifact_and_legal_sidecars_are_pinned() {
        assert_eq!(sha256(TARBALL), BUNDLED_CLAUDE_ACP_TARBALL_SHA256);
        assert!(!PATCH.is_empty());
        assert!(!PROVENANCE.is_empty());
        assert!(!LICENSE.is_empty());
    }

    #[test]
    fn runtime_digests_cover_release_targets_and_fail_closed_elsewhere() {
        for target in [
            ("macos", "aarch64"),
            ("macos", "x86_64"),
            ("windows", "x86_64"),
            ("windows", "aarch64"),
        ] {
            assert!(runtime_sha256_for_target(target.0, target.1).is_some());
        }
        assert!(runtime_sha256_for_target("linux", "x86_64").is_none());
        assert!(runtime_sha256_for_target("macos", "powerpc").is_none());
    }

    #[test]
    fn dist_digest_rejects_tampering_and_extra_files() {
        let temp = tempfile::tempdir().unwrap();
        let dist = temp.path().join("dist");
        std::fs::create_dir(&dist).unwrap();
        std::fs::write(dist.join("index.js"), b"index").unwrap();
        let original = runtime_tree_sha256(&dist).unwrap();
        std::fs::write(dist.join("index.js"), b"tampered").unwrap();
        assert_ne!(
            runtime_tree_sha256(&dist).as_deref(),
            Some(original.as_str())
        );
        std::fs::write(dist.join("extra.js"), b"extra").unwrap();
        assert_ne!(
            runtime_tree_sha256(&dist).as_deref(),
            Some(original.as_str())
        );
    }

    #[test]
    fn installed_adapter_binds_private_location_and_dependency_closure() {
        let temp = tempfile::tempdir().unwrap();
        let prefix = temp.path().join("claude-runtime");
        let dist = installed_dist(&prefix);
        let dependency = dist
            .parent()
            .unwrap()
            .join("node_modules/@anthropic-ai/claude-agent-sdk/sdk.mjs");
        std::fs::create_dir_all(&dist).unwrap();
        std::fs::create_dir_all(dependency.parent().unwrap()).unwrap();
        std::fs::write(dist.join("index.js"), b"adapter").unwrap();
        std::fs::write(&dependency, b"sdk").unwrap();
        let bin = installed_bin(&prefix);
        std::fs::create_dir_all(&bin).unwrap();
        let name = if cfg!(windows) {
            "claude-agent-acp.cmd"
        } else {
            "claude-agent-acp"
        };
        let adapter = bin.join(name);
        let runtime = dist.parent().unwrap();
        let digest = runtime_tree_sha256(runtime).unwrap();

        assert!(!adapter_verified_at(&prefix, &adapter, &digest));
        std::fs::write(&adapter, b"unused marker").unwrap();

        assert!(adapter_verified_at(&prefix, &adapter, &digest));
        std::fs::write(&dependency, b"tampered sdk").unwrap();
        assert!(!adapter_verified_at(&prefix, &adapter, &digest));
        assert!(!adapter_verified_at(
            &prefix,
            &temp.path().join(name),
            &digest,
        ));
    }

    #[cfg(unix)]
    #[test]
    fn dist_digest_rejects_symlinks() {
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().unwrap();
        let dist = temp.path().join("dist");
        std::fs::create_dir(&dist).unwrap();
        std::fs::write(dist.join("index.js"), b"index").unwrap();
        symlink(dist.join("index.js"), dist.join("alias.js")).unwrap();
        assert!(runtime_tree_sha256(&dist).is_none());
    }
}
