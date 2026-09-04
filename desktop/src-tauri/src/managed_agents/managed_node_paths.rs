use sha2::{Digest, Sha256};
use std::io::Write as _;
use std::path::{Path, PathBuf};

pub(crate) const BUNDLED_CODEX_ACP_VERSION: &str = "1.9.0";
pub(crate) const BUNDLED_CODEX_ACP_TARBALL_SHA256: &str =
    "5ba217a3afdba012f5f8e3e145f747d47eb4196c0283195edfb3c2212b388a4c";
pub(crate) const BUNDLED_CODEX_ACP_DIST_SHA256: &str =
    "80dddafac734af0a0db6977482a42b96633d1ebf2416d0be4bd6cf3669cf4c6e";

const BUNDLED_CODEX_ACP_TARBALL_NAME: &str = "agentclientprotocol-codex-acp-1.9.0-nemo.tgz";
const BUNDLED_CODEX_ACP_TARBALL: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/codex-acp/agentclientprotocol-codex-acp-1.9.0-nemo.tgz"
));
const BUNDLED_CODEX_ACP_PATCH: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/codex-acp/nemo-system-prompt.patch"
));
const BUNDLED_CODEX_ACP_PROVENANCE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/codex-acp/PROVENANCE.md"
));
const BUNDLED_CODEX_LICENSE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/codex-acp/OPENAI-CODEX-LICENSE.txt"
));

pub(crate) fn buzz_managed_npm_prefix() -> Option<PathBuf> {
    dirs::data_dir().map(|dir| dir.join("Buzz").join("node-tools"))
}

const BUZZ_MANAGED_NODE_VERSION: &str = "v24.18.0";

pub(crate) fn buzz_managed_node_root() -> Option<PathBuf> {
    dirs::data_dir().map(|dir| dir.join("Buzz").join("runtimes").join("node"))
}

pub(crate) fn buzz_managed_node_bin_dir() -> Option<PathBuf> {
    let (platform, bin_subdir): (&str, Option<&str>) =
        match (std::env::consts::OS, std::env::consts::ARCH) {
            ("macos", "aarch64") => ("darwin-arm64", Some("bin")),
            ("macos", "x86_64") => ("darwin-x64", Some("bin")),
            ("linux", "x86_64") => ("linux-x64", Some("bin")),
            ("linux", "aarch64") => ("linux-arm64", Some("bin")),
            // Windows zips have node.exe + npm.cmd at the archive root — no bin/ subdir
            ("windows", "x86_64") => ("win-x64", None),
            ("windows", "aarch64") => ("win-arm64", None),
            _ => return None,
        };
    buzz_managed_node_root().map(|root| {
        let dir = root.join(BUZZ_MANAGED_NODE_VERSION).join(platform);
        match bin_subdir {
            Some(sub) => dir.join(sub),
            None => dir,
        }
    })
}

pub(crate) fn buzz_managed_node_bin_path() -> Option<PathBuf> {
    buzz_managed_node_bin_dir().map(|bin| {
        #[cfg(windows)]
        {
            bin.join("node.exe")
        }
        #[cfg(not(windows))]
        {
            bin.join("node")
        }
    })
}

pub(crate) fn buzz_managed_npm_bin_dir() -> Option<PathBuf> {
    buzz_managed_npm_prefix().map(|prefix| {
        #[cfg(windows)]
        {
            prefix
        }
        #[cfg(not(windows))]
        {
            prefix.join("bin")
        }
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn write_bundled_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if std::fs::read(path).is_ok_and(|existing| existing == bytes) {
        return Ok(());
    }
    let parent = path.parent().ok_or_else(|| {
        format!(
            "bundled Codex adapter path has no parent: {}",
            path.display()
        )
    })?;
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("create bundled Codex adapter directory: {error}"))?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("file"),
        uuid::Uuid::new_v4()
    ));
    let write_result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(|error| format!("create bundled Codex adapter temp file: {error}"))?;
        file.write_all(bytes)
            .map_err(|error| format!("write bundled Codex adapter temp file: {error}"))?;
        file.sync_all()
            .map_err(|error| format!("sync bundled Codex adapter temp file: {error}"))?;
        if path.exists() {
            std::fs::remove_file(path)
                .map_err(|error| format!("replace bundled Codex adapter file: {error}"))?;
        }
        std::fs::rename(&temp, path)
            .map_err(|error| format!("install bundled Codex adapter file: {error}"))?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    write_result
}

/// Materialize the compile-time bundled adapter and its license/provenance
/// sidecars beneath Buzz's private npm prefix. The embedded tarball is checked
/// before and after writing; an altered build artifact therefore fails before
/// npm can execute package lifecycle logic.
pub(crate) fn materialize_bundled_codex_acp() -> Result<PathBuf, String> {
    let actual = sha256_hex(BUNDLED_CODEX_ACP_TARBALL);
    if actual != BUNDLED_CODEX_ACP_TARBALL_SHA256 {
        return Err(format!(
            "bundled Codex adapter integrity mismatch: expected {BUNDLED_CODEX_ACP_TARBALL_SHA256}, got {actual}"
        ));
    }
    let prefix = buzz_managed_npm_prefix().ok_or_else(|| {
        "failed to resolve Buzz app-data directory for bundled Codex adapter".to_string()
    })?;
    let dir = prefix
        .join(".bundled")
        .join(format!("codex-acp-{}-nemo", BUNDLED_CODEX_ACP_VERSION));
    let tarball = dir.join(BUNDLED_CODEX_ACP_TARBALL_NAME);
    let patch = dir.join("nemo-system-prompt.patch");
    let provenance = dir.join("PROVENANCE.md");
    let license = dir.join("OPENAI-CODEX-LICENSE.txt");
    for (path, bytes) in [
        (tarball.as_path(), BUNDLED_CODEX_ACP_TARBALL),
        (patch.as_path(), BUNDLED_CODEX_ACP_PATCH),
        (provenance.as_path(), BUNDLED_CODEX_ACP_PROVENANCE),
        (license.as_path(), BUNDLED_CODEX_LICENSE),
    ] {
        write_bundled_file(path, bytes)?;
    }
    let written = std::fs::read(&tarball)
        .map_err(|error| format!("read materialized Codex adapter tarball: {error}"))?;
    let actual = sha256_hex(&written);
    if actual != BUNDLED_CODEX_ACP_TARBALL_SHA256 {
        let _ = std::fs::remove_file(&tarball);
        return Err(format!(
            "materialized Codex adapter integrity mismatch: expected {BUNDLED_CODEX_ACP_TARBALL_SHA256}, got {actual}"
        ));
    }
    Ok(tarball)
}

fn managed_codex_acp_dist_path(prefix: &Path) -> PathBuf {
    #[cfg(windows)]
    let modules = prefix.join("node_modules");
    #[cfg(not(windows))]
    let modules = prefix.join("lib").join("node_modules");
    modules
        .join("@agentclientprotocol")
        .join("codex-acp")
        .join("dist")
        .join("index.js")
}

#[cfg(unix)]
fn bundled_codex_acp_entrypoint_matches(adapter_path: &Path, dist: &Path) -> bool {
    adapter_path.canonicalize().ok() == dist.canonicalize().ok()
}

#[cfg(any(windows, test))]
fn expected_codex_acp_windows_cmd() -> &'static [u8] {
    b"@ECHO off\r\nGOTO start\r\n:find_dp0\r\nSET dp0=%~dp0\r\nEXIT /b\r\n:start\r\nSETLOCAL\r\nCALL :find_dp0\r\n\r\nIF EXIST \"%dp0%\\node.exe\" (\r\n  SET \"_prog=%dp0%\\node.exe\"\r\n) ELSE (\r\n  SET \"_prog=node\"\r\n  SET PATHEXT=%PATHEXT:;.JS;=;%\r\n)\r\n\r\nendLocal & goto #_undefined_# 2>NUL || title %COMSPEC% & \"%_prog%\"  \"%dp0%\\node_modules\\@agentclientprotocol\\codex-acp\\dist\\index.js\" %*\r\n"
}

#[cfg(any(windows, test))]
fn codex_acp_windows_cmd_matches(bytes: &[u8]) -> bool {
    bytes == expected_codex_acp_windows_cmd()
}

#[cfg(windows)]
fn bundled_codex_acp_entrypoint_matches(adapter_path: &Path, _dist: &Path) -> bool {
    std::fs::read(adapter_path).is_ok_and(|bytes| codex_acp_windows_cmd_matches(&bytes))
}

/// A public/global adapter never satisfies the managed Codex contract. A
/// Buzz-private shim is accepted only when the installed compiled payload is
/// byte-for-byte the reviewed build.
pub(crate) fn bundled_codex_acp_is_verified(adapter_path: &Path) -> bool {
    let Some(prefix) = buzz_managed_npm_prefix() else {
        return false;
    };
    let Some(bin_dir) = buzz_managed_npm_bin_dir() else {
        return false;
    };
    if adapter_path.parent() != Some(bin_dir.as_path()) {
        return false;
    }
    let expected_name = if cfg!(windows) {
        "codex-acp.cmd"
    } else {
        "codex-acp"
    };
    if adapter_path.file_name().and_then(|name| name.to_str()) != Some(expected_name) {
        return false;
    }
    let dist = managed_codex_acp_dist_path(&prefix);
    if !std::fs::read(&dist).is_ok_and(|bytes| sha256_hex(&bytes) == BUNDLED_CODEX_ACP_DIST_SHA256)
    {
        return false;
    }
    bundled_codex_acp_entrypoint_matches(adapter_path, &dist)
}

pub(crate) fn buzz_managed_command_path(command: &str, basename: &str) -> Option<PathBuf> {
    if command.contains(std::path::MAIN_SEPARATOR)
        || !matches!(
            command,
            "codex-acp" | "claude-agent-acp" | "claude-code-acp" | "node" | "npm"
        )
    {
        return None;
    }

    let mut dirs = Vec::new();
    if let Some(managed_bin) = buzz_managed_npm_bin_dir() {
        dirs.push(managed_bin);
    }
    if let Some(managed_node_bin) = buzz_managed_node_bin_dir() {
        dirs.push(managed_node_bin);
    }

    let candidate = dirs
        .into_iter()
        .map(|dir| dir.join(basename))
        .find(|candidate| is_executable_file(candidate))?;
    if command == "codex-acp" && !bundled_codex_acp_is_verified(&candidate) {
        return None;
    }
    Some(candidate)
}

fn is_executable_file(path: &std::path::Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }

    #[cfg(not(unix))]
    {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_codex_adapter_matches_pinned_checksum_and_has_legal_metadata() {
        assert_eq!(
            sha256_hex(BUNDLED_CODEX_ACP_TARBALL),
            BUNDLED_CODEX_ACP_TARBALL_SHA256
        );
        assert!(!BUNDLED_CODEX_ACP_PATCH.is_empty());
        assert!(!BUNDLED_CODEX_ACP_PROVENANCE.is_empty());
        assert!(!BUNDLED_CODEX_LICENSE.is_empty());
    }

    #[test]
    fn windows_wrapper_template_rejects_any_tampering() {
        let expected = expected_codex_acp_windows_cmd();
        assert!(codex_acp_windows_cmd_matches(expected));
        let mut tampered = expected.to_vec();
        tampered.extend_from_slice(b"echo compromised\r\n");
        assert!(!codex_acp_windows_cmd_matches(&tampered));
    }

    #[cfg(unix)]
    #[test]
    fn unix_entrypoint_must_resolve_to_the_hashed_dist_file() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("temp dir");
        let dist = dir.path().join("dist.js");
        let other = dir.path().join("other.js");
        let shim = dir.path().join("codex-acp");
        std::fs::write(&dist, b"expected").expect("write dist");
        std::fs::write(&other, b"tampered").expect("write other");
        symlink(&dist, &shim).expect("link dist");
        assert!(bundled_codex_acp_entrypoint_matches(&shim, &dist));
        std::fs::remove_file(&shim).expect("remove link");
        symlink(&other, &shim).expect("link tampered entrypoint");
        assert!(!bundled_codex_acp_entrypoint_matches(&shim, &dist));
    }
}
