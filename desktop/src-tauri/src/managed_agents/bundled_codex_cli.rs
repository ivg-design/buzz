//! Verified native Codex CLI shipped inside a release bundle.

use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use std::{
    path::{Component, Path, PathBuf},
    process::Command,
    sync::OnceLock,
};

const EXPECTED_MANIFEST_SHA256: Option<&str> =
    option_env!("BUZZ_DESKTOP_BUNDLED_CODEX_CLI_MANIFEST_SHA256");
const EXPECTED_TARGET: Option<&str> =
    option_env!("BUZZ_DESKTOP_BUNDLED_CODEX_CLI_TARGET");

static RESOURCE_DIR: OnceLock<PathBuf> = OnceLock::new();
static VERIFIED_CLI: OnceLock<CliState> = OnceLock::new();

#[derive(Debug)]
enum CliState {
    NotConfigured,
    Ready(PathBuf),
    Invalid(String),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Provenance {
    target: String,
    codex_path: String,
    payloads: Vec<Payload>,
}

#[derive(Debug, Deserialize)]
struct Payload {
    path: String,
    sha256: String,
}

pub(crate) fn initialize_resource_dir(path: PathBuf) {
    let _ = RESOURCE_DIR.set(path);
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn safe_relative_path(value: &str) -> Result<PathBuf, String> {
    let path = Path::new(value);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(format!("unsafe bundled Codex CLI path {value:?}"));
    }
    Ok(path.to_path_buf())
}

fn verify_cli(
    bundle_root: &Path,
    expected_manifest_sha256: &str,
    expected_target: &str,
) -> Result<PathBuf, String> {
    let manifest_path = bundle_root.join("PROVENANCE.json");
    let manifest_bytes = std::fs::read(&manifest_path)
        .map_err(|error| format!("failed to read {}: {error}", manifest_path.display()))?;
    let actual_manifest_sha256 = sha256(&manifest_bytes);
    if actual_manifest_sha256 != expected_manifest_sha256 {
        return Err(format!(
            "bundled Codex CLI provenance checksum mismatch: expected {expected_manifest_sha256}, got {actual_manifest_sha256}"
        ));
    }

    let provenance: Provenance = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| format!("failed to parse {}: {error}", manifest_path.display()))?;
    if provenance.target != expected_target {
        return Err(format!(
            "bundled Codex CLI target {} does not match application target {expected_target}",
            provenance.target
        ));
    }

    let codex_relative = safe_relative_path(&provenance.codex_path)?;
    let mut codex_verified = false;
    for payload in &provenance.payloads {
        let relative = safe_relative_path(&payload.path)?;
        if payload.sha256.len() != 64 || !payload.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(format!(
                "invalid SHA-256 for bundled Codex CLI payload {:?}",
                payload.path
            ));
        }
        let path = bundle_root.join(&relative);
        let bytes = std::fs::read(&path)
            .map_err(|error| format!("failed to read bundled payload {}: {error}", path.display()))?;
        let actual = sha256(&bytes);
        if !actual.eq_ignore_ascii_case(&payload.sha256) {
            return Err(format!(
                "bundled payload checksum mismatch for {}: expected {}, got {actual}",
                payload.path, payload.sha256
            ));
        }
        codex_verified |= relative == codex_relative;
    }
    if !codex_verified {
        return Err("bundled Codex CLI provenance does not verify the CLI path".to_string());
    }
    Ok(bundle_root.join(codex_relative))
}

fn cli_state() -> &'static CliState {
    VERIFIED_CLI.get_or_init(|| {
        let (Some(expected_sha), Some(expected_target)) =
            (EXPECTED_MANIFEST_SHA256, EXPECTED_TARGET)
        else {
            return CliState::NotConfigured;
        };
        let Some(resource_dir) = RESOURCE_DIR.get() else {
            return CliState::Invalid(
                "Tauri resource directory was not initialized before Codex launch".to_string(),
            );
        };
        match verify_cli(
            &resource_dir.join("codex-cli"),
            expected_sha,
            expected_target,
        ) {
            Ok(path) => CliState::Ready(path),
            Err(error) => CliState::Invalid(error),
        }
    })
}

/// Bind an adapter process to the verified native Codex executable.
///
/// Development builds without a bundled CLI preserve their existing PATH
/// behavior. Release builds fail closed if the packaged payload is absent or
/// does not match its build-stamped provenance.
pub(crate) fn configure_command(command: &mut Command) -> Result<(), String> {
    match cli_state() {
        CliState::NotConfigured => Ok(()),
        CliState::Ready(path) => {
            command.env("CODEX_PATH", path);
            Ok(())
        }
        CliState::Invalid(error) => {
            command.env_remove("CODEX_PATH");
            Err(format!("invalid bundled Codex CLI: {error}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{safe_relative_path, verify_cli};
    use serde_json::json;

    fn write_manifest(root: &Path, payload: &[u8], recorded_hash: String) -> Vec<u8> {
        std::fs::create_dir_all(root.join("runtime/bin")).unwrap();
        std::fs::write(root.join("runtime/bin/codex"), payload).unwrap();
        let manifest = json!({
            "target": "aarch64-apple-darwin",
            "codexPath": "runtime/bin/codex",
            "payloads": [{"path": "runtime/bin/codex", "sha256": recorded_hash}]
        });
        let bytes = serde_json::to_vec(&manifest).unwrap();
        std::fs::write(root.join("PROVENANCE.json"), &bytes).unwrap();
        bytes
    }

    use std::path::Path;

    #[test]
    fn verified_manifest_binds_cli_payload() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = write_manifest(dir.path(), b"codex", super::sha256(b"codex"));
        let path = verify_cli(
            dir.path(),
            &super::sha256(&bytes),
            "aarch64-apple-darwin",
        )
        .unwrap();
        assert_eq!(path, dir.path().join("runtime/bin/codex"));
    }

    #[test]
    fn verified_manifest_rejects_payload_tampering() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = write_manifest(dir.path(), b"tampered", super::sha256(b"original"));
        let error = verify_cli(
            dir.path(),
            &super::sha256(&bytes),
            "aarch64-apple-darwin",
        )
        .unwrap_err();
        assert!(error.contains("payload checksum mismatch"), "{error}");
    }

    #[test]
    fn bundle_paths_reject_parent_traversal() {
        assert!(safe_relative_path("../codex").is_err());
        assert!(safe_relative_path("runtime/bin/codex").is_ok());
    }
}
