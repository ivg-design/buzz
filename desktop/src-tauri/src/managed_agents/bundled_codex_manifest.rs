//! Shared build-time and runtime verification for the bundled native Codex CLI.

use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashSet,
    path::{Component, Path, PathBuf},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Provenance {
    schema_version: u32,
    target: String,
    codex_path: String,
    payloads: Vec<Payload>,
}

#[derive(Debug, Deserialize)]
struct Payload {
    path: String,
    bytes: u64,
    sha256: String,
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
                Component::CurDir
                    | Component::ParentDir
                    | Component::RootDir
                    | Component::Prefix(_)
            )
        })
    {
        return Err(format!("unsafe bundled Codex CLI path {value:?}"));
    }
    Ok(path.to_path_buf())
}

/// Release-like builds always require the payload; debug builds may opt in.
// This source is also compiled into the desktop crate, while this helper is
// consumed only when the same file is included from build.rs.
#[allow(dead_code)]
pub(crate) fn bundle_required(debug_build: bool, explicitly_required: bool) -> bool {
    !debug_build || explicitly_required
}

/// Verify the provenance document and every payload it names.
pub(crate) fn verify_bundle(
    bundle_root: &Path,
    expected_manifest_sha256: Option<&str>,
    expected_target: &str,
) -> Result<(PathBuf, String), String> {
    let canonical_root = bundle_root
        .canonicalize()
        .map_err(|error| format!("failed to resolve {}: {error}", bundle_root.display()))?;
    let manifest_path = bundle_root.join("PROVENANCE.json");
    let manifest_bytes = std::fs::read(&manifest_path)
        .map_err(|error| format!("failed to read {}: {error}", manifest_path.display()))?;
    let manifest_sha256 = sha256(&manifest_bytes);
    if let Some(expected) = expected_manifest_sha256 {
        if expected != manifest_sha256 {
            return Err(format!(
                "bundled Codex CLI provenance checksum mismatch: expected {expected}, got {manifest_sha256}"
            ));
        }
    }

    let provenance: Provenance = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| format!("failed to parse {}: {error}", manifest_path.display()))?;
    if provenance.schema_version != 1 {
        return Err(format!(
            "unsupported bundled Codex CLI provenance schema {}",
            provenance.schema_version
        ));
    }
    if provenance.target != expected_target {
        return Err(format!(
            "bundled Codex CLI target {} does not match application target {expected_target}",
            provenance.target
        ));
    }

    let codex_relative = safe_relative_path(&provenance.codex_path)?;
    let mut seen = HashSet::new();
    let mut codex_verified = false;
    for payload in &provenance.payloads {
        let relative = safe_relative_path(&payload.path)?;
        if !seen.insert(relative.clone()) {
            return Err(format!("duplicate bundled payload path {:?}", payload.path));
        }
        if payload.sha256.len() != 64
            || !payload.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(format!(
                "invalid SHA-256 for bundled Codex CLI payload {:?}",
                payload.path
            ));
        }
        let path = bundle_root.join(&relative);
        let metadata = path
            .symlink_metadata()
            .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(format!(
                "bundled Codex CLI payload must be a regular file: {}",
                path.display()
            ));
        }
        let canonical_path = path
            .canonicalize()
            .map_err(|error| format!("failed to resolve {}: {error}", path.display()))?;
        if !canonical_path.starts_with(&canonical_root) {
            return Err(format!(
                "bundled Codex CLI payload escapes its resource directory: {}",
                path.display()
            ));
        }
        if metadata.len() != payload.bytes {
            return Err(format!(
                "bundled payload size mismatch for {}: expected {}, got {}",
                payload.path,
                payload.bytes,
                metadata.len()
            ));
        }
        let bytes = std::fs::read(&path).map_err(|error| {
            format!("failed to read bundled payload {}: {error}", path.display())
        })?;
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

    Ok((bundle_root.join(codex_relative), manifest_sha256))
}

#[cfg(test)]
mod tests {
    use super::{bundle_required, safe_relative_path, verify_bundle};
    use serde_json::json;

    fn write_manifest(root: &Path, payload: &[u8], recorded_hash: String) -> Vec<u8> {
        std::fs::create_dir_all(root.join("runtime/bin")).unwrap();
        std::fs::write(root.join("runtime/bin/codex"), payload).unwrap();
        let manifest = json!({
            "schemaVersion": 1,
            "target": "aarch64-apple-darwin",
            "codexPath": "runtime/bin/codex",
            "payloads": [{
                "path": "runtime/bin/codex",
                "bytes": payload.len(),
                "sha256": recorded_hash
            }]
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
        let verified = verify_bundle(
            dir.path(),
            Some(&super::sha256(&bytes)),
            "aarch64-apple-darwin",
        )
        .unwrap();
        assert_eq!(verified.0, dir.path().join("runtime/bin/codex"));
    }

    #[test]
    fn verified_manifest_rejects_payload_tampering() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = write_manifest(dir.path(), b"tampered", super::sha256(b"original"));
        let error = verify_bundle(
            dir.path(),
            Some(&super::sha256(&bytes)),
            "aarch64-apple-darwin",
        )
        .unwrap_err();
        assert!(error.contains("payload checksum mismatch"), "{error}");
    }

    #[test]
    fn verified_manifest_rejects_payload_size_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = write_manifest(dir.path(), b"codex", super::sha256(b"codex"));
        std::fs::write(dir.path().join("runtime/bin/codex"), b"codex-extra").unwrap();
        let error = verify_bundle(
            dir.path(),
            Some(&super::sha256(&bytes)),
            "aarch64-apple-darwin",
        )
        .unwrap_err();
        assert!(error.contains("size mismatch"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn verified_manifest_rejects_payload_symlink() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let bytes = write_manifest(dir.path(), b"codex", super::sha256(b"codex"));
        let outside = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(outside.path(), b"codex").unwrap();
        std::fs::remove_file(dir.path().join("runtime/bin/codex")).unwrap();
        symlink(outside.path(), dir.path().join("runtime/bin/codex")).unwrap();
        let error = verify_bundle(
            dir.path(),
            Some(&super::sha256(&bytes)),
            "aarch64-apple-darwin",
        )
        .unwrap_err();
        assert!(error.contains("regular file"), "{error}");
    }

    #[test]
    fn bundle_paths_reject_ambiguous_or_parent_components() {
        assert!(safe_relative_path("../codex").is_err());
        assert!(safe_relative_path("./runtime/bin/codex").is_err());
        assert!(safe_relative_path("runtime/bin/codex").is_ok());
    }

    #[test]
    fn release_requires_bundle_without_an_external_flag() {
        assert!(bundle_required(false, false));
        assert!(bundle_required(false, true));
        assert!(!bundle_required(true, false));
        assert!(bundle_required(true, true));
    }
}
