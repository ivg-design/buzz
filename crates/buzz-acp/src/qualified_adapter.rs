//! Immutable adapter qualification for privileged A2A Job sessions.

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

const DARWIN_ARM64_RUNTIME_SHA256: &str =
    "a2040fe41ef0fd64789801a73165280594339194966d1bdbf8b874b006efc831";
const DARWIN_X64_RUNTIME_SHA256: &str =
    "d9a97f0eab8a57d20f3d1f8d1f9b84cb843a438b5309396e90db8ab17fe054e4";
const WINDOWS_X64_RUNTIME_SHA256: &str =
    "88586945dfd3353ca49659af7593d1a256addeb71e6d31bcea04e34640b7a619";
const WINDOWS_ARM64_RUNTIME_SHA256: &str =
    "959224a2d434d25c352510aae19eb7db4be5496c4d6019d336cabd86c3fe01f1";

fn collect_files(root: &Path, directory: &Path, files: &mut Vec<(String, Vec<u8>)>) -> Option<()> {
    let mut entries = std::fs::read_dir(directory)
        .ok()?
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        // npm's platform-specific launch wrappers are never executed by Buzz;
        // Desktop launches the verified index.js through managed Node.
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
            let relative = path.strip_prefix(root).ok()?;
            let relative = relative
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

pub(crate) fn directory_tree_sha256(root: &Path) -> Option<String> {
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

fn qualified_runtime_path(command: &str, args: &[String]) -> Option<PathBuf> {
    let command_name = Path::new(command)
        .file_name()?
        .to_str()?
        .to_ascii_lowercase();
    if !matches!(command_name.as_str(), "node" | "node.exe") {
        return None;
    }
    let index = PathBuf::from(args.first()?);
    if !index.is_absolute() || index.file_name()?.to_str()? != "index.js" {
        return None;
    }
    let dist = index.parent()?;
    if dist.file_name()?.to_str()? != "dist"
        || dist.parent()?.file_name()?.to_str()? != "claude-agent-acp"
        || dist.parent()?.parent()?.file_name()?.to_str()? != "@agentclientprotocol"
    {
        return None;
    }
    Some(dist.parent()?.to_path_buf())
}

fn is_checksum_qualified_claude_adapter_with_expected(
    command: &str,
    args: &[String],
    expected: &str,
) -> bool {
    qualified_runtime_path(command, args)
        .and_then(|runtime| directory_tree_sha256(&runtime))
        .as_deref()
        == Some(expected)
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

pub(crate) fn is_checksum_qualified_claude_adapter(command: &str, args: &[String]) -> bool {
    expected_runtime_sha256().is_some_and(|expected| {
        is_checksum_qualified_claude_adapter_with_expected(command, args, expected)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (tempfile::TempDir, PathBuf, String) {
        let temp = tempfile::tempdir().unwrap();
        let dist = temp
            .path()
            .join("@agentclientprotocol/claude-agent-acp/dist");
        std::fs::create_dir_all(&dist).unwrap();
        std::fs::write(dist.join("index.js"), b"index").unwrap();
        std::fs::write(dist.join("acp-agent.js"), b"policy").unwrap();
        let digest = directory_tree_sha256(dist.parent().unwrap()).unwrap();
        (temp, dist, digest)
    }

    #[test]
    fn tree_digest_binds_paths_contents_and_inventory() {
        let (_temp, dist, digest) = fixture();
        let runtime = dist.parent().unwrap();
        std::fs::write(dist.join("acp-agent.js"), b"tampered").unwrap();
        assert_ne!(
            directory_tree_sha256(runtime).as_deref(),
            Some(digest.as_str())
        );
        std::fs::write(dist.join("extra.js"), b"extra").unwrap();
        assert_ne!(
            directory_tree_sha256(runtime).as_deref(),
            Some(digest.as_str())
        );
    }

    #[cfg(unix)]
    #[test]
    fn tree_digest_rejects_symlinks() {
        use std::os::unix::fs::symlink;
        let (_temp, dist, _) = fixture();
        symlink(dist.join("index.js"), dist.join("alias.js")).unwrap();
        assert!(directory_tree_sha256(&dist).is_none());
    }

    #[test]
    fn launch_shape_requires_node_and_scoped_package_path() {
        let (_temp, dist, digest) = fixture();
        let args = vec![dist.join("index.js").display().to_string()];
        assert_eq!(
            qualified_runtime_path("/verified/node", &args),
            dist.parent().map(Path::to_path_buf)
        );
        assert!(is_checksum_qualified_claude_adapter_with_expected(
            "/verified/node",
            &args,
            &digest,
        ));
        assert!(qualified_runtime_path("claude-agent-acp", &args).is_none());
        assert!(qualified_runtime_path("node", &[]).is_none());
    }

    #[test]
    fn target_digest_is_present_only_for_supported_release_targets() {
        let expected = expected_runtime_sha256();
        let supported = matches!(
            (std::env::consts::OS, std::env::consts::ARCH),
            ("macos", "aarch64")
                | ("macos", "x86_64")
                | ("windows", "x86_64")
                | ("windows", "aarch64")
        );
        assert_eq!(expected.is_some(), supported);
        assert!(runtime_sha256_for_target("linux", "x86_64").is_none());
        assert!(runtime_sha256_for_target("macos", "powerpc").is_none());
    }
}
