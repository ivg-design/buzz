//! Credential-free session shims exposed to model-controlled shells.

use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Session-scoped PATH additions.
///
/// Only deterministic, credential-free helpers are installed. Relay and git
/// signing personalities deliberately stay outside the model shell boundary.
pub struct Shim {
    _dir: TempDir,
    pub path_env: String,
    pub git_env: Vec<(String, String)>,
}

impl Shim {
    pub fn install() -> std::io::Result<Self> {
        let dir = tempfile::Builder::new().prefix("buzz-dev-mcp-").tempdir()?;
        set_owner_only(dir.path())?;

        let self_exe = std::env::current_exe()?;
        for name in ["rg", "tree"] {
            symlink(&self_exe, &dir.path().join(name))?;
        }

        let original = std::env::var_os("PATH").unwrap_or_default();
        let mut entries = vec![PathBuf::from(dir.path())];
        entries.extend(std::env::split_paths(&original));
        let path_env = std::env::join_paths(entries)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?
            .to_string_lossy()
            .into_owned();

        Ok(Self {
            _dir: dir,
            path_env,
            git_env: Vec::new(),
        })
    }
}

#[cfg(unix)]
fn set_owner_only(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = std::fs::metadata(path)?.permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(path, permissions)
}

#[cfg(not(unix))]
fn set_owner_only(_: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn symlink(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(src, dst)
}

#[cfg(not(unix))]
fn symlink(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::copy(src, dst.with_extension("exe")).map(|_| ())
}

pub fn artifact_dir(session_root: &Path) -> PathBuf {
    let path = session_root.join("artifacts");
    let _ = std::fs::create_dir_all(&path);
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installs_only_credential_free_multicall_helpers() {
        let shim = Shim::install().expect("shim");
        let first = std::env::split_paths(&shim.path_env).next().expect("path");
        assert!(first.join(executable("rg")).exists());
        assert!(first.join(executable("tree")).exists());
        assert!(!first.join(executable("buzz")).exists());
        assert!(!first.join(executable("git-credential-nostr")).exists());
        assert!(!first.join(executable("git-sign-nostr")).exists());
        assert!(shim.git_env.is_empty());
    }

    fn executable(name: &str) -> String {
        if cfg!(windows) {
            format!("{name}.exe")
        } else {
            name.to_owned()
        }
    }
}
