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
    read_roots: Vec<PathBuf>,
}

impl Shim {
    pub fn install() -> std::io::Result<Self> {
        let dir = tempfile::Builder::new().prefix("buzz-dev-mcp-").tempdir()?;
        set_owner_only(dir.path())?;

        let self_exe = std::env::current_exe()?;
        for name in ["rg", "tree"] {
            symlink(&self_exe, &dir.path().join(name))?;
        }
        // `/usr/bin/git` is an xcrun launcher on macOS. It writes an ambient
        // per-user cache outside the session before starting Git, which a
        // deny-by-default model sandbox must not permit. Resolve the immutable
        // developer-tool binary up front and expose only that binary through
        // the private shim directory.
        #[cfg(target_os = "macos")]
        if let Some(git) = [
            PathBuf::from("/Library/Developer/CommandLineTools/usr/bin/git"),
            PathBuf::from("/Applications/Xcode.app/Contents/Developer/usr/bin/git"),
        ]
        .into_iter()
        .find(|candidate| candidate.is_file())
        {
            symlink(&git, &dir.path().join("git"))?;
        }

        let mut entries = vec![PathBuf::from(dir.path())];
        // Never relay the ambient PATH into the model shell. It may contain a
        // checkout-local bin directory or an auth-helper directory chosen by
        // the launching application. System locations come first; a small set
        // of canonical operator toolchain bins follows for cargo/node workflows.
        #[cfg(unix)]
        {
            for candidate in [
                PathBuf::from("/usr/bin"),
                PathBuf::from("/bin"),
                PathBuf::from("/usr/sbin"),
                PathBuf::from("/sbin"),
                PathBuf::from("/opt/homebrew/bin"),
                PathBuf::from("/usr/local/bin"),
            ] {
                push_canonical_directory(&mut entries, candidate);
            }
            if let Some(home) = std::env::var_os("HOME")
                .map(PathBuf::from)
                .and_then(|path| path.canonicalize().ok())
            {
                for relative in [".cargo/bin", ".local/bin"] {
                    push_canonical_directory(&mut entries, home.join(relative));
                }
                if let Some(original) = std::env::var_os("PATH") {
                    let nvm_root = home.join(".nvm/versions");
                    for candidate in std::env::split_paths(&original) {
                        if candidate.starts_with(&nvm_root)
                            && candidate.file_name().is_some_and(|name| name == "bin")
                        {
                            push_canonical_directory(&mut entries, candidate);
                        }
                    }
                }
            }
        }
        let path_env = std::env::join_paths(entries)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?
            .to_string_lossy()
            .into_owned();

        let mut read_roots = vec![dir.path().canonicalize()?];
        if let Some(parent) = self_exe.parent().and_then(|path| path.canonicalize().ok()) {
            read_roots.push(parent);
        }
        Ok(Self {
            _dir: dir,
            path_env,
            git_env: Vec::new(),
            read_roots,
        })
    }

    pub(crate) fn read_roots(&self) -> &[PathBuf] {
        &self.read_roots
    }
}

#[cfg(unix)]
fn push_canonical_directory(entries: &mut Vec<PathBuf>, candidate: PathBuf) {
    if let Ok(canonical) = candidate.canonicalize() {
        if canonical.is_dir() && !entries.contains(&canonical) {
            entries.push(canonical);
        }
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
