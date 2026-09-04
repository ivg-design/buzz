//! Checkout-confined path resolution and descriptor-based file I/O.
//!
//! Model-controlled paths are interpreted relative to the immutable checkout
//! root. Absolute paths are accepted only inside that checkout, `~` and parent
//! traversal are rejected, and Unix walks never follow symlinks.

use crate::shell::SharedState;
use rmcp::ErrorData;
use std::path::{Component, Path, PathBuf};

pub(crate) const MAX_FILE_BYTES: u64 = 10 * 1024 * 1024;
pub(crate) const PROTECTED_PATHS_ENV: &str = "BUZZ_MCP_PROTECTED_PATHS_JSON";

#[derive(Clone, Debug, Default)]
pub(crate) struct ProtectedPathPolicy {
    paths: Vec<PathBuf>,
}

impl ProtectedPathPolicy {
    /// Consume the one-shot non-secret policy before constructing tool state.
    pub(crate) fn take_from_environment() -> Result<Self, String> {
        let value = std::env::var_os(PROTECTED_PATHS_ENV);
        std::env::remove_var(PROTECTED_PATHS_ENV);
        let Some(value) = value else {
            return Ok(Self::default());
        };
        let value = value
            .into_string()
            .map_err(|_| format!("{PROTECTED_PATHS_ENV} must be UTF-8"))?;
        Self::from_json(&value)
    }

    fn from_json(value: &str) -> Result<Self, String> {
        let paths: Vec<String> = serde_json::from_str(value)
            .map_err(|error| format!("invalid {PROTECTED_PATHS_ENV}: {error}"))?;
        let mut normalized = Vec::with_capacity(paths.len());
        for raw in paths {
            let path = PathBuf::from(&raw);
            if !path.is_absolute() {
                return Err(format!(
                    "{PROTECTED_PATHS_ENV} entry must be absolute: {raw}"
                ));
            }
            let path = normalize_absolute(&path)
                .ok_or_else(|| format!("{PROTECTED_PATHS_ENV} entry is not normalized: {raw}"))?;
            normalized.push(path);
        }
        normalized.sort();
        normalized.dedup();
        Ok(Self { paths: normalized })
    }

    #[cfg(test)]
    pub(crate) fn from_paths(paths: impl IntoIterator<Item = PathBuf>) -> Self {
        Self {
            paths: paths
                .into_iter()
                .map(|path| path.canonicalize().unwrap_or(path))
                .collect(),
        }
    }

    pub(crate) fn paths(&self) -> &[PathBuf] {
        &self.paths
    }

    pub(crate) fn protects(&self, path: &Path) -> bool {
        self.paths
            .iter()
            .any(|protected| path == protected || path.starts_with(protected))
    }
}

fn normalize_absolute(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                out.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => return None,
        }
    }
    Some(out)
}

pub(crate) struct OpenedFile {
    pub(crate) display: PathBuf,
    pub(crate) relative: PathBuf,
    #[cfg(unix)]
    stat: nix::libc::stat,
}

fn invalid(message: impl Into<String>) -> ErrorData {
    ErrorData::invalid_params(message.into(), None)
}

fn io_error(action: &str, path: &Path, error: impl std::fmt::Display) -> ErrorData {
    ErrorData::internal_error(format!("cannot {action} {}: {error}", path.display()), None)
}

/// Convert an optional workdir and a requested path to a normalized relative
/// path beneath `root`. This lexical gate runs before descriptor traversal.
pub(crate) fn confined_relative_path(
    root: &Path,
    path: &str,
    workdir: Option<&str>,
) -> Result<PathBuf, String> {
    if path.is_empty() {
        return Err("path must not be empty".to_string());
    }
    if path == "~" || path.starts_with("~/") || path.starts_with("~\\") {
        return Err("tilde paths are outside the session checkout".to_string());
    }
    let root = normalize_absolute(root)
        .ok_or_else(|| "session checkout root must be absolute and normalized".to_string())?;
    let workdir_relative = match workdir {
        None => PathBuf::new(),
        Some(raw) => lexical_beneath(&root, Path::new(raw), Path::new(""), "workdir")?,
    };
    lexical_beneath(&root, Path::new(path), &workdir_relative, "path")
}

fn lexical_beneath(
    _root: &Path,
    requested: &Path,
    relative_base: &Path,
    label: &str,
) -> Result<PathBuf, String> {
    if requested.is_absolute() {
        return Err(format!("{label} must be relative to the session checkout"));
    }
    let candidate = relative_base.join(requested);
    let mut out = PathBuf::new();
    for component in candidate.components() {
        match component {
            Component::Normal(value) => out.push(value),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!("{label} must remain inside the session checkout"));
            }
        }
    }
    Ok(out)
}

pub(crate) fn resolve_workdir(
    state: &SharedState,
    workdir: Option<&str>,
) -> Result<PathBuf, String> {
    let relative = confined_relative_path(&state.cwd, ".", workdir)?;
    #[cfg(unix)]
    {
        let _ = open_directory_chain(&state.cwd, &relative)?;
        Ok(state.cwd.join(relative))
    }
    #[cfg(not(unix))]
    {
        let _ = (state, relative);
        Err("generic workdirs require descriptor no-follow support".to_string())
    }
}

pub(crate) fn read_text_file(
    state: &SharedState,
    path: &str,
    workdir: Option<&str>,
) -> Result<(OpenedFile, String), ErrorData> {
    let (opened, bytes) = read_file_bytes(state, path, workdir, MAX_FILE_BYTES)?;
    let content = String::from_utf8(bytes)
        .map_err(|error| io_error("decode as UTF-8", &opened.display, error))?;
    Ok((opened, content))
}

pub(crate) fn read_file_bytes(
    state: &SharedState,
    path: &str,
    workdir: Option<&str>,
    max_bytes: u64,
) -> Result<(OpenedFile, Vec<u8>), ErrorData> {
    let relative = confined_relative_path(&state.cwd, path, workdir).map_err(invalid)?;
    if relative.as_os_str().is_empty() {
        return Err(invalid("path must name a file inside the session checkout"));
    }
    let display = state.cwd.join(&relative);
    #[cfg(unix)]
    {
        read_file_bytes_unix(&state.cwd, relative, display, max_bytes)
    }
    #[cfg(not(unix))]
    {
        let _ = (state, relative, display, max_bytes);
        Err(invalid(
            "generic file reads are unavailable: descriptor-confined no-follow traversal is not implemented on this platform",
        ))
    }
}

#[cfg(unix)]
fn open_directory_chain(root: &Path, relative: &Path) -> Result<std::os::fd::OwnedFd, String> {
    use nix::fcntl::{open, openat, OFlag};
    use nix::sys::stat::Mode;
    let flags = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
    let mut fd = open(root, flags, Mode::empty())
        .map_err(|error| format!("session checkout is not safely accessible: {error}"))?;
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err("path must remain inside the session checkout".to_string());
        };
        fd = openat(&fd, name, flags, Mode::empty())
            .map_err(|error| format!("directory is not safely accessible: {error}"))?;
    }
    Ok(fd)
}

#[cfg(unix)]
fn read_file_bytes_unix(
    root: &Path,
    relative: PathBuf,
    display: PathBuf,
    max_bytes: u64,
) -> Result<(OpenedFile, Vec<u8>), ErrorData> {
    use nix::fcntl::{openat, OFlag};
    use nix::sys::stat::{fstat, Mode, SFlag};
    use std::io::Read;
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    let name = relative
        .file_name()
        .ok_or_else(|| invalid("path must name a file"))?;
    let parent_fd = open_directory_chain(root, parent).map_err(invalid)?;
    let fd = openat(
        &parent_fd,
        name,
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| io_error("open safely", &display, error))?;
    let stat = fstat(&fd).map_err(|error| io_error("inspect", &display, error))?;
    if SFlag::from_bits_truncate(stat.st_mode) != SFlag::S_IFREG {
        return Err(invalid(format!(
            "not a regular file: {}",
            display.display()
        )));
    }
    let length = u64::try_from(stat.st_size).unwrap_or(u64::MAX);
    if length > max_bytes {
        return Err(invalid(format!(
            "file too large: {} is {} bytes (limit {} bytes)",
            display.display(),
            length,
            max_bytes
        )));
    }
    let mut file = std::fs::File::from(fd);
    let mut bytes = Vec::with_capacity(length as usize);
    file.by_ref()
        .take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| io_error("read", &display, error))?;
    if bytes.len() as u64 > max_bytes {
        return Err(invalid(format!(
            "file grew past {max_bytes} bytes during read: {}",
            display.display()
        )));
    }
    Ok((
        OpenedFile {
            display,
            relative,
            stat,
        },
        bytes,
    ))
}

pub(crate) fn ensure_writable(state: &SharedState, target: &OpenedFile) -> Result<(), ErrorData> {
    if target.relative.components().any(|component| {
        matches!(component, Component::Normal(name) if name.to_string_lossy().eq_ignore_ascii_case(".git"))
    }) {
        return Err(invalid("generic file tools may not modify Git authority paths"));
    }
    if state.protected_paths.protects(&target.display) {
        return Err(invalid(format!(
            "path is protected from model writes: {}",
            target.display.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn atomic_replace(
    state: &SharedState,
    target: &OpenedFile,
    content: &[u8],
) -> Result<(), ErrorData> {
    use nix::fcntl::{openat, renameat, AtFlags, OFlag};
    use nix::sys::stat::{fchmod, fstatat, Mode, SFlag};
    use nix::unistd::{unlinkat, UnlinkatFlags};
    use std::io::Write;

    ensure_writable(state, target)?;
    let parent = target.relative.parent().unwrap_or_else(|| Path::new(""));
    let name = target
        .relative
        .file_name()
        .ok_or_else(|| invalid("path must name a file"))?;
    let parent_fd = open_directory_chain(&state.cwd, parent).map_err(invalid)?;
    let current = fstatat(&parent_fd, name, AtFlags::AT_SYMLINK_NOFOLLOW)
        .map_err(|error| io_error("revalidate", &target.display, error))?;
    if SFlag::from_bits_truncate(current.st_mode) != SFlag::S_IFREG
        || current.st_dev != target.stat.st_dev
        || current.st_ino != target.stat.st_ino
    {
        return Err(invalid("file changed while preparing replacement; retry"));
    }

    let temp_name = format!(".buzz-mcp-write-{}-{}", std::process::id(), next_nonce());
    let temp_path = Path::new(&temp_name);
    let fd = openat(
        &parent_fd,
        temp_path,
        OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .map_err(|error| io_error("create replacement", &target.display, error))?;
    let write_result = (|| -> Result<(), ErrorData> {
        fchmod(&fd, Mode::from_bits_truncate(target.stat.st_mode & 0o7777))
            .map_err(|error| io_error("set replacement permissions", &target.display, error))?;
        let mut file = std::fs::File::from(fd);
        file.write_all(content)
            .and_then(|()| file.flush())
            .and_then(|()| file.sync_all())
            .map_err(|error| io_error("write replacement", &target.display, error))?;
        let now = fstatat(&parent_fd, name, AtFlags::AT_SYMLINK_NOFOLLOW)
            .map_err(|error| io_error("revalidate", &target.display, error))?;
        if now.st_dev != target.stat.st_dev || now.st_ino != target.stat.st_ino {
            return Err(invalid("file changed while preparing replacement; retry"));
        }
        renameat(&parent_fd, temp_path, &parent_fd, name)
            .map_err(|error| io_error("install replacement", &target.display, error))?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = unlinkat(&parent_fd, temp_path, UnlinkatFlags::NoRemoveDir);
    }
    write_result
}

#[cfg(not(unix))]
pub(crate) fn atomic_replace(
    state: &SharedState,
    target: &OpenedFile,
    content: &[u8],
) -> Result<(), ErrorData> {
    use std::io::Write;
    ensure_writable(state, target)?;
    let parent = target
        .display
        .parent()
        .ok_or_else(|| invalid("invalid path"))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| io_error("create replacement", &target.display, error))?;
    temporary
        .write_all(content)
        .map_err(|error| io_error("write replacement", &target.display, error))?;
    temporary
        .persist(&target.display)
        .map_err(|error| io_error("install replacement", &target.display, error.error))?;
    Ok(())
}

fn next_nonce() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NONCE: AtomicU64 = AtomicU64::new(0);
    NONCE.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn state(root: &Path) -> SharedState {
        let shim = crate::shim::Shim::install().expect("shim");
        SharedState::new_for_test(root.to_path_buf(), shim, ProtectedPathPolicy::default())
            .expect("state")
    }

    #[test]
    fn confines_absolute_tilde_parent_and_workdir() {
        let root = tempdir().expect("root");
        let outside = tempdir().expect("outside");
        let root_abs = root.path().canonicalize().expect("root canonical");
        assert!(confined_relative_path(&root_abs, "file", None).is_ok());
        assert!(confined_relative_path(
            &root_abs,
            &root_abs.join("file").display().to_string(),
            None
        )
        .is_err());
        assert!(
            confined_relative_path(&root_abs, "file", Some(&root_abs.display().to_string()))
                .is_err()
        );
        assert!(confined_relative_path(&root_abs, "~/secret", None).is_err());
        assert!(confined_relative_path(&root_abs, "../secret", None).is_err());
        assert!(confined_relative_path(
            &root_abs,
            &outside.path().join("secret").display().to_string(),
            None
        )
        .is_err());
        assert!(confined_relative_path(
            &root_abs,
            "file",
            Some(&outside.path().display().to_string())
        )
        .is_err());
    }

    #[test]
    fn protected_policy_rejects_malformed_and_relative_entries() {
        assert!(ProtectedPathPolicy::from_json("not-json").is_err());
        assert!(ProtectedPathPolicy::from_json(r#"["relative/path"]"#).is_err());
        assert!(ProtectedPathPolicy::from_json(r#"["/absolute/../ambiguous"]"#).is_err());
        assert!(ProtectedPathPolicy::from_json(r#"["/absolute/path"]"#).is_ok());
    }

    #[cfg(not(unix))]
    #[test]
    fn file_tools_fail_closed_without_descriptor_no_follow_support() {
        let root = tempdir().expect("root");
        fs::write(root.path().join("ordinary"), "inside").expect("file");
        let state = state(root.path());
        let error = read_text_file(&state, "ordinary", None)
            .err()
            .expect("unsupported platform");
        assert!(format!("{error:?}").contains("no-follow traversal is not implemented"));
    }

    #[cfg(unix)]
    #[test]
    fn read_rejects_intermediate_and_final_symlinks() {
        use std::os::unix::fs::symlink;
        let root = tempdir().expect("root");
        let outside = tempdir().expect("outside");
        fs::write(outside.path().join("secret"), "nope").expect("secret");
        symlink(outside.path(), root.path().join("linked-dir")).expect("dir link");
        symlink(
            outside.path().join("secret"),
            root.path().join("linked-file"),
        )
        .expect("file link");
        let state = state(root.path());
        assert!(read_text_file(&state, "linked-dir/secret", None).is_err());
        assert!(read_text_file(&state, "linked-file", None).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn atomic_replace_refuses_symlink_swap_and_preserves_outside() {
        use std::os::unix::fs::symlink;
        let root = tempdir().expect("root");
        let outside = tempdir().expect("outside");
        let victim = outside.path().join("victim");
        fs::write(&victim, "operator").expect("victim");
        let local = root.path().join("local");
        fs::write(&local, "model").expect("local");
        let state = state(root.path());
        let (opened, _) = read_text_file(&state, "local", None).expect("read");
        fs::remove_file(&local).expect("remove");
        symlink(&victim, &local).expect("swap");
        assert!(atomic_replace(&state, &opened, b"poison").is_err());
        assert_eq!(
            fs::read_to_string(&victim).expect("victim read"),
            "operator"
        );
    }

    #[cfg(unix)]
    #[test]
    fn concurrent_symlink_swaps_never_disclose_outside_reads() {
        use std::os::unix::fs::symlink;
        use std::sync::Arc;
        let root = tempdir().expect("root");
        let outside = tempdir().expect("outside");
        let victim = outside.path().join("victim");
        fs::write(&victim, "operator-secret").expect("victim");
        let local = root.path().join("local");
        fs::write(&local, "inside").expect("local");
        let state = state(root.path());
        let local = Arc::new(local);
        let victim_for_writer = victim.clone();
        let local_for_writer = Arc::clone(&local);
        let writer = std::thread::spawn(move || {
            for _ in 0..250 {
                let _ = fs::remove_file(&*local_for_writer);
                let _ = symlink(&victim_for_writer, &*local_for_writer);
                std::thread::yield_now();
                let _ = fs::remove_file(&*local_for_writer);
                let _ = fs::write(&*local_for_writer, "inside");
            }
        });
        for _ in 0..500 {
            if let Ok((_, value)) = read_text_file(&state, "local", None) {
                assert!(
                    value.is_empty() || value == "inside",
                    "outside read: {value:?}"
                );
            }
        }
        writer.join().expect("writer");
        assert_eq!(
            fs::read_to_string(victim).expect("victim"),
            "operator-secret"
        );
    }
}
