//! Private one-shot startup channel from the desktop to `buzz-acp`.

use serde::Serialize;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use zeroize::{Zeroize, Zeroizing};

pub(super) const MARKER_ENV: &str = "BUZZ_ACP_STARTUP_STDIN";
const SCHEMA_VERSION: &str = "buzz.acp-startup.v1";
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);

const STARTUP_ENV_KEYS: &[&str] = &[
    "BUZZ_PRIVATE_KEY",
    "BUZZ_ACP_PRIVATE_KEY",
    "NOSTR_PRIVATE_KEY",
    "BUZZ_AUTH_TAG",
    "BUZZ_ACP_JOB_GRANTS_JSON",
    "BUZZ_ACP_JOB_GRANTS_FILE",
    "BUZZ_ACP_JOB_LEDGER_DIR",
    "BUZZ_ACP_OWNER_GITHUB_LOGIN",
    "BUZZ_ACP_ALLOW_INSECURE_LOOPBACK_JOBS",
    "BUZZ_ACP_SETUP_PAYLOAD",
];

pub(super) struct StartupPayload<'a> {
    private_key: &'a str,
    auth_tag: Option<&'a str>,
    job_grants_json: Option<Zeroizing<String>>,
    job_grants_file: Option<PathBuf>,
    job_ledger_dir: Option<PathBuf>,
    owner_github_login: Option<String>,
    allow_insecure_loopback_jobs: bool,
    setup_payload: Option<&'a str>,
}

#[derive(Serialize)]
struct WirePayload<'a> {
    schema_version: &'static str,
    private_key: &'a str,
    auth_tag: Option<&'a str>,
    job_grants_json: Option<&'a str>,
    job_grants_file: Option<&'a std::path::Path>,
    job_ledger_dir: Option<&'a std::path::Path>,
    owner_github_login: Option<&'a str>,
    allow_insecure_loopback_jobs: bool,
    setup_payload: Option<&'a str>,
}

impl<'a> StartupPayload<'a> {
    pub(super) fn capture(
        private_key: &'a str,
        auth_tag: Option<&'a str>,
        setup_payload: Option<&'a str>,
    ) -> Result<Self, String> {
        if private_key.is_empty() || private_key.len() > 256 {
            return Err("managed agent private key has an invalid length".into());
        }
        let explicit_json = nonempty_env("BUZZ_ACP_JOB_GRANTS_JSON").map(Zeroizing::new);
        let explicit_file = path_env("BUZZ_ACP_JOB_GRANTS_FILE")?;
        let desktop_json = if explicit_json.is_none() && explicit_file.is_none() {
            Some(crate::commands::load_managed_agent_grants_json()?)
        } else {
            None
        };
        let (job_grants_json, job_grants_file) =
            resolve_job_grant_sources(explicit_json, explicit_file, desktop_json)?;
        Ok(Self {
            private_key,
            auth_tag,
            job_grants_json,
            job_grants_file,
            job_ledger_dir: path_env("BUZZ_ACP_JOB_LEDGER_DIR")?,
            owner_github_login: nonempty_env("BUZZ_ACP_OWNER_GITHUB_LOGIN"),
            allow_insecure_loopback_jobs: nonempty_env("BUZZ_ACP_ALLOW_INSECURE_LOOPBACK_JOBS")
                .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes")),
            setup_payload,
        })
    }

    pub(super) fn configure_command(command: &mut std::process::Command) {
        for name in STARTUP_ENV_KEYS {
            command.env_remove(name);
        }
        command.env(MARKER_ENV, "1");
    }

    pub(super) fn deliver(&self, child: &mut std::process::Child) -> Result<(), String> {
        let wire = WirePayload {
            schema_version: SCHEMA_VERSION,
            private_key: self.private_key,
            auth_tag: self.auth_tag,
            job_grants_json: self.job_grants_json.as_ref().map(|value| value.as_str()),
            job_grants_file: self.job_grants_file.as_deref(),
            job_ledger_dir: self.job_ledger_dir.as_deref(),
            owner_github_login: self.owner_github_login.as_deref(),
            allow_insecure_loopback_jobs: self.allow_insecure_loopback_jobs,
            setup_payload: self.setup_payload,
        };
        let mut encoded = Zeroizing::new(
            serde_json::to_vec(&wire)
                .map_err(|error| format!("failed to encode secure startup payload: {error}"))?,
        );
        if encoded.len() > MAX_PAYLOAD_BYTES {
            return Err(format!(
                "secure startup payload exceeds {MAX_PAYLOAD_BYTES} bytes"
            ));
        }
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| "secure startup pipe was not opened".to_owned())?;

        #[cfg(unix)]
        {
            let result = write_payload_bounded(&mut stdin, &encoded, DELIVERY_TIMEOUT);
            encoded.zeroize();
            drop(stdin);
            if let Err(error) = result {
                let termination = super::process::terminate_process(child.id());
                let _ = child.wait();
                return match termination {
                    Ok(()) => Err(error),
                    Err(termination) => Err(format!(
                        "{error}; failed to terminate rejected ACP process: {termination}"
                    )),
                };
            }
            Ok(())
        }

        #[cfg(not(unix))]
        {
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            let writer = std::thread::spawn(move || {
                let result = stdin
                    .write_all(&encoded)
                    .and_then(|()| stdin.flush())
                    .map_err(|error| format!("failed to deliver secure startup payload: {error}"));
                encoded.zeroize();
                drop(stdin);
                let _ = tx.send(result);
            });
            match rx.recv_timeout(DELIVERY_TIMEOUT) {
                Ok(result) => {
                    let _ = writer.join();
                    result
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // Closing the child end unblocks the writer so secret-bearing
                    // memory cannot outlive this failed spawn indefinitely.
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = writer.join();
                    Err("secure startup payload delivery timed out".into())
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    let _ = writer.join();
                    Err("secure startup payload writer stopped unexpectedly".into())
                }
            }
        }
    }
}

fn resolve_job_grant_sources(
    explicit_json: Option<Zeroizing<String>>,
    explicit_file: Option<PathBuf>,
    desktop_json: Option<Zeroizing<String>>,
) -> Result<(Option<Zeroizing<String>>, Option<PathBuf>), String> {
    match (explicit_json, explicit_file, desktop_json) {
        (Some(_), Some(_), _) => {
            Err("set only one of BUZZ_ACP_JOB_GRANTS_JSON or BUZZ_ACP_JOB_GRANTS_FILE".into())
        }
        (Some(json), None, _) => Ok((Some(json), None)),
        (None, Some(file), _) => Ok((None, Some(file))),
        (None, None, Some(json)) => Ok((Some(json), None)),
        (None, None, None) => Ok((None, None)),
    }
}

/// Write the startup secret without moving it into an unbounded helper thread.
///
/// A child that never reads stdin must not be able to retain a desktop thread
/// containing the managed-agent key. Unix pipes can be made non-blocking, so
/// the payload remains in this bounded call and is zeroized before the rejected
/// process group is terminated by the caller.
#[cfg(unix)]
fn write_payload_bounded(
    stdin: &mut std::process::ChildStdin,
    payload: &[u8],
    timeout: Duration,
) -> Result<(), String> {
    use std::os::fd::AsRawFd;

    let fd = stdin.as_raw_fd();
    let original_flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if original_flags == -1 {
        return Err(format!(
            "failed to inspect secure startup pipe: {}",
            std::io::Error::last_os_error()
        ));
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, original_flags | libc::O_NONBLOCK) } == -1 {
        return Err(format!(
            "failed to bound secure startup pipe: {}",
            std::io::Error::last_os_error()
        ));
    }

    let deadline = std::time::Instant::now() + timeout;
    let result = (|| {
        let mut offset = 0;
        while offset < payload.len() {
            match stdin.write(&payload[offset..]) {
                Ok(0) => return Err("secure startup pipe closed before delivery".to_owned()),
                Ok(written) => offset += written,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if std::time::Instant::now() >= deadline {
                        return Err("secure startup payload delivery timed out".to_owned());
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => {
                    return Err(format!("failed to deliver secure startup payload: {error}"))
                }
            }
        }
        stdin
            .flush()
            .map_err(|error| format!("failed to deliver secure startup payload: {error}"))
    })();

    // Best effort: the descriptor is dropped immediately after this function,
    // so failure to restore blocking mode cannot extend secret lifetime.
    unsafe {
        libc::fcntl(fd, libc::F_SETFL, original_flags);
    }
    result
}

/// Resolve the only executable allowed to receive the startup secret payload.
///
/// The persisted `acp_command` remains useful for diagnostics, but it is not
/// an authority to redirect managed-agent signing keys to another program.
pub(super) fn trusted_harness_path(resolved: &Path) -> Result<PathBuf, String> {
    let expected = super::sweep::expected_harness_exe_path()
        .ok_or_else(|| "cannot resolve the bundled buzz-acp harness".to_owned())?;
    verify_exact_harness_path(resolved, &expected)
}

fn verify_exact_harness_path(resolved: &Path, expected: &Path) -> Result<PathBuf, String> {
    let resolved = std::fs::canonicalize(resolved)
        .map_err(|_| "cannot verify the configured ACP harness executable".to_owned())?;
    let expected = std::fs::canonicalize(expected)
        .map_err(|_| "the bundled buzz-acp harness is unavailable".to_owned())?;
    if resolved != expected {
        return Err(
            "managed-agent secrets may be delivered only to this Buzz installation's bundled buzz-acp harness"
                .into(),
        );
    }
    Ok(expected)
}

/// Re-check the kernel-observed child image after `exec` and before writing a
/// secret byte. This closes the path-replacement window between resolution and
/// spawn on supported Unix release targets.
#[cfg(target_os = "macos")]
pub(super) fn verify_spawned_harness(
    child: &std::process::Child,
    expected: &Path,
) -> Result<(), String> {
    let actual = super::sweep::proc_exe_path_from_procargs2(child.id())
        .and_then(|path| std::fs::canonicalize(path).ok())
        .ok_or_else(|| "cannot verify the spawned buzz-acp executable".to_owned())?;
    if actual == expected {
        Ok(())
    } else {
        Err("spawned ACP process does not match the bundled buzz-acp executable".into())
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
pub(super) fn verify_spawned_harness(
    child: &std::process::Child,
    expected: &Path,
) -> Result<(), String> {
    let actual = std::fs::read_link(format!("/proc/{}/exe", child.id()))
        .ok()
        .and_then(|path| std::fs::canonicalize(path).ok())
        .ok_or_else(|| "cannot verify the spawned buzz-acp executable".to_owned())?;
    if actual == expected {
        Ok(())
    } else {
        Err("spawned ACP process does not match the bundled buzz-acp executable".into())
    }
}

#[cfg(not(unix))]
pub(super) fn verify_spawned_harness(
    _child: &std::process::Child,
    _expected: &Path,
) -> Result<(), String> {
    // The exact canonical path was checked immediately before CreateProcess.
    // Windows does not expose a dependency-free post-spawn image lookup here.
    Ok(())
}

fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn path_env(name: &str) -> Result<Option<PathBuf>, String> {
    std::env::var_os(name)
        .map(|value| {
            if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(PathBuf::from(value)))
            }
        })
        .transpose()
        .map(Option::flatten)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_boundary_contains_only_nonsecret_marker() {
        let mut command = std::process::Command::new("true");
        for name in STARTUP_ENV_KEYS {
            command.env(name, format!("sentinel-{name}"));
        }
        StartupPayload::configure_command(&mut command);
        let changes: std::collections::HashMap<_, _> = command
            .get_envs()
            .map(|(name, value)| (name.to_owned(), value.map(ToOwned::to_owned)))
            .collect();
        assert_eq!(
            changes.get(std::ffi::OsStr::new(MARKER_ENV)),
            Some(&Some(std::ffi::OsString::from("1")))
        );
        for name in STARTUP_ENV_KEYS {
            assert_eq!(changes.get(std::ffi::OsStr::new(name)), Some(&None));
        }
    }

    #[test]
    fn authenticated_desktop_grants_are_inline_without_overriding_explicit_sources() {
        let desktop = Zeroizing::new(r#"{"version":1,"grants":[]}"#.to_string());
        let (json, file) =
            resolve_job_grant_sources(None, None, Some(desktop)).expect("resolve desktop grants");
        assert!(json.is_some());
        assert!(file.is_none());

        let explicit_json = Zeroizing::new(r#"{"version":1,"grants":[]}"#.to_string());
        let (json, file) = resolve_job_grant_sources(Some(explicit_json), None, None)
            .expect("resolve explicit inline grants");
        assert!(json.is_some());
        assert!(file.is_none());

        let directory = tempfile::tempdir().expect("temporary grants directory");
        assert_eq!(
            resolve_job_grant_sources(None, Some(directory.path().join("external.json")), None)
                .expect("resolve explicit file")
                .1,
            Some(directory.path().join("external.json"))
        );
    }

    #[test]
    fn ambiguous_external_grant_sources_fail_closed() {
        let inline = Zeroizing::new(r#"{"version":1,"grants":[]}"#.to_string());
        let file = PathBuf::from("external.json");
        assert!(resolve_job_grant_sources(Some(inline), Some(file), None).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn exact_harness_path_accepts_symlink_to_expected_and_rejects_other_binary() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("harness fixture directory");
        let expected = directory.path().join("buzz-acp");
        let other = directory.path().join("custom-acp");
        let alias = directory.path().join("buzz-acp-alias");
        std::fs::write(&expected, b"expected").expect("expected fixture");
        std::fs::write(&other, b"other").expect("other fixture");
        symlink(&expected, &alias).expect("harness symlink");

        assert_eq!(
            verify_exact_harness_path(&alias, &expected).expect("verified alias"),
            expected.canonicalize().expect("canonical expected")
        );
        assert!(verify_exact_harness_path(&other, &expected).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn spawned_process_environment_and_argv_never_contain_piped_secrets() {
        use std::time::{Duration, Instant};

        let directory = tempfile::tempdir().expect("temporary startup-pipe directory");
        let env_capture = directory.path().join("child.env");
        let private_sentinel = "nsec1-private-startup-sentinel";
        let auth_sentinel = "auth-tag-private-startup-sentinel";
        let payload = StartupPayload::capture(private_sentinel, Some(auth_sentinel), None)
            .expect("capture startup payload");

        let mut command = std::process::Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("cat >/dev/null; /usr/bin/env > \"$1\"; sleep 30")
            .arg("buzz-startup-test")
            .arg(&env_capture)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        for name in STARTUP_ENV_KEYS {
            command.env(name, format!("legacy-{name}-{private_sentinel}"));
        }
        StartupPayload::configure_command(&mut command);
        let mut child = command.spawn().expect("spawn startup-pipe probe");
        payload
            .deliver(&mut child)
            .expect("deliver startup payload");

        let deadline = Instant::now() + Duration::from_secs(2);
        while !env_capture.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let child_env = std::fs::read_to_string(&env_capture).expect("captured child env");
        let ps = std::process::Command::new("/bin/ps")
            .args(["eww", "-p", &child.id().to_string()])
            .output()
            .expect("inspect spawned process")
            .stdout;
        let process_view = String::from_utf8_lossy(&ps);

        for secret in [private_sentinel, auth_sentinel] {
            assert!(
                !child_env.contains(secret),
                "secret leaked into child environment"
            );
            assert!(
                !process_view.contains(secret),
                "secret leaked into process argv/env"
            );
        }
        assert!(child_env.contains("BUZZ_ACP_STARTUP_STDIN=1"));

        let _ = child.kill();
        let _ = child.wait();
    }

    #[cfg(unix)]
    #[test]
    fn stalled_startup_reader_hits_a_bounded_nonblocking_deadline() {
        use std::os::unix::process::CommandExt;

        let mut command = std::process::Command::new("/bin/sh");
        command
            .args(["-c", "sleep 30"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .process_group(0);
        let mut child = command.spawn().expect("spawn stalled startup reader");
        let mut stdin = child.stdin.take().expect("startup pipe");
        let payload = vec![b'x'; MAX_PAYLOAD_BYTES];
        let started = std::time::Instant::now();
        let error = write_payload_bounded(&mut stdin, &payload, Duration::from_millis(50))
            .expect_err("a full unread pipe must time out");
        assert!(error.contains("timed out"));
        assert!(started.elapsed() < Duration::from_secs(1));

        drop(stdin);
        let _ = super::super::process::terminate_process(child.id());
        let _ = child.wait();
    }
}

#[cfg(test)]
#[path = "startup_pipe_closed_relay_tests.rs"]
mod closed_relay_tests;
