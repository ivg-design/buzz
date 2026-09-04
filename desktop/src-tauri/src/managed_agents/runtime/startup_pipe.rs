//! Private one-shot startup channel from the desktop to `buzz-acp`.

use serde::Serialize;
use std::io::Write;
use std::path::PathBuf;
use zeroize::{Zeroize, Zeroizing};

pub(super) const MARKER_ENV: &str = "BUZZ_ACP_STARTUP_STDIN";
const SCHEMA_VERSION: &str = "buzz.acp-startup.v1";
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

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
        Ok(Self {
            private_key,
            auth_tag,
            job_grants_json: nonempty_env("BUZZ_ACP_JOB_GRANTS_JSON").map(Zeroizing::new),
            job_grants_file: path_env("BUZZ_ACP_JOB_GRANTS_FILE")?,
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
        let result = stdin
            .write_all(&encoded)
            .and_then(|()| stdin.flush())
            .map_err(|error| format!("failed to deliver secure startup payload: {error}"));
        encoded.zeroize();
        drop(stdin);
        result
    }
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
}

#[cfg(test)]
#[path = "startup_pipe_closed_relay_tests.rs"]
mod closed_relay_tests;
