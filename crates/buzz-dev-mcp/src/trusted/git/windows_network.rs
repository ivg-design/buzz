//! One-shot Windows credential delivery for trusted GitHub operations.

#![allow(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use zeroize::{Zeroize as _, Zeroizing};

use super::{
    github_repository_path, parse_filled_credential, private_tempdir, run_managed,
    set_process_group, GitHubCredential, GitHubCredentialStore, GitHubCredentialStoreInner,
    ProcessGroupGuard, MAX_CREDENTIAL_OUTPUT_BYTES,
};

pub(super) async fn capture_operator_github_credentials_with(
    git: &Path,
    repositories: &[String],
    timeout: Duration,
) -> Result<GitHubCredentialStore, String> {
    let operator_profile = operator_profile()?;
    let deadline = tokio::time::Instant::now() + timeout;
    let repositories = repositories.iter().collect::<BTreeSet<_>>();
    let capture_directory = private_tempdir("buzz-git-credential-capture-")?;
    let mut by_repository = BTreeMap::new();
    for repository in repositories {
        let path = github_repository_path(repository)?;
        let mut query = Zeroizing::new(Vec::with_capacity(path.len() + 48));
        query.extend_from_slice(b"protocol=https\nhost=github.com\npath=");
        query.extend_from_slice(path.as_bytes());
        query.extend_from_slice(b"\n\n");

        let mut command = Command::new(git);
        command
            .args(["-c", "credential.useHttpPath=true", "credential", "fill"])
            .current_dir(capture_directory.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_clear()
            .env("USERPROFILE", &operator_profile)
            .env("HOME", &operator_profile)
            .env("PATH", operator_helper_path(git))
            .env("LC_ALL", "C")
            .env("GIT_CEILING_DIRECTORIES", capture_directory.path())
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_ASKPASS", "")
            .env("SSH_ASKPASS", "")
            .env("GCM_INTERACTIVE", "Never")
            .env("GCM_GUI_PROMPT", "0");
        copy_validated_path_environment(
            &mut command,
            &["SystemRoot", "APPDATA", "LOCALAPPDATA", "PROGRAMDATA"],
        );
        set_process_group(&mut command);
        let cancellation = CancellationToken::new();
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| "GitHub credential capture failed".to_owned())?;
        let output = run_managed(
            command,
            Some(query),
            remaining,
            MAX_CREDENTIAL_OUTPUT_BYTES,
            &cancellation,
        )
        .await
        .map_err(|_| "GitHub credential capture failed".to_owned())?;
        if !output.status.success() {
            return Err("GitHub credential capture failed".into());
        }
        let credential = parse_filled_credential(&output.stdout, path)?;
        by_repository.insert(repository.clone(), credential);
    }
    Ok(GitHubCredentialStore {
        _inner: Arc::new(GitHubCredentialStoreInner { by_repository }),
    })
}

pub(super) struct WindowsCredentialBroker {
    server: tokio::net::windows::named_pipe::NamedPipeServer,
    #[cfg(test)]
    pipe_name: String,
    helper_path: String,
    credential: Zeroizing<Vec<u8>>,
}

impl WindowsCredentialBroker {
    pub(super) fn new(records: Zeroizing<Vec<u8>>, repository: &str) -> Result<Self, String> {
        use tokio::net::windows::named_pipe::ServerOptions;

        let repository_path = github_repository_path(repository)?;
        let credential = parse_filled_credential(&records, repository_path)?;
        let credential = credential_store_record(&credential, repository_path);
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let pipe_name = format!(r"\\.\pipe\buzz-git-credential-{nonce}");
        let helper_path = format!("//./pipe/buzz-git-credential-{nonce}");
        let mut security = super::windows_private::OwnerOnlySecurityAttributes::new()?;
        let server = unsafe {
            ServerOptions::new()
                .access_inbound(false)
                .access_outbound(true)
                .first_pipe_instance(true)
                .reject_remote_clients(true)
                .max_instances(1)
                .out_buffer_size(MAX_CREDENTIAL_OUTPUT_BYTES as u32)
                .create_with_security_attributes_raw(&pipe_name, security.as_mut_ptr().cast())
        }
        .map_err(|_| "failed to create trusted Git credential broker".to_owned())?;
        Ok(Self {
            server,
            #[cfg(test)]
            pipe_name,
            helper_path,
            credential,
        })
    }

    pub(super) fn git_helper(&self) -> String {
        format!("store --file={}", self.helper_path)
    }

    pub(super) async fn serve(mut self, process_group: &ProcessGroupGuard) -> Result<(), String> {
        use std::os::windows::io::AsRawHandle as _;
        use windows_sys::Win32::System::Pipes::GetNamedPipeClientProcessId;

        self.server
            .connect()
            .await
            .map_err(|_| "trusted Git credential broker connection failed".to_owned())?;
        let mut client_pid = 0_u32;
        let got_pid = unsafe {
            GetNamedPipeClientProcessId(self.server.as_raw_handle().cast(), &mut client_pid)
        };
        if got_pid == 0 || client_pid == 0 || !process_group.contains_process(client_pid)? {
            return Err("trusted Git credential broker rejected an unauthorized client".into());
        }
        self.server
            .write_all(&self.credential)
            .await
            .map_err(|_| "trusted Git credential broker delivery failed".to_owned())?;
        self.server
            .shutdown()
            .await
            .map_err(|_| "trusted Git credential broker shutdown failed".to_owned())?;
        self.credential.zeroize();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt as _;
    use tokio::net::windows::named_pipe::ClientOptions;

    use super::*;
    use crate::trusted::git::process::{
        apply_isolated_environment, resolve_system_git, run_managed_with_sidecar,
        wait_for_process_group_exit, ManagedSidecar, MAX_OUTPUT_BYTES,
    };

    const REPOSITORY: &str = "https://github.com/mysteropodes/nemo";

    fn credential_records() -> Zeroizing<Vec<u8>> {
        Zeroizing::new(
            b"protocol=https\nhost=github.com\npath=mysteropodes/nemo\nusername=probe+user\npassword=se:cr@t/%\n\n"
                .to_vec(),
        )
    }

    fn query() -> Zeroizing<Vec<u8>> {
        Zeroizing::new(b"protocol=https\nhost=github.com\npath=mysteropodes/nemo\n\n".to_vec())
    }

    #[tokio::test]
    async fn git_credential_store_reads_one_shot_pipe_inside_exact_job() {
        let git = resolve_system_git().expect("Git for Windows");
        let broker = WindowsCredentialBroker::new(credential_records(), REPOSITORY)
            .expect("credential broker");
        let helper_file = broker.helper_path.clone();
        let mut command = Command::new(git);
        command
            .arg("credential-store")
            .arg(format!("--file={helper_file}"))
            .arg("get")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        apply_isolated_environment(&mut command);
        set_process_group(&mut command);
        let output = run_managed_with_sidecar(
            command,
            Some(query()),
            Duration::from_secs(5),
            MAX_OUTPUT_BYTES,
            &CancellationToken::new(),
            ManagedSidecar::WindowsCredential(broker),
        )
        .await
        .expect("one-shot credential delivery");
        assert!(output.status.success());
        let fields = String::from_utf8(output.stdout.to_vec()).expect("credential output");
        assert!(fields.lines().any(|line| line == "username=probe+user"));
        assert!(fields.lines().any(|line| line == "password=se:cr@t/%"));
    }

    #[tokio::test]
    async fn credential_pipe_rejects_client_outside_exact_job() {
        let git = resolve_system_git().expect("Git for Windows");
        let broker = WindowsCredentialBroker::new(credential_records(), REPOSITORY)
            .expect("credential broker");
        let pipe_name = broker.pipe_name.clone();

        let mut command = Command::new(git);
        command
            .args(["hash-object", "--stdin"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        apply_isolated_environment(&mut command);
        set_process_group(&mut command);
        let mut child = command.spawn().expect("contained child");
        let mut process_group = ProcessGroupGuard::new(&child, child.id()).expect("Git job");

        let mut client = ClientOptions::new()
            .read(true)
            .write(false)
            .open(&pipe_name)
            .expect("outside client connection");
        let error = broker
            .serve(&process_group)
            .await
            .expect_err("outside client must not receive credentials");
        assert_eq!(
            error,
            "trusted Git credential broker rejected an unauthorized client"
        );
        let mut received = Zeroizing::new(Vec::new());
        let _ = client.read_to_end(&mut received).await;
        assert!(!received.windows(6).any(|window| window == b"se:cr@"));

        process_group.terminate();
        let _ = child.start_kill();
        tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .expect("child termination timeout")
            .expect("child termination");
        wait_for_process_group_exit(&process_group)
            .await
            .expect("job empty");
        process_group.disarm();
    }
}

fn credential_store_record(
    credential: &GitHubCredential,
    repository_path: &str,
) -> Zeroizing<Vec<u8>> {
    let mut record = Zeroizing::new(Vec::with_capacity(
        credential.username.len() + credential.password.len() + repository_path.len() + 32,
    ));
    record.extend_from_slice(b"https://");
    percent_encode_credential(&credential.username, &mut record);
    record.push(b':');
    percent_encode_credential(&credential.password, &mut record);
    record.extend_from_slice(b"@github.com/");
    record.extend_from_slice(repository_path.as_bytes());
    record.push(b'\n');
    record
}

fn percent_encode_credential(value: &str, output: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            output.push(byte);
        } else {
            output.extend_from_slice(&[
                b'%',
                HEX[(byte >> 4) as usize],
                HEX[(byte & 0x0f) as usize],
            ]);
        }
    }
}

fn operator_profile() -> Result<PathBuf, String> {
    let profile = PathBuf::from(
        std::env::var_os("USERPROFILE")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "operator Windows profile is unavailable".to_owned())?,
    );
    let profile = profile
        .canonicalize()
        .map_err(|_| "operator Windows profile is invalid".to_owned())?;
    if !profile.is_absolute() || !profile.is_dir() {
        return Err("operator Windows profile is invalid".into());
    }
    Ok(profile)
}

fn operator_helper_path(git: &Path) -> OsString {
    let mut directories = Vec::new();
    if let Some(parent) = git.parent() {
        directories.push(parent.to_path_buf());
        if parent.file_name().is_some_and(|name| {
            name.eq_ignore_ascii_case("cmd") || name.eq_ignore_ascii_case("bin")
        }) {
            if let Some(root) = parent.parent() {
                directories.push(root.join("mingw64").join("bin"));
                directories.push(root.join("usr").join("bin"));
            }
        }
    }
    if let Some(program_files) = std::env::var_os("ProgramFiles") {
        directories.push(PathBuf::from(program_files).join("GitHub CLI"));
    }
    if let Some(local) = std::env::var_os("LOCALAPPDATA") {
        directories.push(PathBuf::from(local).join("Programs").join("GitHub CLI"));
    }
    if let Some(root) = system_root() {
        directories.push(root.join("System32"));
    }
    directories.retain(|directory| directory.is_absolute() && directory.is_dir());
    directories.sort();
    directories.dedup();
    std::env::join_paths(directories).unwrap_or_default()
}

fn copy_validated_path_environment(command: &mut Command, names: &[&str]) {
    for name in names {
        let Some(value) = std::env::var_os(name) else {
            continue;
        };
        let path = PathBuf::from(value);
        if path.is_absolute() && path.is_dir() {
            command.env(name, path);
        }
    }
}

fn system_root() -> Option<PathBuf> {
    let root = PathBuf::from(std::env::var_os("SystemRoot")?);
    (root.is_absolute() && root.is_dir()).then_some(root)
}
