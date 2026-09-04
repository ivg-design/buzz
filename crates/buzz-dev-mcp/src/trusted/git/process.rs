#[cfg(target_os = "macos")]
use std::collections::{BTreeMap, BTreeSet};
#[cfg(target_os = "macos")]
use std::ffi::OsStr;
#[cfg(any(unix, windows))]
use std::ffi::OsString;
#[cfg(target_os = "macos")]
use std::fs::{File, OpenOptions};
#[cfg(target_os = "macos")]
use std::io::Write as _;
#[cfg(target_os = "macos")]
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::Path;
#[cfg(any(unix, windows))]
use std::path::PathBuf;
use std::process::ExitStatus;
#[cfg(any(unix, windows))]
use std::process::Stdio;
use std::sync::Arc;
#[cfg(any(unix, windows))]
use std::time::Duration;

#[cfg(any(unix, windows))]
use nostr::ToBech32;
#[cfg(any(unix, windows))]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(any(unix, windows))]
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use super::super::scope::TrustedGitCheckout;
use super::super::{PrivilegedGitDisposition, TrustedRelay};

#[cfg(any(unix, windows))]
const GIT_TIMEOUT: Duration = Duration::from_secs(300);
#[cfg(any(unix, windows))]
const RECONCILIATION_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(target_os = "macos")]
const CREDENTIAL_CAPTURE_TIMEOUT: Duration = Duration::from_secs(20);
#[cfg(any(unix, windows))]
const IO_JOIN_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(any(unix, windows))]
const PROCESS_GROUP_POLL_INTERVAL: Duration = Duration::from_millis(10);
#[cfg(any(unix, windows))]
const PROCESS_GROUP_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(any(unix, windows))]
const MAX_OUTPUT_BYTES: usize = 1024 * 1024;
#[cfg(any(unix, windows))]
const CHECKOUT_INSPECTION_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(any(unix, windows))]
const MAX_CHECKOUT_INSPECTION_BYTES: usize = 64 * 1024;
#[cfg(target_os = "macos")]
const MAX_CREDENTIAL_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_INPUT_BYTES: usize = 4 * 1024 * 1024;
const CANCELLED_ERROR: &str = "trusted Git operation was cancelled";
const TIMED_OUT_ERROR: &str = "trusted Git operation timed out";
#[cfg(target_os = "macos")]
const MAX_CREDENTIAL_USERNAME_BYTES: usize = 256;
#[cfg(target_os = "macos")]
const MAX_CREDENTIAL_PASSWORD_BYTES: usize = 4096;
#[cfg(target_os = "macos")]
const MAX_PRIVATE_PACKS: usize = 16;
#[cfg(target_os = "macos")]
const MAX_PRIVATE_PACK_BYTES: u64 = 2 * 1024 * 1024 * 1024;
#[cfg(target_os = "macos")]
const SECRET_FD: i32 = 3;
#[cfg(target_os = "macos")]
const PRIVATE_TEMP_ROOT: &str = "/private/tmp";

pub(super) struct GitOutput {
    pub(super) stdout: Vec<u8>,
}

pub(in crate::trusted) struct CheckoutInspectionOutput {
    pub(in crate::trusted) status: ExitStatus,
    pub(in crate::trusted) stdout: Vec<u8>,
    pub(in crate::trusted) stderr: Vec<u8>,
}

/// Run read-only checkout inspection through the same bounded, descendant-safe
/// process seam as privileged Git. This is deliberately unavailable on
/// platforms where the process-tree containment proof is not implemented.
pub(in crate::trusted) async fn inspect_checkout_git(
    cwd: &Path,
    args: &[&str],
) -> Result<CheckoutInspectionOutput, String> {
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (cwd, args);
        Err("local A2A checkout inspection is unavailable on this platform".into())
    }
    #[cfg(any(unix, windows))]
    {
        let git =
            resolve_system_git().ok_or_else(|| "trusted system git was not found".to_owned())?;
        inspect_checkout_git_with(&git, cwd, args, CHECKOUT_INSPECTION_TIMEOUT).await
    }
}

#[cfg(any(unix, windows))]
async fn inspect_checkout_git_with(
    git: &Path,
    cwd: &Path,
    args: &[&str],
    timeout: Duration,
) -> Result<CheckoutInspectionOutput, String> {
    let mut command = Command::new(git);
    command
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    apply_isolated_environment(&mut command);
    apply_config_environment(
        &mut command,
        &[
            ("core.hooksPath".into(), null_device().into()),
            ("core.fsmonitor".into(), "false".into()),
            ("core.attributesFile".into(), null_device().into()),
            ("credential.helper".into(), String::new()),
            ("protocol.allow".into(), "never".into()),
        ],
    );
    set_process_group(&mut command);
    let cancellation = CancellationToken::new();
    let mut output = run_managed(
        command,
        None,
        timeout,
        MAX_CHECKOUT_INSPECTION_BYTES,
        &cancellation,
    )
    .await
    .map_err(|error| format!("bounded local checkout inspection failed: {error}"))?;
    Ok(CheckoutInspectionOutput {
        status: output.status,
        stdout: std::mem::take(&mut *output.stdout),
        stderr: std::mem::take(&mut *output.stderr),
    })
}

/// Exact observation made after a possibly side-effecting ref command.
pub(super) struct RefMutationOutcome {
    pub(super) disposition: PrivilegedGitDisposition,
    pub(super) previous_object: Option<String>,
    pub(super) intended_object: String,
    pub(super) observed_object: Option<String>,
    pub(super) command_error: Option<String>,
}

/// Repository-scoped credentials captured before any model process starts.
///
/// Clones share one allocation so credentials are not copied between scoped
/// MCP sessions. This type and its contents intentionally do not implement
/// `Debug`; username and password allocations are zeroized on final drop.
#[derive(Clone, Default)]
pub(crate) struct GitHubCredentialStore {
    _inner: Arc<GitHubCredentialStoreInner>,
}

#[derive(Default)]
struct GitHubCredentialStoreInner {
    #[cfg(target_os = "macos")]
    by_repository: BTreeMap<String, GitHubCredential>,
}

#[cfg(target_os = "macos")]
struct GitHubCredential {
    username: Zeroizing<String>,
    password: Zeroizing<String>,
}

impl GitHubCredentialStore {
    #[cfg(target_os = "macos")]
    fn records_for(&self, repository: &str) -> Result<Zeroizing<Vec<u8>>, String> {
        let path = github_repository_path(repository)?;
        let credential = self._inner.by_repository.get(repository).ok_or_else(|| {
            "operator GitHub credentials were not captured for this repository".to_owned()
        })?;
        let mut records = Zeroizing::new(Vec::with_capacity(
            credential.username.len() + credential.password.len() + path.len() + 64,
        ));
        records.extend_from_slice(b"protocol=https\nhost=github.com\npath=");
        records.extend_from_slice(path.as_bytes());
        records.extend_from_slice(b"\nusername=");
        records.extend_from_slice(credential.username.as_bytes());
        records.extend_from_slice(b"\npassword=");
        records.extend_from_slice(credential.password.as_bytes());
        records.extend_from_slice(b"\n\n");
        Ok(records)
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        #[cfg(target_os = "macos")]
        {
            self._inner.by_repository.is_empty()
        }
        #[cfg(not(target_os = "macos"))]
        {
            true
        }
    }
}

/// Resolve an operator credential once for every canonical GitHub repository
/// that has an explicit fetch or push grant.
///
/// This is the only trusted Git path that inherits the real operator HOME.
/// Helper commands execute during capture, but neither their configuration nor
/// their output is ever replayed to a later child process.
pub(crate) async fn capture_operator_github_credentials(
    repositories: &[String],
) -> Result<GitHubCredentialStore, String> {
    if repositories.is_empty() {
        return Ok(GitHubCredentialStore::default());
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = repositories;
        Err("operator GitHub credential capture is currently available only on macOS".into())
    }
    #[cfg(target_os = "macos")]
    {
        let git =
            resolve_system_git().ok_or_else(|| "trusted system git was not found".to_owned())?;
        let home = std::env::var_os("HOME")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                "operator HOME is unavailable for GitHub credential capture".to_owned()
            })?;
        let home_path = Path::new(&home);
        if !home_path.is_absolute() || !home_path.is_dir() {
            return Err("operator HOME is invalid for GitHub credential capture".into());
        }
        capture_operator_github_credentials_with(
            &git,
            &home,
            repositories,
            CREDENTIAL_CAPTURE_TIMEOUT,
        )
        .await
    }
}

#[cfg(target_os = "macos")]
async fn capture_operator_github_credentials_with(
    git: &Path,
    operator_home: &OsStr,
    repositories: &[String],
    timeout: Duration,
) -> Result<GitHubCredentialStore, String> {
    let deadline = tokio::time::Instant::now() + timeout;
    let repositories = repositories.iter().collect::<BTreeSet<_>>();
    let capture_directory = tempfile::Builder::new()
        .prefix("buzz-git-credential-capture-")
        .tempdir_in(PRIVATE_TEMP_ROOT)
        .map_err(|_| "failed to create GitHub credential capture directory".to_owned())?;
    std::fs::set_permissions(
        capture_directory.path(),
        std::fs::Permissions::from_mode(0o700),
    )
    .map_err(|_| "failed to secure GitHub credential capture directory".to_owned())?;
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
            .env("HOME", operator_home)
            // Operator-owned helpers such as `gh auth git-credential` may be
            // installed by Homebrew. This wider PATH exists only for trusted,
            // pre-model credential capture.
            .env("PATH", operator_helper_path())
            .env("LC_ALL", "C")
            // Credential commands normally discover repository-local config
            // by walking parent directories. Stop at this fresh directory so
            // no checkout-controlled helper can shadow operator discovery.
            .env("GIT_CEILING_DIRECTORIES", capture_directory.path())
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_ASKPASS", "")
            .env("SSH_ASKPASS", "");
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

pub(super) async fn git_output(
    relay: &TrustedRelay,
    checkout: &TrustedGitCheckout,
    args: &[&str],
    cancellation: &CancellationToken,
) -> Result<GitOutput, String> {
    local_git_output(relay, checkout, args, None, cancellation).await
}

pub(super) async fn git_output_with_input(
    relay: &TrustedRelay,
    checkout: &TrustedGitCheckout,
    args: &[&str],
    input: Vec<u8>,
    cancellation: &CancellationToken,
) -> Result<GitOutput, String> {
    if input.len() > MAX_INPUT_BYTES {
        return Err("trusted Git input exceeded 4 MiB".into());
    }
    local_git_output(
        relay,
        checkout,
        args,
        Some(Zeroizing::new(input)),
        cancellation,
    )
    .await
}

async fn local_git_output(
    relay: &TrustedRelay,
    checkout: &TrustedGitCheckout,
    args: &[&str],
    input: Option<Zeroizing<Vec<u8>>>,
    cancellation: &CancellationToken,
) -> Result<GitOutput, String> {
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (relay, checkout, args, input, cancellation);
        return Err("trusted Project Git is unavailable on this platform".into());
    }
    #[cfg(any(unix, windows))]
    {
        local_git_output_with_timeout(relay, checkout, args, input, GIT_TIMEOUT, cancellation).await
    }
}

#[cfg(any(unix, windows))]
async fn local_git_output_with_timeout(
    relay: &TrustedRelay,
    checkout: &TrustedGitCheckout,
    args: &[&str],
    input: Option<Zeroizing<Vec<u8>>>,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<GitOutput, String> {
    let git = resolve_system_git().ok_or_else(|| "trusted system git was not found".to_owned())?;
    let mut command = Command::new(git);
    command
        .args(args)
        .current_dir(&checkout.root)
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    apply_isolated_environment(&mut command);
    apply_config_environment(&mut command, &local_git_config(relay));
    set_process_group(&mut command);
    let mut output = run_managed(command, input, timeout, MAX_OUTPUT_BYTES, cancellation).await?;
    if !output.status.success() {
        return Err(local_error(&output.stderr));
    }
    Ok(GitOutput {
        stdout: std::mem::take(&mut *output.stdout),
    })
}

/// Read one exact full ref without consulting checkout configuration.
pub(super) async fn local_ref(
    relay: &TrustedRelay,
    checkout: &TrustedGitCheckout,
    full_ref: &str,
    cancellation: &CancellationToken,
) -> Result<Option<String>, String> {
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (relay, checkout, full_ref, cancellation);
        return Err("trusted Project Git is unavailable on this platform".into());
    }
    #[cfg(any(unix, windows))]
    {
        let output = local_git_output_with_timeout(
            relay,
            checkout,
            &[
                "for-each-ref",
                "--count=2",
                "--format=%(objectname)%00%(refname)",
                full_ref,
            ],
            None,
            RECONCILIATION_TIMEOUT,
            cancellation,
        )
        .await?;
        parse_exact_ref_records(&output.stdout, full_ref)
    }
}

/// Apply an exact compare-and-swap local ref update, then independently read
/// the ref even when the mutator was cancelled or timed out.
pub(super) async fn update_local_ref_reconciled(
    relay: &TrustedRelay,
    checkout: &TrustedGitCheckout,
    full_ref: &str,
    previous_object: Option<String>,
    intended_object: String,
    reflog_message: &str,
    cancellation: &CancellationToken,
) -> RefMutationOutcome {
    let expected = previous_object
        .clone()
        .unwrap_or_else(|| "0".repeat(intended_object.len()));
    let command = git_output(
        relay,
        checkout,
        &[
            "update-ref",
            "-m",
            reflog_message,
            full_ref,
            &intended_object,
            &expected,
        ],
        cancellation,
    )
    .await
    .map(|_| ());
    let reconciliation = CancellationToken::new();
    let observed = local_ref(relay, checkout, full_ref, &reconciliation).await;
    reconciled_ref_outcome(previous_object, intended_object, command, observed)
}

/// Fetch through an isolated private bare repository, then import the exact
/// fetched commit into the checkout without a network-capable checkout child.
pub(super) async fn fetch_branch(
    relay: &TrustedRelay,
    checkout: &TrustedGitCheckout,
    destination: &str,
    previous_object: Option<String>,
    cancellation: &CancellationToken,
) -> Result<RefMutationOutcome, String> {
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (relay, checkout, destination, previous_object, cancellation);
        return Err(
            "trusted GitHub network operations are currently available only on macOS".into(),
        );
    }
    #[cfg(target_os = "macos")]
    {
        github_repository_path(&checkout.repository)?;
        validate_object_id(&checkout.head_sha)?;
        let git =
            resolve_system_git().ok_or_else(|| "trusted system git was not found".to_owned())?;
        let private = PrivateBareRepo::create(&git, cancellation).await?;
        private.configure_for_network(&checkout.repository)?;
        let credentials = credentials_for_operation(relay, checkout).await?;
        let credential = credentials.records_for(&checkout.repository)?;
        let source = format!("refs/heads/{}:refs/buzz/remote", checkout.branch);
        run_network_git(
            &git,
            &private,
            &[
                "fetch",
                "--quiet",
                "--no-tags",
                "--no-write-fetch-head",
                "--no-recurse-submodules",
                &checkout.repository,
                &source,
            ],
            credential,
            cancellation,
        )
        .await?;
        let fetched = private
            .output(
                &git,
                &["rev-parse", "--verify", "refs/buzz/remote^{commit}"],
                cancellation,
            )
            .await?;
        let fetched = text(&fetched.stdout)?.to_owned();
        validate_object_id(&fetched)?;

        private
            .output(&git, &["repack", "-a", "-d", "-q"], cancellation)
            .await?;
        let object_directory = checkout_object_directory(checkout)?;
        import_private_packs(&git, &private, &object_directory, cancellation).await?;
        git_output(
            relay,
            checkout,
            &["cat-file", "-e", &format!("{fetched}^{{commit}}")],
            cancellation,
        )
        .await?;
        Ok(update_local_ref_reconciled(
            relay,
            checkout,
            destination,
            previous_object,
            fetched,
            "fetch: Buzz trusted Project update",
            cancellation,
        )
        .await)
    }
}

/// Push one already-validated immutable commit, non-force, from a fresh bare
/// repository linked only to the validated checkout object database.
pub(super) async fn push_commit(
    relay: &TrustedRelay,
    checkout: &TrustedGitCheckout,
    cancellation: &CancellationToken,
) -> Result<RefMutationOutcome, String> {
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (relay, checkout, cancellation);
        return Err(
            "trusted GitHub network operations are currently available only on macOS".into(),
        );
    }
    #[cfg(target_os = "macos")]
    {
        github_repository_path(&checkout.repository)?;
        validate_object_id(&checkout.head_sha)?;
        let git =
            resolve_system_git().ok_or_else(|| "trusted system git was not found".to_owned())?;
        let private = PrivateBareRepo::create(&git, cancellation).await?;
        private.configure_for_network(&checkout.repository)?;
        let object_directory = checkout_object_directory(checkout)?;
        private.install_alternate(&object_directory)?;
        private
            .output(
                &git,
                &[
                    "cat-file",
                    "-e",
                    &format!("{}^{{commit}}", checkout.head_sha),
                ],
                cancellation,
            )
            .await?;
        let branch_ref = format!("refs/heads/{}", checkout.branch);
        let credentials = credentials_for_operation(relay, checkout).await?;
        push_ref_reconciled(
            &git,
            &private,
            &credentials,
            &checkout.repository,
            &branch_ref,
            &checkout.head_sha,
            cancellation,
        )
        .await
    }
}

#[cfg(target_os = "macos")]
async fn credentials_for_operation(
    relay: &TrustedRelay,
    checkout: &TrustedGitCheckout,
) -> Result<GitHubCredentialStore, String> {
    if relay
        .github_credentials
        .records_for(&checkout.repository)
        .is_ok()
    {
        return Ok(relay.github_credentials.clone());
    }
    if checkout.repository_wide && checkout.repository == buzz_core::nemo::REPOSITORY {
        return capture_operator_github_credentials(std::slice::from_ref(&checkout.repository))
            .await
            .map_err(|_| {
                "Nemo GitHub authentication is unavailable; sign in with GitHub before fetch or push"
                    .to_owned()
            });
    }
    Err("operator GitHub credentials were not captured for this repository".into())
}

#[cfg(target_os = "macos")]
async fn push_ref_reconciled(
    git: &Path,
    private: &PrivateBareRepo,
    credentials: &GitHubCredentialStore,
    repository: &str,
    branch_ref: &str,
    intended_object: &str,
    cancellation: &CancellationToken,
) -> Result<RefMutationOutcome, String> {
    let previous_object = remote_ref(
        git,
        private,
        credentials,
        repository,
        branch_ref,
        cancellation,
    )
    .await?;
    if previous_object.as_deref() == Some(intended_object) {
        return Ok(RefMutationOutcome {
            disposition: PrivilegedGitDisposition::Applied,
            previous_object: previous_object.clone(),
            intended_object: intended_object.to_owned(),
            observed_object: previous_object,
            command_error: None,
        });
    }

    let destination = format!("{intended_object}:{branch_ref}");
    let credential = credentials.records_for(repository)?;
    let command = run_network_git(
        git,
        private,
        &[
            "push",
            "--porcelain",
            "--no-verify",
            "--no-follow-tags",
            repository,
            &destination,
        ],
        credential,
        cancellation,
    )
    .await;

    // Request/session cancellation cannot skip outcome reconciliation. This is
    // a new bounded, read-only network child with a fresh one-shot credential
    // channel; it never inherits the already-cancelled token or mutates a ref.
    let reconciliation = CancellationToken::new();
    let observed = remote_ref(
        git,
        private,
        credentials,
        repository,
        branch_ref,
        &reconciliation,
    )
    .await;
    Ok(reconciled_ref_outcome(
        previous_object,
        intended_object.to_owned(),
        command,
        observed,
    ))
}

#[cfg(target_os = "macos")]
struct PrivateBareRepo {
    directory: tempfile::TempDir,
}

#[cfg(target_os = "macos")]
impl PrivateBareRepo {
    async fn create(git: &Path, cancellation: &CancellationToken) -> Result<Self, String> {
        let directory = tempfile::Builder::new()
            .prefix("buzz-trusted-git-")
            .tempdir_in(PRIVATE_TEMP_ROOT)
            .map_err(|_| "failed to create private Git repository".to_owned())?;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
            .map_err(|_| "failed to secure private Git repository".to_owned())?;
        let mut command = Command::new(git);
        command
            .args(["init", "--bare", "--quiet", "."])
            .current_dir(directory.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        apply_isolated_environment(&mut command);
        set_process_group(&mut command);
        let output =
            run_managed(command, None, GIT_TIMEOUT, MAX_OUTPUT_BYTES, cancellation).await?;
        if !output.status.success() {
            return Err("failed to initialize private Git repository".into());
        }
        let config = directory.path().join("config");
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600))
            .map_err(|_| "failed to secure private Git configuration".to_owned())?;
        Ok(Self { directory })
    }

    fn path(&self) -> &Path {
        self.directory.path()
    }

    fn configure_for_network(&self, repository: &str) -> Result<(), String> {
        let path = github_repository_path(repository)?;
        let contents = format!(
            "[core]\n\
             \trepositoryFormatVersion = 0\n\
             \tfileMode = true\n\
             \tbare = true\n\
             \thooksPath = /dev/null\n\
             \tfsmonitor = false\n\
             \taskPass =\n\
             \tattributesFile = /dev/null\n\
             [credential]\n\
             \thelper =\n\
             \tuseHttpPath = true\n\
             [credential \"https://github.com/{path}\"]\n\
             \thelper = \"!f() {{ test \\\"$1\\\" = get && /bin/cat <&3 || :; }}; f\"\n\
             [protocol]\n\
             \tallow = never\n\
             [protocol \"https\"]\n\
             \tallow = always\n\
             [protocol \"http\"]\n\
             \tallow = never\n\
             [protocol \"ssh\"]\n\
             \tallow = never\n\
             [protocol \"file\"]\n\
             \tallow = never\n\
             [http]\n\
             \tsslVerify = true\n\
             \textraHeader =\n\
             \tfollowRedirects = false\n\
             [fetch]\n\
             \tunpackLimit = 1\n\
             \twriteCommitGraph = false\n\
             [transfer]\n\
             \tunpackLimit = 1\n\
             \tfsckObjects = true\n\
             [receive]\n\
             \tfsckObjects = true\n\
             [push]\n\
             \tgpgSign = false\n\
             \trecurseSubmodules = no\n\
             [gc]\n\
             \tauto = 0\n\
             [maintenance]\n\
             \tauto = false\n"
        );
        write_private_file(&self.path().join("config"), contents.as_bytes())
    }

    fn install_alternate(&self, object_directory: &Path) -> Result<(), String> {
        let path = object_directory
            .to_str()
            .filter(|value| !value.contains(['\n', '\r', '\0']))
            .ok_or_else(|| "trusted checkout object path is invalid".to_owned())?;
        let info = self.path().join("objects/info");
        std::fs::create_dir_all(&info)
            .map_err(|_| "failed to prepare private Git alternate".to_owned())?;
        std::fs::set_permissions(&info, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| "failed to secure private Git alternate".to_owned())?;
        write_private_file(&info.join("alternates"), format!("{path}\n").as_bytes())
    }

    async fn output(
        &self,
        git: &Path,
        args: &[&str],
        cancellation: &CancellationToken,
    ) -> Result<GitOutput, String> {
        let mut command = Command::new(git);
        command
            .arg("--git-dir=.")
            .args(args)
            .current_dir(self.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        apply_isolated_environment(&mut command);
        set_process_group(&mut command);
        let mut output =
            run_managed(command, None, GIT_TIMEOUT, MAX_OUTPUT_BYTES, cancellation).await?;
        if !output.status.success() {
            return Err(local_error(&output.stderr));
        }
        Ok(GitOutput {
            stdout: std::mem::take(&mut *output.stdout),
        })
    }
}

#[cfg(target_os = "macos")]
async fn run_network_git(
    git: &Path,
    private: &PrivateBareRepo,
    args: &[&str],
    credential: Zeroizing<Vec<u8>>,
    cancellation: &CancellationToken,
) -> Result<(), String> {
    let output = run_network_git_with(git, private, args, credential, cancellation)
        .await
        .map_err(network_error)?;
    if output.status.success() {
        Ok(())
    } else {
        Err("trusted Git network operation failed".into())
    }
}

#[cfg(target_os = "macos")]
async fn run_network_git_with(
    git: &Path,
    private: &PrivateBareRepo,
    args: &[&str],
    credential: Zeroizing<Vec<u8>>,
    cancellation: &CancellationToken,
) -> Result<ManagedOutput, String> {
    run_network_git_with_timeout(git, private, args, credential, GIT_TIMEOUT, cancellation).await
}

#[cfg(target_os = "macos")]
async fn run_network_git_with_timeout(
    git: &Path,
    private: &PrivateBareRepo,
    args: &[&str],
    credential: Zeroizing<Vec<u8>>,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<ManagedOutput, String> {
    // Duplicate the one-shot stdin pipe to fd 3, close stdin, then exec Git.
    // The fixed helper is the only child that reads fd 3; no credential bytes
    // appear in argv, environment, logs, or a filesystem object.
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg(format!("exec {SECRET_FD}<&0; exec </dev/null; exec \"$@\""))
        .arg("buzz-trusted-git")
        .arg(git)
        .arg("--git-dir=.")
        .args(args)
        .current_dir(private.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    apply_isolated_environment(&mut command);
    set_process_group(&mut command);
    run_managed(
        command,
        Some(credential),
        timeout,
        MAX_OUTPUT_BYTES,
        cancellation,
    )
    .await
}

#[cfg(target_os = "macos")]
async fn remote_ref(
    git: &Path,
    private: &PrivateBareRepo,
    credentials: &GitHubCredentialStore,
    repository: &str,
    branch_ref: &str,
    cancellation: &CancellationToken,
) -> Result<Option<String>, String> {
    let credential = credentials.records_for(repository)?;
    let output = run_network_git_with_timeout(
        git,
        private,
        &["ls-remote", "--quiet", "--refs", repository, branch_ref],
        credential,
        RECONCILIATION_TIMEOUT,
        cancellation,
    )
    .await
    .map_err(network_error)?;
    if !output.status.success() {
        return Err("trusted Git remote-ref reconciliation failed".into());
    }
    parse_ls_remote(&output.stdout, branch_ref)
}

fn reconciled_ref_outcome(
    previous_object: Option<String>,
    intended_object: String,
    command: Result<(), String>,
    observed: Result<Option<String>, String>,
) -> RefMutationOutcome {
    let command_succeeded = command.is_ok();
    let command_error = command.err();
    let (observed_object, disposition) = match observed {
        Ok(observed_object) if observed_object.as_deref() == Some(intended_object.as_str()) => {
            (observed_object, PrivilegedGitDisposition::Applied)
        }
        Ok(observed_object) if !command_succeeded && observed_object == previous_object => {
            (observed_object, PrivilegedGitDisposition::NotApplied)
        }
        Ok(observed_object) => (observed_object, PrivilegedGitDisposition::Ambiguous),
        Err(_) => (None, PrivilegedGitDisposition::Ambiguous),
    };
    RefMutationOutcome {
        disposition,
        previous_object,
        intended_object,
        observed_object,
        command_error,
    }
}

#[cfg(any(unix, windows))]
fn parse_exact_ref_records(bytes: &[u8], expected_ref: &str) -> Result<Option<String>, String> {
    let mut object = None;
    for record in bytes
        .split(|byte| *byte == b'\n')
        .filter(|row| !row.is_empty())
    {
        let Some(separator) = record.iter().position(|byte| *byte == 0) else {
            return Err("trusted Git returned an invalid ref record".into());
        };
        let candidate = std::str::from_utf8(&record[..separator])
            .map_err(|_| "trusted Git returned an invalid ref object".to_owned())?;
        let full_ref = std::str::from_utf8(&record[separator + 1..])
            .map_err(|_| "trusted Git returned an invalid ref name".to_owned())?;
        super::validate_object_id(candidate)?;
        if full_ref != expected_ref || object.replace(candidate.to_owned()).is_some() {
            return Err("trusted Git ref query was not exact".into());
        }
    }
    Ok(object)
}

#[cfg(target_os = "macos")]
fn parse_ls_remote(bytes: &[u8], expected_ref: &str) -> Result<Option<String>, String> {
    let mut object = None;
    for row in bytes
        .split(|byte| *byte == b'\n')
        .filter(|row| !row.is_empty())
    {
        let line = std::str::from_utf8(row)
            .map_err(|_| "trusted Git remote-ref output was not UTF-8".to_owned())?;
        let (candidate, full_ref) = line
            .split_once('\t')
            .ok_or_else(|| "trusted Git remote-ref output was malformed".to_owned())?;
        validate_object_id(candidate)?;
        if full_ref != expected_ref || object.replace(candidate.to_owned()).is_some() {
            return Err("trusted Git remote-ref query was not exact".into());
        }
    }
    Ok(object)
}

#[cfg(target_os = "macos")]
fn checkout_object_directory(checkout: &TrustedGitCheckout) -> Result<PathBuf, String> {
    let common = checkout
        .git_common_dir
        .canonicalize()
        .map_err(|_| "trusted checkout Git directory is unavailable".to_owned())?;
    if common != checkout.git_common_dir {
        return Err("trusted checkout Git directory changed".into());
    }
    let expected_objects = common.join("objects");
    let objects = expected_objects
        .canonicalize()
        .map_err(|_| "trusted checkout object database is unavailable".to_owned())?;
    if objects != expected_objects || !objects.is_dir() {
        return Err("trusted checkout object database is unavailable".into());
    }
    Ok(objects)
}

#[cfg(target_os = "macos")]
async fn import_private_packs(
    git: &Path,
    private: &PrivateBareRepo,
    checkout_objects: &Path,
    cancellation: &CancellationToken,
) -> Result<(), String> {
    let pack_directory = private.path().join("objects/pack");
    let mut packs = std::fs::read_dir(pack_directory)
        .map_err(|_| "private Git fetch produced no object pack".to_owned())?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension() == Some(OsStr::new("pack")))
        .collect::<Vec<_>>();
    packs.sort();
    if packs.is_empty() || packs.len() > MAX_PRIVATE_PACKS {
        return Err("private Git fetch produced an invalid pack set".into());
    }
    let mut total = 0_u64;
    for pack in packs {
        let size = pack
            .metadata()
            .map_err(|_| "failed to inspect private Git pack".to_owned())?
            .len();
        total = total
            .checked_add(size)
            .ok_or_else(|| "private Git pack set is too large".to_owned())?;
        if total > MAX_PRIVATE_PACK_BYTES {
            return Err("private Git pack set is too large".into());
        }
        let file = File::open(&pack).map_err(|_| "failed to open private Git pack".to_owned())?;
        let mut command = Command::new(git);
        command
            .arg("--git-dir=.")
            .args(["index-pack", "--stdin", "--fix-thin"])
            .current_dir(private.path())
            .stdin(Stdio::from(file))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        apply_isolated_environment(&mut command);
        command.env("GIT_OBJECT_DIRECTORY", checkout_objects);
        set_process_group(&mut command);
        let output =
            run_managed(command, None, GIT_TIMEOUT, MAX_OUTPUT_BYTES, cancellation).await?;
        if !output.status.success() {
            return Err("failed to import trusted Git objects".into());
        }
    }
    Ok(())
}

#[cfg(any(unix, windows))]
struct ManagedOutput {
    status: ExitStatus,
    stdout: Zeroizing<Vec<u8>>,
    stderr: Zeroizing<Vec<u8>>,
}

#[cfg(any(unix, windows))]
async fn run_managed(
    mut command: Command,
    input: Option<Zeroizing<Vec<u8>>>,
    timeout: Duration,
    max_output: usize,
    cancellation: &CancellationToken,
) -> Result<ManagedOutput, String> {
    let mut child = command
        .spawn()
        .map_err(|_| "failed to start trusted Git".to_owned())?;
    let pid = child.id();
    // The explicit paths below perform orderly bounded cleanup. This guard is
    // the last-resort group kill if the async future itself is dropped.
    let mut process_group = match ProcessGroupGuard::new(&child, pid) {
        Ok(group) => group,
        Err(error) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(error);
        }
    };
    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            terminate_process_group(&process_group, &mut child).await?;
            return Err("trusted Git stdout unavailable".into());
        }
    };
    let stderr = match child.stderr.take() {
        Some(stderr) => stderr,
        None => {
            terminate_process_group(&process_group, &mut child).await?;
            return Err("trusted Git stderr unavailable".into());
        }
    };
    let stdin = child.stdin.take();
    let mut stdout_task = tokio::spawn(read_bounded(stdout, max_output));
    let mut stderr_task = tokio::spawn(read_bounded(stderr, max_output));
    let mut writer_task = tokio::spawn(async move {
        match (stdin, input) {
            (Some(mut stdin), Some(input)) => {
                let result = stdin.write_all(&input).await;
                drop(stdin);
                result
            }
            (None, None) => Ok(()),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "trusted Git input pipe was unavailable",
            )),
        }
    });

    enum WaitOutcome {
        Status(ExitStatus),
        Cancelled,
        TimedOut,
        Failed,
    }
    let outcome = tokio::select! {
        result = tokio::time::timeout(timeout, child.wait()) => match result {
            Ok(Ok(status)) => WaitOutcome::Status(status),
            Ok(Err(_)) => WaitOutcome::Failed,
            Err(_) => WaitOutcome::TimedOut,
        },
        _ = cancellation.cancelled() => match child.try_wait() {
            // Cancellation and process exit may become ready in the same poll.
            // A status that the kernel already made available wins, so an
            // applied command is never relabelled as merely cancelled.
            Ok(Some(status)) => WaitOutcome::Status(status),
            Ok(None) => WaitOutcome::Cancelled,
            Err(_) => WaitOutcome::Failed,
        },
    };
    if !matches!(outcome, WaitOutcome::Status(_)) {
        terminate_process_group(&process_group, &mut child).await?;
    } else {
        // Git has reaped its direct children before exiting. Kill any process
        // that nevertheless retained the dedicated group (and our pipe fds)
        // before joining the bounded I/O tasks.
        process_group.terminate();
        wait_for_process_group_exit(&process_group).await?;
    }
    // The guard is disarmed only after both the direct child and every member
    // of its dedicated process group have been proven gone.
    process_group.disarm();

    let tasks = tokio::time::timeout(IO_JOIN_TIMEOUT, async {
        tokio::join!(&mut writer_task, &mut stdout_task, &mut stderr_task)
    })
    .await;
    let (writer, stdout, stderr) = match tasks {
        Ok(results) => results,
        Err(_) => {
            writer_task.abort();
            stdout_task.abort();
            stderr_task.abort();
            let _ = tokio::join!(writer_task, stdout_task, stderr_task);
            return Err("trusted Git I/O cleanup timed out".into());
        }
    };
    let writer = writer.map_err(|_| "trusted Git input task failed".to_owned())?;
    if let Err(error) = writer {
        if error.kind() != std::io::ErrorKind::BrokenPipe {
            return Err("failed to write trusted Git input".into());
        }
    }
    let stdout = stdout.map_err(|_| "trusted Git stdout task failed".to_owned())??;
    let stderr = stderr.map_err(|_| "trusted Git stderr task failed".to_owned())??;
    match outcome {
        WaitOutcome::Status(status) => Ok(ManagedOutput {
            status,
            stdout,
            stderr,
        }),
        WaitOutcome::Cancelled => Err(CANCELLED_ERROR.into()),
        WaitOutcome::TimedOut => Err(TIMED_OUT_ERROR.into()),
        WaitOutcome::Failed => Err("failed to wait for trusted Git".into()),
    }
}

#[cfg(any(unix, windows))]
async fn read_bounded<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    limit: usize,
) -> Result<Zeroizing<Vec<u8>>, String> {
    let mut retained = Zeroizing::new(Vec::new());
    let mut total = 0_usize;
    // Credential-capture stdout can contain secrets. Keep the scratch buffer
    // zeroizing too, including when an I/O error returns before the explicit
    // per-read wipe below.
    let mut buffer = Zeroizing::new([0_u8; 8192]);
    loop {
        let read = reader
            .read(&mut *buffer)
            .await
            .map_err(|_| "failed to read trusted Git output".to_owned())?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read);
        if total <= limit {
            retained.extend_from_slice(&buffer[..read]);
        }
        buffer[..read].fill(0);
    }
    if total > limit {
        return Err("trusted Git output exceeded its bounded limit".into());
    }
    Ok(retained)
}

#[cfg(any(unix, windows))]
async fn terminate_process_group(
    process_group: &ProcessGroupGuard,
    child: &mut tokio::process::Child,
) -> Result<(), String> {
    process_group.terminate();
    let _ = child.start_kill();
    match tokio::time::timeout(PROCESS_GROUP_CLEANUP_TIMEOUT, child.wait()).await {
        Ok(Ok(_)) => {}
        Ok(Err(_)) | Err(_) => abort_process_containment_failure(),
    }
    wait_for_process_group_exit(process_group).await
}

#[cfg(unix)]
async fn wait_for_process_group_exit(process_group: &ProcessGroupGuard) -> Result<(), String> {
    use nix::errno::Errno;
    use nix::sys::signal::killpg;
    use nix::unistd::Pid;

    let Some(pid) = process_group.pid.and_then(|pid| i32::try_from(pid).ok()) else {
        abort_process_containment_failure();
    };
    let group = Pid::from_raw(pid);
    let deadline = tokio::time::Instant::now() + PROCESS_GROUP_CLEANUP_TIMEOUT;
    loop {
        match killpg(group, None) {
            Err(Errno::ESRCH) => return Ok(()),
            Ok(()) | Err(Errno::EPERM) => {
                // Reassert SIGKILL in case a group member was between fork and
                // exec during the first signal. If the bounded proof cannot
                // complete, terminate the harness rather than release the
                // outer privilege lease while a child may still be alive.
                kill_process_group(Some(pid as u32));
                if tokio::time::Instant::now() >= deadline {
                    abort_process_containment_failure();
                }
                tokio::time::sleep(PROCESS_GROUP_POLL_INTERVAL).await;
            }
            Err(_) => abort_process_containment_failure(),
        }
    }
}

#[cfg(unix)]
fn apply_isolated_environment(command: &mut Command) {
    command
        .env_clear()
        .env("PATH", isolated_path())
        .env("LC_ALL", "C")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_ASKPASS", "")
        .env("SSH_ASKPASS", "")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_PROTOCOL_FROM_USER", "0")
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env("GIT_NO_LAZY_FETCH", "1")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_NO_REPLACE_OBJECTS", "1");
}

#[cfg(windows)]
fn apply_isolated_environment(command: &mut Command) {
    command
        .env_clear()
        .env("PATH", isolated_path())
        .env("LC_ALL", "C")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_ASKPASS", "")
        .env("SSH_ASKPASS", "")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "NUL")
        .env("GIT_PROTOCOL_FROM_USER", "0")
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env("GIT_NO_LAZY_FETCH", "1")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_NO_REPLACE_OBJECTS", "1");
    if let Some(system_root) = windows_system_root() {
        command.env("SystemRoot", system_root);
    }
    let temporary = std::env::temp_dir();
    if temporary.is_absolute() && temporary.is_dir() {
        command.env("TEMP", &temporary).env("TMP", temporary);
    }
}

#[cfg(any(unix, windows))]
fn local_git_config(relay: &TrustedRelay) -> Vec<(String, String)> {
    let signer = relay.signer_pubkey();
    let npub = relay
        .keys
        .public_key()
        .to_bech32()
        .unwrap_or_else(|_| signer.clone());
    vec![
        ("core.hooksPath".into(), null_device().into()),
        ("core.fsmonitor".into(), "false".into()),
        ("core.askPass".into(), String::new()),
        ("core.attributesFile".into(), null_device().into()),
        ("credential.helper".into(), String::new()),
        ("credential.https://github.com.helper".into(), String::new()),
        ("protocol.allow".into(), "never".into()),
        ("protocol.https.allow".into(), "never".into()),
        ("protocol.http.allow".into(), "never".into()),
        ("protocol.ssh.allow".into(), "never".into()),
        ("protocol.file.allow".into(), "never".into()),
        ("http.sslVerify".into(), "true".into()),
        ("http.extraHeader".into(), String::new()),
        ("user.name".into(), npub),
        (
            "user.email".into(),
            format!("{signer}@{}", relay.relay_host),
        ),
        ("commit.gpgSign".into(), "false".into()),
        ("tag.gpgSign".into(), "false".into()),
    ]
}

#[cfg(any(unix, windows))]
fn apply_config_environment(command: &mut Command, config: &[(String, String)]) {
    command.env("GIT_CONFIG_COUNT", config.len().to_string());
    for (index, (key, value)) in config.iter().enumerate() {
        command.env(format!("GIT_CONFIG_KEY_{index}"), key);
        command.env(format!("GIT_CONFIG_VALUE_{index}"), value);
    }
}

#[cfg(target_os = "macos")]
fn parse_filled_credential(bytes: &[u8], expected_path: &str) -> Result<GitHubCredential, String> {
    if bytes.contains(&b'\r') || bytes.contains(&0) {
        return Err("GitHub credential helper returned invalid data".into());
    }
    let output = std::str::from_utf8(bytes)
        .map_err(|_| "GitHub credential helper returned invalid data".to_owned())?;
    let mut protocol = None;
    let mut host = None;
    let mut path = None;
    let mut username = None;
    let mut password = None;
    for line in output.lines().filter(|line| !line.is_empty()) {
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| "GitHub credential helper returned invalid data".to_owned())?;
        match key {
            "protocol" => set_once(&mut protocol, value)?,
            "host" => set_once(&mut host, value)?,
            "path" => set_once(&mut path, value)?,
            "username" => set_once(&mut username, value)?,
            "password" => set_once(&mut password, value)?,
            "quit" if value == "true" => {
                return Err("GitHub credential helper declined the request".into())
            }
            _ => {}
        }
    }
    if protocol != Some("https") || host != Some("github.com") || path != Some(expected_path) {
        return Err("GitHub credential helper returned a mismatched repository".into());
    }
    let username = username
        .filter(|value| valid_credential_value(value, MAX_CREDENTIAL_USERNAME_BYTES))
        .ok_or_else(|| "GitHub credential helper returned an invalid username".to_owned())?;
    let password = password
        .filter(|value| valid_credential_value(value, MAX_CREDENTIAL_PASSWORD_BYTES))
        .ok_or_else(|| "GitHub credential helper returned an invalid password".to_owned())?;
    Ok(GitHubCredential {
        username: Zeroizing::new(username.to_owned()),
        password: Zeroizing::new(password.to_owned()),
    })
}

#[cfg(target_os = "macos")]
fn set_once<'a>(slot: &mut Option<&'a str>, value: &'a str) -> Result<(), String> {
    if slot.replace(value).is_some() {
        Err("GitHub credential helper returned duplicate fields".into())
    } else {
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn valid_credential_value(value: &str, limit: usize) -> bool {
    !value.is_empty() && value.len() <= limit && !value.chars().any(char::is_control)
}

#[cfg(target_os = "macos")]
fn github_repository_path(repository: &str) -> Result<&str, String> {
    let path = repository
        .strip_prefix("https://github.com/")
        .ok_or_else(|| "trusted Git requires a canonical GitHub HTTPS repository".to_owned())?;
    if repository != repository.to_ascii_lowercase()
        || path.split('/').count() != 2
        || path.split('/').any(|part| !valid_github_part(part))
    {
        return Err("trusted Git requires a canonical GitHub HTTPS repository".into());
    }
    Ok(path)
}

#[cfg(target_os = "macos")]
fn valid_github_part(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(target_os = "macos")]
fn validate_object_id(value: &str) -> Result<(), String> {
    if matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err("trusted Git received an invalid object ID".into())
    }
}

#[cfg(target_os = "macos")]
fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| "failed to write private Git configuration".to_owned())?;
    file.write_all(bytes)
        .and_then(|_| file.flush())
        .map_err(|_| "failed to write private Git configuration".to_owned())?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|_| "failed to secure private Git configuration".to_owned())
}

#[cfg(unix)]
fn resolve_system_git() -> Option<PathBuf> {
    let candidates: &[&str] = if cfg!(target_os = "macos") {
        &[
            "/Library/Developer/CommandLineTools/usr/bin/git",
            "/Applications/Xcode.app/Contents/Developer/usr/bin/git",
            "/usr/bin/git",
        ]
    } else {
        &["/usr/bin/git", "/bin/git"]
    };
    candidates
        .iter()
        .map(PathBuf::from)
        .find(|candidate| candidate.is_file())
        .and_then(|candidate| candidate.canonicalize().ok())
}

#[cfg(windows)]
fn resolve_system_git() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    for base in [
        std::env::var_os("ProgramFiles"),
        std::env::var_os("ProgramFiles(x86)"),
    ]
    .into_iter()
    .flatten()
    {
        candidates.push(PathBuf::from(&base).join("Git").join("cmd").join("git.exe"));
        candidates.push(PathBuf::from(base).join("Git").join("bin").join("git.exe"));
    }
    if let Some(local) = std::env::var_os("LOCALAPPDATA") {
        candidates.push(
            PathBuf::from(local)
                .join("Programs")
                .join("Git")
                .join("cmd")
                .join("git.exe"),
        );
    }
    if let Some(registry) = windows_git_from_registry() {
        candidates.insert(0, registry.join("cmd").join("git.exe"));
        candidates.insert(1, registry.join("bin").join("git.exe"));
    }
    candidates.into_iter().find_map(windows_git_candidate)
}

#[cfg(windows)]
fn windows_git_candidate(candidate: PathBuf) -> Option<PathBuf> {
    if !candidate.is_absolute()
        || is_windows_apps_alias(&candidate)
        || !candidate
            .file_name()
            .is_some_and(|name| name.eq_ignore_ascii_case("git.exe"))
        || !candidate.is_file()
    {
        return None;
    }
    let canonical = candidate.canonicalize().ok()?;
    (!is_windows_apps_alias(&canonical)).then_some(canonical)
}

#[cfg(windows)]
fn is_windows_apps_alias(path: &Path) -> bool {
    let mut components = path.components().peekable();
    while components.peek().is_some() {
        let mut remaining = components.clone();
        if remaining.next().is_some_and(|component| {
            component
                .as_os_str()
                .to_string_lossy()
                .eq_ignore_ascii_case("Microsoft")
        }) && remaining.next().is_some_and(|component| {
            component
                .as_os_str()
                .to_string_lossy()
                .eq_ignore_ascii_case("WindowsApps")
        }) {
            return true;
        }
        components.next();
    }
    false
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn windows_git_from_registry() -> Option<PathBuf> {
    use std::os::windows::ffi::OsStringExt as _;
    use windows_sys::Win32::Foundation::{ERROR_MORE_DATA, ERROR_SUCCESS};
    use windows_sys::Win32::System::Registry::{
        RegCloseKey, RegOpenKeyExW, RegQueryValueExW, HKEY_CURRENT_USER, HKEY_LOCAL_MACHINE,
        KEY_READ,
    };

    let key: Vec<u16> = "SOFTWARE\\GitForWindows"
        .encode_utf16()
        .chain(Some(0))
        .collect();
    let value: Vec<u16> = "InstallPath".encode_utf16().chain(Some(0)).collect();
    // SAFETY: key/value buffers remain null terminated for each call and every
    // successfully opened registry handle is closed before returning.
    unsafe {
        for hive in [HKEY_LOCAL_MACHINE, HKEY_CURRENT_USER] {
            let mut handle = std::ptr::null_mut();
            if RegOpenKeyExW(hive, key.as_ptr(), 0, KEY_READ, &mut handle) != ERROR_SUCCESS {
                continue;
            }
            let mut byte_len = 0;
            let status = RegQueryValueExW(
                handle,
                value.as_ptr(),
                std::ptr::null(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut byte_len,
            );
            if (status != ERROR_SUCCESS && status != ERROR_MORE_DATA) || byte_len == 0 {
                RegCloseKey(handle);
                continue;
            }
            let mut data = vec![0_u16; (byte_len as usize).div_ceil(2)];
            let status = RegQueryValueExW(
                handle,
                value.as_ptr(),
                std::ptr::null(),
                std::ptr::null_mut(),
                data.as_mut_ptr().cast(),
                &mut byte_len,
            );
            RegCloseKey(handle);
            if status != ERROR_SUCCESS {
                continue;
            }
            while data.last() == Some(&0) {
                data.pop();
            }
            let root = PathBuf::from(OsString::from_wide(&data));
            if root.is_absolute() && root.is_dir() {
                return Some(root);
            }
        }
    }
    None
}

#[cfg(unix)]
fn isolated_path() -> OsString {
    "/usr/bin:/bin:/usr/sbin:/sbin".into()
}

#[cfg(windows)]
fn isolated_path() -> OsString {
    let mut directories = Vec::new();
    if let Some(git) = resolve_system_git() {
        if let Some(parent) = git.parent() {
            directories.push(parent.to_path_buf());
        }
    }
    if let Some(root) = windows_system_root() {
        directories.push(root.join("System32"));
    }
    std::env::join_paths(directories).unwrap_or_default()
}

#[cfg(windows)]
fn windows_system_root() -> Option<PathBuf> {
    let root = PathBuf::from(std::env::var_os("SystemRoot")?);
    (root.is_absolute() && root.is_dir()).then_some(root)
}

#[cfg(unix)]
fn null_device() -> &'static str {
    "/dev/null"
}

#[cfg(windows)]
fn null_device() -> &'static str {
    "NUL"
}

#[cfg(target_os = "macos")]
fn operator_helper_path() -> OsString {
    "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin".into()
}

#[cfg(unix)]
fn set_process_group(command: &mut Command) {
    command.process_group(0).kill_on_drop(true);
}

#[cfg(windows)]
fn set_process_group(command: &mut Command) {
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    command.creation_flags(CREATE_NO_WINDOW).kill_on_drop(true);
}

#[cfg(unix)]
struct ProcessGroupGuard {
    pid: Option<u32>,
}

#[cfg(unix)]
impl ProcessGroupGuard {
    fn new(_child: &tokio::process::Child, pid: Option<u32>) -> Result<Self, String> {
        if pid.is_none() {
            return Err("trusted Git process identity is unavailable".into());
        }
        Ok(Self { pid })
    }

    fn terminate(&self) {
        kill_process_group(self.pid);
    }

    fn disarm(&mut self) {
        self.pid = None;
    }
}

#[cfg(unix)]
impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        kill_process_group(self.pid);
        // `run_managed` normally disarms only after its async reap proof. If
        // the whole future is dropped or unwinds, synchronously complete the
        // same proof before stack unwinding can drop the outer operation
        // lease. Cleanup is time-bounded, but failure to prove containment is
        // fatal so the lease is never silently released around a live child.
        reap_process_group_blocking(self.pid);
    }
}

#[cfg(unix)]
fn kill_process_group(pid: Option<u32>) {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;
    if let Some(pid) = pid.and_then(|pid| i32::try_from(pid).ok()) {
        let _ = killpg(Pid::from_raw(pid), Signal::SIGKILL);
    }
}

#[cfg(unix)]
fn reap_process_group_blocking(pid: Option<u32>) {
    use nix::errno::Errno;
    use nix::sys::signal::killpg;
    use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
    use nix::unistd::Pid;

    let Some(pid) = pid.and_then(|pid| i32::try_from(pid).ok()) else {
        // A disarmed guard stores `None`; there is no process left to reap.
        return;
    };
    let process = Pid::from_raw(pid);
    let deadline = std::time::Instant::now() + PROCESS_GROUP_CLEANUP_TIMEOUT;
    loop {
        match waitpid(process, Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) => {
                kill_process_group(Some(pid as u32));
            }
            Ok(_) | Err(Errno::ECHILD) => break,
            Err(Errno::EINTR) => continue,
            Err(_) => abort_process_containment_failure(),
        }
        if std::time::Instant::now() >= deadline {
            abort_process_containment_failure();
        }
        std::thread::sleep(PROCESS_GROUP_POLL_INTERVAL);
    }
    loop {
        match killpg(process, None) {
            Err(Errno::ESRCH) => break,
            Ok(()) | Err(Errno::EPERM) => {
                kill_process_group(Some(pid as u32));
            }
            Err(_) => abort_process_containment_failure(),
        }
        if std::time::Instant::now() >= deadline {
            abort_process_containment_failure();
        }
        std::thread::sleep(PROCESS_GROUP_POLL_INTERVAL);
    }
}

#[cfg(windows)]
struct ProcessGroupGuard {
    job: windows_sys::Win32::Foundation::HANDLE,
    armed: bool,
}

#[cfg(windows)]
#[allow(unsafe_code)]
unsafe impl Send for ProcessGroupGuard {}

#[cfg(windows)]
#[allow(unsafe_code)]
unsafe impl Sync for ProcessGroupGuard {}

#[cfg(windows)]
#[allow(unsafe_code)]
impl ProcessGroupGuard {
    fn new(child: &tokio::process::Child, _pid: Option<u32>) -> Result<Self, String> {
        use std::mem::{size_of, zeroed};
        use windows_sys::Win32::Foundation::{CloseHandle, FALSE, HANDLE};
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
            SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        };

        let process = child
            .raw_handle()
            .ok_or_else(|| "trusted Git process handle is unavailable".to_owned())?
            as HANDLE;
        // SAFETY: the anonymous job has no caller-owned security attributes or
        // name. `info` is the exact documented Win32 layout and `process` is
        // borrowed from the live Tokio child until assignment completes.
        unsafe {
            let job: HANDLE = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if job.is_null() {
                return Err("failed to create trusted Git process container".into());
            }
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = zeroed();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                std::ptr::addr_of!(info).cast(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            ) == FALSE
                || AssignProcessToJobObject(job, process) == FALSE
            {
                CloseHandle(job);
                return Err("failed to contain trusted Git process tree".into());
            }
            Ok(Self { job, armed: true })
        }
    }

    fn terminate(&self) {
        use windows_sys::Win32::System::JobObjects::TerminateJobObject;
        if self.armed && !self.job.is_null() {
            // SAFETY: `job` is live until this guard's Drop closes it.
            unsafe {
                TerminateJobObject(self.job, 137);
            }
        }
    }

    fn active_processes(&self) -> Result<u32, String> {
        use std::mem::{size_of, zeroed};
        use windows_sys::Win32::Foundation::FALSE;
        use windows_sys::Win32::System::JobObjects::{
            JobObjectBasicAccountingInformation, QueryInformationJobObject,
            JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
        };
        // SAFETY: `info` has the exact queried layout and the output size
        // matches it. The job handle remains owned by this guard.
        unsafe {
            let mut info: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = zeroed();
            if QueryInformationJobObject(
                self.job,
                JobObjectBasicAccountingInformation,
                std::ptr::addr_of_mut!(info).cast(),
                size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                std::ptr::null_mut(),
            ) == FALSE
            {
                return Err("failed to inspect trusted Git process container".into());
            }
            Ok(info.ActiveProcesses)
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(windows)]
async fn wait_for_process_group_exit(process_group: &ProcessGroupGuard) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + PROCESS_GROUP_CLEANUP_TIMEOUT;
    loop {
        if process_group.active_processes()? == 0 {
            return Ok(());
        }
        process_group.terminate();
        if tokio::time::Instant::now() >= deadline {
            abort_process_containment_failure();
        }
        tokio::time::sleep(PROCESS_GROUP_POLL_INTERVAL).await;
    }
}

#[cfg(windows)]
#[allow(unsafe_code)]
impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        use windows_sys::Win32::Foundation::CloseHandle;
        if self.job.is_null() {
            return;
        }
        if self.armed {
            self.terminate();
            let deadline = std::time::Instant::now() + PROCESS_GROUP_CLEANUP_TIMEOUT;
            loop {
                match self.active_processes() {
                    Ok(0) => break,
                    Ok(_) => self.terminate(),
                    Err(_) => abort_process_containment_failure(),
                }
                if std::time::Instant::now() >= deadline {
                    abort_process_containment_failure();
                }
                std::thread::sleep(PROCESS_GROUP_POLL_INTERVAL);
            }
        }
        // SAFETY: the handle is owned by this guard and closed exactly once.
        unsafe {
            CloseHandle(self.job);
        }
        self.job = std::ptr::null_mut();
    }
}

#[cfg(any(unix, windows))]
fn abort_process_containment_failure() -> ! {
    // This path means an active trusted child may still hold the operation's
    // privilege lease. Continuing would be a boundary violation; abort is the
    // bounded fail-closed outcome and lets the supervisor restart cleanly.
    eprintln!("fatal: failed to contain a trusted Git process tree");
    std::process::abort()
}

#[cfg(any(unix, windows))]
fn local_error(bytes: &[u8]) -> String {
    let message = String::from_utf8_lossy(bytes).trim().to_owned();
    if message.is_empty() {
        "trusted Git operation failed".into()
    } else {
        message
    }
}

#[cfg(target_os = "macos")]
fn network_error(error: String) -> String {
    match error.as_str() {
        CANCELLED_ERROR | TIMED_OUT_ERROR => error,
        _ => "trusted Git network operation failed".into(),
    }
}

pub(super) fn is_cancellation_error(error: &str) -> bool {
    error == CANCELLED_ERROR
}

pub(super) fn text(bytes: &[u8]) -> Result<&str, String> {
    std::str::from_utf8(bytes)
        .map(str::trim)
        .map_err(|_| "trusted Git output was not UTF-8".into())
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use crate::trusted::{GrantSet, TrustedConfig};

    fn run_test_git(cwd: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new(resolve_system_git().expect("Git for Windows"))
            .args(args)
            .current_dir(cwd)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "NUL")
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .expect("run test Git");
        assert!(
            output.status.success(),
            "test Git failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("UTF-8 test Git output")
            .trim()
            .to_owned()
    }

    fn test_relay() -> TrustedRelay {
        let keys = nostr::Keys::generate();
        TrustedRelay::new(TrustedConfig {
            relay_url: "http://127.0.0.1:1".to_owned(),
            owner_pubkey: keys.public_key().to_hex(),
            keys,
            auth_tag: None,
            auth_tag_json: None,
            owner_github_login: None,
            grants: GrantSet::default(),
            a2a_channel_id: None,
            session_channel_id: None,
            session_thread_root_id: None,
            job_operation_id: None,
            job_request_event_id: None,
            session_working_directory: None,
            github_credentials: Default::default(),
            allow_insecure_loopback: true,
        })
        .expect("test relay")
    }

    #[tokio::test]
    async fn checkout_inspection_and_local_commit_run_in_a_windows_job() {
        let repository = tempfile::tempdir().expect("repository");
        run_test_git(
            repository.path(),
            &["init", "--quiet", "--initial-branch=main"],
        );
        std::fs::write(repository.path().join("tracked.txt"), b"windows\n").expect("write fixture");
        run_test_git(repository.path(), &["add", "--", "tracked.txt"]);
        run_test_git(
            repository.path(),
            &[
                "-c",
                "user.name=Windows Test",
                "-c",
                "user.email=windows@example.invalid",
                "-c",
                "commit.gpgSign=false",
                "commit",
                "--quiet",
                "--no-gpg-sign",
                "-m",
                "fixture",
            ],
        );
        let head = run_test_git(repository.path(), &["rev-parse", "HEAD"]);
        let root = repository
            .path()
            .canonicalize()
            .expect("canonical checkout");
        let checkout = TrustedGitCheckout {
            root: root.clone(),
            git_common_dir: root.join(".git").canonicalize().expect("Git directory"),
            repository: "https://github.com/mysteropodes/nemo".to_owned(),
            base_sha: head.clone(),
            head_sha: head.clone(),
            branch: "main".to_owned(),
            path_prefixes: vec!["tracked.txt".to_owned()],
            repository_wide: true,
        };
        let inspected = inspect_checkout_git(
            &root,
            &["rev-parse", "--verify", &format!("{head}^{{commit}}")],
        )
        .await
        .expect("bounded checkout inspection");
        assert!(inspected.status.success());
        assert_eq!(text(&inspected.stdout).expect("inspection text"), head);
        let relay = test_relay();
        let output = git_output(
            &relay,
            &checkout,
            &["rev-parse", "HEAD"],
            &CancellationToken::new(),
        )
        .await
        .expect("trusted local Git");
        assert_eq!(text(&output.stdout).expect("Git text"), head);

        std::fs::write(root.join("tracked.txt"), b"windows changed\n").expect("update fixture");
        run_test_git(&root, &["add", "--", "tracked.txt"]);
        let tree = git_output(
            &relay,
            &checkout,
            &["write-tree"],
            &CancellationToken::new(),
        )
        .await
        .expect("write tree");
        let tree = text(&tree.stdout).expect("tree id").to_owned();
        let commit = git_output(
            &relay,
            &checkout,
            &[
                "commit-tree",
                &tree,
                "-p",
                &head,
                "--no-gpg-sign",
                "-m",
                "trusted Windows commit",
            ],
            &CancellationToken::new(),
        )
        .await
        .expect("create commit");
        let commit = text(&commit.stdout).expect("commit id").to_owned();
        let outcome = update_local_ref_reconciled(
            &relay,
            &checkout,
            "refs/heads/main",
            Some(head),
            commit.clone(),
            "test: trusted Windows ref update",
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(outcome.disposition, PrivilegedGitDisposition::Applied);
        assert_eq!(outcome.observed_object.as_deref(), Some(commit.as_str()));
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;
    use crate::trusted::{GrantSet, TrustedConfig};

    fn run_test_git(cwd: &Path, args: &[&str]) -> String {
        let git = resolve_system_git().expect("system Git");
        let output = std::process::Command::new(git)
            .args(args)
            .current_dir(cwd)
            .env_clear()
            .env("PATH", isolated_path())
            .env("LC_ALL", "C")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .expect("run test Git");
        assert!(
            output.status.success(),
            "test Git failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("UTF-8 test Git output")
            .trim()
            .to_owned()
    }

    fn write_executable(path: &Path, body: &str) {
        write_private_file(path, body.as_bytes()).expect("write script");
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .expect("make executable");
    }

    fn write_fake_helper(home: &Path, body: &str) {
        let helper = home.join("fake-credential-helper");
        write_executable(&helper, body);
        let helper = helper
            .to_string_lossy()
            .replace('\\', "\\\\")
            .replace('"', "\\\"");
        write_private_file(
            &home.join(".gitconfig"),
            format!("[credential]\n\thelper = \"{helper}\"\n").as_bytes(),
        )
        .expect("write gitconfig");
    }

    fn test_relay() -> TrustedRelay {
        let keys = nostr::Keys::generate();
        TrustedRelay::new(TrustedConfig {
            relay_url: "http://127.0.0.1:1".to_owned(),
            owner_pubkey: keys.public_key().to_hex(),
            keys,
            auth_tag: None,
            auth_tag_json: None,
            owner_github_login: None,
            grants: GrantSet::default(),
            a2a_channel_id: None,
            session_channel_id: None,
            session_thread_root_id: None,
            job_operation_id: None,
            job_request_event_id: None,
            session_working_directory: None,
            github_credentials: Default::default(),
            allow_insecure_loopback: true,
        })
        .expect("test relay")
    }

    #[tokio::test]
    async fn fake_helper_is_called_once_per_repository_and_only_values_are_retained() {
        let Some(git) = resolve_system_git() else {
            return;
        };
        let home = tempfile::tempdir().expect("home");
        write_fake_helper(
            home.path(),
            "#!/bin/sh\npwd > \"${0%/*}/helper-cwd\"\n\
             if /usr/bin/git rev-parse --git-dir >/dev/null 2>&1; then\n\
             printf '%s\\n' repository > \"${0%/*}/helper-repository-state\"\n\
             else\n\
             printf '%s\\n' non-repository > \"${0%/*}/helper-repository-state\"\n\
             fi\n\
             printf '%s\\n' \"$1\" >> \"${0%/*}/calls\"\n\
             if [ \"$1\" = get ]; then\n\
             printf '%s\\n' 'username=fake-user' 'password=fake-token'\n\
             fi\n",
        );
        let repositories = vec![
            "https://github.com/block/buzz".to_owned(),
            "https://github.com/mysteropodes/nemo".to_owned(),
        ];
        let store = capture_operator_github_credentials_with(
            &git,
            home.path().as_os_str(),
            &repositories,
            Duration::from_secs(5),
        )
        .await
        .expect("capture credentials");
        let calls = std::fs::read_to_string(home.path().join("calls")).expect("helper calls");
        assert_eq!(calls.lines().collect::<Vec<_>>(), ["get", "get"]);
        let helper_cwd = std::fs::read_to_string(home.path().join("helper-cwd"))
            .expect("helper cwd")
            .trim()
            .to_owned();
        assert_ne!(Path::new(&helper_cwd), std::env::current_dir().unwrap());
        assert!(Path::new(&helper_cwd).starts_with(PRIVATE_TEMP_ROOT));
        assert_eq!(
            std::fs::read_to_string(home.path().join("helper-repository-state"))
                .expect("helper repository state")
                .trim(),
            "non-repository"
        );
        assert!(
            !Path::new(&helper_cwd).exists(),
            "capture directory was not removed"
        );
        let records = store
            .records_for("https://github.com/block/buzz")
            .expect("credential records");
        let records = std::str::from_utf8(&records).expect("UTF-8 records");
        assert!(records.contains("username=fake-user\n"));
        assert!(records.contains("password=fake-token\n"));
        assert!(!records.contains("fake-credential-helper"));
    }

    #[tokio::test]
    async fn credential_capture_errors_never_replay_helper_output() {
        let Some(git) = resolve_system_git() else {
            return;
        };
        let home = tempfile::tempdir().expect("home");
        write_fake_helper(
            home.path(),
            "#!/bin/sh\nprintf '%s\\n' 'sentinel-secret-from-helper' >&2\nexit 1\n",
        );
        let error = capture_operator_github_credentials_with(
            &git,
            home.path().as_os_str(),
            &["https://github.com/block/buzz".to_owned()],
            Duration::from_secs(5),
        )
        .await
        .err()
        .expect("capture must fail");
        assert!(!error.contains("sentinel-secret-from-helper"));
    }

    #[tokio::test]
    async fn timed_out_capture_kills_the_entire_process_group() {
        let home = tempfile::tempdir().expect("home");
        let fake_git = home.path().join("fake-git");
        write_executable(
            &fake_git,
            "#!/bin/sh\n/bin/sleep 30 &\nprintf '%s' \"$!\" > \"${0%/*}/child-pid\"\n/bin/sleep 30\n",
        );
        let error = capture_operator_github_credentials_with(
            &fake_git,
            home.path().as_os_str(),
            &["https://github.com/block/buzz".to_owned()],
            Duration::from_secs(2),
        )
        .await
        .err()
        .expect("capture must time out");
        assert_eq!(error, "GitHub credential capture failed");
        let pid = std::fs::read_to_string(home.path().join("child-pid"))
            .expect("child pid")
            .parse::<i32>()
            .expect("numeric pid");
        let mut gone = false;
        for _ in 0..100 {
            if nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err() {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(gone, "capture grandchild survived process-group cleanup");
    }

    #[tokio::test]
    async fn credential_capture_uses_one_overall_deadline() {
        let home = tempfile::tempdir().expect("home");
        let fake_git = home.path().join("fake-git");
        write_executable(
            &fake_git,
            "#!/bin/sh\n/bin/sleep 0.15\n/bin/cat\nprintf '%s\\n' \\
             'username=fake-user' 'password=fake-token'\n",
        );
        let repositories = [
            "https://github.com/block/buzz".to_owned(),
            "https://github.com/mysteropodes/nemo".to_owned(),
        ];
        let started = tokio::time::Instant::now();
        let error = capture_operator_github_credentials_with(
            &fake_git,
            home.path().as_os_str(),
            &repositories,
            Duration::from_millis(250),
        )
        .await
        .err()
        .expect("second repository must exhaust the shared deadline");
        assert_eq!(error, "GitHub credential capture failed");
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn successful_capture_kills_a_background_process_before_returning() {
        let home = tempfile::tempdir().expect("home");
        let fake_git = home.path().join("fake-git");
        write_executable(
            &fake_git,
            "#!/bin/sh\n/bin/sleep 30 &\nprintf '%s' \"$!\" > \"${0%/*}/child-pid\"\n\
             printf '%s\\n' 'protocol=https' 'host=github.com' 'path=block/buzz' \\
             'username=fake-user' 'password=fake-token'\n",
        );
        let store = capture_operator_github_credentials_with(
            &fake_git,
            home.path().as_os_str(),
            &["https://github.com/block/buzz".to_owned()],
            Duration::from_secs(5),
        )
        .await
        .expect("capture succeeds");
        assert!(!store.is_empty());
        let pid = std::fs::read_to_string(home.path().join("child-pid"))
            .expect("child pid")
            .parse::<i32>()
            .expect("numeric pid");
        let mut gone = false;
        for _ in 0..100 {
            if nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err() {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(gone, "successful capture left a background child alive");
    }

    #[tokio::test]
    async fn dropped_runner_future_reaps_its_direct_child_and_process_group() {
        let fixture = tempfile::tempdir().expect("fixture");
        let fake_git = fixture.path().join("fake-git");
        write_executable(
            &fake_git,
            "#!/bin/sh\n/bin/sleep 30 &\nprintf '%s' \"$!\" > \"${0%/*}/child-pid\"\nwait\n",
        );
        let child_pid = fixture.path().join("child-pid");
        let mut command = Command::new(&fake_git);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        apply_isolated_environment(&mut command);
        set_process_group(&mut command);
        let cancellation = CancellationToken::new();
        let runner = tokio::spawn(async move {
            run_managed(
                command,
                None,
                Duration::from_secs(60),
                MAX_OUTPUT_BYTES,
                &cancellation,
            )
            .await
        });
        for _ in 0..500 {
            if child_pid.is_file() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(child_pid.is_file(), "fake child did not start");
        runner.abort();
        let _ = runner.await;

        let pid = std::fs::read_to_string(child_pid)
            .expect("child pid")
            .parse::<i32>()
            .expect("numeric child pid");
        assert!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err(),
            "runner future dropped before its process group was gone"
        );
    }

    #[tokio::test]
    async fn checkout_inspection_timeout_reaps_descendants() {
        let fixture = tempfile::tempdir().expect("fixture");
        let fake_git = fixture.path().join("fake-git");
        write_executable(
            &fake_git,
            "#!/bin/sh\n/bin/sleep 30 &\nprintf '%s' \"$!\" > \"${0%/*}/inspection-child-pid\"\nwait\n",
        );
        let started = std::time::Instant::now();
        let error = match inspect_checkout_git_with(
            &fake_git,
            fixture.path(),
            &["rev-parse", "HEAD"],
            Duration::from_millis(500),
        )
        .await
        {
            Err(error) => error,
            Ok(_) => panic!("inspection must time out"),
        };
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "bounded inspection exceeded its cleanup budget"
        );
        assert!(error.contains("timed out"));
        let pid = std::fs::read_to_string(fixture.path().join("inspection-child-pid"))
            .expect("inspection child pid")
            .parse::<i32>()
            .expect("numeric inspection child pid");
        assert!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err(),
            "bounded inspection returned before its process group was gone"
        );
    }

    #[tokio::test]
    async fn one_shot_secret_fd_keeps_credentials_out_of_argv_and_environment() {
        let Some(system_git) = resolve_system_git() else {
            return;
        };
        let cancellation = CancellationToken::new();
        let private = PrivateBareRepo::create(&system_git, &cancellation)
            .await
            .expect("private repo");
        private
            .configure_for_network("https://github.com/block/buzz")
            .expect("private config");
        let fake_git = private.path().join("fake-git");
        write_executable(
            &fake_git,
            "#!/bin/sh\nprintf 'ARG=%s\\n' \"$@\"\n/usr/bin/env\n/bin/cat <&3 >/dev/null\n",
        );
        let sentinel = "fd-only-sentinel-token";
        let output = run_network_git_with(
            &fake_git,
            &private,
            &["push", "https://github.com/block/buzz", "deadbeef"],
            Zeroizing::new(format!("username=fake\npassword={sentinel}\n\n").into_bytes()),
            &cancellation,
        )
        .await
        .expect("fake network Git");
        assert!(output.status.success());
        assert!(!String::from_utf8_lossy(&output.stdout).contains(sentinel));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(sentinel));
    }

    #[tokio::test]
    async fn private_git_credential_helper_consumes_the_inherited_fd_once() {
        let Some(git) = resolve_system_git() else {
            return;
        };
        let cancellation = CancellationToken::new();
        let private = PrivateBareRepo::create(&git, &cancellation)
            .await
            .expect("private repo");
        private
            .configure_for_network("https://github.com/block/buzz")
            .expect("private config");
        let query = private.path().join("credential-query");
        write_private_file(
            &query,
            b"protocol=https\nhost=github.com\npath=block/buzz\n\n",
        )
        .expect("credential query");

        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("exec 3<&0; exec 0<\"$1\"; shift; exec \"$@\"")
            .arg("credential-probe")
            .arg(&query)
            .arg(&git)
            .arg("--git-dir=.")
            .args(["credential", "fill"])
            .current_dir(private.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        apply_isolated_environment(&mut command);
        set_process_group(&mut command);
        let sentinel = "one-shot-helper-sentinel";
        let output = run_managed(
            command,
            Some(Zeroizing::new(
                format!("username=fake\npassword={sentinel}\n\n").into_bytes(),
            )),
            Duration::from_secs(5),
            MAX_OUTPUT_BYTES,
            &cancellation,
        )
        .await
        .expect("credential fill");
        assert!(output.status.success(), "credential fill failed");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("username=fake\n"));
        assert!(stdout.contains(&format!("password={sentinel}\n")));
    }

    #[tokio::test]
    async fn local_commit_and_fetch_refs_are_cas_updated_and_observed_exactly() {
        let source = tempfile::tempdir().expect("source");
        run_test_git(source.path(), &["init", "--quiet", "--initial-branch=main"]);
        std::fs::write(source.path().join("tracked.txt"), b"first\n").expect("first file");
        run_test_git(source.path(), &["add", "--", "tracked.txt"]);
        run_test_git(
            source.path(),
            &[
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "commit.gpgSign=false",
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--quiet",
                "--no-gpg-sign",
                "-m",
                "first",
            ],
        );
        let first = run_test_git(source.path(), &["rev-parse", "HEAD"]);
        std::fs::write(source.path().join("tracked.txt"), b"second\n").expect("second file");
        run_test_git(source.path(), &["add", "--", "tracked.txt"]);
        run_test_git(
            source.path(),
            &[
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "commit.gpgSign=false",
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--quiet",
                "--no-gpg-sign",
                "-m",
                "second",
            ],
        );
        let second = run_test_git(source.path(), &["rev-parse", "HEAD"]);
        run_test_git(source.path(), &["reset", "--hard", &first]);
        run_test_git(
            source.path(),
            &["update-ref", "refs/remotes/origin/main", &first],
        );

        let root = source.path().canonicalize().expect("canonical checkout");
        let checkout = TrustedGitCheckout {
            root: root.clone(),
            git_common_dir: root.join(".git").canonicalize().expect("Git directory"),
            repository: "https://github.com/block/buzz".to_owned(),
            base_sha: first.clone(),
            head_sha: first.clone(),
            branch: "main".to_owned(),
            path_prefixes: vec!["tracked.txt".to_owned()],
            repository_wide: false,
        };
        let relay = test_relay();
        for full_ref in ["refs/heads/main", "refs/remotes/origin/main"] {
            let outcome = update_local_ref_reconciled(
                &relay,
                &checkout,
                full_ref,
                Some(first.clone()),
                second.clone(),
                "test: exact trusted ref update",
                &CancellationToken::new(),
            )
            .await;
            assert_eq!(outcome.disposition, PrivilegedGitDisposition::Applied);
            assert_eq!(outcome.previous_object.as_deref(), Some(first.as_str()));
            assert_eq!(outcome.intended_object, second);
            assert_eq!(outcome.observed_object.as_deref(), Some(second.as_str()));
            assert!(outcome.command_error.is_none());
            assert_eq!(
                local_ref(&relay, &checkout, full_ref, &CancellationToken::new())
                    .await
                    .expect("exact ref read")
                    .as_deref(),
                Some(second.as_str())
            );
        }
    }

    #[tokio::test]
    async fn cancelled_push_applied_by_remote_is_reconciled_to_the_exact_object() {
        let Some(git) = resolve_system_git() else {
            return;
        };
        let repository = "https://github.com/block/buzz";
        let branch_ref = "refs/heads/main";
        let previous = "1111111111111111111111111111111111111111";
        let intended = "2222222222222222222222222222222222222222";
        let fixture = tempfile::tempdir().expect("fixture");
        let state = fixture.path().join("remote-ref");
        let accepted = fixture.path().join("accepted");
        let child_pid = fixture.path().join("child-pid");
        std::fs::write(&state, previous).expect("initial remote ref");
        for path in [&state, &accepted, &child_pid] {
            assert!(
                !path.to_string_lossy().contains('\''),
                "temporary test path unexpectedly needs shell escaping"
            );
        }

        let private = PrivateBareRepo::create(&git, &CancellationToken::new())
            .await
            .expect("private repo");
        private
            .configure_for_network(repository)
            .expect("private network config");
        let fake_git = fixture.path().join("fake-git");
        write_executable(
            &fake_git,
            &format!(
                "#!/bin/sh\n\
                 state='{}'\n\
                 accepted='{}'\n\
                 child_pid='{}'\n\
                 mode=\n\
                 target=\n\
                 for argument in \"$@\"; do\n\
                   case \"$argument\" in\n\
                     ls-remote) mode=read ;;\n\
                     push) mode=push ;;\n\
                     *:refs/heads/main) target=${{argument%%:*}} ;;\n\
                   esac\n\
                 done\n\
                 /bin/cat <&3 >/dev/null\n\
                 if [ \"$mode\" = read ]; then\n\
                   value=$(/bin/cat \"$state\")\n\
                   if [ -n \"$value\" ]; then\n\
                     printf '%s\\t%s\\n' \"$value\" refs/heads/main\n\
                   fi\n\
                   exit 0\n\
                 fi\n\
                 if [ \"$mode\" = push ] && [ -n \"$target\" ]; then\n\
                   printf '%s' \"$target\" > \"$state\"\n\
                   : > \"$accepted\"\n\
                   /bin/sleep 30 &\n\
                   printf '%s' \"$!\" > \"$child_pid\"\n\
                   wait\n\
                 fi\n\
                 exit 1\n",
                state.display(),
                accepted.display(),
                child_pid.display(),
            ),
        );
        let mut by_repository = BTreeMap::new();
        by_repository.insert(
            repository.to_owned(),
            GitHubCredential {
                username: Zeroizing::new("fake-user".to_owned()),
                password: Zeroizing::new("fake-token".to_owned()),
            },
        );
        let credentials = GitHubCredentialStore {
            _inner: Arc::new(GitHubCredentialStoreInner { by_repository }),
        };
        let cancellation = CancellationToken::new();
        let cancel = cancellation.clone();
        let wait_for_accept = async {
            for _ in 0..500 {
                if accepted.is_file() {
                    cancel.cancel();
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("fake remote never accepted the ref");
        };
        let (outcome, ()) = tokio::join!(
            push_ref_reconciled(
                &fake_git,
                &private,
                &credentials,
                repository,
                branch_ref,
                intended,
                &cancellation,
            ),
            wait_for_accept,
        );
        let outcome = outcome.expect("reconciled push outcome");

        assert_eq!(outcome.disposition, PrivilegedGitDisposition::Applied);
        assert_eq!(outcome.previous_object.as_deref(), Some(previous));
        assert_eq!(outcome.intended_object, intended);
        assert_eq!(outcome.observed_object.as_deref(), Some(intended));
        assert_eq!(outcome.command_error.as_deref(), Some(CANCELLED_ERROR));
        assert_eq!(
            std::fs::read_to_string(&state).expect("remote state"),
            intended
        );
        let pid = std::fs::read_to_string(&child_pid)
            .expect("fake remote child pid")
            .parse::<i32>()
            .expect("numeric fake remote child pid");
        assert!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err(),
            "run_managed returned before the cancelled process group was gone"
        );
    }

    #[test]
    fn exact_ref_reconciliation_never_guesses_across_a_third_object() {
        let previous = Some("1111111111111111111111111111111111111111".to_owned());
        let intended = "2222222222222222222222222222222222222222".to_owned();
        let third = Some("3333333333333333333333333333333333333333".to_owned());
        let ambiguous = reconciled_ref_outcome(
            previous.clone(),
            intended.clone(),
            Err(CANCELLED_ERROR.into()),
            Ok(third.clone()),
        );
        assert_eq!(ambiguous.disposition, PrivilegedGitDisposition::Ambiguous);
        assert_eq!(ambiguous.previous_object, previous);
        assert_eq!(ambiguous.intended_object, intended);
        assert_eq!(ambiguous.observed_object, third);
    }

    #[tokio::test]
    async fn private_pack_import_and_push_alternate_use_the_exact_object_database() {
        let Some(git) = resolve_system_git() else {
            return;
        };
        let source = tempfile::tempdir().expect("source");
        run_test_git(source.path(), &["init", "--quiet", "--initial-branch=main"]);
        std::fs::write(source.path().join("tracked.txt"), b"trusted object\n")
            .expect("write tracked file");
        run_test_git(source.path(), &["add", "--", "tracked.txt"]);
        run_test_git(
            source.path(),
            &[
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "commit.gpgSign=false",
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--quiet",
                "--no-gpg-sign",
                "-m",
                "test",
            ],
        );
        let commit = run_test_git(source.path(), &["rev-parse", "HEAD"]);
        validate_object_id(&commit).expect("commit ID");
        let cancellation = CancellationToken::new();

        let received = PrivateBareRepo::create(&git, &cancellation)
            .await
            .expect("received repo");
        received
            .configure_for_network("https://github.com/block/buzz")
            .expect("private config");
        let source_path = source.path().to_str().expect("UTF-8 source path");
        received
            .output(
                &git,
                &[
                    "-c",
                    "protocol.file.allow=always",
                    "fetch",
                    "--quiet",
                    "--no-tags",
                    source_path,
                    "refs/heads/main:refs/buzz/remote",
                ],
                &cancellation,
            )
            .await
            .expect("local fixture fetch");
        received
            .output(&git, &["repack", "-a", "-d", "-q"], &cancellation)
            .await
            .expect("repack fixture");

        let imported = PrivateBareRepo::create(&git, &cancellation)
            .await
            .expect("imported repo");
        import_private_packs(
            &git,
            &received,
            &imported.path().join("objects"),
            &cancellation,
        )
        .await
        .expect("import pack");
        imported
            .output(
                &git,
                &["cat-file", "-e", &format!("{commit}^{{commit}}")],
                &cancellation,
            )
            .await
            .expect("imported exact commit");

        let alternate = PrivateBareRepo::create(&git, &cancellation)
            .await
            .expect("alternate repo");
        alternate
            .install_alternate(
                &source
                    .path()
                    .join(".git/objects")
                    .canonicalize()
                    .expect("source objects"),
            )
            .expect("install alternate");
        alternate
            .output(
                &git,
                &["cat-file", "-e", &format!("{commit}^{{commit}}")],
                &cancellation,
            )
            .await
            .expect("alternate exact commit");
    }

    #[tokio::test]
    async fn private_bare_repository_is_mode_0700_and_removed_on_drop() {
        let Some(git) = resolve_system_git() else {
            return;
        };
        let cancellation = CancellationToken::new();
        let path;
        {
            let private = PrivateBareRepo::create(&git, &cancellation)
                .await
                .expect("private repo");
            path = private.path().to_path_buf();
            let mode = private
                .path()
                .metadata()
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o700);
            private
                .configure_for_network("https://github.com/block/buzz")
                .expect("private config");
            let config_mode = private
                .path()
                .join("config")
                .metadata()
                .expect("config metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(config_mode, 0o600);
        }
        assert!(!path.exists());
    }

    #[test]
    fn rejects_noncanonical_or_line_unsafe_credentials() {
        assert!(github_repository_path("https://github.com/Block/buzz").is_err());
        assert!(parse_filled_credential(
            b"protocol=https\nhost=github.com\npath=block/buzz\nusername=user\npassword=bad\rvalue\n",
            "block/buzz",
        )
        .is_err());
        assert!(parse_filled_credential(
            b"protocol=https\nhost=example.com\npath=block/buzz\nusername=user\npassword=token\n",
            "block/buzz",
        )
        .is_err());
    }
}

#[cfg(all(test, not(unix)))]
mod unsupported_platform_tests {
    use super::*;

    #[tokio::test]
    async fn checkout_inspection_fails_closed_without_descendant_containment() {
        let error = match inspect_checkout_git(Path::new("."), &["rev-parse", "HEAD"]).await {
            Err(error) => error,
            Ok(_) => panic!("unsupported platform must fail closed"),
        };
        assert_eq!(
            error,
            "local A2A checkout inspection is unavailable on this platform: descendant-safe Git inspection is implemented only on Unix"
        );
    }
}
