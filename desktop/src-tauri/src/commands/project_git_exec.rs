//! Shared git subprocess plumbing for the project commands.
//!
//! Runs the system `git` with an ephemeral, env-only auth configuration:
//! the identity nsec is handed to `git-credential-nostr` via environment
//! variables so nothing key-related ever touches disk or global git config.

use crate::app_state::AppState;
use crate::managed_agents::bounded_command::{
    output_with_limits, BoundedCommandError, BoundedCommandLimits,
};
use crate::managed_agents::resolve_command;
use nostr::{Keys, ToBech32};
use std::process::Command;
use std::time::{Duration, Instant};
use url::Url;

/// Wall-clock cap for a single git invocation. Remote operations talk to
/// relay-supplied clone URLs, so a slow or adversarial remote must not pin
/// `spawn_blocking` threads indefinitely.
const LOCAL_GIT_TIMEOUT: Duration = Duration::from_secs(60);
const REMOTE_GIT_TIMEOUT: Duration = Duration::from_secs(300);

/// Maximum bytes retained across stdout and stderr for one git process.
///
/// Repository metadata can be large, but it must remain bounded: a public
/// remote controls ref names, paths, commit messages, and failure output. The
/// drain threads enforce this aggregate ceiling while the process is running,
/// then the poll loop kills the process and returns an error on overflow.
const GIT_CAPTURE_LIMIT_BYTES: u64 = 8 * 1024 * 1024;
const GIT_REQUEST_COMMAND_LIMIT: usize = 80;
const GIT_REQUEST_STORAGE_LIMIT_BYTES: u64 = 64 * 1024 * 1024;

/// Shared ceilings for one user-facing Git request. Remote repository browsing
/// can require several subprocesses, so per-process bounds alone are not
/// sufficient: every command consumes the same deadline, output allowance,
/// command count, and (for disposable clones) temporary-storage quota.
pub(crate) struct GitRequestBudget {
    deadline: Instant,
    remaining_commands: usize,
    remaining_capture_bytes: u64,
    storage_root: Option<std::path::PathBuf>,
    storage_limit_bytes: u64,
}

impl GitRequestBudget {
    pub(crate) fn remote(storage_root: &std::path::Path) -> Self {
        Self {
            deadline: Instant::now() + REMOTE_GIT_TIMEOUT,
            remaining_commands: GIT_REQUEST_COMMAND_LIMIT,
            remaining_capture_bytes: GIT_CAPTURE_LIMIT_BYTES,
            storage_root: Some(storage_root.to_path_buf()),
            storage_limit_bytes: GIT_REQUEST_STORAGE_LIMIT_BYTES,
        }
    }

    pub(crate) fn local() -> Self {
        Self {
            deadline: Instant::now() + LOCAL_GIT_TIMEOUT,
            remaining_commands: GIT_REQUEST_COMMAND_LIMIT,
            remaining_capture_bytes: GIT_CAPTURE_LIMIT_BYTES,
            storage_root: None,
            storage_limit_bytes: 0,
        }
    }

    fn next_limits(&mut self) -> Result<(Duration, u64), String> {
        if self.remaining_commands == 0 {
            return Err("git request exceeded its subprocess limit".to_string());
        }
        self.remaining_commands -= 1;
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("git request exceeded its wall-clock deadline".to_string());
        }
        if self.remaining_capture_bytes == 0 {
            return Err("git request exceeded its output byte limit".to_string());
        }
        Ok((remaining, self.remaining_capture_bytes))
    }

    fn charge_output(&mut self, bytes: u64) -> Result<(), String> {
        self.remaining_capture_bytes = self
            .remaining_capture_bytes
            .checked_sub(bytes)
            .ok_or_else(|| "git request exceeded its output byte limit".to_string())?;
        Ok(())
    }
}

fn git_subcommand<'a>(args: &'a [&str]) -> Option<&'a str> {
    let mut index = 0;
    while let Some(argument) = args.get(index).copied() {
        match argument {
            "-c" | "--config" | "-C" | "--git-dir" | "--work-tree" => index += 2,
            "--no-pager" | "--paginate" | "--end-of-options" => index += 1,
            argument
                if argument.starts_with("--config=")
                    || argument.starts_with("--git-dir=")
                    || argument.starts_with("--work-tree=") =>
            {
                index += 1;
            }
            argument if argument.starts_with('-') => index += 1,
            subcommand => return Some(subcommand),
        }
    }
    None
}

fn git_needs_credentials(args: &[&str]) -> bool {
    matches!(
        git_subcommand(args),
        Some("clone" | "fetch" | "push" | "pull" | "ls-remote" | "merge")
    )
}

pub(crate) struct GitAuthConfig {
    git_path: std::path::PathBuf,
    credential_helper: Option<std::path::PathBuf>,
    nsec: String,
    allow_file_transport: bool,
}

pub(crate) fn run_git(
    args: &[&str],
    cwd: Option<&std::path::Path>,
    auth: &GitAuthConfig,
) -> Result<String, String> {
    run_git_with_limits(args, cwd, auth, None)
}

pub(crate) fn run_git_in_request(
    args: &[&str],
    cwd: Option<&std::path::Path>,
    auth: &GitAuthConfig,
    budget: &mut GitRequestBudget,
) -> Result<String, String> {
    run_git_with_limits(args, cwd, auth, Some(budget))
}

fn run_git_with_limits(
    args: &[&str],
    cwd: Option<&std::path::Path>,
    auth: &GitAuthConfig,
    mut budget: Option<&mut GitRequestBudget>,
) -> Result<String, String> {
    let mut command = Command::new(&auth.git_path);
    command.args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let needs_credentials = git_needs_credentials(args);
    let default_timeout = if needs_credentials {
        REMOTE_GIT_TIMEOUT
    } else {
        LOCAL_GIT_TIMEOUT
    };
    configure_git_auth(&mut command, auth, needs_credentials);
    crate::util::configure_no_window(&mut command);
    let (timeout, capture_limit, storage_root, storage_limit) = match budget.as_deref_mut() {
        Some(budget) => {
            let (timeout, capture_limit) = budget.next_limits()?;
            (
                timeout,
                capture_limit,
                budget.storage_root.as_deref(),
                budget.storage_limit_bytes,
            )
        }
        None => (default_timeout, GIT_CAPTURE_LIMIT_BYTES, None, 0),
    };
    let output = output_with_limits(
        command,
        BoundedCommandLimits {
            timeout,
            capture_limit,
            storage_root: storage_root.map(|root| (root, storage_limit)),
        },
    )
    .map_err(|error| match error {
        BoundedCommandError::Spawn => "failed to run git".to_string(),
        BoundedCommandError::Setup => "failed to establish bounded git process".to_string(),
        BoundedCommandError::Timeout => {
            format!("git timed out after {}s", timeout.as_secs())
        }
        BoundedCommandError::OutputLimit(limit) => {
            format!("git output exceeded the {limit} byte capture limit")
        }
        BoundedCommandError::StorageLimit(limit) => {
            format!("git temporary data exceeded the {limit} byte storage limit")
        }
        BoundedCommandError::Wait => "failed to wait for git".to_string(),
        BoundedCommandError::Read => "failed to read bounded git output".to_string(),
    })?;
    let status = output.status;
    let stdout = output.stdout;
    let stderr = output.stderr;
    if let Some(budget) = budget {
        budget.charge_output((stdout.len() + stderr.len()) as u64)?;
    }
    let stdout = String::from_utf8_lossy(&stdout).to_string();
    let stderr = String::from_utf8_lossy(&stderr).to_string();
    if !status.success() {
        let stderr = stderr.trim().to_string();
        return Err(if stderr.is_empty() {
            format!("git exited with status {status}")
        } else {
            stderr
        });
    }
    Ok(stdout)
}

fn configure_git_auth(command: &mut Command, auth: &GitAuthConfig, needs_credentials: bool) {
    // `GIT_CONFIG_KEY_*` and `GIT_CONFIG_VALUE_*` are an indexed injection
    // surface. Remove inherited entries before installing the exact ephemeral
    // configuration below. `GIT_CONFIG_COUNT` is overwritten as well, but
    // removing the indexed values keeps them unavailable to any child helper.
    for (key, _) in std::env::vars_os() {
        let key_text = key.to_string_lossy();
        if key_text.starts_with("GIT_CONFIG_KEY_")
            || key_text.starts_with("GIT_CONFIG_VALUE_")
        {
            command.env_remove(key);
        }
    }
    for key in [
        "GIT_CONFIG",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_SYSTEM",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_NOSYSTEM",
        "GIT_EXEC_PATH",
        "GIT_ALLOW_PROTOCOL",
        "GIT_PROTOCOL_FROM_USER",
        "GIT_DIR",
        "GIT_COMMON_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_ASKPASS",
        "SSH_ASKPASS",
        "SSH_ASKPASS_REQUIRE",
        "GIT_SSH",
        "GIT_SSH_COMMAND",
        "GIT_PROXY_COMMAND",
        "GIT_EXTERNAL_DIFF",
        "GIT_CURL_VERBOSE",
        "GIT_TRACE",
        "GIT_TRACE_CURL",
        "GIT_TRACE_CURL_NO_DATA",
        "GIT_TRACE_PACKET",
        "GIT_TRACE_PERFORMANCE",
        "GIT_TRACE_SETUP",
        "GIT_TRACE_SHALLOW",
        "GIT_TRACE2",
        "GIT_TRACE2_EVENT",
        "GIT_TRACE2_PERF",
        "NOSTR_PRIVATE_KEY",
        "BUZZ_PRIVATE_KEY",
        "BUZZ_AUTH_TAG",
    ] {
        command.env_remove(key);
    }
    command.env("GIT_TERMINAL_PROMPT", "0");
    command.env("GIT_CONFIG_NOSYSTEM", "1");
    command.env("GIT_PROTOCOL_FROM_USER", "0");
    command.env("GIT_ATTR_NOSYSTEM", "1");
    command.env("GIT_NO_LAZY_FETCH", "1");
    // Git for Windows maps `/dev/null` to `NUL` internally, so this value
    // disables the global config file on every platform.
    command.env("GIT_CONFIG_GLOBAL", "/dev/null");

    // Base entries: disable any inherited credential helper, and neutralize
    // repo-local hooks — every process git spawns inherits our environment
    // (including NOSTR_PRIVATE_KEY below), and a cloned repository's hooks
    // must never run with the identity key in reach.
    let mut entries: Vec<(&str, String)> = vec![
        ("credential.helper", String::new()),
        ("core.askPass", String::new()),
        ("core.hooksPath", "/dev/null".to_string()),
        ("core.fsmonitor", "false".to_string()),
        ("http.sslVerify", "true".to_string()),
        ("http.extraHeader", String::new()),
        ("protocol.allow", "never".to_string()),
        ("protocol.http.allow", "always".to_string()),
        ("protocol.https.allow", "always".to_string()),
        ("protocol.ext.allow", "never".to_string()),
        (
            "protocol.file.allow",
            if auth.allow_file_transport {
                "always"
            } else {
                "never"
            }
            .to_string(),
        ),
    ];
    if needs_credentials {
        let Some(cred_helper) = &auth.credential_helper else {
            return apply_git_config(command, &entries);
        };
        command.env("NOSTR_PRIVATE_KEY", &auth.nsec);
        entries.push((
            "credential.helper",
            credential_helper_config_value(cred_helper),
        ));
        entries.push(("credential.useHttpPath", "true".to_string()));
    }
    apply_git_config(command, &entries);
}

/// Fail closed on repository/worktree configuration that can redirect,
/// intercept, authenticate, or weaken a network operation. Includes are
/// expanded and entries are filtered by Git's reported scope, so values hidden
/// in a local include or worktree config receive the same treatment as values
/// written directly in `.git/config`.
pub(crate) fn ensure_safe_local_network_config(
    repo_dir: &std::path::Path,
    auth: &GitAuthConfig,
) -> Result<(), String> {
    let output = run_git(
        &[
            "config",
            "--includes",
            "--null",
            "--list",
            "--show-origin",
            "--show-scope",
        ],
        Some(repo_dir),
        auth,
    )
    .map_err(|error| format!("validate local checkout config: {error}"))?;
    let fields = output.split('\0').filter(|field| !field.is_empty()).collect::<Vec<_>>();
    if fields.len() % 3 != 0 {
        return Err("Local checkout config metadata was malformed.".to_string());
    }
    for entry in fields.chunks_exact(3) {
        let scope = entry[0];
        if !matches!(scope, "local" | "worktree") {
            continue;
        }
        let key = entry[2]
            .split_once('\n')
            .map(|(key, _)| key)
            .unwrap_or(entry[2])
            .to_ascii_lowercase();
        let expected_origin_key = matches!(
            key.as_str(),
            "remote.origin.url"
                | "remote.origin.fetch"
                | "remote.origin.promisor"
                | "remote.origin.partialclonefilter"
        );
        let dangerous = key == "http"
            || key.starts_with("http.")
            || key == "credential"
            || key.starts_with("credential.")
            || key == "url"
            || key.starts_with("url.")
            || key == "core.gitproxy"
            || (key.starts_with("remote.origin.") && !expected_origin_key);
        if dangerous {
            return Err(format!(
                "Local checkout configures disallowed network setting {key}; remove it before fetching."
            ));
        }
    }
    Ok(())
}

/// Format a path for git `credential.helper`.
///
/// Git for Windows invokes helpers via MinGW bash, which treats `\` as
/// escapes. Forward slashes work on every platform git supports.
fn credential_helper_config_value(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn apply_git_config(command: &mut Command, entries: &[(&str, String)]) {
    command.env("GIT_CONFIG_COUNT", entries.len().to_string());
    for (index, (key, value)) in entries.iter().enumerate() {
        command.env(format!("GIT_CONFIG_KEY_{index}"), key);
        command.env(format!("GIT_CONFIG_VALUE_{index}"), value);
    }
}

pub(crate) fn build_git_auth_config(state: &AppState) -> Result<GitAuthConfig, String> {
    let keys = state.signing_keys()?;
    build_git_auth_config_for_keys(&keys)
}

pub(crate) fn build_git_clone_auth_config(
    clone_url: &str,
    state: &AppState,
) -> Result<GitAuthConfig, String> {
    if validate_github_clone_url(clone_url).is_ok() {
        return build_anonymous_git_auth_config();
    }
    build_git_auth_config(state)
}

fn build_anonymous_git_auth_config() -> Result<GitAuthConfig, String> {
    Ok(GitAuthConfig {
        git_path: resolve_command("git").ok_or_else(|| "git was not found on PATH".to_string())?,
        credential_helper: None,
        nsec: String::new(),
        allow_file_transport: false,
    })
}

pub(crate) fn build_git_auth_config_for_keys(keys: &Keys) -> Result<GitAuthConfig, String> {
    let git_path = resolve_command("git").ok_or_else(|| "git was not found on PATH".to_string())?;
    let credential_helper = resolve_command("git-credential-nostr");
    let nsec = keys
        .secret_key()
        .to_bech32()
        .map_err(|error| format!("encode identity key: {error}"))?;
    Ok(GitAuthConfig {
        git_path,
        credential_helper,
        nsec,
        allow_file_transport: false,
    })
}

#[cfg(test)]
pub(crate) fn build_test_git_auth_config() -> Result<GitAuthConfig, String> {
    let mut auth = build_git_auth_config_for_keys(&Keys::generate())?;
    auth.allow_file_transport = true;
    Ok(auth)
}

/// Normalizes and validates a relay-supplied branch name. Strips a
/// `refs/heads/` prefix, then rejects anything outside a conservative
/// character allowlist, path traversal (`..`), leading/trailing `/`, and
/// flag-shaped values (leading `-`) so a branch can never reach git as an
/// option instead of a positional argument.
pub(crate) fn clean_branch(value: Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.trim_start_matches("refs/heads/"))
        .filter(|value| {
            !value.is_empty()
                && !value.starts_with('-')
                && !value.contains("..")
                && !value.starts_with('/')
                && !value.ends_with('/')
                && value
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '_' | '.' | '-'))
        })
        .map(ToString::to_string)
}

pub(crate) fn clean_target_ref(value: Option<String>) -> Option<String> {
    let value = value?.trim().to_string();
    for prefix in ["refs/tags/", "refs/nostr/"] {
        if let Some(name) = value.strip_prefix(prefix) {
            let clean_name = clean_branch(Some(name.to_string()))?;
            return (clean_name == name).then_some(format!("{prefix}{clean_name}"));
        }
    }
    None
}

pub(crate) fn validate_clone_url(clone_url: &str) -> Result<(), String> {
    let parsed = Url::parse(clone_url).map_err(|error| format!("invalid clone URL: {error}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err("clone URL must be http or https".into());
    }
    // Buzz git remotes are served at `…/git/<owner-pubkey>/<repo-id>` — a
    // literal `git` segment followed by the 64-hex owner pubkey and a
    // non-empty repository id (the relay may live under a path prefix).
    let segments = parsed
        .path_segments()
        .map(|segments| segments.filter(|s| !s.is_empty()).collect::<Vec<_>>())
        .unwrap_or_default();
    let is_buzz_repo_path = segments
        .iter()
        .rposition(|segment| *segment == "git")
        .filter(|index| segments.len() == index + 3)
        .map(|index| {
            segments[index + 1].len() == 64
                && segments[index + 1].chars().all(|c| c.is_ascii_hexdigit())
                && !segments[index + 2].is_empty()
        })
        .unwrap_or(false);
    if !is_buzz_repo_path {
        return Err("clone URL must point at a Buzz git repository".into());
    }
    Ok(())
}

fn validate_github_clone_url(clone_url: &str) -> Result<(), String> {
    let parsed = Url::parse(clone_url).map_err(|error| format!("invalid clone URL: {error}"))?;
    if parsed.scheme() != "https"
        || parsed.host_str() != Some("github.com")
        || parsed.port().is_some()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err("GitHub clone URL must use public https://github.com/owner/repository".into());
    }
    let segments = parsed
        .path_segments()
        .map(|segments| {
            segments
                .filter(|segment| !segment.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let valid_segment = |segment: &&str| {
        !segment.starts_with('-')
            && !segment.contains("..")
            && segment.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
            })
    };
    if segments.len() != 2 || !segments.iter().all(valid_segment) {
        return Err("GitHub clone URL must name one owner and repository".into());
    }
    Ok(())
}

pub(crate) fn validate_local_clone_url(clone_url: &str) -> Result<(), String> {
    if validate_clone_url(clone_url).is_ok() || validate_github_clone_url(clone_url).is_ok() {
        return Ok(());
    }
    Err("clone URL must point at a Buzz repository or public GitHub repository".into())
}

pub(crate) fn validate_local_clone_url_for_workspace(
    clone_url: &str,
    state: &AppState,
) -> Result<(), String> {
    let relay_base = crate::relay::relay_api_base_url_with_override(state);
    validate_local_clone_url_against_relay(clone_url, &relay_base)
}

fn validate_local_clone_url_against_relay(clone_url: &str, relay_base: &str) -> Result<(), String> {
    if validate_github_clone_url(clone_url).is_ok() {
        return Ok(());
    }
    validate_clone_url_against_relay(clone_url, relay_base)
}

pub(crate) fn clone_url_owner(clone_url: &str) -> Option<String> {
    let parsed = Url::parse(clone_url).ok()?;
    let segments = parsed
        .path_segments()?
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let index = segments.iter().rposition(|segment| *segment == "git")?;
    (segments.len() == index + 3).then(|| segments[index + 1].to_ascii_lowercase())
}

pub(crate) fn validate_workspace_clone_url(
    clone_url: &str,
    state: &AppState,
) -> Result<(), String> {
    let relay_base = crate::relay::relay_api_base_url_with_override(state);
    validate_clone_url_against_relay(clone_url, &relay_base)
}

fn validate_clone_url_against_relay(clone_url: &str, relay_base: &str) -> Result<(), String> {
    validate_clone_url(clone_url)?;
    let clone = Url::parse(clone_url).map_err(|error| format!("invalid clone URL: {error}"))?;
    let relay = Url::parse(relay_base)
        .map_err(|error| format!("configured relay URL is invalid: {error}"))?;
    if clone.scheme() != relay.scheme()
        || clone.host_str() != relay.host_str()
        || clone.port_or_known_default() != relay.port_or_known_default()
    {
        return Err("clone URL must use the active workspace relay".into());
    }
    let relay_path = relay.path().trim_end_matches('/');
    if !relay_path.is_empty() && !clone.path().starts_with(&format!("{relay_path}/")) {
        return Err("clone URL must use the active workspace relay path".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        build_anonymous_git_auth_config, clean_branch, clean_target_ref, configure_git_auth,
        credential_helper_config_value, git_needs_credentials, git_subcommand, run_git,
        validate_clone_url, validate_clone_url_against_relay, validate_local_clone_url,
        validate_local_clone_url_against_relay, GIT_CAPTURE_LIMIT_BYTES,
    };

    #[test]
    fn credential_helper_config_value_uses_forward_slashes() {
        let path =
            std::path::PathBuf::from(r"C:\Users\x\AppData\Local\Buzz\git-credential-nostr.exe");
        assert_eq!(
            credential_helper_config_value(&path),
            "C:/Users/x/AppData/Local/Buzz/git-credential-nostr.exe",
        );
    }

    #[test]
    fn git_subcommand_skips_global_config_options() {
        assert_eq!(
            git_subcommand(&[
                "-c",
                "user.name=Buzz User",
                "-c",
                "user.email=user@example.com",
                "merge",
                "HEAD",
            ]),
            Some("merge")
        );
        assert_eq!(
            git_subcommand(&["--config=credential.useHttpPath=true", "fetch", "origin"]),
            Some("fetch")
        );
    }

    #[test]
    fn remote_and_promisor_operations_receive_credentials() {
        assert!(git_needs_credentials(&["fetch", "origin"]));
        assert!(git_needs_credentials(&[
            "-c",
            "user.name=Buzz User",
            "merge",
            "HEAD"
        ]));
        assert!(!git_needs_credentials(&["rev-parse", "HEAD"]));
    }

    #[test]
    fn clean_branch_accepts_plain_and_prefixed_names() {
        assert_eq!(
            clean_branch(Some("refs/heads/feature/x-1".into())),
            Some("feature/x-1".to_string())
        );
        assert_eq!(
            clean_branch(Some(" main ".into())),
            Some("main".to_string())
        );
    }

    #[test]
    fn clean_branch_rejects_flag_shaped_and_traversal_values() {
        assert_eq!(clean_branch(Some("--upload-pack=/tmp/evil".into())), None);
        assert_eq!(clean_branch(Some("-x".into())), None);
        assert_eq!(clean_branch(Some("a/../b".into())), None);
        assert_eq!(clean_branch(Some("/leading".into())), None);
        assert_eq!(clean_branch(Some("trailing/".into())), None);
        assert_eq!(clean_branch(Some("bad name".into())), None);
        assert_eq!(clean_branch(None), None);
    }

    #[test]
    fn clean_target_ref_accepts_only_tags_and_pull_request_refs() {
        assert_eq!(
            clean_target_ref(Some("refs/tags/v1.0.0".into())),
            Some("refs/tags/v1.0.0".to_string())
        );
        assert_eq!(
            clean_target_ref(Some("refs/nostr/abc123".into())),
            Some("refs/nostr/abc123".to_string())
        );
        assert_eq!(clean_target_ref(Some("refs/heads/main".into())), None);
        assert_eq!(clean_target_ref(Some("refs/tags/../main".into())), None);
    }

    #[test]
    fn validate_clone_url_requires_buzz_repo_shape() {
        let owner = "a".repeat(64);
        assert!(validate_clone_url(&format!("https://relay.example/git/{owner}/repo")).is_ok());
        assert!(
            validate_clone_url(&format!("https://relay.example/prefix/git/{owner}/repo")).is_ok()
        );
        assert!(validate_clone_url("https://relay.example/git/short/repo").is_err());
        assert!(validate_clone_url("https://evil.example/has/git/inpath").is_err());
        assert!(validate_clone_url(&format!("ssh://relay.example/git/{owner}/repo")).is_err());
        assert!(validate_clone_url(&format!(
            "https://relay.example/git/{owner}/repo/unexpected"
        ))
        .is_err());
    }

    #[test]
    fn workspace_clone_url_requires_exact_relay_origin_and_prefix() {
        let owner = "a".repeat(64);
        let valid = format!("https://relay.example/prefix/git/{owner}/repo");
        assert!(validate_clone_url_against_relay(&valid, "https://relay.example/prefix").is_ok());
        assert!(validate_clone_url_against_relay(&valid, "http://relay.example/prefix").is_err());
        assert!(
            validate_clone_url_against_relay(&valid, "https://relay.example:8443/prefix").is_err()
        );
        assert!(validate_clone_url_against_relay(&valid, "https://relay.example/other").is_err());
        assert!(validate_clone_url_against_relay(
            &format!("https://evil.example/prefix/git/{owner}/repo"),
            "https://relay.example/prefix",
        )
        .is_err());
        assert!(validate_clone_url_against_relay(
            "https://github.com/mysteropodes/nemo.git",
            "https://relay.example/prefix",
        )
        .is_err());
    }

    #[test]
    fn local_clone_url_allows_only_public_github_https_urls() {
        assert!(validate_local_clone_url("https://github.com/block/buzz").is_ok());
        assert!(validate_local_clone_url("https://github.com/block/buzz.git").is_ok());
        assert!(validate_local_clone_url("http://github.com/block/buzz").is_err());
        assert!(validate_local_clone_url("https://github.com/block/buzz/issues").is_err());
        assert!(validate_local_clone_url("https://user@github.com/block/buzz").is_err());
        assert!(validate_local_clone_url("https://github.com.evil.test/block/buzz").is_err());
        assert!(validate_local_clone_url("https://gitlab.com/block/buzz").is_err());
    }

    #[test]
    fn local_workspace_reads_accept_safe_github_without_weakening_relay_scoping() {
        let owner = "a".repeat(64);
        let active_relay = "https://relay.example/community";
        assert!(validate_local_clone_url_against_relay(
            "https://github.com/mysteropodes/nemo.git",
            active_relay,
        )
        .is_ok());
        assert!(validate_local_clone_url_against_relay(
            &format!("{active_relay}/git/{owner}/nemo"),
            active_relay,
        )
        .is_ok());
        assert!(validate_local_clone_url_against_relay(
            &format!("https://other.example/community/git/{owner}/nemo"),
            active_relay,
        )
        .is_err());
    }

    #[test]
    fn public_github_reads_use_anonymous_git_auth() {
        let auth = build_anonymous_git_auth_config().expect("build anonymous git config");
        assert!(auth.credential_helper.is_none());
        assert!(auth.nsec.is_empty());
        assert!(!auth.allow_file_transport);
    }

    #[cfg(unix)]
    #[test]
    fn anonymous_git_subprocess_scrubs_inherited_prompt_and_secret_environment() {
        let auth = build_anonymous_git_auth_config().expect("build anonymous git config");
        let mut command = std::process::Command::new("env");
        for key in [
            "GIT_CONFIG",
            "GIT_CONFIG_PARAMETERS",
            "GIT_EXEC_PATH",
            "GIT_ALLOW_PROTOCOL",
            "GIT_ASKPASS",
            "SSH_ASKPASS",
            "SSH_ASKPASS_REQUIRE",
            "GIT_SSH",
            "GIT_SSH_COMMAND",
            "GIT_PROXY_COMMAND",
            "GIT_TRACE_CURL",
            "NOSTR_PRIVATE_KEY",
            "BUZZ_PRIVATE_KEY",
            "BUZZ_AUTH_TAG",
        ] {
            command.env(key, "must-not-reach-child");
        }
        configure_git_auth(&mut command, &auth, true);
        let output = command.output().expect("run child environment probe");
        assert!(output.status.success());
        let environment = String::from_utf8(output.stdout).expect("environment is utf-8");
        for key in [
            "GIT_CONFIG",
            "GIT_CONFIG_PARAMETERS",
            "GIT_EXEC_PATH",
            "GIT_ALLOW_PROTOCOL",
            "GIT_ASKPASS",
            "SSH_ASKPASS",
            "SSH_ASKPASS_REQUIRE",
            "GIT_SSH",
            "GIT_SSH_COMMAND",
            "GIT_PROXY_COMMAND",
            "GIT_TRACE_CURL",
            "NOSTR_PRIVATE_KEY",
            "BUZZ_PRIVATE_KEY",
            "BUZZ_AUTH_TAG",
        ] {
            assert!(
                !environment
                    .lines()
                    .any(|line| line.starts_with(&format!("{key}="))),
                "{key} leaked into child environment"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn inherited_git_config_cannot_activate_a_custom_remote_helper() {
        use std::os::unix::fs::PermissionsExt;

        let mut auth = build_anonymous_git_auth_config().expect("build anonymous git config");
        auth.allow_file_transport = true;
        let root = tempfile::tempdir().expect("create temporary directory");
        let remote = root.path().join("remote.git");
        let checkout = root.path().join("checkout");
        let helper = root.path().join("git-remote-evil");
        let marker = root.path().join("helper-ran");
        let remote_path = remote.to_str().expect("remote path is utf-8");
        let checkout_path = checkout.to_str().expect("checkout path is utf-8");

        run_git(&["init", "--bare", "--", remote_path], None, &auth)
            .expect("initialize remote");
        run_git(&["init", "--", checkout_path], None, &auth).expect("initialize checkout");
        run_git(
            &["remote", "add", "origin", remote_path],
            Some(&checkout),
            &auth,
        )
        .expect("configure origin");
        std::fs::write(
            &helper,
            "#!/bin/sh\nprintf invoked > \"$BUZZ_GIT_HELPER_MARKER\"\nexit 1\n",
        )
        .expect("write custom helper");
        let mut permissions = std::fs::metadata(&helper)
            .expect("read helper metadata")
            .permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&helper, permissions).expect("make helper executable");

        let inherited_path = std::env::var_os("PATH").unwrap_or_default();
        let helper_path = std::env::join_paths(
            std::iter::once(root.path().to_path_buf())
                .chain(std::env::split_paths(&inherited_path)),
        )
        .expect("compose helper PATH");
        let mut command = std::process::Command::new(&auth.git_path);
        command
            .args(["ls-remote", "origin"])
            .current_dir(&checkout)
            .env("PATH", helper_path)
            .env("GIT_CONFIG_PARAMETERS", "'remote.origin.vcs=evil'")
            .env("GIT_EXEC_PATH", root.path())
            .env("BUZZ_GIT_HELPER_MARKER", &marker);
        configure_git_auth(&mut command, &auth, true);
        let output = command.output().expect("run hardened git subprocess");

        assert!(
            output.status.success(),
            "safe origin should be read after inherited override removal: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !marker.exists(),
            "inherited Git config executed a custom remote helper"
        );
    }

    #[cfg(unix)]
    #[test]
    fn git_output_capture_fails_closed_above_the_aggregate_limit() {
        let mut auth = build_anonymous_git_auth_config().expect("build anonymous git config");
        auth.git_path = "/bin/sh".into();
        let script = format!("head -c {} /dev/zero", GIT_CAPTURE_LIMIT_BYTES + 8192);

        let error = run_git(&["-c", &script], None, &auth)
            .expect_err("over-limit git output must fail");

        assert!(error.contains("output exceeded"), "{error}");
        assert!(error.contains(&GIT_CAPTURE_LIMIT_BYTES.to_string()), "{error}");
    }
}
