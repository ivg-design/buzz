use std::collections::{BTreeSet, HashSet};
use std::path::{Component, Path, PathBuf};

use serde::Deserialize;

use super::git::inspect_checkout_git;

const MAX_GRANTS: usize = 128;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantDocument {
    version: u32,
    grants: Vec<JobGrant>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobGrant {
    project_address: String,
    home_channel: String,
    repository: String,
    requester_pubkeys: Vec<String>,
    capabilities: Vec<String>,
    /// Privileged Git mutations are opt-in independently from the job's
    /// semantic capability. Older grants deserialize to an empty deny-all set.
    #[serde(default)]
    git_operations: Vec<String>,
    path_prefixes: Vec<String>,
    base_sha: String,
    branch: String,
    worktree_id: String,
    checkout_root: PathBuf,
    /// Captured before any model process starts and never deserialized from
    /// the operator document. Shared worktrees therefore serialize on the
    /// same immutable common Git directory.
    #[serde(skip)]
    git_common_dir: PathBuf,
}

/// One exact local authorization selected for an outbound request.
#[derive(Clone)]
pub struct GrantMatch {
    pub project_address: String,
    pub home_channel: String,
    pub repository: String,
    pub base_sha: String,
    pub branch: String,
    pub worktree_id: String,
}

/// Exact, operator-granted checkout available to harness-owned Git tools.
///
/// This type is intentionally crate-private and has no serialization or
/// `Debug` implementation. Model-facing tools cannot supply or widen any of
/// these values.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct TrustedGitCheckout {
    pub(super) root: PathBuf,
    pub(super) git_common_dir: PathBuf,
    pub(super) repository: String,
    pub(super) base_sha: String,
    pub(super) head_sha: String,
    pub(super) branch: String,
    pub(super) path_prefixes: Vec<String>,
}

/// Validated local, operator-controlled collaboration grants.
///
/// No `Debug` implementation is provided so the allowlist cannot be dumped by
/// broad state diagnostics.
#[derive(Clone, Default)]
pub struct GrantSet {
    grants: Vec<JobGrant>,
}

impl GrantSet {
    pub fn load(
        _cwd: &Path,
        inline: Option<String>,
        file: Option<PathBuf>,
    ) -> Result<Self, String> {
        if inline.is_some() && file.is_some() {
            return Err(
                "set only one of BUZZ_ACP_JOB_GRANTS_JSON or BUZZ_ACP_JOB_GRANTS_FILE".into(),
            );
        }
        let raw = match (inline, file) {
            (Some(raw), None) if !raw.trim().is_empty() => Some(raw),
            (None, Some(path)) => Some(
                std::fs::read_to_string(path)
                    .map_err(|error| format!("reading local A2A grants: {error}"))?,
            ),
            // An absent explicit source is deny-all. In particular, never
            // discover a checkout-local grant document that a model can edit.
            (None, None) => None,
            _ => None,
        };
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        let mut document: GrantDocument = serde_json::from_str(&raw)
            .map_err(|error| format!("parsing local A2A grants: {error}"))?;
        if document.version != 1 || document.grants.len() > MAX_GRANTS {
            return Err(format!(
                "local A2A grants require version 1 and at most {MAX_GRANTS} entries"
            ));
        }
        for grant in &mut document.grants {
            validate_grant(grant)?;
        }
        Ok(Self {
            grants: document.grants,
        })
    }

    pub fn channels(&self) -> Vec<String> {
        self.grants
            .iter()
            .map(|grant| grant.home_channel.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Canonical repositories whose explicit grant requires an operator
    /// credential. Commit-only grants never trigger credential resolution.
    pub(super) fn credential_repositories(&self) -> Vec<String> {
        self.grants
            .iter()
            .filter(|grant| {
                grant
                    .git_operations
                    .iter()
                    .any(|operation| matches!(operation.as_str(), "fetch" | "push"))
            })
            .map(|grant| grant.repository.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub(super) fn git_lock_key(
        &self,
        session_working_directory: Option<&Path>,
    ) -> Result<PathBuf, String> {
        let root = session_working_directory
            .ok_or_else(|| "trusted Git requires a receiver-verified checkout".to_owned())?
            .canonicalize()
            .map_err(|_| "trusted Git checkout is unavailable".to_owned())?;
        let keys = self
            .grants
            .iter()
            .filter(|grant| grant.checkout_root == root)
            .map(|grant| grant.git_common_dir.clone())
            .collect::<BTreeSet<_>>();
        match keys.into_iter().collect::<Vec<_>>().as_slice() {
            [key] => Ok(key.clone()),
            [] => Err("trusted Git checkout has no local grant".into()),
            _ => Err("trusted Git checkout has conflicting common directories".into()),
        }
    }

    pub async fn outbound(
        &self,
        recipient: &str,
        capability: &str,
        paths: &[String],
        worktree_id: &str,
    ) -> Result<GrantMatch, String> {
        let mut matches = Vec::new();
        for grant in &self.grants {
            let statically_allowed = grant.requester_pubkeys.iter().any(|peer| peer == recipient)
                && grant
                    .capabilities
                    .iter()
                    .any(|allowed| allowed == capability)
                && grant.worktree_id == worktree_id
                && paths
                    .iter()
                    .all(|path| path_allowed(path, &grant.path_prefixes));
            if statically_allowed {
                if let Ok(checkout) = inspect_checkout(grant).await {
                    matches.push((grant, checkout));
                }
            }
        }
        match matches.as_slice() {
            [(grant, checkout)] => Ok(GrantMatch {
                project_address: grant.project_address.clone(),
                home_channel: grant.home_channel.clone(),
                repository: grant.repository.clone(),
                base_sha: checkout.base_sha.clone(),
                branch: checkout.branch.clone(),
                worktree_id: grant.worktree_id.clone(),
            }),
            [] => Err("operation is outside the local A2A grant or checkout scope".into()),
            _ => Err("operation matches multiple local A2A grants; narrow the grant file".into()),
        }
    }

    pub fn allows_event(&self, job: &buzz_core::job::JobEvent, signer: &str) -> bool {
        let common = job.common();
        self.grants.iter().any(|grant| {
            grant.project_address == common.project.address
                && grant.home_channel == common.project.home_channel
                && grant.repository == common.repository.canonical
                && grant.base_sha == common.repository.base_sha
                && grant.branch == common.repository.branch
                && grant.worktree_id == common.repository.worktree_id
                && (common.sender_pubkey == signer || common.recipient_pubkey == signer)
                && [
                    common.sender_pubkey.as_str(),
                    common.recipient_pubkey.as_str(),
                ]
                .into_iter()
                .any(|peer| {
                    grant
                        .requester_pubkeys
                        .iter()
                        .any(|allowed| allowed == peer)
                })
                && common
                    .repository
                    .paths
                    .iter()
                    .all(|path| path_allowed(path, &grant.path_prefixes))
                && match job {
                    buzz_core::job::JobEvent::Request(request) => grant
                        .capabilities
                        .iter()
                        .any(|allowed| allowed == &request.capability),
                    _ => true,
                }
        })
    }

    pub fn allows_superseding_request(
        &self,
        project: &buzz_core::job::JobProject,
        repository: &buzz_core::job::JobRepository,
        recipient: &str,
        capability: &str,
    ) -> bool {
        self.grants.iter().any(|grant| {
            grant.project_address == project.address
                && grant.home_channel == project.home_channel
                && grant.repository == repository.canonical
                && grant.requester_pubkeys.iter().any(|peer| peer == recipient)
                && grant
                    .capabilities
                    .iter()
                    .any(|allowed| allowed == capability)
                && grant.base_sha == repository.base_sha
                && grant.branch == repository.branch
                && grant.worktree_id == repository.worktree_id
                && repository
                    .paths
                    .iter()
                    .all(|path| path_allowed(path, &grant.path_prefixes))
        })
    }

    /// Select the one checkout fixed to this job session's channel and
    /// receiver-verified working directory.
    ///
    /// Unlike outbound dispatch, Git operations may advance HEAD after a
    /// signed commit. The original grant SHA must remain an ancestor and every
    /// other checkout invariant remains exact.
    pub(super) async fn trusted_git_checkout(
        &self,
        session_channel_id: Option<&str>,
        session_working_directory: Option<&Path>,
        request: &buzz_core::job::JobRequest,
        operation: super::ProjectGitOperation,
    ) -> Result<TrustedGitCheckout, String> {
        let channel = session_channel_id
            .ok_or_else(|| "trusted Git requires a channel-bound session".to_owned())?;
        let working_directory = session_working_directory
            .ok_or_else(|| "trusted Git requires a receiver-verified checkout".to_owned())?
            .canonicalize()
            .map_err(|_| "trusted Git checkout is unavailable".to_owned())?;
        let common = &request.common;
        if common.project.home_channel != channel {
            return Err("trusted Git request does not match the session channel".into());
        }
        let candidate_grants = self.grants.iter().filter(|grant| {
            grant.home_channel == channel
                && grant.project_address == common.project.address
                && grant.repository == common.repository.canonical
                && grant.base_sha == common.repository.base_sha
                && grant.branch == common.repository.branch
                && grant.worktree_id == common.repository.worktree_id
                && common
                    .repository
                    .paths
                    .iter()
                    .all(|path| path_allowed(path, &grant.path_prefixes))
                && grant.checkout_root == working_directory
                && grant
                    .git_operations
                    .iter()
                    .any(|allowed| allowed == operation.as_str())
        });
        let mut matches = Vec::new();
        for grant in candidate_grants {
            if let Ok(checkout) =
                inspect_trusted_git_checkout(grant, &common.repository.paths).await
            {
                matches.push(checkout);
            }
        }
        matches.sort_by(|left, right| {
            (
                &left.repository,
                &left.base_sha,
                &left.branch,
                &left.path_prefixes,
            )
                .cmp(&(
                    &right.repository,
                    &right.base_sha,
                    &right.branch,
                    &right.path_prefixes,
                ))
        });
        matches.dedup();
        match matches.as_slice() {
            [checkout] => Ok(checkout.clone()),
            [] => Err("session checkout is outside the local Project grant".into()),
            _ => Err("session checkout matches conflicting local Project grants".into()),
        }
    }
}

struct Checkout {
    base_sha: String,
    branch: String,
}

async fn inspect_checkout(grant: &JobGrant) -> Result<Checkout, String> {
    let cwd = grant
        .checkout_root
        .canonicalize()
        .map_err(|_| "local A2A checkout is unavailable".to_owned())?;
    let top = PathBuf::from(git(&cwd, &["rev-parse", "--show-toplevel"]).await?);
    if top.canonicalize().ok().as_ref() != Some(&cwd) {
        return Err("local A2A checkout root no longer matches its grant".into());
    }
    let base_sha = git(&cwd, &["rev-parse", "HEAD"]).await?;
    let branch = git(&cwd, &["symbolic-ref", "--quiet", "--short", "HEAD"]).await?;
    let remote = git(
        &cwd,
        &[
            "config",
            "--local",
            "--no-includes",
            "--get",
            "remote.origin.url",
        ],
    )
    .await?;
    let repository = canonical_github_remote(&remote)?;
    let git_common_dir = PathBuf::from(
        git(
            &cwd,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )
        .await?,
    )
    .canonicalize()
    .map_err(|_| "local A2A Git directory is unavailable".to_owned())?;
    if repository != grant.repository
        || base_sha != grant.base_sha
        || branch != grant.branch
        || git_common_dir != grant.git_common_dir
    {
        return Err("local A2A checkout no longer matches its exact grant".into());
    }
    Ok(Checkout { base_sha, branch })
}

async fn inspect_trusted_git_checkout(
    grant: &JobGrant,
    request_paths: &[String],
) -> Result<TrustedGitCheckout, String> {
    let root = grant
        .checkout_root
        .canonicalize()
        .map_err(|_| "local Project checkout is unavailable".to_owned())?;
    let top = PathBuf::from(git(&root, &["rev-parse", "--show-toplevel"]).await?);
    if top.canonicalize().ok().as_ref() != Some(&root) {
        return Err("local Project checkout root no longer matches its grant".into());
    }
    let head_sha = git(&root, &["rev-parse", "HEAD"]).await?;
    let branch = git(&root, &["symbolic-ref", "--quiet", "--short", "HEAD"]).await?;
    let remote = git(
        &root,
        &[
            "config",
            "--local",
            "--no-includes",
            "--get",
            "remote.origin.url",
        ],
    )
    .await?;
    let repository = canonical_github_remote(&remote)?;
    let git_common_dir = PathBuf::from(
        git(
            &root,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )
        .await?,
    )
    .canonicalize()
    .map_err(|_| "local Project Git directory is unavailable".to_owned())?;
    if repository != grant.repository
        || branch != grant.branch
        || git_common_dir != grant.git_common_dir
    {
        return Err("local Project checkout no longer matches its grant".into());
    }
    if !git_status(
        &root,
        &["merge-base", "--is-ancestor", &grant.base_sha, &head_sha],
    )
    .await?
    {
        return Err("local Project checkout no longer descends from its granted base".into());
    }
    Ok(TrustedGitCheckout {
        root,
        git_common_dir,
        repository: grant.repository.clone(),
        base_sha: grant.base_sha.clone(),
        head_sha,
        branch,
        // The accepted signed request may be narrower than the operator's
        // reusable grant. Git operations use the request's exact path scope.
        path_prefixes: request_paths.to_vec(),
    })
}

async fn git(cwd: &Path, args: &[&str]) -> Result<String, String> {
    let output = inspect_checkout_git(cwd, args).await?;
    if !output.status.success() || !output.stderr.is_empty() {
        return Err("local checkout inspection failed".into());
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|_| "git output was not UTF-8".into())
}

async fn git_status(cwd: &Path, args: &[&str]) -> Result<bool, String> {
    let output = inspect_checkout_git(cwd, args).await?;
    if !output.stderr.is_empty() {
        return Err("local checkout ancestry inspection failed".into());
    }
    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err("local checkout ancestry inspection failed".into()),
    }
}

fn canonical_github_remote(value: &str) -> Result<String, String> {
    let value = value.strip_suffix(".git").unwrap_or(value);
    let path = value
        .strip_prefix("https://github.com/")
        .or_else(|| value.strip_prefix("git@github.com:"))
        .ok_or_else(|| "origin must be a canonical GitHub HTTPS or SSH remote".to_owned())?;
    if path.split('/').count() != 2 || path.split('/').any(|part| !valid_github_part(part)) {
        return Err("origin must identify one GitHub owner/repository".into());
    }
    Ok(format!("https://github.com/{}", path.to_ascii_lowercase()))
}

fn valid_github_part(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

pub(super) fn path_allowed(path: &str, prefixes: &[String]) -> bool {
    valid_relative_path(path)
        && !prefixes.is_empty()
        && prefixes
            .iter()
            .any(|prefix| path == prefix || path.starts_with(&format!("{prefix}/")))
}

fn validate_grant(grant: &mut JobGrant) -> Result<(), String> {
    validate_project(&grant.project_address)?;
    validate_uuid("home_channel", &grant.home_channel)?;
    if canonical_github_remote(&grant.repository)? != grant.repository {
        return Err("repository must be canonical lowercase https://github.com/owner/repo".into());
    }
    if grant.requester_pubkeys.is_empty() || grant.capabilities.is_empty() {
        return Err("each grant requires peer pubkeys and capabilities".into());
    }
    unique_valid(&grant.requester_pubkeys, valid_pubkey, "requester_pubkeys")?;
    unique_valid(&grant.capabilities, valid_token, "capabilities")?;
    unique_valid(&grant.git_operations, valid_git_operation, "git_operations")?;
    if grant.path_prefixes.is_empty() {
        return Err("path_prefixes must contain at least one repository-relative path".into());
    }
    unique_valid(&grant.path_prefixes, valid_relative_path, "path_prefixes")?;
    if !valid_sha(&grant.base_sha) {
        return Err("base_sha must be 40 or 64 lowercase hexadecimal characters".into());
    }
    if !valid_branch(&grant.branch) || !valid_token(&grant.worktree_id) {
        return Err("branch and worktree_id must be canonical printable tokens".into());
    }
    if !grant.checkout_root.is_absolute() {
        return Err("checkout_root must be an absolute existing directory".into());
    }
    grant.checkout_root = grant
        .checkout_root
        .canonicalize()
        .map_err(|_| "checkout_root must be an absolute existing directory".to_owned())?;
    grant.git_common_dir = resolve_git_common_dir(&grant.checkout_root)?;
    Ok(())
}

/// Resolve the immutable Git storage identity without launching a process.
///
/// Grant loading is synchronous and occurs before any model process starts.
/// The checkout itself is revalidated through the bounded Git inspection seam
/// immediately before dispatch or a privileged Git operation.
fn resolve_git_common_dir(checkout_root: &Path) -> Result<PathBuf, String> {
    fn one_path(raw: &str, prefix: Option<&str>) -> Option<PathBuf> {
        let value = raw.trim();
        if value.is_empty() || value.as_bytes().contains(&0) || value.lines().count() != 1 {
            return None;
        }
        let value = match prefix {
            Some(prefix) => value.strip_prefix(prefix)?.trim(),
            None => value,
        };
        (!value.is_empty()).then(|| PathBuf::from(value))
    }

    let dot_git = checkout_root.join(".git");
    let git_dir = if dot_git.is_dir() {
        dot_git
            .canonicalize()
            .map_err(|_| "checkout_root must name an existing Git worktree".to_owned())?
    } else {
        let raw = std::fs::read_to_string(&dot_git)
            .map_err(|_| "checkout_root must name an existing Git worktree".to_owned())?;
        let path = one_path(&raw, Some("gitdir:"))
            .ok_or_else(|| "checkout_root has an invalid Git worktree pointer".to_owned())?;
        let path = if path.is_absolute() {
            path
        } else {
            checkout_root.join(path)
        };
        path.canonicalize()
            .map_err(|_| "checkout_root must name an existing Git worktree".to_owned())?
    };
    if !git_dir.is_dir() {
        return Err("checkout_root must name an existing Git worktree".into());
    }
    let common_path = git_dir.join("commondir");
    if !common_path.exists() {
        return Ok(git_dir);
    }
    let raw = std::fs::read_to_string(common_path)
        .map_err(|_| "checkout_root has an invalid Git common directory".to_owned())?;
    let path = one_path(&raw, None)
        .ok_or_else(|| "checkout_root has an invalid Git common directory".to_owned())?;
    let path = if path.is_absolute() {
        path
    } else {
        git_dir.join(path)
    };
    let common = path
        .canonicalize()
        .map_err(|_| "checkout_root has an invalid Git common directory".to_owned())?;
    if !common.is_dir() {
        return Err("checkout_root has an invalid Git common directory".into());
    }
    Ok(common)
}

fn unique_valid(values: &[String], valid: fn(&str) -> bool, name: &str) -> Result<(), String> {
    let mut seen = HashSet::new();
    if values.len() > 128
        || values
            .iter()
            .any(|value| !valid(value) || !seen.insert(value))
    {
        return Err(format!("{name} must be unique, bounded, and canonical"));
    }
    Ok(())
}

fn validate_project(value: &str) -> Result<(), String> {
    let mut parts = value.splitn(3, ':');
    if parts.next() != Some("30621")
        || parts.next().is_none_or(|owner| !valid_pubkey(owner))
        || parts.next().is_none_or(|slug| !valid_token(slug))
    {
        return Err("project_address must be a canonical 30621 coordinate".into());
    }
    Ok(())
}

fn validate_uuid(name: &str, value: &str) -> Result<(), String> {
    let parsed = uuid::Uuid::parse_str(value).map_err(|_| format!("{name} must be a UUID"))?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(format!("{name} must be a canonical non-nil UUID"));
    }
    Ok(())
}

fn valid_pubkey(value: &str) -> bool {
    nostr::PublicKey::parse(value).is_ok_and(|parsed| parsed.to_hex() == value)
}

fn valid_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.trim() == value
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn valid_branch(value: &str) -> bool {
    valid_token(value)
        && !value.starts_with('-')
        && !value.ends_with('.')
        && !value.contains("..")
        && !value.contains("@{")
        && !value
            .bytes()
            .any(|byte| matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\'))
}

fn valid_git_operation(value: &str) -> bool {
    matches!(value, "commit" | "fetch" | "push")
}

fn valid_sha(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_relative_path(value: &str) -> bool {
    !value.is_empty()
        && !value.contains('\\')
        && !Path::new(value).is_absolute()
        && Path::new(value)
            .components()
            .all(|component| match component {
                Component::Normal(name) => !name
                    .to_str()
                    .is_some_and(|value| value.eq_ignore_ascii_case(".git")),
                _ => false,
            })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn legacy_grant_is_rejected_without_an_exact_checkout() {
        let peer = "a".repeat(64);
        let raw = format!(
            r#"{{"version":1,"grants":[{{"project_address":"30621:{peer}:nemo","home_channel":"3580ca9b-47b4-4af9-b22a-1068778f26c6","repository":"https://github.com/mysteropodes/nemo","requester_pubkeys":["{peer}"],"capabilities":["rust"],"path_prefixes":["crates"]}}]}}"#
        );
        assert!(serde_json::from_str::<GrantDocument>(&raw).is_err());
    }

    #[test]
    fn remote_normalization_accepts_https_and_ssh_only() {
        assert_eq!(
            canonical_github_remote("git@github.com:Mysteropodes/Nemo.git").unwrap(),
            "https://github.com/mysteropodes/nemo"
        );
        assert!(canonical_github_remote("https://evil.example/a/b").is_err());
        assert!(canonical_github_remote("https://github.com/a/b/c").is_err());
    }

    #[test]
    fn path_scope_never_treats_empty_prefixes_as_global() {
        assert!(!path_allowed("crates/a", &[]));
        assert!(path_allowed("crates/a", &["crates".into()]));
        assert!(!path_allowed("other/a", &["crates".into()]));
        assert!(!path_allowed("crates/../secret", &["crates".into()]));
        assert!(!path_allowed("crates/.GiT/config", &["crates".into()]));
    }

    #[test]
    fn git_operations_are_explicit_bounded_and_deny_by_default() {
        let peer = "a".repeat(64);
        let checkout = tempfile::tempdir().expect("checkout");
        assert!(Command::new("git")
            .arg("init")
            .arg(checkout.path())
            .status()
            .expect("git init")
            .success());
        let base = serde_json::json!({
            "version": 1,
            "grants": [{
                "project_address": format!("30621:{peer}:nemo"),
                "home_channel": "3580ca9b-47b4-4af9-b22a-1068778f26c6",
                "repository": "https://github.com/mysteropodes/nemo",
                "requester_pubkeys": [peer],
                "capabilities": ["rust"],
                "path_prefixes": ["crates"],
                "base_sha": "b".repeat(40),
                "branch": "codex/a2a",
                "worktree_id": "a2a",
                "checkout_root": checkout.path(),
            }]
        });
        let without: GrantDocument =
            serde_json::from_value(base.clone()).expect("legacy grant parses deny-all");
        assert!(without.grants[0].git_operations.is_empty());

        let mut allowed = base.clone();
        allowed["grants"][0]["git_operations"] = serde_json::json!(["commit", "fetch", "push"]);
        let allowed: GrantDocument = serde_json::from_value(allowed).expect("operations parse");
        assert!(validate_grant(&mut allowed.grants[0].clone()).is_ok());

        let mut invalid = base;
        invalid["grants"][0]["git_operations"] = serde_json::json!(["commit", "shell"]);
        let mut invalid: GrantDocument = serde_json::from_value(invalid).expect("schema parses");
        assert!(validate_grant(&mut invalid.grants[0]).is_err());
    }

    #[test]
    fn absent_explicit_grant_source_ignores_checkout_local_document() {
        let checkout = tempfile::tempdir().expect("checkout");
        std::fs::create_dir(checkout.path().join(".buzz")).expect("buzz dir");
        std::fs::write(
            checkout.path().join(".buzz/agent-job-grants.json"),
            r#"{"version":1,"grants":"model-poison"}"#,
        )
        .expect("poisoned local grant");
        let grants = GrantSet::load(checkout.path(), None, None)
            .expect("absent explicit source is empty, not local discovery");
        assert!(grants.channels().is_empty());
    }

    #[tokio::test]
    async fn outbound_revalidates_the_exact_local_checkout() {
        let checkout = tempfile::tempdir().expect("checkout");
        let run = |args: &[&str]| {
            let output = Command::new("git")
                .arg("-C")
                .arg(checkout.path())
                .args(args)
                .output()
                .expect("git fixture command");
            assert!(
                output.status.success(),
                "git fixture command failed: {args:?}"
            );
        };
        run(&["init"]);
        run(&["config", "user.name", "Buzz Test"]);
        run(&["config", "user.email", "buzz-test@example.invalid"]);
        run(&["checkout", "-b", "codex/a2a"]);
        std::fs::write(checkout.path().join("fixture.txt"), "fixture\n").expect("fixture");
        run(&["add", "fixture.txt"]);
        run(&["commit", "-m", "fixture"]);
        run(&[
            "remote",
            "add",
            "origin",
            "https://github.com/mysteropodes/nemo.git",
        ]);
        let head = String::from_utf8(
            Command::new("git")
                .args([
                    "-C",
                    checkout.path().to_str().expect("path"),
                    "rev-parse",
                    "HEAD",
                ])
                .output()
                .expect("head")
                .stdout,
        )
        .expect("utf8 head")
        .trim()
        .to_owned();
        let peer = "a".repeat(64);
        let grant = serde_json::json!({
            "version": 1,
            "grants": [{
                "project_address": format!("30621:{peer}:nemo"),
                "home_channel": "3580ca9b-47b4-4af9-b22a-1068778f26c6",
                "repository": "https://github.com/mysteropodes/nemo",
                "requester_pubkeys": [peer],
                "capabilities": ["rust"],
                "path_prefixes": ["crates"],
                "base_sha": head,
                "branch": "codex/a2a",
                "worktree_id": "a2a",
                "checkout_root": checkout.path(),
            }]
        });
        let grants = GrantSet::load(checkout.path(), Some(grant.to_string()), None)
            .expect("exact checkout grant");
        assert!(grants
            .outbound(&"a".repeat(64), "rust", &["crates/buzz-acp".into()], "a2a")
            .await
            .is_ok());

        run(&["checkout", "-b", "other"]);
        assert!(grants
            .outbound(&"a".repeat(64), "rust", &["crates/buzz-acp".into()], "a2a")
            .await
            .is_err());
    }
}
