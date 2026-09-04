use std::collections::{BTreeSet, HashSet};
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use serde::Deserialize;

use super::HARNESS_ONLY_ENV;

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
    #[serde(default)]
    path_prefixes: Vec<String>,
    #[serde(default)]
    branches: Vec<String>,
    #[serde(default)]
    worktree_ids: Vec<String>,
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

/// Validated local, operator-controlled collaboration grants.
///
/// No `Debug` implementation is provided so the allowlist cannot be dumped by
/// broad state diagnostics.
#[derive(Clone, Default)]
pub struct GrantSet {
    grants: Vec<JobGrant>,
    cwd: PathBuf,
}

impl GrantSet {
    pub fn load(cwd: &Path, inline: Option<String>, file: Option<PathBuf>) -> Result<Self, String> {
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
            _ => None,
        };
        let Some(raw) = raw else {
            return Ok(Self {
                grants: Vec::new(),
                cwd: cwd.to_owned(),
            });
        };
        let document: GrantDocument = serde_json::from_str(&raw)
            .map_err(|error| format!("parsing local A2A grants: {error}"))?;
        if document.version != 1 || document.grants.len() > MAX_GRANTS {
            return Err(format!(
                "local A2A grants require version 1 and at most {MAX_GRANTS} entries"
            ));
        }
        for grant in &document.grants {
            validate_grant(grant)?;
        }
        Ok(Self {
            grants: document.grants,
            cwd: cwd.to_owned(),
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

    pub fn outbound(
        &self,
        recipient: &str,
        capability: &str,
        paths: &[String],
        worktree_id: &str,
    ) -> Result<GrantMatch, String> {
        let checkout = inspect_checkout(&self.cwd)?;
        let matches: Vec<&JobGrant> = self
            .grants
            .iter()
            .filter(|grant| {
                grant.repository == checkout.repository
                    && grant.requester_pubkeys.iter().any(|peer| peer == recipient)
                    && grant
                        .capabilities
                        .iter()
                        .any(|allowed| allowed == capability)
                    && grant
                        .branches
                        .iter()
                        .any(|allowed| allowed == &checkout.branch)
                    && grant
                        .worktree_ids
                        .iter()
                        .any(|allowed| allowed == worktree_id)
                    && paths
                        .iter()
                        .all(|path| path_allowed(path, &grant.path_prefixes))
            })
            .collect();
        match matches.as_slice() {
            [grant] => Ok(GrantMatch {
                project_address: grant.project_address.clone(),
                home_channel: grant.home_channel.clone(),
                repository: grant.repository.clone(),
                base_sha: checkout.base_sha,
                branch: checkout.branch,
                worktree_id: worktree_id.to_owned(),
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
                && grant
                    .branches
                    .iter()
                    .any(|allowed| allowed == &common.repository.branch)
                && grant
                    .worktree_ids
                    .iter()
                    .any(|allowed| allowed == &common.repository.worktree_id)
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
                && grant
                    .branches
                    .iter()
                    .any(|allowed| allowed == &repository.branch)
                && grant
                    .worktree_ids
                    .iter()
                    .any(|allowed| allowed == &repository.worktree_id)
                && repository
                    .paths
                    .iter()
                    .all(|path| path_allowed(path, &grant.path_prefixes))
        })
    }
}

struct Checkout {
    repository: String,
    base_sha: String,
    branch: String,
}

fn inspect_checkout(cwd: &Path) -> Result<Checkout, String> {
    let base_sha = git(cwd, &["rev-parse", "HEAD"])?;
    let branch = git(cwd, &["branch", "--show-current"])?;
    if branch.is_empty() {
        return Err("outbound A2A requires a named local branch".into());
    }
    let remote = git(cwd, &["config", "--get", "remote.origin.url"])?;
    let repository = canonical_github_remote(&remote)?;
    Ok(Checkout {
        repository,
        base_sha,
        branch,
    })
}

fn git(cwd: &Path, args: &[&str]) -> Result<String, String> {
    let mut command = Command::new("git");
    command.args(args).current_dir(cwd);
    for name in HARNESS_ONLY_ENV {
        command.env_remove(name);
    }
    let output = command
        .output()
        .map_err(|error| format!("running git: {error}"))?;
    if !output.status.success() {
        return Err("local checkout inspection failed".into());
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|_| "git output was not UTF-8".into())
}

fn canonical_github_remote(value: &str) -> Result<String, String> {
    let value = value.strip_suffix(".git").unwrap_or(value);
    let path = value
        .strip_prefix("https://github.com/")
        .or_else(|| value.strip_prefix("git@github.com:"))
        .ok_or_else(|| "origin must be a canonical GitHub HTTPS or SSH remote".to_owned())?;
    if path.split('/').count() != 2 || path.split('/').any(|part| part.is_empty()) {
        return Err("origin must identify one GitHub owner/repository".into());
    }
    Ok(format!("https://github.com/{}", path.to_ascii_lowercase()))
}

fn path_allowed(path: &str, prefixes: &[String]) -> bool {
    valid_relative_path(path)
        && !prefixes.is_empty()
        && prefixes
            .iter()
            .any(|prefix| path == prefix || path.starts_with(&format!("{prefix}/")))
}

fn validate_grant(grant: &JobGrant) -> Result<(), String> {
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
    unique_valid(&grant.path_prefixes, valid_relative_path, "path_prefixes")?;
    unique_valid(&grant.branches, valid_branch, "branches")?;
    unique_valid(&grant.worktree_ids, valid_token, "worktree_ids")?;
    Ok(())
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

fn valid_relative_path(value: &str) -> bool {
    !value.is_empty()
        && !value.contains('\\')
        && !Path::new(value).is_absolute()
        && Path::new(value)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_grant_parses_but_has_no_outbound_checkout_authority() {
        let peer = "a".repeat(64);
        let raw = format!(
            r#"{{"version":1,"grants":[{{"project_address":"30621:{peer}:nemo","home_channel":"3580ca9b-47b4-4af9-b22a-1068778f26c6","repository":"https://github.com/mysteropodes/nemo","requester_pubkeys":["{peer}"],"capabilities":["rust"],"path_prefixes":["crates"]}}]}}"#
        );
        let parsed: GrantDocument = serde_json::from_str(&raw).expect("grant");
        assert!(parsed.grants[0].branches.is_empty());
        assert!(parsed.grants[0].worktree_ids.is_empty());
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
    }
}
