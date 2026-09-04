use std::collections::{BTreeSet, HashSet};
use std::path::{Component, Path};

use buzz_core::job::JobRequest;
use serde::Deserialize;
use thiserror::Error;
use uuid::Uuid;

const MAX_GRANTS: usize = 128;
const MAX_CAPABILITIES_PER_GRANT: usize = 64;
const MAX_PATH_PREFIXES_PER_GRANT: usize = 128;
const MAX_CHECKOUTS_PER_GRANT: usize = 128;

#[derive(Debug, Error)]
pub enum GrantError {
    #[error("reading agent job grants: {0}")]
    Read(#[from] std::io::Error),
    #[error("parsing agent job grants: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("invalid agent job grants: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantDocument {
    version: u32,
    grants: Vec<JobGrant>,
}

#[derive(Debug, Clone, Deserialize)]
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

/// Local, operator-controlled capabilities for accepting signed job requests.
#[derive(Debug, Clone, Default)]
pub struct GrantSet {
    grants: Vec<JobGrant>,
}

impl GrantSet {
    pub fn load(cwd: &Path) -> Result<Self, GrantError> {
        if let Ok(raw) = std::env::var("BUZZ_ACP_JOB_GRANTS_JSON") {
            return Self::from_json(&raw);
        }
        let path = std::env::var_os("BUZZ_ACP_JOB_GRANTS_FILE")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| cwd.join(".buzz/agent-job-grants.json"));
        match std::fs::read_to_string(path) {
            Ok(raw) => Self::from_json(&raw),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn from_json(raw: &str) -> Result<Self, GrantError> {
        let document: GrantDocument = serde_json::from_str(raw)?;
        if document.version != 1 {
            return Err(GrantError::Invalid("version must be 1".into()));
        }
        if document.grants.len() > MAX_GRANTS {
            return Err(GrantError::Invalid(format!(
                "at most {MAX_GRANTS} grants are allowed"
            )));
        }
        for grant in &document.grants {
            validate_grant(grant)?;
        }
        Ok(Self {
            grants: document.grants,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.grants.is_empty()
    }

    pub fn capabilities_for(&self, request: &JobRequest) -> Option<Vec<String>> {
        Some(self.matching_grant(request)?.capabilities.clone())
    }

    /// Canonical Project home channels that need an agent-job subscription.
    ///
    /// The grant document is the sole source: chat subscription mode and its
    /// channel allowlist must never widen or narrow this receiver surface.
    pub fn home_channels(&self) -> Vec<Uuid> {
        self.grants
            .iter()
            .filter_map(|grant| Uuid::parse_str(&grant.home_channel).ok())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn contains_home_channel(&self, channel_id: Uuid) -> bool {
        self.grants
            .iter()
            .any(|grant| grant.home_channel == channel_id.to_string())
    }

    fn matching_grant(&self, request: &JobRequest) -> Option<&JobGrant> {
        self.grants.iter().find(|grant| {
            grant.project_address == request.common.project.address
                && grant.home_channel == request.common.project.home_channel
                && grant.repository == request.common.repository.canonical
                && grant
                    .requester_pubkeys
                    .iter()
                    .any(|requester| requester == &request.common.sender_pubkey)
                && grant
                    .capabilities
                    .iter()
                    .any(|capability| capability == &request.capability)
                && grant
                    .branches
                    .iter()
                    .any(|branch| branch == &request.common.repository.branch)
                && grant
                    .worktree_ids
                    .iter()
                    .any(|worktree| worktree == &request.common.repository.worktree_id)
                && request.common.repository.paths.iter().all(|path| {
                    grant.path_prefixes.is_empty()
                        || grant
                            .path_prefixes
                            .iter()
                            .any(|prefix| path == prefix || path.starts_with(&format!("{prefix}/")))
                })
        })
    }
}

fn validate_grant(grant: &JobGrant) -> Result<(), GrantError> {
    let mut project = grant.project_address.splitn(3, ':');
    if project.next() != Some("30621")
        || project.next().is_none_or(|owner| {
            owner.len() != 64
                || !owner
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        || project.next().is_none_or(str::is_empty)
    {
        return Err(GrantError::Invalid(
            "project_address must be a canonical 30621 coordinate".into(),
        ));
    }
    let channel = Uuid::parse_str(&grant.home_channel)
        .map_err(|_| GrantError::Invalid("home_channel must be a UUID".into()))?;
    if channel.to_string() != grant.home_channel {
        return Err(GrantError::Invalid(
            "home_channel must use canonical UUID spelling".into(),
        ));
    }
    validate_repository(&grant.repository)?;
    if grant.capabilities.is_empty() || grant.capabilities.len() > MAX_CAPABILITIES_PER_GRANT {
        return Err(GrantError::Invalid(format!(
            "capabilities must contain 1-{MAX_CAPABILITIES_PER_GRANT} entries"
        )));
    }
    if grant.requester_pubkeys.is_empty() || grant.requester_pubkeys.len() > MAX_GRANTS {
        return Err(GrantError::Invalid(format!(
            "requester_pubkeys must contain 1-{MAX_GRANTS} entries"
        )));
    }
    let mut requesters = HashSet::new();
    for requester in &grant.requester_pubkeys {
        let key = nostr::PublicKey::from_hex(requester).map_err(|_| {
            GrantError::Invalid("requester_pubkeys must contain canonical public keys".into())
        })?;
        if key.to_hex() != *requester || !requesters.insert(requester) {
            return Err(GrantError::Invalid(
                "requester_pubkeys must contain unique lowercase public keys".into(),
            ));
        }
    }
    if grant.path_prefixes.len() > MAX_PATH_PREFIXES_PER_GRANT {
        return Err(GrantError::Invalid(format!(
            "at most {MAX_PATH_PREFIXES_PER_GRANT} path prefixes are allowed"
        )));
    }
    let mut capabilities = HashSet::new();
    for capability in &grant.capabilities {
        if !valid_token(capability) || !capabilities.insert(capability) {
            return Err(GrantError::Invalid(
                "capabilities must be unique 1-128 byte printable tokens".into(),
            ));
        }
    }
    validate_checkout_tokens("branches", &grant.branches)?;
    validate_checkout_tokens("worktree_ids", &grant.worktree_ids)?;
    let mut prefixes = HashSet::new();
    for prefix in &grant.path_prefixes {
        if !valid_relative_path(prefix) || !prefixes.insert(prefix) {
            return Err(GrantError::Invalid(
                "path_prefixes must be unique normalized repository-relative paths".into(),
            ));
        }
    }
    Ok(())
}

fn validate_checkout_tokens(field: &str, values: &[String]) -> Result<(), GrantError> {
    if values.is_empty() || values.len() > MAX_CHECKOUTS_PER_GRANT {
        return Err(GrantError::Invalid(format!(
            "{field} must contain 1-{MAX_CHECKOUTS_PER_GRANT} entries"
        )));
    }
    let mut unique = HashSet::new();
    if values
        .iter()
        .any(|value| !valid_token(value) || !unique.insert(value))
    {
        return Err(GrantError::Invalid(format!(
            "{field} must contain unique 1-128 byte printable tokens"
        )));
    }
    Ok(())
}

fn valid_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.trim() == value
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn valid_relative_path(value: &str) -> bool {
    !value.is_empty()
        && !value.contains('\\')
        && !Path::new(value).is_absolute()
        && Path::new(value)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn validate_repository(value: &str) -> Result<(), GrantError> {
    let url = url::Url::parse(value)
        .map_err(|_| GrantError::Invalid("repository must be a URL".into()))?;
    let segments: Vec<_> = url
        .path_segments()
        .map(Iterator::collect)
        .unwrap_or_default();
    if url.scheme() != "https"
        || url.host_str() != Some("github.com")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || segments.len() != 2
        || segments.iter().any(|segment| segment.is_empty())
        || value.ends_with('/')
        || value.ends_with(".git")
    {
        return Err(GrantError::Invalid(
            "repository must be canonical https://github.com/owner/repo".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::job::{JobCommon, JobProject, JobRepository, JobSponsor, JOB_SCHEMA_VERSION};

    fn request() -> JobRequest {
        JobRequest {
            common: JobCommon {
                schema_version: JOB_SCHEMA_VERSION.into(),
                operation_id: "31dbb246-bc79-4ddc-aab0-2773f05b5cb2".into(),
                idempotency_key: "idem".into(),
                coordinator_epoch: 1,
                project: JobProject {
                    address: format!("30621:{}:nemo", "a".repeat(64)),
                    home_channel: "3580ca9b-47b4-4af9-b22a-1068778f26c6".into(),
                },
                repository: JobRepository {
                    canonical: "https://github.com/mysteropodes/nemo".into(),
                    github_issue: None,
                    github_pr: None,
                    github_run: None,
                    base_sha: "b".repeat(40),
                    branch: "codex/a2a".into(),
                    worktree_id: "a2a".into(),
                    paths: vec!["crates/buzz-acp".into()],
                    contracts: vec![],
                },
                sender_pubkey: "c".repeat(64),
                recipient_pubkey: "d".repeat(64),
                sponsor: JobSponsor {
                    pubkey: "c".repeat(64),
                    github_login: "owner".into(),
                },
                expires_at: "2030-01-01T00:00:00Z".into(),
            },
            capability: "rust".into(),
            summary: "Do work".into(),
            acceptance: vec!["Tests pass".into()],
            supersedes_event_id: None,
        }
    }

    #[test]
    fn exact_tuple_capability_and_path_are_required() {
        let raw = format!(
            r#"{{"version":1,"grants":[{{"project_address":"30621:{}:nemo","home_channel":"3580ca9b-47b4-4af9-b22a-1068778f26c6","repository":"https://github.com/mysteropodes/nemo","requester_pubkeys":["{}"],"capabilities":["rust"],"path_prefixes":["crates"],"branches":["codex/a2a"],"worktree_ids":["a2a"]}}]}}"#,
            "a".repeat(64),
            "c".repeat(64)
        );
        let grants = GrantSet::from_json(&raw).expect("valid grants");
        let mut candidate = request();
        assert!(grants.capabilities_for(&candidate).is_some());
        candidate.common.repository.canonical = "https://github.com/block/buzz".into();
        assert!(grants.capabilities_for(&candidate).is_none());
        candidate.common.repository.canonical = "https://github.com/mysteropodes/nemo".into();
        candidate.common.repository.branch = "main".into();
        assert!(grants.capabilities_for(&candidate).is_none());
        candidate.common.repository.branch = "codex/a2a".into();
        candidate.common.repository.worktree_id = "other".into();
        assert!(grants.capabilities_for(&candidate).is_none());
    }

    #[test]
    fn home_channels_are_grant_derived_deduplicated_and_stable() {
        let left = Uuid::parse_str("1580ca9b-47b4-4af9-b22a-1068778f26c6").unwrap();
        let right = Uuid::parse_str("3580ca9b-47b4-4af9-b22a-1068778f26c6").unwrap();
        let raw = format!(
            r#"{{"version":1,"grants":[
                {{"project_address":"30621:{}:nemo","home_channel":"{right}","repository":"https://github.com/mysteropodes/nemo","requester_pubkeys":["{}"],"capabilities":["rust"],"path_prefixes":[],"branches":["codex/a2a"],"worktree_ids":["a2a"]}},
                {{"project_address":"30621:{}:other","home_channel":"{left}","repository":"https://github.com/mysteropodes/other","requester_pubkeys":["{}"],"capabilities":["review"],"path_prefixes":[],"branches":["codex/review"],"worktree_ids":["review"]}},
                {{"project_address":"30621:{}:nemo","home_channel":"{right}","repository":"https://github.com/mysteropodes/nemo","requester_pubkeys":["{}"],"capabilities":["test"],"path_prefixes":[],"branches":["codex/test"],"worktree_ids":["test"]}}
            ]}}"#,
            "a".repeat(64),
            "c".repeat(64),
            "b".repeat(64),
            "d".repeat(64),
            "a".repeat(64),
            "e".repeat(64),
        );
        let grants = GrantSet::from_json(&raw).expect("valid grants");
        assert_eq!(grants.home_channels(), vec![left, right]);
        assert!(grants.contains_home_channel(left));
        assert!(grants.contains_home_channel(right));
        assert!(!grants.contains_home_channel(Uuid::new_v4()));
        assert!(GrantSet::default().home_channels().is_empty());
    }
}
