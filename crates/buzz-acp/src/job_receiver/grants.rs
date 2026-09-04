use std::collections::{BTreeSet, HashSet};
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use buzz_core::job::JobRequest;
use serde::Deserialize;
use thiserror::Error;
use uuid::Uuid;

const MAX_GRANTS: usize = 128;
const MAX_CAPABILITIES_PER_GRANT: usize = 64;
const MAX_PATH_PREFIXES_PER_GRANT: usize = 128;

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

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct JobGrant {
    project_address: String,
    home_channel: String,
    repository: String,
    requester_pubkeys: Vec<String>,
    capabilities: Vec<String>,
    path_prefixes: Vec<String>,
    base_sha: String,
    branch: String,
    worktree_id: String,
    checkout_root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct GrantMatch {
    pub capabilities: Vec<String>,
    pub checkout_root: PathBuf,
}

/// Local, operator-controlled capabilities for accepting signed job requests.
#[derive(Debug, Clone, Default)]
pub struct GrantSet {
    grants: Vec<JobGrant>,
}

impl GrantSet {
    pub fn load_from(
        cwd: &Path,
        grants_json: Option<String>,
        grants_file: Option<std::path::PathBuf>,
    ) -> Result<Self, GrantError> {
        if let Some(raw) = grants_json {
            return Self::from_json(&raw);
        }
        let path = grants_file.unwrap_or_else(|| cwd.join(".buzz/agent-job-grants.json"));
        match std::fs::read_to_string(path) {
            Ok(raw) => Self::from_json(&raw),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn from_json(raw: &str) -> Result<Self, GrantError> {
        let mut document: GrantDocument = serde_json::from_str(raw)?;
        if document.version != 1 {
            return Err(GrantError::Invalid("version must be 1".into()));
        }
        if document.grants.len() > MAX_GRANTS {
            return Err(GrantError::Invalid(format!(
                "at most {MAX_GRANTS} grants are allowed"
            )));
        }
        for grant in &mut document.grants {
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

    pub fn authorize_request(&self, request: &JobRequest) -> Option<GrantMatch> {
        let grant = self.matching_grant(request)?;
        let checkout_root = checkout_matches(grant, request)?;
        Some(GrantMatch {
            capabilities: grant.capabilities.clone(),
            checkout_root,
        })
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
        let mut matches = self.grants.iter().filter(|grant| {
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
                && grant.base_sha == request.common.repository.base_sha
                && grant.branch == request.common.repository.branch
                && grant.worktree_id == request.common.repository.worktree_id
                && request.common.repository.paths.iter().all(|path| {
                    grant
                        .path_prefixes
                        .iter()
                        .any(|prefix| path == prefix || path.starts_with(&format!("{prefix}/")))
                })
        });
        let matched = matches.next()?;
        matches.next().is_none().then_some(matched)
    }
}

fn validate_grant(grant: &mut JobGrant) -> Result<(), GrantError> {
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
    if grant.path_prefixes.is_empty() || grant.path_prefixes.len() > MAX_PATH_PREFIXES_PER_GRANT {
        return Err(GrantError::Invalid(format!(
            "path_prefixes must contain 1-{MAX_PATH_PREFIXES_PER_GRANT} entries"
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
    if !valid_sha(&grant.base_sha) {
        return Err(GrantError::Invalid(
            "base_sha must be 40 or 64 lowercase hexadecimal characters".into(),
        ));
    }
    if !valid_token(&grant.branch) || !valid_token(&grant.worktree_id) {
        return Err(GrantError::Invalid(
            "branch and worktree_id must be 1-128 byte printable tokens".into(),
        ));
    }
    if !grant.checkout_root.is_absolute() {
        return Err(GrantError::Invalid(
            "checkout_root must be an absolute existing Git checkout".into(),
        ));
    }
    grant.checkout_root = grant.checkout_root.canonicalize().map_err(|_| {
        GrantError::Invalid("checkout_root must be an absolute existing Git checkout".into())
    })?;
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

fn valid_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.trim() == value
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn valid_sha(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn checkout_matches(grant: &JobGrant, request: &JobRequest) -> Option<PathBuf> {
    let root = grant.checkout_root.canonicalize().ok()?;
    let top = git_output(&root, &["rev-parse", "--show-toplevel"])?;
    if Path::new(&top).canonicalize().ok()? != root {
        return None;
    }
    let origin = canonical_git_origin(&git_output(&root, &["remote", "get-url", "origin"])?)?;
    let branch = git_output(&root, &["symbolic-ref", "--quiet", "--short", "HEAD"])?;
    let head = git_output(&root, &["rev-parse", "HEAD"])?;
    (origin == request.common.repository.canonical
        && branch == request.common.repository.branch
        && head == request.common.repository.base_sha)
        .then_some(root)
}

fn git_output(root: &Path, args: &[&str]) -> Option<String> {
    let mut command = Command::new("git");
    command
        .arg("-C")
        .arg(root)
        .args(args)
        .env_clear()
        .env(
            "PATH",
            std::env::var_os("PATH").unwrap_or_else(|| "/usr/bin:/bin".into()),
        )
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1");
    let output = command.output().ok()?;
    if !output.status.success() || !output.stderr.is_empty() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn canonical_git_origin(value: &str) -> Option<String> {
    let path = if let Some(path) = value.strip_prefix("git@github.com:") {
        path.to_owned()
    } else {
        let url = url::Url::parse(value).ok()?;
        if url.scheme() != "https"
            || url.host_str() != Some("github.com")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return None;
        }
        url.path().trim_start_matches('/').to_owned()
    };
    let path = path.strip_suffix(".git").unwrap_or(&path);
    let mut segments = path.split('/');
    let owner = segments.next()?;
    let repository = segments.next()?;
    if owner.is_empty() || repository.is_empty() || segments.next().is_some() {
        return None;
    }
    Some(format!(
        "https://github.com/{}/{}",
        owner.to_ascii_lowercase(),
        repository.to_ascii_lowercase()
    ))
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

    fn initialized_checkout() -> tempfile::TempDir {
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
        checkout
    }

    #[test]
    fn exact_tuple_capability_and_path_are_required() {
        let checkout = tempfile::tempdir().expect("checkout");
        let raw = format!(
            r#"{{"version":1,"grants":[{{"project_address":"30621:{}:nemo","home_channel":"3580ca9b-47b4-4af9-b22a-1068778f26c6","repository":"https://github.com/mysteropodes/nemo","requester_pubkeys":["{}"],"capabilities":["rust"],"path_prefixes":["crates"],"base_sha":"{}","branch":"codex/a2a","worktree_id":"a2a","checkout_root":{}}}]}}"#,
            "a".repeat(64),
            "c".repeat(64),
            "b".repeat(40),
            serde_json::to_string(checkout.path()).unwrap(),
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
        let checkout = tempfile::tempdir().expect("checkout");
        let root = serde_json::to_string(checkout.path()).unwrap();
        let left = Uuid::parse_str("1580ca9b-47b4-4af9-b22a-1068778f26c6").unwrap();
        let right = Uuid::parse_str("3580ca9b-47b4-4af9-b22a-1068778f26c6").unwrap();
        let raw = format!(
            r#"{{"version":1,"grants":[
                {{"project_address":"30621:{}:nemo","home_channel":"{right}","repository":"https://github.com/mysteropodes/nemo","requester_pubkeys":["{}"],"capabilities":["rust"],"path_prefixes":["src"],"base_sha":"{}","branch":"codex/a2a","worktree_id":"a2a","checkout_root":{root}}},
                {{"project_address":"30621:{}:other","home_channel":"{left}","repository":"https://github.com/mysteropodes/other","requester_pubkeys":["{}"],"capabilities":["review"],"path_prefixes":["src"],"base_sha":"{}","branch":"codex/review","worktree_id":"review","checkout_root":{root}}},
                {{"project_address":"30621:{}:nemo","home_channel":"{right}","repository":"https://github.com/mysteropodes/nemo","requester_pubkeys":["{}"],"capabilities":["test"],"path_prefixes":["src"],"base_sha":"{}","branch":"codex/test","worktree_id":"test","checkout_root":{root}}}
            ]}}"#,
            "a".repeat(64),
            "c".repeat(64),
            "a".repeat(40),
            "b".repeat(64),
            "d".repeat(64),
            "b".repeat(40),
            "a".repeat(64),
            "e".repeat(64),
            "c".repeat(40),
        );
        let grants = GrantSet::from_json(&raw).expect("valid grants");
        assert_eq!(grants.home_channels(), vec![left, right]);
        assert!(grants.contains_home_channel(left));
        assert!(grants.contains_home_channel(right));
        assert!(!grants.contains_home_channel(Uuid::new_v4()));
        assert!(GrantSet::default().home_channels().is_empty());
    }

    #[test]
    fn empty_prefixes_and_case_insensitive_git_prefixes_are_rejected() {
        let checkout = tempfile::tempdir().expect("checkout");
        for prefixes in [serde_json::json!([]), serde_json::json!(["src/.GiT"])] {
            let raw = serde_json::json!({
                "version": 1,
                "grants": [{
                    "project_address": format!("30621:{}:nemo", "a".repeat(64)),
                    "home_channel": "3580ca9b-47b4-4af9-b22a-1068778f26c6",
                    "repository": "https://github.com/mysteropodes/nemo",
                    "requester_pubkeys": ["c".repeat(64)],
                    "capabilities": ["rust"],
                    "path_prefixes": prefixes,
                    "base_sha": "b".repeat(40),
                    "branch": "codex/a2a",
                    "worktree_id": "a2a",
                    "checkout_root": checkout.path(),
                }]
            });
            assert!(GrantSet::from_json(&raw.to_string()).is_err());
        }
    }

    #[test]
    fn live_checkout_remote_branch_and_head_are_revalidated_on_every_admission() {
        let checkout = initialized_checkout();
        let head = git_output(checkout.path(), &["rev-parse", "HEAD"]).unwrap();
        let mut candidate = request();
        candidate.common.repository.base_sha = head.clone();
        let raw = serde_json::json!({
            "version": 1,
            "grants": [{
                "project_address": candidate.common.project.address.clone(),
                "home_channel": candidate.common.project.home_channel.clone(),
                "repository": candidate.common.repository.canonical.clone(),
                "requester_pubkeys": [candidate.common.sender_pubkey.clone()],
                "capabilities": [candidate.capability.clone()],
                "path_prefixes": ["crates"],
                "base_sha": head,
                "branch": "codex/a2a",
                "worktree_id": "a2a",
                "checkout_root": checkout.path(),
            }]
        });
        let grants = GrantSet::from_json(&raw.to_string()).expect("valid checkout grant");
        assert!(grants.authorize_request(&candidate).is_some());

        let run = |args: &[&str]| {
            assert!(Command::new("git")
                .arg("-C")
                .arg(checkout.path())
                .args(args)
                .status()
                .expect("git mutation")
                .success());
        };
        run(&["checkout", "-b", "other"]);
        assert!(grants.authorize_request(&candidate).is_none());
        run(&["checkout", "codex/a2a"]);
        std::fs::write(checkout.path().join("second.txt"), "second\n").expect("second");
        run(&["add", "second.txt"]);
        run(&["commit", "-m", "head drift"]);
        assert!(grants.authorize_request(&candidate).is_none());
        run(&["reset", "--hard", &candidate.common.repository.base_sha]);
        run(&[
            "remote",
            "set-url",
            "origin",
            "https://github.com/mysteropodes/other.git",
        ]);
        assert!(grants.authorize_request(&candidate).is_none());
    }
}
