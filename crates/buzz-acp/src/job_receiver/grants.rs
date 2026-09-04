use std::collections::{BTreeSet, HashSet};
#[cfg(windows)]
use std::ffi::OsString;
use std::io::Read as _;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use buzz_core::job::JobRequest;
use serde::Deserialize;
use thiserror::Error;
use uuid::Uuid;

const MAX_GRANTS: usize = 128;
const MAX_CAPABILITIES_PER_GRANT: usize = 64;
const MAX_PATH_PREFIXES_PER_GRANT: usize = 128;
const MAX_GRANT_DOCUMENT_BYTES: u64 = 768 * 1024;
const MAX_GIT_OUTPUT_BYTES: u64 = 64 * 1024;
const GIT_COMMAND_TIMEOUT: Duration = Duration::from_secs(2);
const GIT_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

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
    #[serde(default)]
    git_operations: Vec<String>,
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

#[derive(Debug)]
pub(crate) struct PreparedJobSources {
    pub grants_json: Option<String>,
    pub grant_source_file: Option<PathBuf>,
    pub ledger_root: Option<PathBuf>,
    pub protected_ledger_root: Option<PathBuf>,
}

/// Local, operator-controlled capabilities for accepting signed job requests.
#[derive(Debug, Clone, Default)]
pub struct GrantSet {
    grants: Vec<JobGrant>,
    nemo_workspace: bool,
    nemo_checkout: Option<NemoCheckout>,
}

#[derive(Debug, Clone)]
struct NemoCheckout {
    root: PathBuf,
}

impl GrantSet {
    pub fn load_from(
        cwd: &Path,
        grants_json: Option<String>,
        grants_file: Option<std::path::PathBuf>,
    ) -> Result<Self, GrantError> {
        Self::load_with_nemo(cwd, grants_json, grants_file, false)
    }

    pub fn load_with_nemo(
        cwd: &Path,
        grants_json: Option<String>,
        grants_file: Option<std::path::PathBuf>,
        nemo_workspace: bool,
    ) -> Result<Self, GrantError> {
        if nemo_workspace {
            return Ok(Self {
                grants: Vec::new(),
                nemo_workspace: true,
                nemo_checkout: discover_nemo_checkout(cwd),
            });
        }
        match (grants_json, grants_file) {
            (Some(_), Some(_)) => Err(GrantError::Invalid(
                "grant JSON and grant file are mutually exclusive".into(),
            )),
            (Some(raw), None) => Self::from_json(&raw),
            (None, Some(path)) => {
                let (_, raw) = read_secure_grant_file(cwd, &path)?;
                Self::from_json(&raw)
            }
            // There is deliberately no checkout-local fallback. An absent
            // operator-controlled source means there are no grants.
            (None, None) => Ok(Self::default()),
        }
    }

    pub fn from_json(raw: &str) -> Result<Self, GrantError> {
        if raw.len() as u64 > MAX_GRANT_DOCUMENT_BYTES {
            return Err(GrantError::Invalid(format!(
                "grant document exceeds {MAX_GRANT_DOCUMENT_BYTES} bytes"
            )));
        }
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
            nemo_workspace: false,
            nemo_checkout: None,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.grants.is_empty() && !self.nemo_workspace
    }

    pub fn capabilities_for(&self, request: &JobRequest) -> Option<Vec<String>> {
        if self.nemo_workspace && static_nemo_request_allowed(request) {
            return Some(vec![request.capability.clone()]);
        }
        Some(self.matching_grant(request)?.capabilities.clone())
    }

    pub fn git_operations_for(&self, request: &JobRequest) -> Option<Vec<String>> {
        if self.nemo_workspace && static_nemo_request_allowed(request) {
            return Some(vec!["commit".into(), "fetch".into(), "push".into()]);
        }
        Some(self.matching_grant(request)?.git_operations.clone())
    }

    pub fn authorize_request(&self, request: &JobRequest) -> Result<Option<GrantMatch>, String> {
        if self.nemo_workspace && static_nemo_request_allowed(request) {
            let checkout = self
                .nemo_checkout
                .as_ref()
                .ok_or_else(|| "the Nemo repository checkout is unavailable".to_owned())?;
            let checkout_root = prepare_nemo_worktree(checkout, request)?;
            return Ok(Some(GrantMatch {
                capabilities: vec![request.capability.clone()],
                checkout_root,
            }));
        }
        let Some(grant) = self.matching_grant(request) else {
            return Ok(None);
        };
        let Some(checkout_root) = checkout_matches(grant, request) else {
            return Ok(None);
        };
        Ok(Some(GrantMatch {
            capabilities: grant.capabilities.clone(),
            checkout_root,
        }))
    }

    /// Canonical Project home channels that need an agent-job subscription.
    ///
    /// The grant document is the sole source: chat subscription mode and its
    /// channel allowlist must never widen or narrow this receiver surface.
    pub fn home_channels(&self) -> Vec<Uuid> {
        let mut channels = self
            .grants
            .iter()
            .filter_map(|grant| Uuid::parse_str(&grant.home_channel).ok())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if self.nemo_workspace {
            if let Ok(channel) = Uuid::parse_str(buzz_core::nemo::HOME_CHANNEL) {
                if !channels.contains(&channel) {
                    channels.push(channel);
                    channels.sort();
                }
            }
        }
        channels
    }

    pub fn contains_home_channel(&self, channel_id: Uuid) -> bool {
        (self.nemo_workspace && channel_id.to_string() == buzz_core::nemo::HOME_CHANNEL)
            || self
                .grants
                .iter()
                .any(|grant| grant.home_channel == channel_id.to_string())
    }

    pub(super) fn checkout_roots(&self) -> impl Iterator<Item = &Path> {
        self.grants
            .iter()
            .map(|grant| grant.checkout_root.as_path())
            .chain(
                self.nemo_checkout
                    .iter()
                    .map(|checkout| checkout.root.as_path()),
            )
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

pub(crate) fn prepare_job_sources(
    cwd: &Path,
    grants_json: Option<String>,
    grants_file: Option<PathBuf>,
    ledger_root: Option<PathBuf>,
    nemo_workspace: bool,
) -> Result<PreparedJobSources, GrantError> {
    if nemo_workspace {
        let grants = GrantSet::load_with_nemo(cwd, None, None, true)?;
        let (ledger_root, protected_ledger_root) = match ledger_root {
            Some(root) => {
                let root = prepare_private_ledger_root(cwd, &root, grants.checkout_roots())?;
                (Some(root.clone()), Some(root))
            }
            None => {
                let base = super::default_ledger_base()?;
                let base = prepare_private_ledger_root(cwd, &base, grants.checkout_roots())?;
                (None, Some(base))
            }
        };
        return Ok(PreparedJobSources {
            grants_json: None,
            grant_source_file: None,
            ledger_root,
            protected_ledger_root,
        });
    }
    let (grants_json, grant_source_file) = match (grants_json, grants_file) {
        (Some(_), Some(_)) => {
            return Err(GrantError::Invalid(
                "grant JSON and grant file are mutually exclusive".into(),
            ));
        }
        (Some(raw), None) => (Some(raw), None),
        (None, Some(path)) => {
            let (canonical, raw) = read_secure_grant_file(cwd, &path)?;
            (Some(raw), Some(canonical))
        }
        (None, None) => (None, None),
    };
    let grants = match grants_json.as_deref() {
        Some(raw) => GrantSet::from_json(raw)?,
        None => GrantSet::default(),
    };
    if let Some(source) = grant_source_file.as_deref() {
        reject_model_controlled_path(cwd, source, grants.checkout_roots())?;
    }
    let (ledger_root, protected_ledger_root) = match ledger_root {
        Some(root) => {
            let root = prepare_private_ledger_root(cwd, &root, grants.checkout_roots())?;
            (Some(root.clone()), Some(root))
        }
        None if grants.is_empty() => (None, None),
        None => {
            let base = super::default_ledger_base()?;
            let base = prepare_private_ledger_root(cwd, &base, grants.checkout_roots())?;
            (None, Some(base))
        }
    };
    Ok(PreparedJobSources {
        grants_json,
        grant_source_file,
        ledger_root,
        protected_ledger_root,
    })
}

pub(super) fn prepare_private_ledger_root<'a>(
    cwd: &Path,
    requested: &Path,
    checkout_roots: impl Iterator<Item = &'a Path>,
) -> Result<PathBuf, GrantError> {
    if !requested.is_absolute()
        || requested
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(GrantError::Invalid(
            "job ledger root must be a normalized absolute path".into(),
        ));
    }
    let canonical = canonicalize_for_private_directory(requested)?;
    reject_model_controlled_path(cwd, &canonical, checkout_roots)?;
    secure_private_directory(&canonical)?;
    Ok(canonical)
}

fn canonicalize_for_private_directory(path: &Path) -> Result<PathBuf, GrantError> {
    let mut existing = path;
    let mut suffix = Vec::new();
    loop {
        match std::fs::symlink_metadata(existing) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(GrantError::Invalid(
                        "job ledger root must not contain symbolic links".into(),
                    ));
                }
                if !metadata.is_dir() {
                    return Err(GrantError::Invalid(
                        "job ledger root parent must be a directory".into(),
                    ));
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = existing.file_name().ok_or_else(|| {
                    GrantError::Invalid("job ledger root has no existing ancestor".into())
                })?;
                suffix.push(name.to_os_string());
                existing = existing.parent().ok_or_else(|| {
                    GrantError::Invalid("job ledger root has no existing ancestor".into())
                })?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    reject_symlink_components(existing)?;
    let mut canonical = existing.canonicalize()?;
    for component in suffix.into_iter().rev() {
        canonical.push(component);
    }
    Ok(canonical)
}

fn reject_model_controlled_path<'a>(
    cwd: &Path,
    candidate: &Path,
    checkout_roots: impl Iterator<Item = &'a Path>,
) -> Result<(), GrantError> {
    let cwd = cwd.canonicalize().map_err(|_| {
        GrantError::Invalid("working directory must be an existing canonical path".into())
    })?;
    if candidate.starts_with(&cwd)
        || checkout_roots
            .filter_map(|root| root.canonicalize().ok())
            .any(|root| candidate.starts_with(root))
    {
        return Err(GrantError::Invalid(
            "grant and ledger paths must be outside every model-controlled checkout".into(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn secure_private_directory(path: &Path) -> Result<(), GrantError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    std::fs::create_dir_all(path)?;
    reject_symlink_components(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(GrantError::Invalid(
            "job ledger root must be a real directory".into(),
        ));
    }
    if metadata.uid() != nix::unistd::geteuid().as_raw() {
        return Err(GrantError::Invalid(
            "job ledger root must be owned by the current operator".into(),
        ));
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    let secured = std::fs::symlink_metadata(path)?;
    if secured.permissions().mode() & 0o077 != 0 {
        return Err(GrantError::Invalid(
            "job ledger root could not be restricted to its owner".into(),
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn secure_private_directory(path: &Path) -> Result<(), GrantError> {
    super::windows_private::secure_directory(path).map_err(GrantError::from)
}

#[cfg(all(not(unix), not(windows)))]
fn secure_private_directory(_path: &Path) -> Result<(), GrantError> {
    Err(GrantError::Invalid(
        "job ledgers are disabled until this platform has owner-only ACL validation".into(),
    ))
}

/// Read an operator grant document exactly once before any model-controlled
/// process exists. The returned canonical path is retained only so generic MCP
/// tools can deny access to the source; all authorization uses the returned
/// immutable in-memory bytes.
pub(super) fn read_secure_grant_file(
    cwd: &Path,
    path: &Path,
) -> Result<(PathBuf, String), GrantError> {
    if !path.is_absolute() {
        return Err(GrantError::Invalid(
            "grant file must be an absolute path outside the checkout".into(),
        ));
    }
    let cwd = cwd.canonicalize().map_err(|_| {
        GrantError::Invalid("working directory must be an existing canonical path".into())
    })?;
    reject_symlink_components(path)?;
    let canonical = path.canonicalize().map_err(|error| {
        GrantError::Invalid(format!(
            "grant file must be an existing regular file: {error}"
        ))
    })?;
    if canonical.starts_with(&cwd) {
        return Err(GrantError::Invalid(
            "grant file must be outside the model-controlled checkout".into(),
        ));
    }

    let mut file = open_private_grant_file(&canonical)?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_GRANT_DOCUMENT_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_GRANT_DOCUMENT_BYTES {
        return Err(GrantError::Invalid(format!(
            "grant document exceeds {MAX_GRANT_DOCUMENT_BYTES} bytes"
        )));
    }
    let raw = String::from_utf8(bytes)
        .map_err(|_| GrantError::Invalid("grant document must be UTF-8 JSON".into()))?;
    Ok((canonical, raw))
}

#[cfg(unix)]
fn open_private_grant_file(path: &Path) -> Result<std::fs::File, GrantError> {
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(GrantError::Invalid(
            "grant file must be a regular file".into(),
        ));
    }
    if metadata.uid() != nix::unistd::geteuid().as_raw() {
        return Err(GrantError::Invalid(
            "grant file must be owned by the current operator".into(),
        ));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(GrantError::Invalid(
            "grant file permissions must not grant group or other access".into(),
        ));
    }
    Ok(file)
}

#[cfg(windows)]
fn open_private_grant_file(path: &Path) -> Result<std::fs::File, GrantError> {
    super::windows_private::open_private_read(path).map_err(GrantError::from)
}

#[cfg(all(not(unix), not(windows)))]
fn open_private_grant_file(_path: &Path) -> Result<std::fs::File, GrantError> {
    Err(GrantError::Invalid(
        "grant files are disabled until this platform has owner-only ACL validation; use secure inline grant JSON"
            .into(),
    ))
}

fn reject_symlink_components(path: &Path) -> Result<(), GrantError> {
    let mut cursor = PathBuf::new();
    for component in path.components() {
        cursor.push(component.as_os_str());
        match std::fs::symlink_metadata(&cursor) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(GrantError::Invalid(
                    "grant file path must not contain symbolic links".into(),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
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
    let mut git_operations = HashSet::new();
    for operation in &grant.git_operations {
        if !matches!(operation.as_str(), "commit" | "fetch" | "push")
            || !git_operations.insert(operation)
        {
            return Err(GrantError::Invalid(
                "git_operations must contain unique commit, fetch, or push entries".into(),
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

fn static_nemo_request_allowed(request: &JobRequest) -> bool {
    let repository = &request.common.repository;
    buzz_core::nemo::matches(
        &request.common.project.address,
        &request.common.project.home_channel,
        &repository.canonical,
    ) && nostr::PublicKey::parse(&request.common.sender_pubkey)
        .is_ok_and(|key| key.to_hex() == request.common.sender_pubkey)
        && valid_token(&request.capability)
        && valid_sha(&repository.base_sha)
        && buzz_core::nemo::valid_worktree_component(&repository.worktree_id)
        && repository.branch == format!("codex/{}", repository.worktree_id)
        && !repository.paths.is_empty()
        && repository
            .paths
            .iter()
            .all(|path| valid_relative_path(path))
}

fn discover_nemo_checkout(cwd: &Path) -> Option<NemoCheckout> {
    let repos = cwd.join("REPOS").canonicalize().ok();
    let mut candidates = Vec::new();
    if let Some(repos) = &repos {
        candidates.push((repos.join("nemo"), Some(repos)));
        candidates.push((repos.join("mysteropodes--nemo"), Some(repos)));
    }
    candidates.push((cwd.to_path_buf(), None));
    candidates.into_iter().find_map(|(candidate, boundary)| {
        let root = candidate.canonicalize().ok()?;
        (root.is_dir()
            && root.join(".git").exists()
            && boundary.is_none_or(|boundary| root.starts_with(boundary)))
        .then_some(NemoCheckout { root })
    })
}

fn prepare_nemo_worktree(checkout: &NemoCheckout, request: &JobRequest) -> Result<PathBuf, String> {
    let source = checkout
        .root
        .canonicalize()
        .map_err(|_| "the Nemo repository checkout is unavailable".to_owned())?;
    checkout_config_is_safe(&source)
        .ok_or_else(|| "the Nemo repository Git configuration is unsafe".to_owned())?;
    let top = PathBuf::from(
        git_output(&source, &["rev-parse", "--show-toplevel"])
            .ok_or_else(|| "the Nemo repository root could not be verified".to_owned())?,
    );
    let origin = canonical_git_origin(
        &git_output(&source, &["remote", "get-url", "origin"])
            .ok_or_else(|| "the Nemo repository origin could not be verified".to_owned())?,
    )
    .ok_or_else(|| "the Nemo repository origin is not canonical".to_owned())?;
    if top.canonicalize().ok().as_ref() != Some(&source) || origin != buzz_core::nemo::REPOSITORY {
        return Err("the local checkout is not the managed Nemo repository".into());
    }

    let commit_spec = format!("{}^{{commit}}", request.common.repository.base_sha);
    let mut resolved = git_output(&source, &["rev-parse", "--verify", &commit_spec]);
    if resolved.as_deref() != Some(request.common.repository.base_sha.as_str()) {
        let fetched = git_success_with_timeout(
            &source,
            &[
                "fetch",
                "--quiet",
                "--no-tags",
                "--no-write-fetch-head",
                "--no-recurse-submodules",
                "origin",
                &request.common.repository.base_sha,
            ],
            GIT_FETCH_TIMEOUT,
        );
        if !fetched {
            return Err(
                "the requested Nemo base commit is absent locally and fetch from origin failed; check network or GitHub authentication"
                    .into(),
            );
        }
        resolved = git_output(&source, &["rev-parse", "--verify", &commit_spec]);
    }
    if resolved.as_deref() != Some(request.common.repository.base_sha.as_str()) {
        return Err("the requested Nemo base commit is unavailable after fetch".into());
    }

    let parent = source
        .parent()
        .and_then(|path| path.canonicalize().ok())
        .ok_or_else(|| "the Nemo checkout parent is unavailable".to_owned())?;
    let worktrees = parent.join("nemo-worktrees");
    match std::fs::symlink_metadata(&worktrees) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err("the Nemo worktree directory is unsafe".into())
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(&worktrees)
                .map_err(|_| "the Nemo worktree directory could not be created".to_owned())?;
        }
        Err(_) => return Err("the Nemo worktree directory is unavailable".into()),
    }
    let worktrees = worktrees
        .canonicalize()
        .map_err(|_| "the Nemo worktree directory could not be verified".to_owned())?;
    if worktrees.parent() != Some(parent.as_path()) {
        return Err("the Nemo worktree directory escaped its repository root".into());
    }
    let target = worktrees.join(&request.common.repository.worktree_id);
    match std::fs::symlink_metadata(&target) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err("the requested Nemo worktree path is unsafe".into())
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let target_argument = git_worktree_path_argument(&target)
                .ok_or_else(|| "the Nemo worktree path is not supported by Git".to_owned())?;
            let branch_ref = format!("refs/heads/{}", request.common.repository.branch);
            let existing = git_output(&source, &["rev-parse", "--verify", &branch_ref]);
            let added = match existing {
                Some(value)
                    if git_success(
                        &source,
                        &[
                            "merge-base",
                            "--is-ancestor",
                            &request.common.repository.base_sha,
                            &value,
                        ],
                    ) =>
                {
                    git_success(
                        &source,
                        &[
                            "-c",
                            &format!("core.hooksPath={}", null_device()),
                            "worktree",
                            "add",
                            &target_argument,
                            &request.common.repository.branch,
                        ],
                    )
                }
                None => git_success(
                    &source,
                    &[
                        "-c",
                        &format!("core.hooksPath={}", null_device()),
                        "worktree",
                        "add",
                        "-b",
                        &request.common.repository.branch,
                        &target_argument,
                        &request.common.repository.base_sha,
                    ],
                ),
                _ => false,
            };
            if !added {
                return Err(
                    "the Nemo job worktree could not be created from its signed base".into(),
                );
            }
        }
        Err(_) => return Err("the Nemo job worktree path could not be inspected".into()),
    }

    let target = target
        .canonicalize()
        .map_err(|_| "the Nemo job worktree is unavailable".to_owned())?;
    if !target.starts_with(&worktrees) {
        return Err("the Nemo job worktree escaped its managed root".into());
    }
    checkout_config_is_safe(&target)
        .ok_or_else(|| "the Nemo job worktree Git configuration is unsafe".to_owned())?;
    let target_top = PathBuf::from(
        git_output(&target, &["rev-parse", "--show-toplevel"])
            .ok_or_else(|| "the Nemo job worktree root could not be verified".to_owned())?,
    );
    let branch = git_output(&target, &["symbolic-ref", "--quiet", "--short", "HEAD"])
        .ok_or_else(|| "the Nemo job worktree branch could not be verified".to_owned())?;
    let head = git_output(&target, &["rev-parse", "HEAD"])
        .ok_or_else(|| "the Nemo job worktree head could not be verified".to_owned())?;
    let target_origin = canonical_git_origin(
        &git_output(&target, &["remote", "get-url", "origin"])
            .ok_or_else(|| "the Nemo job worktree origin could not be verified".to_owned())?,
    )
    .ok_or_else(|| "the Nemo job worktree origin is not canonical".to_owned())?;
    let source_common = git_common_dir(&source)
        .ok_or_else(|| "the Nemo repository common Git directory is unavailable".to_owned())?;
    let target_common = git_common_dir(&target)
        .ok_or_else(|| "the Nemo job worktree common Git directory is unavailable".to_owned())?;
    let base_is_ancestor = git_success(
        &target,
        &[
            "merge-base",
            "--is-ancestor",
            &request.common.repository.base_sha,
            &head,
        ],
    );
    if target_top.canonicalize().ok().as_ref() != Some(&target)
        || branch != request.common.repository.branch
        || target_origin != buzz_core::nemo::REPOSITORY
        || source_common != target_common
        || !base_is_ancestor
    {
        return Err("the Nemo job worktree no longer matches its signed repository scope".into());
    }
    Ok(target)
}

fn git_common_dir(root: &Path) -> Option<PathBuf> {
    PathBuf::from(git_output(
        root,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )?)
    .canonicalize()
    .ok()
}

fn checkout_matches(grant: &JobGrant, request: &JobRequest) -> Option<PathBuf> {
    let root = grant.checkout_root.canonicalize().ok()?;
    checkout_config_is_safe(&root)?;
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
    let bytes = git_run(root, args)?;
    let value = String::from_utf8(bytes).ok()?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn git_success(root: &Path, args: &[&str]) -> bool {
    git_run(root, args).is_some()
}

fn git_run(root: &Path, args: &[&str]) -> Option<Vec<u8>> {
    git_run_with_timeout(root, args, GIT_COMMAND_TIMEOUT)
}

fn git_success_with_timeout(root: &Path, args: &[&str], timeout: Duration) -> bool {
    git_run_with_timeout(root, args, timeout).is_some()
}

fn git_run_with_timeout(root: &Path, args: &[&str], timeout: Duration) -> Option<Vec<u8>> {
    let git = system_git()?;
    let mut command = Command::new(&git);
    command.arg("-C");
    #[cfg(windows)]
    command.arg(git_worktree_path_argument(root)?);
    #[cfg(not(windows))]
    command.arg(root);
    command
        .args(args)
        .env_clear()
        .env("PATH", system_path(&git)?)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GCM_INTERACTIVE", "never")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", null_device())
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env("GIT_ASKPASS", system_false())
        .env("SSH_ASKPASS", system_false());
    #[cfg(windows)]
    apply_windows_runtime_environment(&mut command)?;
    let output = match crate::bounded_command::output_with_limits(
        command,
        crate::bounded_command::Limits {
            timeout,
            stdout_bytes: MAX_GIT_OUTPUT_BYTES,
            stderr_bytes: MAX_GIT_OUTPUT_BYTES,
        },
    ) {
        Ok(output) => output,
        Err(_error) => {
            #[cfg(test)]
            eprintln!("bounded Git command failed before exit: {_error:?}");
            return None;
        }
    };
    if !output.status.success() {
        #[cfg(test)]
        eprintln!(
            "bounded Git command exited unsuccessfully: {:?}",
            output.status.code()
        );
        return None;
    }
    Some(output.stdout)
}

#[cfg(windows)]
fn git_worktree_path_argument(path: &Path) -> Option<String> {
    const VERBATIM_PREFIX: &str = r"\\?\";
    const VERBATIM_UNC_PREFIX: &str = r"\\?\UNC\";

    let value = path.to_str()?;
    if value
        .get(..VERBATIM_UNC_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(VERBATIM_UNC_PREFIX))
    {
        return Some(format!(r"\\{}", value.get(VERBATIM_UNC_PREFIX.len()..)?));
    }
    if let Some(value) = value.strip_prefix(VERBATIM_PREFIX) {
        let bytes = value.as_bytes();
        if bytes.len() < 3
            || !bytes[0].is_ascii_alphabetic()
            || bytes[1] != b':'
            || !matches!(bytes[2], b'\\' | b'/')
        {
            return None;
        }
        return Some(value.to_owned());
    }
    Some(value.to_owned())
}

#[cfg(not(windows))]
fn git_worktree_path_argument(path: &Path) -> Option<String> {
    path.to_str().map(str::to_owned)
}

#[cfg(windows)]
fn apply_windows_runtime_environment(command: &mut Command) -> Option<()> {
    let system_root = PathBuf::from(std::env::var_os("SystemRoot")?);
    if !system_root.is_absolute() || !system_root.is_dir() {
        return None;
    }
    command.env("SystemRoot", system_root);

    let temporary = std::env::temp_dir();
    if temporary.is_absolute() && temporary.is_dir() {
        command.env("TEMP", &temporary).env("TMP", temporary);
    }
    Some(())
}

fn checkout_config_is_safe(root: &Path) -> Option<()> {
    let local = git_output(
        root,
        &[
            "config",
            "--local",
            "--no-includes",
            "--name-only",
            "--null",
            "--list",
        ],
    )?;
    let keys = parse_config_keys(&local)?;
    if keys.iter().any(|key| dangerous_local_config_key(key)) {
        return None;
    }
    if keys.iter().any(|key| key == "extensions.worktreeconfig") {
        let worktree = git_output(
            root,
            &[
                "config",
                "--worktree",
                "--no-includes",
                "--name-only",
                "--null",
                "--list",
            ],
        )?;
        if parse_config_keys(&worktree)?
            .iter()
            .any(|key| dangerous_local_config_key(key))
        {
            return None;
        }
    }
    Some(())
}

fn parse_config_keys(output: &str) -> Option<Vec<String>> {
    let keys = output
        .split('\0')
        .filter(|value| !value.is_empty())
        .map(|value| value.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();
    keys.iter()
        .all(|key| !key.is_empty() && key.bytes().all(|byte| byte.is_ascii_graphic()))
        .then_some(keys)
}

fn dangerous_local_config_key(key: &str) -> bool {
    const EXACT: &[&str] = &[
        "core.alternaterefscommand",
        "core.askpass",
        "core.attributesfile",
        "core.editor",
        "core.fsmonitor",
        "core.gitproxy",
        "core.hookspath",
        "core.pager",
        "core.sshcommand",
        "core.worktree",
        "credential.helper",
        "credential.usehttppath",
        "gpg.format",
        "sequence.editor",
        "user.signingkey",
    ];
    EXACT.contains(&key)
        || (key.starts_with("remote.")
            && !matches!(key, "remote.origin.url" | "remote.origin.fetch"))
        || (key.starts_with("branch.") && !key.ends_with(".remote") && !key.ends_with(".merge"))
        || [
            "credential.",
            "diff.",
            "filter.",
            "gpg.",
            "http.",
            "https.",
            "include.",
            "includeif.",
            "merge.",
            "protocol.",
            "submodule.",
            "url.",
        ]
        .iter()
        .any(|prefix| key.starts_with(prefix))
        || matches!(
            key,
            "commit.gpgsign" | "tag.gpgsign" | "format.signoff" | "format.signingkey"
        )
}

#[cfg(unix)]
fn system_git() -> Option<PathBuf> {
    [Path::new("/usr/bin/git"), Path::new("/bin/git")]
        .into_iter()
        .find(|path| path.is_file())
        .and_then(|path| path.canonicalize().ok())
}

#[cfg(windows)]
fn system_git() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    for base in [
        std::env::var_os("ProgramFiles"),
        std::env::var_os("ProgramFiles(x86)"),
    ]
    .into_iter()
    .flatten()
    {
        candidates.push(PathBuf::from(&base).join("Git/cmd/git.exe"));
        candidates.push(PathBuf::from(base).join("Git/bin/git.exe"));
    }
    if let Some(local) = std::env::var_os("LOCALAPPDATA") {
        candidates.push(PathBuf::from(local).join("Programs/Git/cmd/git.exe"));
    }
    candidates.into_iter().find_map(|candidate| {
        (candidate.is_absolute() && candidate.is_file())
            .then(|| candidate.canonicalize().ok())
            .flatten()
    })
}

#[cfg(all(not(unix), not(windows)))]
fn system_git() -> Option<PathBuf> {
    None
}

#[cfg(unix)]
fn system_path(_git: &Path) -> Option<&'static str> {
    Some("/usr/bin:/bin")
}

#[cfg(windows)]
fn system_path(git: &Path) -> Option<OsString> {
    let mut paths = vec![git.parent()?.to_path_buf()];
    if let Some(root) = std::env::var_os("SystemRoot") {
        paths.push(PathBuf::from(root).join("System32"));
    }
    std::env::join_paths(paths).ok()
}

#[cfg(all(not(unix), not(windows)))]
fn system_path(_git: &Path) -> Option<&'static str> {
    None
}

#[cfg(unix)]
fn null_device() -> &'static str {
    "/dev/null"
}

#[cfg(windows)]
fn null_device() -> &'static str {
    // Git for Windows interprets its config paths through the MSYS layer;
    // the Win32 device name `NUL` is rejected as an invalid config file.
    "/dev/null"
}

#[cfg(all(not(unix), not(windows)))]
fn null_device() -> &'static str {
    "NUL"
}

#[cfg(unix)]
fn system_false() -> &'static str {
    "/usr/bin/false"
}

#[cfg(not(unix))]
fn system_false() -> &'static str {
    ""
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

    fn grant_json(checkout: &Path) -> String {
        serde_json::json!({
            "version": 1,
            "grants": [{
                "project_address": format!("30621:{}:nemo", "a".repeat(64)),
                "home_channel": "3580ca9b-47b4-4af9-b22a-1068778f26c6",
                "repository": "https://github.com/mysteropodes/nemo",
                "requester_pubkeys": ["c".repeat(64)],
                "capabilities": ["rust"],
                "path_prefixes": ["crates"],
                "base_sha": "b".repeat(40),
                "branch": "codex/a2a",
                "worktree_id": "a2a",
                "checkout_root": checkout,
            }]
        })
        .to_string()
    }

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
    fn absent_grants_do_not_fall_back_to_checkout_files() {
        let checkout = tempfile::tempdir().expect("checkout");
        std::fs::create_dir(checkout.path().join(".buzz")).expect("buzz dir");
        std::fs::write(
            checkout.path().join(".buzz/agent-job-grants.json"),
            grant_json(checkout.path()),
        )
        .expect("legacy grant file");

        assert!(GrantSet::load_from(checkout.path(), None, None)
            .expect("no grants")
            .is_empty());
    }

    #[test]
    fn inline_and_file_grant_sources_are_rejected_as_ambiguous() {
        let checkout = tempfile::tempdir().expect("checkout");
        let error = GrantSet::load_from(
            checkout.path(),
            Some(grant_json(checkout.path())),
            Some(PathBuf::from("/operator/grants.json")),
        )
        .expect_err("ambiguous grant sources must fail");
        assert!(error.to_string().contains("mutually exclusive"));
    }

    #[cfg(unix)]
    #[test]
    fn secure_grant_file_is_loaded_once_outside_checkout() {
        use std::os::unix::fs::PermissionsExt as _;

        let checkout = tempfile::tempdir().expect("checkout");
        let operator = tempfile::tempdir().expect("operator state");
        let path = operator
            .path()
            .canonicalize()
            .expect("canonical operator state")
            .join("grants.json");
        let original = grant_json(checkout.path());
        std::fs::write(&path, &original).expect("grant file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("private grant file");
        let ledger = operator
            .path()
            .canonicalize()
            .expect("canonical operator state")
            .join("ledger");

        let prepared = prepare_job_sources(
            checkout.path(),
            None,
            Some(path.clone()),
            Some(ledger.clone()),
            false,
        )
        .expect("secure source");
        std::fs::write(&path, "not-json").expect("mutate source after capture");
        assert_eq!(prepared.grants_json.as_deref(), Some(original.as_str()));
        assert_eq!(prepared.grant_source_file.as_deref(), Some(path.as_path()));
        assert_eq!(prepared.ledger_root.as_deref(), Some(ledger.as_path()));
        assert_eq!(
            prepared.protected_ledger_root.as_deref(),
            Some(ledger.as_path())
        );
        assert!(
            !GrantSet::from_json(prepared.grants_json.as_deref().unwrap())
                .expect("captured grants")
                .is_empty()
        );
    }

    #[cfg(unix)]
    #[test]
    fn insecure_checkout_local_and_symlinked_grant_files_fail_closed() {
        use std::os::unix::fs::{symlink, PermissionsExt as _};

        let checkout = tempfile::tempdir().expect("checkout");
        let inside = checkout.path().join("grants.json");
        std::fs::write(&inside, grant_json(checkout.path())).expect("inside grant");
        std::fs::set_permissions(&inside, std::fs::Permissions::from_mode(0o600))
            .expect("private inside grant");
        assert!(read_secure_grant_file(checkout.path(), &inside).is_err());

        let operator = tempfile::tempdir().expect("operator state");
        let operator = operator.path().canonicalize().expect("canonical operator");
        let target = operator.join("target.json");
        std::fs::write(&target, grant_json(checkout.path())).expect("target grant");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600))
            .expect("private target grant");
        let link = operator.join("linked.json");
        symlink(&target, &link).expect("grant symlink");
        assert!(read_secure_grant_file(checkout.path(), &link).is_err());

        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644))
            .expect("loose target mode");
        assert!(read_secure_grant_file(checkout.path(), &target).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn ledger_root_is_private_and_cannot_live_in_a_granted_checkout() {
        use std::os::unix::fs::PermissionsExt as _;

        let workspace = tempfile::tempdir().expect("model workspace");
        let checkout = tempfile::tempdir().expect("checkout");
        let inside = checkout.path().join(".buzz/ledger");
        assert!(prepare_private_ledger_root(
            workspace.path(),
            &inside,
            std::iter::once(checkout.path())
        )
        .is_err());

        let operator = tempfile::tempdir().expect("operator state");
        let outside = operator
            .path()
            .canonicalize()
            .expect("canonical operator")
            .join("private/ledger");
        let prepared = prepare_private_ledger_root(
            workspace.path(),
            &outside,
            std::iter::once(checkout.path()),
        )
        .expect("private ledger");
        assert_eq!(prepared, outside);
        assert_eq!(
            std::fs::metadata(prepared).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[test]
    fn exact_tuple_capability_and_path_are_required() {
        let checkout = tempfile::tempdir().expect("checkout");
        let raw = format!(
            r#"{{"version":1,"grants":[{{"project_address":"30621:{}:nemo","home_channel":"3580ca9b-47b4-4af9-b22a-1068778f26c6","repository":"https://github.com/mysteropodes/nemo","requester_pubkeys":["{}"],"capabilities":["rust"],"git_operations":["commit"],"path_prefixes":["crates"],"base_sha":"{}","branch":"codex/a2a","worktree_id":"a2a","checkout_root":{}}}]}}"#,
            "a".repeat(64),
            "c".repeat(64),
            "b".repeat(40),
            serde_json::to_string(checkout.path()).unwrap(),
        );
        let grants = GrantSet::from_json(&raw).expect("valid grants");
        let mut candidate = request();
        assert!(grants.capabilities_for(&candidate).is_some());
        assert_eq!(
            grants.git_operations_for(&candidate),
            Some(vec!["commit".into()])
        );
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
        assert!(grants.authorize_request(&candidate).unwrap().is_some());

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
        assert!(grants.authorize_request(&candidate).unwrap().is_none());
        run(&["checkout", "codex/a2a"]);
        std::fs::write(checkout.path().join("second.txt"), "second\n").expect("second");
        run(&["add", "second.txt"]);
        run(&["commit", "-m", "head drift"]);
        assert!(grants.authorize_request(&candidate).unwrap().is_none());
        run(&["reset", "--hard", &candidate.common.repository.base_sha]);
        run(&[
            "remote",
            "set-url",
            "origin",
            "https://github.com/mysteropodes/other.git",
        ]);
        assert!(grants.authorize_request(&candidate).unwrap().is_none());
    }

    #[test]
    fn dangerous_local_git_configuration_is_rejected() {
        let checkout = initialized_checkout();
        let head = git_output(checkout.path(), &["rev-parse", "HEAD"]).unwrap();
        let mut candidate = request();
        candidate.common.repository.base_sha = head;
        let raw = serde_json::json!({
            "version": 1,
            "grants": [{
                "project_address": candidate.common.project.address.clone(),
                "home_channel": candidate.common.project.home_channel.clone(),
                "repository": candidate.common.repository.canonical.clone(),
                "requester_pubkeys": [candidate.common.sender_pubkey.clone()],
                "capabilities": [candidate.capability.clone()],
                "path_prefixes": ["crates"],
                "base_sha": candidate.common.repository.base_sha.clone(),
                "branch": "codex/a2a",
                "worktree_id": "a2a",
                "checkout_root": checkout.path(),
            }]
        });
        let grants = GrantSet::from_json(&raw.to_string()).expect("valid checkout grant");
        assert!(grants.authorize_request(&candidate).unwrap().is_some());

        let run = |args: &[&str]| {
            assert!(Command::new(system_git().expect("system git"))
                .arg("-C")
                .arg(checkout.path())
                .args(args)
                .status()
                .expect("git config mutation")
                .success());
        };
        run(&["config", "--local", "credential.helper", "!/tmp/steal"]);
        assert!(grants.authorize_request(&candidate).unwrap().is_none());
        run(&["config", "--local", "--unset", "credential.helper"]);
        run(&[
            "remote",
            "add",
            "attacker",
            "https://github.com/attacker/other.git",
        ]);
        assert!(grants.authorize_request(&candidate).unwrap().is_none());
    }

    #[test]
    fn admission_git_is_absolute_and_does_not_search_the_checkout() {
        let git = system_git().expect("supported Unix test host has system Git");
        assert!(git.is_absolute());
        assert!(!git.starts_with(std::env::current_dir().expect("cwd")));
    }

    #[cfg(windows)]
    #[test]
    fn git_paths_remove_only_supported_windows_verbatim_prefixes() {
        assert_eq!(
            git_worktree_path_argument(Path::new(r"\\?\C:\Users\Buzz\nemo")),
            Some(r"C:\Users\Buzz\nemo".to_owned())
        );
        assert_eq!(
            git_worktree_path_argument(Path::new(r"\\?\UNC\server\share\nemo")),
            Some(r"\\server\share\nemo".to_owned())
        );
        assert!(git_worktree_path_argument(Path::new(r"\\?\Volume{abc}\nemo")).is_none());
    }

    #[test]
    fn managed_nemo_worktree_is_created_and_reused_after_worker_progress() {
        let harness = tempfile::tempdir().expect("harness");
        let checkout = harness.path().join("REPOS/nemo");
        std::fs::create_dir_all(&checkout).expect("checkout");
        let run = |root: &Path, args: &[&str]| {
            assert!(Command::new("git")
                .arg("-C")
                .arg(root)
                .args(args)
                .status()
                .expect("Git fixture")
                .success());
        };
        run(&checkout, &["init", "--quiet"]);
        run(&checkout, &["config", "user.name", "Buzz Test"]);
        run(
            &checkout,
            &["config", "user.email", "buzz-test@example.invalid"],
        );
        std::fs::write(checkout.join("fixture.txt"), "fixture\n").expect("fixture");
        run(&checkout, &["add", "fixture.txt"]);
        run(&checkout, &["commit", "--quiet", "-m", "fixture"]);
        run(
            &checkout,
            &[
                "remote",
                "add",
                "origin",
                "https://github.com/mysteropodes/nemo.git",
            ],
        );
        let base = git_output(&checkout, &["rev-parse", "HEAD"]).expect("base");
        let mut candidate = request();
        candidate.common.project.address = buzz_core::nemo::PROJECT_ADDRESS.into();
        candidate.common.project.home_channel = buzz_core::nemo::HOME_CHANNEL.into();
        candidate.common.repository.canonical = buzz_core::nemo::REPOSITORY.into();
        candidate.common.repository.base_sha = base;
        candidate.common.repository.worktree_id = "worker_2".into();
        candidate.common.repository.branch = "codex/worker_2".into();
        candidate.common.repository.paths = vec!["new/source.rs".into()];
        candidate.common.sender_pubkey = nostr::Keys::generate().public_key().to_hex();

        let grants = GrantSet::load_with_nemo(harness.path(), None, None, true)
            .expect("managed Nemo grants");
        let first = grants
            .authorize_request(&candidate)
            .expect("first admission")
            .expect("managed grant")
            .checkout_root;
        assert!(first.ends_with("nemo-worktrees/worker_2"));

        std::fs::write(first.join("progress.txt"), "progress\n").expect("progress");
        run(&first, &["add", "progress.txt"]);
        run(&first, &["commit", "--quiet", "-m", "worker progress"]);
        let progressed_head = git_output(&first, &["rev-parse", "HEAD"]).expect("progress head");

        let resumed = grants
            .authorize_request(&candidate)
            .expect("resume admission")
            .expect("managed grant")
            .checkout_root;
        assert_eq!(resumed, first);
        assert_eq!(
            git_output(&resumed, &["rev-parse", "HEAD"]).as_deref(),
            Some(progressed_head.as_str()),
            "resume must preserve legitimate worker commits"
        );
    }
}
