//! Desktop-managed, project-scoped A2A grant settings.
//!
//! The persisted document is the exact version-1 schema consumed by both
//! `buzz-acp` and the trusted surface in `buzz-dev-mcp`. It contains public
//! identities and local checkout coordinates only; signing keys never enter
//! this module.

use atomic_write_file::AtomicWriteFile;
use hmac::{digest::KeyInit, Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    process::Command,
    sync::{Mutex, OnceLock},
};
use tauri::State;
use zeroize::Zeroizing;

use crate::{app_state::AppState, managed_agents::RelayAgentInfo};

use super::{
    agent_discovery::list_relay_agents_for_selection, project_repo_paths::canonical_repos_roots,
};

const GRANT_SCHEMA_VERSION: u32 = 1;
const AUTHORITY_SCHEMA_VERSION: u32 = 1;
const AUTHORITY_KEY: &str = "a2a.grant-authority.v1";
const AUTHORITY_DOMAIN: &[u8] = b"buzz.a2a-grants.v1\0";
const MAX_GRANTS: usize = 128;
const MAX_CAPABILITIES_PER_GRANT: usize = 64;
const MAX_DOCUMENT_BYTES: usize = 512 * 1024;
const MAX_REPOSITORIES_SCANNED: usize = 512;
const MAX_VALUES_PER_GRANT: usize = 128;
const MAX_GIT_OUTPUT_BYTES: usize = 16 * 1024;

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aGrantScopeInput {
    repos_dir: Option<String>,
    project_dtag: String,
    project_address: String,
    home_channel: String,
    repository: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aGrantSelectionInput {
    scope: A2aGrantScopeInput,
    checkout_root: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aGrantUpsertInput {
    scope: A2aGrantScopeInput,
    checkout_root: String,
    expected_branch: String,
    expected_base_sha: String,
    peer_pubkey: String,
    capability: String,
    path_prefixes: Vec<String>,
    worktree_id: String,
    expected_relay_url: String,
    expected_signer_pubkey: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aGrantRemoveInput {
    scope: A2aGrantScopeInput,
    checkout_root: String,
    grant_id: String,
    expected_relay_url: String,
    expected_signer_pubkey: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct A2aCheckoutInfo {
    path: String,
    branch: String,
    base_sha: String,
    suggested_worktree_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct A2aGrantView {
    id: String,
    requester_pubkeys: Vec<String>,
    capabilities: Vec<String>,
    path_prefixes: Vec<String>,
    worktree_id: String,
    status: &'static str,
    status_message: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct A2aGrantState {
    storage: String,
    checkout: A2aCheckoutInfo,
    grants: Vec<A2aGrantView>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GrantDocument {
    version: u32,
    grants: Vec<StoredGrant>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredGrant {
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

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GrantAuthorityState {
    version: u32,
    secret_hex: String,
    current: AuthoritySlot,
    pending: Option<AuthoritySlot>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AuthoritySlot {
    revision: u64,
    mac: String,
}

fn grant_store_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

trait AuthorityStore {
    fn load(&self) -> Result<Option<Zeroizing<String>>, String>;
    fn store_verified(&self, value: &str) -> Result<(), String>;
}

struct KeychainAuthorityStore;

impl AuthorityStore for KeychainAuthorityStore {
    fn load(&self) -> Result<Option<Zeroizing<String>>, String> {
        crate::secret_store::SecretStore::shared(crate::app_state::keyring_service())
            .load(AUTHORITY_KEY)
            .map(|value| value.map(Zeroizing::new))
    }

    fn store_verified(&self, value: &str) -> Result<(), String> {
        let store = crate::secret_store::SecretStore::shared(crate::app_state::keyring_service());
        store.store(AUTHORITY_KEY, value)?;
        match store.verify_stored_raw(AUTHORITY_KEY, value)? {
            true => Ok(()),
            false => Err("OS credential vault did not retain the A2A authority state".into()),
        }
    }
}

/// Return the authenticated version-1 grant document for a managed child.
///
/// The caller sends these already-verified bytes through the private startup
/// pipe. A managed model never receives an authoritative filesystem path or
/// the credential-vault MAC key.
pub(crate) fn load_managed_agent_grants_json() -> Result<Zeroizing<String>, String> {
    if !cfg!(feature = "system-keyring") {
        return serde_json::to_string(&empty_document())
            .map(Zeroizing::new)
            .map_err(|error| format!("encode disabled A2A grant set: {error}"));
    }
    let _guard = grant_store_lock()
        .lock()
        .map_err(|_| "A2A grant store lock is unavailable".to_string())?;
    let path = grant_file_path()?;
    let (document, _) = load_authorized_document(&KeychainAuthorityStore, &path)?;
    serde_json::to_string(&document)
        .map(Zeroizing::new)
        .map_err(|error| format!("encode authenticated A2A grants: {error}"))
}

#[tauri::command]
pub async fn list_a2a_checkouts(scope: A2aGrantScopeInput) -> Result<Vec<A2aCheckoutInfo>, String> {
    tauri::async_runtime::spawn_blocking(move || list_checkouts(&scope))
        .await
        .map_err(|error| format!("A2A checkout scan failed: {error}"))?
}

#[tauri::command]
pub async fn get_a2a_grants(input: A2aGrantSelectionInput) -> Result<A2aGrantState, String> {
    tauri::async_runtime::spawn_blocking(move || {
        get_grant_state(&input.scope, &input.checkout_root)
    })
    .await
    .map_err(|error| format!("A2A grant read failed: {error}"))?
}

#[tauri::command]
pub async fn upsert_a2a_grant(
    input: A2aGrantUpsertInput,
    state: State<'_, AppState>,
) -> Result<A2aGrantState, String> {
    let _workspace_guard = state.workspace_apply_lock.clone().lock_owned().await;
    let relay_base = crate::relay::relay_api_base_url_with_override(&state);
    crate::relay::assert_expected_relay_scope(Some(&input.expected_relay_url), &relay_base)?;
    let signer = state.signing_keys()?.public_key().to_hex();
    crate::relay::assert_expected_signer(Some(&input.expected_signer_pubkey), &signer)?;
    require_workspace_project_scope(
        crate::relay::relay_ws_url_with_override(&state),
        &input.scope,
    )
    .await?;
    validate_pubkey(&input.peer_pubkey, "peer_pubkey")?;
    let requested = HashSet::from([input.peer_pubkey.clone()]);
    let peers =
        list_relay_agents_for_selection(&state, Some(&requested), Some(&input.scope.home_channel))
            .await?;
    verify_selected_peer(&peers, &input.peer_pubkey, &input.scope.home_channel)?;

    tauri::async_runtime::spawn_blocking(move || upsert_grant(input))
        .await
        .map_err(|error| format!("A2A grant write failed: {error}"))?
}

#[tauri::command]
pub async fn remove_a2a_grant(
    input: A2aGrantRemoveInput,
    state: State<'_, AppState>,
) -> Result<A2aGrantState, String> {
    let _workspace_guard = state.workspace_apply_lock.clone().lock_owned().await;
    let relay_base = crate::relay::relay_api_base_url_with_override(&state);
    crate::relay::assert_expected_relay_scope(Some(&input.expected_relay_url), &relay_base)?;
    let signer = state.signing_keys()?.public_key().to_hex();
    crate::relay::assert_expected_signer(Some(&input.expected_signer_pubkey), &signer)?;
    require_workspace_project_scope(
        crate::relay::relay_ws_url_with_override(&state),
        &input.scope,
    )
    .await?;
    tauri::async_runtime::spawn_blocking(move || remove_grant(input))
        .await
        .map_err(|error| format!("A2A grant removal failed: {error}"))?
}

async fn require_workspace_project_scope(
    relay_url: String,
    scope: &A2aGrantScopeInput,
) -> Result<(), String> {
    let project = tauri::async_runtime::spawn_blocking(move || {
        crate::managed_agents::load_workspace_project_for_relay(&relay_url)
    })
    .await
    .map_err(|error| format!("Workspace Project read failed: {error}"))??
    .ok_or_else(|| "configure a Workspace Project before changing A2A grants".to_string())?;
    if !workspace_project_matches_scope(&project, scope) {
        return Err(
            "the requested grant does not match the active community's Workspace Project".into(),
        );
    }
    Ok(())
}

fn workspace_project_matches_scope(
    project: &crate::managed_agents::WorkspaceProject,
    scope: &A2aGrantScopeInput,
) -> bool {
    project.project_address == scope.project_address
        && project.home_channel == scope.home_channel
        && project.repository == scope.repository
}

fn list_checkouts(scope: &A2aGrantScopeInput) -> Result<Vec<A2aCheckoutInfo>, String> {
    validate_scope(scope)?;
    let mut scanned = 0usize;
    let mut seen = HashSet::new();
    let mut matches = Vec::new();
    for repos_root in canonical_repos_roots(scope.repos_dir.as_deref())? {
        for entry in std::fs::read_dir(&repos_root)
            .map_err(|error| format!("read repositories directory: {error}"))?
        {
            scanned += 1;
            if scanned > MAX_REPOSITORIES_SCANNED {
                return Err(format!(
                    "repository scan exceeded {MAX_REPOSITORIES_SCANNED} entries; choose a narrower repositories folder"
                ));
            }
            let Ok(entry) = entry else { continue };
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_dir() || file_type.is_symlink() {
                continue;
            }
            let Ok(root) = entry.path().canonicalize() else {
                continue;
            };
            if root.parent() != Some(repos_root.as_path()) || !seen.insert(root.clone()) {
                continue;
            }
            let Ok(checkout) = inspect_checkout(&root) else {
                continue;
            };
            if checkout.repository == scope.repository {
                matches.push(checkout.view());
            }
        }
    }
    matches.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(matches)
}

fn get_grant_state(
    scope: &A2aGrantScopeInput,
    checkout_root: &str,
) -> Result<A2aGrantState, String> {
    ensure_default_grant_source()?;
    validate_scope(scope)?;
    let checkout = selected_checkout(scope, checkout_root)?;
    let path = grant_file_path()?;
    let _guard = grant_store_lock()
        .lock()
        .map_err(|_| "A2A grant store lock is unavailable".to_string())?;
    let (document, _) = load_authorized_document(&KeychainAuthorityStore, &path)?;
    Ok(state_from(document, scope, checkout, &path))
}

fn upsert_grant(input: A2aGrantUpsertInput) -> Result<A2aGrantState, String> {
    ensure_default_grant_source()?;
    validate_scope(&input.scope)?;
    validate_new_capability(&input.capability)?;
    validate_worktree_id(&input.worktree_id)?;
    validate_pubkey(&input.peer_pubkey, "peer_pubkey")?;
    let checkout = selected_checkout(&input.scope, &input.checkout_root)?;
    if checkout.branch != input.expected_branch || checkout.base_sha != input.expected_base_sha {
        return Err("the selected checkout changed; refresh before saving the grant".into());
    }
    validate_safe_paths(&checkout.root, &input.path_prefixes)?;

    let path = grant_file_path()?;
    let _guard = grant_store_lock()
        .lock()
        .map_err(|_| "A2A grant store lock is unavailable".to_string())?;
    let (mut document, authority) = load_authorized_document(&KeychainAuthorityStore, &path)?;
    let replacement = StoredGrant {
        project_address: input.scope.project_address.clone(),
        home_channel: input.scope.home_channel.clone(),
        repository: input.scope.repository.clone(),
        requester_pubkeys: vec![input.peer_pubkey.clone()],
        capabilities: vec![input.capability.clone()],
        path_prefixes: input.path_prefixes,
        base_sha: checkout.base_sha.clone(),
        branch: checkout.branch.clone(),
        worktree_id: input.worktree_id.clone(),
        checkout_root: checkout.root.clone(),
    };
    let mut replaced = false;
    for grant in &mut document.grants {
        if grant.project_address == replacement.project_address
            && grant.home_channel == replacement.home_channel
            && grant.repository == replacement.repository
            && grant.checkout_root == replacement.checkout_root
            && grant.requester_pubkeys == replacement.requester_pubkeys
            && grant.capabilities == replacement.capabilities
            && grant.worktree_id == replacement.worktree_id
        {
            *grant = replacement.clone();
            replaced = true;
            break;
        }
    }
    if !replaced {
        if document.grants.len() >= MAX_GRANTS {
            return Err(format!("at most {MAX_GRANTS} A2A grants are allowed"));
        }
        document.grants.push(replacement);
    }
    validate_document(&mut document)?;
    write_authorized_document(&KeychainAuthorityStore, &path, &document, authority)?;
    Ok(state_from(document, &input.scope, checkout, &path))
}

fn remove_grant(input: A2aGrantRemoveInput) -> Result<A2aGrantState, String> {
    ensure_default_grant_source()?;
    validate_scope(&input.scope)?;
    if !is_lower_hex(&input.grant_id, 64) {
        return Err("grant_id must be a canonical SHA-256 identifier".into());
    }
    let checkout = selected_checkout(&input.scope, &input.checkout_root)?;
    let path = grant_file_path()?;
    let _guard = grant_store_lock()
        .lock()
        .map_err(|_| "A2A grant store lock is unavailable".to_string())?;
    let (mut document, authority) = load_authorized_document(&KeychainAuthorityStore, &path)?;
    let matching_indices = document
        .grants
        .iter()
        .enumerate()
        .filter(|(_, grant)| {
            grant_matches_scope(grant, &input.scope)
                && grant.checkout_root == checkout.root
                && grant_id(grant).is_ok_and(|id| id == input.grant_id)
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let [index] = matching_indices.as_slice() else {
        return Err("the selected A2A grant no longer exists or is ambiguous".into());
    };
    document.grants.remove(*index);
    write_authorized_document(&KeychainAuthorityStore, &path, &document, authority)?;
    Ok(state_from(document, &input.scope, checkout, &path))
}

fn verify_selected_peer(
    agents: &[RelayAgentInfo],
    selected_pubkey: &str,
    home_channel: &str,
) -> Result<(), String> {
    let matches = agents
        .iter()
        .filter(|agent| agent.pubkey == selected_pubkey)
        .collect::<Vec<_>>();
    let [agent] = matches.as_slice() else {
        return Err("the selected peer is not a verified relay agent".into());
    };
    let owner = agent
        .owner_pubkey
        .as_deref()
        .ok_or_else(|| "the selected peer has no verified owner identity".to_string())?;
    validate_pubkey(owner, "peer owner pubkey")?;
    if !agent
        .channel_ids
        .iter()
        .any(|channel| channel == home_channel)
    {
        return Err("the selected peer is not a member of this project's home channel".into());
    }
    Ok(())
}

#[derive(Clone)]
struct InspectedCheckout {
    root: PathBuf,
    repository: String,
    branch: String,
    base_sha: String,
}

impl InspectedCheckout {
    fn view(&self) -> A2aCheckoutInfo {
        A2aCheckoutInfo {
            path: self.root.display().to_string(),
            branch: self.branch.clone(),
            base_sha: self.base_sha.clone(),
            suggested_worktree_id: suggested_worktree_id(&self.root, &self.base_sha),
        }
    }
}

fn selected_checkout(
    scope: &A2aGrantScopeInput,
    selected_root: &str,
) -> Result<InspectedCheckout, String> {
    let selected = PathBuf::from(selected_root);
    if !selected.is_absolute() {
        return Err("checkout_root must be an absolute local path".into());
    }
    let selected = selected
        .canonicalize()
        .map_err(|_| "the selected checkout is unavailable".to_string())?;
    let allowed = canonical_repos_roots(scope.repos_dir.as_deref())?
        .into_iter()
        .any(|root| selected.parent() == Some(root.as_path()));
    if !allowed {
        return Err("the selected checkout is outside the configured repositories folder".into());
    }
    let checkout = inspect_checkout(&selected)?;
    if checkout.repository != scope.repository {
        return Err("the selected checkout origin does not match the project repository".into());
    }
    Ok(checkout)
}

fn inspect_checkout(root: &Path) -> Result<InspectedCheckout, String> {
    let metadata = root
        .symlink_metadata()
        .map_err(|_| "the local checkout is unavailable".to_string())?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("the local checkout must be a real directory".into());
    }
    let dot_git = root.join(".git");
    let git_metadata = dot_git
        .symlink_metadata()
        .map_err(|_| "the local checkout has no Git metadata".to_string())?;
    if git_metadata.file_type().is_symlink() || !(git_metadata.is_file() || git_metadata.is_dir()) {
        return Err("the local checkout has unsafe Git metadata".into());
    }
    let top = PathBuf::from(git_output(root, &["rev-parse", "--show-toplevel"])?);
    if top.canonicalize().ok().as_deref() != Some(root) {
        return Err("the selected path is not the Git checkout root".into());
    }
    let repository = canonical_github_remote(&git_output(root, &["remote", "get-url", "origin"])?)?;
    let branch = git_output(root, &["symbolic-ref", "--quiet", "--short", "HEAD"])
        .map_err(|_| "detached checkouts cannot receive A2A grants".to_string())?;
    if !valid_branch(&branch) {
        return Err("the checkout branch is not a canonical branch name".into());
    }
    let base_sha = git_output(root, &["rev-parse", "HEAD"])?;
    if !matches!(base_sha.len(), 40 | 64) || !is_lower_hex(&base_sha, base_sha.len()) {
        return Err("the checkout HEAD is not a canonical commit identifier".into());
    }
    Ok(InspectedCheckout {
        root: root.to_owned(),
        repository,
        branch,
        base_sha,
    })
}

fn git_output(root: &Path, args: &[&str]) -> Result<String, String> {
    let null_config = if cfg!(windows) { "NUL" } else { "/dev/null" };
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .env_clear()
        .env(
            "PATH",
            std::env::var_os("PATH").unwrap_or_else(|| "/usr/bin:/bin".into()),
        )
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", null_config)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_PAGER", "cat")
        .env("LC_ALL", "C")
        .output()
        .map_err(|error| format!("inspect local Git checkout: {error}"))?;
    if output.stdout.len() > MAX_GIT_OUTPUT_BYTES || output.stderr.len() > MAX_GIT_OUTPUT_BYTES {
        return Err("local Git inspection exceeded its output limit".into());
    }
    if !output.status.success() || !output.stderr.is_empty() {
        return Err("local Git inspection failed".into());
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|_| "local Git output was not UTF-8".into())
}

fn canonical_github_remote(value: &str) -> Result<String, String> {
    let trimmed = value.trim().trim_end_matches('/');
    let path = if let Some(path) = trimmed.strip_prefix("git@github.com:") {
        path.to_owned()
    } else {
        let url = url::Url::parse(trimmed)
            .map_err(|_| "repository must be a canonical GitHub URL".to_string())?;
        if url.scheme() != "https"
            || url.host_str() != Some("github.com")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err("repository must be a canonical GitHub HTTPS or SSH remote".into());
        }
        url.path().trim_start_matches('/').to_owned()
    };
    let path = path.strip_suffix(".git").unwrap_or(&path);
    let parts = path.split('/').collect::<Vec<_>>();
    if parts.len() != 2 || parts.iter().any(|part| part.is_empty()) {
        return Err("repository must identify one GitHub owner/repository".into());
    }
    Ok(format!(
        "https://github.com/{}/{}",
        parts[0].to_ascii_lowercase(),
        parts[1].to_ascii_lowercase()
    ))
}

fn validate_scope(scope: &A2aGrantScopeInput) -> Result<(), String> {
    let mut project = scope.project_address.splitn(3, ':');
    if project.next() != Some("30621")
        || project
            .next()
            .is_none_or(|owner| validate_pubkey(owner, "project owner").is_err())
        || project.next() != Some(scope.project_dtag.as_str())
        || !valid_token(&scope.project_dtag)
    {
        return Err("project_address must match the selected canonical NIP-MP project".into());
    }
    let channel = uuid::Uuid::parse_str(&scope.home_channel)
        .map_err(|_| "project home channel must be a UUID".to_string())?;
    if channel.is_nil() || channel.to_string() != scope.home_channel {
        return Err("project home channel must be a canonical non-nil UUID".into());
    }
    if canonical_github_remote(&scope.repository)? != scope.repository {
        return Err("repository must be canonical lowercase https://github.com/owner/repo".into());
    }
    Ok(())
}

fn validate_document(document: &mut GrantDocument) -> Result<(), String> {
    if document.version != GRANT_SCHEMA_VERSION {
        return Err(format!(
            "A2A grant file version must be {GRANT_SCHEMA_VERSION}"
        ));
    }
    if document.grants.len() > MAX_GRANTS {
        return Err(format!("at most {MAX_GRANTS} A2A grants are allowed"));
    }
    for grant in &mut document.grants {
        validate_stored_grant(grant)?;
    }
    Ok(())
}

fn validate_stored_grant(grant: &mut StoredGrant) -> Result<(), String> {
    let scope = A2aGrantScopeInput {
        repos_dir: None,
        project_dtag: grant
            .project_address
            .splitn(3, ':')
            .nth(2)
            .unwrap_or("")
            .to_owned(),
        project_address: grant.project_address.clone(),
        home_channel: grant.home_channel.clone(),
        repository: grant.repository.clone(),
    };
    validate_scope(&scope)?;
    unique_valid(
        &grant.requester_pubkeys,
        |value| validate_pubkey(value, "requester").is_ok(),
        "requester_pubkeys",
    )?;
    unique_valid_bounded(
        &grant.capabilities,
        MAX_CAPABILITIES_PER_GRANT,
        valid_token,
        "capabilities",
    )?;
    unique_valid(&grant.path_prefixes, valid_relative_path, "path_prefixes")?;
    if !matches!(grant.base_sha.len(), 40 | 64)
        || !is_lower_hex(&grant.base_sha, grant.base_sha.len())
    {
        return Err("base_sha must be a canonical commit identifier".into());
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
        .map_err(|_| "checkout_root must be an absolute existing directory".to_string())?;
    Ok(())
}

fn unique_valid(
    values: &[String],
    valid: impl Fn(&str) -> bool,
    label: &str,
) -> Result<(), String> {
    unique_valid_bounded(values, MAX_VALUES_PER_GRANT, valid, label)
}

fn unique_valid_bounded(
    values: &[String],
    maximum: usize,
    valid: impl Fn(&str) -> bool,
    label: &str,
) -> Result<(), String> {
    let mut seen = HashSet::new();
    if values.is_empty()
        || values.len() > maximum
        || values
            .iter()
            .any(|value| !valid(value) || !seen.insert(value))
    {
        return Err(format!(
            "{label} must be non-empty, unique, bounded, and canonical"
        ));
    }
    Ok(())
}

fn validate_safe_paths(root: &Path, prefixes: &[String]) -> Result<(), String> {
    unique_valid(prefixes, valid_relative_path, "path_prefixes")?;
    let root = root
        .canonicalize()
        .map_err(|_| "the selected checkout is unavailable".to_string())?;
    for prefix in prefixes {
        let mut cursor = root.clone();
        for component in Path::new(prefix).components() {
            let Component::Normal(name) = component else {
                return Err(format!(
                    "allowed path `{prefix}` is not repository-relative"
                ));
            };
            cursor.push(name);
            let metadata = cursor
                .symlink_metadata()
                .map_err(|_| format!("allowed path `{prefix}` must already exist"))?;
            if metadata.file_type().is_symlink() {
                return Err(format!("allowed path `{prefix}` crosses a symbolic link"));
            }
            let canonical = cursor
                .canonicalize()
                .map_err(|_| format!("allowed path `{prefix}` is unavailable"))?;
            if !canonical.starts_with(&root) {
                return Err(format!("allowed path `{prefix}` escapes the checkout"));
            }
        }
    }
    Ok(())
}

fn validate_new_capability(value: &str) -> Result<(), String> {
    let mut bytes = value.bytes();
    let valid = value.len() <= 64
        && bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        });
    if valid {
        Ok(())
    } else {
        Err("capability must start with a lowercase letter and use only lowercase letters, numbers, dot, underscore, or hyphen (64 bytes max)".into())
    }
}

fn validate_worktree_id(value: &str) -> Result<(), String> {
    let mut bytes = value.bytes();
    let valid = value.len() <= 128
        && bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err("worktree_id must start with a letter or number and use only letters, numbers, dot, underscore, or hyphen (128 bytes max)".into())
    }
}

fn validate_pubkey(value: &str, label: &str) -> Result<(), String> {
    let parsed = nostr::PublicKey::from_hex(value)
        .map_err(|_| format!("{label} must be a canonical lowercase public key"))?;
    if parsed.to_hex() != value {
        return Err(format!("{label} must be a canonical lowercase public key"));
    }
    Ok(())
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
        && value.len() <= 1024
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains("//")
        && !value.contains('\\')
        && value.split('/').all(|component| {
            !component.is_empty()
                && component != "."
                && component != ".."
                && !component.eq_ignore_ascii_case(".git")
        })
}

fn is_lower_hex(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn suggested_worktree_id(root: &Path, base_sha: &str) -> String {
    let candidate = root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("")
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    let candidate = candidate.trim_matches('-');
    if validate_worktree_id(candidate).is_ok() {
        candidate.to_owned()
    } else {
        format!("checkout-{}", &base_sha[..8.min(base_sha.len())])
    }
}

fn ensure_default_grant_source() -> Result<(), String> {
    let inline = std::env::var("BUZZ_ACP_JOB_GRANTS_JSON")
        .ok()
        .is_some_and(|value| !value.trim().is_empty());
    let file = std::env::var_os("BUZZ_ACP_JOB_GRANTS_FILE").is_some_and(|value| !value.is_empty());
    if inline || file {
        return Err("A2A grants are controlled by a process-level override; remove it and restart Buzz to use Settings".into());
    }
    Ok(())
}

fn grant_file_path() -> Result<PathBuf, String> {
    crate::managed_agents::a2a_grants_file_path()
        .ok_or_else(|| "Buzz could not resolve its local A2A settings folder".to_string())
}

fn empty_document() -> GrantDocument {
    GrantDocument {
        version: GRANT_SCHEMA_VERSION,
        grants: Vec::new(),
    }
}

fn read_document_file(path: &Path) -> Result<Option<GrantDocument>, String> {
    let metadata = match path.symlink_metadata() {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("inspect {}: {error}", path.display())),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err("A2A grant file must be a regular file".into());
    }
    if metadata.len() > MAX_DOCUMENT_BYTES as u64 {
        return Err(format!(
            "A2A grant file exceeds the {MAX_DOCUMENT_BYTES}-byte limit"
        ));
    }

    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|error| format!("open authenticated A2A grants: {error}"))?;
    let mut raw = Vec::new();
    file.take(MAX_DOCUMENT_BYTES as u64 + 1)
        .read_to_end(&mut raw)
        .map_err(|error| format!("read authenticated A2A grants: {error}"))?;
    if raw.len() > MAX_DOCUMENT_BYTES {
        return Err(format!(
            "A2A grant file exceeds the {MAX_DOCUMENT_BYTES}-byte limit"
        ));
    }
    let mut document: GrantDocument = serde_json::from_slice(&raw)
        .map_err(|error| format!("parse authenticated A2A grants: {error}"))?;
    validate_document(&mut document)?;
    Ok(Some(document))
}

fn canonical_document(document: &GrantDocument) -> Result<Vec<u8>, String> {
    let encoded = serde_json::to_vec(document)
        .map_err(|error| format!("encode authenticated A2A grants: {error}"))?;
    if encoded.len() > MAX_DOCUMENT_BYTES {
        return Err(format!(
            "A2A grant document exceeds the {MAX_DOCUMENT_BYTES}-byte limit"
        ));
    }
    Ok(encoded)
}

fn slot_for(
    secret_hex: &str,
    revision: u64,
    document: &GrantDocument,
) -> Result<AuthoritySlot, String> {
    let mac = authority_mac(secret_hex, revision, document)?;
    Ok(AuthoritySlot {
        revision,
        mac: hex::encode(mac.finalize().into_bytes()),
    })
}

fn authority_mac(
    secret_hex: &str,
    revision: u64,
    document: &GrantDocument,
) -> Result<Hmac<Sha256>, String> {
    let secret = Zeroizing::new(
        hex::decode(secret_hex).map_err(|_| "A2A authority key is malformed".to_string())?,
    );
    if secret.len() != 32 || !is_lower_hex(secret_hex, 64) {
        return Err("A2A authority key is malformed".into());
    }
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&secret)
        .map_err(|_| "A2A authority key is malformed".to_string())?;
    mac.update(AUTHORITY_DOMAIN);
    mac.update(&revision.to_be_bytes());
    mac.update(&canonical_document(document)?);
    Ok(mac)
}

fn slot_matches(
    secret_hex: &str,
    slot: &AuthoritySlot,
    document: &GrantDocument,
) -> Result<bool, String> {
    let supplied = hex::decode(&slot.mac)
        .map_err(|_| "OS credential vault contains malformed A2A authority state".to_string())?;
    if supplied.len() != 32 {
        return Ok(false);
    }
    Ok(authority_mac(secret_hex, slot.revision, document)?
        .verify_slice(&supplied)
        .is_ok())
}

fn validate_authority(state: &GrantAuthorityState) -> Result<(), String> {
    if state.version != AUTHORITY_SCHEMA_VERSION
        || !is_lower_hex(&state.secret_hex, 64)
        || state.current.revision == 0
        || !is_lower_hex(&state.current.mac, 64)
    {
        return Err("OS credential vault contains malformed A2A authority state".into());
    }
    if let Some(pending) = &state.pending {
        if pending.revision != state.current.revision.saturating_add(1)
            || !is_lower_hex(&pending.mac, 64)
        {
            return Err("OS credential vault contains malformed pending A2A state".into());
        }
    }
    Ok(())
}

fn persist_authority(
    store: &impl AuthorityStore,
    state: &GrantAuthorityState,
) -> Result<(), String> {
    validate_authority(state)?;
    let encoded = Zeroizing::new(
        serde_json::to_string(state)
            .map_err(|error| format!("encode A2A authority state: {error}"))?,
    );
    store.store_verified(&encoded)
}

fn initialize_authority(
    store: &impl AuthorityStore,
    path: &Path,
) -> Result<(GrantDocument, GrantAuthorityState), String> {
    let document = read_document_file(path)?.unwrap_or_else(empty_document);
    if !document.grants.is_empty() {
        return Err(
            "An unauthenticated A2A grant file already exists. Remove it before enabling Settings-managed grants."
                .into(),
        );
    }
    let mut secret = Zeroizing::new([0u8; 32]);
    getrandom::getrandom(secret.as_mut())
        .map_err(|error| format!("generate A2A authority key: {error}"))?;
    let secret_hex = Zeroizing::new(hex::encode(secret.as_ref()));
    let state = GrantAuthorityState {
        version: AUTHORITY_SCHEMA_VERSION,
        current: slot_for(&secret_hex, 1, &document)?,
        secret_hex: secret_hex.to_string(),
        pending: None,
    };
    write_document_file(path, &document)?;
    persist_authority(store, &state)?;
    Ok((document, state))
}

fn load_authorized_document(
    store: &impl AuthorityStore,
    path: &Path,
) -> Result<(GrantDocument, GrantAuthorityState), String> {
    let Some(raw_state) = store.load()? else {
        return initialize_authority(store, path);
    };
    let mut state: GrantAuthorityState = serde_json::from_str(&raw_state)
        .map_err(|_| "OS credential vault contains malformed A2A authority state".to_string())?;
    validate_authority(&state)?;

    let document = match read_document_file(path)? {
        Some(document) => document,
        None => {
            let empty = empty_document();
            if !slot_matches(&state.secret_hex, &state.current, &empty)? {
                return Err(
                    "Authenticated A2A grant file is missing; access remains disabled".into(),
                );
            }
            write_document_file(path, &empty)?;
            empty
        }
    };

    let pending_matches = match state.pending.as_ref() {
        Some(pending) => slot_matches(&state.secret_hex, pending, &document)?,
        None => false,
    };
    if pending_matches {
        state.current = state
            .pending
            .take()
            .ok_or_else(|| "pending A2A authority state disappeared".to_string())?;
        persist_authority(store, &state)?;
        return Ok((document, state));
    }
    if slot_matches(&state.secret_hex, &state.current, &document)? {
        if state.pending.take().is_some() {
            persist_authority(store, &state)?;
        }
        return Ok((document, state));
    }
    Err("A2A grant file was modified or rolled back outside Buzz; access remains disabled".into())
}

fn write_authorized_document(
    store: &impl AuthorityStore,
    path: &Path,
    document: &GrantDocument,
    mut state: GrantAuthorityState,
) -> Result<(), String> {
    if slot_matches(&state.secret_hex, &state.current, document)? {
        return Ok(());
    }
    let revision = state
        .current
        .revision
        .checked_add(1)
        .ok_or_else(|| "A2A authority revision is exhausted".to_string())?;
    let pending = slot_for(&state.secret_hex, revision, document)?;
    state.pending = Some(pending.clone());
    persist_authority(store, &state)?;
    write_document_file(path, document)?;

    let persisted = read_document_file(path)?
        .ok_or_else(|| "A2A grant file disappeared during commit".to_string())?;
    if !slot_matches(&state.secret_hex, &pending, &persisted)? {
        return Err("A2A grant file changed during commit; access remains disabled".into());
    }
    state.current = pending;
    state.pending = None;
    persist_authority(store, &state)
}

fn write_document_file(path: &Path, document: &GrantDocument) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "A2A grant path has no parent directory".to_string())?;
    match parent.symlink_metadata() {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => return Err("A2A settings folder must be a real directory".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(parent)
                .map_err(|error| format!("create A2A settings folder: {error}"))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
                    .map_err(|error| format!("protect A2A settings folder: {error}"))?;
            }
        }
        Err(error) => return Err(format!("inspect A2A settings folder: {error}")),
    }
    if let Ok(metadata) = path.symlink_metadata() {
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err("A2A grant file must be a regular file".into());
        }
    }
    let mut encoded = serde_json::to_vec_pretty(document)
        .map_err(|error| format!("encode A2A grants: {error}"))?;
    encoded.push(b'\n');
    if encoded.len() > MAX_DOCUMENT_BYTES {
        return Err(format!(
            "A2A grant document exceeds the {MAX_DOCUMENT_BYTES}-byte limit"
        ));
    }
    let mut file =
        AtomicWriteFile::open(path).map_err(|error| format!("open A2A grant file: {error}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("protect A2A grant file: {error}"))?;
    }
    file.write_all(&encoded)
        .map_err(|error| format!("write A2A grant file: {error}"))?;
    file.commit()
        .map_err(|error| format!("commit A2A grant file: {error}"))
}

fn state_from(
    document: GrantDocument,
    scope: &A2aGrantScopeInput,
    checkout: InspectedCheckout,
    path: &Path,
) -> A2aGrantState {
    let grants = document
        .grants
        .into_iter()
        .filter(|grant| grant_matches_scope(grant, scope) && grant.checkout_root == checkout.root)
        .map(|grant| {
            let status_message = relevant_grant_status(&grant, &checkout).err();
            A2aGrantView {
                id: grant_id(&grant).unwrap_or_else(|_| "invalid".to_string()),
                requester_pubkeys: grant.requester_pubkeys,
                capabilities: grant.capabilities,
                path_prefixes: grant.path_prefixes,
                worktree_id: grant.worktree_id,
                status: if status_message.is_some() {
                    "stale"
                } else {
                    "ready"
                },
                status_message,
            }
        })
        .collect();
    A2aGrantState {
        storage: format!("OS credential vault protected · {}", path.display()),
        checkout: checkout.view(),
        grants,
    }
}

fn relevant_grant_status(grant: &StoredGrant, checkout: &InspectedCheckout) -> Result<(), String> {
    if grant.repository != checkout.repository
        || grant.branch != checkout.branch
        || grant.base_sha != checkout.base_sha
    {
        return Err("Checkout HEAD or branch changed; save this grant again before use.".into());
    }
    validate_safe_paths(&checkout.root, &grant.path_prefixes)
}

fn grant_matches_scope(grant: &StoredGrant, scope: &A2aGrantScopeInput) -> bool {
    grant.project_address == scope.project_address
        && grant.home_channel == scope.home_channel
        && grant.repository == scope.repository
}

fn grant_id(grant: &StoredGrant) -> Result<String, String> {
    let bytes = serde_json::to_vec(grant).map_err(|error| format!("encode grant id: {error}"))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

#[cfg(test)]
#[path = "a2a_grants_tests.rs"]
mod tests;
