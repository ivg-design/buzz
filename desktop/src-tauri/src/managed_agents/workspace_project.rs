//! Owner-selected Project whose reviewed instructions apply to every managed agent.
//!
//! The record is scoped by canonical community relay and stored only in the OS
//! credential vault. It is public metadata, but keeping the selection out of a
//! model-writable file prevents a managed harness from replacing its own
//! instruction authority.

use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
};

pub(crate) const WORKSPACE_PROJECT_CHANNEL_ENV: &str = "BUZZ_ACP_WORKSPACE_PROJECT_CHANNEL";
pub(crate) const WORKSPACE_PROJECT_ADDRESS_ENV: &str = "BUZZ_ACP_WORKSPACE_PROJECT_ADDRESS";
pub(crate) const WORKSPACE_PROJECT_REPOSITORY_ENV: &str = "BUZZ_ACP_WORKSPACE_PROJECT_REPOSITORY";
pub(crate) const WORKSPACE_PROJECT_REVISION_ENV: &str = "BUZZ_ACP_WORKSPACE_PROJECT_REVISION";

pub(crate) const BUILTIN_INSTRUCTION_REVISION: &str = "builtin";

const STORE_KEY: &str = "workspace-projects.v1";
const SCHEMA_VERSION: u32 = 1;
const MAX_PROJECTS: usize = 128;
const MAX_DISPLAY_NAME_BYTES: usize = 256;
const MAX_DTAG_BYTES: usize = 128;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceProject {
    pub project_address: String,
    pub home_channel: String,
    pub repository: String,
    pub display_name: String,
    pub instruction_revision: String,
}

impl WorkspaceProject {
    pub(crate) fn managed_nemo() -> Self {
        Self {
            project_address: buzz_core_pkg::nemo::PROJECT_ADDRESS.into(),
            home_channel: buzz_core_pkg::nemo::HOME_CHANNEL.into(),
            repository: buzz_core_pkg::nemo::REPOSITORY.into(),
            display_name: buzz_core_pkg::nemo::DISPLAY_NAME.into(),
            instruction_revision: BUILTIN_INSTRUCTION_REVISION.into(),
        }
    }

    pub(crate) fn is_managed_nemo(&self) -> bool {
        buzz_core_pkg::nemo::matches(&self.project_address, &self.home_channel, &self.repository)
            && self.instruction_revision == BUILTIN_INSTRUCTION_REVISION
    }
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceProjectDocument {
    #[serde(default = "schema_version")]
    version: u32,
    #[serde(default)]
    projects: BTreeMap<String, WorkspaceProject>,
}

fn schema_version() -> u32 {
    SCHEMA_VERSION
}

fn store_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

trait ProjectStore {
    fn load(&self) -> Result<Option<String>, String>;
    fn store_verified(&self, value: &str) -> Result<(), String>;
}

struct KeychainProjectStore;

impl ProjectStore for KeychainProjectStore {
    fn load(&self) -> Result<Option<String>, String> {
        crate::secret_store::SecretStore::shared(crate::app_state::keyring_service())
            .load(STORE_KEY)
    }

    fn store_verified(&self, value: &str) -> Result<(), String> {
        let store = crate::secret_store::SecretStore::shared(crate::app_state::keyring_service());
        store.store(STORE_KEY, value)?;
        if store.verify_stored_raw(STORE_KEY, value)? {
            Ok(())
        } else {
            Err("OS credential vault did not retain the Workspace Project".into())
        }
    }
}

fn canonical_relay(relay_url: &str) -> Result<String, String> {
    buzz_core_pkg::relay::normalize_relay_url(relay_url).map_err(|error| error.to_string())
}

fn load_document(store: &impl ProjectStore) -> Result<WorkspaceProjectDocument, String> {
    let Some(raw) = store.load()? else {
        return Ok(WorkspaceProjectDocument {
            version: SCHEMA_VERSION,
            projects: BTreeMap::new(),
        });
    };
    if raw.len() > 256 * 1024 {
        return Err("Workspace Project settings exceed the size limit".into());
    }
    let document: WorkspaceProjectDocument = serde_json::from_str(&raw)
        .map_err(|error| format!("invalid Workspace Project settings: {error}"))?;
    validate_document(&document)?;
    Ok(document)
}

fn validate_document(document: &WorkspaceProjectDocument) -> Result<(), String> {
    if document.version != SCHEMA_VERSION {
        return Err(format!(
            "Workspace Project settings version must be {SCHEMA_VERSION}"
        ));
    }
    if document.projects.len() > MAX_PROJECTS {
        return Err(format!(
            "at most {MAX_PROJECTS} workspace relays are allowed"
        ));
    }
    for (relay, project) in &document.projects {
        if canonical_relay(relay)? != *relay {
            return Err("Workspace Project relay key is not canonical".into());
        }
        validate_workspace_project(project)?;
    }
    Ok(())
}

pub(crate) fn validate_workspace_project(project: &WorkspaceProject) -> Result<(), String> {
    let mut address = project.project_address.splitn(3, ':');
    let owner = address.next().zip(address.next()).zip(address.next());
    let Some((("30621", owner), dtag)) = owner else {
        return Err("projectAddress must be a canonical NIP-MP Project address".into());
    };
    if owner.len() != 64
        || !owner
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || dtag.is_empty()
        || dtag.len() > MAX_DTAG_BYTES
        || dtag.chars().any(char::is_control)
    {
        return Err("projectAddress must be a canonical NIP-MP Project address".into());
    }
    let channel = uuid::Uuid::parse_str(&project.home_channel)
        .map_err(|_| "homeChannel must be a canonical non-nil UUID".to_string())?;
    if channel.is_nil() || channel.to_string() != project.home_channel {
        return Err("homeChannel must be a canonical non-nil UUID".into());
    }
    if canonical_github_repository(&project.repository)? != project.repository {
        return Err("repository must be canonical lowercase https://github.com/owner/repo".into());
    }
    let display_name = project.display_name.trim();
    if display_name.is_empty()
        || display_name.len() > MAX_DISPLAY_NAME_BYTES
        || display_name.chars().any(char::is_control)
        || display_name != project.display_name
    {
        return Err("displayName must be trimmed visible text within 256 bytes".into());
    }
    if !project.is_managed_nemo()
        && (!matches!(project.instruction_revision.len(), 40 | 64)
            || !project
                .instruction_revision
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()))
    {
        return Err("instructionRevision must be a lowercase Git commit hash".into());
    }
    Ok(())
}

fn canonical_github_repository(value: &str) -> Result<String, String> {
    let url = url::Url::parse(value)
        .map_err(|_| "repository must be a canonical GitHub URL".to_string())?;
    if url.scheme() != "https"
        || url.host_str() != Some("github.com")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("repository must be a canonical GitHub HTTPS URL".into());
    }
    let parts = url.path().trim_matches('/').split('/').collect::<Vec<_>>();
    if parts.len() != 2
        || parts.iter().any(|part| {
            part.is_empty()
                || matches!(*part, "." | "..")
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        })
    {
        return Err("repository must identify one GitHub owner/repository".into());
    }
    Ok(format!(
        "https://github.com/{}/{}",
        parts[0].to_ascii_lowercase(),
        parts[1].trim_end_matches(".git").to_ascii_lowercase()
    ))
}

fn project_for_relay(
    store: &impl ProjectStore,
    relay_url: &str,
) -> Result<Option<WorkspaceProject>, String> {
    let relay = canonical_relay(relay_url)?;
    Ok(load_document(store)?.projects.get(&relay).cloned())
}

fn set_project_for_relay(
    store: &impl ProjectStore,
    relay_url: &str,
    project: Option<WorkspaceProject>,
) -> Result<(Option<WorkspaceProject>, bool), String> {
    let relay = canonical_relay(relay_url)?;
    if let Some(project) = &project {
        validate_workspace_project(project)?;
    }
    let _guard = store_lock()
        .lock()
        .map_err(|_| "Workspace Project settings lock is unavailable".to_string())?;
    let mut document = load_document(store)?;
    let previous = document.projects.get(&relay).cloned();
    if previous == project {
        return Ok((project, false));
    }
    match project.clone() {
        Some(project) => {
            document.projects.insert(relay, project);
        }
        None => {
            document.projects.remove(&relay);
        }
    }
    validate_document(&document)?;
    let encoded = serde_json::to_string(&document)
        .map_err(|error| format!("encode Workspace Project settings: {error}"))?;
    store.store_verified(&encoded)?;
    Ok((project, true))
}

pub(crate) fn load_workspace_project_for_relay(
    relay_url: &str,
) -> Result<Option<WorkspaceProject>, String> {
    if canonical_relay(relay_url)? == buzz_core_pkg::nemo::RELAY_URL {
        return Ok(Some(WorkspaceProject::managed_nemo()));
    }
    if !cfg!(feature = "system-keyring") {
        return Ok(None);
    }
    project_for_relay(&KeychainProjectStore, relay_url)
}

pub(crate) fn save_workspace_project_for_relay(
    relay_url: &str,
    project: Option<WorkspaceProject>,
) -> Result<(Option<WorkspaceProject>, bool), String> {
    if canonical_relay(relay_url)? == buzz_core_pkg::nemo::RELAY_URL {
        let managed = WorkspaceProject::managed_nemo();
        let _ = project;
        return Ok((Some(managed), false));
    }
    if !cfg!(feature = "system-keyring") {
        return Err("Workspace Project settings require the OS credential vault".into());
    }
    set_project_for_relay(&KeychainProjectStore, relay_url, project)
}

pub(crate) fn apply_workspace_project_env(
    command: &mut std::process::Command,
    project: Option<&WorkspaceProject>,
) -> Result<(), String> {
    command.env_remove(WORKSPACE_PROJECT_CHANNEL_ENV);
    command.env_remove(WORKSPACE_PROJECT_ADDRESS_ENV);
    command.env_remove(WORKSPACE_PROJECT_REPOSITORY_ENV);
    command.env_remove(WORKSPACE_PROJECT_REVISION_ENV);
    if let Some(project) = project {
        validate_workspace_project(project)?;
        command.env(WORKSPACE_PROJECT_CHANNEL_ENV, &project.home_channel);
        command.env(WORKSPACE_PROJECT_ADDRESS_ENV, &project.project_address);
        command.env(WORKSPACE_PROJECT_REPOSITORY_ENV, &project.repository);
        if project.instruction_revision != BUILTIN_INSTRUCTION_REVISION {
            command.env(
                WORKSPACE_PROJECT_REVISION_ENV,
                &project.instruction_revision,
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct MemoryStore(Mutex<Option<String>>);

    impl ProjectStore for MemoryStore {
        fn load(&self) -> Result<Option<String>, String> {
            Ok(self.0.lock().map_err(|error| error.to_string())?.clone())
        }

        fn store_verified(&self, value: &str) -> Result<(), String> {
            *self.0.lock().map_err(|error| error.to_string())? = Some(value.into());
            Ok(())
        }
    }

    fn project() -> WorkspaceProject {
        WorkspaceProject {
            project_address: format!("30621:{}:nemo", "a".repeat(64)),
            home_channel: "3580ca9b-47b4-4af9-b22a-1068778f26c6".into(),
            repository: "https://github.com/mysteropodes/nemo".into(),
            display_name: "Nemo".into(),
            instruction_revision: "b".repeat(40),
        }
    }

    #[test]
    fn canonical_relay_round_trip_isolated_from_other_relays() {
        let store = MemoryStore::default();
        set_project_for_relay(&store, "WSS://BUZZ.EXAMPLE/", Some(project()))
            .expect("store project");
        assert_eq!(
            project_for_relay(&store, "wss://buzz.example").expect("load project"),
            Some(project())
        );
        assert_eq!(
            project_for_relay(&store, "wss://other.example").expect("load other relay"),
            None
        );
    }

    #[test]
    fn clearing_one_relay_preserves_another() {
        let store = MemoryStore::default();
        set_project_for_relay(&store, "wss://a.example", Some(project())).expect("store A");
        let mut other = project();
        other.display_name = "Other".into();
        set_project_for_relay(&store, "wss://b.example", Some(other.clone())).expect("store B");
        set_project_for_relay(&store, "wss://a.example", None).expect("clear A");
        assert_eq!(project_for_relay(&store, "wss://a.example").unwrap(), None);
        assert_eq!(
            project_for_relay(&store, "wss://b.example").unwrap(),
            Some(other)
        );
    }

    #[test]
    fn nemo_relay_always_uses_builtin_workspace_without_saved_configuration() {
        let expected = WorkspaceProject::managed_nemo();
        assert!(expected.is_managed_nemo());
        assert!(validate_workspace_project(&expected).is_ok());
        assert_eq!(
            load_workspace_project_for_relay("WSS://BUZZ.MOGRAPH.LIFE/")
                .expect("managed workspace"),
            Some(expected.clone())
        );
        assert_eq!(
            save_workspace_project_for_relay(buzz_core_pkg::nemo::RELAY_URL, None)
                .expect("managed workspace is immutable"),
            (Some(expected), false)
        );
    }

    #[test]
    fn builtin_nemo_selector_omits_revision_pin_from_agent_environment() {
        let mut command = std::process::Command::new("true");
        apply_workspace_project_env(&mut command, Some(&WorkspaceProject::managed_nemo()))
            .expect("managed environment");
        let env = command.get_envs().collect::<BTreeMap<_, _>>();
        assert_eq!(
            env.get(std::ffi::OsStr::new(WORKSPACE_PROJECT_ADDRESS_ENV))
                .and_then(|value| value.as_ref())
                .and_then(|value| value.to_str()),
            Some(buzz_core_pkg::nemo::PROJECT_ADDRESS)
        );
        assert_eq!(
            env.get(std::ffi::OsStr::new(WORKSPACE_PROJECT_REVISION_ENV)),
            Some(&None),
            "the child environment must explicitly remove any inherited revision pin"
        );
    }

    #[test]
    fn complete_selector_is_applied_or_removed_together_after_overrides() {
        let mut command = std::process::Command::new("true");
        command
            .env(WORKSPACE_PROJECT_CHANNEL_ENV, "attacker-channel")
            .env(WORKSPACE_PROJECT_ADDRESS_ENV, "attacker-address")
            .env(WORKSPACE_PROJECT_REPOSITORY_ENV, "attacker-repository")
            .env(WORKSPACE_PROJECT_REVISION_ENV, "attacker-revision");
        apply_workspace_project_env(&mut command, Some(&project())).expect("apply project");
        let env = command.get_envs().collect::<BTreeMap<_, _>>();
        assert_eq!(
            env.get(std::ffi::OsStr::new(WORKSPACE_PROJECT_CHANNEL_ENV))
                .and_then(|value| value.as_ref())
                .and_then(|value| value.to_str()),
            Some(project().home_channel.as_str())
        );
        assert_eq!(
            env.get(std::ffi::OsStr::new(WORKSPACE_PROJECT_ADDRESS_ENV))
                .and_then(|value| value.as_ref())
                .and_then(|value| value.to_str()),
            Some(project().project_address.as_str())
        );
        assert_eq!(
            env.get(std::ffi::OsStr::new(WORKSPACE_PROJECT_REPOSITORY_ENV))
                .and_then(|value| value.as_ref())
                .and_then(|value| value.to_str()),
            Some(project().repository.as_str())
        );
        assert_eq!(
            env.get(std::ffi::OsStr::new(WORKSPACE_PROJECT_REVISION_ENV))
                .and_then(|value| value.as_ref())
                .and_then(|value| value.to_str()),
            Some(project().instruction_revision.as_str())
        );
        apply_workspace_project_env(&mut command, None).expect("clear project env");
        assert_eq!(env_value(&command, WORKSPACE_PROJECT_CHANNEL_ENV), None);
        assert_eq!(env_value(&command, WORKSPACE_PROJECT_ADDRESS_ENV), None);
        assert_eq!(env_value(&command, WORKSPACE_PROJECT_REPOSITORY_ENV), None);
        assert_eq!(env_value(&command, WORKSPACE_PROJECT_REVISION_ENV), None);
    }

    fn env_value<'a>(command: &'a std::process::Command, key: &str) -> Option<&'a str> {
        command
            .get_envs()
            .find(|(name, _)| *name == std::ffi::OsStr::new(key))
            .and_then(|(_, value)| value)
            .and_then(std::ffi::OsStr::to_str)
    }

    #[test]
    fn malformed_or_partial_authority_is_rejected() {
        let mut candidate = project();
        candidate.instruction_revision = "ABC".into();
        assert!(validate_workspace_project(&candidate).is_err());
        candidate = project();
        candidate.home_channel = uuid::Uuid::nil().to_string();
        assert!(validate_workspace_project(&candidate).is_err());
        candidate = project();
        candidate.repository = "https://github.com/Mysteropodes/Nemo".into();
        assert!(validate_workspace_project(&candidate).is_err());
        candidate = project();
        candidate.repository = "https://github.com/mysteropodes/%2e%2e".into();
        assert!(validate_workspace_project(&candidate).is_err());
    }
}
