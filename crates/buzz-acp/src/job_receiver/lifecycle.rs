use std::fs::File;
#[cfg(not(windows))]
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use nostr::Event;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const LIFECYCLE_VERSION: u32 = 3;
const HUMAN_REPORT_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum LifecycleError {
    #[error("job lifecycle state I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("job lifecycle state JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("job lifecycle state is invalid: {0}")]
    Invalid(String),
    #[error("job lifecycle blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LifecycleState {
    version: u32,
    accepted_event_id: String,
    head_event_id: String,
    outbox: Option<Event>,
    outbox_terminal: bool,
    #[serde(default)]
    outbox_machine_confirmed: bool,
    #[serde(default)]
    outbox_conversation_confirmed: bool,
    cancel_event_id: Option<String>,
    terminal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HumanReportState {
    version: u32,
    request_event_id: String,
    outbox: Option<Event>,
    confirmed_event_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelDecision {
    Observed,
    Replay,
    AlreadyTerminal,
}

/// Exact durable state consulted before a job-scoped privileged operation.
///
/// This deliberately exposes event IDs and state only. The trusted MCP never
/// receives the lifecycle file path or any signing material.
#[derive(Debug, Clone)]
pub(super) struct PrivilegeSnapshot {
    pub(super) accepted_event_id: String,
    pub(super) head_event_id: String,
    pub(super) pending_outbox: Option<Event>,
    pub(super) cancel_event_id: Option<String>,
    pub(super) terminal: bool,
}

#[derive(Debug, Clone)]
pub struct LifecycleStore {
    path: PathBuf,
    lock_path: PathBuf,
}

/// Independent durable outbox for the worker's human-facing chat report.
///
/// Report delivery is intentionally separate from the job lifecycle head: a
/// retry may republish the exact signed chat event, but it must never replay
/// the task or claim another lifecycle transition.
#[derive(Debug, Clone)]
pub struct HumanReportStore {
    path: PathBuf,
    lock_path: PathBuf,
    request_event_id: String,
}

impl LifecycleStore {
    pub fn new(root: &Path, key: &str) -> Self {
        Self {
            path: root.join(format!("{key}.lifecycle.json")),
            lock_path: root.join(format!("{key}.lifecycle.lock")),
        }
    }

    pub(super) fn privilege_lock_path(&self) -> PathBuf {
        self.lock_path.with_extension("privilege.lock")
    }

    pub fn human_report_store(&self, request_event_id: &str) -> HumanReportStore {
        HumanReportStore {
            path: self.path.with_extension("report.json"),
            lock_path: self.lock_path.with_extension("report.lock"),
            request_event_id: request_event_id.to_owned(),
        }
    }

    pub async fn initialize(&self, accepted_event_id: String) -> Result<(), LifecycleError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.initialize_blocking(accepted_event_id)).await?
    }

    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    pub async fn snapshot(&self) -> Result<(String, Option<Event>, bool), LifecycleError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let state = store.read()?;
            Ok((state.head_event_id, state.outbox, state.terminal))
        })
        .await?
    }

    pub async fn pending_cancel(&self) -> Result<Option<String>, LifecycleError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let state = store.read()?;
            Ok((!state.terminal).then_some(state.cancel_event_id).flatten())
        })
        .await?
    }

    pub(super) async fn privilege_snapshot(&self) -> Result<PrivilegeSnapshot, LifecycleError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let state = store.with_lock(|| store.read())?;
            Ok(PrivilegeSnapshot {
                accepted_event_id: state.accepted_event_id,
                head_event_id: state.head_event_id,
                pending_outbox: state.outbox,
                cancel_event_id: state.cancel_event_id,
                terminal: state.terminal,
            })
        })
        .await?
    }

    pub async fn stage(
        &self,
        event: Event,
        terminal: bool,
        expected_head: String,
    ) -> Result<(), LifecycleError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.stage_blocking(event, terminal, &expected_head))
            .await?
    }

    pub async fn confirm(&self, event_id: String) -> Result<(), LifecycleError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.confirm_blocking(&event_id, false)).await?
    }

    /// Record acknowledgement of the deterministic ordinary-chat mirror.
    pub async fn confirm_conversation(&self, event_id: String) -> Result<(), LifecycleError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.confirm_blocking(&event_id, true)).await?
    }

    /// Frozen transition plus its independently durable delivery acknowledgements.
    pub async fn pending_delivery(&self) -> Result<Option<(Event, bool, bool)>, LifecycleError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let state = store.with_lock(|| store.read())?;
            Ok(state.outbox.map(|event| {
                (
                    event,
                    state.outbox_machine_confirmed,
                    state.outbox_conversation_confirmed,
                )
            }))
        })
        .await?
    }

    pub async fn observe_cancel(
        &self,
        event_id: String,
        expected_head: String,
    ) -> Result<CancelDecision, LifecycleError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.observe_cancel_blocking(&event_id, &expected_head)
        })
        .await?
    }

    /// Adopt a relay-confirmed terminal event published outside `JobEmitter`.
    ///
    /// The caller must verify the signed event and its exact predecessor before
    /// invoking this method. A stored terminal proves that any locally frozen
    /// sibling for `expected_head` lost the relay's serialized transition slot.
    #[cfg(test)]
    pub(super) async fn observe_external_terminal(
        &self,
        event_id: String,
        expected_head: String,
    ) -> Result<(), LifecycleError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.observe_external_terminal_blocking(&event_id, &expected_head)
        })
        .await?
    }

    fn initialize_blocking(&self, accepted_event_id: String) -> Result<(), LifecycleError> {
        self.with_lock(|| {
            if self.path.exists() {
                let state = self.read()?;
                if state.accepted_event_id != accepted_event_id {
                    return Err(LifecycleError::Invalid(
                        "lifecycle root does not match accepted receipt".into(),
                    ));
                }
                return Ok(());
            }
            let state = LifecycleState {
                version: LIFECYCLE_VERSION,
                accepted_event_id: accepted_event_id.clone(),
                head_event_id: accepted_event_id,
                outbox: None,
                outbox_terminal: false,
                outbox_machine_confirmed: false,
                outbox_conversation_confirmed: false,
                cancel_event_id: None,
                terminal: false,
            };
            write_new_or_validate(&self.path, &state)
        })
    }

    fn stage_blocking(
        &self,
        event: Event,
        terminal: bool,
        expected_head: &str,
    ) -> Result<(), LifecycleError> {
        self.with_lock(|| {
            let mut state = self.read()?;
            if state.terminal {
                return Err(LifecycleError::Invalid("job is already terminal".into()));
            }
            if state.outbox.is_some() {
                return Err(LifecycleError::Invalid(
                    "a frozen lifecycle event is awaiting acknowledgement".into(),
                ));
            }
            if state.head_event_id != expected_head {
                return Err(LifecycleError::Invalid(
                    "lifecycle head changed before transition claim".into(),
                ));
            }
            state.outbox = Some(event);
            state.outbox_terminal = terminal;
            state.outbox_machine_confirmed = false;
            state.outbox_conversation_confirmed = false;
            replace_private(&self.path, &state)
        })
    }

    fn confirm_blocking(&self, event_id: &str, conversation: bool) -> Result<(), LifecycleError> {
        self.with_lock(|| {
            let mut state = self.read()?;
            // Submission acknowledgement can race the background outbox retry
            // with the live emitter. Once either path confirms this exact
            // event, the other confirmation is already satisfied. Treat that
            // replay as success instead of reporting a permanently pending
            // outbox after the winner has cleared it.
            if state.head_event_id == event_id {
                return Ok(());
            }
            let Some(event) = state.outbox.as_ref() else {
                return Err(LifecycleError::Invalid("lifecycle outbox is empty".into()));
            };
            if event.id.to_hex() != event_id {
                return Err(LifecycleError::Invalid(
                    "acknowledgement does not match the frozen lifecycle event".into(),
                ));
            }
            if conversation {
                state.outbox_conversation_confirmed = true;
            } else {
                state.outbox_machine_confirmed = true;
            }
            let requires_conversation = buzz_core::job::JobEvent::parse(event)
                .ok()
                .is_some_and(|job| job.common().conversation.is_some());
            if !state.outbox_machine_confirmed
                || (requires_conversation && !state.outbox_conversation_confirmed)
            {
                return replace_private(&self.path, &state);
            }
            state.head_event_id = event_id.into();
            state.outbox = None;
            state.terminal = state.outbox_terminal;
            state.outbox_terminal = false;
            state.outbox_machine_confirmed = false;
            state.outbox_conversation_confirmed = false;
            replace_private(&self.path, &state)
        })
    }

    fn observe_cancel_blocking(
        &self,
        event_id: &str,
        expected_head: &str,
    ) -> Result<CancelDecision, LifecycleError> {
        self.with_lock(|| {
            let mut state = self.read()?;
            if state.terminal {
                return Ok(CancelDecision::AlreadyTerminal);
            }
            if state.cancel_event_id.as_deref() == Some(event_id) {
                return Ok(CancelDecision::Replay);
            }
            if state.cancel_event_id.is_some() {
                return Err(LifecycleError::Invalid(
                    "a different cancellation is already recorded".into(),
                ));
            }
            let pending_is_predecessor = state
                .outbox
                .as_ref()
                .is_some_and(|event| event.id.to_hex() == expected_head);
            if state.head_event_id != expected_head && !pending_is_predecessor {
                return Err(LifecycleError::Invalid(
                    "cancellation predecessor does not match the current lifecycle head".into(),
                ));
            }
            // `observe_cancel` is called only for a verified Cancel that the
            // relay already stored. If the Cancel follows the frozen outbox,
            // that predecessor must also have been stored even though its local
            // confirmation had not completed. Otherwise, a frozen local sibling
            // with the current head lost the relay's serialized transition slot.
            // In both cases the exact Cancel is now authoritative, so retire the
            // outbox instead of retrying an impossible transition forever.
            state.outbox = None;
            state.outbox_terminal = false;
            state.outbox_machine_confirmed = false;
            state.outbox_conversation_confirmed = false;
            state.head_event_id = event_id.into();
            state.cancel_event_id = Some(event_id.into());
            replace_private(&self.path, &state)?;
            Ok(CancelDecision::Observed)
        })
    }

    #[cfg(test)]
    fn observe_external_terminal_blocking(
        &self,
        event_id: &str,
        expected_head: &str,
    ) -> Result<(), LifecycleError> {
        self.with_lock(|| {
            let mut state = self.read()?;
            if state.terminal {
                return (state.head_event_id == event_id)
                    .then_some(())
                    .ok_or_else(|| {
                        LifecycleError::Invalid(
                            "a different terminal event is already recorded".into(),
                        )
                    });
            }
            if state.head_event_id != expected_head {
                return Err(LifecycleError::Invalid(
                    "terminal predecessor does not match the current lifecycle head".into(),
                ));
            }
            state.outbox = None;
            state.outbox_terminal = false;
            state.outbox_machine_confirmed = false;
            state.outbox_conversation_confirmed = false;
            state.cancel_event_id = None;
            state.head_event_id = event_id.into();
            state.terminal = true;
            replace_private(&self.path, &state)
        })
    }

    fn with_lock<T>(
        &self,
        operation: impl FnOnce() -> Result<T, LifecycleError>,
    ) -> Result<T, LifecycleError> {
        let parent = self
            .lock_path
            .parent()
            .ok_or_else(|| LifecycleError::Invalid("lock path has no parent".into()))?;
        std::fs::create_dir_all(parent)?;
        #[cfg(windows)]
        let lock = super::windows_private::open_private_lock(&self.lock_path, true)?.0;
        #[cfg(not(windows))]
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.lock_path)?;
        fs2::FileExt::lock_exclusive(&lock)?;
        let result = operation();
        fs2::FileExt::unlock(&lock)?;
        result
    }

    fn read(&self) -> Result<LifecycleState, LifecycleError> {
        let state: LifecycleState = serde_json::from_slice(&read_private_bytes(&self.path)?)?;
        if state.version != LIFECYCLE_VERSION {
            return Err(LifecycleError::Invalid("unsupported state version".into()));
        }
        Ok(state)
    }
}

impl HumanReportStore {
    pub async fn snapshot(&self) -> Result<(Option<Event>, Option<String>), LifecycleError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.snapshot_blocking()).await?
    }

    pub async fn stage(&self, event: Event) -> Result<(), LifecycleError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.stage_blocking(event)).await?
    }

    pub async fn confirm(&self, event_id: String) -> Result<(), LifecycleError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.confirm_blocking(&event_id)).await?
    }

    fn snapshot_blocking(&self) -> Result<(Option<Event>, Option<String>), LifecycleError> {
        self.with_lock(|| {
            if !self.path.exists() {
                return Ok((None, None));
            }
            let state = self.read()?;
            Ok((state.outbox, state.confirmed_event_id))
        })
    }

    fn stage_blocking(&self, event: Event) -> Result<(), LifecycleError> {
        self.with_lock(|| {
            if self.path.exists() {
                let state = self.read()?;
                if state.confirmed_event_id.as_deref() == Some(event.id.to_hex().as_str())
                    || state
                        .outbox
                        .as_ref()
                        .is_some_and(|pending| pending.id == event.id)
                {
                    return Ok(());
                }
                return Err(LifecycleError::Invalid(
                    "a different human report is already frozen or confirmed".into(),
                ));
            }
            write_new_human_report(
                &self.path,
                &HumanReportState {
                    version: HUMAN_REPORT_VERSION,
                    request_event_id: self.request_event_id.clone(),
                    outbox: Some(event),
                    confirmed_event_id: None,
                },
            )
        })
    }

    fn confirm_blocking(&self, event_id: &str) -> Result<(), LifecycleError> {
        self.with_lock(|| {
            let mut state = self.read()?;
            if state.confirmed_event_id.as_deref() == Some(event_id) {
                return Ok(());
            }
            let Some(event) = state.outbox.as_ref() else {
                return Err(LifecycleError::Invalid(
                    "human report outbox is empty".into(),
                ));
            };
            if event.id.to_hex() != event_id {
                return Err(LifecycleError::Invalid(
                    "acknowledgement does not match the frozen human report".into(),
                ));
            }
            state.outbox = None;
            state.confirmed_event_id = Some(event_id.to_owned());
            replace_private_human_report(&self.path, &state)
        })
    }

    fn with_lock<T>(
        &self,
        operation: impl FnOnce() -> Result<T, LifecycleError>,
    ) -> Result<T, LifecycleError> {
        let parent = self
            .lock_path
            .parent()
            .ok_or_else(|| LifecycleError::Invalid("report lock path has no parent".into()))?;
        std::fs::create_dir_all(parent)?;
        #[cfg(windows)]
        let lock = super::windows_private::open_private_lock(&self.lock_path, true)?.0;
        #[cfg(not(windows))]
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.lock_path)?;
        fs2::FileExt::lock_exclusive(&lock)?;
        let result = operation();
        fs2::FileExt::unlock(&lock)?;
        result
    }

    fn read(&self) -> Result<HumanReportState, LifecycleError> {
        let state: HumanReportState = serde_json::from_slice(&read_private_bytes(&self.path)?)?;
        if state.version != HUMAN_REPORT_VERSION || state.request_event_id != self.request_event_id
        {
            return Err(LifecycleError::Invalid(
                "human report state does not match this request".into(),
            ));
        }
        Ok(state)
    }
}

fn write_new_human_report(path: &Path, state: &HumanReportState) -> Result<(), LifecycleError> {
    let parent = path
        .parent()
        .ok_or_else(|| LifecycleError::Invalid("report path has no parent".into()))?;
    let mut file = private_new(path)?;
    file.write_all(&serde_json::to_vec(state)?)?;
    file.sync_all()?;
    sync_directory(parent)?;
    Ok(())
}

fn replace_private_human_report(
    path: &Path,
    state: &HumanReportState,
) -> Result<(), LifecycleError> {
    let parent = path
        .parent()
        .ok_or_else(|| LifecycleError::Invalid("report path has no parent".into()))?;
    let temporary = parent.join(format!(".report-{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut file = private_new(&temporary)?;
        file.write_all(&serde_json::to_vec(state)?)?;
        file.sync_all()?;
        #[cfg(windows)]
        super::windows_private::replace_private_file(&temporary, path)?;
        #[cfg(not(windows))]
        std::fs::rename(&temporary, path)?;
        sync_directory(parent)?;
        Ok::<_, LifecycleError>(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

fn write_new_or_validate(path: &Path, state: &LifecycleState) -> Result<(), LifecycleError> {
    let bytes = serde_json::to_vec(state)?;
    match private_new(path) {
        Ok(mut file) => {
            file.write_all(&bytes)?;
            file.sync_all()?;
            sync_directory(
                path.parent()
                    .ok_or_else(|| LifecycleError::Invalid("state path has no parent".into()))?,
            )?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing: LifecycleState = serde_json::from_slice(&read_private_bytes(path)?)?;
            if existing.version == LIFECYCLE_VERSION
                && existing.accepted_event_id == state.accepted_event_id
            {
                Ok(())
            } else {
                Err(LifecycleError::Invalid(
                    "existing lifecycle root does not match accepted receipt".into(),
                ))
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn sync_directory(_path: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        File::open(_path)?.sync_all()?;
    }
    // std does not expose a portable Windows directory fsync. File contents
    // are flushed there; full directory-entry crash durability is guaranteed
    // on the Unix targets used by Buzz desktop and relay deployments.
    Ok(())
}

fn replace_private(path: &Path, state: &LifecycleState) -> Result<(), LifecycleError> {
    let parent = path
        .parent()
        .ok_or_else(|| LifecycleError::Invalid("state path has no parent".into()))?;
    let temporary = parent.join(format!(".lifecycle-{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut file = private_new(&temporary)?;
        file.write_all(&serde_json::to_vec(state)?)?;
        file.sync_all()?;
        #[cfg(windows)]
        super::windows_private::replace_private_file(&temporary, path)?;
        #[cfg(not(windows))]
        std::fs::rename(&temporary, path)?;
        sync_directory(parent)?;
        Ok::<_, LifecycleError>(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

#[cfg(unix)]
fn private_new(path: &Path) -> Result<File, std::io::Error> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(windows)]
fn private_new(path: &Path) -> Result<File, std::io::Error> {
    super::windows_private::create_private_new(path)
}

#[cfg(all(not(unix), not(windows)))]
fn private_new(path: &Path) -> Result<File, std::io::Error> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

#[cfg(windows)]
fn read_private_bytes(path: &Path) -> Result<Vec<u8>, std::io::Error> {
    use std::io::Read as _;
    let mut file = super::windows_private::open_private_read(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

#[cfg(not(windows))]
fn read_private_bytes(path: &Path) -> Result<Vec<u8>, std::io::Error> {
    std::fs::read(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind};

    fn event(content: &str) -> Event {
        EventBuilder::new(Kind::TextNote, content)
            .sign_with_keys(&Keys::generate())
            .expect("sign")
    }

    #[tokio::test]
    async fn initialize_rejects_a_different_accepted_root() {
        let root = std::env::temp_dir().join(format!("buzz-lifecycle-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create root");
        let store = LifecycleStore::new(&root, "job");
        store.initialize("a".repeat(64)).await.expect("initialize");
        store
            .initialize("a".repeat(64))
            .await
            .expect("same accepted root is idempotent");
        assert!(store.initialize("b".repeat(64)).await.is_err());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn frozen_outbox_survives_restart_until_exact_confirmation() {
        let root = std::env::temp_dir().join(format!("buzz-lifecycle-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create root");
        let store = LifecycleStore::new(&root, "job");
        store.initialize("a".repeat(64)).await.expect("initialize");
        let progress = event("progress");
        store
            .stage(progress.clone(), false, "a".repeat(64))
            .await
            .expect("stage");

        let reopened = LifecycleStore::new(&root, "job");
        let (head, pending, terminal) = reopened.snapshot().await.expect("reopen");
        assert_eq!(head, "a".repeat(64));
        assert_eq!(pending.expect("frozen event").id, progress.id);
        assert!(!terminal);
        assert!(reopened.confirm("b".repeat(64)).await.is_err());
        reopened
            .confirm(progress.id.to_hex())
            .await
            .expect("confirm exact event");
        let (head, pending, terminal) = reopened.snapshot().await.expect("confirmed");
        assert_eq!(head, progress.id.to_hex());
        assert!(pending.is_none());
        assert!(!terminal);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn human_report_outbox_replays_exact_event_without_advancing_lifecycle() {
        let root = tempfile::tempdir().expect("root");
        let lifecycle = LifecycleStore::new(root.path(), "job");
        lifecycle
            .initialize("a".repeat(64))
            .await
            .expect("initialize");
        let report = lifecycle.human_report_store(&"b".repeat(64));
        let signed = event("human-readable report");
        report.stage(signed.clone()).await.expect("stage report");

        let reopened = LifecycleStore::new(root.path(), "job").human_report_store(&"b".repeat(64));
        let (pending, confirmed) = reopened.snapshot().await.expect("snapshot");
        assert_eq!(pending.expect("pending report").id, signed.id);
        assert!(confirmed.is_none());
        assert!(reopened.confirm("c".repeat(64)).await.is_err());
        reopened
            .confirm(signed.id.to_hex())
            .await
            .expect("confirm report");
        reopened
            .confirm(signed.id.to_hex())
            .await
            .expect("idempotent confirmation");
        let (pending, confirmed) = reopened.snapshot().await.expect("confirmed snapshot");
        assert!(pending.is_none());
        assert_eq!(confirmed.as_deref(), Some(signed.id.to_hex().as_str()));

        let (head, pending, terminal) = lifecycle.snapshot().await.expect("lifecycle unchanged");
        assert_eq!(head, "a".repeat(64));
        assert!(pending.is_none());
        assert!(!terminal);
    }

    #[tokio::test]
    async fn human_report_store_binds_request_and_one_signed_event() {
        let root = tempfile::tempdir().expect("root");
        let lifecycle = LifecycleStore::new(root.path(), "job");
        let first = lifecycle.human_report_store(&"a".repeat(64));
        let signed = event("first");
        first.stage(signed.clone()).await.expect("stage first");
        first
            .stage(signed.clone())
            .await
            .expect("same frozen event is idempotent");
        assert!(first.stage(event("different")).await.is_err());
        assert!(lifecycle
            .human_report_store(&"b".repeat(64))
            .snapshot()
            .await
            .is_err());
    }

    #[tokio::test]
    async fn duplicate_confirmation_of_the_current_head_is_idempotent() {
        for terminal_event in [false, true] {
            let root = tempfile::tempdir().expect("root");
            let live = LifecycleStore::new(root.path(), "job");
            live.initialize("a".repeat(64)).await.expect("initialize");
            let progress = event("progress or terminal");
            live.stage(progress.clone(), terminal_event, "a".repeat(64))
                .await
                .expect("stage");
            let retry = LifecycleStore::new(root.path(), "job");
            let (first, second) = tokio::join!(
                live.confirm(progress.id.to_hex()),
                retry.confirm(progress.id.to_hex()),
            );
            first.expect("live confirmer");
            second.expect("racing retry confirmer");

            let reopened = LifecycleStore::new(root.path(), "job");
            let (head, pending, terminal) = reopened.snapshot().await.expect("snapshot");
            assert_eq!(head, progress.id.to_hex());
            assert!(pending.is_none());
            assert_eq!(terminal, terminal_event);
            assert!(reopened.confirm("b".repeat(64)).await.is_err());
        }
    }

    #[tokio::test]
    async fn duplicate_head_confirmation_preserves_a_newer_terminal_outbox() {
        let root = tempfile::tempdir().expect("root");
        let store = LifecycleStore::new(root.path(), "job");
        store.initialize("a".repeat(64)).await.expect("initialize");
        let progress = event("progress");
        store
            .stage(progress.clone(), false, "a".repeat(64))
            .await
            .expect("progress");
        store
            .confirm(progress.id.to_hex())
            .await
            .expect("confirm progress");
        let terminal_event = event("terminal");
        store
            .stage(terminal_event.clone(), true, progress.id.to_hex())
            .await
            .expect("terminal");
        store
            .confirm(progress.id.to_hex())
            .await
            .expect("late progress confirmation");
        let (head, pending, terminal) = store.snapshot().await.expect("pending terminal");
        assert_eq!(head, progress.id.to_hex());
        assert_eq!(
            pending.expect("terminal outbox retained").id,
            terminal_event.id
        );
        assert!(!terminal);
        store
            .confirm(terminal_event.id.to_hex())
            .await
            .expect("confirm terminal");
        assert!(store.confirm(progress.id.to_hex()).await.is_err());
        assert!(store.snapshot().await.expect("terminal snapshot").2);
    }

    #[tokio::test]
    async fn terminal_outbox_blocks_sibling_transition() {
        let root = std::env::temp_dir().join(format!("buzz-lifecycle-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create root");
        let store = LifecycleStore::new(&root, "job");
        store.initialize("a".repeat(64)).await.expect("initialize");
        let result = event("result");
        store
            .stage(result.clone(), true, "a".repeat(64))
            .await
            .expect("stage");
        assert!(store
            .stage(event("fork"), true, "a".repeat(64))
            .await
            .is_err());
        store.confirm(result.id.to_hex()).await.expect("confirm");
        assert!(store
            .stage(event("late"), false, result.id.to_hex())
            .await
            .is_err());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn concurrent_stores_have_one_transition_winner_and_no_orphan() {
        let root = std::env::temp_dir().join(format!("buzz-lifecycle-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create root");
        let first = LifecycleStore::new(&root, "job");
        let second = LifecycleStore::new(&root, "job");
        let accepted = "a".repeat(64);
        first
            .initialize(accepted.clone())
            .await
            .expect("initialize");
        let left = event("left");
        let right = event("right");
        let (left_result, right_result) = tokio::join!(
            first.stage(left.clone(), true, accepted.clone()),
            second.stage(right.clone(), true, accepted)
        );
        assert_ne!(left_result.is_ok(), right_result.is_ok());
        let (_, pending, terminal) = first.snapshot().await.expect("snapshot");
        let winner = pending.expect("one frozen winner");
        assert!(winner.id == left.id || winner.id == right.id);
        assert!(!terminal);
        first
            .confirm(winner.id.to_hex())
            .await
            .expect("confirm winner");
        let (head, pending, terminal) = first.snapshot().await.expect("confirmed");
        assert_eq!(head, winner.id.to_hex());
        assert!(pending.is_none());
        assert!(terminal);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn cancellation_is_one_durable_head_transition() {
        let root = std::env::temp_dir().join(format!("buzz-lifecycle-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create root");
        let store = LifecycleStore::new(&root, "job");
        let accepted = "a".repeat(64);
        let cancel = "b".repeat(64);
        store
            .initialize(accepted.clone())
            .await
            .expect("initialize");
        assert_eq!(
            store
                .observe_cancel(cancel.clone(), accepted)
                .await
                .expect("observe"),
            CancelDecision::Observed
        );
        let reopened = LifecycleStore::new(&root, "job");
        assert_eq!(
            reopened
                .observe_cancel(cancel.clone(), "a".repeat(64))
                .await
                .expect("replay"),
            CancelDecision::Replay
        );
        let (head, pending, terminal) = reopened.snapshot().await.expect("snapshot");
        assert_eq!(head, cancel);
        assert!(pending.is_none());
        assert!(!terminal);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn stored_cancel_retires_a_frozen_losing_successor() {
        let root = std::env::temp_dir().join(format!("buzz-lifecycle-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create root");
        let store = LifecycleStore::new(&root, "job");
        let accepted = "a".repeat(64);
        store
            .initialize(accepted.clone())
            .await
            .expect("initialize");
        store
            .stage(event("local sibling"), false, accepted.clone())
            .await
            .expect("freeze sibling");

        let cancel = "b".repeat(64);
        assert_eq!(
            store
                .observe_cancel(cancel.clone(), accepted)
                .await
                .expect("stored cancel wins"),
            CancelDecision::Observed
        );
        let snapshot = store.privilege_snapshot().await.expect("snapshot");
        assert_eq!(snapshot.head_event_id, cancel);
        assert!(snapshot.pending_outbox.is_none());
        assert!(snapshot.cancel_event_id.is_some());
        assert!(!snapshot.terminal);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn stored_cancel_adopts_its_frozen_acknowledged_predecessor() {
        let root = std::env::temp_dir().join(format!("buzz-lifecycle-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create root");
        let store = LifecycleStore::new(&root, "job");
        let accepted = "a".repeat(64);
        store
            .initialize(accepted.clone())
            .await
            .expect("initialize");
        let marker = event("relay-acknowledged marker");
        store
            .stage(marker.clone(), false, accepted)
            .await
            .expect("freeze marker before local acknowledgement");

        let cancel = "b".repeat(64);
        assert_eq!(
            store
                .observe_cancel(cancel.clone(), marker.id.to_hex())
                .await
                .expect("Cancel proves its predecessor was relay stored"),
            CancelDecision::Observed
        );
        let snapshot = store.privilege_snapshot().await.expect("snapshot");
        assert_eq!(snapshot.head_event_id, cancel);
        assert!(snapshot.pending_outbox.is_none());
        assert!(snapshot.cancel_event_id.is_some());
        assert!(!snapshot.terminal);
        assert!(
            store.confirm(marker.id.to_hex()).await.is_err(),
            "the delayed local marker confirmation must lose to Cancel"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn relay_confirmed_external_terminal_is_idempotent_and_retires_sibling() {
        let root = std::env::temp_dir().join(format!("buzz-lifecycle-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create root");
        let store = LifecycleStore::new(&root, "job");
        let accepted = "a".repeat(64);
        store
            .initialize(accepted.clone())
            .await
            .expect("initialize");
        store
            .stage(event("local sibling"), false, accepted.clone())
            .await
            .expect("freeze sibling");

        let handoff = "c".repeat(64);
        store
            .observe_external_terminal(handoff.clone(), accepted.clone())
            .await
            .expect("adopt handoff");
        store
            .observe_external_terminal(handoff.clone(), accepted)
            .await
            .expect("exact replay");
        let snapshot = store.privilege_snapshot().await.expect("snapshot");
        assert_eq!(snapshot.accepted_event_id, "a".repeat(64));
        assert_eq!(snapshot.head_event_id, handoff);
        assert!(snapshot.pending_outbox.is_none());
        assert!(snapshot.cancel_event_id.is_none());
        assert!(snapshot.terminal);
        assert!(store
            .observe_external_terminal("d".repeat(64), "a".repeat(64))
            .await
            .is_err());
        std::fs::remove_dir_all(root).ok();
    }
}
