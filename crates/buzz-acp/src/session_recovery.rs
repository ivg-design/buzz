//! Durable provider-session bindings for normal Buzz conversations and threads.
//!
//! The store contains identifiers and interruption state only. Provider
//! transcripts, credentials, and ephemeral trusted-MCP capabilities remain in
//! their existing provider/process stores.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::scope::SessionScope;

const RECOVERY_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum SessionRecoveryError {
    #[error("session recovery I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("session recovery data is invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("session recovery store has unsupported version {0}")]
    UnsupportedVersion(u32),
    #[error("session recovery store lock is poisoned")]
    Poisoned,
    #[error("provider-session binding was not found for the requested scope/session")]
    BindingNotFound,
    #[error("session recovery path must not be a symlink")]
    Symlink,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum RecoveryPhase {
    Idle,
    TurnStarted {
        turn_id: String,
        trigger_event_ids: Vec<String>,
        started_at: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedSessionBinding {
    pub scope: SessionScope,
    pub provider: String,
    pub provider_session_id: String,
    pub cwd: String,
    pub phase: RecoveryPhase,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecoveryDocument {
    version: u32,
    bindings: Vec<PersistedSessionBinding>,
}

impl Default for RecoveryDocument {
    fn default() -> Self {
        Self {
            version: RECOVERY_VERSION,
            bindings: Vec::new(),
        }
    }
}

/// Thread-safe handle for the pair-scoped recovery document.
#[derive(Clone, Debug)]
pub struct SessionRecoveryStore {
    path: PathBuf,
    document: Arc<Mutex<RecoveryDocument>>,
}

impl SessionRecoveryStore {
    /// Open a recovery document. Missing files produce an empty store; corrupt
    /// or unknown documents return an error so callers can fail closed.
    pub fn open(path: PathBuf) -> Result<Self, SessionRecoveryError> {
        reject_symlink(&path)?;
        let document = match fs::read(&path) {
            Ok(bytes) => {
                let document: RecoveryDocument = serde_json::from_slice(&bytes)?;
                if document.version != RECOVERY_VERSION {
                    return Err(SessionRecoveryError::UnsupportedVersion(document.version));
                }
                document
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                RecoveryDocument::default()
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            path,
            document: Arc::new(Mutex::new(document)),
        })
    }

    /// Open a store for production recovery. Invalid prior bytes are ignored
    /// rather than trusted; the next successful binding write atomically
    /// replaces them with a current document.
    pub fn open_fail_closed(path: PathBuf) -> Self {
        match Self::open(path.clone()) {
            Ok(store) => store,
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    "ignoring unusable provider-session recovery document: {error}"
                );
                Self {
                    path,
                    document: Arc::new(Mutex::new(RecoveryDocument::default())),
                }
            }
        }
    }

    /// Return an exact binding. Normal branch/revision and instruction changes
    /// are intentionally absent from this identity check: current instructions
    /// are supplied again on resume where the adapter supports that extension.
    pub fn binding(
        &self,
        scope: &SessionScope,
        provider: &str,
        cwd: &str,
    ) -> Result<Option<PersistedSessionBinding>, SessionRecoveryError> {
        if scope.is_job() {
            return Ok(None);
        }
        let document = self
            .document
            .lock()
            .map_err(|_| SessionRecoveryError::Poisoned)?;
        Ok(document
            .bindings
            .iter()
            .find(|binding| {
                &binding.scope == scope && binding.provider == provider && binding.cwd == cwd
            })
            .cloned())
    }

    /// Insert or replace a normal conversation/thread binding atomically.
    pub fn record_binding(
        &self,
        binding: PersistedSessionBinding,
    ) -> Result<(), SessionRecoveryError> {
        if binding.scope.is_job() {
            return Ok(());
        }
        self.mutate(|document| {
            // A Buzz scope owns one active provider conversation. Switching
            // providers replaces the old binding rather than leaving a stale
            // session that could be revived by a later provider switch.
            document
                .bindings
                .retain(|candidate| candidate.scope != binding.scope);
            document.bindings.push(binding);
        })
    }

    /// Mark the prompt boundary before provider delivery.
    pub fn mark_turn_started(
        &self,
        scope: &SessionScope,
        provider_session_id: &str,
        turn_id: &str,
        trigger_event_ids: &[String],
        started_at: &str,
    ) -> Result<(), SessionRecoveryError> {
        self.mutate_binding(scope, provider_session_id, |binding| {
            binding.phase = RecoveryPhase::TurnStarted {
                turn_id: turn_id.to_owned(),
                trigger_event_ids: trigger_event_ids.to_vec(),
                started_at: started_at.to_owned(),
            };
        })
    }

    /// Mark a provider turn as returned. A process death before this write
    /// leaves `turn_started` as the durable reconciliation signal.
    pub fn mark_idle(
        &self,
        scope: &SessionScope,
        provider_session_id: &str,
    ) -> Result<(), SessionRecoveryError> {
        self.mutate_binding(scope, provider_session_id, |binding| {
            binding.phase = RecoveryPhase::Idle;
        })
    }

    /// Remove a binding when Buzz deliberately rotates that provider session.
    pub fn remove(&self, scope: &SessionScope) -> Result<(), SessionRecoveryError> {
        if scope.is_job() {
            return Ok(());
        }
        self.mutate(|document| document.bindings.retain(|binding| &binding.scope != scope))
    }

    fn mutate(
        &self,
        update: impl FnOnce(&mut RecoveryDocument),
    ) -> Result<(), SessionRecoveryError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| SessionRecoveryError::Poisoned)?;
        let mut next = document.clone();
        update(&mut next);
        persist_document(&self.path, &next)?;
        *document = next;
        Ok(())
    }

    fn mutate_binding(
        &self,
        scope: &SessionScope,
        provider_session_id: &str,
        update: impl FnOnce(&mut PersistedSessionBinding),
    ) -> Result<(), SessionRecoveryError> {
        // Temporary job sessions use the A2A ledger's execution fence and
        // terminal receipts. They never create resumable conversation bindings,
        // so neither their prompt-start nor completion boundary belongs here.
        if scope.is_job() {
            return Ok(());
        }
        let mut document = self
            .document
            .lock()
            .map_err(|_| SessionRecoveryError::Poisoned)?;
        let mut next = document.clone();
        let binding = next
            .bindings
            .iter_mut()
            .find(|binding| {
                &binding.scope == scope && binding.provider_session_id == provider_session_id
            })
            .ok_or(SessionRecoveryError::BindingNotFound)?;
        update(binding);
        persist_document(&self.path, &next)?;
        *document = next;
        Ok(())
    }
}

fn reject_symlink(path: &Path) -> Result<(), SessionRecoveryError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(SessionRecoveryError::Symlink),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn persist_document(path: &Path, document: &RecoveryDocument) -> Result<(), SessionRecoveryError> {
    reject_symlink(path)?;
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "session recovery path has no parent",
        )
    })?;
    fs::create_dir_all(parent)?;
    let temp = parent.join(format!(
        ".session-recovery-{}-{}.tmp",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&temp)?;
    let bytes = serde_json::to_vec_pretty(document)?;
    let write_result = (|| -> Result<(), SessionRecoveryError> {
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    write_result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn scope() -> SessionScope {
        SessionScope::Conversation {
            channel_id: Uuid::new_v4(),
        }
    }

    #[test]
    fn persists_exact_binding_and_interrupted_turn_without_revision_identity() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sessions.json");
        let scope = scope();
        let store = SessionRecoveryStore::open(path.clone()).unwrap();
        store
            .record_binding(PersistedSessionBinding {
                scope: scope.clone(),
                provider: "codex-acp".into(),
                provider_session_id: "thread-1".into(),
                cwd: "/workspace".into(),
                phase: RecoveryPhase::Idle,
            })
            .unwrap();
        store
            .mark_turn_started(
                &scope,
                "thread-1",
                "turn-1",
                &["event-1".into()],
                "2026-09-04T00:00:00Z",
            )
            .unwrap();

        let reopened = SessionRecoveryStore::open(path).unwrap();
        let binding = reopened
            .binding(&scope, "codex-acp", "/workspace")
            .unwrap()
            .unwrap();
        assert_eq!(binding.provider_session_id, "thread-1");
        assert!(matches!(binding.phase, RecoveryPhase::TurnStarted { .. }));
        assert!(reopened
            .binding(&scope, "claude-agent-acp", "/workspace")
            .unwrap()
            .is_none());
    }

    #[test]
    fn deliberate_rotation_removes_only_the_target_scope() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sessions.json");
        let first = scope();
        let second = scope();
        let store = SessionRecoveryStore::open(path.clone()).unwrap();
        for (scope, id) in [(&first, "one"), (&second, "two")] {
            store
                .record_binding(PersistedSessionBinding {
                    scope: scope.clone(),
                    provider: "codex-acp".into(),
                    provider_session_id: id.into(),
                    cwd: "/workspace".into(),
                    phase: RecoveryPhase::Idle,
                })
                .unwrap();
        }
        store.remove(&first).unwrap();
        let reopened = SessionRecoveryStore::open(path).unwrap();
        assert!(reopened
            .binding(&first, "codex-acp", "/workspace")
            .unwrap()
            .is_none());
        assert!(reopened
            .binding(&second, "codex-acp", "/workspace")
            .unwrap()
            .is_some());
    }

    #[test]
    fn corrupt_and_unknown_documents_fail_closed() {
        let dir = tempdir().unwrap();
        let corrupt = dir.path().join("corrupt.json");
        fs::write(&corrupt, b"{").unwrap();
        assert!(matches!(
            SessionRecoveryStore::open(corrupt),
            Err(SessionRecoveryError::Json(_))
        ));

        let unknown = dir.path().join("unknown.json");
        fs::write(&unknown, br#"{"version":2,"bindings":[]}"#).unwrap();
        assert!(matches!(
            SessionRecoveryStore::open(unknown),
            Err(SessionRecoveryError::UnsupportedVersion(2))
        ));
    }

    #[test]
    fn turn_boundary_fails_when_exact_binding_is_missing() {
        let dir = tempdir().unwrap();
        let store = SessionRecoveryStore::open(dir.path().join("sessions.json")).unwrap();
        assert!(matches!(
            store.mark_turn_started(
                &scope(),
                "missing-session",
                "turn-1",
                &[],
                "2026-09-04T00:00:00Z"
            ),
            Err(SessionRecoveryError::BindingNotFound)
        ));
    }

    #[test]
    fn job_boundaries_preserve_interrupted_conversation_and_need_no_recovery_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sessions.json");
        let conversation = scope();
        let store = SessionRecoveryStore::open(path.clone()).unwrap();
        let binding = PersistedSessionBinding {
            scope: conversation.clone(),
            provider: "claude-agent-acp".into(),
            provider_session_id: "conversation-session".into(),
            cwd: "/workspace".into(),
            phase: RecoveryPhase::TurnStarted {
                turn_id: "interrupted-conversation".into(),
                trigger_event_ids: vec!["event-1".into()],
                started_at: "2026-09-05T00:00:00Z".into(),
            },
        };
        store.record_binding(binding.clone()).unwrap();
        let original = fs::read(&path).unwrap();
        // Unavailable conversation storage must block normal writes, but it
        // must not prevent an independently journaled A2A job from running.
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        let job = SessionScope::Job {
            channel_id: conversation.channel_id(),
            operation_id: Uuid::new_v4().to_string(),
            request_event_id: "a".repeat(64),
        };
        store
            .record_binding(PersistedSessionBinding {
                scope: job.clone(),
                provider_session_id: "job-session".into(),
                ..binding.clone()
            })
            .unwrap();
        store
            .mark_turn_started(&job, "job-session", "job-turn", &[], "now")
            .unwrap();
        store.mark_idle(&job, "job-session").unwrap();
        store.remove(&job).unwrap();
        assert!(store
            .binding(&job, "claude-agent-acp", "/workspace")
            .unwrap()
            .is_none());
        assert!(matches!(
            store.mark_idle(&conversation, "conversation-session"),
            Err(SessionRecoveryError::Io(_))
        ));
        assert_eq!(
            store
                .binding(&conversation, "claude-agent-acp", "/workspace")
                .unwrap(),
            Some(binding.clone()),
            "failed normal writes must retain the interrupted boundary"
        );
        fs::remove_dir(&path).unwrap();
        fs::write(&path, original).unwrap();
        let reopened = SessionRecoveryStore::open(path).unwrap();
        assert_eq!(
            reopened
                .binding(&conversation, "claude-agent-acp", "/workspace")
                .unwrap(),
            Some(binding)
        );
        assert!(reopened
            .binding(&job, "claude-agent-acp", "/workspace")
            .unwrap()
            .is_none());
    }
}
