//! Durable ACP-owned journal for job-scoped Git side effects.
//!
//! The trusted Git runner reports what it observed, but this module owns the
//! durable lifecycle. A journal is initialized before a job prompt can run;
//! each invocation is prepared before its relay marker, marked in-flight
//! before the opaque lease crosses into the runner, and finalized only after
//! ACP validates the producer receipt against its independently held binding.

use std::fs::{File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use buzz_core::job::{semantic_request_digest, JobEvent};
use buzz_dev_mcp::{
    PrivilegedGitDisposition, PrivilegedGitOperationReceipt, PrivilegedOperationOutcome,
    ProjectGitOperation,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use super::ledger::StoredClaim;
use super::lifecycle::LifecycleStore;

const JOURNAL_VERSION: u32 = 1;
const RECEIPT_SCHEMA_VERSION: &str = "buzz.git-operation-receipt.v1";
const MAX_RECORDS: usize = 256;
const MAX_JOURNAL_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Error)]
pub(super) enum GitReceiptJournalError {
    #[error("Git receipt journal I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Git receipt journal JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Git receipt journal is invalid: {0}")]
    Invalid(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GitReceiptBinding {
    pub(super) invocation_id: Uuid,
    pub(super) operation: ProjectGitOperation,
    pub(super) community_id: String,
    pub(super) project_address: String,
    pub(super) session_channel_id: String,
    pub(super) operation_id: String,
    pub(super) request_event_id: String,
    pub(super) requester_pubkey: String,
    pub(super) worker_pubkey: String,
    pub(super) scope_digest: String,
    pub(super) repository: String,
    pub(super) branch_ref: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GitEffect {
    NotApplied,
    Applied,
    Ambiguous,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GitEffectSummary {
    pub(crate) effect: GitEffect,
    pub(crate) operation_count: usize,
    pub(crate) applied_count: usize,
    pub(crate) ambiguous_count: usize,
}

#[derive(Clone, Debug)]
pub(super) struct GitReceiptJournal {
    path: PathBuf,
    job: GitJobBinding,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u32,
    job: GitJobBinding,
    records: Vec<InvocationRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GitJobBinding {
    community_id: String,
    project_address: String,
    session_channel_id: String,
    operation_id: String,
    request_event_id: String,
    requester_pubkey: String,
    worker_pubkey: String,
    scope_digest: String,
    repository: String,
    branch: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvocationRecord {
    binding: GitReceiptBinding,
    marker_event_id: Option<String>,
    phase: ReceiptPhase,
    outcome: Option<RecordedOutcome>,
    receipt: Option<PrivilegedGitOperationReceipt>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReceiptPhase {
    Prepared,
    InFlight,
    Final,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RecordedOutcome {
    Completed,
    Failed,
    Cancelled,
    Indeterminate,
}

impl From<PrivilegedOperationOutcome> for RecordedOutcome {
    fn from(value: PrivilegedOperationOutcome) -> Self {
        match value {
            PrivilegedOperationOutcome::Completed => Self::Completed,
            PrivilegedOperationOutcome::Failed => Self::Failed,
            PrivilegedOperationOutcome::Cancelled => Self::Cancelled,
            PrivilegedOperationOutcome::Indeterminate => Self::Indeterminate,
        }
    }
}

impl GitReceiptJournal {
    fn from_privilege_lock(
        lock_path: &Path,
        job: GitJobBinding,
    ) -> Result<Self, GitReceiptJournalError> {
        let parent = lock_path.parent().ok_or_else(|| {
            GitReceiptJournalError::Invalid("privilege lock has no parent directory".into())
        })?;
        let name = lock_path.file_name().ok_or_else(|| {
            GitReceiptJournalError::Invalid("privilege lock has no file name".into())
        })?;
        Ok(Self {
            path: parent.join(format!("{}.git-receipts.json", name.to_string_lossy())),
            job,
        })
    }

    pub(super) fn for_claim(
        lifecycle: &LifecycleStore,
        community_id: &str,
        worker_pubkey: &str,
        claim: &StoredClaim,
    ) -> Result<Self, GitReceiptJournalError> {
        let job = job_binding(community_id, worker_pubkey, claim)?;
        Self::from_privilege_lock(&lifecycle.privilege_lock_path(), job)
    }

    /// Create the durable empty sentinel before a model-controlled prompt can
    /// invoke trusted Git. An existing journal must parse and validate exactly.
    pub(super) fn initialize(&self) -> Result<(), GitReceiptJournalError> {
        let journal = Journal {
            version: JOURNAL_VERSION,
            job: self.job.clone(),
            records: Vec::new(),
        };
        match private_new(&self.path) {
            Ok(mut file) => {
                let bytes = serde_json::to_vec(&journal)?;
                file.write_all(&bytes)?;
                file.sync_all()?;
                sync_parent(&self.path)?;
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = self.read_validated()?;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn prepare(&self, binding: GitReceiptBinding) -> Result<(), GitReceiptJournalError> {
        self.update(|journal| {
            if let Some(existing) = journal
                .records
                .iter()
                .find(|record| record.binding.invocation_id == binding.invocation_id)
            {
                return if existing.binding == binding
                    && existing.phase == ReceiptPhase::Prepared
                    && existing.marker_event_id.is_none()
                    && existing.outcome.is_none()
                    && existing.receipt.is_none()
                {
                    Ok(())
                } else {
                    Err(GitReceiptJournalError::Invalid(
                        "Git invocation UUID conflicts with an existing receipt record".into(),
                    ))
                };
            }
            if journal.records.len() >= MAX_RECORDS {
                return Err(GitReceiptJournalError::Invalid(format!(
                    "Git receipt journal contains the maximum {MAX_RECORDS} invocations"
                )));
            }
            if summarize(journal)?.effect == GitEffect::Ambiguous {
                return Err(GitReceiptJournalError::Invalid(
                    "an unresolved Git invocation blocks additional privileged operations".into(),
                ));
            }
            journal.records.push(InvocationRecord {
                binding,
                marker_event_id: None,
                phase: ReceiptPhase::Prepared,
                outcome: None,
                receipt: None,
            });
            Ok(())
        })
    }

    pub(super) fn bind_marker(
        &self,
        invocation_id: Uuid,
        marker_event_id: &str,
    ) -> Result<(), GitReceiptJournalError> {
        require_hex(marker_event_id, &[64], "privilege marker event ID")?;
        self.update(|journal| {
            if journal.records.iter().any(|record| {
                record.binding.invocation_id != invocation_id
                    && record.marker_event_id.as_deref() == Some(marker_event_id)
            }) {
                return Err(GitReceiptJournalError::Invalid(
                    "privilege marker is already bound to another invocation".into(),
                ));
            }
            let record = record_mut(journal, invocation_id)?;
            if record.phase != ReceiptPhase::Prepared
                || record.outcome.is_some()
                || record.receipt.is_some()
            {
                return Err(GitReceiptJournalError::Invalid(
                    "only a prepared Git invocation may bind a marker".into(),
                ));
            }
            match record.marker_event_id.as_deref() {
                None => record.marker_event_id = Some(marker_event_id.to_owned()),
                Some(existing) if existing == marker_event_id => {}
                Some(_) => {
                    return Err(GitReceiptJournalError::Invalid(
                        "Git invocation is already bound to a different marker".into(),
                    ));
                }
            }
            Ok(())
        })
    }

    pub(super) fn mark_in_flight(
        &self,
        invocation_id: Uuid,
        marker_event_id: &str,
    ) -> Result<(), GitReceiptJournalError> {
        self.update(|journal| {
            let record = record_mut(journal, invocation_id)?;
            require_marker(record, marker_event_id)?;
            match record.phase {
                ReceiptPhase::Prepared => record.phase = ReceiptPhase::InFlight,
                ReceiptPhase::InFlight => {}
                ReceiptPhase::Final => {
                    return Err(GitReceiptJournalError::Invalid(
                        "a final Git invocation cannot return to in-flight".into(),
                    ));
                }
            }
            Ok(())
        })
    }

    pub(super) fn finalize(
        &self,
        invocation_id: Uuid,
        marker_event_id: &str,
        outcome: PrivilegedOperationOutcome,
        receipt: PrivilegedGitOperationReceipt,
    ) -> Result<PrivilegedGitOperationReceipt, GitReceiptJournalError> {
        let mut normalized = None;
        self.update(|journal| {
            let record = record_mut(journal, invocation_id)?;
            require_marker(record, marker_event_id)?;
            let receipt = validate_and_normalize_receipt(&record.binding, outcome, receipt)?;
            let recorded_outcome = RecordedOutcome::from(outcome);
            match record.phase {
                ReceiptPhase::InFlight => {
                    record.phase = ReceiptPhase::Final;
                    record.outcome = Some(recorded_outcome);
                    record.receipt = Some(receipt.clone());
                }
                ReceiptPhase::Final
                    if record.outcome == Some(recorded_outcome)
                        && record.receipt.as_ref() == Some(&receipt) => {}
                ReceiptPhase::Prepared => {
                    return Err(GitReceiptJournalError::Invalid(
                        "a prepared Git invocation cannot be finalized before in-flight".into(),
                    ));
                }
                ReceiptPhase::Final => {
                    return Err(GitReceiptJournalError::Invalid(
                        "a final Git invocation cannot be rewritten".into(),
                    ));
                }
            }
            normalized = Some(receipt);
            Ok(())
        })?;
        normalized.ok_or_else(|| {
            GitReceiptJournalError::Invalid("normalized Git receipt was not retained".into())
        })
    }

    /// Record a known no-effect outcome when authority expires or cancellation
    /// wins after the in-flight fence is durable but before the lease is handed
    /// to the trusted runner.
    pub(super) fn finalize_unstarted(
        &self,
        invocation_id: Uuid,
        marker_event_id: &str,
        outcome: PrivilegedOperationOutcome,
    ) -> Result<(), GitReceiptJournalError> {
        if !matches!(
            outcome,
            PrivilegedOperationOutcome::Failed | PrivilegedOperationOutcome::Cancelled
        ) {
            return Err(GitReceiptJournalError::Invalid(
                "an unstarted Git invocation must finish failed or cancelled".into(),
            ));
        }
        self.update(|journal| {
            let record = record_mut(journal, invocation_id)?;
            require_marker(record, marker_event_id)?;
            if record.phase != ReceiptPhase::InFlight {
                return Err(GitReceiptJournalError::Invalid(
                    "only an in-flight Git invocation can be closed before execution".into(),
                ));
            }
            let receipt = validate_and_normalize_receipt(
                &record.binding,
                outcome,
                PrivilegedGitOperationReceipt {
                    schema_version: RECEIPT_SCHEMA_VERSION.into(),
                    invocation_id: record.binding.invocation_id,
                    operation: record.binding.operation,
                    session_channel_id: record.binding.session_channel_id.clone(),
                    operation_id: record.binding.operation_id.clone(),
                    request_event_id: record.binding.request_event_id.clone(),
                    worker_pubkey: record.binding.worker_pubkey.clone(),
                    scope_digest: Some(record.binding.scope_digest.clone()),
                    repository: Some(record.binding.repository.clone()),
                    branch_ref: Some(record.binding.branch_ref.clone()),
                    previous_object: None,
                    intended_object: None,
                    observed_object: None,
                    disposition: PrivilegedGitDisposition::NotApplied,
                },
            )?;
            record.phase = ReceiptPhase::Final;
            record.outcome = Some(RecordedOutcome::from(outcome));
            record.receipt = Some(receipt);
            Ok(())
        })
    }

    pub(super) fn summary(&self) -> Result<GitEffectSummary, GitReceiptJournalError> {
        summarize(&self.read_validated()?)
    }

    fn update(
        &self,
        mutate: impl FnOnce(&mut Journal) -> Result<(), GitReceiptJournalError>,
    ) -> Result<(), GitReceiptJournalError> {
        let mut journal = self.read_validated()?;
        mutate(&mut journal)?;
        validate_journal(&journal)?;
        replace_private(&self.path, &journal)
    }

    fn read_validated(&self) -> Result<Journal, GitReceiptJournalError> {
        let mut file = open_private_read(&self.path)?;
        let length = file.metadata()?.len();
        if length > MAX_JOURNAL_BYTES {
            return Err(GitReceiptJournalError::Invalid(format!(
                "Git receipt journal exceeds {MAX_JOURNAL_BYTES} bytes"
            )));
        }
        let mut bytes = Vec::with_capacity(length as usize);
        std::io::Read::by_ref(&mut file)
            .take(MAX_JOURNAL_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_JOURNAL_BYTES {
            return Err(GitReceiptJournalError::Invalid(format!(
                "Git receipt journal exceeds {MAX_JOURNAL_BYTES} bytes"
            )));
        }
        let journal: Journal = serde_json::from_slice(&bytes)?;
        validate_journal(&journal)?;
        if journal.job != self.job {
            return Err(GitReceiptJournalError::Invalid(
                "Git receipt journal belongs to a different job claim".into(),
            ));
        }
        Ok(journal)
    }
}

pub(super) fn summary_for_lifecycle(
    lifecycle: &LifecycleStore,
    community_id: &str,
    worker_pubkey: &str,
    claim: &StoredClaim,
) -> Result<GitEffectSummary, GitReceiptJournalError> {
    GitReceiptJournal::for_claim(lifecycle, community_id, worker_pubkey, claim)?.summary()
}

/// Persist the empty no-effect sentinel for a lifecycle that is durably known
/// not to have started its model prompt. This is used for a requester Cancel
/// that wins before admission; once a prompt has started, a missing journal is
/// deliberately left missing and therefore classifies as ambiguous.
pub(super) fn initialize_for_unstarted_lifecycle(
    lifecycle: &LifecycleStore,
    community_id: &str,
    worker_pubkey: &str,
    claim: &StoredClaim,
) -> Result<(), GitReceiptJournalError> {
    GitReceiptJournal::for_claim(lifecycle, community_id, worker_pubkey, claim)?.initialize()
}

fn job_binding(
    community_id: &str,
    worker_pubkey: &str,
    claim: &StoredClaim,
) -> Result<GitJobBinding, GitReceiptJournalError> {
    claim.request_event.verify().map_err(|error| {
        GitReceiptJournalError::Invalid(format!("stored request signature: {error}"))
    })?;
    let JobEvent::Request(request) = JobEvent::parse(&claim.request_event)
        .map_err(|error| GitReceiptJournalError::Invalid(error.to_string()))?
    else {
        return Err(GitReceiptJournalError::Invalid(
            "stored claim root is not a job request".into(),
        ));
    };
    let digest = semantic_request_digest(&request)
        .map_err(|error| GitReceiptJournalError::Invalid(error.to_string()))?;
    if claim.community != community_id
        || claim.requester != request.common.sender_pubkey
        || claim.request_event_id != claim.request_event.id.to_hex()
        || claim.digest != digest
        || request.common.recipient_pubkey != worker_pubkey
    {
        return Err(GitReceiptJournalError::Invalid(
            "stored claim does not match the Git journal job binding".into(),
        ));
    }
    let binding = GitJobBinding {
        community_id: community_id.into(),
        project_address: request.common.project.address,
        session_channel_id: request.common.project.home_channel,
        operation_id: request.common.operation_id,
        request_event_id: claim.request_event_id.clone(),
        requester_pubkey: claim.requester.clone(),
        worker_pubkey: worker_pubkey.into(),
        scope_digest: claim.digest.clone(),
        repository: request.common.repository.canonical,
        branch: request.common.repository.branch,
    };
    validate_job_binding(&binding)?;
    Ok(binding)
}

fn record_mut(
    journal: &mut Journal,
    invocation_id: Uuid,
) -> Result<&mut InvocationRecord, GitReceiptJournalError> {
    journal
        .records
        .iter_mut()
        .find(|record| record.binding.invocation_id == invocation_id)
        .ok_or_else(|| {
            GitReceiptJournalError::Invalid("Git invocation has no prepared receipt record".into())
        })
}

fn require_marker(
    record: &InvocationRecord,
    marker_event_id: &str,
) -> Result<(), GitReceiptJournalError> {
    if record.marker_event_id.as_deref() == Some(marker_event_id) {
        Ok(())
    } else {
        Err(GitReceiptJournalError::Invalid(
            "Git invocation marker does not match its durable receipt record".into(),
        ))
    }
}

fn validate_journal(journal: &Journal) -> Result<(), GitReceiptJournalError> {
    if journal.version != JOURNAL_VERSION {
        return Err(GitReceiptJournalError::Invalid(
            "unsupported Git receipt journal version".into(),
        ));
    }
    if journal.records.len() > MAX_RECORDS {
        return Err(GitReceiptJournalError::Invalid(
            "Git receipt journal contains too many records".into(),
        ));
    }
    validate_job_binding(&journal.job)?;
    let mut invocations = std::collections::HashSet::new();
    let mut markers = std::collections::HashSet::new();
    for record in &journal.records {
        if !invocations.insert(record.binding.invocation_id) {
            return Err(GitReceiptJournalError::Invalid(
                "Git receipt journal repeats an invocation UUID".into(),
            ));
        }
        validate_binding(&record.binding)?;
        if !record_matches_job(&record.binding, &journal.job) {
            return Err(GitReceiptJournalError::Invalid(
                "Git receipt record belongs to a different job claim".into(),
            ));
        }
        if let Some(marker) = &record.marker_event_id {
            require_hex(marker, &[64], "privilege marker event ID")?;
            if !markers.insert(marker) {
                return Err(GitReceiptJournalError::Invalid(
                    "Git receipt journal repeats a privilege marker".into(),
                ));
            }
        }
        match record.phase {
            ReceiptPhase::Prepared if record.outcome.is_none() && record.receipt.is_none() => {}
            ReceiptPhase::InFlight
                if record.marker_event_id.is_some()
                    && record.outcome.is_none()
                    && record.receipt.is_none() => {}
            ReceiptPhase::Final
                if record.marker_event_id.is_some()
                    && record.outcome.is_some()
                    && record.receipt.is_some() =>
            {
                let stored = record.receipt.clone().expect("checked Some");
                let normalized = validate_and_normalize_receipt(
                    &record.binding,
                    outcome_from_recorded(record.outcome.expect("checked Some")),
                    stored.clone(),
                )?;
                if normalized != stored {
                    return Err(GitReceiptJournalError::Invalid(
                        "final Git receipt omitted a normalized ACP binding".into(),
                    ));
                }
            }
            _ => {
                return Err(GitReceiptJournalError::Invalid(
                    "Git receipt record has an invalid phase payload".into(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_job_binding(binding: &GitJobBinding) -> Result<(), GitReceiptJournalError> {
    require_hex(&binding.request_event_id, &[64], "request event ID")?;
    require_hex(&binding.requester_pubkey, &[64], "requester pubkey")?;
    require_hex(&binding.worker_pubkey, &[64], "worker pubkey")?;
    require_hex(&binding.scope_digest, &[64], "scope digest")?;
    Uuid::parse_str(&binding.session_channel_id).map_err(|_| {
        GitReceiptJournalError::Invalid("session channel binding is not a UUID".into())
    })?;
    Uuid::parse_str(&binding.operation_id).map_err(|_| {
        GitReceiptJournalError::Invalid("job operation binding is not a UUID".into())
    })?;
    if binding.community_id.is_empty()
        || binding.project_address.is_empty()
        || binding.repository.is_empty()
        || binding.branch.is_empty()
        || binding.branch.contains(['\n', '\r', '\0'])
    {
        return Err(GitReceiptJournalError::Invalid(
            "Git journal job binding contains an empty or malformed coordinate".into(),
        ));
    }
    Ok(())
}

fn record_matches_job(record: &GitReceiptBinding, job: &GitJobBinding) -> bool {
    let expected_ref = match record.operation {
        ProjectGitOperation::Commit | ProjectGitOperation::Push => {
            format!("refs/heads/{}", job.branch)
        }
        ProjectGitOperation::Fetch => format!("refs/remotes/origin/{}", job.branch),
        ProjectGitOperation::Handoff => return false,
    };
    record.community_id == job.community_id
        && record.project_address == job.project_address
        && record.session_channel_id == job.session_channel_id
        && record.operation_id == job.operation_id
        && record.request_event_id == job.request_event_id
        && record.requester_pubkey == job.requester_pubkey
        && record.worker_pubkey == job.worker_pubkey
        && record.scope_digest == job.scope_digest
        && record.repository == job.repository
        && record.branch_ref == expected_ref
}

fn validate_binding(binding: &GitReceiptBinding) -> Result<(), GitReceiptJournalError> {
    if binding.operation == ProjectGitOperation::Handoff {
        return Err(GitReceiptJournalError::Invalid(
            "handoff must not enter the Git receipt journal".into(),
        ));
    }
    require_hex(&binding.request_event_id, &[64], "request event ID")?;
    require_hex(&binding.requester_pubkey, &[64], "requester pubkey")?;
    require_hex(&binding.worker_pubkey, &[64], "worker pubkey")?;
    require_hex(&binding.scope_digest, &[64], "scope digest")?;
    Uuid::parse_str(&binding.session_channel_id).map_err(|_| {
        GitReceiptJournalError::Invalid("session channel binding is not a UUID".into())
    })?;
    Uuid::parse_str(&binding.operation_id).map_err(|_| {
        GitReceiptJournalError::Invalid("job operation binding is not a UUID".into())
    })?;
    if binding.community_id.is_empty()
        || binding.project_address.is_empty()
        || binding.repository.is_empty()
        || !binding.branch_ref.starts_with("refs/")
        || binding.branch_ref.contains(['\n', '\r', '\0'])
    {
        return Err(GitReceiptJournalError::Invalid(
            "Git receipt binding contains an empty or malformed coordinate".into(),
        ));
    }
    Ok(())
}

fn validate_and_normalize_receipt(
    binding: &GitReceiptBinding,
    outcome: PrivilegedOperationOutcome,
    mut receipt: PrivilegedGitOperationReceipt,
) -> Result<PrivilegedGitOperationReceipt, GitReceiptJournalError> {
    validate_binding(binding)?;
    if receipt.schema_version != RECEIPT_SCHEMA_VERSION
        || receipt.invocation_id != binding.invocation_id
        || receipt.operation != binding.operation
        || receipt.session_channel_id != binding.session_channel_id
        || receipt.operation_id != binding.operation_id
        || receipt.request_event_id != binding.request_event_id
        || receipt.worker_pubkey != binding.worker_pubkey
    {
        return Err(GitReceiptJournalError::Invalid(
            "Git producer receipt does not match the ACP-held invocation binding".into(),
        ));
    }
    normalize_optional_binding(
        &mut receipt.scope_digest,
        &binding.scope_digest,
        receipt.disposition,
        "scope digest",
    )?;
    normalize_optional_binding(
        &mut receipt.repository,
        &binding.repository,
        receipt.disposition,
        "repository",
    )?;
    normalize_optional_binding(
        &mut receipt.branch_ref,
        &binding.branch_ref,
        receipt.disposition,
        "branch ref",
    )?;
    for (label, value) in [
        ("previous object", receipt.previous_object.as_deref()),
        ("intended object", receipt.intended_object.as_deref()),
        ("observed object", receipt.observed_object.as_deref()),
    ] {
        if let Some(value) = value {
            require_hex(value, &[40, 64], label)?;
        }
    }
    match (outcome, receipt.disposition) {
        (PrivilegedOperationOutcome::Completed, PrivilegedGitDisposition::Applied) => {
            let intended = receipt.intended_object.as_ref().ok_or_else(|| {
                GitReceiptJournalError::Invalid("applied Git receipt omits intended object".into())
            })?;
            if receipt.observed_object.as_ref() != Some(intended) {
                return Err(GitReceiptJournalError::Invalid(
                    "applied Git receipt did not observe its exact intended object".into(),
                ));
            }
        }
        (
            PrivilegedOperationOutcome::Failed | PrivilegedOperationOutcome::Cancelled,
            PrivilegedGitDisposition::NotApplied,
        ) => {
            if receipt.intended_object.is_none() {
                if receipt.observed_object.is_some() {
                    return Err(GitReceiptJournalError::Invalid(
                        "preparation failure reports an impossible observed object".into(),
                    ));
                }
            } else if receipt.observed_object != receipt.previous_object {
                return Err(GitReceiptJournalError::Invalid(
                    "not-applied Git receipt does not preserve the previous object".into(),
                ));
            }
        }
        (PrivilegedOperationOutcome::Indeterminate, PrivilegedGitDisposition::Ambiguous) => {
            if receipt.intended_object.is_none() {
                return Err(GitReceiptJournalError::Invalid(
                    "ambiguous Git receipt omits intended object".into(),
                ));
            }
        }
        _ => {
            return Err(GitReceiptJournalError::Invalid(
                "Git operation outcome conflicts with its effect disposition".into(),
            ));
        }
    }
    Ok(receipt)
}

fn normalize_optional_binding(
    actual: &mut Option<String>,
    expected: &str,
    disposition: PrivilegedGitDisposition,
    label: &str,
) -> Result<(), GitReceiptJournalError> {
    match actual {
        Some(value) if value == expected => Ok(()),
        Some(_) => Err(GitReceiptJournalError::Invalid(format!(
            "Git producer receipt has the wrong {label}"
        ))),
        None if disposition == PrivilegedGitDisposition::NotApplied => {
            *actual = Some(expected.to_owned());
            Ok(())
        }
        None => Err(GitReceiptJournalError::Invalid(format!(
            "Git producer receipt omits its {label}"
        ))),
    }
}

fn summarize(journal: &Journal) -> Result<GitEffectSummary, GitReceiptJournalError> {
    validate_journal(journal)?;
    let mut applied_count = 0;
    let mut ambiguous_count = 0;
    for record in &journal.records {
        match record.phase {
            ReceiptPhase::Prepared => {}
            ReceiptPhase::InFlight => ambiguous_count += 1,
            ReceiptPhase::Final => match record
                .receipt
                .as_ref()
                .expect("validated final receipt")
                .disposition
            {
                PrivilegedGitDisposition::Applied => applied_count += 1,
                PrivilegedGitDisposition::NotApplied => {}
                PrivilegedGitDisposition::Ambiguous => ambiguous_count += 1,
            },
        }
    }
    let effect = if ambiguous_count > 0 {
        GitEffect::Ambiguous
    } else if applied_count > 0 {
        GitEffect::Applied
    } else {
        GitEffect::NotApplied
    };
    Ok(GitEffectSummary {
        effect,
        operation_count: journal.records.len(),
        applied_count,
        ambiguous_count,
    })
}

fn outcome_from_recorded(value: RecordedOutcome) -> PrivilegedOperationOutcome {
    match value {
        RecordedOutcome::Completed => PrivilegedOperationOutcome::Completed,
        RecordedOutcome::Failed => PrivilegedOperationOutcome::Failed,
        RecordedOutcome::Cancelled => PrivilegedOperationOutcome::Cancelled,
        RecordedOutcome::Indeterminate => PrivilegedOperationOutcome::Indeterminate,
    }
}

fn require_hex(value: &str, lengths: &[usize], label: &str) -> Result<(), GitReceiptJournalError> {
    if lengths.contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(GitReceiptJournalError::Invalid(format!(
            "{label} is not canonical lowercase hexadecimal"
        )))
    }
}

fn replace_private(path: &Path, journal: &Journal) -> Result<(), GitReceiptJournalError> {
    let parent = path.parent().ok_or_else(|| {
        GitReceiptJournalError::Invalid("Git receipt journal has no parent".into())
    })?;
    let temporary = parent.join(format!(".git-receipts-{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut file = private_new(&temporary)?;
        let bytes = serde_json::to_vec(journal)?;
        if bytes.len() as u64 > MAX_JOURNAL_BYTES {
            return Err(GitReceiptJournalError::Invalid(format!(
                "Git receipt journal exceeds {MAX_JOURNAL_BYTES} bytes"
            )));
        }
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        sync_parent(path)?;
        Ok::<_, GitReceiptJournalError>(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn sync_parent(path: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    File::open(
        path.parent()
            .ok_or_else(|| std::io::Error::other("journal path has no parent"))?,
    )?
    .sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn private_new(path: &Path) -> Result<File, std::io::Error> {
    use std::os::unix::fs::OpenOptionsExt as _;
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(not(unix))]
fn private_new(_path: &Path) -> Result<File, std::io::Error> {
    Err(std::io::Error::other(
        "Git receipt journals require owner-only no-follow file support",
    ))
}

#[cfg(unix)]
fn open_private_read(path: &Path) -> Result<File, GitReceiptJournalError> {
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(GitReceiptJournalError::Invalid(
            "Git receipt journal must be an operator-owned mode-0600 regular file".into(),
        ));
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_private_read(_path: &Path) -> Result<File, GitReceiptJournalError> {
    Err(GitReceiptJournalError::Invalid(
        "Git receipt journals require owner-only no-follow file support".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job_binding() -> GitJobBinding {
        GitJobBinding {
            community_id: Uuid::new_v4().to_string(),
            project_address: format!("30621:{}:nemo", "a".repeat(64)),
            session_channel_id: Uuid::new_v4().to_string(),
            operation_id: Uuid::new_v4().to_string(),
            request_event_id: "b".repeat(64),
            requester_pubkey: "c".repeat(64),
            worker_pubkey: "d".repeat(64),
            scope_digest: "e".repeat(64),
            repository: "https://github.com/mysteropodes/nemo".into(),
            branch: "codex/a2a".into(),
        }
    }

    fn binding(job: &GitJobBinding, operation: ProjectGitOperation) -> GitReceiptBinding {
        let branch_ref = match operation {
            ProjectGitOperation::Fetch => format!("refs/remotes/origin/{}", job.branch),
            _ => format!("refs/heads/{}", job.branch),
        };
        GitReceiptBinding {
            invocation_id: Uuid::new_v4(),
            operation,
            community_id: job.community_id.clone(),
            project_address: job.project_address.clone(),
            session_channel_id: job.session_channel_id.clone(),
            operation_id: job.operation_id.clone(),
            request_event_id: job.request_event_id.clone(),
            requester_pubkey: job.requester_pubkey.clone(),
            worker_pubkey: job.worker_pubkey.clone(),
            scope_digest: job.scope_digest.clone(),
            repository: job.repository.clone(),
            branch_ref,
        }
    }

    fn receipt(
        binding: &GitReceiptBinding,
        disposition: PrivilegedGitDisposition,
    ) -> PrivilegedGitOperationReceipt {
        let intended = "2".repeat(40);
        PrivilegedGitOperationReceipt {
            schema_version: RECEIPT_SCHEMA_VERSION.into(),
            invocation_id: binding.invocation_id,
            operation: binding.operation,
            session_channel_id: binding.session_channel_id.clone(),
            operation_id: binding.operation_id.clone(),
            request_event_id: binding.request_event_id.clone(),
            worker_pubkey: binding.worker_pubkey.clone(),
            scope_digest: Some(binding.scope_digest.clone()),
            repository: Some(binding.repository.clone()),
            branch_ref: Some(binding.branch_ref.clone()),
            previous_object: Some("1".repeat(40)),
            intended_object: Some(intended.clone()),
            observed_object: match disposition {
                PrivilegedGitDisposition::Applied => Some(intended),
                PrivilegedGitDisposition::NotApplied => Some("1".repeat(40)),
                PrivilegedGitDisposition::Ambiguous => None,
            },
            disposition,
        }
    }

    fn journal() -> (tempfile::TempDir, GitReceiptJournal, GitJobBinding) {
        let root = tempfile::tempdir().expect("journal root");
        let job = job_binding();
        let store =
            GitReceiptJournal::from_privilege_lock(&root.path().join("job.lock"), job.clone())
                .expect("journal path");
        store.initialize().expect("journal initialize");
        (root, store, job)
    }

    fn advance(
        store: &GitReceiptJournal,
        binding: &GitReceiptBinding,
        disposition: PrivilegedGitDisposition,
    ) {
        let marker = "f".repeat(64);
        store.prepare(binding.clone()).expect("prepare");
        store
            .bind_marker(binding.invocation_id, &marker)
            .expect("bind marker");
        store
            .mark_in_flight(binding.invocation_id, &marker)
            .expect("in flight");
        let outcome = match disposition {
            PrivilegedGitDisposition::Applied => PrivilegedOperationOutcome::Completed,
            PrivilegedGitDisposition::NotApplied => PrivilegedOperationOutcome::Failed,
            PrivilegedGitDisposition::Ambiguous => PrivilegedOperationOutcome::Indeterminate,
        };
        store
            .finalize(
                binding.invocation_id,
                &marker,
                outcome,
                receipt(binding, disposition),
            )
            .expect("finalize");
    }

    #[cfg(unix)]
    #[test]
    fn initialized_empty_journal_is_private_and_cancel_safe() {
        use std::os::unix::fs::PermissionsExt as _;
        let (_root, store, _job) = journal();
        assert_eq!(store.summary().unwrap().effect, GitEffect::NotApplied);
        assert_eq!(
            std::fs::metadata(&store.path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn exact_phase_order_and_replay_are_enforced() {
        let (_root, store, job) = journal();
        let binding = binding(&job, ProjectGitOperation::Commit);
        let marker = "f".repeat(64);
        store.prepare(binding.clone()).unwrap();
        store.prepare(binding.clone()).unwrap();
        assert!(store
            .mark_in_flight(binding.invocation_id, &marker)
            .is_err());
        store.bind_marker(binding.invocation_id, &marker).unwrap();
        store.bind_marker(binding.invocation_id, &marker).unwrap();
        store
            .mark_in_flight(binding.invocation_id, &marker)
            .unwrap();
        assert_eq!(store.summary().unwrap().effect, GitEffect::Ambiguous);
        let final_receipt = receipt(&binding, PrivilegedGitDisposition::Applied);
        store
            .finalize(
                binding.invocation_id,
                &marker,
                PrivilegedOperationOutcome::Completed,
                final_receipt.clone(),
            )
            .unwrap();
        store
            .finalize(
                binding.invocation_id,
                &marker,
                PrivilegedOperationOutcome::Completed,
                final_receipt,
            )
            .unwrap();
        assert_eq!(store.summary().unwrap().effect, GitEffect::Applied);
    }

    #[test]
    fn aggregate_uses_ambiguous_then_applied_then_not_applied_precedence() {
        let (_root, store, job) = journal();
        let applied = binding(&job, ProjectGitOperation::Commit);
        advance(&store, &applied, PrivilegedGitDisposition::Applied);

        let not_applied = binding(&job, ProjectGitOperation::Push);
        // Each record must have a unique marker.
        let marker = "9".repeat(64);
        store.prepare(not_applied.clone()).unwrap();
        store
            .bind_marker(not_applied.invocation_id, &marker)
            .unwrap();
        store
            .mark_in_flight(not_applied.invocation_id, &marker)
            .unwrap();
        store
            .finalize(
                not_applied.invocation_id,
                &marker,
                PrivilegedOperationOutcome::Failed,
                receipt(&not_applied, PrivilegedGitDisposition::NotApplied),
            )
            .unwrap();
        let summary = store.summary().unwrap();
        assert_eq!(summary.effect, GitEffect::Applied);
        assert_eq!(summary.applied_count, 1);

        let ambiguous = binding(&job, ProjectGitOperation::Fetch);
        store.prepare(ambiguous.clone()).unwrap();
        store
            .bind_marker(ambiguous.invocation_id, &"8".repeat(64))
            .unwrap();
        store
            .mark_in_flight(ambiguous.invocation_id, &"8".repeat(64))
            .unwrap();
        let summary = store.summary().unwrap();
        assert_eq!(summary.effect, GitEffect::Ambiguous);
        assert_eq!(summary.ambiguous_count, 1);
        assert_eq!(summary.operation_count, 3);
    }

    #[test]
    fn producer_binding_object_and_outcome_mismatches_leave_in_flight() {
        let (_root, store, job) = journal();
        let binding = binding(&job, ProjectGitOperation::Commit);
        let marker = "f".repeat(64);
        store.prepare(binding.clone()).unwrap();
        store.bind_marker(binding.invocation_id, &marker).unwrap();
        store
            .mark_in_flight(binding.invocation_id, &marker)
            .unwrap();
        let mut wrong = receipt(&binding, PrivilegedGitDisposition::Applied);
        wrong.worker_pubkey = "0".repeat(64);
        assert!(store
            .finalize(
                binding.invocation_id,
                &marker,
                PrivilegedOperationOutcome::Completed,
                wrong,
            )
            .is_err());
        assert_eq!(store.summary().unwrap().effect, GitEffect::Ambiguous);

        let wrong_outcome = receipt(&binding, PrivilegedGitDisposition::Applied);
        assert!(store
            .finalize(
                binding.invocation_id,
                &marker,
                PrivilegedOperationOutcome::Failed,
                wrong_outcome,
            )
            .is_err());
        assert_eq!(store.summary().unwrap().effect, GitEffect::Ambiguous);
    }

    #[test]
    fn not_applied_preparation_receipt_can_fill_known_scope_only() {
        let (_root, store, job) = journal();
        let binding = binding(&job, ProjectGitOperation::Fetch);
        let marker = "f".repeat(64);
        store.prepare(binding.clone()).unwrap();
        store.bind_marker(binding.invocation_id, &marker).unwrap();
        store
            .mark_in_flight(binding.invocation_id, &marker)
            .unwrap();
        let mut sparse = receipt(&binding, PrivilegedGitDisposition::NotApplied);
        sparse.scope_digest = None;
        sparse.repository = None;
        sparse.branch_ref = None;
        sparse.previous_object = None;
        sparse.intended_object = None;
        sparse.observed_object = None;
        let normalized = store
            .finalize(
                binding.invocation_id,
                &marker,
                PrivilegedOperationOutcome::Failed,
                sparse,
            )
            .unwrap();
        assert_eq!(
            normalized.scope_digest.as_deref(),
            Some(binding.scope_digest.as_str())
        );
        assert_eq!(
            normalized.repository.as_deref(),
            Some(binding.repository.as_str())
        );
        assert_eq!(
            normalized.branch_ref.as_deref(),
            Some(binding.branch_ref.as_str())
        );
        assert_eq!(store.summary().unwrap().effect, GitEffect::NotApplied);
    }

    #[test]
    fn swapped_empty_or_mixed_job_journals_fail_closed() {
        let (root, store, job) = journal();
        let other_job = job_binding();
        let swapped = GitReceiptJournal::from_privilege_lock(
            &root.path().join("job.lock"),
            other_job.clone(),
        )
        .expect("swapped journal path");
        assert!(swapped.summary().is_err());

        assert!(store
            .prepare(binding(&other_job, ProjectGitOperation::Commit))
            .is_err());
        assert_eq!(store.summary().unwrap().effect, GitEffect::NotApplied);
        assert_ne!(job, other_job);
    }
}
