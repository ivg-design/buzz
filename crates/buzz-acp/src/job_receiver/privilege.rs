//! ACP-owned lifecycle fence for trusted, job-scoped Git operations.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use buzz_core::job::{
    semantic_request_digest, JobControlAction, JobEvent, JobProgressStatus, JobRequest, JobSponsor,
};
use buzz_dev_mcp::{
    JobPrivilegeGate as TrustedJobPrivilegeGate, PrivilegeFuture, PrivilegedGitOperationReceipt,
    PrivilegedOperationOutcome, ProjectGitOperation, TrustedGitOperationLease,
};
use chrono::{DateTime, Utc};
use fs2::FileExt as _;
use nostr::Event;
use sha2::{Digest, Sha256};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::authority;
use super::git_receipt_journal::{
    GitEffect, GitEffectSummary, GitReceiptBinding, GitReceiptJournal,
};
use super::grants::GrantSet;
use super::ledger::{JobLedger, StoredClaim};
use super::lifecycle::LifecycleStore;
use super::JobEmitter;
use crate::relay::{AuthenticatedContext, RestClient};
use crate::scope::SessionScope;

const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(20);
// The trusted runner reaps a cancelled child before releasing its operation
// lease. Keep revocation bounded even if that invariant regresses: callers
// must not publish a terminal lifecycle event when this drain times out.
const PRIVILEGE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Process-wide lookup used when an ACP provider session is created after job
/// admission. Entries contain opaque lifecycle capabilities, never raw keys.
#[derive(Clone, Default)]
pub(crate) struct JobPrivilegeRegistry {
    entries: Arc<RwLock<HashMap<SessionScope, Arc<JobPrivilege>>>>,
}

impl JobPrivilegeRegistry {
    pub(crate) fn register(
        &self,
        scope: SessionScope,
        gate: Arc<JobPrivilege>,
    ) -> Result<(), String> {
        if gate.scope != scope {
            return Err("job privilege scope does not match its registry key".into());
        }
        let mut entries = self
            .entries
            .write()
            .map_err(|_| "job privilege registry is unavailable".to_owned())?;
        if let Some(existing) = entries.get(&scope) {
            if Arc::ptr_eq(existing, &gate) {
                return Ok(());
            }
            return Err("a job privilege is already registered for this scope".into());
        }
        entries.insert(scope, gate);
        Ok(())
    }

    pub(crate) fn for_session(
        &self,
        scope: &SessionScope,
        working_directory: &Path,
    ) -> Result<Option<Arc<dyn TrustedJobPrivilegeGate>>, String> {
        if !scope.is_job() {
            return Ok(None);
        }
        let gate = self
            .entries
            .read()
            .map_err(|_| "job privilege registry is unavailable".to_owned())?
            .get(scope)
            .cloned()
            .ok_or_else(|| "job privilege lifecycle capability is unavailable".to_owned())?;
        let directory = working_directory
            .canonicalize()
            .map_err(|_| "job privilege checkout is unavailable".to_owned())?;
        if directory != gate.checkout_root {
            return Err("job privilege checkout does not match the admitted request".into());
        }
        Ok(Some(gate))
    }

    pub(crate) fn revoke(&self, scope: &SessionScope) {
        if let Ok(entries) = self.entries.read() {
            if let Some(gate) = entries.get(scope) {
                gate.revoked.cancel();
            }
        }
    }

    /// Revoke one job capability, then wait until any active privileged child
    /// has reaped and released its durable operation lease.
    pub(crate) async fn revoke_and_wait(&self, scope: &SessionScope) -> Result<(), String> {
        let gate = self
            .entries
            .read()
            .map_err(|_| "job privilege registry is unavailable".to_owned())?
            .get(scope)
            .cloned();
        let Some(gate) = gate else {
            return Ok(());
        };

        // Cancellation must be observable by the child before we contend on
        // the lock it owns. OperationLease::finish unlocks only after reap.
        gate.revoked.cancel();
        wait_for_file_lock_release(
            &gate.lock_path,
            gate.lock_identity,
            Instant::now() + PRIVILEGE_DRAIN_TIMEOUT,
        )
        .await
    }

    /// Read the aggregate only after the caller has drained the privilege
    /// lock. Missing or corrupt durable state is an error and must be surfaced
    /// as an indeterminate terminal, never as a safe cancellation.
    pub(crate) fn git_effect_summary(
        &self,
        scope: &SessionScope,
    ) -> Result<GitEffectSummary, String> {
        let gate = self
            .entries
            .read()
            .map_err(|_| "job privilege registry is unavailable".to_owned())?
            .get(scope)
            .cloned()
            .ok_or_else(|| "job privilege lifecycle capability is unavailable".to_owned())?;
        gate.git_receipts
            .summary()
            .map_err(|error| error.to_string())
    }

    pub(crate) fn remove(&self, scope: &SessionScope) {
        if let Ok(mut entries) = self.entries.write() {
            if let Some(gate) = entries.remove(scope) {
                gate.revoked.cancel();
            }
        }
    }

    pub(crate) fn revoke_channel(&self, channel_id: Uuid) {
        if let Ok(entries) = self.entries.read() {
            for (scope, gate) in entries.iter() {
                if scope.channel_id() == channel_id {
                    gate.revoked.cancel();
                }
            }
        }
    }

    /// Revoke every capability in a removed channel and wait under one shared
    /// deadline for all active operation leases to drain.
    pub(crate) async fn revoke_channel_and_wait(&self, channel_id: Uuid) -> Result<(), String> {
        let gates = self
            .entries
            .read()
            .map_err(|_| "job privilege registry is unavailable".to_owned())?
            .iter()
            .filter(|(scope, _)| scope.channel_id() == channel_id)
            .map(|(_, gate)| gate.clone())
            .collect::<Vec<_>>();

        for gate in &gates {
            gate.revoked.cancel();
        }
        let deadline = Instant::now() + PRIVILEGE_DRAIN_TIMEOUT;
        for gate in gates {
            wait_for_file_lock_release(&gate.lock_path, gate.lock_identity, deadline).await?;
        }
        Ok(())
    }
}

/// Exact admitted-job authority retained solely by the ACP harness.
#[derive(Clone)]
pub(crate) struct JobPrivilege {
    scope: SessionScope,
    tenant: AuthenticatedContext,
    agent_pubkey: String,
    rest: RestClient,
    sponsor: JobSponsor,
    grants: GrantSet,
    ledger: JobLedger,
    claim: StoredClaim,
    request: JobRequest,
    emitter: JobEmitter,
    lifecycle: LifecycleStore,
    checkout_root: PathBuf,
    lock_path: PathBuf,
    lock_identity: LockIdentity,
    git_receipts: GitReceiptJournal,
    revoked: CancellationToken,
    allow_insecure_loopback: bool,
}

impl JobPrivilege {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        scope: SessionScope,
        tenant: AuthenticatedContext,
        agent_pubkey: String,
        rest: RestClient,
        sponsor: JobSponsor,
        grants: GrantSet,
        ledger: JobLedger,
        claim: StoredClaim,
        request: JobRequest,
        emitter: JobEmitter,
        lifecycle: LifecycleStore,
        checkout_root: PathBuf,
        allow_insecure_loopback: bool,
    ) -> Result<Arc<Self>, String> {
        let lock_path = checkout_privilege_lock_path(&lifecycle, &checkout_root)?;
        let lock_identity = initialize_lock_file(&lock_path)?;
        let git_receipts =
            GitReceiptJournal::for_claim(&lifecycle, &tenant.community_id, &agent_pubkey, &claim)
                .map_err(|error| error.to_string())?;
        // This empty sentinel is fsynced before the admitted prompt can enter
        // the model pool. Its absence later is therefore an integrity failure,
        // not evidence that no Git side effect occurred.
        git_receipts
            .initialize()
            .map_err(|error| error.to_string())?;
        Ok(Arc::new(Self {
            scope,
            tenant,
            agent_pubkey,
            rest,
            sponsor,
            grants,
            ledger,
            claim,
            request,
            emitter,
            lifecycle,
            checkout_root,
            lock_path,
            lock_identity,
            git_receipts,
            revoked: CancellationToken::new(),
            allow_insecure_loopback,
        }))
    }

    async fn begin_operation(
        &self,
        operation: ProjectGitOperation,
        invocation_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<Box<dyn TrustedGitOperationLease>, String> {
        let request_expires_at = ensure_request_live_at(&self.request, Utc::now())?;
        let request_deadline = monotonic_deadline(
            request_expires_at,
            "signed job request expired before privileged operation start",
        )?;
        let lock = acquire_file_lock(
            &self.lock_path,
            self.lock_identity,
            &cancellation,
            &self.revoked,
        )
        .await?;
        self.validate_active(operation).await?;
        if cancellation.is_cancelled() || self.revoked.is_cancelled() {
            return Err("job privilege was cancelled before execution".into());
        }
        ensure_deadline_live(
            request_deadline,
            "signed job request expired before privileged operation start",
        )?;

        if operation != ProjectGitOperation::Handoff {
            self.git_receipts
                .prepare(self.git_receipt_binding(operation, invocation_id)?)
                .map_err(|error| format!("preparing durable Git receipt: {error}"))?;
        }

        let marker_id = match self
            .emitter
            .progress(
                JobProgressStatus::Progress,
                format!(
                    "privileged-operation:{}:{}",
                    operation.as_str(),
                    invocation_id
                ),
                Vec::new(),
            )
            .await
        {
            Ok(marker_id) => marker_id,
            Err(error) => {
                // The emitter may already have frozen a pending marker. Once a
                // marker attempt exists, this gate must never silently retry an
                // invocation whose execution state is ambiguous.
                self.revoked.cancel();
                return Err(format!("fencing privileged operation: {error}"));
            }
        };

        if operation != ProjectGitOperation::Handoff {
            if let Err(error) = self.git_receipts.bind_marker(invocation_id, &marker_id) {
                self.revoked.cancel();
                return Err(format!("binding durable Git receipt marker: {error}"));
            }
        }

        let result = async {
            if cancellation.is_cancelled() || self.revoked.is_cancelled() {
                return Err("job privilege was cancelled before final authorization".into());
            }
            ensure_deadline_live(
                request_deadline,
                "signed job request expired before privileged operation start",
            )?;

            // This is deliberately the final server-owned authority lookup.
            // The signed marker is relay-acknowledged first, so authorization
            // can be checked as close as possible to process creation without
            // leaving an unrecorded privileged attempt.
            let authorization_expires_at = authority::authorize(
                &self.rest,
                &self.tenant,
                &self.request,
                &self.claim.request_event_id,
                &self.claim.digest,
                &self.sponsor,
                self.allow_insecure_loopback,
            )
            .await
            .map_err(|error| error.to_string())?;
            let authorization_deadline = monotonic_deadline(
                authorization_expires_at,
                "job authorization expired before privileged operation start",
            )?;
            let operation_deadline = request_deadline.min(authorization_deadline);

            // Re-read every local durable fence after the final authority call.
            // Cancellation and both deadlines are then checked again
            // immediately before the opaque lease crosses into the typed
            // runner.
            self.validate_active(operation).await?;
            if cancellation.is_cancelled() || self.revoked.is_cancelled() {
                return Err("job privilege was cancelled during final authorization".into());
            }
            ensure_deadline_live(
                request_deadline,
                "signed job request expired before privileged operation start",
            )?;
            ensure_deadline_live(
                authorization_deadline,
                "job authorization expired before privileged operation start",
            )?;

            let lease_cancellation = self.revoked.child_token();
            let deadline_cancellation = lease_cancellation.clone();
            let deadline_task = tokio::spawn(async move {
                tokio::time::sleep_until(operation_deadline).await;
                deadline_cancellation.cancel();
            });
            if operation != ProjectGitOperation::Handoff {
                if let Err(error) = self.git_receipts.mark_in_flight(invocation_id, &marker_id) {
                    deadline_task.abort();
                    lease_cancellation.cancel();
                    return Err(format!("fencing in-flight Git receipt: {error}"));
                }
            }
            if cancellation.is_cancelled() || self.revoked.is_cancelled() {
                deadline_task.abort();
                lease_cancellation.cancel();
                if operation != ProjectGitOperation::Handoff {
                    self.git_receipts
                        .finalize_unstarted(
                            invocation_id,
                            &marker_id,
                            PrivilegedOperationOutcome::Cancelled,
                        )
                        .map_err(|error| {
                            format!("closing cancelled pre-execution Git receipt: {error}")
                        })?;
                }
                return Err("job privilege was cancelled before process start".into());
            }
            if let Err(error) = ensure_deadline_live(
                operation_deadline,
                "job authority expired before privileged operation start",
            ) {
                deadline_task.abort();
                lease_cancellation.cancel();
                if operation != ProjectGitOperation::Handoff {
                    self.git_receipts
                        .finalize_unstarted(
                            invocation_id,
                            &marker_id,
                            PrivilegedOperationOutcome::Cancelled,
                        )
                        .map_err(|journal_error| {
                            format!("closing expired pre-execution Git receipt: {journal_error}")
                        })?;
                }
                return Err(error);
            }

            Ok(Box::new(OperationLease {
                gate: self.clone(),
                operation,
                invocation_id,
                marker_id,
                handoff_event_id: None,
                cancellation: lease_cancellation,
                deadline_task: Some(deadline_task),
                lock: Some(lock),
                finished: false,
            }) as Box<dyn TrustedGitOperationLease>)
        }
        .await;
        if result.is_err() {
            // A confirmed marker now records this invocation. Fail closed so a
            // final-auth or expiry failure cannot be retried as if no attempt
            // had crossed the durable lifecycle fence.
            self.revoked.cancel();
        }
        result
    }

    fn git_receipt_binding(
        &self,
        operation: ProjectGitOperation,
        invocation_id: Uuid,
    ) -> Result<GitReceiptBinding, String> {
        let branch_ref = match operation {
            ProjectGitOperation::Commit | ProjectGitOperation::Push => {
                format!("refs/heads/{}", self.request.common.repository.branch)
            }
            ProjectGitOperation::Fetch => {
                format!(
                    "refs/remotes/origin/{}",
                    self.request.common.repository.branch
                )
            }
            ProjectGitOperation::Handoff => {
                return Err("handoff must not create a Git receipt binding".into());
            }
        };
        let SessionScope::Job {
            channel_id,
            operation_id,
            request_event_id,
        } = &self.scope
        else {
            return Err("privileged Git operation is not bound to a job scope".into());
        };
        Ok(GitReceiptBinding {
            invocation_id,
            operation,
            community_id: self.tenant.community_id.clone(),
            project_address: self.request.common.project.address.clone(),
            session_channel_id: channel_id.to_string(),
            operation_id: operation_id.clone(),
            request_event_id: request_event_id.clone(),
            requester_pubkey: self.claim.requester.clone(),
            worker_pubkey: self.agent_pubkey.clone(),
            scope_digest: self.claim.digest.clone(),
            repository: self.request.common.repository.canonical.clone(),
            branch_ref,
        })
    }

    async fn validate_active(&self, operation: ProjectGitOperation) -> Result<(), String> {
        let durable_claim = self
            .ledger
            .reload_claim(&self.claim)
            .await
            .map_err(|error| format!("reloading durable job claim: {error}"))?;
        validate_stored_claim(self, &durable_claim, Utc::now())?;
        if !self
            .ledger
            .prompt_started(&self.claim)
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("job prompt has not crossed its durable start fence".into());
        }
        let snapshot = self
            .lifecycle
            .privilege_snapshot()
            .await
            .map_err(|error| error.to_string())?;
        if snapshot.accepted_event_id != self.claim.accepted.id.to_hex()
            || !is_event_id(&snapshot.head_event_id)
            || snapshot.pending_outbox.is_some()
            || snapshot.cancel_event_id.is_some()
            || snapshot.terminal
        {
            return Err("job lifecycle is not active for a privileged operation".into());
        }
        if self.revoked.is_cancelled() {
            return Err("job privilege has been revoked".into());
        }
        if self
            .git_receipts
            .summary()
            .map_err(|error| format!("reading durable Git receipt state: {error}"))?
            .effect
            == GitEffect::Ambiguous
        {
            return Err(
                "an unresolved Git invocation blocks additional privileged operations".into(),
            );
        }
        if operation != ProjectGitOperation::Handoff {
            let operations = self
                .grants
                .git_operations_for(&self.request)
                .ok_or_else(|| "job no longer matches an exact local grant".to_owned())?;
            if !operations
                .iter()
                .any(|allowed| allowed == operation.as_str())
            {
                return Err(format!(
                    "local grant does not allow trusted Git {}",
                    operation.as_str()
                ));
            }
        }
        Ok(())
    }

    fn validate_handoff_event(&self, marker_id: &str, event: &Event) -> Result<(), String> {
        event
            .verify()
            .map_err(|error| format!("privileged handoff signature: {error}"))?;
        let JobEvent::Control(control) =
            JobEvent::parse(event).map_err(|error| error.to_string())?
        else {
            return Err("privileged handoff result is not a control event".into());
        };
        let expected_common = response_common(&self.request, &self.agent_pubkey, &self.sponsor);
        if control.action != JobControlAction::Handoff
            || control.followup.request_event_id != self.claim.request_event_id
            || control.followup.prior_event_id.as_deref() != Some(marker_id)
            || control.followup.common != expected_common
            || control.handoff_to.is_none()
        {
            return Err("privileged handoff does not match the fenced job lifecycle".into());
        }
        Ok(())
    }
}

/// Serialize trusted Git operations across every job admitted for the same
/// canonical checkout. Receipt journals remain lifecycle-specific; only the
/// execution lease is checkout-scoped so a timed-out prior child cannot
/// overlap a successor job mutating the same repository.
fn checkout_privilege_lock_path(
    lifecycle: &LifecycleStore,
    checkout_root: &Path,
) -> Result<PathBuf, String> {
    let canonical_checkout = checkout_root
        .canonicalize()
        .map_err(|_| "job privilege checkout is unavailable".to_owned())?;
    let lifecycle_lock = lifecycle.privilege_lock_path();
    let ledger_root = lifecycle_lock
        .parent()
        .ok_or_else(|| "job privilege lock has no private ledger root".to_owned())?;
    let mut digest = Sha256::new();
    digest.update(b"buzz-acp-checkout-privilege-lock-v1\0");
    update_path_digest(&mut digest, &canonical_checkout);
    Ok(ledger_root.join(format!(
        "checkout-{}.privilege.lock",
        hex::encode(digest.finalize())
    )))
}

#[cfg(unix)]
fn update_path_digest(digest: &mut Sha256, path: &Path) {
    use std::os::unix::ffi::OsStrExt as _;
    digest.update(path.as_os_str().as_bytes());
}

#[cfg(windows)]
fn update_path_digest(digest: &mut Sha256, path: &Path) {
    use std::os::windows::ffi::OsStrExt as _;
    for unit in path.as_os_str().encode_wide() {
        digest.update(unit.to_le_bytes());
    }
}

#[cfg(not(any(unix, windows)))]
fn update_path_digest(digest: &mut Sha256, path: &Path) {
    digest.update(path.as_os_str().to_string_lossy().as_bytes());
}

impl TrustedJobPrivilegeGate for JobPrivilege {
    fn begin<'a>(
        &'a self,
        operation: ProjectGitOperation,
        invocation_id: Uuid,
        cancellation: CancellationToken,
    ) -> PrivilegeFuture<'a, Result<Box<dyn TrustedGitOperationLease>, String>> {
        Box::pin(self.begin_operation(operation, invocation_id, cancellation))
    }
}

struct OperationLease {
    gate: JobPrivilege,
    operation: ProjectGitOperation,
    invocation_id: Uuid,
    marker_id: String,
    handoff_event_id: Option<String>,
    cancellation: CancellationToken,
    deadline_task: Option<tokio::task::JoinHandle<()>>,
    lock: Option<File>,
    finished: bool,
}

impl TrustedGitOperationLease for OperationLease {
    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    fn stage_handoff<'a>(
        &'a mut self,
        event: Event,
        cancellation: CancellationToken,
    ) -> PrivilegeFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if self.operation != ProjectGitOperation::Handoff {
                return Err("Git operation leases cannot stage a Handoff".into());
            }
            if self.handoff_event_id.is_some() {
                return Err("privileged Handoff is already durably staged".into());
            }
            if cancellation.is_cancelled()
                || self.cancellation.is_cancelled()
                || self.gate.revoked.is_cancelled()
            {
                return Err("privileged Handoff was cancelled before durable staging".into());
            }
            if let Err(error) = self.gate.validate_handoff_event(&self.marker_id, &event) {
                self.gate.revoked.cancel();
                return Err(error);
            }
            let event_id = event.id.to_hex();
            if let Err(error) = self
                .gate
                .lifecycle
                .stage(event, true, self.marker_id.clone())
                .await
            {
                self.gate.revoked.cancel();
                return Err(format!("staging durable Handoff event: {error}"));
            }
            self.handoff_event_id = Some(event_id);
            Ok(())
        })
    }

    fn finish(
        mut self: Box<Self>,
        outcome: PrivilegedOperationOutcome,
        git_receipt: Option<PrivilegedGitOperationReceipt>,
        terminal_event_id: Option<String>,
    ) -> PrivilegeFuture<'static, Result<(), String>> {
        Box::pin(async move {
            if let Some(deadline_task) = self.deadline_task.take() {
                deadline_task.abort();
            }
            let result = if self.operation == ProjectGitOperation::Handoff {
                if git_receipt.is_some() {
                    Err("privileged handoff must not return a Git receipt".into())
                } else if outcome == PrivilegedOperationOutcome::Completed {
                    match terminal_event_id {
                        Some(event_id)
                            if self.handoff_event_id.as_deref() == Some(event_id.as_str()) =>
                        {
                            let result =
                                self.gate
                                    .lifecycle
                                    .confirm(event_id)
                                    .await
                                    .map_err(|error| {
                                        format!("confirming durable Handoff event: {error}")
                                    });
                            if result.is_ok() {
                                self.gate.revoked.cancel();
                            }
                            result
                        }
                        Some(_) => Err(
                            "successful privileged handoff returned a different terminal event id"
                                .into(),
                        ),
                        None => Err(
                            "successful privileged handoff omitted its terminal event id".into(),
                        ),
                    }
                } else if terminal_event_id.is_some() {
                    Err("only a successful handoff may return a terminal event id".into())
                } else {
                    if self.handoff_event_id.is_some() {
                        // The exact event remains frozen for retry after an
                        // uncertain/failed submission. No sibling operation or
                        // terminal may cross that lifecycle transition.
                        self.gate.revoked.cancel();
                    }
                    Ok(())
                }
            } else if terminal_event_id.is_some() {
                Err("Git operations must not return a lifecycle terminal event id".into())
            } else {
                let receipt = git_receipt.ok_or_else(|| {
                    "privileged Git operation omitted its durable receipt".to_owned()
                });
                match receipt.and_then(|receipt| {
                    self.gate
                        .git_receipts
                        .finalize(self.invocation_id, &self.marker_id, outcome, receipt)
                        .map_err(|error| error.to_string())
                }) {
                    Ok(receipt) => {
                        if receipt.disposition == buzz_dev_mcp::PrivilegedGitDisposition::Ambiguous
                        {
                            self.gate.revoked.cancel();
                        }
                        Ok(())
                    }
                    Err(error) => Err(format!("persisting final Git receipt: {error}")),
                }
            };
            if result.is_err() {
                // Missing, mismatched, or unpersisted producer state leaves the
                // durable record InFlight (therefore ambiguous) and blocks any
                // later privileged retry for this job.
                self.gate.revoked.cancel();
            }
            if let Some(lock) = self.lock.take() {
                let _ = fs2::FileExt::unlock(&lock);
            }
            self.finished = true;
            result
        })
    }
}

impl Drop for OperationLease {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some(deadline_task) = self.deadline_task.take() {
            deadline_task.abort();
        }
        self.cancellation.cancel();
        self.gate.revoked.cancel();
        if let Some(lock) = self.lock.take() {
            let _ = fs2::FileExt::unlock(&lock);
        }
    }
}

fn validate_stored_claim(
    gate: &JobPrivilege,
    durable: &StoredClaim,
    now: DateTime<Utc>,
) -> Result<(), String> {
    let request_id = durable.request_event.id.to_hex();
    let parsed = JobEvent::parse(&durable.request_event).map_err(|error| error.to_string())?;
    let JobEvent::Request(request) = parsed else {
        return Err("stored claim root is not a request".into());
    };
    let digest = semantic_request_digest(&request).map_err(|error| error.to_string())?;
    let (scope_channel, scope_operation, scope_request) = match &gate.scope {
        SessionScope::Job {
            channel_id,
            operation_id,
            request_event_id,
        } => (channel_id.to_string(), operation_id, request_event_id),
        _ => return Err("privileged operation is not bound to a job scope".into()),
    };
    if durable.community != gate.tenant.community_id
        || gate.tenant.pubkey != gate.agent_pubkey
        || durable.requester != request.common.sender_pubkey
        || durable.idempotency_key != request.common.idempotency_key
        || durable.request_event_id != request_id
        || durable.request_event_id != *scope_request
        || request.common.operation_id != *scope_operation
        || request.common.project.home_channel != scope_channel
        || request.common.recipient_pubkey != gate.agent_pubkey
        || durable.digest != digest
        || request != gate.request
    {
        return Err("stored claim does not match the exact job session binding".into());
    }
    ensure_request_live_at(&request, now)?;
    super::verified_durable_response_common(durable, &gate.agent_pubkey, &gate.sponsor.pubkey)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn ensure_request_live_at(
    request: &JobRequest,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>, String> {
    let expires_at = DateTime::parse_from_rfc3339(&request.common.expires_at)
        .map_err(|_| "signed job request expiry is invalid".to_owned())?
        .with_timezone(&Utc);
    if expires_at <= now {
        return Err("signed job request expired before privileged operation start".into());
    }
    Ok(expires_at)
}

fn monotonic_deadline(expires_at: DateTime<Utc>, expired_message: &str) -> Result<Instant, String> {
    let remaining = expires_at
        .signed_duration_since(Utc::now())
        .to_std()
        .map_err(|_| expired_message.to_owned())?;
    if remaining.is_zero() {
        return Err(expired_message.to_owned());
    }
    Instant::now()
        .checked_add(remaining)
        .ok_or_else(|| "job authority deadline is out of range".to_owned())
}

fn ensure_deadline_live(deadline: Instant, expired_message: &str) -> Result<(), String> {
    if Instant::now() >= deadline {
        Err(expired_message.to_owned())
    } else {
        Ok(())
    }
}

fn is_event_id(value: &str) -> bool {
    value.len() == 64
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn response_common(
    request: &JobRequest,
    agent_pubkey: &str,
    sponsor: &JobSponsor,
) -> buzz_core::job::JobCommon {
    let mut common = request.common.clone();
    common.sender_pubkey = agent_pubkey.into();
    common.recipient_pubkey = request.common.sender_pubkey.clone();
    common.sponsor = sponsor.clone();
    common
}

async fn acquire_file_lock(
    path: &Path,
    expected_identity: LockIdentity,
    cancellation: &CancellationToken,
    revoked: &CancellationToken,
) -> Result<File, String> {
    let file = open_lock_file(path, false, Some(expected_identity))?.0;
    loop {
        if cancellation.is_cancelled() {
            return Err("job privilege lock wait was cancelled".into());
        }
        if revoked.is_cancelled() {
            return Err("job privilege was revoked while waiting".into());
        }
        match file.try_lock_exclusive() {
            Ok(()) => return Ok(file),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err("job privilege lock wait was cancelled".into()),
                    _ = revoked.cancelled() => return Err("job privilege was revoked while waiting".into()),
                    _ = tokio::time::sleep(LOCK_RETRY_INTERVAL) => {}
                }
            }
            Err(_) => return Err("locking job privilege state failed".into()),
        }
    }
}

async fn wait_for_file_lock_release(
    path: &Path,
    expected_identity: LockIdentity,
    deadline: Instant,
) -> Result<(), String> {
    let file = open_lock_file(path, false, Some(expected_identity))?.0;
    loop {
        match file.try_lock_exclusive() {
            Ok(()) => {
                let _ = fs2::FileExt::unlock(&file);
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(
                        "timed out waiting for active job privilege operation to stop".into(),
                    );
                }
                tokio::time::sleep_until((now + LOCK_RETRY_INTERVAL).min(deadline)).await;
            }
            Err(_) => return Err("locking job privilege state failed".into()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LockIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

fn initialize_lock_file(path: &Path) -> Result<LockIdentity, String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|_| "creating job privilege lock directory failed".to_owned())?;
    }
    let (_, identity) = open_lock_file(path, true, None)?;
    Ok(identity)
}

#[cfg(unix)]
fn open_lock_file(
    path: &Path,
    create: bool,
    expected_identity: Option<LockIdentity>,
) -> Result<(File, LockIdentity), String> {
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(create)
        .truncate(false)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
    let file = options
        .open(path)
        .map_err(|_| "opening job privilege lock failed".to_owned())?;
    let metadata = file
        .metadata()
        .map_err(|_| "reading job privilege lock identity failed".to_owned())?;
    if !metadata.is_file()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.nlink() != 1
    {
        return Err("job privilege lock must be an operator-owned, owner-only regular file".into());
    }
    let identity = LockIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    if expected_identity.is_some_and(|expected| expected != identity) {
        return Err("job privilege lock identity changed".into());
    }
    Ok((file, identity))
}

#[cfg(not(unix))]
fn open_lock_file(
    _path: &Path,
    _create: bool,
    _expected_identity: Option<LockIdentity>,
) -> Result<(File, LockIdentity), String> {
    Err("job privilege locks require owner-only no-follow file support".into())
}

#[cfg(test)]
mod tests;
