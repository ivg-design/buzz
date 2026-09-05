mod authority;
mod cancel;
mod emitter;
mod git_receipt_journal;
mod grants;
mod lease;
mod ledger;
mod lifecycle;
mod outcome;
mod paths;
mod privilege;
mod prompt;
mod receipts;
mod revocation;
#[cfg_attr(windows, allow(unsafe_code))]
#[cfg(windows)]
mod windows_private;

use std::path::{Path, PathBuf};

use buzz_core::job::{
    semantic_request_digest, JobClaimStatus, JobCommon, JobEvent, JobRequest, JobSponsor,
    MAX_JOB_TTL_SECONDS,
};
use chrono::{DateTime, Utc};
use nostr::{Event, Keys};
use thiserror::Error;
use uuid::Uuid;

pub use emitter::JobEmitter;
use emitter::{build_claim_receipts, build_declined_receipt};
use git_receipt_journal::GitEffect;
pub(crate) use grants::prepare_job_sources;
use grants::{GrantError, GrantSet};
use lease::ReceiverLease;
use ledger::{
    ClaimDecision, DeclineDecision, DeclineLookup, JobLedger, LedgerError, StoredClaim,
    StoredDecline,
};
use lifecycle::LifecycleError;
pub use outcome::{parse_terminal_outcome, TerminalDisposition};
pub(crate) use privilege::{JobPrivilege, JobPrivilegeRegistry};
pub use prompt::format_job_prompt;

const SETUP_FAILURE_REASON: &str = "workspace_setup_failed";

pub(crate) fn guard_terminal_with_git_effect(
    disposition: TerminalDisposition,
    summary: Result<git_receipt_journal::GitEffectSummary, String>,
) -> TerminalDisposition {
    match summary {
        Ok(summary)
            if summary.effect == GitEffect::Applied
                && matches!(
                    disposition,
                    TerminalDisposition::Failed {
                        retryable: true,
                        ..
                    }
                ) =>
        {
            TerminalDisposition::Indeterminate {
                code: "applied_git_operation".into(),
                message: format!(
                    "{} of {} privileged Git operations have a durable applied effect; automatic retry is unsafe and repository state requires reconciliation",
                    summary.applied_count, summary.operation_count
                ),
            }
        }
        Ok(summary) if summary.effect != GitEffect::Ambiguous => disposition,
        Ok(summary) => TerminalDisposition::Indeterminate {
            code: "ambiguous_git_operation".into(),
            message: format!(
                "{} of {} privileged Git operations have an ambiguous durable effect; repository state requires reconciliation",
                summary.ambiguous_count, summary.operation_count
            ),
        },
        Err(_) => TerminalDisposition::Indeterminate {
            code: "git_receipt_journal_unavailable".into(),
            message: "The durable Git receipt journal is missing or invalid; repository state requires reconciliation".into(),
        },
    }
}

/// Verify the immutable request and Processed -> Accepted chain, then return
/// the exact worker-authored common coordinates frozen in those receipts.
/// Sponsor login is audit metadata and may legitimately change across a
/// restart; worker and owner public-key bindings remain authoritative.
fn verified_durable_response_common(
    claim: &StoredClaim,
    agent_pubkey: &str,
    current_sponsor_pubkey: &str,
) -> Result<JobCommon, ReceiverError> {
    claim
        .request_event
        .verify()
        .map_err(|error| ReceiverError::Receipt(format!("stored request signature: {error}")))?;
    claim
        .processed
        .verify()
        .map_err(|error| ReceiverError::Receipt(format!("stored Processed signature: {error}")))?;
    claim
        .accepted
        .verify()
        .map_err(|error| ReceiverError::Receipt(format!("stored Accepted signature: {error}")))?;

    let JobEvent::Request(request) = JobEvent::parse(&claim.request_event)
        .map_err(|error| ReceiverError::Receipt(format!("stored request event: {error}")))?
    else {
        return Err(ReceiverError::Receipt(
            "stored lifecycle root is not a job request".into(),
        ));
    };
    let JobEvent::Accepted(processed) = JobEvent::parse(&claim.processed)
        .map_err(|error| ReceiverError::Receipt(format!("stored Processed event: {error}")))?
    else {
        return Err(ReceiverError::Receipt(
            "stored Processed receipt is not kind 43002".into(),
        ));
    };
    let JobEvent::Accepted(accepted) = JobEvent::parse(&claim.accepted)
        .map_err(|error| ReceiverError::Receipt(format!("stored Accepted event: {error}")))?
    else {
        return Err(ReceiverError::Receipt(
            "stored Accepted receipt is not kind 43002".into(),
        ));
    };

    let request_id = claim.request_event.id.to_hex();
    let digest = semantic_request_digest(&request)
        .map_err(|error| ReceiverError::Receipt(error.to_string()))?;
    let response_common = processed.followup.common.clone();
    let mut expected_common = request.common.clone();
    expected_common.sender_pubkey = agent_pubkey.to_owned();
    expected_common.recipient_pubkey = request.common.sender_pubkey.clone();
    expected_common.sponsor = response_common.sponsor.clone();
    if claim.request_event_id != request_id
        || claim.requester != request.common.sender_pubkey
        || claim.idempotency_key != request.common.idempotency_key
        || claim.digest != digest
        || claim.processed.pubkey.to_hex() != agent_pubkey
        || claim.accepted.pubkey.to_hex() != agent_pubkey
        || response_common != expected_common
        || response_common.sponsor.pubkey != current_sponsor_pubkey
        || processed.claim.status != JobClaimStatus::Processed
        || processed.claim.scope_digest != digest
        || processed.followup.request_event_id != request_id
        || processed.followup.prior_event_id.is_some()
        || accepted.claim.status != JobClaimStatus::Accepted
        || accepted.claim.scope_digest != digest
        || accepted.followup.common != response_common
        || accepted.followup.request_event_id != request_id
        || accepted.followup.prior_event_id.as_deref() != Some(claim.processed.id.to_hex().as_str())
    {
        return Err(ReceiverError::Receipt(
            "stored claim receipts do not match the exact accepted chain or current key bindings"
                .into(),
        ));
    }
    Ok(response_common)
}

fn verified_durable_decline(
    decline: &StoredDecline,
    agent_pubkey: &str,
    current_sponsor_pubkey: &str,
) -> Result<JobCommon, ReceiverError> {
    decline
        .request_event
        .verify()
        .map_err(|error| ReceiverError::Receipt(format!("stored request signature: {error}")))?;
    decline
        .declined
        .verify()
        .map_err(|error| ReceiverError::Receipt(format!("stored Declined signature: {error}")))?;
    let JobEvent::Request(request) = JobEvent::parse(&decline.request_event)
        .map_err(|error| ReceiverError::Receipt(format!("stored request event: {error}")))?
    else {
        return Err(ReceiverError::Receipt(
            "stored decline root is not a job request".into(),
        ));
    };
    let JobEvent::Accepted(receipt) = JobEvent::parse(&decline.declined)
        .map_err(|error| ReceiverError::Receipt(format!("stored Declined event: {error}")))?
    else {
        return Err(ReceiverError::Receipt(
            "stored Declined receipt is not kind 43002".into(),
        ));
    };

    let request_id = decline.request_event.id.to_hex();
    let digest = semantic_request_digest(&request)
        .map_err(|error| ReceiverError::Receipt(error.to_string()))?;
    let mut expected_common = request.common.clone();
    expected_common.sender_pubkey = agent_pubkey.to_owned();
    expected_common.recipient_pubkey = request.common.sender_pubkey.clone();
    expected_common.sponsor = receipt.followup.common.sponsor.clone();
    if decline.request_event_id != request_id
        || decline.requester != request.common.sender_pubkey
        || decline.idempotency_key != request.common.idempotency_key
        || decline.digest != digest
        || decline.declined.pubkey.to_hex() != agent_pubkey
        || receipt.followup.common != expected_common
        || receipt.followup.common.sponsor.pubkey != current_sponsor_pubkey
        || receipt.followup.request_event_id != request_id
        || receipt.followup.prior_event_id.is_some()
        || receipt.claim.status != JobClaimStatus::Declined
        || receipt.claim.scope_digest != digest
        || receipt.claim.reason.as_deref() != Some(SETUP_FAILURE_REASON)
    {
        return Err(ReceiverError::Receipt(
            "stored Declined receipt does not match its exact request and current key bindings"
                .into(),
        ));
    }
    Ok(receipt.followup.common)
}

/// A terminal already frozen in the lifecycle outbox predates the current
/// process, so replay it only when the durable Git journal proves that its
/// semantics are still possible. In particular, an old Cancelled event cannot
/// hide an earlier applied Git operation, and no terminal may cross an
/// unresolved or unavailable journal.
fn validate_pending_terminal_git_effect(
    lifecycle: &lifecycle::LifecycleStore,
    event: &Event,
    expected_head: &str,
    claim: &StoredClaim,
    agent_pubkey: &str,
    current_sponsor: &JobSponsor,
) -> Result<(), ReceiverError> {
    event.verify().map_err(|error| {
        ReceiverError::Receipt(format!("stored lifecycle event signature: {error}"))
    })?;
    let expected_common =
        verified_durable_response_common(claim, agent_pubkey, &current_sponsor.pubkey)?;

    let parsed = JobEvent::parse(event)
        .map_err(|error| ReceiverError::Receipt(format!("stored lifecycle event: {error}")))?;
    let (followup, cancelled, safe_without_journal, retryable_failure) = match &parsed {
        JobEvent::Progress(progress) => (&progress.followup, false, true, false),
        JobEvent::Result(result) => (&result.followup, false, false, false),
        JobEvent::Error(error) => (
            &error.followup,
            false,
            error.outcome == buzz_core::job::JobErrorOutcome::Indeterminate && !error.retryable,
            error.outcome == buzz_core::job::JobErrorOutcome::Failed && error.retryable,
        ),
        JobEvent::Control(control)
            if matches!(
                control.action,
                buzz_core::job::JobControlAction::Cancelled
                    | buzz_core::job::JobControlAction::Release
                    | buzz_core::job::JobControlAction::Handoff
            ) =>
        {
            (
                &control.followup,
                control.action == buzz_core::job::JobControlAction::Cancelled,
                false,
                false,
            )
        }
        _ => {
            return Err(ReceiverError::Receipt(
                "stored lifecycle outbox contains an invalid transition kind".into(),
            ));
        }
    };
    if followup.common != expected_common
        || followup.request_event_id != claim.request_event_id
        || followup.prior_event_id.as_deref() != Some(expected_head)
    {
        return Err(ReceiverError::Receipt(
            "stored lifecycle event does not match its exact request chain".into(),
        ));
    }
    if safe_without_journal {
        return Ok(());
    }
    let summary = git_receipt_journal::summary_for_lifecycle(
        lifecycle,
        &claim.community,
        agent_pubkey,
        claim,
    )
    .map_err(|error| ReceiverError::Privilege(error.to_string()))?;
    if summary.effect == GitEffect::Ambiguous {
        return Err(ReceiverError::Privilege(format!(
            "refusing to replay a frozen terminal across {} ambiguous Git operation(s)",
            summary.ambiguous_count
        )));
    }
    if cancelled && summary.effect == GitEffect::Applied {
        return Err(ReceiverError::Privilege(format!(
            "refusing to replay Cancelled after {} applied Git operation(s)",
            summary.applied_count
        )));
    }
    if retryable_failure && summary.effect == GitEffect::Applied {
        return Err(ReceiverError::Privilege(format!(
            "refusing to replay retryable Failed after {} applied Git operation(s)",
            summary.applied_count
        )));
    }
    Ok(())
}

use crate::prompt_project::PromptProjectInfo;
use crate::relay::{AuthenticatedContext, RelayError, RestClient};
use crate::scope::SessionScope;

#[derive(Debug, Error)]
pub enum ReceiverError {
    #[error(transparent)]
    Grants(#[from] GrantError),
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    Relay(#[from] RelayError),
    #[error("building job receipts: {0}")]
    Receipt(String),
    #[error("authenticated tenant binding failed: {0}")]
    Tenant(String),
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error("job privilege registry failed: {0}")]
    Privilege(String),
}

pub struct JobDispatch {
    pub scope: SessionScope,
    pub event: Event,
    pub emitter: JobEmitter,
    pub claim: StoredClaim,
    pub checkout_root: PathBuf,
    pub(crate) privilege: std::sync::Arc<JobPrivilege>,
}

pub enum HandleOutcome {
    Consumed,
    Dispatch(Box<JobDispatch>),
}

pub use cancel::CancelOutcome;
pub(crate) use cancel::{CancellationTerminal, JobCancel};

#[derive(Clone, Default)]
pub struct ReceiverSources {
    pub grants_json: Option<String>,
    pub grants_file: Option<PathBuf>,
    pub ledger_root: Option<PathBuf>,
    pub allow_insecure_loopback: bool,
    pub nemo_workspace: bool,
}

/// Durable admission boundary for addressed agent-job requests.
pub struct JobReceiver {
    tenant: AuthenticatedContext,
    tenant_generation: u64,
    tenant_invalid: bool,
    agent_pubkey: String,
    keys: Keys,
    rest: RestClient,
    sponsor: JobSponsor,
    grants: GrantSet,
    ledger: JobLedger,
    _lease: ReceiverLease,
    allow_insecure_loopback: bool,
}

impl JobReceiver {
    pub fn has_configured_grants(
        cwd: &Path,
        sources: &ReceiverSources,
    ) -> Result<bool, ReceiverError> {
        Ok(!GrantSet::load_with_nemo(
            cwd,
            sources.grants_json.clone(),
            sources.grants_file.clone(),
            sources.nemo_workspace,
        )?
        .is_empty())
    }

    pub fn from_sources(
        tenant: AuthenticatedContext,
        keys: Keys,
        rest: RestClient,
        sponsor: JobSponsor,
        cwd: &Path,
        sources: ReceiverSources,
    ) -> Result<Self, ReceiverError> {
        let agent_pubkey = keys.public_key().to_hex();
        validate_tenant_identity(&tenant, &agent_pubkey)?;
        let sponsor_key = nostr::PublicKey::parse(&sponsor.pubkey)
            .map_err(|_| ReceiverError::Tenant("agent sponsor is not a public key".into()))?;
        if sponsor_key.to_hex() != sponsor.pubkey
            || sponsor.github_login.trim().is_empty()
            || sponsor.github_login.len() > 128
        {
            return Err(ReceiverError::Tenant(
                "agent sponsor must contain a canonical owner key and login metadata".into(),
            ));
        }
        let grants = GrantSet::load_with_nemo(
            cwd,
            sources.grants_json,
            sources.grants_file,
            sources.nemo_workspace,
        )?;
        let ledger_candidate = match sources.ledger_root {
            Some(root) => root,
            None => default_ledger_root(&tenant.community_id, &agent_pubkey)?,
        };
        let ledger_root =
            grants::prepare_private_ledger_root(cwd, &ledger_candidate, grants.checkout_roots())?;
        let lease = ReceiverLease::acquire(&ledger_root)?;
        Ok(Self {
            tenant,
            tenant_generation: 0,
            tenant_invalid: false,
            agent_pubkey,
            keys,
            rest,
            sponsor,
            grants,
            ledger: JobLedger::new(ledger_root),
            _lease: lease,
            allow_insecure_loopback: sources.allow_insecure_loopback,
        })
    }

    #[cfg(test)]
    fn for_test(
        tenant: AuthenticatedContext,
        keys: Keys,
        rest: RestClient,
        sponsor: JobSponsor,
        grants: GrantSet,
        ledger_root: PathBuf,
    ) -> Self {
        let lease = ReceiverLease::acquire(&ledger_root).expect("exclusive test receiver lease");
        Self {
            agent_pubkey: keys.public_key().to_hex(),
            tenant,
            tenant_generation: 0,
            tenant_invalid: false,
            keys,
            rest,
            sponsor,
            grants,
            ledger: JobLedger::new(ledger_root),
            _lease: lease,
            allow_insecure_loopback: true,
        }
    }

    pub fn enabled(&self) -> bool {
        !self.tenant_invalid && !self.grants.is_empty()
    }

    pub fn subscription_channels(&self) -> Vec<Uuid> {
        self.grants.home_channels()
    }

    pub fn subscribes_to_channel(&self, channel_id: Uuid) -> bool {
        self.grants.contains_home_channel(channel_id)
    }

    pub async fn pending_events(&self) -> Result<Vec<Event>, ReceiverError> {
        Ok(self
            .ledger
            .pending_claims()
            .await?
            .into_iter()
            .filter(|claim| claim.community == self.tenant.community_id)
            .map(|claim| claim.request_event)
            .collect())
    }

    /// Retry every frozen durable receipt/outbox event using its exact signed ID.
    pub async fn retry_outboxes(&self) -> Result<(), ReceiverError> {
        let mut first_error = None;
        for decline in self.ledger.declines().await? {
            if decline.community != self.tenant.community_id {
                continue;
            }
            if let Err(error) = receipts::publish_decline(self, &decline, false).await {
                tracing::warn!(
                    request_event_id = %decline.request_event_id,
                    "durable agent-job decline retry remains pending: {error}"
                );
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        for claim in self.ledger.claims().await? {
            if claim.community != self.tenant.community_id {
                continue;
            }
            let result = async {
                let processed_acked = self
                    .ledger
                    .receipt_acked(&claim, ledger::ReceiptKind::Processed)
                    .await?;
                let accepted_acked = self
                    .ledger
                    .receipt_acked(&claim, ledger::ReceiptKind::Accepted)
                    .await?;
                if !processed_acked || !accepted_acked {
                    let _ = verified_durable_response_common(
                        &claim,
                        &self.agent_pubkey,
                        &self.sponsor.pubkey,
                    )?;
                }
                let _ = receipts::publish(self, &claim, false).await?;
                let lifecycle = self.ledger.lifecycle_store(&claim);
                if lifecycle.exists() {
                    if !self.ledger.prompt_started(&claim).await? {
                        git_receipt_journal::initialize_for_unstarted_lifecycle(
                            &lifecycle,
                            &self.tenant.community_id,
                            &self.agent_pubkey,
                            &claim,
                        )
                        .map_err(|error| ReceiverError::Privilege(error.to_string()))?;
                    }
                    let (head, pending, _) = lifecycle.snapshot().await?;
                    if let Some(event) = pending {
                        validate_pending_terminal_git_effect(
                            &lifecycle,
                            &event,
                            &head,
                            &claim,
                            &self.agent_pubkey,
                            &self.sponsor,
                        )?;
                        self.rest.submit_event_confirmed(&event).await?;
                        lifecycle.confirm(event.id.to_hex()).await?;
                    }
                }
                Ok::<(), ReceiverError>(())
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(
                    request_event_id = %claim.request_event_id,
                    "durable agent-job outbox retry remains pending: {error}"
                );
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Replay frozen lifecycle outbox events and close interrupted prior processes.
    pub async fn recover_lifecycle(&self) -> Result<(), ReceiverError> {
        for claim in self.ledger.claims().await? {
            if claim.community != self.tenant.community_id {
                continue;
            }
            let lifecycle = self.ledger.lifecycle_store(&claim);
            if !lifecycle.exists() {
                if !self
                    .ledger
                    .receipt_acked(&claim, ledger::ReceiptKind::Accepted)
                    .await?
                {
                    // Processed may already have a relay-stored Cancel child.
                    // Do not invent an Accepted lifecycle anchor before that
                    // inbound control event is replayed from the relay.
                    continue;
                }
                lifecycle.initialize(claim.accepted.id.to_hex()).await?;
            }
            let prompt_started = self.ledger.prompt_started(&claim).await?;
            if !prompt_started {
                git_receipt_journal::initialize_for_unstarted_lifecycle(
                    &lifecycle,
                    &self.tenant.community_id,
                    &self.agent_pubkey,
                    &claim,
                )
                .map_err(|error| ReceiverError::Privilege(error.to_string()))?;
            }
            let (head, pending, _) = lifecycle.snapshot().await?;
            if let Some(event) = pending {
                validate_pending_terminal_git_effect(
                    &lifecycle,
                    &event,
                    &head,
                    &claim,
                    &self.agent_pubkey,
                    &self.sponsor,
                )?;
                self.rest.submit_event_confirmed(&event).await?;
                lifecycle.confirm(event.id.to_hex()).await?;
            }
            let (_, _, terminal) = lifecycle.snapshot().await?;
            let pending_cancel = lifecycle.pending_cancel().await?.is_some();
            if !terminal && (pending_cancel || prompt_started) {
                let durable_common = verified_durable_response_common(
                    &claim,
                    &self.agent_pubkey,
                    &self.sponsor.pubkey,
                )?;
                claim.request_event.verify().map_err(|error| {
                    ReceiverError::Receipt(format!("stored request signature: {error}"))
                })?;
                let JobEvent::Request(request) = JobEvent::parse(&claim.request_event)
                    .map_err(|error| ReceiverError::Receipt(error.to_string()))?
                else {
                    return Err(ReceiverError::Receipt(
                        "stored claim is not a job request".into(),
                    ));
                };
                let emitter = JobEmitter::new(
                    &request,
                    claim.request_event_id.clone(),
                    self.keys.clone(),
                    self.rest.clone(),
                    lifecycle.clone(),
                    self.grants.capabilities_for(&request).unwrap_or_default(),
                    claim.digest.clone(),
                    durable_common.sponsor.clone(),
                );
                if pending_cancel {
                    (if prompt_started {
                        cancel::CancellationTerminal::interrupted_full_host_turn()
                    } else {
                        cancel::terminal_for_lifecycle(
                            &lifecycle,
                            &self.tenant.community_id,
                            &self.agent_pubkey,
                            &claim,
                        )
                    })
                    .publish(&emitter)
                    .await
                    .map_err(|error| ReceiverError::Receipt(error.to_string()))?;
                } else {
                    emitter
                        .indeterminate(
                            "worker_interrupted".into(),
                            "Worker process restarted before recording a terminal outcome".into(),
                        )
                        .await
                        .map_err(|error| ReceiverError::Receipt(error.to_string()))?;
                }
            }
        }
        Ok(())
    }

    pub async fn mark_prompt_started(&self, claim: &StoredClaim) -> Result<bool, ReceiverError> {
        Ok(self.ledger.mark_prompt_started(claim).await?)
    }

    /// Refresh the authenticated tenant after a relay connection generation changes.
    pub async fn observe_connection_generation(
        &mut self,
        generation: u64,
        rest: &RestClient,
    ) -> Result<(), ReceiverError> {
        if self.tenant_invalid {
            return Err(ReceiverError::Tenant("tenant binding is invalid".into()));
        }
        if generation <= self.tenant_generation {
            return Ok(());
        }
        let refreshed = rest.authenticated_context().await?;
        if validate_tenant_identity(&refreshed, &self.agent_pubkey).is_err()
            || refreshed.community_id != self.tenant.community_id
            || refreshed.host != self.tenant.host
        {
            self.tenant_invalid = true;
            return Err(ReceiverError::Tenant(
                "authenticated tenant changed or no longer matches the local signer".into(),
            ));
        }
        self.tenant_generation = generation;
        Ok(())
    }

    pub async fn handle_request(
        &self,
        channel_id: Uuid,
        event: Event,
        project: Option<&PromptProjectInfo>,
    ) -> Result<HandleOutcome, ReceiverError> {
        let request = match verified_request(&event, channel_id, &self.agent_pubkey) {
            Some(request) => request,
            None => return Ok(HandleOutcome::Consumed),
        };
        if !project_authorizes(project, &request)
            || self.grants.capabilities_for(&request).is_none()
        {
            tracing::warn!(
                channel_id = %channel_id,
                request_event_id = %event.id,
                "dropping agent job outside the authoritative local grant"
            );
            return Ok(HandleOutcome::Consumed);
        }
        if prompt::render(&request, &event.id.to_hex()).is_none() {
            tracing::warn!(
                request_event_id = %event.id,
                "dropping agent job whose bounded prompt representation is too large"
            );
            return Ok(HandleOutcome::Consumed);
        }

        let digest = semantic_request_digest(&request)
            .map_err(|error| ReceiverError::Receipt(error.to_string()))?;
        let receipts = build_claim_receipts(
            &request,
            &event.id.to_hex(),
            &digest,
            &self.keys,
            &self.sponsor,
        )
        .map_err(|error| ReceiverError::Receipt(error.to_string()))?;
        let candidate = StoredClaim::new(
            self.tenant.community_id.clone(),
            request.common.sender_pubkey.clone(),
            request.common.idempotency_key.clone(),
            digest,
            event.id.to_hex(),
            event.clone(),
            receipts.processed,
            receipts.accepted,
        );
        let _authorization_expires_at = authority::authorize(
            &self.rest,
            &self.tenant,
            &request,
            &event.id.to_hex(),
            &candidate.digest,
            &self.sponsor,
            self.allow_insecure_loopback,
        )
        .await?;
        match self
            .ledger
            .lookup_decline(
                &self.tenant.community_id,
                &request.common.sender_pubkey,
                &request.common.idempotency_key,
                &candidate.digest,
            )
            .await?
        {
            DeclineLookup::Replay(decline) => {
                verified_durable_decline(&decline, &self.agent_pubkey, &self.sponsor.pubkey)?;
                receipts::publish_decline(self, &decline, true).await?;
                return Ok(HandleOutcome::Consumed);
            }
            DeclineLookup::Conflict { existing_digest } => {
                tracing::warn!(
                    request_event_id = %event.id,
                    existing_digest = %existing_digest,
                    "rejecting changed agent job body for an existing idempotency key"
                );
                return Ok(HandleOutcome::Consumed);
            }
            DeclineLookup::Claimed | DeclineLookup::Absent => {}
        }
        let grant_match = match self.grants.authorize_request(&request) {
            Ok(Some(grant_match)) => grant_match,
            Ok(None) => {
                tracing::warn!(
                    channel_id = %channel_id,
                    request_event_id = %event.id,
                    "dropping agent job outside the verified local repository scope"
                );
                return Ok(HandleOutcome::Consumed);
            }
            Err(error) => {
                tracing::warn!(
                    channel_id = %channel_id,
                    request_event_id = %event.id,
                    "declining agent job because its Nemo worktree could not be prepared: {error}"
                );
                let declined = build_declined_receipt(
                    &request,
                    &event.id.to_hex(),
                    &candidate.digest,
                    &self.keys,
                    &self.sponsor,
                    SETUP_FAILURE_REASON,
                )
                .map_err(|error| ReceiverError::Receipt(error.to_string()))?;
                let decline = StoredDecline::new(
                    self.tenant.community_id.clone(),
                    request.common.sender_pubkey.clone(),
                    request.common.idempotency_key.clone(),
                    candidate.digest.clone(),
                    event.id.to_hex(),
                    event.clone(),
                    declined,
                );
                match self.ledger.decline(decline).await? {
                    DeclineDecision::New(stored) => {
                        receipts::publish_decline(self, &stored, false).await?;
                    }
                    DeclineDecision::Replay(stored) => {
                        receipts::publish_decline(self, &stored, true).await?;
                    }
                    DeclineDecision::Claimed => {}
                    DeclineDecision::Conflict { existing_digest } => {
                        tracing::warn!(
                            request_event_id = %event.id,
                            existing_digest = %existing_digest,
                            "rejecting changed agent job body for an existing idempotency key"
                        );
                    }
                }
                return Ok(HandleOutcome::Consumed);
            }
        };
        if !paths::request_paths_are_contained(&grant_match.checkout_root, &request) {
            tracing::warn!(
                channel_id = %channel_id,
                request_event_id = %event.id,
                "dropping agent job whose requested paths escape the verified checkout"
            );
            return Ok(HandleOutcome::Consumed);
        }
        let (stored, force_receipt_replay) = match self.ledger.claim(candidate).await? {
            ClaimDecision::New(stored) => (stored, false),
            ClaimDecision::Replay(stored) => (stored, true),
            ClaimDecision::Declined(stored) => {
                receipts::publish_decline(self, &stored, true).await?;
                return Ok(HandleOutcome::Consumed);
            }
            ClaimDecision::Conflict { existing_digest } => {
                tracing::warn!(
                    request_event_id = %event.id,
                    existing_digest = %existing_digest,
                    "rejecting changed agent job body for an existing idempotency key"
                );
                return Ok(HandleOutcome::Consumed);
            }
        };
        let durable_common =
            verified_durable_response_common(&stored, &self.agent_pubkey, &self.sponsor.pubkey)?;

        if receipts::publish(self, &stored, force_receipt_replay).await?
            == receipts::PublishOutcome::CancelledBeforeAccept
        {
            return Ok(HandleOutcome::Consumed);
        }
        if self.ledger.prompt_started(&stored).await? {
            return Ok(HandleOutcome::Consumed);
        }

        let scope = SessionScope::Job {
            channel_id,
            operation_id: request.common.operation_id.clone(),
            request_event_id: event.id.to_hex(),
        };
        let lifecycle = self.ledger.lifecycle_store(&stored);
        lifecycle
            .initialize(stored.accepted.id.to_hex())
            .await
            .map_err(|error| ReceiverError::Receipt(error.to_string()))?;
        let (_, pending_lifecycle, terminal) = lifecycle.snapshot().await?;
        if terminal || pending_lifecycle.is_some() {
            return Ok(HandleOutcome::Consumed);
        }
        let emitter = JobEmitter::new(
            &request,
            event.id.to_hex(),
            self.keys.clone(),
            self.rest.clone(),
            lifecycle,
            grant_match.capabilities,
            stored.digest.clone(),
            durable_common.sponsor.clone(),
        );
        let privilege = JobPrivilege::new(
            scope.clone(),
            self.tenant.clone(),
            self.agent_pubkey.clone(),
            self.rest.clone(),
            durable_common.sponsor,
            self.grants.clone(),
            self.ledger.clone(),
            stored.clone(),
            request.clone(),
            emitter.clone(),
            self.ledger.lifecycle_store(&stored),
            grant_match.checkout_root.clone(),
            self.allow_insecure_loopback,
        )
        .map_err(ReceiverError::Privilege)?;
        Ok(HandleOutcome::Dispatch(Box::new(JobDispatch {
            scope,
            event,
            emitter,
            claim: stored,
            checkout_root: grant_match.checkout_root,
            privilege,
        })))
    }

    /// Observe an addressed requester cancellation and durably fence the
    /// worker lifecycle before the caller signals or removes any prompt.
    pub(crate) async fn handle_cancel(
        &self,
        privileges: &JobPrivilegeRegistry,
        channel_id: Uuid,
        event: Event,
    ) -> Result<CancelOutcome, ReceiverError> {
        cancel::handle(self, privileges, channel_id, event).await
    }

    pub async fn terminate_channel(&self, channel_id: Uuid) -> Result<usize, ReceiverError> {
        revocation::terminate_channel(self, channel_id).await
    }
}

fn verified_request(event: &Event, channel_id: Uuid, agent_pubkey: &str) -> Option<JobRequest> {
    event.verify().ok()?;
    let JobEvent::Request(request) = JobEvent::parse(event).ok()? else {
        return None;
    };
    if request.common.recipient_pubkey != agent_pubkey
        || request.common.project.home_channel != channel_id.to_string()
        || !expiry_is_live_and_bounded(&request.common.expires_at)
    {
        return None;
    }
    Some(request)
}

fn expiry_is_live_and_bounded(value: &str) -> bool {
    let Ok(expiry) = DateTime::parse_from_rfc3339(value) else {
        return false;
    };
    let expiry = expiry.with_timezone(&Utc);
    let now = Utc::now();
    expiry > now && expiry <= now + chrono::Duration::seconds(MAX_JOB_TTL_SECONDS)
}

fn project_authorizes(project: Option<&PromptProjectInfo>, request: &JobRequest) -> bool {
    let Some(project) = project else {
        return false;
    };
    if project.coordinate != request.common.project.address {
        return false;
    }
    let Some(repo_id) = project.default_repo_id.as_deref() else {
        return false;
    };
    request
        .common
        .repository
        .canonical
        .rsplit('/')
        .next()
        .is_some_and(|name| name == repo_id)
}

fn validate_tenant_identity(
    tenant: &AuthenticatedContext,
    agent_pubkey: &str,
) -> Result<(), ReceiverError> {
    if tenant.pubkey != agent_pubkey {
        return Err(ReceiverError::Tenant(
            "/api/context signer does not match the local agent key".into(),
        ));
    }
    Ok(())
}

fn default_ledger_root(community_id: &str, agent_pubkey: &str) -> Result<PathBuf, ReceiverError> {
    Ok(default_ledger_base()?.join(community_id).join(agent_pubkey))
}

fn default_ledger_base() -> Result<PathBuf, GrantError> {
    #[cfg(target_os = "macos")]
    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        if home.is_absolute() {
            return Ok(home.join("Library/Application Support/Buzz/agent-jobs"));
        }
    }
    #[cfg(target_os = "windows")]
    if let Some(local) = std::env::var_os("LOCALAPPDATA") {
        let local = PathBuf::from(local);
        if local.is_absolute() {
            return Ok(local.join("Buzz/agent-jobs"));
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    if let Some(state) = std::env::var_os("XDG_STATE_HOME") {
        let state = PathBuf::from(state);
        if state.is_absolute() {
            return Ok(state.join("buzz/agent-jobs"));
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        if home.is_absolute() {
            return Ok(home.join(".local/state/buzz/agent-jobs"));
        }
    }
    Err(GrantError::Invalid(
        "no absolute operator state directory is available for the job ledger".into(),
    ))
}

#[cfg(test)]
mod tests;
