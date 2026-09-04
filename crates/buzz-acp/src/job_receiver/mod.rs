mod authority;
mod cancel;
mod emitter;
mod grants;
mod lease;
mod ledger;
mod lifecycle;
mod outcome;
mod paths;
mod prompt;
mod receipts;
mod revocation;

use std::path::{Path, PathBuf};

use buzz_core::job::{
    semantic_request_digest, JobEvent, JobRequest, JobSponsor, MAX_JOB_TTL_SECONDS,
};
use chrono::{DateTime, Utc};
use nostr::{Event, Keys};
use thiserror::Error;
use uuid::Uuid;

use emitter::build_claim_receipts;
pub use emitter::JobEmitter;
use grants::{GrantError, GrantSet};
use lease::ReceiverLease;
use ledger::{ClaimDecision, JobLedger, LedgerError, StoredClaim};
use lifecycle::LifecycleError;
pub use outcome::{parse_terminal_outcome, TerminalDisposition};
pub use prompt::format_job_prompt;

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
}

pub struct JobDispatch {
    pub scope: SessionScope,
    pub event: Event,
    pub emitter: JobEmitter,
    pub claim: StoredClaim,
}

pub enum HandleOutcome {
    Consumed,
    Dispatch(Box<JobDispatch>),
}

pub use cancel::CancelOutcome;

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
    repository_root: PathBuf,
}

impl JobReceiver {
    pub fn has_configured_grants(cwd: &Path) -> Result<bool, ReceiverError> {
        Ok(!GrantSet::load(cwd)?.is_empty())
    }

    pub fn from_env(
        tenant: AuthenticatedContext,
        keys: Keys,
        rest: RestClient,
        sponsor: JobSponsor,
        cwd: &Path,
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
        let grants = GrantSet::load(cwd)?;
        let ledger_root = std::env::var_os("BUZZ_ACP_JOB_LEDGER_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| default_ledger_root(cwd, &tenant.community_id, &agent_pubkey));
        let lease = ReceiverLease::acquire(&ledger_root)?;
        let repository_root = cwd.canonicalize().map_err(LedgerError::Io)?;
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
            repository_root,
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
        let repository_root = std::env::current_dir().expect("test repository root");
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
            repository_root,
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
        for claim in self.ledger.claims().await? {
            if claim.community != self.tenant.community_id {
                continue;
            }
            let result = async {
                receipts::publish(self, &claim, false).await?;
                let lifecycle = self.ledger.lifecycle_store(&claim);
                if lifecycle.exists() {
                    let (_, pending, _) = lifecycle.snapshot().await?;
                    if let Some(event) = pending {
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
            lifecycle.initialize(claim.accepted.id.to_hex()).await?;
            let (_, pending, _) = lifecycle.snapshot().await?;
            if let Some(event) = pending {
                self.rest.submit_event_confirmed(&event).await?;
                lifecycle.confirm(event.id.to_hex()).await?;
            }
            let (_, _, terminal) = lifecycle.snapshot().await?;
            if !terminal && self.ledger.prompt_started(&claim).await? {
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
                let pending_cancel = lifecycle.pending_cancel().await?.is_some();
                let emitter = JobEmitter::new(
                    &request,
                    claim.request_event_id.clone(),
                    self.keys.clone(),
                    self.rest.clone(),
                    lifecycle,
                    self.grants.capabilities_for(&request).unwrap_or_default(),
                    claim.digest.clone(),
                    self.sponsor.clone(),
                );
                if pending_cancel {
                    emitter
                        .control(
                            buzz_core::job::JobControlAction::Cancelled,
                            "requester_cancelled".into(),
                            None,
                        )
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
        let grant_capabilities = self.grants.capabilities_for(&request);
        if !project_authorizes(project, &request)
            || grant_capabilities.is_none()
            || !paths::request_paths_are_contained(&self.repository_root, &request)
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
        authority::authorize(
            &self.rest,
            &self.tenant,
            &request,
            &event.id.to_hex(),
            &candidate.digest,
            &self.sponsor,
        )
        .await?;
        let (stored, force_receipt_replay) = match self.ledger.claim(candidate).await? {
            ClaimDecision::New(stored) => (stored, false),
            ClaimDecision::Replay(stored) => (stored, true),
            ClaimDecision::Conflict { existing_digest } => {
                tracing::warn!(
                    request_event_id = %event.id,
                    existing_digest = %existing_digest,
                    "rejecting changed agent job body for an existing idempotency key"
                );
                return Ok(HandleOutcome::Consumed);
            }
        };

        receipts::publish(self, &stored, force_receipt_replay).await?;
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
            grant_capabilities.unwrap_or_default(),
            stored.digest.clone(),
            self.sponsor.clone(),
        );
        Ok(HandleOutcome::Dispatch(Box::new(JobDispatch {
            scope,
            event,
            emitter,
            claim: stored,
        })))
    }

    /// Observe an addressed requester cancellation and durably fence the
    /// worker lifecycle before the caller signals or removes any prompt.
    pub async fn handle_cancel(
        &self,
        channel_id: Uuid,
        event: Event,
    ) -> Result<CancelOutcome, ReceiverError> {
        cancel::handle(self, channel_id, event).await
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

fn default_ledger_root(cwd: &Path, community_id: &str, agent_pubkey: &str) -> PathBuf {
    #[cfg(target_os = "macos")]
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home)
            .join("Library/Application Support/Buzz/agent-jobs")
            .join(community_id)
            .join(agent_pubkey);
    }
    #[cfg(target_os = "windows")]
    if let Some(local) = std::env::var_os("LOCALAPPDATA") {
        return PathBuf::from(local)
            .join("Buzz/agent-jobs")
            .join(community_id)
            .join(agent_pubkey);
    }
    if let Some(state) = std::env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(state)
            .join("buzz/agent-jobs")
            .join(community_id)
            .join(agent_pubkey);
    }
    cwd.join(".buzz/agent-jobs")
        .join(community_id)
        .join(agent_pubkey)
}

#[cfg(test)]
mod tests;
