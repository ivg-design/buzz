use chrono::{DateTime, Utc};
use nostr::{Event, PublicKey};
use thiserror::Error;

use buzz_core::job::{
    semantic_request_digest, JobClaimStatus, JobControlAction, JobEvent,
    JOB_TERMINAL_AUDIT_GRACE_SECONDS, MAX_JOB_TTL_SECONDS,
};
use buzz_core::tenant::TenantContext;
use buzz_db::EventQuery;

use crate::state::AppState;

use super::authority::{
    require_channel_member_locked, require_channel_nonmember_locked,
    require_current_relay_membership, require_current_relay_membership_locked,
    require_registered_agent, require_registered_agent_locked, validate_superseding_request,
};
use super::history::validate_operation_history;
use super::lifecycle::{
    is_membership_revoked_terminal, requires_predecessor, validate_membership_revoked_predecessor,
    validate_predecessor, validate_transition,
};
use super::project::{
    load_job_event_locked, resolve_repository_link_locked, validate_project_binding,
    validate_project_binding_locked, validate_sponsor, validate_sponsor_locked,
};

/// Job validation failure classified for the ingest transport.
#[derive(Debug, Error)]
pub enum JobAuthError {
    /// Signed envelope or lifecycle shape is invalid.
    #[error("{0}")]
    Invalid(String),
    /// Authenticated actor lacks the required relationship.
    #[error("{0}")]
    Restricted(String),
    /// Authoritative state could not be read.
    #[error("{0}")]
    Internal(String),
}

/// Validated job plus the cross-pod operation lock held through event insert.
pub struct ValidatedJob {
    _job: JobEvent,
    lock: buzz_db::JobOperationLock,
    existing: Option<buzz_core::StoredEvent>,
}

impl ValidatedJob {
    /// Insert the validated event on the same transaction that holds the lock.
    pub async fn insert_event(
        &mut self,
        tenant: &TenantContext,
        event: &Event,
        channel_id: uuid::Uuid,
    ) -> Result<(buzz_core::StoredEvent, bool), JobAuthError> {
        if let Some(existing) = self.existing.take() {
            return Ok((existing, false));
        }
        self.lock
            .insert_event(tenant.community(), event, channel_id)
            .await
            .map_err(|error| JobAuthError::Internal(format!("inserting fenced job event: {error}")))
    }

    /// Release the operation fence after the generic event insert finishes.
    pub async fn commit(self) -> Result<(), JobAuthError> {
        self.lock.commit().await.map_err(|error| {
            JobAuthError::Internal(format!("releasing job operation lock: {error}"))
        })
    }
}

/// Validate one job event against community-scoped durable authority state.
pub async fn validate_job_event(
    tenant: &TenantContext,
    state: &AppState,
    event: &Event,
) -> Result<ValidatedJob, JobAuthError> {
    let job = JobEvent::parse(event).map_err(|error| JobAuthError::Invalid(error.to_string()))?;
    let common = job.common();
    let managed_nemo = is_managed_nemo_tenant(tenant, state, &job);
    // This shape is only a candidate until the locked request/predecessor
    // checks below prove it is the accepted worker closing its active chain.
    let membership_revoked_terminal = is_membership_revoked_terminal(&job);
    if managed_nemo && membership_revoked_terminal {
        return Err(JobAuthError::Restricted(
            "Nemo workspace membership follows current community enrollment".into(),
        ));
    }
    let lock_domains = match &job {
        JobEvent::Request(_) => vec![
            format!(
                "request:{}:{}",
                event.pubkey.to_hex(),
                common.idempotency_key
            ),
            format!("operation:{}", common.operation_id),
        ],
        _ => vec![format!("operation:{}", common.operation_id)],
    };
    let actor = PublicKey::parse(&common.sender_pubkey)
        .map_err(|_| JobAuthError::Invalid("sender_pubkey must be a public key".into()))?;
    require_current_relay_membership(tenant, state, &actor, "job signer").await?;

    // A byte-identical durable retry remains successful even after its job
    // expires or terminates. Preflight avoids taking attacker-chosen advisory
    // locks unless that exact signed event already exists; the locked re-read
    // is authoritative and prevents re-running fanout or side effects.
    let exact_id = event.id.to_bytes().to_vec();
    if state
        .db
        .get_event_by_id_for_event_write(tenant.community(), &exact_id)
        .await
        .map_err(|error| JobAuthError::Internal(format!("checking job event replay: {error}")))?
        .is_some()
    {
        let mut lock = state
            .db
            .acquire_job_operation_locks(tenant.community(), &lock_domains)
            .await
            .map_err(|error| {
                JobAuthError::Internal(format!("acquiring job operation lock: {error}"))
            })?;
        if let Some(stored) = find_exact_event(&mut lock, tenant, &exact_id).await? {
            return Ok(ValidatedJob {
                _job: job,
                lock,
                existing: Some(stored),
            });
        }
    }

    let expiry = DateTime::parse_from_rfc3339(&common.expires_at)
        .map_err(|_| JobAuthError::Invalid("expires_at must be RFC3339".into()))?
        .with_timezone(&Utc);
    let now = Utc::now();
    validate_job_time(&job, event.created_at.as_secs() as i64, expiry, now)?;

    let channel_id = common
        .project
        .home_channel
        .parse()
        .map_err(|_| JobAuthError::Invalid("project.home_channel must be a UUID".into()))?;
    if !membership_revoked_terminal && !managed_nemo {
        match state
            .is_member_cached(tenant.community(), channel_id, &actor.to_bytes())
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return Err(JobAuthError::Restricted(
                    "job signer must be a direct member of the project home channel".into(),
                ))
            }
            Err(error) => {
                return Err(JobAuthError::Internal(format!(
                    "checking job signer channel membership: {error}"
                )))
            }
        }
    }
    let recipient = PublicKey::parse(&common.recipient_pubkey)
        .map_err(|_| JobAuthError::Invalid("recipient_pubkey must be a public key".into()))?;
    if managed_nemo {
        require_current_relay_membership(tenant, state, &recipient, "job recipient").await?;
    } else {
        match state
            .is_member_cached(tenant.community(), channel_id, &recipient.to_bytes())
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return Err(JobAuthError::Restricted(
                    "job recipient must be a direct member of the project home channel".into(),
                ))
            }
            Err(error) => {
                return Err(JobAuthError::Internal(format!(
                    "checking job recipient channel membership: {error}"
                )))
            }
        }
    }

    validate_project_binding(tenant, state, &job).await?;
    validate_sponsor(tenant, state, &job).await?;
    if matches!(job, JobEvent::Request(_)) {
        require_registered_agent(tenant, state, &recipient, "job recipient").await?;
    }

    let mut lock = state
        .db
        .acquire_job_operation_locks(tenant.community(), &lock_domains)
        .await
        .map_err(|error| {
            JobAuthError::Internal(format!("acquiring job operation lock: {error}"))
        })?;
    if let Some(stored) = find_exact_event(&mut lock, tenant, &exact_id).await? {
        return Ok(ValidatedJob {
            _job: job,
            lock,
            existing: Some(stored),
        });
    }

    // Re-read and lock every mutable authority row inside the same transaction
    // that owns the operation fence and event insert. Membership removal,
    // owner rebinding, and Project replacement therefore cannot commit between
    // validation and storage.
    require_current_relay_membership_locked(tenant, &mut lock, &actor, "job signer").await?;
    if !membership_revoked_terminal && !managed_nemo {
        require_channel_member_locked(tenant, &mut lock, channel_id, &actor, "job signer").await?;
    }
    if managed_nemo {
        require_current_relay_membership_locked(tenant, &mut lock, &recipient, "job recipient")
            .await?;
    } else {
        require_channel_member_locked(tenant, &mut lock, channel_id, &recipient, "job recipient")
            .await?;
    }
    let project = validate_project_binding_locked(tenant, &mut lock, &job).await?;
    // Every transition, including the receiver's privileged-operation marker,
    // re-resolves and locks the current Project -> repository announcement.
    // This makes the marker itself the authorization linearization point;
    // a repository rebind cannot commit between preflight and marker storage.
    let _repository =
        resolve_repository_link_locked(tenant, &mut lock, &project, &common.repository.canonical)
            .await?;
    validate_sponsor_locked(tenant, &mut lock, &job).await?;
    if matches!(job, JobEvent::Request(_)) {
        require_registered_agent_locked(tenant, &mut lock, &recipient, "job recipient").await?;
    }

    if let JobEvent::Request(request) = &job {
        if let Some(supersedes) = &request.supersedes_event_id {
            let handoff =
                load_job_event_locked(tenant, &mut lock, supersedes, "supersedes_event_id").await?;
            let old_root_id = match &handoff {
                JobEvent::Control(control) => control.followup.request_event_id.as_str(),
                _ => "",
            };
            if old_root_id.is_empty() {
                return Err(JobAuthError::Invalid(
                    "supersedes_event_id must reference a handoff".into(),
                ));
            }
            let old_root =
                load_job_event_locked(tenant, &mut lock, old_root_id, "handoff request root")
                    .await?;
            validate_superseding_request(request, &handoff, &old_root)?;
        }
    }

    if let Some(request_id) = job.request_event_id() {
        let request =
            load_job_event_locked(tenant, &mut lock, request_id, "request_event_id").await?;
        if !matches!(request, JobEvent::Request(_)) {
            return Err(JobAuthError::Invalid(
                "request_event_id must reference kind 43001".into(),
            ));
        }
        let requester = PublicKey::parse(&request.common().sender_pubkey)
            .map_err(|_| JobAuthError::Invalid("request sender_pubkey is invalid".into()))?;
        let worker = PublicKey::parse(&request.common().recipient_pubkey)
            .map_err(|_| JobAuthError::Invalid("request recipient_pubkey is invalid".into()))?;
        require_current_relay_membership_locked(
            tenant,
            &mut lock,
            &requester,
            "stored job requester",
        )
        .await?;
        require_registered_agent_locked(tenant, &mut lock, &worker, "stored job recipient").await?;
        if !managed_nemo {
            require_channel_member_locked(
                tenant,
                &mut lock,
                channel_id,
                &requester,
                "stored job requester",
            )
            .await?;
        }
        if membership_revoked_terminal {
            require_channel_nonmember_locked(
                tenant,
                &mut lock,
                channel_id,
                &worker,
                "stored job recipient",
            )
            .await?;
        } else if !managed_nemo {
            require_channel_member_locked(
                tenant,
                &mut lock,
                channel_id,
                &worker,
                "stored job recipient",
            )
            .await?;
        }
        validate_sponsor_locked(tenant, &mut lock, &request).await?;
        validate_transition(&job, &request)?;
        validate_claim_digest(&job, &request)?;

        if let Some(prior_id) = job.prior_event_id() {
            let prior =
                load_job_event_locked(tenant, &mut lock, prior_id, "prior_event_id").await?;
            validate_predecessor(&job, &prior, request_id)?;
            if membership_revoked_terminal {
                validate_membership_revoked_predecessor(&job, &prior, &request)?;
            }
        } else if requires_predecessor(&job) {
            return Err(JobAuthError::Invalid(
                "this job transition requires prior_event_id".into(),
            ));
        }
    }

    if let JobEvent::Control(control) = &job {
        if let Some(handoff_to) = &control.handoff_to {
            let target = PublicKey::parse(handoff_to)
                .map_err(|_| JobAuthError::Invalid("handoff_to must be a public key".into()))?;
            if !managed_nemo {
                require_channel_member_locked(
                    tenant,
                    &mut lock,
                    channel_id,
                    &target,
                    "handoff target",
                )
                .await?;
            }
            require_registered_agent_locked(tenant, &mut lock, &target, "handoff target").await?;
        }
    }

    validate_operation_history(tenant, &mut lock, event, &job, channel_id).await?;

    Ok(ValidatedJob {
        _job: job,
        lock,
        existing: None,
    })
}

pub(super) fn is_managed_nemo_tenant(
    tenant: &TenantContext,
    state: &AppState,
    job: &JobEvent,
) -> bool {
    let common = job.common();
    tenant.host() == buzz_core::nemo::RELAY_HOST
        && buzz_core::relay::normalize_relay_url(&state.config.relay_url)
            .ok()
            .as_deref()
            == Some(buzz_core::nemo::RELAY_URL)
        && buzz_core::nemo::matches(
            &common.project.address,
            &common.project.home_channel,
            &common.repository.canonical,
        )
}

pub(super) fn validate_job_time(
    job: &JobEvent,
    event_created_at: i64,
    expiry: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<(), JobAuthError> {
    const MAX_EVENT_TIMESTAMP_DRIFT_SECONDS: i64 = 900;
    if (event_created_at - now.timestamp()).abs() > MAX_EVENT_TIMESTAMP_DRIFT_SECONDS {
        return Err(JobAuthError::Invalid(
            "event timestamp too far from server time".into(),
        ));
    }
    if event_created_at > expiry.timestamp() || now > expiry {
        let grace_ends = expiry.timestamp() + JOB_TERMINAL_AUDIT_GRACE_SECONDS;
        if !is_terminal_audit(job) || event_created_at > grace_ends || now.timestamp() > grace_ends
        {
            return Err(JobAuthError::Invalid(
                "job event is past expires_at and is not an allowed terminal audit within grace"
                    .into(),
            ));
        }
    }
    if expiry.timestamp() - now.timestamp() > MAX_JOB_TTL_SECONDS
        || expiry.timestamp() - event_created_at > MAX_JOB_TTL_SECONDS
    {
        return Err(JobAuthError::Invalid(format!(
            "expires_at exceeds the {MAX_JOB_TTL_SECONDS}-second maximum job lifetime"
        )));
    }
    Ok(())
}

fn is_terminal_audit(job: &JobEvent) -> bool {
    matches!(job, JobEvent::Accepted(body) if body.claim.status == JobClaimStatus::Declined)
        || matches!(job, JobEvent::Error(_))
        || matches!(
            job,
            JobEvent::Control(body) if body.action == JobControlAction::Cancelled
        )
}

pub(super) fn validate_claim_digest(
    current: &JobEvent,
    request: &JobEvent,
) -> Result<(), JobAuthError> {
    let JobEvent::Accepted(receipt) = current else {
        return Ok(());
    };
    let JobEvent::Request(request) = request else {
        return Err(JobAuthError::Invalid(
            "claim request root is not kind 43001".into(),
        ));
    };
    let expected = semantic_request_digest(request)
        .map_err(|error| JobAuthError::Invalid(error.to_string()))?;
    if receipt.claim.scope_digest != expected {
        return Err(JobAuthError::Invalid(
            "claim.scope_digest does not match the receiver-computed request digest".into(),
        ));
    }
    Ok(())
}

async fn find_exact_event(
    lock: &mut buzz_db::JobOperationLock,
    tenant: &TenantContext,
    id: &[u8],
) -> Result<Option<buzz_core::StoredEvent>, JobAuthError> {
    let mut query = EventQuery::for_community(tenant.community());
    query.ids = Some(vec![id.to_vec()]);
    query.limit = Some(2);
    query.max_limit = Some(2);
    let mut stored = lock
        .query_events(&query)
        .await
        .map_err(|error| JobAuthError::Internal(format!("checking job event replay: {error}")))?;
    if stored.len() > 1 {
        return Err(JobAuthError::Internal(
            "event ID resolved to multiple tenant rows".into(),
        ));
    }
    Ok(stored.pop())
}
