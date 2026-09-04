//! Relay authorization for signed agent job events.
//!
//! Durable storage remains the normal Nostr event table. This module validates
//! the project/channel binding and signed transition chain before ingest writes
//! an event; execution-side exactly-once claiming remains a receiver ledger
//! concern because generic event-ID dedup cannot fence two semantic requests.

use chrono::{DateTime, Utc};
use nostr::PublicKey;

use buzz_core::job::{semantic_request_digest, JobControlAction, JobEvent};
use buzz_core::job_authorization::JobAuthorizationRequest;
use buzz_core::tenant::TenantContext;
use buzz_db::EventQuery;

use crate::state::AppState;

mod authority;
mod gate;
mod history;
mod lifecycle;
#[cfg(test)]
mod postgres_tests;
mod project;
#[cfg(test)]
mod tests;

use authority::{effective_owner_locked, require_channel_member_locked};
pub use gate::{validate_job_event, JobAuthError, ValidatedJob};
use history::is_terminal;
use project::{
    load_job_event_locked, resolve_repository_link_locked, validate_project_binding_locked,
    validate_sponsor_locked,
};

/// Fresh server evidence returned by the HTTP authorization preflight.
pub(crate) struct JobAuthorizationEvidence {
    pub(crate) project_head_event_id: String,
    pub(crate) repository_coordinate: String,
    pub(crate) repository_announcement_event_id: String,
    pub(crate) requester_owner_pubkey: String,
    pub(crate) recipient_owner_pubkey: String,
}

/// Re-authorize a stored request against current mutable relay state.
pub(crate) async fn authorize_stored_request(
    tenant: &TenantContext,
    state: &AppState,
    caller: &PublicKey,
    authorization: &JobAuthorizationRequest,
) -> Result<JobAuthorizationEvidence, JobAuthError> {
    authorization.validate().map_err(JobAuthError::Invalid)?;
    if authorization.community_id != tenant.community().as_uuid().to_string()
        || authorization.relay_host != tenant.host()
    {
        return Err(JobAuthError::Restricted(
            "authorization community or relay host does not match the Host-bound tenant".into(),
        ));
    }
    if caller.to_hex() != authorization.recipient_pubkey {
        return Err(JobAuthError::Restricted(
            "authorization caller must be the addressed job recipient".into(),
        ));
    }
    let request_id = hex::decode(&authorization.request_event_id)
        .map_err(|_| JobAuthError::Invalid("request_event_id must be hex".into()))?;
    let stored = state
        .db
        .get_event_by_id_for_event_write(tenant.community(), &request_id)
        .await
        .map_err(|error| JobAuthError::Internal(format!("loading authorization root: {error}")))?
        .ok_or_else(|| JobAuthError::Restricted("stored job request was not found".into()))?;
    buzz_core::verify_event(&stored.event).map_err(|error| {
        JobAuthError::Invalid(format!("stored job signature is invalid: {error}"))
    })?;
    let parsed = JobEvent::parse(&stored.event).map_err(|error| {
        JobAuthError::Invalid(format!("stored job request is invalid: {error}"))
    })?;
    let JobEvent::Request(preflight) = &parsed else {
        return Err(JobAuthError::Invalid(
            "request_event_id must reference kind 43001".into(),
        ));
    };
    let lock_domains = vec![
        format!(
            "request:{}:{}",
            preflight.common.sender_pubkey, preflight.common.idempotency_key
        ),
        format!("operation:{}", preflight.common.operation_id),
    ];
    let mut lock = state
        .db
        .acquire_job_operation_locks(tenant.community(), &lock_domains)
        .await
        .map_err(|error| JobAuthError::Internal(format!("locking authorization root: {error}")))?;
    let root = load_job_event_locked(
        tenant,
        &mut lock,
        &authorization.request_event_id,
        "request_event_id",
    )
    .await?;
    let JobEvent::Request(request) = &root else {
        return Err(JobAuthError::Invalid(
            "request_event_id must reference kind 43001".into(),
        ));
    };
    let digest = semantic_request_digest(request)
        .map_err(|error| JobAuthError::Invalid(error.to_string()))?;
    if authorization.semantic_digest != digest
        || authorization.channel_id != request.common.project.home_channel
        || authorization.project_address != request.common.project.address
        || authorization.repository != request.common.repository
        || authorization.requester_pubkey != request.common.sender_pubkey
        || authorization.recipient_pubkey != request.common.recipient_pubkey
    {
        return Err(JobAuthError::Restricted(
            "authorization bindings do not match the stored signed request".into(),
        ));
    }
    let expiry = DateTime::parse_from_rfc3339(&request.common.expires_at)
        .map_err(|_| JobAuthError::Invalid("stored request expiry is invalid".into()))?
        .with_timezone(&Utc);
    if Utc::now() > expiry {
        return Err(JobAuthError::Restricted(
            "stored job request has expired".into(),
        ));
    }
    let channel_id = authorization
        .channel_id
        .parse()
        .map_err(|_| JobAuthError::Invalid("channel_id must be a UUID".into()))?;
    let requester = PublicKey::parse(&authorization.requester_pubkey)
        .map_err(|_| JobAuthError::Invalid("requester_pubkey is invalid".into()))?;
    let recipient = PublicKey::parse(&authorization.recipient_pubkey)
        .map_err(|_| JobAuthError::Invalid("recipient_pubkey is invalid".into()))?;
    let requester_owner =
        effective_owner_locked(tenant, &mut lock, &requester, false, "job requester").await?;
    let recipient_owner =
        effective_owner_locked(tenant, &mut lock, &recipient, true, "job recipient").await?;
    if !gate::is_managed_nemo_tenant(tenant, state, &root) {
        require_channel_member_locked(tenant, &mut lock, channel_id, &requester, "job requester")
            .await?;
        require_channel_member_locked(tenant, &mut lock, channel_id, &recipient, "job recipient")
            .await?;
    }
    validate_sponsor_locked(tenant, &mut lock, &root).await?;
    ensure_authorizable_history(
        tenant,
        &mut lock,
        &authorization.request_event_id,
        channel_id,
    )
    .await?;
    let project = validate_project_binding_locked(tenant, &mut lock, &root).await?;
    let (repository_coordinate, announcement) = resolve_repository_link_locked(
        tenant,
        &mut lock,
        &project,
        &authorization.repository.canonical,
    )
    .await?;
    let evidence = JobAuthorizationEvidence {
        project_head_event_id: project.event.id.to_hex(),
        repository_coordinate,
        repository_announcement_event_id: announcement.event.id.to_hex(),
        requester_owner_pubkey: requester_owner,
        recipient_owner_pubkey: recipient_owner,
    };
    lock.commit().await.map_err(|error| {
        JobAuthError::Internal(format!("committing authorization read: {error}"))
    })?;
    Ok(evidence)
}

async fn ensure_authorizable_history(
    tenant: &TenantContext,
    lock: &mut buzz_db::JobOperationLock,
    root_id: &str,
    channel_id: uuid::Uuid,
) -> Result<(), JobAuthError> {
    let mut query = EventQuery::for_community(tenant.community());
    query.kinds = Some((43001..=43006).collect());
    query.channel_id = Some(channel_id);
    query.e_tags = Some(vec![root_id.to_owned()]);
    query.limit = Some(1_001);
    query.max_limit = Some(1_001);
    let stored = lock.query_events(&query).await.map_err(|error| {
        JobAuthError::Internal(format!("loading authorization history: {error}"))
    })?;
    if stored.len() > 1_000 {
        return Err(JobAuthError::Invalid(
            "job operation history exceeds the authorization bound".into(),
        ));
    }
    for item in stored {
        let event = JobEvent::parse(&item.event).map_err(|error| {
            JobAuthError::Internal(format!("stored authorization history is invalid: {error}"))
        })?;
        if is_terminal(&event)
            || matches!(event, JobEvent::Control(ref control) if control.action == JobControlAction::Cancel)
        {
            return Err(JobAuthError::Restricted(
                "job operation is cancelled or terminal and cannot begin execution".into(),
            ));
        }
    }
    Ok(())
}
