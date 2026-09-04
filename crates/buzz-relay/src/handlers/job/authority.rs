use nostr::PublicKey;

use buzz_core::job::{JobControlAction, JobEvent, JobRequest};
use buzz_core::tenant::TenantContext;

use crate::state::AppState;

use super::gate::JobAuthError;

pub(super) async fn require_registered_agent(
    tenant: &TenantContext,
    state: &AppState,
    pubkey: &PublicKey,
    label: &str,
) -> Result<(), JobAuthError> {
    let record = state
        .db
        .get_agent_channel_policy(tenant.community(), &pubkey.to_bytes())
        .await
        .map_err(|error| JobAuthError::Internal(format!("loading {label} ownership: {error}")))?;
    let Some((_, Some(owner))) = record else {
        return Err(JobAuthError::Restricted(format!(
            "{label} must be a currently registered sponsored agent"
        )));
    };
    require_relay_member_hex(tenant, state, &hex::encode(owner), label).await?;
    Ok(())
}

pub(super) async fn require_registered_agent_locked(
    tenant: &TenantContext,
    lock: &mut buzz_db::JobOperationLock,
    pubkey: &PublicKey,
    label: &str,
) -> Result<(), JobAuthError> {
    let owner = lock
        .user_owner_for_share(tenant.community(), &pubkey.to_bytes())
        .await
        .map_err(|error| JobAuthError::Internal(format!("locking {label} ownership: {error}")))?
        .flatten()
        .ok_or_else(|| {
            JobAuthError::Restricted(format!(
                "{label} must be a currently registered sponsored agent"
            ))
        })?;
    require_relay_member_hex_locked(tenant, lock, &hex::encode(owner), label).await
}

/// Re-resolve relay admission from the writer on every privileged job write.
/// Existing transport authentication may outlive a member removal; jobs do not.
pub(super) async fn require_current_relay_membership(
    tenant: &TenantContext,
    state: &AppState,
    pubkey: &PublicKey,
    label: &str,
) -> Result<(), JobAuthError> {
    let record = state
        .db
        .get_agent_channel_policy(tenant.community(), &pubkey.to_bytes())
        .await
        .map_err(|error| JobAuthError::Internal(format!("loading {label} identity: {error}")))?
        .ok_or_else(|| JobAuthError::Restricted(format!("{label} is not a registered user")))?;
    let membership_pubkey = record.1.map(hex::encode).unwrap_or_else(|| pubkey.to_hex());
    require_relay_member_hex(tenant, state, &membership_pubkey, label).await
}

pub(super) async fn require_current_relay_membership_locked(
    tenant: &TenantContext,
    lock: &mut buzz_db::JobOperationLock,
    pubkey: &PublicKey,
    label: &str,
) -> Result<(), JobAuthError> {
    let owner = lock
        .user_owner_for_share(tenant.community(), &pubkey.to_bytes())
        .await
        .map_err(|error| JobAuthError::Internal(format!("locking {label} identity: {error}")))?
        .ok_or_else(|| JobAuthError::Restricted(format!("{label} is not a registered user")))?;
    let membership_pubkey = owner.map(hex::encode).unwrap_or_else(|| pubkey.to_hex());
    require_relay_member_hex_locked(tenant, lock, &membership_pubkey, label).await
}

async fn require_relay_member_hex(
    tenant: &TenantContext,
    state: &AppState,
    membership_pubkey: &str,
    label: &str,
) -> Result<(), JobAuthError> {
    let member = state
        .db
        .get_relay_member(tenant.community(), membership_pubkey)
        .await
        .map_err(|error| JobAuthError::Internal(format!("loading {label} membership: {error}")))?;
    if member.is_none() {
        return Err(JobAuthError::Restricted(format!(
            "{label} no longer has current relay membership"
        )));
    }
    Ok(())
}

async fn require_relay_member_hex_locked(
    tenant: &TenantContext,
    lock: &mut buzz_db::JobOperationLock,
    membership_pubkey: &str,
    label: &str,
) -> Result<(), JobAuthError> {
    let present = lock
        .relay_member_for_share(tenant.community(), membership_pubkey)
        .await
        .map_err(|error| JobAuthError::Internal(format!("locking {label} membership: {error}")))?;
    if !present {
        return Err(JobAuthError::Restricted(format!(
            "{label} no longer has current relay membership"
        )));
    }
    Ok(())
}

pub(super) async fn require_channel_member_locked(
    tenant: &TenantContext,
    lock: &mut buzz_db::JobOperationLock,
    channel_id: uuid::Uuid,
    pubkey: &PublicKey,
    label: &str,
) -> Result<(), JobAuthError> {
    let present = lock
        .channel_member_for_share(tenant.community(), channel_id, &pubkey.to_bytes())
        .await
        .map_err(|error| {
            JobAuthError::Internal(format!("locking {label} channel membership: {error}"))
        })?;
    if !present {
        return Err(JobAuthError::Restricted(format!(
            "{label} must be a direct member of the project home channel"
        )));
    }
    Ok(())
}

pub(super) async fn effective_owner_locked(
    tenant: &TenantContext,
    lock: &mut buzz_db::JobOperationLock,
    pubkey: &PublicKey,
    require_agent: bool,
    label: &str,
) -> Result<String, JobAuthError> {
    let owner = lock
        .user_owner_for_share(tenant.community(), &pubkey.to_bytes())
        .await
        .map_err(|error| JobAuthError::Internal(format!("locking {label} identity: {error}")))?
        .ok_or_else(|| JobAuthError::Restricted(format!("{label} is not a registered user")))?;
    if require_agent && owner.is_none() {
        return Err(JobAuthError::Restricted(format!(
            "{label} must be a currently registered sponsored agent"
        )));
    }
    let effective = owner.map(hex::encode).unwrap_or_else(|| pubkey.to_hex());
    require_relay_member_hex_locked(tenant, lock, &effective, label).await?;
    Ok(effective)
}

pub(super) fn validate_superseding_request(
    request: &JobRequest,
    superseded: &JobEvent,
    old_root: &JobEvent,
) -> Result<(), JobAuthError> {
    let JobEvent::Control(handoff) = superseded else {
        return Err(JobAuthError::Invalid(
            "supersedes_event_id must reference a kind 43005 handoff".into(),
        ));
    };
    if handoff.action != JobControlAction::Handoff {
        return Err(JobAuthError::Invalid(
            "supersedes_event_id must reference a handoff action".into(),
        ));
    }
    let Some(target) = handoff.handoff_to.as_deref() else {
        return Err(JobAuthError::Invalid(
            "superseded handoff is missing handoff_to".into(),
        ));
    };
    let JobEvent::Request(original) = old_root else {
        return Err(JobAuthError::Invalid(
            "superseded handoff request root is not kind 43001".into(),
        ));
    };
    let old = &handoff.followup.common;
    let next = &request.common;
    if next.sender_pubkey != original.common.sender_pubkey
        || next.recipient_pubkey != target
        || next.operation_id != old.operation_id
        || next.idempotency_key != old.idempotency_key
        || next.project != old.project
        || next.repository != old.repository
        || old.coordinator_epoch.checked_add(1) != Some(next.coordinator_epoch)
        || next.sponsor != original.common.sponsor
        || next.expires_at != original.common.expires_at
        || request.capability != original.capability
        || request.summary != original.summary
        || request.acceptance != original.acceptance
    {
        return Err(JobAuthError::Restricted(
            "superseding request does not match the authorized handoff scope and next epoch".into(),
        ));
    }
    Ok(())
}
