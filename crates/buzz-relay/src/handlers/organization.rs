//! Validate organization overlays before the ordinary durable event insert.

use std::sync::Arc;

use buzz_core::{organization, tenant::TenantContext};
use nostr::Event;

use crate::state::AppState;

/// Validate every reference under the relay-resolved community. The normal
/// ingest MessagesWrite, membership, archived-channel and timeout gates apply
/// before this function; there is no separate agent tier or organization grant.
pub async fn validate(
    tenant: &TenantContext,
    event: &Event,
    state: &Arc<AppState>,
) -> Result<(), String> {
    let (channel, change) = organization::parse_change(event)?;
    let mut targets = Vec::with_capacity(change.references().len());
    for id in change.references() {
        let bytes = hex::decode(id).map_err(|_| "invalid organization target".to_owned())?;
        let stored = state
            .db
            .get_event_by_id_for_event_write(tenant.community(), &bytes)
            .await
            .map_err(|error| format!("organization target lookup failed: {error}"))?
            .ok_or("organization target is unavailable in this channel")?;
        if stored.channel_id != Some(channel) {
            return Err("organization target is unavailable in this channel".into());
        }
        targets.push(stored.event);
    }
    organization::validate_references(event, &targets)?;
    if let organization::OrganizationAction::Participants { agent_pubkeys, .. } = &change.action {
        validate_participants(tenant, state, channel, agent_pubkeys).await?;
    }
    Ok(())
}

async fn validate_participants(
    tenant: &TenantContext,
    state: &AppState,
    channel_id: uuid::Uuid,
    agent_pubkeys: &[String],
) -> Result<(), String> {
    let channel = state
        .db
        .get_channel_for_event_write(tenant.community(), channel_id)
        .await
        .map_err(|_| "thread channel is unavailable".to_owned())?;
    for pubkey in agent_pubkeys {
        let bytes = hex::decode(pubkey).map_err(|_| "invalid thread participant".to_owned())?;
        let owner = state
            .db
            .get_agent_channel_policy(tenant.community(), &bytes)
            .await
            .map_err(|error| format!("thread participant lookup failed: {error}"))?
            .and_then(|(_, owner)| owner)
            .ok_or("thread participants must be currently enrolled agents")?;
        if state
            .db
            .get_relay_member(tenant.community(), &hex::encode(owner))
            .await
            .map_err(|error| format!("thread participant enrollment lookup failed: {error}"))?
            .is_none()
        {
            return Err("thread participant is no longer enrolled in this community".into());
        }
        // Match ordinary message access without adding channel membership.
        // A DM's participant set remains fixed even if its visibility is bad.
        if (channel.visibility != "open" || channel.channel_type == "dm")
            && !state
                .db
                .is_member(tenant.community(), channel_id, &bytes)
                .await
                .map_err(|error| format!("thread participant access lookup failed: {error}"))?
        {
            return Err("thread participant is not a member of this private conversation".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
