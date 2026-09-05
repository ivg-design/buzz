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
    Ok(())
}
