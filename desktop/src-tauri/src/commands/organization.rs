//! Authenticated reversible organization writes using the ordinary relay store.

use buzz_core_pkg::organization::{build_change_event, OrganizationAction, OrganizationChange};
use nostr::Timestamp;
use tauri::State;

use crate::{app_state::AppState, relay};

/// Apply one atomic organization change and return its exact acknowledged event.
/// Both scope arguments are captured when the user opens the organization UI.
#[tauri::command]
pub async fn apply_conversation_organization(
    channel_id: String,
    action: OrganizationAction,
    expected_relay_url: String,
    expected_signer_pubkey: String,
    state: State<'_, AppState>,
) -> Result<serde_json::Value, String> {
    if expected_relay_url.trim().is_empty() || expected_signer_pubkey.trim().is_empty() {
        return Err("organization requires the current community and identity".into());
    }
    let channel = uuid::Uuid::parse_str(&channel_id)
        .map_err(|_| "invalid organization channel".to_owned())?;
    let change = OrganizationChange { version: 1, action };
    change.validate()?;
    let base = relay::relay_api_base_url_with_override(&state);
    let keys = state.signing_keys()?;
    relay::assert_expected_relay_scope(Some(&expected_relay_url), &base)?;
    relay::assert_expected_signer(Some(&expected_signer_pubkey), &keys.public_key().to_hex())?;
    let latest = relay::query_relay_at_with_keys(
        &state, &base,
        &[serde_json::json!({
            "#h": [channel], "kinds": [buzz_core_pkg::kind::KIND_CONVERSATION_ORGANIZATION], "limit": 1,
        })],
        &keys, None,
    ).await?;
    let event = build_change_event(channel, &change, &keys, Timestamp::now().as_secs(), &latest)?;
    let result = relay::submit_signed_event_at_with_keys(&event, &state, &base, &keys).await?;
    if result.event_id != event.id.to_hex() || !result.accepted {
        return Err("relay did not acknowledge the exact organization change".into());
    }
    serde_json::to_value(event).map_err(|error| error.to_string())
}
