//! Owner/admin management endpoints for durable relay invites.
//!
//! These endpoints expose only non-secret row metadata. The reusable invite
//! code and its stored SHA-256 digest never appear in list or revoke responses.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::Json,
};
use serde_json::{json, Value};

use crate::state::AppState;

use super::{api_error, bridge, internal_error};

fn can_manage_invites(role: &str) -> bool {
    role == "owner" || role == "admin"
}

async fn authenticate_manager(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    method: &str,
    path: &str,
) -> Result<(buzz_core::TenantContext, String), (StatusCode, Json<Value>)> {
    let raw_host = headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let tenant = crate::tenant::bind_community(&state.db, raw_host)
        .await
        .map_err(|_| {
            api_error(
                StatusCode::NOT_FOUND,
                "relay: no community is configured for this host",
            )
        })?;
    let url = bridge::nip98_expected_url(&state.config.relay_url, &tenant, path);
    let bridge::VerifiedBridgeAuth {
        pubkey,
        event_id_bytes,
        ..
    } = bridge::verify_bridge_auth_with_options(headers, method, &url, None, true, false)?;
    bridge::check_nip98_replay(state, &tenant, event_id_bytes).await?;

    let sender_hex = pubkey.to_hex();
    let role = state
        .db
        .get_relay_member(tenant.community(), &sender_hex)
        .await
        .map_err(|error| internal_error(&format!("invite manager role lookup: {error}")))?
        .map(|member| member.role)
        .unwrap_or_default();
    if !can_manage_invites(&role) {
        return Err(api_error(
            StatusCode::FORBIDDEN,
            "only relay owners and admins can manage invites",
        ));
    }
    Ok((tenant, sender_hex))
}

fn pending_invite_json(invite: buzz_db::relay_invite_admin::PendingRelayInvite) -> Value {
    json!({
        "id": invite.id,
        "max_uses": invite.max_uses,
        "use_count": invite.use_count,
        "uses_remaining": invite.uses_remaining,
        "expires_at": invite.expires_at.timestamp(),
        "created_by": invite.created_by,
        "created_at": invite.created_at.timestamp(),
    })
}

/// `GET /api/invites` — list pending invite metadata for an owner/admin.
pub async fn list_invites(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let (tenant, _) = authenticate_manager(&state, &headers, "GET", "/api/invites").await?;
    let invites = state
        .db
        .list_pending_relay_invites(tenant.community())
        .await
        .map_err(|error| internal_error(&format!("invite list: {error}")))?
        .into_iter()
        .map(pending_invite_json)
        .collect::<Vec<_>>();
    Ok(Json(json!({ "invites": invites })))
}

/// `DELETE /api/invites/{invite_id}` — revoke one scoped invite.
pub async fn revoke_invite(
    State(state): State<Arc<AppState>>,
    Path(invite_id): Path<uuid::Uuid>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let path = format!("/api/invites/{invite_id}");
    let (tenant, actor) = authenticate_manager(&state, &headers, "DELETE", &path).await?;
    let removed = state
        .db
        .revoke_relay_invite(tenant.community(), invite_id)
        .await
        .map_err(|error| match error {
            buzz_db::DbError::AccessDenied(_) => api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "community writes are temporarily unavailable",
            ),
            other => internal_error(&format!("invite revoke: {other}")),
        })?;
    if !removed {
        return Err(api_error(StatusCode::NOT_FOUND, "invite_not_found"));
    }
    tracing::info!(
        community = %tenant.community(),
        invite_id = %invite_id,
        revoked_by = %actor,
        "relay invite revoked"
    );
    Ok(Json(json!({ "status": "revoked", "id": invite_id })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone as _, Utc};

    #[test]
    fn only_owner_and_admin_manage_invites() {
        assert!(can_manage_invites("owner"));
        assert!(can_manage_invites("admin"));
        assert!(!can_manage_invites("member"));
        assert!(!can_manage_invites(""));
    }

    #[test]
    fn pending_metadata_never_serializes_bearer_material() {
        let value = pending_invite_json(buzz_db::relay_invite_admin::PendingRelayInvite {
            id: uuid::Uuid::nil(),
            max_uses: Some(1),
            use_count: 0,
            uses_remaining: Some(1),
            expires_at: Utc.timestamp_opt(2, 0).single().expect("timestamp"),
            created_by: "ab".repeat(32),
            created_at: Utc.timestamp_opt(1, 0).single().expect("timestamp"),
        });
        let encoded = value.to_string();
        assert!(!encoded.contains("code"));
        assert!(!encoded.contains("token"));
        assert!(!encoded.contains("hash"));
        assert_eq!(value["uses_remaining"], 1);
    }
}
