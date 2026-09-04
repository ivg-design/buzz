//! Immediate NIP-98 authorization preflight for durable A2A job receivers.

use std::sync::Arc;

use axum::{body::Bytes, extract::State, http::HeaderMap, http::StatusCode, response::Json};
use buzz_core::job::{JOB_TERMINAL_AUDIT_GRACE_SECONDS, MAX_JOB_TTL_SECONDS};
use buzz_core::job_authorization::{
    JobAuthorizationBinding, JobAuthorizationRequest, JobAuthorizationResponse,
    JOB_AUTHORIZATION_SCHEMA_VERSION, JOB_AUTHORIZATION_TTL_SECONDS,
};
use chrono::{Duration, SecondsFormat, Timelike, Utc};
use sha2::{Digest, Sha256};

use crate::handlers::job::{authorize_stored_request, JobAuthError};
use crate::state::AppState;

use super::bridge::{
    check_nip98_replay, enforce_http_admission, nip98_expected_url,
    verify_bridge_auth_with_options, VerifiedBridgeAuth,
};
use super::{api_error, internal_error};

/// Re-authorize one stored request against current Project and membership state.
pub async fn authorize(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<JobAuthorizationResponse>, (StatusCode, Json<serde_json::Value>)> {
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
    require_secure_transport(&state, tenant.host())?;
    let url = nip98_expected_url(&state.config.relay_url, &tenant, "/api/jobs/authorize");
    let VerifiedBridgeAuth {
        pubkey,
        event_id_bytes,
        signed_created_at,
    } = verify_bridge_auth_with_options(
        &headers,
        "POST",
        &url,
        Some(&body),
        state.config.require_auth_token,
        true,
    )?;
    enforce_http_admission(&state, &tenant, &pubkey).await?;
    check_nip98_replay(&state, &tenant, event_id_bytes).await?;
    let auth_tag = super::relay_members::extract_auth_tag_header(&headers);
    super::relay_members::enforce_relay_membership(
        &state,
        tenant.community(),
        &pubkey.to_bytes(),
        auth_tag,
        signed_created_at,
    )
    .await?;
    let request = JobAuthorizationRequest::parse_strict(&body).map_err(|error| {
        api_error(
            StatusCode::BAD_REQUEST,
            &format!("invalid request: {error}"),
        )
    })?;
    mark_nonce_once(&state, &tenant, &request.nonce).await?;
    let evidence = authorize_stored_request(&tenant, &state, &pubkey, &request)
        .await
        .map_err(map_job_error)?;
    let issued = Utc::now()
        .with_nanosecond(0)
        .ok_or_else(|| internal_error("constructing job authorization timestamp failed"))?;
    let response = JobAuthorizationResponse {
        schema_version: JOB_AUTHORIZATION_SCHEMA_VERSION.into(),
        authorized: true,
        authorization_id: uuid::Uuid::new_v4().to_string(),
        issued_at: issued.to_rfc3339_opts(SecondsFormat::Secs, true),
        expires_at: (issued + Duration::seconds(JOB_AUTHORIZATION_TTL_SECONDS))
            .to_rfc3339_opts(SecondsFormat::Secs, true),
        binding: JobAuthorizationBinding::from(&request),
        project_head_event_id: evidence.project_head_event_id,
        repository_coordinate: evidence.repository_coordinate,
        repository_announcement_event_id: evidence.repository_announcement_event_id,
        requester_owner_pubkey: evidence.requester_owner_pubkey,
        recipient_owner_pubkey: evidence.recipient_owner_pubkey,
    };
    response
        .validate_for(&request, issued)
        .map_err(|error| internal_error(&format!("invalid authorization response: {error}")))?;
    Ok(Json(response))
}

async fn mark_nonce_once(
    state: &AppState,
    tenant: &buzz_core::TenantContext,
    nonce: &str,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    let mut hash = Sha256::new();
    hash.update(b"buzz.jobs.authorize.nonce.v1\0");
    hash.update(tenant.community().as_uuid().as_bytes());
    hash.update(nonce.as_bytes());
    let event_id = nostr::EventId::from_byte_array(hash.finalize().into());
    match state
        .nip98_replay
        .try_mark(
            tenant,
            &event_id,
            (MAX_JOB_TTL_SECONDS + JOB_TERMINAL_AUDIT_GRACE_SECONDS) as u64,
        )
        .await
    {
        Ok(true) => Ok(()),
        Ok(false) => Err(api_error(
            StatusCode::CONFLICT,
            "job authorization nonce was already used",
        )),
        Err(error) => {
            tracing::warn!(%error, "job authorization nonce replay guard unavailable");
            Err(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "job authorization nonce replay check unavailable",
            ))
        }
    }
}

fn require_secure_transport(
    state: &AppState,
    tenant_host: &str,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    require_secure_transport_config(
        &state.config.relay_url,
        state.config.require_auth_token,
        tenant_host,
    )
}

fn require_secure_transport_config(
    relay_url: &str,
    require_auth_token: bool,
    tenant_host: &str,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    if relay_url.trim_start().starts_with("wss://") {
        return Ok(());
    }
    let host_without_port = tenant_host
        .strip_prefix('[')
        .and_then(|value| value.split_once(']').map(|(host, _)| host))
        .or_else(|| tenant_host.split(':').next())
        .unwrap_or(tenant_host);
    let loopback = matches!(host_without_port, "localhost" | "127.0.0.1" | "::1");
    if !require_auth_token && loopback {
        return Ok(());
    }
    Err(api_error(
        StatusCode::UPGRADE_REQUIRED,
        "job authorization requires HTTPS; HTTP is allowed only for loopback dev mode",
    ))
}

fn map_job_error(error: JobAuthError) -> (StatusCode, Json<serde_json::Value>) {
    match error {
        JobAuthError::Invalid(message) => api_error(StatusCode::BAD_REQUEST, &message),
        JobAuthError::Restricted(message) => api_error(StatusCode::FORBIDDEN, &message),
        JobAuthError::Internal(message) => internal_error(&message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secure_transport_allows_only_tls_or_explicit_loopback_dev() {
        fn accepts(relay_url: &str, require_auth: bool, host: &str) -> bool {
            require_secure_transport_config(relay_url, require_auth, host).is_ok()
        }

        assert!(accepts("wss://relay.example", true, "relay.example"));
        assert!(accepts("ws://localhost:3000", false, "localhost:3000"));
        assert!(!accepts("ws://relay.example", false, "relay.example"));
        assert!(!accepts("ws://localhost:3000", true, "localhost:3000"));
    }
}

#[cfg(test)]
mod postgres_tests;
