use buzz_core::job::{JobRequest, JobSponsor};
use buzz_core::job_authorization::{
    JobAuthorizationRequest, JobAuthorizationResponse, JOB_AUTHORIZATION_SCHEMA_VERSION,
};
use chrono::Utc;

use super::ReceiverError;
use crate::relay::{AuthenticatedContext, RestClient};

const AUTHORIZATION_PATH: &str = "/api/jobs/authorize";

/// Revalidate server-owned authority immediately before the local durable claim.
pub async fn authorize(
    rest: &RestClient,
    tenant: &AuthenticatedContext,
    request: &JobRequest,
    request_event_id: &str,
    semantic_digest: &str,
    recipient_sponsor: &JobSponsor,
    allow_insecure_loopback: bool,
) -> Result<(), ReceiverError> {
    require_secure_transport(&rest.base_url, allow_insecure_loopback)?;
    let authorization = JobAuthorizationRequest {
        schema_version: JOB_AUTHORIZATION_SCHEMA_VERSION.into(),
        nonce: uuid::Uuid::new_v4().to_string(),
        request_event_id: request_event_id.into(),
        semantic_digest: semantic_digest.into(),
        community_id: tenant.community_id.clone(),
        relay_host: tenant.host.clone(),
        channel_id: request.common.project.home_channel.clone(),
        project_address: request.common.project.address.clone(),
        repository: request.common.repository.clone(),
        requester_pubkey: request.common.sender_pubkey.clone(),
        recipient_pubkey: request.common.recipient_pubkey.clone(),
    };
    authorization.validate().map_err(ReceiverError::Tenant)?;
    let raw = rest
        .post_authenticated_json_raw(AUTHORIZATION_PATH, &authorization)
        .await?;
    let value = super::outcome::parse_unique_json(&raw).map_err(|error| {
        ReceiverError::Tenant(format!("invalid {AUTHORIZATION_PATH} response: {error}"))
    })?;
    let response: JobAuthorizationResponse = serde_json::from_value(value).map_err(|error| {
        ReceiverError::Tenant(format!("invalid {AUTHORIZATION_PATH} response: {error}"))
    })?;
    response
        .validate_for(&authorization, Utc::now())
        .map_err(ReceiverError::Tenant)?;
    if response.requester_owner_pubkey != request.common.sponsor.pubkey
        || response.recipient_owner_pubkey != recipient_sponsor.pubkey
    {
        return Err(ReceiverError::Tenant(
            "job authorization owner bindings do not match the signed request and local worker"
                .into(),
        ));
    }
    Ok(())
}

fn require_secure_transport(
    base_url: &str,
    allow_insecure_loopback: bool,
) -> Result<(), ReceiverError> {
    let relay_url = url::Url::parse(base_url)
        .map_err(|_| ReceiverError::Tenant("job authorization relay URL is invalid".into()))?;
    let explicit_loopback_dev = matches!(
        relay_url.host_str(),
        Some("127.0.0.1" | "::1" | "[::1]" | "localhost")
    ) && allow_insecure_loopback;
    if relay_url.scheme() != "https" && !explicit_loopback_dev {
        return Err(ReceiverError::Tenant(
            "job authorization requires HTTPS (plain HTTP is limited to explicit loopback dev mode)"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arbitrary_plain_http_is_rejected() {
        assert!(require_secure_transport("http://relay.example", true).is_err());
        assert!(require_secure_transport("https://relay.example", false).is_ok());
        for loopback in [
            "http://127.0.0.1:3000",
            "http://localhost:3000",
            "http://[::1]:3000",
        ] {
            assert!(require_secure_transport(loopback, false).is_err());
            assert!(require_secure_transport(loopback, true).is_ok());
        }
    }
}
