use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use base64::{engine::general_purpose::STANDARD, Engine};
use buzz_core::job::{semantic_request_digest, JobEvent};
use buzz_core::job_authorization::{
    JobAuthorizationRequest, JobAuthorizationResponse, JOB_AUTHORIZATION_SCHEMA_VERSION,
};
use nostr::{EventBuilder, Keys, Kind, Tag, Timestamp};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tower::ServiceExt;
use uuid::Uuid;

use crate::router::build_router;
use crate::test_support::job::{persist, JobFixture};

fn authorization_request(fixture: &JobFixture, root: &nostr::Event, nonce: Uuid) -> String {
    let JobEvent::Request(request) = JobEvent::parse(root).expect("parse stored request") else {
        unreachable!()
    };
    serde_json::to_string(&JobAuthorizationRequest {
        schema_version: JOB_AUTHORIZATION_SCHEMA_VERSION.into(),
        nonce: nonce.to_string(),
        request_event_id: root.id.to_hex(),
        semantic_digest: semantic_request_digest(&request).expect("semantic digest"),
        community_id: fixture.tenant.community().as_uuid().to_string(),
        relay_host: fixture.tenant.host().into(),
        channel_id: fixture.channel_id.to_string(),
        project_address: fixture.project_address.clone(),
        repository: request.common.repository,
        requester_pubkey: fixture.requester.public_key().to_hex(),
        recipient_pubkey: fixture.worker.public_key().to_hex(),
    })
    .expect("serialize authorization request")
}

fn nip98_auth_header(keys: &Keys, url: &str, signed_body: &[u8], created_at: Timestamp) -> String {
    let hash: [u8; 32] = Sha256::digest(signed_body).into();
    let tags = vec![
        Tag::parse(["u", url]).expect("u tag"),
        Tag::parse(["method", "POST"]).expect("method tag"),
        Tag::parse(["payload", hex::encode(hash).as_str()]).expect("payload tag"),
    ];
    let event = EventBuilder::new(Kind::HttpAuth, "")
        .tags(tags)
        .custom_created_at(created_at)
        .sign_with_keys(keys)
        .expect("sign NIP-98 event");
    let encoded = STANDARD.encode(serde_json::to_vec(&event).expect("serialize NIP-98 event"));
    format!("Nostr {encoded}")
}

async fn post(
    fixture: &JobFixture,
    signer: &Keys,
    signed_body: &str,
    actual_body: &str,
    created_at: Timestamp,
) -> axum::response::Response {
    let path = "/api/jobs/authorize";
    let url = format!("https://{}{}", fixture.tenant.host(), path);
    let auth = nip98_auth_header(signer, &url, signed_body.as_bytes(), created_at);
    build_router(Arc::clone(&fixture.state))
        .oneshot(
            Request::post(path)
                .header(header::HOST, fixture.tenant.host())
                .header(header::AUTHORIZATION, auth)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(actual_body.to_owned()))
                .expect("authorization request"),
        )
        .await
        .expect("authorization response")
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read response body");
    serde_json::from_slice(&bytes).expect("response JSON")
}

async fn seeded_request(fixture: &JobFixture, idempotency: &str) -> nostr::Event {
    let root = fixture.request(Uuid::new_v4(), idempotency);
    assert!(persist(fixture, &root).await.expect("store request root"));
    root
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn full_authorize_route_returns_fresh_exact_body_bound_evidence() {
    let fixture = JobFixture::new(6).await;
    let root = seeded_request(&fixture, "http-valid").await;
    let body = authorization_request(&fixture, &root, Uuid::new_v4());

    let response = post(&fixture, &fixture.worker, &body, &body, Timestamp::now()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let decoded: JobAuthorizationResponse =
        serde_json::from_value(response_json(response).await).expect("authorization response");
    let request = JobAuthorizationRequest::parse_strict(body.as_bytes()).expect("valid request");
    decoded
        .validate_for(&request, chrono::Utc::now())
        .expect("fresh exact response");
    assert_eq!(decoded.repository_coordinate, fixture.repository_coordinate);
    assert_eq!(
        decoded.requester_owner_pubkey,
        fixture.requester.public_key().to_hex()
    );
    assert_eq!(
        decoded.recipient_owner_pubkey,
        fixture.requester.public_key().to_hex()
    );
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn authorize_route_rejects_duplicate_json_and_nip98_body_substitution() {
    let fixture = JobFixture::new(6).await;
    let root = seeded_request(&fixture, "http-json-binding").await;
    let body = authorization_request(&fixture, &root, Uuid::new_v4());
    let nonce = serde_json::from_str::<Value>(&body).expect("body")["nonce"]
        .as_str()
        .expect("nonce")
        .to_owned();
    let duplicate = body.replacen('{', &format!("{{\"nonce\":\"{nonce}\","), 1);
    let response = post(
        &fixture,
        &fixture.worker,
        &duplicate,
        &duplicate,
        Timestamp::now(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let changed = body.replace("\"github_issue\":\"1\"", "\"github_issue\":\"2\"");
    assert_ne!(changed, body);
    let response = post(&fixture, &fixture.worker, &body, &changed, Timestamp::now()).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn authorize_route_rejects_wrong_signer_and_current_member_removal() {
    let fixture = JobFixture::new(6).await;
    let root = seeded_request(&fixture, "http-authority").await;
    let body = authorization_request(&fixture, &root, Uuid::new_v4());
    let response = post(&fixture, &fixture.requester, &body, &body, Timestamp::now()).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    fixture
        .state
        .db
        .remove_member(
            fixture.tenant.community(),
            fixture.channel_id,
            &fixture.worker.public_key().to_bytes(),
            &fixture.requester.public_key().to_bytes(),
        )
        .await
        .expect("remove worker from project channel");
    let removed_body = authorization_request(&fixture, &root, Uuid::new_v4());
    let response = post(
        &fixture,
        &fixture.worker,
        &removed_body,
        &removed_body,
        Timestamp::now(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn authorization_nonce_is_single_use_under_replay_and_concurrency() {
    let fixture = JobFixture::new(8).await;
    let root = seeded_request(&fixture, "http-nonce").await;
    let body = authorization_request(&fixture, &root, Uuid::new_v4());
    let now = Timestamp::now().as_secs();
    let first = post(
        &fixture,
        &fixture.worker,
        &body,
        &body,
        Timestamp::from(now.saturating_sub(1)),
    )
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    let replay = post(
        &fixture,
        &fixture.worker,
        &body,
        &body,
        Timestamp::from(now),
    )
    .await;
    assert_eq!(replay.status(), StatusCode::CONFLICT);

    let concurrent_body = authorization_request(&fixture, &root, Uuid::new_v4());
    let (left, right) = tokio::join!(
        post(
            &fixture,
            &fixture.worker,
            &concurrent_body,
            &concurrent_body,
            Timestamp::from(now.saturating_sub(2)),
        ),
        post(
            &fixture,
            &fixture.worker,
            &concurrent_body,
            &concurrent_body,
            Timestamp::from(now.saturating_sub(3)),
        )
    );
    let mut statuses = [left.status(), right.status()];
    statuses.sort_by_key(|status| status.as_u16());
    assert_eq!(statuses, [StatusCode::OK, StatusCode::CONFLICT]);
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn invalid_json_does_not_consume_the_embedded_authorization_nonce() {
    let fixture = JobFixture::new(6).await;
    let root = seeded_request(&fixture, "http-invalid-nonce").await;
    let body = authorization_request(&fixture, &root, Uuid::new_v4());
    let invalid = format!("{} trailing", body);
    let now = Timestamp::now().as_secs();
    let response = post(
        &fixture,
        &fixture.worker,
        &invalid,
        &invalid,
        Timestamp::from(now.saturating_sub(1)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let valid = post(
        &fixture,
        &fixture.worker,
        &body,
        &body,
        Timestamp::from(now),
    )
    .await;
    assert_eq!(valid.status(), StatusCode::OK);
}
