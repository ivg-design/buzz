use super::*;

#[test]
fn tool_surface_has_no_lifecycle_outcome_parameters() {
    let schemas = [
        serde_json::to_string(&schemars::schema_for!(A2aDispatchParams)).unwrap(),
        serde_json::to_string(&schemars::schema_for!(A2aCancelParams)).unwrap(),
        serde_json::to_string(&schemars::schema_for!(A2aHandoffParams)).unwrap(),
    ]
    .join("\n");
    for forbidden in ["progress", "result", "accepted", "failed", "indeterminate"] {
        assert!(!schemas.contains(&format!("\\\"{forbidden}\\\"")));
    }
}

#[test]
fn chat_schema_accepts_only_content() {
    assert!(serde_json::from_value::<ChatSendParams>(serde_json::json!({
        "content": "hello"
    }))
    .is_ok());
    for forbidden in ["channel", "thread", "recipient", "kind", "url", "owner"] {
        let mut value = serde_json::json!({"content": "hello"});
        value[forbidden] = serde_json::json!("attacker-controlled");
        assert!(
            serde_json::from_value::<ChatSendParams>(value).is_err(),
            "chat accepted caller-controlled {forbidden}"
        );
    }
}

#[test]
fn dispatch_rejects_authority_fields_outside_the_typed_surface() {
    let base = serde_json::json!({
        "operation_id": "a580ca9b-47b4-4af9-b22a-1068778f26c6",
        "idempotency_key": "sentinel",
        "coordinator_epoch": 1,
        "recipient_pubkey": "a".repeat(64),
        "capability": "rust",
        "summary": "sentinel",
        "acceptance": ["passes"],
        "worktree_id": "nemo-a2a",
        "paths": ["crates/buzz-dev-mcp"]
    });
    assert!(serde_json::from_value::<A2aDispatchParams>(base.clone()).is_ok());
    for forbidden in [
        "project",
        "channel",
        "repository",
        "base_sha",
        "branch",
        "sender_pubkey",
        "sponsor",
        "event_kind",
        "lifecycle_state",
    ] {
        let mut value = base.clone();
        value[forbidden] = serde_json::json!("attacker-controlled");
        assert!(
            serde_json::from_value::<A2aDispatchParams>(value).is_err(),
            "dispatch accepted caller-controlled {forbidden}"
        );
    }
}

fn job_bound_relay() -> Arc<TrustedRelay> {
    let keys = nostr::Keys::generate();
    Arc::new(
        TrustedRelay::new(super::super::TrustedConfig {
            relay_url: "http://127.0.0.1:1".into(),
            keys,
            auth_tag: None,
            auth_tag_json: None,
            owner_pubkey: "b".repeat(64),
            owner_github_login: Some("owner".into()),
            grants: super::super::GrantSet::default(),
            session_channel_id: Some("3580ca9b-47b4-4af9-b22a-1068778f26c6".into()),
            session_thread_root_id: None,
            job_operation_id: Some("a580ca9b-47b4-4af9-b22a-1068778f26c6".into()),
            job_request_event_id: Some("a".repeat(64)),
            allow_insecure_loopback: true,
        })
        .expect("test relay"),
    )
}

#[tokio::test]
async fn job_session_cannot_dispatch_a_sibling_operation() {
    let result = dispatch(
        &job_bound_relay(),
        A2aDispatchParams {
            operation_id: "b580ca9b-47b4-4af9-b22a-1068778f26c6".into(),
            idempotency_key: "sibling".into(),
            coordinator_epoch: 1,
            recipient_pubkey: "b".repeat(64),
            capability: "rust".into(),
            summary: "must not publish".into(),
            acceptance: vec!["no event".into()],
            worktree_id: "other".into(),
            paths: vec!["crates".into()],
            contracts: vec![],
            github_issue: None,
            github_pr: None,
            github_run: None,
            supersedes_event_id: None,
            ttl_seconds: 60,
        },
        tokio_util::sync::CancellationToken::new(),
    )
    .await;
    let encoded = serde_json::to_string(&result).unwrap();
    assert!(encoded.contains("unavailable inside a one-shot job session"));
    assert!(!encoded.contains("event_id"));
}

#[tokio::test]
async fn job_session_cannot_list_sibling_inbox_items() {
    let result = inbox(
        &job_bound_relay(),
        A2aInboxParams { limit: 50 },
        tokio_util::sync::CancellationToken::new(),
    )
    .await;
    let encoded = serde_json::to_string(&result).unwrap();
    assert!(encoded.contains("unavailable inside a one-shot job session"));
    assert!(!encoded.contains("events"));
}

#[tokio::test]
async fn job_session_cannot_send_duplicate_user_facing_chat() {
    let result = send_chat(
        &job_bound_relay(),
        ChatSendParams {
            content: "must not publish".into(),
        },
        tokio_util::sync::CancellationToken::new(),
    )
    .await;
    let encoded = serde_json::to_string(&result).unwrap();
    assert!(encoded.contains("unavailable inside a one-shot job session"));
    assert!(!encoded.contains("event_id"));
}
