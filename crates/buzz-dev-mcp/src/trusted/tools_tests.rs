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
