use super::super::{ProjectGitCommitParams, ProjectGitParams};
use super::*;
use std::process::Command;

fn managed_nemo_relay_without_github_login() -> (tempfile::TempDir, TrustedRelay) {
    let harness = tempfile::tempdir().expect("harness");
    let checkout = harness.path().join("REPOS/nemo");
    std::fs::create_dir_all(&checkout).expect("checkout");
    let run = |args: &[&str]| {
        assert!(Command::new("git")
            .arg("-C")
            .arg(&checkout)
            .args(args)
            .status()
            .expect("Git fixture")
            .success());
    };
    run(&["init", "--quiet"]);
    run(&["config", "user.name", "Buzz Test"]);
    run(&["config", "user.email", "buzz-test@example.invalid"]);
    std::fs::write(checkout.join("fixture.txt"), "fixture\n").expect("fixture");
    run(&["add", "fixture.txt"]);
    run(&["commit", "--quiet", "-m", "fixture"]);
    run(&[
        "remote",
        "add",
        "origin",
        "https://github.com/mysteropodes/nemo.git",
    ]);
    let keys = nostr::Keys::generate();
    let relay = TrustedRelay::new(super::super::TrustedConfig {
        relay_url: buzz_core::nemo::RELAY_URL.into(),
        owner_pubkey: keys.public_key().to_hex(),
        owner_github_login: None,
        keys,
        auth_tag: None,
        auth_tag_json: None,
        grants: super::super::GrantSet::load_with_nemo(harness.path(), None, None, true)
            .expect("managed grants"),
        a2a_channel_id: Some(buzz_core::nemo::HOME_CHANNEL.into()),
        session_channel_id: None,
        session_thread_root_id: None,
        job_operation_id: None,
        job_request_event_id: None,
        session_working_directory: None,
        github_credentials: Default::default(),
        allow_insecure_loopback: false,
    })
    .expect("trusted relay");
    (harness, relay)
}

fn managed_dispatch_params(worktree_id: Option<&str>) -> A2aDispatchParams {
    A2aDispatchParams {
        operation_id: "a580ca9b-47b4-4af9-b22a-1068778f26c6".into(),
        idempotency_key: "nemo-auto".into(),
        coordinator_epoch: 1,
        recipient_pubkey: "b".repeat(64),
        capability: "rust".into(),
        summary: "Implement bounded Nemo work".into(),
        acceptance: vec!["Focused tests pass".into()],
        worktree_id: worktree_id.map(str::to_owned),
        paths: vec!["new/source.rs".into()],
        contracts: vec![],
        github_issue: None,
        github_pr: None,
        github_run: None,
        supersedes_event_id: None,
        ttl_seconds: 600,
    }
}

#[tokio::test]
async fn managed_nemo_dispatch_needs_no_github_login_and_uses_portable_worktree_ids() {
    let (_harness, relay) = managed_nemo_relay_without_github_login();
    let request = build_request(&relay, managed_dispatch_params(Some("worker_2")))
        .await
        .expect("managed dispatch");
    let JobEvent::Request(request) = request else {
        panic!("expected request");
    };
    assert_eq!(
        request.common.sponsor.github_login,
        buzz_core::nemo::UNLINKED_GITHUB_LOGIN
    );
    assert_eq!(request.common.repository.worktree_id, "worker_2");
    assert_eq!(request.common.repository.branch, "codex/worker_2");

    for invalid in ["team/worker", ".hidden", "trailing."] {
        let error = build_request(&relay, managed_dispatch_params(Some(invalid)))
            .await
            .expect_err("receiver-incompatible worktree id");
        assert!(error.contains("outside the Nemo repository policy"));
    }
}

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

#[test]
fn project_git_schemas_expose_no_checkout_or_transport_authority() {
    assert!(serde_json::from_value::<ProjectGitParams>(serde_json::json!({})).is_ok());
    assert!(
        serde_json::from_value::<ProjectGitCommitParams>(serde_json::json!({
            "message": "bounded change"
        }))
        .is_ok()
    );
    for forbidden in [
        "root",
        "path",
        "paths",
        "repository",
        "remote",
        "url",
        "branch",
        "refspec",
        "force",
        "credential_helper",
        "signing_key",
    ] {
        let mut empty = serde_json::json!({});
        empty[forbidden] = serde_json::json!("attacker-controlled");
        assert!(
            serde_json::from_value::<ProjectGitParams>(empty).is_err(),
            "empty Project Git schema accepted {forbidden}"
        );

        let mut commit = serde_json::json!({"message": "bounded change"});
        commit[forbidden] = serde_json::json!("attacker-controlled");
        assert!(
            serde_json::from_value::<ProjectGitCommitParams>(commit).is_err(),
            "Project Git commit schema accepted {forbidden}"
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
            a2a_channel_id: None,
            session_channel_id: Some("3580ca9b-47b4-4af9-b22a-1068778f26c6".into()),
            session_thread_root_id: None,
            job_operation_id: Some("a580ca9b-47b4-4af9-b22a-1068778f26c6".into()),
            job_request_event_id: Some("a".repeat(64)),
            session_working_directory: None,
            github_credentials: Default::default(),
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
            worktree_id: Some("other".into()),
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
