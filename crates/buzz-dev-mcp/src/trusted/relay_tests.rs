use super::*;
use buzz_core::job::JobControlAction;

fn test_relay() -> TrustedRelay {
    let keys = nostr::Keys::generate();
    TrustedRelay {
        http: reqwest::Client::new(),
        base_url: "https://relay.example".to_owned(),
        relay_host: "relay.example".to_owned(),
        owner_pubkey: keys.public_key().to_hex(),
        owner_github_login: None,
        keys,
        auth_tag: None,
        auth_tag_json: None,
        grants: super::super::GrantSet::default(),
        a2a_channel_id: None,
        session_channel_id: None,
        session_thread_root_id: std::sync::RwLock::new(None),
        job_operation_id: None,
        job_request_event_id: None,
        session_working_directory: None,
        github_credentials: Default::default(),
    }
}

fn owner_backed_relay() -> TrustedRelay {
    let keys = nostr::Keys::generate();
    let owner = nostr::Keys::generate();
    let auth_tag_json = buzz_sdk::nip_oa::compute_auth_tag(&owner, &keys.public_key(), "kind=9")
        .expect("owner authorization");
    let auth_tag =
        buzz_sdk::nip_oa::parse_auth_tag(&auth_tag_json).expect("parsed owner authorization");
    TrustedRelay {
        http: reqwest::Client::new(),
        base_url: "https://relay.example".to_owned(),
        relay_host: "relay.example".to_owned(),
        owner_pubkey: owner.public_key().to_hex(),
        owner_github_login: None,
        keys,
        auth_tag: Some(auth_tag),
        auth_tag_json: Some(auth_tag_json),
        grants: super::super::GrantSet::default(),
        a2a_channel_id: None,
        session_channel_id: None,
        session_thread_root_id: std::sync::RwLock::new(None),
        job_operation_id: None,
        job_request_event_id: None,
        session_working_directory: None,
        github_credentials: Default::default(),
    }
}

#[test]
fn owner_backed_job_uses_http_authorization_without_an_envelope_auth_tag() {
    use buzz_core::job::{
        JobCommon, JobEvent, JobProject, JobRepository, JobRequest, JobSponsor, JOB_SCHEMA_VERSION,
    };

    let relay = owner_backed_relay();
    let recipient = nostr::Keys::generate();
    let job = JobEvent::Request(JobRequest {
        common: JobCommon {
            schema_version: JOB_SCHEMA_VERSION.into(),
            operation_id: "a580ca9b-47b4-4af9-b22a-1068778f26c6".into(),
            idempotency_key: "owner-backed-job".into(),
            coordinator_epoch: 1,
            project: JobProject {
                address: format!("30621:{}:nemo", relay.owner_pubkey),
                home_channel: "3580ca9b-47b4-4af9-b22a-1068778f26c6".into(),
            },
            repository: JobRepository {
                canonical: "https://github.com/mysteropodes/nemo".into(),
                github_issue: None,
                github_pr: None,
                github_run: None,
                base_sha: "a".repeat(40),
                branch: "codex/buzz-owner-auth".into(),
                worktree_id: "buzz-owner-auth".into(),
                paths: vec!["a2a-smoke.tmp".into()],
                contracts: Vec::new(),
            },
            sender_pubkey: relay.signer_pubkey(),
            recipient_pubkey: recipient.public_key().to_hex(),
            sponsor: JobSponsor {
                pubkey: relay.owner_pubkey.clone(),
                github_login: buzz_core::nemo::UNLINKED_GITHUB_LOGIN.into(),
            },
            expires_at: (chrono::Utc::now() + chrono::Duration::minutes(10))
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        },
        capability: "workspace-file-smoke".into(),
        summary: "Create, verify, and remove one temporary file.".into(),
        acceptance: vec!["The temporary file is absent after verification.".into()],
        supersedes_event_id: None,
    });

    let event = relay
        .prepare_job_event(job)
        .expect("owner-backed job event");
    assert!(buzz_core::job::JobEvent::parse(&event).is_ok());
    assert!(!event
        .tags
        .iter()
        .any(|tag| tag.as_slice().first().map(String::as_str) == Some("auth")));

    let chat = relay
        .build_chat_event(
            uuid::Uuid::parse_str("3580ca9b-47b4-4af9-b22a-1068778f26c6").unwrap(),
            "chat authorization sentinel",
            None,
        )
        .expect("owner-backed chat event");
    assert!(chat
        .tags
        .iter()
        .any(|tag| tag.as_slice().first().map(String::as_str) == Some("auth")));
}

#[test]
fn production_transport_requires_tls() {
    assert!(normalize_relay_url("http://relay.example", false).is_err());
    assert!(normalize_relay_url("ws://127.0.0.1:3000", false).is_err());
    assert!(normalize_relay_url("ws://127.0.0.1:3000", true).is_ok());
    assert_eq!(
        normalize_relay_url("wss://relay.example", false).unwrap().0,
        "https://relay.example"
    );
}

#[test]
fn model_publisher_kind_allowlist_is_exact() {
    assert!(PublishClass::ModelJob.accepts(43001));
    assert!(PublishClass::ModelJob.accepts(43005));
    for forbidden in [43002, 43003, 43004, 43006, 1, 9, 9040] {
        assert!(!PublishClass::ModelJob.accepts(forbidden));
    }
    assert!(PublishClass::Chat.accepts(9));
    assert!(!PublishClass::Chat.accepts(40002));
}

#[test]
fn model_control_actions_exclude_inbound_lifecycle_publication() {
    assert!(model_control_action_allowed(JobControlAction::Cancel));
    assert!(model_control_action_allowed(JobControlAction::Handoff));
    assert!(!model_control_action_allowed(JobControlAction::Cancelled));
    assert!(!model_control_action_allowed(JobControlAction::Release));
}

#[test]
fn normal_chat_is_kind_nine_and_channel_bound() {
    let relay = test_relay();
    let channel = uuid::Uuid::parse_str("3580ca9b-47b4-4af9-b22a-1068778f26c6").unwrap();
    let event = relay
        .build_chat_event(channel, "regression sentinel", None)
        .expect("chat event");

    assert_eq!(u32::from(event.kind.as_u16()), 9);
    assert_eq!(event.content, "regression sentinel");
    let tags: Vec<Vec<String>> = event
        .tags
        .iter()
        .map(|tag| tag.as_slice().to_vec())
        .collect();
    assert!(tags
        .iter()
        .any(|tag| tag == &["h", "3580ca9b-47b4-4af9-b22a-1068778f26c6"]));
    assert!(!tags
        .iter()
        .any(|tag| matches!(tag.first(), Some(name) if name == "e" || name == "p")));
}

#[test]
fn threaded_chat_uses_only_the_fixed_thread_reference() {
    let relay = test_relay();
    let channel = uuid::Uuid::parse_str("3580ca9b-47b4-4af9-b22a-1068778f26c6").unwrap();
    let root_hex = "a".repeat(64);
    let root = nostr::EventId::parse(&root_hex).unwrap();
    let thread = buzz_sdk::ThreadRef {
        root_event_id: root,
        parent_event_id: root,
    };
    let event = relay
        .build_chat_event(channel, "thread sentinel", Some(&thread))
        .expect("threaded chat event");
    let tags: Vec<Vec<String>> = event
        .tags
        .iter()
        .map(|tag| tag.as_slice().to_vec())
        .collect();
    for tag in tags
        .iter()
        .filter(|tag| tag.first().map(String::as_str) == Some("e"))
    {
        assert_eq!(tag.get(1), Some(&root_hex));
    }
    assert!(tags
        .iter()
        .any(|tag| tag.first().map(String::as_str) == Some("e")));
    assert!(!tags
        .iter()
        .any(|tag| tag.first().map(String::as_str) == Some("p")));
}

#[test]
fn chat_thread_destination_can_be_set_replaced_and_cleared() {
    let relay = test_relay();
    let channel = uuid::Uuid::parse_str("3580ca9b-47b4-4af9-b22a-1068778f26c6").unwrap();
    let first = "a".repeat(64);
    let second = "b".repeat(64);

    relay.set_chat_thread_root_id(Some(&first)).unwrap();
    let first_event = relay
        .build_session_chat_event(channel, "first")
        .expect("first destination");
    let first_refs = first_event
        .tags
        .iter()
        .filter(|tag| tag.as_slice().first().map(String::as_str) == Some("e"))
        .collect::<Vec<_>>();
    assert!(!first_refs.is_empty());
    assert!(first_refs
        .iter()
        .all(|tag| tag.as_slice().get(1) == Some(&first)));

    relay.set_chat_thread_root_id(Some(&second)).unwrap();
    let second_event = relay
        .build_session_chat_event(channel, "second")
        .expect("replacement destination");
    let second_refs = second_event
        .tags
        .iter()
        .filter(|tag| tag.as_slice().first().map(String::as_str) == Some("e"))
        .collect::<Vec<_>>();
    assert!(!second_refs.is_empty());
    assert!(second_refs
        .iter()
        .all(|tag| tag.as_slice().get(1) == Some(&second)));

    assert!(relay
        .set_chat_thread_root_id(Some("not-an-event-id"))
        .is_err());
    let unchanged = relay
        .build_session_chat_event(channel, "unchanged")
        .expect("invalid update preserves destination");
    let unchanged_refs = unchanged
        .tags
        .iter()
        .filter(|tag| tag.as_slice().first().map(String::as_str) == Some("e"))
        .collect::<Vec<_>>();
    assert!(!unchanged_refs.is_empty());
    assert!(unchanged_refs
        .iter()
        .all(|tag| tag.as_slice().get(1) == Some(&second)));

    relay.set_chat_thread_root_id(None).unwrap();
    let timeline = relay
        .build_session_chat_event(channel, "timeline")
        .expect("cleared destination");
    assert!(!timeline
        .tags
        .iter()
        .any(|tag| tag.as_slice().first().map(String::as_str) == Some("e")));
}
