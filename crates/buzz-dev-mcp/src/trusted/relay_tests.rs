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
        session_thread_root_id: None,
        job_operation_id: None,
        job_request_event_id: None,
        session_working_directory: None,
        github_credentials: Default::default(),
    }
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
