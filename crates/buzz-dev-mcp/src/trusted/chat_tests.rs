use super::*;
use crate::trusted::{dispatch, A2aDispatchParams};

use nostr::{EventBuilder, Kind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const CHANNEL: &str = "3580ca9b-47b4-4af9-b22a-1068778f26c6";

fn relay() -> TrustedRelay {
    let keys = nostr::Keys::generate();
    TrustedRelay::new(super::super::TrustedConfig {
        relay_url: "https://relay.example".into(),
        owner_pubkey: keys.public_key().to_hex(),
        owner_github_login: None,
        keys,
        auth_tag: None,
        auth_tag_json: None,
        grants: super::super::GrantSet::default(),
        a2a_channel_id: None,
        session_channel_id: Some(CHANNEL.into()),
        session_thread_root_id: Some("a".repeat(64)),
        job_operation_id: Some("a580ca9b-47b4-4af9-b22a-1068778f26c6".into()),
        job_request_event_id: Some("a".repeat(64)),
        session_working_directory: None,
        github_credentials: Default::default(),
        allow_insecure_loopback: false,
    })
    .unwrap()
}

fn managed_relay(relay_url: String, keys: nostr::Keys) -> (tempfile::TempDir, TrustedRelay) {
    let harness = tempfile::tempdir().unwrap();
    let checkout = harness.path().join("REPOS/nemo");
    std::fs::create_dir_all(&checkout).unwrap();
    let run = |args: &[&str]| {
        assert!(std::process::Command::new("git")
            .arg("-C")
            .arg(&checkout)
            .args(args)
            .status()
            .unwrap()
            .success());
    };
    run(&["init", "--quiet"]);
    run(&["config", "user.name", "Buzz Test"]);
    run(&["config", "user.email", "buzz-test@example.invalid"]);
    std::fs::write(checkout.join("fixture.txt"), "fixture\n").unwrap();
    run(&["add", "fixture.txt"]);
    run(&["commit", "--quiet", "-m", "fixture"]);
    run(&[
        "remote",
        "add",
        "origin",
        "https://github.com/mysteropodes/nemo.git",
    ]);
    let relay = TrustedRelay::new(super::super::TrustedConfig {
        relay_url,
        owner_pubkey: keys.public_key().to_hex(),
        owner_github_login: None,
        keys,
        auth_tag: None,
        auth_tag_json: None,
        grants: super::super::GrantSet::load_with_nemo(harness.path(), None, None, true).unwrap(),
        a2a_channel_id: Some(buzz_core::nemo::HOME_CHANNEL.into()),
        session_channel_id: Some(buzz_core::nemo::HOME_CHANNEL.into()),
        session_thread_root_id: None,
        job_operation_id: Some("a580ca9b-47b4-4af9-b22a-1068778f26c6".into()),
        job_request_event_id: Some("a".repeat(64)),
        session_working_directory: Some(checkout),
        github_credentials: Default::default(),
        allow_insecure_loopback: true,
    })
    .unwrap();
    (harness, relay)
}

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> (String, Vec<u8>) {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 1024];
        let read = stream.read(&mut chunk).await.unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
    let content_length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let mut chunk = [0_u8; 1024];
        let read = stream.read(&mut chunk).await.unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&chunk[..read]);
    }
    (
        headers,
        bytes[header_end..header_end + content_length].to_vec(),
    )
}

async fn write_json_response(stream: &mut tokio::net::TcpStream, body: &[u8]) {
    let headers = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
}

#[tokio::test]
async fn job_session_can_create_a_visible_thread_through_authenticated_transport() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let keys = nostr::Keys::generate();
    let signer = keys.public_key().to_hex();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut context_stream, _) = listener.accept().await.unwrap();
            let (headers, _) = read_http_request(&mut context_stream).await;
            assert!(headers.starts_with("GET /api/context HTTP/1.1"));
            assert!(headers
                .to_ascii_lowercase()
                .contains("authorization: nostr "));
            let context = serde_json::json!({
                "schema_version": buzz_core::COMMUNITY_CONTEXT_SCHEMA_VERSION,
                "community_id": "12345678-1234-4234-8234-123456789abc",
                "host": address.to_string(),
                "pubkey": signer,
            });
            write_json_response(&mut context_stream, context.to_string().as_bytes()).await;
        }

        let (mut publish_stream, _) = listener.accept().await.unwrap();
        let (headers, body) = read_http_request(&mut publish_stream).await;
        assert!(headers.starts_with("POST /events HTTP/1.1"));
        assert!(headers
            .to_ascii_lowercase()
            .contains("authorization: nostr "));
        let event: Event = serde_json::from_slice(&body).unwrap();
        let ack = serde_json::json!({
            "event_id": event.id.to_hex(),
            "accepted": true,
            "message": "stored",
        });
        write_json_response(&mut publish_stream, ack.to_string().as_bytes()).await;
        event
    });
    let (_harness, relay) = managed_relay(format!("http://{address}"), keys);
    let result = create_thread(
        &Arc::new(relay),
        ChatThreadCreateParams {
            content: "Visible before delegated work starts".into(),
            channel_id: None,
            recipient_pubkeys: Vec::new(),
        },
        CancellationToken::new(),
    )
    .await;
    assert_ne!(result.is_error, Some(true));
    let event = server.await.unwrap();
    assert_eq!(event.kind, Kind::Custom(9));
    assert_eq!(
        event_channel(&event)
            .map(|value| value.to_string())
            .as_deref(),
        Some(buzz_core::nemo::HOME_CHANNEL)
    );
    assert!(buzz_core::nip10::parse_thread_markers(&event.tags)
        .resolve()
        .is_none());
}

#[tokio::test]
async fn visible_task_preparation_reuses_a_signed_unaddressed_assignment_root() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let keys = nostr::Keys::generate();
    let signer = keys.public_key().to_hex();
    let operation_id = "a580ca9b-47b4-4af9-b22a-1068778f26c6";
    let root = EventBuilder::new(Kind::Custom(9), "Visible delegated task")
        .tags([
            Tag::parse(["h", buzz_core::nemo::HOME_CHANNEL]).unwrap(),
            task_assignment_tag(operation_id).unwrap(),
            Tag::parse(["i", operation_id]).unwrap(),
        ])
        .sign_with_keys(&keys)
        .unwrap();
    let expected_root = root.id.to_hex();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (headers, _) = read_http_request(&mut stream).await;
            assert!(headers.starts_with("GET /api/context HTTP/1.1"));
            let context = serde_json::json!({
                "schema_version": buzz_core::COMMUNITY_CONTEXT_SCHEMA_VERSION,
                "community_id": "12345678-1234-4234-8234-123456789abc",
                "host": address.to_string(),
                "pubkey": signer,
            });
            write_json_response(&mut stream, context.to_string().as_bytes()).await;
        }
        let (mut stream, _) = listener.accept().await.unwrap();
        let (headers, _) = read_http_request(&mut stream).await;
        assert!(headers.starts_with("POST /query HTTP/1.1"));
        write_json_response(
            &mut stream,
            serde_json::to_string(&vec![root]).unwrap().as_bytes(),
        )
        .await;
    });
    let (_harness, relay) = managed_relay(format!("http://{address}"), keys);
    let prepared = relay
        .prepare_visible_task_thread(
            None,
            None,
            operation_id,
            "Visible delegated task",
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    server.await.unwrap();
    assert_eq!(prepared.channel_id, buzz_core::nemo::HOME_CHANNEL);
    assert_eq!(prepared.thread_root_id, expected_root);
}

#[tokio::test]
async fn rejected_task_root_prevents_the_machine_job_publish() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let keys = nostr::Keys::generate();
    let signer = keys.public_key().to_hex();
    let server = tokio::spawn(async move {
        let mut published = None;
        while published.is_none() {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (headers, body) = read_http_request(&mut stream).await;
            if headers.starts_with("GET /api/context HTTP/1.1") {
                let context = serde_json::json!({
                    "schema_version": buzz_core::COMMUNITY_CONTEXT_SCHEMA_VERSION,
                    "community_id": "12345678-1234-4234-8234-123456789abc",
                    "host": address.to_string(),
                    "pubkey": signer,
                });
                write_json_response(&mut stream, context.to_string().as_bytes()).await;
            } else if headers.starts_with("POST /query HTTP/1.1") {
                write_json_response(&mut stream, b"[]").await;
            } else {
                assert!(headers.starts_with("POST /events HTTP/1.1"));
                let event: Event = serde_json::from_slice(&body).unwrap();
                let ack = serde_json::json!({
                    "event_id": event.id.to_hex(),
                    "accepted": false,
                    "message": "rejected",
                });
                write_json_response(&mut stream, ack.to_string().as_bytes()).await;
                published = Some(event);
            }
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err()
        );
        published.unwrap()
    });
    let (_harness, mut relay) = managed_relay(format!("http://{address}"), keys);
    relay.job_operation_id = None;
    relay.job_request_event_id = None;
    let result = dispatch(
        &Arc::new(relay),
        A2aDispatchParams {
            operation_id: "a580ca9b-47b4-4af9-b22a-1068778f26c6".into(),
            idempotency_key: "visible-before-execution".into(),
            coordinator_epoch: 1,
            recipient_pubkey: "b".repeat(64),
            capability: "consultation".into(),
            title: Some("Visible delegated task".into()),
            origin: None,
            conversation: None,
            summary: "Do not execute before the task root exists".into(),
            acceptance: vec!["No job is published after task-root rejection".into()],
            worktree_id: Some("visible-task".into()),
            paths: Vec::new(),
            contracts: Vec::new(),
            github_issue: None,
            github_pr: None,
            github_run: None,
            supersedes_event_id: None,
            ttl_seconds: 60,
        },
        CancellationToken::new(),
    )
    .await;
    let encoded = serde_json::to_string(&result).unwrap();
    assert!(encoded.contains("did not acknowledge"), "{encoded}");
    assert!(!encoded.contains("request_event_id"));
    let only_publish = server.await.unwrap();
    assert_eq!(only_publish.kind, Kind::Custom(9));
    assert!(recipients(&only_publish).unwrap().is_empty());
}

fn tag_values(event: &Event, name: &str) -> Vec<Vec<String>> {
    event
        .tags
        .iter()
        .map(nostr::Tag::as_slice)
        .filter(|values| values.first().map(String::as_str) == Some(name))
        .map(<[String]>::to_vec)
        .collect()
}

#[test]
fn peer_question_and_reply_use_visible_kind_nine_thread_correlation() {
    let relay = relay();
    let recipient = nostr::Keys::generate().public_key().to_hex();
    let root = "b".repeat(64);
    let request_id = "a580ca9b-47b4-4af9-b22a-1068778f26c6";
    let question = relay
        .build_scoped_chat_event(
            uuid::Uuid::parse_str(CHANNEL).unwrap(),
            "Can you verify the seam?",
            Some(&direct_thread_ref(&root).unwrap()),
            &[&recipient],
            &[peer_tag(QUESTION_TAG, request_id).unwrap()],
        )
        .unwrap();

    assert_eq!(question.kind, Kind::Custom(9));
    assert_eq!(
        peer_marker(&question, QUESTION_TAG).as_deref(),
        Some(request_id)
    );
    assert_eq!(
        single_recipient(&question).as_deref(),
        Some(recipient.as_str())
    );
    assert_eq!(
        buzz_core::nip10::parse_thread_markers(&question.tags).resolve(),
        Some((root.clone(), root.clone()))
    );
    let parsed = parse_peer_question(&question).unwrap();
    assert_eq!(parsed.channel, uuid::Uuid::parse_str(CHANNEL).unwrap());
    assert_eq!(parsed.thread_root_id, root);

    let reply = relay
        .build_scoped_chat_event(
            uuid::Uuid::parse_str(CHANNEL).unwrap(),
            "The seam is sound.",
            Some(&thread_ref(&root, &question.id.to_hex()).unwrap()),
            &[&question.pubkey.to_hex()],
            &[peer_tag(REPLY_TAG, request_id).unwrap()],
        )
        .unwrap();
    assert_eq!(peer_marker(&reply, REPLY_TAG).as_deref(), Some(request_id));
    assert_eq!(
        buzz_core::nip10::parse_thread_markers(&reply.tags).resolve(),
        Some((root, question.id.to_hex()))
    );
}

#[test]
fn chat_schemas_keep_authority_bounded_and_routing_explicit() {
    let send = serde_json::from_value::<ChatSendParams>(serde_json::json!({
        "content": "status",
        "channel_id": CHANNEL,
        "thread_root_id": "a".repeat(64),
        "recipient_pubkeys": ["b".repeat(64)],
    }));
    assert!(send.is_ok());
    let create = serde_json::from_value::<ChatThreadCreateParams>(serde_json::json!({
        "content": "Oversight",
        "recipient_pubkeys": [],
    }));
    assert!(create.is_ok());
    let ask = serde_json::from_value::<PeerAskParams>(serde_json::json!({
        "recipient_pubkey": "b".repeat(64),
        "question": "Ready?",
    }));
    assert!(ask.is_ok());

    for forbidden in ["relay", "private_key", "auth_tag", "event_kind"] {
        let mut value = serde_json::json!({"content": "status"});
        value[forbidden] = serde_json::json!("caller-controlled");
        assert!(serde_json::from_value::<ChatSendParams>(value).is_err());
    }
}

#[test]
fn malformed_or_ambiguous_channel_and_peer_tags_are_rejected() {
    let keys = nostr::Keys::generate();
    let event = EventBuilder::new(Kind::Custom(9), "bad")
        .tags([
            Tag::parse(["h", CHANNEL]).unwrap(),
            Tag::parse(["h", "not-a-channel"]).unwrap(),
            peer_tag(QUESTION_TAG, "a580ca9b-47b4-4af9-b22a-1068778f26c6").unwrap(),
            peer_tag(QUESTION_TAG, "b580ca9b-47b4-4af9-b22a-1068778f26c6").unwrap(),
        ])
        .sign_with_keys(&keys)
        .unwrap();
    assert_eq!(event_channel(&event), None);
    assert_eq!(peer_marker(&event, QUESTION_TAG), None);
}

#[test]
fn history_reply_validation_requires_the_exact_canonical_root() {
    let keys = nostr::Keys::generate();
    let root = "a".repeat(64);
    let other = "b".repeat(64);
    let channel = uuid::Uuid::parse_str(CHANNEL).unwrap();
    let good = EventBuilder::new(Kind::Custom(9), "good")
        .tags([
            Tag::parse(["h", CHANNEL]).unwrap(),
            Tag::parse(["e", &root, "", "reply"]).unwrap(),
        ])
        .sign_with_keys(&keys)
        .unwrap();
    let wrong = EventBuilder::new(Kind::Custom(9), "wrong")
        .tags([
            Tag::parse(["h", CHANNEL]).unwrap(),
            Tag::parse(["e", &other, "", "reply"]).unwrap(),
        ])
        .sign_with_keys(&keys)
        .unwrap();
    assert!(valid_thread_reply(&good, channel, &root));
    assert!(!valid_thread_reply(&wrong, channel, &root));
}

#[test]
fn peer_tag_helper_emits_only_the_expected_three_fields() {
    let relay = relay();
    let request_id = "a580ca9b-47b4-4af9-b22a-1068778f26c6";
    let event = relay
        .build_scoped_chat_event(
            uuid::Uuid::parse_str(CHANNEL).unwrap(),
            "question",
            None,
            &[],
            &[peer_tag(QUESTION_TAG, request_id).unwrap()],
        )
        .unwrap();
    assert_eq!(
        tag_values(&event, PEER_TAG),
        vec![vec![
            PEER_TAG.to_owned(),
            QUESTION_TAG.to_owned(),
            request_id.to_owned()
        ]]
    );
}

#[test]
fn task_assignment_root_is_top_level_and_does_not_address_a_peer() {
    let relay = relay();
    let operation_id = "a580ca9b-47b4-4af9-b22a-1068778f26c6";
    let channel = uuid::Uuid::parse_str(CHANNEL).unwrap();
    let event = relay
        .build_scoped_chat_event(
            channel,
            "Visible delegated task",
            None,
            &[],
            &task_assignment_tags(operation_id).unwrap(),
        )
        .unwrap();
    assert!(valid_task_assignment_root(
        &event,
        &relay,
        channel,
        operation_id
    ));
    assert!(recipients(&event).unwrap().is_empty());
    assert!(buzz_core::nip10::parse_thread_markers(&event.tags)
        .resolve()
        .is_none());
}

#[test]
fn mutable_chat_reply_placement_does_not_change_provider_origin() {
    let relay = relay();
    let provider_root = relay.provider_thread_root_id.clone();
    relay
        .set_chat_thread_root_id(Some(&"b".repeat(64)))
        .unwrap();
    assert_eq!(relay.provider_thread_root_id, provider_root);
    assert_eq!(provider_root, Some("a".repeat(64)));
}
