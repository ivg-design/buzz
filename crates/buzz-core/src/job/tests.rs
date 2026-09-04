use nostr::{Event, EventBuilder, Keys, Kind, Tag};
use serde::Deserialize;
use serde_json::Value;

use super::validation::{
    github_repository_tag, validate_inert_references, validate_no_secret_material,
    validate_portable_references,
};
use super::*;
use crate::kind::{KIND_JOB_ACCEPTED, KIND_JOB_ERROR, KIND_JOB_PROGRESS, KIND_JOB_REQUEST};

fn common(sender: &Keys, recipient: &Keys) -> JobCommon {
    JobCommon {
        schema_version: JOB_SCHEMA_VERSION.into(),
        operation_id: "31dbb246-bc79-4ddc-aab0-2773f05b5cb2".into(),
        idempotency_key: "idem-1".into(),
        coordinator_epoch: 1,
        project: JobProject {
            address: format!("30621:{}:nemo", sender.public_key().to_hex()),
            home_channel: "3580ca9b-47b4-4af9-b22a-1068778f26c6".into(),
        },
        repository: JobRepository {
            canonical: "https://github.com/example/repo".into(),
            github_issue: Some("1".into()),
            github_pr: None,
            github_run: None,
            base_sha: "a".repeat(40),
            branch: "codex/a2a".into(),
            worktree_id: "buzz-a2a-core".into(),
            paths: vec!["crates/buzz-core".into()],
            contracts: vec!["contract:cargo-test-buzz-core".into()],
        },
        sender_pubkey: sender.public_key().to_hex(),
        recipient_pubkey: recipient.public_key().to_hex(),
        sponsor: JobSponsor {
            pubkey: sender.public_key().to_hex(),
            github_login: "sponsor".into(),
        },
        expires_at: "2030-01-01T00:00:00Z".into(),
    }
}

fn request_event(sender: &Keys, recipient: &Keys) -> Event {
    let job = JobEvent::Request(JobRequest {
        common: common(sender, recipient),
        capability: "rust".into(),
        summary: "Implement the seam".into(),
        acceptance: vec!["Tests pass".into()],
        supersedes_event_id: None,
    });
    EventBuilder::new(
        Kind::Custom(KIND_JOB_REQUEST as u16),
        job.canonical_json().expect("json"),
    )
    .tags(build_job_tags(&job).expect("tags"))
    .sign_with_keys(sender)
    .expect("sign")
}

#[test]
fn request_round_trips_strict_canonical_json() {
    let sender = Keys::generate();
    let recipient = Keys::generate();
    let event = request_event(&sender, &recipient);
    let parsed = JobEvent::parse(&event).expect("parse");
    assert_eq!(parsed.canonical_json().expect("json"), event.content);
}

#[test]
fn body_author_and_duplicate_route_tags_are_rejected() {
    let sender = Keys::generate();
    let recipient = Keys::generate();
    let event = request_event(&sender, &recipient);
    let mut body: Value = serde_json::from_str(&event.content).expect("body");
    body["sender_pubkey"] = Value::String(recipient.public_key().to_hex());
    let mut tags = event.tags.to_vec();
    tags.push(Tag::parse(["p", &recipient.public_key().to_hex()]).expect("tag"));
    let forged = EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), body.to_string())
        .tags(tags)
        .sign_with_keys(&sender)
        .expect("sign");
    assert!(JobEvent::parse(&forged).is_err());
}

#[test]
fn unknown_null_and_unsafe_paths_are_rejected() {
    let sender = Keys::generate();
    let recipient = Keys::generate();
    let event = request_event(&sender, &recipient);
    for mutation in [
        serde_json::json!({"unknown": true}),
        serde_json::json!({"github_pr": null}),
        serde_json::json!({"paths": ["../escape"]}),
    ] {
        let mut body: Value = serde_json::from_str(&event.content).expect("body");
        if mutation.get("unknown").is_some() {
            body["unknown"] = mutation["unknown"].clone();
        } else if mutation.get("github_pr").is_some() {
            body["repository"]["github_pr"] = Value::Null;
        } else {
            body["repository"]["paths"] = mutation["paths"].clone();
        }
        let forged = EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), body.to_string())
            .tags(event.tags.clone().to_vec())
            .sign_with_keys(&sender)
            .expect("sign");
        assert!(JobEvent::parse(&forged).is_err());
    }
}

#[test]
fn followup_requires_exact_root_and_reply_tags() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let request = request_event(&requester, &worker);
    let mut response_common = common(&worker, &requester);
    response_common.operation_id = JobEvent::parse(&request)
        .expect("request")
        .common()
        .operation_id
        .clone();
    let job = JobEvent::Accepted(JobAccepted {
        followup: JobFollowup {
            common: response_common,
            request_event_id: request.id.to_hex(),
            prior_event_id: None,
        },
        claim: JobClaim {
            status: JobClaimStatus::Processed,
            scope_digest: "b".repeat(64),
            reason: None,
        },
    });
    let event = EventBuilder::new(
        Kind::Custom(KIND_JOB_ACCEPTED as u16),
        job.canonical_json().expect("json"),
    )
    .tags(build_job_tags(&job).expect("tags"))
    .sign_with_keys(&worker)
    .expect("sign");
    assert!(JobEvent::parse(&event).is_ok());

    let mut tags = event.tags.to_vec();
    tags.push(Tag::parse(["e", &request.id.to_hex(), "", "root"]).expect("tag"));
    let duplicate = EventBuilder::new(Kind::Custom(KIND_JOB_ACCEPTED as u16), event.content)
        .tags(tags)
        .sign_with_keys(&worker)
        .expect("sign");
    assert!(JobEvent::parse(&duplicate).is_err());
}

#[test]
fn duplicate_json_key_is_rejected_before_typed_decode() {
    let sender = Keys::generate();
    let recipient = Keys::generate();
    let event = request_event(&sender, &recipient);
    let duplicated = event.content.replacen(
        "{",
        "{\"operation_id\":\"31dbb246-bc79-4ddc-aab0-2773f05b5cb2\",",
        1,
    );
    let forged = EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), duplicated)
        .tags(event.tags.clone().to_vec())
        .sign_with_keys(&sender)
        .expect("sign");
    assert!(JobEvent::parse(&forged).is_err());
}

#[test]
fn github_repository_url_and_tags_are_canonical() {
    let sender = Keys::generate();
    let recipient = Keys::generate();
    let event = request_event(&sender, &recipient);
    assert!(event
        .tags
        .iter()
        .any(|tag| { tag.as_slice() == ["github-repository", "example/repo"] }));
    for bad in [
        "http://github.com/example/repo",
        "https://user@github.com/example/repo",
        "https://github.com/Example/repo",
        "https://github.com/example/repo.git",
        "https://github.com/example/repo/",
        "https://github.com/example/repo?q=1",
        "https://github.com/example/repo#frag",
        "https://evil.example/example/repo",
    ] {
        assert!(github_repository_tag(bad).is_err(), "accepted {bad}");
    }
}

#[test]
fn github_ids_are_positive_decimal_and_issue_pr_are_exclusive() {
    let sender = Keys::generate();
    let recipient = Keys::generate();
    let event = request_event(&sender, &recipient);
    for bad in ["0", "01", "-1", "one"] {
        let mut body: Value = serde_json::from_str(&event.content).expect("body");
        body["repository"]["github_issue"] = Value::String(bad.into());
        let forged = EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), body.to_string())
            .tags(event.tags.clone().to_vec())
            .sign_with_keys(&sender)
            .expect("sign");
        assert!(JobEvent::parse(&forged).is_err(), "accepted {bad}");
    }
    let mut body: Value = serde_json::from_str(&event.content).expect("body");
    body["repository"]["github_pr"] = Value::String("2".into());
    let forged = EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), body.to_string())
        .tags(event.tags.clone().to_vec())
        .sign_with_keys(&sender)
        .expect("sign");
    assert!(JobEvent::parse(&forged).is_err());
}

#[test]
fn auth_duplicate_and_missing_route_tags_are_rejected() {
    let sender = Keys::generate();
    let recipient = Keys::generate();
    let event = request_event(&sender, &recipient);
    let mut auth_tags = event.tags.clone().to_vec();
    auth_tags.push(Tag::parse(["auth", "secret"]).expect("tag"));
    let auth = EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), &event.content)
        .tags(auth_tags)
        .sign_with_keys(&sender)
        .expect("sign");
    assert!(JobEvent::parse(&auth).is_err());

    for name in ["h", "p"] {
        let missing: Vec<Tag> = event
            .tags
            .iter()
            .filter(|tag| tag.as_slice().first().map(String::as_str) != Some(name))
            .cloned()
            .collect();
        let missing = EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), &event.content)
            .tags(missing)
            .sign_with_keys(&sender)
            .expect("sign");
        assert!(JobEvent::parse(&missing).is_err());

        let mut duplicate = event.tags.clone().to_vec();
        duplicate.push(
            event
                .tags
                .iter()
                .find(|tag| tag.as_slice().first().map(String::as_str) == Some(name))
                .expect("route tag")
                .clone(),
        );
        let duplicate = EventBuilder::new(Kind::Custom(KIND_JOB_REQUEST as u16), &event.content)
            .tags(duplicate)
            .sign_with_keys(&sender)
            .expect("sign");
        assert!(JobEvent::parse(&duplicate).is_err());
    }
}

#[test]
fn lifecycle_wire_requires_predecessors() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let request = request_event(&requester, &worker);
    let mut response_common = common(&worker, &requester);
    response_common.project = JobEvent::parse(&request)
        .expect("request")
        .common()
        .project
        .clone();
    for job in [
        JobEvent::Progress(JobProgress {
            followup: JobFollowup {
                common: response_common.clone(),
                request_event_id: request.id.to_hex(),
                prior_event_id: None,
            },
            status: JobProgressStatus::Progress,
            message: "working".into(),
            evidence: Vec::new(),
        }),
        JobEvent::Error(JobError {
            followup: JobFollowup {
                common: response_common.clone(),
                request_event_id: request.id.to_hex(),
                prior_event_id: None,
            },
            outcome: JobErrorOutcome::Failed,
            code: "failed".into(),
            message: "failed".into(),
            retryable: false,
        }),
    ] {
        let forged = EventBuilder::new(
            Kind::Custom(match &job {
                JobEvent::Progress(_) => KIND_JOB_PROGRESS as u16,
                _ => KIND_JOB_ERROR as u16,
            }),
            job.canonical_json().expect("json"),
        )
        .tags(build_job_tags(&job).expect("tags"))
        .sign_with_keys(&worker)
        .expect("sign");
        assert!(JobEvent::parse(&forged).is_err());
    }
}

#[test]
fn host_local_and_secret_references_are_rejected() {
    for value in [
        "/Users/alice/worktree",
        "artifact at /home/alice/out",
        "token=not-for-the-wire",
        "file:///tmp/result",
    ] {
        assert!(validate_portable_references("test", &[value.into()]).is_err());
    }
}

#[test]
fn semantic_request_digest_is_cross_language_stable() {
    let requester = Keys::parse("0000000000000000000000000000000000000000000000000000000000000001")
        .expect("requester key");
    let worker = Keys::parse("0000000000000000000000000000000000000000000000000000000000000002")
        .expect("worker key");
    let request = JobRequest {
        common: common(&requester, &worker),
        capability: "rust".into(),
        summary: "Implement the seam".into(),
        acceptance: vec!["Tests pass".into()],
        supersedes_event_id: None,
    };
    assert_eq!(
        semantic_request_digest(&request).expect("digest"),
        "1380396eed95e902c619746bd0bc332406c22015c93a2c956ff93ee3364f076b"
    );
}

#[derive(Deserialize)]
struct FixtureCorpus {
    cases: Vec<FixtureCase>,
}

#[derive(Deserialize)]
struct FixtureCase {
    name: String,
    kind: u16,
    valid: bool,
    #[serde(default)]
    semantic_digest: Option<String>,
    author_pubkey: String,
    content: Value,
    tags: Vec<Vec<String>>,
}

#[test]
fn shared_cross_language_fixture_corpus_matches_rust_validator() {
    let corpus: FixtureCorpus =
        serde_json::from_str(include_str!("../../tests/fixtures/jobs-v1.json"))
            .expect("fixture corpus");
    assert!(
        corpus.cases.len() >= 10,
        "fixture coverage unexpectedly shrank"
    );
    for case in corpus.cases {
        let event: Event = serde_json::from_value(serde_json::json!({
            "id": "00".repeat(32),
            "pubkey": case.author_pubkey,
            "created_at": 1_893_456_000_u64,
            "kind": case.kind,
            "tags": case.tags,
            "content": serde_json::to_string(&case.content).expect("fixture content"),
            "sig": "00".repeat(64),
        }))
        .unwrap_or_else(|error| panic!("{} event shape: {error}", case.name));
        let result = JobEvent::parse(&event);
        assert_eq!(
            result.is_ok(),
            case.valid,
            "{} unexpectedly returned {result:?}",
            case.name
        );
        if let (Some(expected), Ok(JobEvent::Request(request))) =
            (case.semantic_digest.as_deref(), result.as_ref())
        {
            assert_eq!(
                semantic_request_digest(request).expect("fixture digest"),
                expected,
                "{} semantic digest drifted",
                case.name
            );
        }
    }
}

#[test]
fn strict_content_parser_rejects_before_signing() {
    let sender = Keys::generate();
    let recipient = Keys::generate();
    let event = request_event(&sender, &recipient);
    let duplicate = event
        .content
        .replacen("{", "{\"schema_version\":\"buzz.jobs.v1\",", 1);
    assert!(JobEvent::parse_content(KIND_JOB_REQUEST, &duplicate).is_err());

    let mut null: Value = serde_json::from_str(&event.content).expect("body");
    null["repository"]["github_pr"] = Value::Null;
    assert!(JobEvent::parse_content(KIND_JOB_REQUEST, &null.to_string()).is_err());

    let mut unknown: Value = serde_json::from_str(&event.content).expect("body");
    unknown["unknown"] = Value::Bool(true);
    assert!(JobEvent::parse_content(KIND_JOB_REQUEST, &unknown.to_string()).is_err());
}

#[test]
fn secret_material_and_non_inert_references_are_rejected() {
    for value in [
        "github_pat_abcdefghijkl",
        "ghp_abcdefghijkl",
        "sk-abcdefghijkl",
        "Bearer example",
        "-----BEGIN OPENSSH PRIVATE KEY-----",
    ] {
        assert!(validate_no_secret_material(&Value::String(value.into())).is_err());
    }
    assert!(validate_no_secret_material(&Value::String("risk-based testing".into())).is_ok());
    for value in [
        "https://evil.example/artifact",
        "https://github.com/example/repo?token=x",
        "https://github.com/example/repo#secret",
        "buzz://arbitrary.example/event",
        "untyped artifact",
    ] {
        assert!(validate_inert_references("test", &[value.into()]).is_err());
    }
    for value in [
        format!("git:{}", "a".repeat(40)),
        "contract:unit/test".into(),
        format!("buzz:event:{}", "b".repeat(64)),
        "https://github.com/example/repo/actions/runs/1".into(),
    ] {
        assert!(
            validate_inert_references("test", std::slice::from_ref(&value)).is_ok(),
            "rejected {value}"
        );
    }
}
