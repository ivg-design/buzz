use super::*;
use buzz_core::job::{
    build_job_tags, JobCommon, JobControl, JobControlAction, JobErrorOutcome, JobFollowup,
    JobProject, JobRepository, JobSponsor, JOB_SCHEMA_VERSION,
};
use nostr::{EventBuilder, Kind};
use std::sync::OnceLock;

struct TestCheckout {
    root: PathBuf,
    head: String,
}

fn test_checkout() -> &'static TestCheckout {
    static CHECKOUT: OnceLock<TestCheckout> = OnceLock::new();
    CHECKOUT.get_or_init(|| {
        let root = tempfile::Builder::new()
            .prefix("buzz-acp-job-checkout-")
            .tempdir()
            .expect("temporary checkout")
            .keep();
        let run = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .arg("-C")
                .arg(&root)
                .args(args)
                .env("GIT_TERMINAL_PROMPT", "0")
                .output()
                .expect("run git fixture command");
            assert!(
                output.status.success(),
                "git fixture command failed: {args:?}"
            );
            String::from_utf8(output.stdout)
                .expect("git output")
                .trim()
                .to_owned()
        };
        run(&["init"]);
        run(&["config", "user.name", "Buzz Test"]);
        run(&["config", "user.email", "buzz-test@example.invalid"]);
        run(&["checkout", "-b", "codex/a2a"]);
        std::fs::create_dir_all(root.join("src")).expect("fixture src");
        std::fs::write(root.join("src/fixture.txt"), "fixture\n").expect("fixture file");
        run(&["add", "src/fixture.txt"]);
        run(&["commit", "-m", "test fixture"]);
        run(&[
            "remote",
            "add",
            "origin",
            "https://github.com/mysteropodes/nemo.git",
        ]);
        let head = run(&["rev-parse", "HEAD"]);
        TestCheckout { root, head }
    })
}

pub(super) fn context(keys: &Keys) -> AuthenticatedContext {
    AuthenticatedContext {
        schema_version: "buzz.context.v1".into(),
        community_id: Uuid::new_v4().to_string(),
        host: "example.test".into(),
        pubkey: keys.public_key().to_hex(),
    }
}

pub(super) fn sponsor(keys: &Keys) -> JobSponsor {
    JobSponsor {
        pubkey: keys.public_key().to_hex(),
        github_login: "worker-owner".into(),
    }
}

pub(super) fn fixture(
    sender: &Keys,
    recipient: &Keys,
    channel: Uuid,
    repository: &str,
    idempotency_key: &str,
    summary: &str,
) -> (JobRequest, Event) {
    let checkout = test_checkout();
    let request = JobRequest {
        common: JobCommon {
            schema_version: JOB_SCHEMA_VERSION.into(),
            operation_id: Uuid::new_v4().to_string(),
            idempotency_key: idempotency_key.into(),
            coordinator_epoch: 1,
            project: JobProject {
                address: format!("30621:{}:nemo", sender.public_key().to_hex()),
                home_channel: channel.to_string(),
            },
            repository: JobRepository {
                canonical: repository.into(),
                github_issue: None,
                github_pr: None,
                github_run: None,
                base_sha: checkout.head.clone(),
                branch: "codex/a2a".into(),
                worktree_id: "a2a".into(),
                paths: vec!["src".into()],
                contracts: vec![],
            },
            sender_pubkey: sender.public_key().to_hex(),
            recipient_pubkey: recipient.public_key().to_hex(),
            sponsor: JobSponsor {
                pubkey: sender.public_key().to_hex(),
                github_login: "owner".into(),
            },
            expires_at: (Utc::now() + chrono::Duration::hours(1))
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        },
        capability: "rust".into(),
        summary: summary.into(),
        acceptance: vec!["Tests pass".into()],
        supersedes_event_id: None,
    };
    let body = JobEvent::Request(request.clone());
    let event = EventBuilder::new(
        Kind::Custom(buzz_core::kind::KIND_JOB_REQUEST as u16),
        body.canonical_json().expect("json"),
    )
    .tags(build_job_tags(&body).expect("tags"))
    .sign_with_keys(sender)
    .expect("sign");
    (request, event)
}

pub(super) fn project(request: &JobRequest) -> PromptProjectInfo {
    PromptProjectInfo {
        name: "Nemo".into(),
        slug: "nemo".into(),
        owner: request.common.sender_pubkey.clone(),
        coordinate: request.common.project.address.clone(),
        default_repo_owner: Some(request.common.sender_pubkey.clone()),
        default_repo_id: Some("nemo".into()),
    }
}

pub(super) fn grants(request: &JobRequest) -> GrantSet {
    let checkout = test_checkout();
    GrantSet::from_json(&format!(
            r#"{{"version":1,"grants":[{{"project_address":"{}","home_channel":"{}","repository":"{}","requester_pubkeys":["{}"],"capabilities":["rust"],"path_prefixes":["src"],"base_sha":"{}","branch":"{}","worktree_id":"{}","checkout_root":{}}}]}}"#,
            request.common.project.address,
            request.common.project.home_channel,
            request.common.repository.canonical,
            request.common.sender_pubkey,
            request.common.repository.base_sha,
            request.common.repository.branch,
            request.common.repository.worktree_id,
            serde_json::to_string(&checkout.root).expect("checkout path json"),
        ))
        .expect("grant")
}

pub(super) fn root() -> PathBuf {
    std::env::temp_dir().join(format!("buzz-acp-receiver-{}", Uuid::new_v4()))
}

#[tokio::test]
async fn wrong_recipient_and_unauthorized_repository_never_prompt() {
    let sender = Keys::generate();
    let worker = Keys::generate();
    let other = Keys::generate();
    let channel = Uuid::new_v4();
    let (request, wrong_recipient) = fixture(
        &sender,
        &other,
        channel,
        "https://github.com/mysteropodes/nemo",
        "wrong-recipient",
        "work",
    );
    let (rest, _published, server) = RestClient::accepting_test_pair(worker.clone()).await;
    let ledger_root = root();
    let tenant = context(&worker);
    let worker_sponsor = sponsor(&worker);
    let receiver = JobReceiver::for_test(
        tenant,
        worker,
        rest,
        worker_sponsor,
        grants(&request),
        ledger_root.clone(),
    );
    assert!(matches!(
        receiver
            .handle_request(channel, wrong_recipient, Some(&project(&request)))
            .await
            .expect("handled"),
        HandleOutcome::Consumed
    ));
    drop(receiver);

    let recipient = Keys::generate();
    let (request, event) = fixture(
        &sender,
        &recipient,
        channel,
        "https://github.com/mysteropodes/nemo",
        "wrong-repo",
        "work",
    );
    let (rest, _published, second_server) =
        RestClient::accepting_test_pair(recipient.clone()).await;
    let empty = GrantSet::default();
    let tenant = context(&recipient);
    let recipient_sponsor = sponsor(&recipient);
    let receiver = JobReceiver::for_test(
        tenant,
        recipient,
        rest,
        recipient_sponsor,
        empty,
        ledger_root.clone(),
    );
    assert!(matches!(
        receiver
            .handle_request(channel, event, Some(&project(&request)))
            .await
            .expect("handled"),
        HandleOutcome::Consumed
    ));
    server.abort();
    second_server.abort();
    std::fs::remove_dir_all(ledger_root).ok();
}

#[tokio::test]
async fn duplicate_delivery_replays_frozen_receipts_without_duplicate_prompt() {
    let sender = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let (request, event) = fixture(
        &sender,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "same-key",
        "work",
    );
    let project = project(&request);
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
    let ledger_root = root();
    let tenant = context(&worker);
    let worker_sponsor = sponsor(&worker);
    let receiver = JobReceiver::for_test(
        tenant,
        worker,
        rest,
        worker_sponsor,
        grants(&request),
        ledger_root.clone(),
    );
    let first = receiver
        .handle_request(channel, event.clone(), Some(&project))
        .await
        .expect("first");
    let HandleOutcome::Dispatch(dispatch) = first else {
        panic!("first delivery must dispatch")
    };
    assert!(receiver
        .mark_prompt_started(&dispatch.claim)
        .await
        .expect("mark started"));
    let first_receipts = [
        published.recv().await.expect("processed"),
        published.recv().await.expect("accepted"),
    ];
    let replay = receiver
        .handle_request(channel, event, Some(&project))
        .await
        .expect("replay");
    assert!(matches!(replay, HandleOutcome::Consumed));
    let replay_receipts = [
        published.recv().await.expect("processed replay"),
        published.recv().await.expect("accepted replay"),
    ];
    assert_eq!(first_receipts[0].id, replay_receipts[0].id);
    assert_eq!(first_receipts[1].id, replay_receipts[1].id);
    receiver
        .retry_outboxes()
        .await
        .expect("acknowledged receipts need no background retry");
    assert!(published.try_recv().is_err());
    server.abort();
    std::fs::remove_dir_all(ledger_root).ok();
}

#[test]
fn job_scope_isolated_by_channel_operation_and_request() {
    let channel = Uuid::new_v4();
    let left = SessionScope::Job {
        channel_id: channel,
        operation_id: Uuid::new_v4().to_string(),
        request_event_id: "a".repeat(64),
    };
    let right = SessionScope::Job {
        channel_id: channel,
        operation_id: Uuid::new_v4().to_string(),
        request_event_id: "b".repeat(64),
    };
    assert_ne!(left, right);
}

#[tokio::test]
async fn requester_cancel_fences_claim_and_worker_acknowledges_once() {
    let sender = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let worker = Keys::generate();
    let (request, request_event) = fixture(
        &sender,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "cancel-me-2",
        "work",
    );
    let (rest, mut published, second_server) =
        RestClient::accepting_test_pair(worker.clone()).await;
    let receiver = JobReceiver::for_test(
        context(&worker),
        worker.clone(),
        rest,
        sponsor(&worker),
        grants(&request),
        ledger_root.clone(),
    );
    let HandleOutcome::Dispatch(dispatch) = receiver
        .handle_request(channel, request_event.clone(), Some(&project(&request)))
        .await
        .expect("claim request")
    else {
        panic!("request should dispatch")
    };
    let _ = published.recv().await.expect("processed");
    let accepted = published.recv().await.expect("accepted");

    let control = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common: request.common.clone(),
            request_event_id: request_event.id.to_hex(),
            prior_event_id: Some(accepted.id.to_hex()),
        },
        action: JobControlAction::Cancel,
        reason: "no longer needed".into(),
        handoff_to: None,
    });
    let cancel_event = EventBuilder::new(
        Kind::Custom(buzz_core::kind::KIND_JOB_CANCEL as u16),
        control.canonical_json().expect("control JSON"),
    )
    .tags(build_job_tags(&control).expect("control tags"))
    .sign_with_keys(&sender)
    .expect("sign cancel");
    let CancelOutcome::Cancel(cancel) = receiver
        .handle_cancel(channel, cancel_event.clone())
        .await
        .expect("observe cancel")
    else {
        panic!("claimed request should be cancellable")
    };
    assert_eq!(cancel.scope, dispatch.scope);
    cancel
        .emitter
        .control(
            JobControlAction::Cancelled,
            "requester_cancelled".into(),
            None,
        )
        .await
        .expect("acknowledge cancel");
    let cancelled = published.recv().await.expect("cancelled event");
    assert!(matches!(
        JobEvent::parse(&cancelled).expect("valid cancelled event"),
        JobEvent::Control(control) if control.action == JobControlAction::Cancelled
    ));
    assert!(matches!(
        receiver
            .handle_cancel(channel, cancel_event)
            .await
            .expect("terminal replay"),
        CancelOutcome::Consumed
    ));
    second_server.abort();
    std::fs::remove_dir_all(ledger_root).ok();
}

#[tokio::test]
async fn restart_after_cancel_fence_emits_cancelled_instead_of_indeterminate() {
    let sender = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let (request, request_event) = fixture(
        &sender,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "cancel-crash",
        "work",
    );
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
    let tenant = context(&worker);
    let receiver = JobReceiver::for_test(
        tenant.clone(),
        worker.clone(),
        rest.clone(),
        sponsor(&worker),
        grants(&request),
        ledger_root.clone(),
    );
    let HandleOutcome::Dispatch(dispatch) = receiver
        .handle_request(channel, request_event.clone(), Some(&project(&request)))
        .await
        .expect("claim request")
    else {
        panic!("request should dispatch")
    };
    receiver
        .mark_prompt_started(&dispatch.claim)
        .await
        .expect("mark prompt started");
    let _ = published.recv().await.expect("processed");
    let accepted = published.recv().await.expect("accepted");
    let control = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common: request.common.clone(),
            request_event_id: request_event.id.to_hex(),
            prior_event_id: Some(accepted.id.to_hex()),
        },
        action: JobControlAction::Cancel,
        reason: "stop".into(),
        handoff_to: None,
    });
    let cancel_event = EventBuilder::new(
        Kind::Custom(buzz_core::kind::KIND_JOB_CANCEL as u16),
        control.canonical_json().expect("control JSON"),
    )
    .tags(build_job_tags(&control).expect("control tags"))
    .sign_with_keys(&sender)
    .expect("sign cancel");
    let CancelOutcome::Cancel(cancel) = receiver
        .handle_cancel(channel, cancel_event)
        .await
        .expect("fence cancel")
    else {
        panic!("claimed request should cancel")
    };
    drop(cancel);
    drop(receiver);

    let reopened = JobReceiver::for_test(
        tenant,
        worker.clone(),
        rest,
        sponsor(&worker),
        grants(&request),
        ledger_root.clone(),
    );
    reopened
        .recover_lifecycle()
        .await
        .expect("recover pending cancellation");
    let terminal = published.recv().await.expect("cancelled terminal");
    assert!(matches!(
        JobEvent::parse(&terminal).expect("valid terminal"),
        JobEvent::Control(control) if control.action == JobControlAction::Cancelled
    ));
    assert!(
        published.try_recv().is_err(),
        "must emit exactly one terminal"
    );
    server.abort();
    std::fs::remove_dir_all(ledger_root).ok();
}

mod recovery;
