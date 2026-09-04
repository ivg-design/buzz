use super::*;

use buzz_core::job::{build_job_tags, JobControl, JobFollowup};
use nostr::{EventBuilder, Keys, Kind};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::job_receiver::tests::{
    context, fixture, grants, grants_with_git_operations, project, root, sponsor,
};
use crate::job_receiver::{
    CancelOutcome, CancellationTerminal, HandleOutcome, JobDispatch, JobReceiver,
};

struct Admitted {
    receiver: Option<JobReceiver>,
    dispatch: Box<JobDispatch>,
    request: JobRequest,
    request_event: Event,
    requester: Keys,
    published: mpsc::Receiver<Event>,
    server: tokio::task::JoinHandle<()>,
    ledger_root: PathBuf,
    authorization_calls: Option<Arc<AtomicUsize>>,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        self.server.abort();
        drop(self.receiver.take());
        std::fs::remove_dir_all(&self.ledger_root).ok();
    }
}

#[derive(Clone)]
struct TestRelayBehavior {
    marker_delay: Duration,
    marker_release: Option<Arc<tokio::sync::Notify>>,
    fail_final_authorization: bool,
    final_authorization_ttl_seconds: i64,
}

impl Default for TestRelayBehavior {
    fn default() -> Self {
        Self {
            marker_delay: Duration::ZERO,
            marker_release: None,
            fail_final_authorization: false,
            final_authorization_ttl_seconds: 5,
        }
    }
}

async fn controlled_test_pair(
    keys: Keys,
    behavior: TestRelayBehavior,
) -> (
    RestClient,
    mpsc::Receiver<Event>,
    tokio::task::JoinHandle<()>,
    Arc<AtomicUsize>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind controlled privilege relay");
    let base_url = format!("http://{}", listener.local_addr().expect("test address"));
    let (event_tx, event_rx) = mpsc::channel(64);
    let authorization_calls = Arc::new(AtomicUsize::new(0));
    let server_calls = authorization_calls.clone();
    let server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut request = Vec::with_capacity(4096);
            let mut chunk = [0_u8; 4096];
            let header_end = loop {
                let read = socket.read(&mut chunk).await.unwrap_or_default();
                if read == 0 {
                    break None;
                }
                request.extend_from_slice(&chunk[..read]);
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    break Some(end + 4);
                }
            };
            let Some(header_end) = header_end else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let is_job_authorize = headers
                .lines()
                .next()
                .is_some_and(|line| line.contains(" /api/jobs/authorize "));
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            while request.len() < header_end + content_length {
                let read = socket.read(&mut chunk).await.unwrap_or_default();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
            }
            let body = &request[header_end..header_end.saturating_add(content_length)];

            if is_job_authorize {
                let call = server_calls.fetch_add(1, Ordering::SeqCst) + 1;
                if behavior.fail_final_authorization && call > 1 {
                    let response = "HTTP/1.1 403 Forbidden\r\nContent-Length: 6\r\nConnection: close\r\n\r\ndenied";
                    let _ = socket.write_all(response.as_bytes()).await;
                    continue;
                }
                let Ok(authorization) = serde_json::from_slice::<
                    buzz_core::job_authorization::JobAuthorizationRequest,
                >(body) else {
                    continue;
                };
                let ttl = if call > 1 {
                    behavior.final_authorization_ttl_seconds
                } else {
                    5
                };
                let now = Utc::now();
                let response_body = buzz_core::job_authorization::JobAuthorizationResponse {
                    schema_version: buzz_core::job_authorization::JOB_AUTHORIZATION_SCHEMA_VERSION
                        .into(),
                    authorized: true,
                    authorization_id: Uuid::new_v4().to_string(),
                    issued_at: now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    expires_at: (now + chrono::Duration::seconds(ttl))
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    binding: buzz_core::job_authorization::JobAuthorizationBinding::from(
                        &authorization,
                    ),
                    project_head_event_id: "a".repeat(64),
                    repository_coordinate: format!("30617:{}:nemo", authorization.requester_pubkey),
                    repository_announcement_event_id: "b".repeat(64),
                    requester_owner_pubkey: authorization.requester_pubkey.clone(),
                    recipient_owner_pubkey: authorization.recipient_pubkey.clone(),
                };
                let body = serde_json::to_string(&response_body).expect("authorization JSON");
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                continue;
            }

            let Ok(event) = serde_json::from_slice::<Event>(body) else {
                continue;
            };
            let privilege_marker = matches!(
                JobEvent::parse(&event),
                Ok(JobEvent::Progress(progress))
                    if progress.message.starts_with("privileged-operation:")
            );
            let _ = event_tx.send(event.clone()).await;
            if privilege_marker {
                if let Some(release) = behavior.marker_release.as_ref() {
                    release.notified().await;
                } else if !behavior.marker_delay.is_zero() {
                    tokio::time::sleep(behavior.marker_delay).await;
                }
            }
            let body = serde_json::json!({
                "event_id": event.id.to_hex(),
                "accepted": true,
                "message": ""
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    (
        RestClient {
            http: reqwest::Client::new(),
            base_url,
            keys,
            auth_tag_json: None,
        },
        event_rx,
        server,
        authorization_calls,
    )
}

async fn admitted_with(
    git_operations: &[&str],
    expires_after: chrono::Duration,
    relay_behavior: Option<TestRelayBehavior>,
) -> Admitted {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let (mut request, _) = fixture(
        &requester,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        &format!("privilege-{}", Uuid::new_v4()),
        "Perform the bounded Git operation",
    );
    request.common.expires_at =
        (Utc::now() + expires_after).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let request_body = JobEvent::Request(request.clone());
    let request_event = EventBuilder::new(
        Kind::Custom(buzz_core::kind::KIND_JOB_REQUEST as u16),
        request_body.canonical_json().expect("request JSON"),
    )
    .tags(build_job_tags(&request_body).expect("request tags"))
    .sign_with_keys(&requester)
    .expect("sign request");
    let (rest, mut published, server, authorization_calls) = match relay_behavior {
        Some(behavior) => {
            let (rest, published, server, calls) =
                controlled_test_pair(worker.clone(), behavior).await;
            (rest, published, server, Some(calls))
        }
        None => {
            let (rest, published, server) = RestClient::accepting_test_pair(worker.clone()).await;
            (rest, published, server, None)
        }
    };
    let grant_set = if git_operations.is_empty() {
        grants(&request)
    } else {
        grants_with_git_operations(&request, git_operations)
    };
    let receiver = JobReceiver::for_test(
        context(&worker),
        worker.clone(),
        rest,
        sponsor(&worker),
        grant_set,
        ledger_root.clone(),
    );
    let HandleOutcome::Dispatch(dispatch) = receiver
        .handle_request(channel, request_event.clone(), Some(&project(&request)))
        .await
        .expect("admit request")
    else {
        panic!("authorized request must dispatch")
    };
    let processed = published.recv().await.expect("Processed receipt");
    let accepted = published.recv().await.expect("Accepted receipt");
    assert_eq!(processed.id, dispatch.claim.processed.id);
    assert_eq!(accepted.id, dispatch.claim.accepted.id);
    assert!(receiver
        .mark_prompt_started(&dispatch.claim)
        .await
        .expect("durable prompt fence"));
    Admitted {
        receiver: Some(receiver),
        dispatch,
        request,
        request_event,
        requester,
        published,
        server,
        ledger_root,
        authorization_calls,
    }
}

async fn admitted(git_operations: &[&str]) -> Admitted {
    admitted_with(git_operations, chrono::Duration::hours(1), None).await
}

fn duplicate_gate(gate: &JobPrivilege) -> Arc<JobPrivilege> {
    JobPrivilege::new(
        gate.scope.clone(),
        gate.tenant.clone(),
        gate.agent_pubkey.clone(),
        gate.rest.clone(),
        gate.sponsor.clone(),
        gate.grants.clone(),
        gate.ledger.clone(),
        gate.claim.clone(),
        gate.request.clone(),
        gate.emitter.clone(),
        gate.lifecycle.clone(),
        gate.checkout_root.clone(),
        gate.allow_insecure_loopback,
    )
    .expect("duplicate test gate")
}

fn git_receipt(
    gate: &JobPrivilege,
    operation: ProjectGitOperation,
    invocation_id: Uuid,
    disposition: buzz_dev_mcp::PrivilegedGitDisposition,
) -> PrivilegedGitOperationReceipt {
    let binding = gate
        .git_receipt_binding(operation, invocation_id)
        .expect("test Git receipt binding");
    let intended = "2".repeat(40);
    PrivilegedGitOperationReceipt {
        schema_version: "buzz.git-operation-receipt.v1".into(),
        invocation_id,
        operation,
        session_channel_id: binding.session_channel_id,
        operation_id: binding.operation_id,
        request_event_id: binding.request_event_id,
        worker_pubkey: binding.worker_pubkey,
        scope_digest: Some(binding.scope_digest),
        repository: Some(binding.repository),
        branch_ref: Some(binding.branch_ref),
        previous_object: (disposition == buzz_dev_mcp::PrivilegedGitDisposition::Applied)
            .then(|| "1".repeat(40)),
        intended_object: (disposition != buzz_dev_mcp::PrivilegedGitDisposition::NotApplied)
            .then(|| intended.clone()),
        observed_object: (disposition == buzz_dev_mcp::PrivilegedGitDisposition::Applied)
            .then_some(intended),
        disposition,
    }
}

fn signed_cancel(admitted: &Admitted, prior_event_id: String) -> Event {
    let control = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common: admitted.request.common.clone(),
            request_event_id: admitted.request_event.id.to_hex(),
            prior_event_id: Some(prior_event_id),
        },
        action: JobControlAction::Cancel,
        reason: "stop privileged work".into(),
        handoff_to: None,
    });
    EventBuilder::new(
        Kind::Custom(buzz_core::kind::KIND_JOB_CANCEL as u16),
        control.canonical_json().expect("cancel JSON"),
    )
    .tags(build_job_tags(&control).expect("cancel tags"))
    .sign_with_keys(&admitted.requester)
    .expect("sign cancel")
}

fn signed_handoff(admitted: &Admitted, prior_event_id: String) -> Event {
    let worker = admitted.receiver.as_ref().expect("receiver").keys.clone();
    let mut common = admitted.request.common.clone();
    common.sender_pubkey = worker.public_key().to_hex();
    common.recipient_pubkey = admitted.request.common.sender_pubkey.clone();
    common.sponsor = admitted.dispatch.privilege.sponsor.clone();
    let handoff = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common,
            request_event_id: admitted.request_event.id.to_hex(),
            prior_event_id: Some(prior_event_id),
        },
        action: JobControlAction::Handoff,
        reason: "continue with the next worker".into(),
        handoff_to: Some(Keys::generate().public_key().to_hex()),
    });
    EventBuilder::new(
        Kind::Custom(buzz_core::kind::KIND_JOB_CANCEL as u16),
        handoff.canonical_json().expect("Handoff JSON"),
    )
    .tags(build_job_tags(&handoff).expect("Handoff tags"))
    .sign_with_keys(&worker)
    .expect("sign Handoff")
}

fn reopen_privilege(gate: &JobPrivilege) -> Arc<JobPrivilege> {
    JobPrivilege::new(
        gate.scope.clone(),
        gate.tenant.clone(),
        gate.agent_pubkey.clone(),
        gate.rest.clone(),
        gate.sponsor.clone(),
        gate.grants.clone(),
        gate.ledger.clone(),
        gate.claim.clone(),
        gate.request.clone(),
        gate.emitter.clone(),
        gate.lifecycle.clone(),
        gate.checkout_root.clone(),
        gate.allow_insecure_loopback,
    )
    .expect("reopen exact job privilege")
}

#[tokio::test]
async fn begin_requires_fresh_authorize_and_confirmed_signed_marker() {
    let mut admitted = admitted(&["commit"]).await;
    let registry = JobPrivilegeRegistry::default();
    let privilege = admitted.dispatch.privilege.clone();
    let scope = admitted.dispatch.scope.clone();
    registry
        .register(scope.clone(), privilege.clone())
        .expect("register gate");
    registry
        .register(scope.clone(), privilege.clone())
        .expect("same Arc replay");
    assert!(registry
        .register(scope.clone(), duplicate_gate(&privilege))
        .is_err());
    assert!(registry.for_session(&scope, &admitted.ledger_root).is_err());

    let gate = registry
        .for_session(&scope, &admitted.dispatch.checkout_root)
        .expect("registry lookup")
        .expect("job gate");
    let invocation_id = Uuid::new_v4();
    let lease = gate
        .begin(
            ProjectGitOperation::Commit,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("fresh authority and relay marker");
    let marker = admitted.published.recv().await.expect("privilege marker");
    marker.verify().expect("signed marker");
    let JobEvent::Progress(progress) = JobEvent::parse(&marker).expect("valid progress") else {
        panic!("privilege start must be progress")
    };
    assert_eq!(progress.status, JobProgressStatus::Progress);
    assert_eq!(
        progress.message,
        format!("privileged-operation:commit:{invocation_id}")
    );
    assert_eq!(
        progress.followup.prior_event_id.as_deref(),
        Some(admitted.dispatch.claim.accepted.id.to_hex().as_str())
    );
    assert!(!lease.cancellation_token().is_cancelled());
    lease
        .finish(
            PrivilegedOperationOutcome::Completed,
            Some(git_receipt(
                &privilege,
                ProjectGitOperation::Commit,
                invocation_id,
                buzz_dev_mcp::PrivilegedGitDisposition::Applied,
            )),
            None,
        )
        .await
        .expect("release operation");
}

#[tokio::test]
async fn missing_grant_has_no_marker_but_final_authorize_failure_is_fenced() {
    let mut missing_grant = admitted(&[]).await;
    let denied = missing_grant
        .dispatch
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await;
    let error = match denied {
        Err(error) => error,
        Ok(_) => panic!("empty Git operation grant must deny"),
    };
    assert!(error.contains("does not allow trusted Git commit"));
    assert!(missing_grant.published.try_recv().is_err());

    let mut denied_authorize = admitted(&["commit"]).await;
    let mut rest = denied_authorize.dispatch.privilege.rest.clone();
    rest.base_url = "http://relay.example".into();
    let original = &denied_authorize.dispatch.privilege;
    let gate = JobPrivilege::new(
        original.scope.clone(),
        original.tenant.clone(),
        original.agent_pubkey.clone(),
        rest,
        original.sponsor.clone(),
        original.grants.clone(),
        original.ledger.clone(),
        original.claim.clone(),
        original.request.clone(),
        original.emitter.clone(),
        original.lifecycle.clone(),
        original.checkout_root.clone(),
        true,
    )
    .expect("test gate");
    let denied = gate
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await;
    let error = match denied {
        Err(error) => error,
        Ok(_) => panic!("fresh authorization must fail closed"),
    };
    assert!(error.contains("requires HTTPS"));
    let marker = denied_authorize
        .published
        .recv()
        .await
        .expect("final authorization failure keeps its signed marker");
    let JobEvent::Progress(progress) = JobEvent::parse(&marker).expect("valid marker") else {
        panic!("final authorization fence must be progress")
    };
    assert!(progress.message.starts_with("privileged-operation:commit:"));
    assert!(gate.revoked.is_cancelled());
    assert!(gate
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .is_err());
    assert!(denied_authorize.published.try_recv().is_err());
}

#[tokio::test]
async fn confirmed_marker_precedes_final_authorization_failure_and_blocks_retry() {
    let behavior = TestRelayBehavior {
        fail_final_authorization: true,
        ..TestRelayBehavior::default()
    };
    let mut admitted = admitted_with(&["commit"], chrono::Duration::hours(1), Some(behavior)).await;
    let gate = admitted.dispatch.privilege.clone();

    let denied = gate
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await;
    let error = match denied {
        Err(error) => error,
        Ok(_) => panic!("final authorization denial must fail closed"),
    };
    assert!(error.contains("403"));
    let marker = admitted
        .published
        .recv()
        .await
        .expect("confirmed marker precedes final authorization");
    let snapshot = gate
        .lifecycle
        .privilege_snapshot()
        .await
        .expect("confirmed marker snapshot");
    assert_eq!(snapshot.head_event_id, marker.id.to_hex());
    assert!(snapshot.pending_outbox.is_none());
    assert_eq!(
        admitted
            .authorization_calls
            .as_ref()
            .expect("controlled authorization count")
            .load(Ordering::SeqCst),
        2
    );
    assert!(gate.revoked.is_cancelled());

    assert!(gate
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .is_err());
    assert_eq!(
        admitted
            .authorization_calls
            .as_ref()
            .expect("controlled authorization count")
            .load(Ordering::SeqCst),
        2,
        "a fenced final-auth failure must not be retried"
    );
    assert!(admitted.published.try_recv().is_err());
}

#[tokio::test]
async fn slow_marker_cannot_outlive_the_signed_request_expiry() {
    let marker_release = Arc::new(tokio::sync::Notify::new());
    let behavior = TestRelayBehavior {
        marker_release: Some(marker_release.clone()),
        ..TestRelayBehavior::default()
    };
    let mut admitted =
        admitted_with(&["commit"], chrono::Duration::seconds(5), Some(behavior)).await;
    let gate = admitted.dispatch.privilege.clone();
    let begin_gate = gate.clone();
    let begin = tokio::spawn(async move {
        begin_gate
            .begin(
                ProjectGitOperation::Commit,
                Uuid::new_v4(),
                CancellationToken::new(),
            )
            .await
    });

    let marker = admitted
        .published
        .recv()
        .await
        .expect("marker is staged before its delayed acknowledgement");
    assert!(matches!(
        JobEvent::parse(&marker).expect("valid marker"),
        JobEvent::Progress(_)
    ));
    assert_eq!(
        admitted
            .authorization_calls
            .as_ref()
            .expect("controlled authorization count")
            .load(Ordering::SeqCst),
        1,
        "fresh authorization must wait until the marker is acknowledged"
    );

    // Freeze only after the relay has received the marker request. Advancing a
    // paused clock while reqwest is still establishing the loopback request
    // can otherwise advance its unrelated transport timers as well.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(6)).await;
    tokio::time::resume();
    marker_release.notify_one();
    let denied = begin.await.expect("begin task");
    let error = match denied {
        Err(error) => error,
        Ok(_) => panic!("an expired request must not produce a lease"),
    };
    assert!(
        error.contains("signed job request expired"),
        "unexpected denial: {error}"
    );
    assert_eq!(
        admitted
            .authorization_calls
            .as_ref()
            .expect("controlled authorization count")
            .load(Ordering::SeqCst),
        1,
        "an expired signed request must not reach final authorization"
    );
    assert!(gate.revoked.is_cancelled());
}

#[tokio::test]
async fn expired_durable_claim_is_rejected_before_a_privilege_marker() {
    let mut admitted = admitted(&["commit"]).await;
    let gate = admitted.dispatch.privilege.clone();
    let durable = gate
        .ledger
        .reload_claim(&gate.claim)
        .await
        .expect("reload durable signed claim");
    let expires_at = DateTime::parse_from_rfc3339(&gate.request.common.expires_at)
        .expect("canonical request expiry")
        .with_timezone(&Utc);

    let error = validate_stored_claim(&gate, &durable, expires_at)
        .expect_err("the durable signed request is expired at its exact deadline");
    assert!(error.contains("signed job request expired"));
    assert!(admitted.published.try_recv().is_err());
}

#[tokio::test]
async fn near_request_expiry_cancels_an_active_lease_before_server_expiry() {
    let mut admitted = admitted_with(
        &["commit"],
        chrono::Duration::seconds(4),
        Some(TestRelayBehavior::default()),
    )
    .await;
    let invocation_id = Uuid::new_v4();
    let lease = admitted
        .dispatch
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("near-expiry request is still live at process handoff");
    admitted.published.recv().await.expect("privilege marker");
    let cancellation = lease.cancellation_token();
    assert!(!cancellation.is_cancelled());

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    assert!(cancellation.is_cancelled());
    lease
        .finish(
            PrivilegedOperationOutcome::Cancelled,
            Some(git_receipt(
                &admitted.dispatch.privilege,
                ProjectGitOperation::Commit,
                invocation_id,
                buzz_dev_mcp::PrivilegedGitDisposition::NotApplied,
            )),
            None,
        )
        .await
        .expect("release expired operation");
}

#[tokio::test]
async fn server_authorization_deadline_cancels_an_active_lease() {
    let behavior = TestRelayBehavior {
        final_authorization_ttl_seconds: 2,
        ..TestRelayBehavior::default()
    };
    let mut admitted = admitted_with(&["commit"], chrono::Duration::hours(1), Some(behavior)).await;
    let invocation_id = Uuid::new_v4();
    let lease = admitted
        .dispatch
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("fresh short-lived server authorization");
    admitted.published.recv().await.expect("privilege marker");
    let cancellation = lease.cancellation_token();
    assert!(!cancellation.is_cancelled());

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert!(cancellation.is_cancelled());
    lease
        .finish(
            PrivilegedOperationOutcome::Cancelled,
            Some(git_receipt(
                &admitted.dispatch.privilege,
                ProjectGitOperation::Commit,
                invocation_id,
                buzz_dev_mcp::PrivilegedGitDisposition::NotApplied,
            )),
            None,
        )
        .await
        .expect("release expired operation");
}

#[tokio::test]
async fn handoff_uses_the_same_fence_and_requires_a_terminal_event_id() {
    let mut admitted = admitted(&[]).await;
    let invocation_id = Uuid::new_v4();
    let lease = admitted
        .dispatch
        .privilege
        .begin(
            ProjectGitOperation::Handoff,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("handoff privilege marker");
    let marker = admitted.published.recv().await.expect("handoff marker");
    let JobEvent::Progress(progress) = JobEvent::parse(&marker).expect("valid marker") else {
        panic!("handoff fence must be progress")
    };
    assert_eq!(
        progress.message,
        format!("privileged-operation:handoff:{invocation_id}")
    );
    assert!(lease
        .finish(PrivilegedOperationOutcome::Completed, None, None)
        .await
        .is_err());
    assert!(!admitted
        .dispatch
        .emitter
        .is_terminal()
        .await
        .expect("lifecycle state"));
}

#[tokio::test]
async fn handoff_is_frozen_before_publish_and_retries_the_exact_event() {
    let mut admitted = admitted(&[]).await;
    let invocation_id = Uuid::new_v4();
    let mut lease = admitted
        .dispatch
        .privilege
        .begin(
            ProjectGitOperation::Handoff,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("handoff privilege marker");
    let marker = admitted.published.recv().await.expect("handoff marker");
    let handoff = signed_handoff(&admitted, marker.id.to_hex());

    lease
        .stage_handoff(handoff.clone(), CancellationToken::new())
        .await
        .expect("durably stage exact Handoff");
    let snapshot = admitted
        .dispatch
        .privilege
        .lifecycle
        .privilege_snapshot()
        .await
        .expect("staged Handoff lifecycle");
    assert_eq!(
        snapshot.pending_outbox.expect("frozen Handoff").id,
        handoff.id
    );
    assert!(admitted.published.try_recv().is_err());

    lease
        .finish(PrivilegedOperationOutcome::Failed, None, None)
        .await
        .expect("uncertain publish leaves the exact Handoff pending");
    admitted
        .receiver
        .as_ref()
        .expect("receiver")
        .retry_outboxes()
        .await
        .expect("retry exact Handoff");
    assert_eq!(
        admitted.published.recv().await.expect("retried Handoff").id,
        handoff.id
    );
    assert!(admitted
        .dispatch
        .emitter
        .is_terminal()
        .await
        .expect("terminal lifecycle"));
}

#[tokio::test]
async fn cancelled_handoff_is_not_staged_for_publish() {
    let mut admitted = admitted(&[]).await;
    let mut lease = admitted
        .dispatch
        .privilege
        .begin(
            ProjectGitOperation::Handoff,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .expect("handoff privilege marker");
    let marker = admitted.published.recv().await.expect("handoff marker");
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = lease
        .stage_handoff(signed_handoff(&admitted, marker.id.to_hex()), cancellation)
        .await
        .expect_err("cancelled Handoff must not cross its durable publish fence");
    assert!(error.contains("cancelled before durable staging"));
    let snapshot = admitted
        .dispatch
        .privilege
        .lifecycle
        .privilege_snapshot()
        .await
        .expect("unstaged lifecycle");
    assert_eq!(snapshot.head_event_id, marker.id.to_hex());
    assert!(snapshot.pending_outbox.is_none());
    lease
        .finish(PrivilegedOperationOutcome::Cancelled, None, None)
        .await
        .expect("finish cancelled Handoff");
    assert!(admitted.published.try_recv().is_err());
}

#[tokio::test]
async fn missing_git_journal_blocks_handoff_before_marker() {
    let mut admitted = admitted(&[]).await;
    let gate = &admitted.dispatch.privilege;
    let lifecycle_lock = gate.lifecycle.privilege_lock_path();
    let journal_path = lifecycle_lock.parent().expect("lock parent").join(format!(
        "{}.git-receipts.json",
        lifecycle_lock
            .file_name()
            .expect("lock file")
            .to_string_lossy()
    ));
    std::fs::remove_file(journal_path).expect("remove Git journal sentinel");

    let error = gate
        .begin(
            ProjectGitOperation::Handoff,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("missing Git state must block Handoff");
    assert!(error.contains("reading durable Git receipt state"));
    assert!(admitted.published.try_recv().is_err());
}

#[tokio::test]
async fn dropped_git_lease_revokes_gate_and_ambiguous_journal_blocks_reopened_handoff() {
    let mut admitted = admitted(&["commit"]).await;
    let lease = admitted
        .dispatch
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .expect("begin Git operation");
    let _marker = admitted.published.recv().await.expect("privilege marker");
    let cancellation = lease.cancellation_token();
    drop(lease);
    assert!(cancellation.is_cancelled());
    assert!(admitted.dispatch.privilege.revoked.is_cancelled());
    assert_eq!(
        admitted
            .dispatch
            .privilege
            .git_receipts
            .summary()
            .expect("ambiguous receipt summary")
            .effect,
        GitEffect::Ambiguous
    );

    let reopened = reopen_privilege(&admitted.dispatch.privilege);
    let error = reopened
        .begin(
            ProjectGitOperation::Handoff,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("ambiguous Git state must block a reopened Handoff");
    assert!(error.contains("unresolved Git invocation"));
    assert!(admitted.published.try_recv().is_err());
}

#[tokio::test]
async fn durable_claim_tamper_and_inactive_lifecycle_publish_no_marker() {
    let mut tampered = admitted(&["commit"]).await;
    let claim_path = std::fs::read_dir(&tampered.ledger_root)
        .expect("ledger entries")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.len() == 69
                        && name.ends_with(".json")
                        && name[..64]
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
        })
        .expect("claim record");
    let mut raw: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&claim_path).expect("read claim"))
            .expect("claim JSON");
    raw["digest"] = serde_json::Value::String("0".repeat(64));
    std::fs::write(
        &claim_path,
        serde_json::to_vec(&raw).expect("serialize tamper"),
    )
    .expect("tamper claim");
    let denied = tampered
        .dispatch
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await;
    let error = match denied {
        Err(error) => error,
        Ok(_) => panic!("changed durable claim must deny"),
    };
    assert!(error.contains("durable claim changed"));
    assert!(tampered.published.try_recv().is_err());

    let mut pending = admitted(&["commit"]).await;
    let pending_event = EventBuilder::new(Kind::TextNote, "frozen sibling")
        .sign_with_keys(&Keys::generate())
        .expect("sign pending event");
    pending
        .dispatch
        .privilege
        .lifecycle
        .stage(
            pending_event,
            false,
            pending.dispatch.claim.accepted.id.to_hex(),
        )
        .await
        .expect("stage pending event");
    assert!(pending
        .dispatch
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .is_err());
    assert!(pending.published.try_recv().is_err());

    let mut terminal = admitted(&["commit"]).await;
    terminal
        .dispatch
        .privilege
        .lifecycle
        .observe_external_terminal("f".repeat(64), terminal.dispatch.claim.accepted.id.to_hex())
        .await
        .expect("terminalize lifecycle");
    assert!(terminal
        .dispatch
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .is_err());
    assert!(terminal.published.try_recv().is_err());
}

#[tokio::test]
async fn stored_cancel_revokes_active_lease_before_lifecycle_advance() {
    let mut admitted = admitted(&["commit"]).await;
    let registry = JobPrivilegeRegistry::default();
    let scope = admitted.dispatch.scope.clone();
    registry
        .register(scope.clone(), admitted.dispatch.privilege.clone())
        .expect("register gate");
    let gate = registry
        .for_session(&scope, &admitted.dispatch.checkout_root)
        .expect("registry lookup")
        .expect("job gate");
    let invocation_id = Uuid::new_v4();
    let lease = gate
        .begin(
            ProjectGitOperation::Commit,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("begin operation");
    let marker = admitted.published.recv().await.expect("privilege marker");
    let lease_cancellation = lease.cancellation_token();

    let cancel_event = signed_cancel(&admitted, marker.id.to_hex());
    let cancel_event_id = cancel_event.id.to_hex();
    let lifecycle = admitted.dispatch.privilege.lifecycle.clone();
    let CancelOutcome::Cancel(cancel) = tokio::time::timeout(
        Duration::from_secs(2),
        admitted.receiver.as_ref().expect("receiver").handle_cancel(
            &registry,
            admitted
                .request
                .common
                .project
                .home_channel
                .parse()
                .expect("channel"),
            cancel_event,
        ),
    )
    .await
    .expect("stored Cancel must be observed without waiting for the active lease")
    .expect("observe stored Cancel") else {
        panic!("active claim must observe Cancel")
    };
    assert!(lease_cancellation.is_cancelled());
    assert!(matches!(
        &cancel.terminal,
        CancellationTerminal::Deferred { .. }
    ));
    let cancelled = lifecycle
        .privilege_snapshot()
        .await
        .expect("lifecycle after stored Cancel");
    assert_eq!(cancelled.head_event_id, cancel_event_id);
    assert_eq!(
        cancelled.cancel_event_id.as_deref(),
        Some(cancel_event_id.as_str())
    );
    assert!(!cancelled.terminal);

    let drain = registry.revoke_and_wait(&scope);
    tokio::pin!(drain);
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut drain)
        .await
        .is_err());
    lease
        .finish(
            PrivilegedOperationOutcome::Cancelled,
            Some(git_receipt(
                &admitted.dispatch.privilege,
                ProjectGitOperation::Commit,
                invocation_id,
                buzz_dev_mcp::PrivilegedGitDisposition::NotApplied,
            )),
            None,
        )
        .await
        .expect("release cancelled operation");
    tokio::time::timeout(Duration::from_secs(2), &mut drain)
        .await
        .expect("privileged operation must drain")
        .expect("drain active operation");
    cancel
        .terminal
        .publish(&cancel.emitter)
        .await
        .expect("worker cancellation acknowledgement");
    let terminal = admitted.published.recv().await.expect("Cancelled terminal");
    assert!(matches!(
        JobEvent::parse(&terminal).expect("valid terminal"),
        JobEvent::Control(control) if control.action == JobControlAction::Cancelled
    ));
    assert!(gate
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .is_err());
    assert!(admitted.published.try_recv().is_err());
}

#[tokio::test]
async fn stored_cancel_after_applied_git_receipt_is_indeterminate() {
    let mut admitted = admitted(&["commit"]).await;
    let registry = JobPrivilegeRegistry::default();
    let scope = admitted.dispatch.scope.clone();
    registry
        .register(scope.clone(), admitted.dispatch.privilege.clone())
        .expect("register gate");
    let gate = registry
        .for_session(&scope, &admitted.dispatch.checkout_root)
        .expect("registry lookup")
        .expect("job gate");
    let invocation_id = Uuid::new_v4();
    let lease = gate
        .begin(
            ProjectGitOperation::Commit,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("begin operation");
    let marker = admitted.published.recv().await.expect("privilege marker");
    lease
        .finish(
            PrivilegedOperationOutcome::Completed,
            Some(git_receipt(
                &admitted.dispatch.privilege,
                ProjectGitOperation::Commit,
                invocation_id,
                buzz_dev_mcp::PrivilegedGitDisposition::Applied,
            )),
            None,
        )
        .await
        .expect("persist applied receipt");

    let CancelOutcome::Cancel(cancel) = admitted
        .receiver
        .as_ref()
        .expect("receiver")
        .handle_cancel(
            &registry,
            admitted
                .request
                .common
                .project
                .home_channel
                .parse()
                .expect("channel"),
            signed_cancel(&admitted, marker.id.to_hex()),
        )
        .await
        .expect("observe stored Cancel")
    else {
        panic!("active claim must observe Cancel")
    };
    assert!(matches!(
        cancel.terminal.clone().resolve(),
        CancellationTerminal::Indeterminate { code, .. }
            if code == "cancel_after_applied_git_operation"
    ));
    cancel
        .terminal
        .publish(&cancel.emitter)
        .await
        .expect("publish indeterminate terminal");
    let terminal = admitted.published.recv().await.expect("terminal");
    assert!(matches!(
        JobEvent::parse(&terminal).expect("valid terminal"),
        JobEvent::Error(error)
            if error.outcome == buzz_core::job::JobErrorOutcome::Indeterminate
                && error.code == "cancel_after_applied_git_operation"
                && !error.retryable
    ));
}

#[tokio::test]
async fn missing_git_receipt_makes_stored_cancel_indeterminate() {
    let mut admitted = admitted(&["commit"]).await;
    let registry = JobPrivilegeRegistry::default();
    let scope = admitted.dispatch.scope.clone();
    registry
        .register(scope.clone(), admitted.dispatch.privilege.clone())
        .expect("register gate");
    let gate = registry
        .for_session(&scope, &admitted.dispatch.checkout_root)
        .expect("registry lookup")
        .expect("job gate");
    let lease = gate
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .expect("begin operation");
    let marker = admitted.published.recv().await.expect("privilege marker");
    assert!(lease
        .finish(PrivilegedOperationOutcome::Failed, None, None)
        .await
        .is_err());

    let CancelOutcome::Cancel(cancel) = admitted
        .receiver
        .as_ref()
        .expect("receiver")
        .handle_cancel(
            &registry,
            admitted
                .request
                .common
                .project
                .home_channel
                .parse()
                .expect("channel"),
            signed_cancel(&admitted, marker.id.to_hex()),
        )
        .await
        .expect("observe stored Cancel")
    else {
        panic!("active claim must observe Cancel")
    };
    assert!(matches!(
        cancel.terminal.resolve(),
        CancellationTerminal::Indeterminate { ref code, .. }
            if code == "cancel_during_ambiguous_git_operation"
    ));
}

#[tokio::test]
async fn channel_revocation_waits_for_active_lease_release() {
    let mut admitted = admitted(&["commit"]).await;
    let registry = JobPrivilegeRegistry::default();
    let scope = admitted.dispatch.scope.clone();
    registry
        .register(scope.clone(), admitted.dispatch.privilege.clone())
        .expect("register gate");
    let gate = registry
        .for_session(&scope, &admitted.dispatch.checkout_root)
        .expect("registry lookup")
        .expect("job gate");
    let invocation_id = Uuid::new_v4();
    let lease = gate
        .begin(
            ProjectGitOperation::Commit,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("begin operation");
    let marker = admitted.published.recv().await.expect("privilege marker");
    let lease_cancellation = lease.cancellation_token();
    let channel_id = admitted
        .request
        .common
        .project
        .home_channel
        .parse()
        .expect("channel");

    let drain = registry.revoke_channel_and_wait(channel_id);
    tokio::pin!(drain);
    tokio::select! {
        biased;
        result = &mut drain => panic!("channel drain returned before lease release: {result:?}"),
        _ = lease_cancellation.cancelled() => {}
    }
    let draining = admitted
        .dispatch
        .privilege
        .lifecycle
        .privilege_snapshot()
        .await
        .expect("active lifecycle while channel drains");
    assert_eq!(draining.head_event_id, marker.id.to_hex());
    assert!(!draining.terminal);
    lease
        .finish(
            PrivilegedOperationOutcome::Cancelled,
            Some(git_receipt(
                &admitted.dispatch.privilege,
                ProjectGitOperation::Commit,
                invocation_id,
                buzz_dev_mcp::PrivilegedGitDisposition::NotApplied,
            )),
            None,
        )
        .await
        .expect("release cancelled operation");
    tokio::time::timeout(Duration::from_secs(2), &mut drain)
        .await
        .expect("channel privilege drain must complete")
        .expect("channel privilege drain");
    assert!(gate
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .is_err());
    assert!(admitted.published.try_recv().is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn replacement_lock_inode_cannot_fake_an_active_lease_drain() {
    let mut admitted = admitted(&["commit"]).await;
    let registry = JobPrivilegeRegistry::default();
    let scope = admitted.dispatch.scope.clone();
    registry
        .register(scope.clone(), admitted.dispatch.privilege.clone())
        .expect("register gate");
    let invocation_id = Uuid::new_v4();
    let lease = admitted
        .dispatch
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("begin operation");
    let _marker = admitted.published.recv().await.expect("privilege marker");
    let lease_cancellation = lease.cancellation_token();
    let lock_path = admitted.dispatch.privilege.lock_path.clone();
    let original_path = lock_path.with_extension("privilege.lock.original");
    std::fs::rename(&lock_path, &original_path).expect("move original lock inode");
    drop(
        open_lock_file(&lock_path, true, None)
            .expect("create replacement lock")
            .0,
    );

    let error = registry
        .revoke_and_wait(&scope)
        .await
        .expect_err("replacement inode must never prove the original lease drained");
    assert!(error.contains("lock identity changed"));
    assert!(lease_cancellation.is_cancelled());
    lease
        .finish(
            PrivilegedOperationOutcome::Cancelled,
            Some(git_receipt(
                &admitted.dispatch.privilege,
                ProjectGitOperation::Commit,
                invocation_id,
                buzz_dev_mcp::PrivilegedGitDisposition::NotApplied,
            )),
            None,
        )
        .await
        .expect("finish operation on original lock inode");
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_lock_path_blocks_privileged_start() {
    use std::os::unix::fs::symlink;

    let mut admitted = admitted(&["commit"]).await;
    let lock_path = admitted.dispatch.privilege.lock_path.clone();
    let original_path = lock_path.with_extension("privilege.lock.original");
    std::fs::rename(&lock_path, &original_path).expect("move original lock file");
    symlink(&original_path, &lock_path).expect("replace lock with symlink");

    let error = admitted
        .dispatch
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("no-follow lock open must reject symlink substitution");
    assert!(error.contains("opening job privilege lock failed"));
    assert!(admitted.published.try_recv().is_err());
}

#[tokio::test]
async fn result_or_panic_terminal_drain_waits_for_active_lease_release() {
    let mut admitted = admitted(&["commit"]).await;
    let registry = JobPrivilegeRegistry::default();
    let scope = admitted.dispatch.scope.clone();
    registry
        .register(scope.clone(), admitted.dispatch.privilege.clone())
        .expect("register gate");
    let gate = registry
        .for_session(&scope, &admitted.dispatch.checkout_root)
        .expect("registry lookup")
        .expect("job gate");
    let invocation_id = Uuid::new_v4();
    let lease = gate
        .begin(
            ProjectGitOperation::Commit,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("begin operation");
    let marker = admitted.published.recv().await.expect("privilege marker");
    let lease_cancellation = lease.cancellation_token();

    let drain = registry.revoke_and_wait(&scope);
    tokio::pin!(drain);
    tokio::select! {
        biased;
        result = &mut drain => panic!("terminal drain returned before lease release: {result:?}"),
        _ = lease_cancellation.cancelled() => {}
    }
    let draining = admitted
        .dispatch
        .privilege
        .lifecycle
        .privilege_snapshot()
        .await
        .expect("active lifecycle while terminal drains");
    assert_eq!(draining.head_event_id, marker.id.to_hex());
    assert!(!draining.terminal);
    lease
        .finish(
            PrivilegedOperationOutcome::Cancelled,
            Some(git_receipt(
                &admitted.dispatch.privilege,
                ProjectGitOperation::Commit,
                invocation_id,
                buzz_dev_mcp::PrivilegedGitDisposition::NotApplied,
            )),
            None,
        )
        .await
        .expect("release cancelled operation");
    tokio::time::timeout(Duration::from_secs(2), &mut drain)
        .await
        .expect("terminal privilege drain must complete")
        .expect("terminal privilege drain");
    assert!(gate
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .is_err());
    assert!(admitted.published.try_recv().is_err());
}

#[tokio::test]
async fn deferred_terminal_finisher_waits_then_uses_final_git_effect() {
    let mut admitted = admitted(&["commit"]).await;
    let registry = JobPrivilegeRegistry::default();
    let scope = admitted.dispatch.scope.clone();
    registry
        .register(scope.clone(), admitted.dispatch.privilege.clone())
        .expect("register gate");
    let invocation_id = Uuid::new_v4();
    let lease = admitted
        .dispatch
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("begin operation");
    let _marker = admitted.published.recv().await.expect("privilege marker");
    let lease_cancellation = lease.cancellation_token();

    crate::spawn_job_terminal_finisher(
        registry.clone(),
        scope.clone(),
        Some((
            admitted.dispatch.emitter.clone(),
            crate::DeferredJobTerminal::Outcome(crate::job_receiver::TerminalDisposition::Failed {
                code: "worker_failed".into(),
                message: "worker returned before its Git child reaped".into(),
                retryable: true,
            }),
        )),
    );
    tokio::time::timeout(Duration::from_secs(2), lease_cancellation.cancelled())
        .await
        .expect("finisher must revoke the live lease");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        admitted.published.try_recv().is_err(),
        "terminal must not overtake the privileged child"
    );

    lease
        .finish(
            PrivilegedOperationOutcome::Indeterminate,
            Some(git_receipt(
                &admitted.dispatch.privilege,
                ProjectGitOperation::Commit,
                invocation_id,
                buzz_dev_mcp::PrivilegedGitDisposition::Ambiguous,
            )),
            None,
        )
        .await
        .expect("persist ambiguous final receipt and release lease");
    let terminal = tokio::time::timeout(Duration::from_secs(2), admitted.published.recv())
        .await
        .expect("finisher must publish after drain")
        .expect("terminal event");
    assert!(matches!(
        JobEvent::parse(&terminal).expect("valid terminal"),
        JobEvent::Error(error)
            if error.outcome == buzz_core::job::JobErrorOutcome::Indeterminate
                && error.code == "ambiguous_git_operation"
                && !error.retryable
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(registry
        .for_session(&scope, &admitted.dispatch.checkout_root)
        .is_err());
}

#[tokio::test]
async fn stored_cancel_after_pending_marker_prevents_privileged_start() {
    let mut admitted = admitted(&["commit"]).await;
    let registry = JobPrivilegeRegistry::default();
    let scope = admitted.dispatch.scope.clone();
    registry
        .register(scope.clone(), admitted.dispatch.privilege.clone())
        .expect("register gate");
    let gate = registry
        .for_session(&scope, &admitted.dispatch.checkout_root)
        .expect("registry lookup")
        .expect("job gate");
    let marker = EventBuilder::new(Kind::TextNote, "relay-acknowledged marker")
        .sign_with_keys(&Keys::generate())
        .expect("sign marker fixture");
    admitted
        .dispatch
        .privilege
        .lifecycle
        .stage(
            marker.clone(),
            false,
            admitted.dispatch.claim.accepted.id.to_hex(),
        )
        .await
        .expect("freeze marker before local acknowledgement");

    let control = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common: admitted.request.common.clone(),
            request_event_id: admitted.request_event.id.to_hex(),
            prior_event_id: Some(marker.id.to_hex()),
        },
        action: JobControlAction::Cancel,
        reason: "cancel after acknowledged marker".into(),
        handoff_to: None,
    });
    let cancel_event = EventBuilder::new(
        Kind::Custom(buzz_core::kind::KIND_JOB_CANCEL as u16),
        control.canonical_json().expect("Cancel JSON"),
    )
    .tags(build_job_tags(&control).expect("Cancel tags"))
    .sign_with_keys(&admitted.requester)
    .expect("sign Cancel");
    let CancelOutcome::Cancel(_cancel) = admitted
        .receiver
        .as_ref()
        .expect("receiver")
        .handle_cancel(
            &registry,
            admitted
                .request
                .common
                .project
                .home_channel
                .parse()
                .expect("channel"),
            cancel_event,
        )
        .await
        .expect("adopt Cancel whose predecessor awaits local confirmation")
    else {
        panic!("relay-stored Cancel must not be consumed")
    };
    let snapshot = admitted
        .dispatch
        .privilege
        .lifecycle
        .privilege_snapshot()
        .await
        .expect("cancelled snapshot");
    assert_eq!(
        snapshot.cancel_event_id.as_deref(),
        Some(snapshot.head_event_id.as_str())
    );
    assert!(snapshot.pending_outbox.is_none());
    assert!(snapshot.cancel_event_id.is_some());
    assert!(admitted.dispatch.privilege.revoked.is_cancelled());
    assert!(gate
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .is_err());
    assert!(admitted.published.try_recv().is_err());
}
