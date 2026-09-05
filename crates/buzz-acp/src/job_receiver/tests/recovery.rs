use super::*;
use crate::job_receiver::emitter::build_claim_receipts;
use crate::job_receiver::ledger::{ClaimDecision, ReceiptKind, StoredClaim};
use buzz_core::job::{semantic_request_digest, JobError, JobProgress, JobProgressStatus};
use buzz_dev_mcp::{
    JobPrivilegeGate as _, PrivilegedGitDisposition, PrivilegedGitOperationReceipt,
    PrivilegedOperationOutcome, ProjectGitOperation,
};
use tokio_util::sync::CancellationToken;

fn signed_indeterminate(
    request: &JobRequest,
    request_event_id: &str,
    worker: &Keys,
    worker_sponsor: JobSponsor,
    prior_event_id: String,
    code: &str,
) -> Event {
    let mut common = request.common.clone();
    common.sender_pubkey = worker.public_key().to_hex();
    common.recipient_pubkey = request.common.sender_pubkey.clone();
    common.sponsor = worker_sponsor;
    let error = JobEvent::Error(JobError {
        followup: JobFollowup {
            common,
            request_event_id: request_event_id.into(),
            prior_event_id: Some(prior_event_id),
        },
        outcome: JobErrorOutcome::Indeterminate,
        code: code.into(),
        message: "repository state requires reconciliation".into(),
        retryable: false,
    });
    EventBuilder::new(
        Kind::Custom(buzz_core::kind::KIND_JOB_ERROR as u16),
        error.canonical_json().expect("Indeterminate JSON"),
    )
    .tags(build_job_tags(&error).expect("Indeterminate tags"))
    .sign_with_keys(worker)
    .expect("sign Indeterminate")
}

fn signed_failed(
    request: &JobRequest,
    request_event_id: &str,
    worker: &Keys,
    worker_sponsor: JobSponsor,
    prior_event_id: String,
    retryable: bool,
) -> Event {
    let mut common = request.common.clone();
    common.sender_pubkey = worker.public_key().to_hex();
    common.recipient_pubkey = request.common.sender_pubkey.clone();
    common.sponsor = worker_sponsor;
    let error = JobEvent::Error(JobError {
        followup: JobFollowup {
            common,
            request_event_id: request_event_id.into(),
            prior_event_id: Some(prior_event_id),
        },
        outcome: JobErrorOutcome::Failed,
        code: "model_retry".into(),
        message: "model requested retry after Git mutation".into(),
        retryable,
    });
    EventBuilder::new(
        Kind::Custom(buzz_core::kind::KIND_JOB_ERROR as u16),
        error.canonical_json().expect("Failed JSON"),
    )
    .tags(build_job_tags(&error).expect("Failed tags"))
    .sign_with_keys(worker)
    .expect("sign Failed")
}

fn applied_summary() -> git_receipt_journal::GitEffectSummary {
    git_receipt_journal::GitEffectSummary {
        effect: GitEffect::Applied,
        operation_count: 1,
        applied_count: 1,
        ambiguous_count: 0,
    }
}

fn not_applied_summary() -> git_receipt_journal::GitEffectSummary {
    git_receipt_journal::GitEffectSummary {
        effect: GitEffect::NotApplied,
        operation_count: 0,
        applied_count: 0,
        ambiguous_count: 0,
    }
}

#[test]
fn applied_git_effect_never_preserves_a_retryable_failure() {
    let guarded = guard_terminal_with_git_effect(
        TerminalDisposition::Failed {
            code: "try_again".into(),
            message: "model requested a retry".into(),
            retryable: true,
        },
        Ok(applied_summary()),
    );
    assert!(matches!(
        guarded,
        TerminalDisposition::Indeterminate { code, .. } if code == "applied_git_operation"
    ));
}

#[test]
fn applied_git_effect_preserves_success_and_nonretryable_failure() {
    let success = TerminalDisposition::Success {
        summary: "Completed requested work".into(),
        candidate_sha: Some("1".repeat(40)),
        artifacts: vec!["git:1111111111111111111111111111111111111111".into()],
        evidence: Vec::new(),
    };
    assert_eq!(
        guard_terminal_with_git_effect(success.clone(), Ok(applied_summary())),
        success
    );
    let failure = TerminalDisposition::Failed {
        code: "known_failure".into(),
        message: "the mutation completed before a safe terminal failure".into(),
        retryable: false,
    };
    assert_eq!(
        guard_terminal_with_git_effect(failure.clone(), Ok(applied_summary())),
        failure
    );
}

#[test]
fn native_git_success_does_not_require_a_typed_git_receipt() {
    let success = TerminalDisposition::Success {
        summary: "Completed requested work with native host tools".into(),
        candidate_sha: Some("1".repeat(40)),
        artifacts: vec!["git:1111111111111111111111111111111111111111".into()],
        evidence: vec!["native Git and GitHub evidence reported by the worker".into()],
    };
    assert_eq!(
        guard_terminal_with_git_effect(success.clone(), Ok(not_applied_summary())),
        success
    );
}

#[tokio::test]
async fn sponsor_login_drift_replays_exact_outbox_and_keeps_preprompt_gate_usable() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let (request, request_event) = fixture(
        &requester,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "sponsor-login-drift",
        "work",
    );
    let mut sponsor_a = sponsor(&worker);
    sponsor_a.github_login = "owner-login-a".into();
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
    let receiver = JobReceiver::for_test(
        context(&worker),
        worker.clone(),
        rest.clone(),
        sponsor_a.clone(),
        grants_with_git_operations(&request, &["commit"]),
        ledger_root.clone(),
    );
    let tenant = receiver.tenant.clone();
    let HandleOutcome::Dispatch(dispatch) = receiver
        .handle_request(channel, request_event.clone(), Some(&project(&request)))
        .await
        .expect("claim request")
    else {
        panic!("request should dispatch")
    };
    let _ = published.recv().await.expect("Processed receipt");
    let _ = published.recv().await.expect("Accepted receipt");

    let durable_common = verified_durable_response_common(
        &dispatch.claim,
        &worker.public_key().to_hex(),
        &sponsor_a.pubkey,
    )
    .expect("durable accepted chain");
    assert_eq!(durable_common.sponsor.github_login, "owner-login-a");
    assert!(
        verified_durable_response_common(
            &dispatch.claim,
            &worker.public_key().to_hex(),
            &Keys::generate().public_key().to_hex(),
        )
        .is_err(),
        "an owner public-key rebind must still fail closed"
    );

    let progress = JobEvent::Progress(JobProgress {
        followup: JobFollowup {
            common: durable_common,
            request_event_id: request_event.id.to_hex(),
            prior_event_id: Some(dispatch.claim.accepted.id.to_hex()),
        },
        status: JobProgressStatus::Progress,
        message: "frozen before sponsor metadata changed".into(),
        evidence: Vec::new(),
    });
    let progress = EventBuilder::new(
        Kind::Custom(buzz_core::kind::KIND_JOB_PROGRESS as u16),
        progress.canonical_json().expect("Progress JSON"),
    )
    .tags(build_job_tags(&progress).expect("Progress tags"))
    .sign_with_keys(&worker)
    .expect("sign Progress");
    let lifecycle = receiver.ledger.lifecycle_store(&dispatch.claim);
    lifecycle
        .stage(progress.clone(), false, dispatch.claim.accepted.id.to_hex())
        .await
        .expect("freeze exact Progress");
    drop(dispatch);
    drop(receiver);

    let mut sponsor_b = sponsor_a;
    sponsor_b.github_login = "owner-login-b".into();
    let reopened = JobReceiver::for_test(
        tenant,
        worker.clone(),
        rest,
        sponsor_b,
        grants_with_git_operations(&request, &["commit"]),
        ledger_root.clone(),
    );
    reopened
        .retry_outboxes()
        .await
        .expect("login-only drift must not strand exact replay");
    assert_eq!(
        published.recv().await.expect("exact Progress replay").id,
        progress.id
    );

    let HandleOutcome::Dispatch(replayed) = reopened
        .handle_request(channel, request_event, Some(&project(&request)))
        .await
        .expect("exact request replay")
    else {
        panic!("pre-prompt replay should remain dispatchable")
    };
    let _ = published.recv().await.expect("Processed replay");
    let _ = published.recv().await.expect("Accepted replay");
    assert!(reopened
        .mark_prompt_started(&replayed.claim)
        .await
        .expect("durable prompt fence"));
    let invocation_id = Uuid::new_v4();
    let lease = replayed
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("pre-prompt privilege survives login drift");
    let marker = published.recv().await.expect("privilege marker");
    let JobEvent::Progress(marker_body) = JobEvent::parse(&marker).expect("valid marker") else {
        panic!("privilege fence should be Progress")
    };
    assert_eq!(
        marker_body.followup.common.sponsor.github_login,
        "owner-login-a"
    );
    lease
        .finish(
            PrivilegedOperationOutcome::Failed,
            Some(PrivilegedGitOperationReceipt {
                schema_version: "buzz.git-operation-receipt.v1".into(),
                invocation_id,
                operation: ProjectGitOperation::Commit,
                session_channel_id: channel.to_string(),
                operation_id: request.common.operation_id.clone(),
                request_event_id: replayed.claim.request_event_id.clone(),
                worker_pubkey: worker.public_key().to_hex(),
                scope_digest: Some(replayed.claim.digest.clone()),
                repository: Some(request.common.repository.canonical.clone()),
                branch_ref: Some(format!("refs/heads/{}", request.common.repository.branch)),
                previous_object: None,
                intended_object: None,
                observed_object: None,
                disposition: PrivilegedGitDisposition::NotApplied,
            }),
            None,
        )
        .await
        .expect("persist non-applied retry result");

    server.abort();
    drop(reopened);
    std::fs::remove_dir_all(ledger_root).ok();
}

#[tokio::test]
async fn membership_revocation_freezes_exactly_one_terminal() {
    let sender = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let (request, request_event) = fixture(
        &sender,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "revoked",
        "work",
    );
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
    let receiver = JobReceiver::for_test(
        context(&worker),
        worker.clone(),
        rest,
        sponsor(&worker),
        grants(&request),
        ledger_root.clone(),
    );
    assert!(matches!(
        receiver
            .handle_request(channel, request_event, Some(&project(&request)))
            .await
            .expect("claim"),
        HandleOutcome::Dispatch(_)
    ));
    let _ = published.recv().await.expect("processed");
    let _ = published.recv().await.expect("accepted");
    assert_eq!(
        receiver
            .terminate_channel(channel)
            .await
            .expect("terminate revoked channel"),
        1
    );
    let terminal = published.recv().await.expect("terminal");
    assert!(matches!(
        JobEvent::parse(&terminal).expect("valid terminal"),
        JobEvent::Error(error)
            if error.outcome == JobErrorOutcome::Indeterminate
                && error.code == "membership_revoked"
                && !error.retryable
    ));
    assert_eq!(
        receiver
            .terminate_channel(channel)
            .await
            .expect("terminal replay"),
        0
    );
    assert!(
        published.try_recv().is_err(),
        "must not fork terminal state"
    );
    server.abort();
    std::fs::remove_dir_all(ledger_root).ok();
}

#[cfg(unix)]
#[tokio::test]
async fn accepted_job_reaches_provider_with_conversation_recovery_enabled() {
    use crate::acp::{AcpClient, StopReason};
    use crate::pool::{
        run_prompt_task, ChannelInfoResolver, OwnedAgent, PromptContext, PromptExecution,
        SessionState,
    };
    use std::sync::Arc;
    use std::time::Duration;

    let sender = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let tenant = context(&worker);
    let worker_sponsor = sponsor(&worker);
    let (request, request_event) = fixture(
        &sender,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "job-with-conversation-recovery",
        "inspect the accepted job",
    );
    let (rest, mut published, receiver_server) =
        RestClient::accepting_test_pair(worker.clone()).await;
    let receiver = JobReceiver::for_test(
        tenant.clone(),
        worker.clone(),
        rest.clone(),
        worker_sponsor.clone(),
        grants(&request),
        ledger_root.clone(),
    );
    let HandleOutcome::Dispatch(dispatch) = receiver
        .handle_request(channel, request_event.clone(), Some(&project(&request)))
        .await
        .expect("accept job before provider setup")
    else {
        panic!("accepted job must dispatch")
    };
    let _processed = published.recv().await.expect("Processed receipt");
    let _accepted = published.recv().await.expect("Accepted receipt");
    assert!(receiver
        .mark_prompt_started(&dispatch.claim)
        .await
        .expect("persist job execution fence before provider setup"));
    let registry = JobPrivilegeRegistry::default();
    registry
        .register(dispatch.scope.clone(), dispatch.privilege.clone())
        .expect("register actual admitted job capability");

    // Prompt-context queries have no extra metadata; job authority comes from
    // the signed request, receiver grant and registered capability above.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind context query fixture");
    let context_host = listener.local_addr().unwrap().to_string();
    let context_url = format!("http://{context_host}");
    let context_document = AuthenticatedContext {
        schema_version: "buzz.context.v1".into(),
        community_id: tenant.community_id.clone(),
        host: context_host,
        pubkey: worker.public_key().to_hex(),
    };
    let queried_request = request_event.clone();
    let queried_request_id = request_event.id.to_hex();
    let router = axum::Router::new()
        .route(
            "/api/context",
            axum::routing::get(move || {
                let context = context_document.clone();
                async move { axum::Json(context) }
            }),
        )
        .route(
            "/query",
            axum::routing::post(move |axum::Json(filters): axum::Json<serde_json::Value>| {
                let request = queried_request.clone();
                let request_id = queried_request_id.clone();
                async move {
                    let requests_exact_job =
                        filters.as_array().into_iter().flatten().any(|filter| {
                            filter
                                .get("ids")
                                .and_then(serde_json::Value::as_array)
                                .is_some_and(|ids| {
                                    ids.iter().any(|id| id.as_str() == Some(&request_id))
                                })
                        });
                    axum::Json(if requests_exact_job {
                        serde_json::json!([request])
                    } else {
                        serde_json::json!([])
                    })
                }
            }),
        );
    let context_server = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("serve context queries");
    });
    let context_rest = RestClient {
        http: reqwest::Client::new(),
        base_url: context_url.clone(),
        keys: worker.clone(),
        auth_tag_json: None,
    };
    let recovery_path = ledger_root.join("conversation-sessions.json");
    let recovery = crate::session_recovery::SessionRecoveryStore::open(recovery_path.clone())
        .expect("conversation recovery enabled");
    let cwd = dispatch.checkout_root.to_string_lossy().into_owned();
    let batch = crate::queue::FlushBatch {
        channel_id: channel,
        scope: dispatch.scope.clone(),
        events: vec![crate::queue::BatchEvent {
            event: dispatch.event.clone(),
            prompt_tag: "agent-job".into(),
            received_at: std::time::Instant::now(),
        }],
        cancelled_events: vec![],
        cancel_reason: None,
    };
    let trusted_grants = format!(
        r#"{{"version":1,"grants":[{{"project_address":"{}","home_channel":"{}","repository":"{}","requester_pubkeys":["{}"],"capabilities":["rust"],"git_operations":[],"path_prefixes":["src"],"base_sha":"{}","branch":"{}","worktree_id":"{}","checkout_root":{}}}]}}"#,
        request.common.project.address,
        request.common.project.home_channel,
        request.common.repository.canonical,
        request.common.sender_pubkey,
        request.common.repository.base_sha,
        request.common.repository.branch,
        request.common.repository.worktree_id,
        serde_json::to_string(&dispatch.checkout_root).expect("trusted checkout path JSON"),
    );

    for (provider, full_mode, capture_name) in [
        (
            "@agentclientprotocol/claude-agent-acp",
            "bypassPermissions",
            "claude-full-host-job-wire.ndjson",
        ),
        (
            "@agentclientprotocol/codex-acp",
            "agent-full-access",
            "codex-full-host-job-wire.ndjson",
        ),
    ] {
        let identity = buzz_dev_mcp::HarnessTrustedIdentity::new(
            &dispatch.checkout_root,
            context_url.clone(),
            worker.clone(),
            None,
            Some(worker_sponsor.github_login.clone()),
            Some(trusted_grants.clone()),
            None,
            true,
        )
        .expect("harness-owned identity");
        let factory = crate::trusted_mcp::TrustedMcpFactory::with_job_privileges(
            identity,
            Duration::from_secs(60),
            registry.clone(),
        )
        .expect("trusted factory with admitted job registry");
        let capture = ledger_root.join(capture_name);
        let script = r#"while IFS= read -r line; do
  printf '%s\n' "$line" >> "$CAPTURE_FILE"
  [[ "$line" =~ \"id\":([0-9]+) ]] || continue
  id=${BASH_REMATCH[1]}
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":'"$id"',"result":{"protocolVersion":2,"agentCapabilities":{"mcpCapabilities":{"http":true}},"agentInfo":{"name":"__PROVIDER__","version":"test"}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":'"$id"',"result":{"sessionId":"full-host-job-session","configOptions":[{"id":"mode","category":"mode","currentValue":"default","options":[{"value":"default"},{"value":"__MODE__"}]}],"modes":{"availableModes":[{"id":"default"},{"id":"__MODE__"}],"currentModeId":"default"}}}'
      ;;
    *'"method":"session/set_config_option"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":'"$id"',"result":{"configOptions":[{"id":"mode","category":"mode","currentValue":"__MODE__"}],"modes":{"availableModes":[{"id":"default"},{"id":"__MODE__"}],"currentModeId":"__MODE__"}}}'
      ;;
    *'"method":"session/prompt"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":'"$id"',"result":{"stopReason":"end_turn"}}'
      ;;
  esac
done"#
            .replace("__PROVIDER__", provider)
            .replace("__MODE__", full_mode);
        let mut acp = AcpClient::spawn(
            "bash",
            &["-c".into(), script],
            &[(
                "CAPTURE_FILE".into(),
                capture.to_string_lossy().into_owned(),
            )],
            false,
        )
        .await
        .expect("spawn scripted full-host adapter");
        acp.initialize()
            .await
            .expect("initialize full-host adapter");
        let agent = OwnedAgent {
            index: 0,
            acp,
            state: SessionState::default(),
            model_capabilities: None,
            desired_model: None,
            model_overridden: false,
            desired_model_request_id: None,
            desired_model_pending_ack: false,
            startup_effort: None,
            agent_name: provider.into(),
            goose_system_prompt_supported: None,
            protocol_version: 2,
        };
        let ctx = Arc::new(PromptContext {
            mcp_servers: vec![crate::acp::McpServer::stdio(
                "configured-host-mcp",
                "host-mcp",
                vec![],
                vec![],
            )],
            trusted_mcp_factory: Some(factory),
            session_recovery: Some(recovery.clone()),
            initial_message: None,
            idle_timeout: Duration::from_secs(5),
            max_turn_duration: Duration::from_secs(10),
            turn_liveness_interval: Duration::ZERO,
            dedup_mode: crate::config::DedupMode::Queue,
            reply_placement: crate::reply_placement::ReplyPlacement::Thread,
            system_prompt: None,
            session_title: None,
            team_instructions: None,
            workspace_project_channel: None,
            workspace_project_address: None,
            workspace_project_repository: None,
            workspace_project_revision: None,
            heartbeat_prompt: None,
            base_prompt: None,
            cwd: cwd.clone(),
            rest_client: context_rest.clone(),
            channel_info: ChannelInfoResolver::new(
                std::collections::HashMap::from([(
                    channel,
                    crate::relay::ChannelInfo {
                        name: "job-test".into(),
                        channel_type: "stream".into(),
                        description: None,
                    },
                )]),
                context_rest.clone(),
            ),
            context_message_limit: 0,
            max_turns_per_session: 0,
            permission_mode: crate::config::PermissionMode::BypassPermissions,
            agent_keys: worker.clone(),
            agent_owner_pubkey: None,
            memory_enabled: false,
            harness_name: provider.into(),
            relay_url: context_url.clone(),
        });
        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::time::timeout(
            Duration::from_secs(15),
            run_prompt_task(
                agent,
                Some(batch.clone()),
                None,
                ctx,
                result_tx,
                None,
                PromptExecution::new(
                    Some(cwd.clone()),
                    format!("{provider}-job-with-recovery-turn"),
                ),
            ),
        )
        .await
        .expect("job prompt task completes");
        let mut result = result_rx.recv().await.expect("job prompt result");
        match &result.outcome {
            crate::PromptOutcome::Ok(StopReason::EndTurn) => {}
            crate::PromptOutcome::Error(error) => {
                panic!("accepted job must reach {provider} session/prompt: {error}")
            }
            _ => panic!("accepted job did not complete its {provider} turn"),
        }
        assert!(result.batch.is_none(), "completed job must not be requeued");
        result.agent.state.invalidate_scope(&dispatch.scope);
        result.agent.acp.shutdown().await;

        let wire: Vec<serde_json::Value> = std::fs::read_to_string(&capture)
            .expect("read actual ACP wire")
            .lines()
            .map(|line| serde_json::from_str(line).expect("ACP request JSON"))
            .collect();
        assert_eq!(
            wire.iter()
                .filter(|event| event["method"] == "session/prompt")
                .count(),
            1,
            "the accepted job must execute exactly one provider prompt"
        );
        let session = wire
            .iter()
            .find(|event| event["method"] == "session/new")
            .expect("new full-host job session");
        let servers = session["params"]["mcpServers"]
            .as_array()
            .expect("MCP servers");
        assert_eq!(servers.len(), 2);
        assert!(servers
            .iter()
            .any(|server| server["name"] == "configured-host-mcp"));
        assert!(servers
            .iter()
            .any(|server| server["name"] == crate::trusted_mcp::SERVER_NAME));
        assert!(session["params"]["_meta"]["buzz"]["jobPolicy"].is_null());
        assert!(session["params"]["_meta"]["claudeCode"].is_null());
        let mode = wire
            .iter()
            .find(|event| event["method"] == "session/set_config_option")
            .expect("full-host job permission mode");
        assert_eq!(mode["params"]["configId"], "mode");
        assert_eq!(mode["params"]["value"], full_mode);
        assert!(recovery
            .binding(&dispatch.scope, provider, &cwd)
            .expect("Job is outside conversation recovery")
            .is_none());
    }
    assert!(
        !recovery_path.exists(),
        "Job must not create a conversation recovery document"
    );
    let claim = dispatch.claim.clone();
    registry.remove(&dispatch.scope);
    drop(dispatch);
    drop(receiver);
    let reopened = JobReceiver::for_test(
        tenant,
        worker,
        rest,
        worker_sponsor,
        grants(&request),
        ledger_root.clone(),
    );
    assert!(!reopened
        .mark_prompt_started(&claim)
        .await
        .expect("durable A2A execution fence survives restart"));
    assert!(reopened
        .pending_events()
        .await
        .expect("pending jobs")
        .is_empty());
    assert!(matches!(
        reopened
            .handle_request(channel, request_event, Some(&project(&request)))
            .await
            .expect("exact accepted request replay"),
        HandleOutcome::Consumed
    ));
    receiver_server.abort();
    context_server.abort();
    drop(reopened);
    std::fs::remove_dir_all(ledger_root).ok();
}

#[tokio::test]
async fn accepted_preprompt_setup_error_survives_restart_without_redispatch() {
    let sender = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let tenant = context(&worker);
    let worker_sponsor = sponsor(&worker);
    let (request, request_event) = fixture(
        &sender,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "accepted-preprompt-setup-error",
        "work",
    );
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
    let receiver = JobReceiver::for_test(
        tenant.clone(),
        worker.clone(),
        rest.clone(),
        worker_sponsor.clone(),
        grants(&request),
        ledger_root.clone(),
    );
    let HandleOutcome::Dispatch(dispatch) = receiver
        .handle_request(channel, request_event.clone(), Some(&project(&request)))
        .await
        .expect("accept request")
    else {
        panic!("first delivery must dispatch")
    };
    let processed = published.recv().await.expect("Processed receipt");
    let accepted = published.recv().await.expect("Accepted receipt");
    // Production fences redispatch before provider setup, even when setup
    // fails without sending the prompt to the model.
    assert!(receiver
        .mark_prompt_started(&dispatch.claim)
        .await
        .expect("durable pre-provider admission fence"));
    let registry = JobPrivilegeRegistry::default();
    registry
        .register(dispatch.scope.clone(), dispatch.privilege.clone())
        .expect("register admitted job capability");
    let disposition = crate::job_terminal_disposition(
        &crate::PromptOutcome::Error(crate::acp::AcpError::Protocol(
            "session setup rejected permission mode".into(),
        )),
        true,
        None,
        &request.common.operation_id,
        &request_event.id.to_hex(),
        dispatch.emitter.scope_digest(),
    );
    crate::spawn_job_terminal_finisher(
        registry.clone(),
        dispatch.scope.clone(),
        Some((
            dispatch.emitter.clone(),
            crate::DeferredJobTerminal::Outcome(disposition),
        )),
    );
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(2), published.recv())
        .await
        .expect("setup failure must publish a terminal")
        .expect("Failed terminal");
    assert_eq!(
        terminal.kind,
        Kind::Custom(buzz_core::kind::KIND_JOB_ERROR as u16)
    );
    assert!(matches!(
        JobEvent::parse(&terminal).expect("valid terminal"),
        JobEvent::Error(error)
            if error.outcome == JobErrorOutcome::Failed
                && error.code == "worker_startup_failed"
                && error.retryable
                && error.followup.request_event_id == request_event.id.to_hex()
                && error.followup.prior_event_id == Some(accepted.id.to_hex())
    ));
    assert!(
        registry
            .for_session(&dispatch.scope, &dispatch.checkout_root)
            .is_err(),
        "terminal publication must revoke the admitted capability"
    );
    // The relay can expose the event before its HTTP acknowledgement returns.
    // Wait for that acknowledgement before simulating process restart.
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !dispatch
            .emitter
            .is_terminal()
            .await
            .expect("terminal state")
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Failed terminal must be durably acknowledged");
    assert!(receiver
        .pending_events()
        .await
        .expect("pending requests")
        .is_empty());
    let claim = dispatch.claim.clone();
    drop(dispatch);
    drop(receiver);

    let reopened = JobReceiver::for_test(
        tenant,
        worker,
        rest,
        worker_sponsor,
        grants(&request),
        ledger_root.clone(),
    );
    reopened
        .recover_lifecycle()
        .await
        .expect("recover durable Failed terminal");
    assert_eq!(
        reopened
            .ledger
            .lifecycle_store(&claim)
            .snapshot()
            .await
            .expect("reopened lifecycle"),
        (terminal.id.to_hex(), None, true)
    );
    assert!(reopened
        .pending_events()
        .await
        .expect("recovered pending requests")
        .is_empty());
    assert!(!reopened
        .mark_prompt_started(&claim)
        .await
        .expect("reexecution fence survives restart"));
    assert!(
        published.try_recv().is_err(),
        "recovery must not fork a terminal"
    );
    assert!(matches!(
        reopened
            .handle_request(channel, request_event, Some(&project(&request)))
            .await
            .expect("exact request replay"),
        HandleOutcome::Consumed
    ));
    assert_eq!(
        published.recv().await.expect("Processed replay").id,
        processed.id
    );
    assert_eq!(
        published.recv().await.expect("Accepted replay").id,
        accepted.id
    );
    reopened
        .retry_outboxes()
        .await
        .expect("acknowledged terminal remains settled");
    assert!(
        published.try_recv().is_err(),
        "replay must not emit a second terminal"
    );
    server.abort();
    drop(reopened);
    std::fs::remove_dir_all(ledger_root).ok();
}

#[tokio::test]
async fn panic_after_prompt_start_never_redispatches_and_freezes_one_terminal() {
    let sender = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let (request, request_event) = fixture(
        &sender,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "panic-after-start",
        "work",
    );
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
    let receiver = JobReceiver::for_test(
        context(&worker),
        worker.clone(),
        rest,
        sponsor(&worker),
        grants(&request),
        ledger_root.clone(),
    );
    let HandleOutcome::Dispatch(dispatch) = receiver
        .handle_request(channel, request_event, Some(&project(&request)))
        .await
        .expect("claim request")
    else {
        panic!("request should dispatch")
    };
    receiver
        .mark_prompt_started(&dispatch.claim)
        .await
        .expect("durable prompt-start marker");
    let _ = published.recv().await.expect("processed");
    let _ = published.recv().await.expect("accepted");

    let replay_probe = dispatch.emitter.clone();
    dispatch
        .emitter
        .indeterminate(
            "worker_panicked".into(),
            "Worker process stopped before recording a terminal outcome".into(),
        )
        .await
        .expect("freeze panic terminal");
    let terminal = published.recv().await.expect("panic terminal");
    assert!(matches!(
        JobEvent::parse(&terminal).expect("valid terminal"),
        JobEvent::Error(error)
            if error.outcome == JobErrorOutcome::Indeterminate
                && error.code == "worker_panicked"
                && !error.retryable
    ));
    assert!(
        receiver
            .pending_events()
            .await
            .expect("scan pending requests")
            .is_empty(),
        "a prompt-started job must never be redispatched"
    );
    assert!(
        replay_probe
            .indeterminate("worker_panicked".into(), "duplicate".into())
            .await
            .is_err(),
        "a terminal lifecycle must reject a sibling terminal"
    );
    receiver
        .recover_lifecycle()
        .await
        .expect("terminal restart replay");
    assert!(
        published.try_recv().is_err(),
        "panic handling must emit exactly one terminal"
    );
    server.abort();
    std::fs::remove_dir_all(ledger_root).ok();
}

#[tokio::test]
async fn membership_revocation_before_accept_durably_suppresses_acceptance() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let (request, request_event) = fixture(
        &requester,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "revoked-before-accept",
        "work",
    );
    let digest = semantic_request_digest(&request).expect("request digest");
    let worker_sponsor = sponsor(&worker);
    let receipts = build_claim_receipts(
        &request,
        &request_event.id.to_hex(),
        &digest,
        &worker,
        &worker_sponsor,
    )
    .expect("claim receipts");
    let processed = receipts.processed.clone();
    let accepted_id = receipts.accepted.id;
    let tenant = context(&worker);
    let stored = StoredClaim::new(
        tenant.community_id.clone(),
        request.common.sender_pubkey.clone(),
        request.common.idempotency_key.clone(),
        digest,
        request_event.id.to_hex(),
        request_event.clone(),
        receipts.processed,
        receipts.accepted,
    );
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
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
            .ledger
            .claim(stored.clone())
            .await
            .expect("persist claim"),
        ClaimDecision::New(_)
    ));
    receiver
        .ledger
        .mark_receipt_acked(&stored, ReceiptKind::Processed)
        .await
        .expect("Processed was relay stored");

    assert_eq!(
        receiver
            .terminate_channel(channel)
            .await
            .expect("suppress pre-Accept claim"),
        1
    );
    let lifecycle = receiver.ledger.lifecycle_store(&stored);
    let snapshot = lifecycle
        .privilege_snapshot()
        .await
        .expect("suppressed lifecycle");
    assert_eq!(snapshot.accepted_event_id, processed.id.to_hex());
    assert_eq!(snapshot.head_event_id, processed.id.to_hex());
    assert!(snapshot.pending_outbox.is_none());
    assert!(!snapshot.terminal);
    assert!(!receiver
        .ledger
        .receipt_acked(&stored, ReceiptKind::Accepted)
        .await
        .expect("Accepted receipt state"));
    assert!(published.try_recv().is_err(), "must not invent a terminal");

    assert_eq!(
        receiver
            .terminate_channel(channel)
            .await
            .expect("idempotent suppression"),
        0
    );
    receiver
        .retry_outboxes()
        .await
        .expect("suppressed receipt retry");
    assert!(published.try_recv().is_err(), "must not retry Accepted");

    assert!(matches!(
        receiver
            .handle_request(channel, request_event, Some(&project(&request)))
            .await
            .expect("duplicate request"),
        HandleOutcome::Consumed
    ));
    let replayed_processed = published.recv().await.expect("Processed replay");
    assert_eq!(replayed_processed.id, processed.id);
    assert_ne!(replayed_processed.id, accepted_id);
    assert!(published.try_recv().is_err(), "must not publish Accepted");
    server.abort();
    drop(receiver);
    std::fs::remove_dir_all(ledger_root).ok();
}

#[tokio::test]
async fn processed_then_cancel_recovers_without_accept_or_dispatch() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let (request, request_event) = fixture(
        &requester,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "cancel-before-accept",
        "work",
    );
    let digest = semantic_request_digest(&request).expect("request digest");
    let worker_sponsor = sponsor(&worker);
    let receipts = build_claim_receipts(
        &request,
        &request_event.id.to_hex(),
        &digest,
        &worker,
        &worker_sponsor,
    )
    .expect("claim receipts");
    let processed = receipts.processed.clone();
    let tenant = context(&worker);
    let stored = StoredClaim::new(
        tenant.community_id.clone(),
        request.common.sender_pubkey.clone(),
        request.common.idempotency_key.clone(),
        digest,
        request_event.id.to_hex(),
        request_event.clone(),
        receipts.processed,
        receipts.accepted,
    );
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
    let receiver = JobReceiver::for_test(
        tenant.clone(),
        worker.clone(),
        rest.clone(),
        worker_sponsor.clone(),
        grants(&request),
        ledger_root.clone(),
    );
    assert!(matches!(
        receiver
            .ledger
            .claim(stored.clone())
            .await
            .expect("persist claim"),
        ClaimDecision::New(_)
    ));
    receiver
        .ledger
        .mark_receipt_acked(&stored, ReceiptKind::Processed)
        .await
        .expect("Processed was relay stored");
    drop(receiver);

    // A restart before the Cancel replay must not invent an Accepted anchor.
    let receiver = JobReceiver::for_test(
        tenant.clone(),
        worker.clone(),
        rest.clone(),
        worker_sponsor.clone(),
        grants(&request),
        ledger_root.clone(),
    );
    receiver
        .recover_lifecycle()
        .await
        .expect("leave pre-Accept claim awaiting relay replay");
    assert!(!receiver.ledger.lifecycle_store(&stored).exists());
    assert!(published.try_recv().is_err());

    let control = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common: request.common.clone(),
            request_event_id: request_event.id.to_hex(),
            prior_event_id: Some(processed.id.to_hex()),
        },
        action: JobControlAction::Cancel,
        reason: "cancel before claim acceptance".into(),
        handoff_to: None,
    });
    let cancel_event = EventBuilder::new(
        Kind::Custom(buzz_core::kind::KIND_JOB_CANCEL as u16),
        control.canonical_json().expect("Cancel JSON"),
    )
    .tags(build_job_tags(&control).expect("Cancel tags"))
    .sign_with_keys(&requester)
    .expect("sign Cancel");
    let CancelOutcome::Cancel(cancel) = receiver
        .handle_cancel(&JobPrivilegeRegistry::default(), channel, cancel_event)
        .await
        .expect("observe relay-stored pre-Accept Cancel")
    else {
        panic!("pre-Accept Cancel must be claimed for acknowledgement")
    };
    assert!(!receiver
        .ledger
        .receipt_acked(&stored, ReceiptKind::Accepted)
        .await
        .expect("Accepted receipt state"));
    drop(cancel);
    drop(receiver);

    let reopened = JobReceiver::for_test(
        tenant,
        worker.clone(),
        rest,
        worker_sponsor,
        grants(&request),
        ledger_root.clone(),
    );
    reopened
        .recover_lifecycle()
        .await
        .expect("recover Cancelled terminal");
    let terminal = published.recv().await.expect("Cancelled terminal");
    assert!(matches!(
        JobEvent::parse(&terminal).expect("valid terminal"),
        JobEvent::Control(control) if control.action == JobControlAction::Cancelled
    ));
    reopened
        .retry_outboxes()
        .await
        .expect("pre-Accept receipt retry converges");
    assert!(
        published.try_recv().is_err(),
        "Accepted must stay suppressed"
    );

    assert!(matches!(
        reopened
            .handle_request(channel, request_event, Some(&project(&request)))
            .await
            .expect("duplicate request"),
        HandleOutcome::Consumed
    ));
    let replayed_processed = published.recv().await.expect("Processed replay");
    assert_eq!(replayed_processed.id, processed.id);
    assert!(published.try_recv().is_err(), "must not publish Accepted");
    server.abort();
    drop(reopened);
    std::fs::remove_dir_all(ledger_root).ok();
}

#[tokio::test]
async fn recovery_never_replays_cancelled_after_an_applied_git_receipt() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let (request, request_event) = fixture(
        &requester,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "unsafe-cancel-replay",
        "work",
    );
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
    let receiver = JobReceiver::for_test(
        context(&worker),
        worker.clone(),
        rest,
        sponsor(&worker),
        grants_with_git_operations(&request, &["commit"]),
        ledger_root.clone(),
    );
    let HandleOutcome::Dispatch(dispatch) = receiver
        .handle_request(channel, request_event.clone(), Some(&project(&request)))
        .await
        .expect("claim request")
    else {
        panic!("request should dispatch")
    };
    let _ = published.recv().await.expect("Processed receipt");
    let _ = published.recv().await.expect("Accepted receipt");
    assert!(receiver
        .mark_prompt_started(&dispatch.claim)
        .await
        .expect("durable prompt-start marker"));

    let invocation_id = Uuid::new_v4();
    let lease = dispatch
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("begin privileged Git operation");
    let marker = published.recv().await.expect("privilege marker");
    let intended = "2".repeat(40);
    lease
        .finish(
            PrivilegedOperationOutcome::Completed,
            Some(PrivilegedGitOperationReceipt {
                schema_version: "buzz.git-operation-receipt.v1".into(),
                invocation_id,
                operation: ProjectGitOperation::Commit,
                session_channel_id: channel.to_string(),
                operation_id: request.common.operation_id.clone(),
                request_event_id: request_event.id.to_hex(),
                worker_pubkey: worker.public_key().to_hex(),
                scope_digest: Some(dispatch.claim.digest.clone()),
                repository: Some(request.common.repository.canonical.clone()),
                branch_ref: Some(format!("refs/heads/{}", request.common.repository.branch)),
                previous_object: Some("1".repeat(40)),
                intended_object: Some(intended.clone()),
                observed_object: Some(intended),
                disposition: PrivilegedGitDisposition::Applied,
            }),
            None,
        )
        .await
        .expect("persist applied receipt");

    let mut common = request.common.clone();
    common.sender_pubkey = worker.public_key().to_hex();
    common.recipient_pubkey = request.common.sender_pubkey.clone();
    common.sponsor = sponsor(&worker);
    let cancelled = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common,
            request_event_id: request_event.id.to_hex(),
            prior_event_id: Some(marker.id.to_hex()),
        },
        action: JobControlAction::Cancelled,
        reason: "requester_cancelled".into(),
        handoff_to: None,
    });
    let cancelled = EventBuilder::new(
        Kind::Custom(buzz_core::kind::KIND_JOB_CANCEL as u16),
        cancelled.canonical_json().expect("Cancelled JSON"),
    )
    .tags(build_job_tags(&cancelled).expect("Cancelled tags"))
    .sign_with_keys(&worker)
    .expect("sign Cancelled");
    let lifecycle = receiver.ledger.lifecycle_store(&dispatch.claim);
    lifecycle
        .stage(cancelled.clone(), true, marker.id.to_hex())
        .await
        .expect("freeze stale Cancelled terminal");

    let error = receiver
        .recover_lifecycle()
        .await
        .expect_err("recovery must reject false Cancelled replay");
    assert!(error
        .to_string()
        .contains("refusing to replay Cancelled after 1 applied Git operation"));
    let (_, pending, terminal) = lifecycle.snapshot().await.expect("pending lifecycle");
    assert_eq!(
        pending.expect("frozen terminal remains pending").id,
        cancelled.id
    );
    assert!(!terminal);
    assert!(
        published.try_recv().is_err(),
        "unsafe terminal was not sent"
    );
    let retry_error = receiver
        .retry_outboxes()
        .await
        .expect_err("periodic retry must reject false Cancelled replay");
    assert!(retry_error
        .to_string()
        .contains("refusing to replay Cancelled after 1 applied Git operation"));
    let (_, pending, terminal) = lifecycle.snapshot().await.expect("retry lifecycle");
    assert_eq!(
        pending.expect("retry keeps terminal frozen").id,
        cancelled.id
    );
    assert!(!terminal);
    assert!(published.try_recv().is_err(), "unsafe retry was not sent");

    server.abort();
    drop(receiver);
    std::fs::remove_dir_all(ledger_root).ok();
}

#[tokio::test]
async fn recovery_never_replays_retryable_failure_after_an_applied_git_receipt() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let (request, request_event) = fixture(
        &requester,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "unsafe-retryable-failure-replay",
        "work",
    );
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
    let receiver = JobReceiver::for_test(
        context(&worker),
        worker.clone(),
        rest,
        sponsor(&worker),
        grants_with_git_operations(&request, &["commit"]),
        ledger_root.clone(),
    );
    let HandleOutcome::Dispatch(dispatch) = receiver
        .handle_request(channel, request_event.clone(), Some(&project(&request)))
        .await
        .expect("claim request")
    else {
        panic!("request should dispatch")
    };
    let _ = published.recv().await.expect("Processed receipt");
    let _ = published.recv().await.expect("Accepted receipt");
    assert!(receiver
        .mark_prompt_started(&dispatch.claim)
        .await
        .expect("durable prompt-start marker"));

    let invocation_id = Uuid::new_v4();
    let lease = dispatch
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            invocation_id,
            CancellationToken::new(),
        )
        .await
        .expect("begin privileged Git operation");
    let marker = published.recv().await.expect("privilege marker");
    let intended = "2".repeat(40);
    lease
        .finish(
            PrivilegedOperationOutcome::Completed,
            Some(PrivilegedGitOperationReceipt {
                schema_version: "buzz.git-operation-receipt.v1".into(),
                invocation_id,
                operation: ProjectGitOperation::Commit,
                session_channel_id: channel.to_string(),
                operation_id: request.common.operation_id.clone(),
                request_event_id: request_event.id.to_hex(),
                worker_pubkey: worker.public_key().to_hex(),
                scope_digest: Some(dispatch.claim.digest.clone()),
                repository: Some(request.common.repository.canonical.clone()),
                branch_ref: Some(format!("refs/heads/{}", request.common.repository.branch)),
                previous_object: Some("1".repeat(40)),
                intended_object: Some(intended.clone()),
                observed_object: Some(intended),
                disposition: PrivilegedGitDisposition::Applied,
            }),
            None,
        )
        .await
        .expect("persist applied receipt");

    let failed = signed_failed(
        &request,
        &request_event.id.to_hex(),
        &worker,
        sponsor(&worker),
        marker.id.to_hex(),
        true,
    );
    let lifecycle = receiver.ledger.lifecycle_store(&dispatch.claim);
    lifecycle
        .stage(failed.clone(), true, marker.id.to_hex())
        .await
        .expect("freeze legacy retryable Failed terminal");

    let error = receiver
        .recover_lifecycle()
        .await
        .expect_err("recovery must reject unsafe retryable Failed replay");
    assert!(error
        .to_string()
        .contains("refusing to replay retryable Failed after 1 applied Git operation"));
    let (_, pending, terminal) = lifecycle.snapshot().await.expect("pending lifecycle");
    assert_eq!(
        pending.expect("unsafe terminal remains pending").id,
        failed.id
    );
    assert!(!terminal);
    assert!(
        published.try_recv().is_err(),
        "unsafe terminal was not sent"
    );

    server.abort();
    drop(receiver);
    std::fs::remove_dir_all(ledger_root).ok();
}

#[tokio::test]
async fn recovery_replays_exact_indeterminate_across_an_ambiguous_git_receipt() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let (request, request_event) = fixture(
        &requester,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "safe-ambiguous-replay",
        "work",
    );
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
    let receiver = JobReceiver::for_test(
        context(&worker),
        worker.clone(),
        rest,
        sponsor(&worker),
        grants_with_git_operations(&request, &["commit"]),
        ledger_root.clone(),
    );
    let HandleOutcome::Dispatch(dispatch) = receiver
        .handle_request(channel, request_event.clone(), Some(&project(&request)))
        .await
        .expect("claim request")
    else {
        panic!("request should dispatch")
    };
    let _ = published.recv().await.expect("Processed receipt");
    let _ = published.recv().await.expect("Accepted receipt");
    assert!(receiver
        .mark_prompt_started(&dispatch.claim)
        .await
        .expect("durable prompt-start marker"));
    let lease = dispatch
        .privilege
        .begin(
            ProjectGitOperation::Commit,
            Uuid::new_v4(),
            CancellationToken::new(),
        )
        .await
        .expect("begin privileged Git operation");
    let marker = published.recv().await.expect("privilege marker");
    assert!(lease
        .finish(PrivilegedOperationOutcome::Failed, None, None)
        .await
        .is_err());
    let indeterminate = signed_indeterminate(
        &request,
        &request_event.id.to_hex(),
        &worker,
        sponsor(&worker),
        marker.id.to_hex(),
        "ambiguous_git_operation",
    );
    let lifecycle = receiver.ledger.lifecycle_store(&dispatch.claim);
    lifecycle
        .stage(indeterminate.clone(), true, marker.id.to_hex())
        .await
        .expect("freeze safe Indeterminate terminal");

    receiver
        .recover_lifecycle()
        .await
        .expect("replay safe Indeterminate terminal");
    assert_eq!(
        published.recv().await.expect("replayed terminal").id,
        indeterminate.id
    );
    let (_, pending, terminal) = lifecycle.snapshot().await.expect("terminal lifecycle");
    assert!(pending.is_none());
    assert!(terminal);

    server.abort();
    drop(receiver);
    std::fs::remove_dir_all(ledger_root).ok();
}

#[tokio::test]
async fn retry_replays_exact_indeterminate_when_the_git_journal_is_missing() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let (request, request_event) = fixture(
        &requester,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "safe-missing-journal-replay",
        "work",
    );
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
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
    let _ = published.recv().await.expect("Processed receipt");
    let _ = published.recv().await.expect("Accepted receipt");
    assert!(receiver
        .mark_prompt_started(&dispatch.claim)
        .await
        .expect("durable prompt-start marker"));
    let lifecycle = receiver.ledger.lifecycle_store(&dispatch.claim);
    let lock_path = lifecycle.privilege_lock_path();
    let journal_path = lock_path.parent().expect("journal parent").join(format!(
        "{}.git-receipts.json",
        lock_path
            .file_name()
            .expect("lock file name")
            .to_string_lossy()
    ));
    std::fs::remove_file(journal_path).expect("remove journal sentinel");
    let head = lifecycle.snapshot().await.expect("active lifecycle").0;
    let indeterminate = signed_indeterminate(
        &request,
        &request_event.id.to_hex(),
        &worker,
        sponsor(&worker),
        head.clone(),
        "git_receipt_journal_unavailable",
    );
    lifecycle
        .stage(indeterminate.clone(), true, head)
        .await
        .expect("freeze safe Indeterminate terminal");

    receiver
        .retry_outboxes()
        .await
        .expect("retry safe Indeterminate terminal");
    assert_eq!(
        published.recv().await.expect("retried terminal").id,
        indeterminate.id
    );
    let (_, pending, terminal) = lifecycle.snapshot().await.expect("terminal lifecycle");
    assert!(pending.is_none());
    assert!(terminal);

    server.abort();
    drop(receiver);
    std::fs::remove_dir_all(ledger_root).ok();
}
