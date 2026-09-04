use buzz_core::job::{
    build_job_tags, semantic_request_digest, JobAccepted, JobClaim, JobClaimStatus, JobCommon,
    JobControl, JobControlAction, JobError, JobErrorOutcome, JobEvent, JobFollowup, JobProgress,
    JobProgressStatus, JobProject, JobRepository, JobRequest, JobResult, JobSponsor,
    JobSuccessOutcome, JOB_SCHEMA_VERSION,
};
use clap::CommandFactory;
use nostr::{Event, EventBuilder, Keys, Kind, Timestamp};
use serde_json::{json, Value};

use super::projection::project;
use super::publish::{control_name, ensure_delivered, job_kind, same_transition_slot};
use super::query::{decode_cursor, encode_cursor, history_cursor};
use super::CLI_RESULT_SCHEMA_VERSION;

const TS: u64 = 1_800_000_000;

fn common(sender: &Keys, recipient: &Keys) -> JobCommon {
    JobCommon {
        schema_version: JOB_SCHEMA_VERSION.into(),
        operation_id: "31dbb246-bc79-4ddc-aab0-2773f05b5cb2".into(),
        idempotency_key: "cli-test-1".into(),
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
            worktree_id: "cli-test".into(),
            paths: vec!["crates/buzz-cli".into()],
            contracts: vec!["contract:cargo-test-buzz-cli".into()],
        },
        sender_pubkey: sender.public_key().to_hex(),
        recipient_pubkey: recipient.public_key().to_hex(),
        sponsor: JobSponsor {
            pubkey: sender.public_key().to_hex(),
            github_login: "owner".into(),
        },
        expires_at: "2030-01-01T00:00:00Z".into(),
    }
}

fn response_common(root: &JobCommon, worker: &Keys, requester: &Keys) -> JobCommon {
    let mut common = root.clone();
    common.sender_pubkey = worker.public_key().to_hex();
    common.recipient_pubkey = requester.public_key().to_hex();
    common.sponsor.pubkey = worker.public_key().to_hex();
    common
}

fn sign(job: &JobEvent, keys: &Keys) -> Event {
    EventBuilder::new(
        Kind::Custom(job_kind(job) as u16),
        job.canonical_json().expect("canonical job"),
    )
    .tags(build_job_tags(job).expect("job tags"))
    .custom_created_at(Timestamp::from(TS))
    .sign_with_keys(keys)
    .expect("signed job")
}

fn chain() -> Vec<Event> {
    let requester = Keys::parse("0000000000000000000000000000000000000000000000000000000000000011")
        .expect("requester");
    let worker = Keys::parse("0000000000000000000000000000000000000000000000000000000000000012")
        .expect("worker");
    let request_body = JobRequest {
        common: common(&requester, &worker),
        capability: "rust".into(),
        summary: "Build the seam".into(),
        acceptance: vec!["tests pass".into()],
        supersedes_event_id: None,
    };
    let digest = semantic_request_digest(&request_body).expect("digest");
    let request_job = JobEvent::Request(request_body);
    let request = sign(&request_job, &requester);
    let followup_common = response_common(request_job.common(), &worker, &requester);
    let processed_job = JobEvent::Accepted(JobAccepted {
        followup: JobFollowup {
            common: followup_common.clone(),
            request_event_id: request.id.to_hex(),
            prior_event_id: None,
        },
        claim: JobClaim {
            status: JobClaimStatus::Processed,
            scope_digest: digest.clone(),
            reason: None,
        },
    });
    let processed = sign(&processed_job, &worker);
    let accepted_job = JobEvent::Accepted(JobAccepted {
        followup: JobFollowup {
            common: followup_common.clone(),
            request_event_id: request.id.to_hex(),
            prior_event_id: Some(processed.id.to_hex()),
        },
        claim: JobClaim {
            status: JobClaimStatus::Accepted,
            scope_digest: digest,
            reason: None,
        },
    });
    let accepted = sign(&accepted_job, &worker);
    let progress_job = JobEvent::Progress(JobProgress {
        followup: JobFollowup {
            common: followup_common.clone(),
            request_event_id: request.id.to_hex(),
            prior_event_id: Some(accepted.id.to_hex()),
        },
        status: JobProgressStatus::Progress,
        message: "working".into(),
        evidence: vec!["contract:unit".into()],
    });
    let progress = sign(&progress_job, &worker);
    let result_job = JobEvent::Result(JobResult {
        followup: JobFollowup {
            common: followup_common,
            request_event_id: request.id.to_hex(),
            prior_event_id: Some(progress.id.to_hex()),
        },
        outcome: JobSuccessOutcome::Success,
        candidate_sha: Some("b".repeat(40)),
        artifacts: vec![format!("git:{}", "b".repeat(40))],
        evidence: vec!["contract:unit".into()],
        capabilities: Vec::new(),
    });
    let result = sign(&result_job, &worker);
    vec![request, processed, accepted, progress, result]
}

#[test]
fn delivery_output_and_lifecycle_names_are_distinct() {
    assert_eq!(control_name(JobControlAction::Cancel), "cancel");
    assert_eq!(control_name(JobControlAction::Cancelled), "cancelled");
    assert_eq!(control_name(JobControlAction::Release), "release");
    assert_eq!(control_name(JobControlAction::Handoff), "handoff");
    assert_ne!("stored", "completed");
}

#[test]
fn relay_ack_must_accept_the_exact_event_id() {
    let id = "a".repeat(64);
    assert!(ensure_delivered(&json!({"accepted": true, "event_id": id}).to_string(), &id).is_ok());
    assert!(ensure_delivered(
        &json!({"accepted": false, "event_id": id, "message":"no"}).to_string(),
        &id
    )
    .is_err());
    assert!(ensure_delivered(
        &json!({"accepted": true, "event_id": "b".repeat(64)}).to_string(),
        &id
    )
    .is_err());
    assert!(ensure_delivered("not-json", &id).is_err());
}

#[test]
fn transition_slots_do_not_conflate_ordered_milestones() {
    let parsed: Vec<JobEvent> = chain()
        .iter()
        .map(|event| JobEvent::parse(event).expect("job"))
        .collect();
    assert!(!same_transition_slot(&parsed[1], &parsed[2]));
    assert!(!same_transition_slot(&parsed[3], &parsed[4]));
    assert!(same_transition_slot(&parsed[3], &parsed[3]));
}

#[test]
fn graph_reducer_is_order_independent_with_same_second_events() {
    let expected = chain();
    let mut shuffled = expected.clone();
    shuffled.swap(0, 4);
    shuffled.swap(1, 3);
    let jobs = project(shuffled).expect("projection");
    assert_eq!(jobs.len(), 1);
    let job = &jobs[0];
    assert_eq!(job.lifecycle.state, "completed");
    assert!(!job.lifecycle.conflict);
    let expected_ids: Vec<String> = expected.iter().map(|event| event.id.to_hex()).collect();
    assert_eq!(job.event_ids, expected_ids);
    assert_eq!(
        job.events
            .iter()
            .map(|event| event.id.to_hex())
            .collect::<Vec<_>>(),
        job.event_ids
    );
    for milestone in [
        job.lifecycle.processed.as_ref(),
        job.lifecycle.accepted.as_ref(),
        job.lifecycle.completed.as_ref(),
        job.lifecycle.terminal.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        assert!(job.event_ids.contains(&milestone.event_id));
    }
}

#[test]
fn reducer_surfaces_sibling_fork_instead_of_timestamp_ordering() {
    let mut events = chain();
    let parsed = JobEvent::parse(&events[3]).expect("progress");
    let JobEvent::Progress(mut progress) = parsed else {
        unreachable!()
    };
    progress.message = "sibling".into();
    let worker = Keys::parse("0000000000000000000000000000000000000000000000000000000000000012")
        .expect("worker");
    events.push(sign(&JobEvent::Progress(progress), &worker));
    let job = project(events).expect("projection").pop().expect("job");
    assert_eq!(job.lifecycle.state, "conflict");
    assert!(job.lifecycle.conflict);
    assert!(job
        .lifecycle
        .conflicts
        .iter()
        .any(|conflict| conflict.starts_with("multiple_successors:")));
}

#[test]
fn root_and_claimed_cancel_states_are_distinct() {
    let base = chain();
    let request = &base[0];
    let requester = Keys::parse("0000000000000000000000000000000000000000000000000000000000000011")
        .expect("requester");
    let worker = Keys::parse("0000000000000000000000000000000000000000000000000000000000000012")
        .expect("worker");
    let root_job = JobEvent::parse(request).expect("request");
    let root_cancel = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common: root_job.common().clone(),
            request_event_id: request.id.to_hex(),
            prior_event_id: None,
        },
        action: JobControlAction::Cancel,
        reason: "before claim".into(),
        handoff_to: None,
    });
    let root_projection = project(vec![request.clone(), sign(&root_cancel, &requester)])
        .expect("root cancel projection")
        .pop()
        .expect("job");
    assert_eq!(root_projection.lifecycle.state, "cancelled");
    assert!(root_projection.lifecycle.terminal.is_some());

    let accepted = &base[2];
    let cancel_job = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common: root_job.common().clone(),
            request_event_id: request.id.to_hex(),
            prior_event_id: Some(accepted.id.to_hex()),
        },
        action: JobControlAction::Cancel,
        reason: "stop".into(),
        handoff_to: None,
    });
    let cancel = sign(&cancel_job, &requester);
    let pending = project(vec![
        base[0].clone(),
        base[1].clone(),
        base[2].clone(),
        cancel.clone(),
    ])
    .expect("cancel request")
    .pop()
    .expect("job");
    assert_eq!(pending.lifecycle.state, "cancel_requested");
    assert!(pending.lifecycle.terminal.is_none());

    let response = response_common(root_job.common(), &worker, &requester);
    let ack = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common: response,
            request_event_id: request.id.to_hex(),
            prior_event_id: Some(cancel.id.to_hex()),
        },
        action: JobControlAction::Cancelled,
        reason: "quiesced".into(),
        handoff_to: None,
    });
    let done = project(vec![
        base[0].clone(),
        base[1].clone(),
        base[2].clone(),
        cancel,
        sign(&ack, &worker),
    ])
    .expect("cancelled projection")
    .pop()
    .expect("job");
    assert_eq!(done.lifecycle.state, "cancelled");
    assert!(done.lifecycle.terminal.is_some());
}

#[test]
fn cancel_requested_accepts_only_non_retryable_indeterminate_worker_error() {
    let base = chain();
    let request = &base[0];
    let requester = Keys::parse("0000000000000000000000000000000000000000000000000000000000000011")
        .expect("requester");
    let worker = Keys::parse("0000000000000000000000000000000000000000000000000000000000000012")
        .expect("worker");
    let root_job = JobEvent::parse(request).expect("request");
    let cancel_job = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common: root_job.common().clone(),
            request_event_id: request.id.to_hex(),
            prior_event_id: Some(base[2].id.to_hex()),
        },
        action: JobControlAction::Cancel,
        reason: "stop".into(),
        handoff_to: None,
    });
    let cancel = sign(&cancel_job, &requester);
    let error_job = JobEvent::Error(JobError {
        followup: JobFollowup {
            common: response_common(root_job.common(), &worker, &requester),
            request_event_id: request.id.to_hex(),
            prior_event_id: Some(cancel.id.to_hex()),
        },
        outcome: JobErrorOutcome::Indeterminate,
        code: "cancel_after_applied_git_operation".into(),
        message: "repository state requires reconciliation".into(),
        retryable: false,
    });
    let error = sign(&error_job, &worker);
    let projection = project(vec![
        base[0].clone(),
        base[1].clone(),
        base[2].clone(),
        cancel,
        error,
    ])
    .expect("cancel indeterminate projection")
    .pop()
    .expect("job");
    assert_eq!(projection.lifecycle.state, "indeterminate");
    assert!(!projection.lifecycle.conflict);
    assert_eq!(
        projection.lifecycle.terminal.as_ref().unwrap().event_id,
        projection.event_ids[4]
    );
}

#[test]
fn cancel_requested_rejects_failed_worker_error() {
    let base = chain();
    let request = &base[0];
    let requester = Keys::parse("0000000000000000000000000000000000000000000000000000000000000011")
        .expect("requester");
    let worker = Keys::parse("0000000000000000000000000000000000000000000000000000000000000012")
        .expect("worker");
    let root_job = JobEvent::parse(request).expect("request");
    let cancel = sign(
        &JobEvent::Control(JobControl {
            followup: JobFollowup {
                common: root_job.common().clone(),
                request_event_id: request.id.to_hex(),
                prior_event_id: Some(base[2].id.to_hex()),
            },
            action: JobControlAction::Cancel,
            reason: "stop".into(),
            handoff_to: None,
        }),
        &requester,
    );
    let error = sign(
        &JobEvent::Error(JobError {
            followup: JobFollowup {
                common: response_common(root_job.common(), &worker, &requester),
                request_event_id: request.id.to_hex(),
                prior_event_id: Some(cancel.id.to_hex()),
            },
            outcome: JobErrorOutcome::Failed,
            code: "cancelled_failure".into(),
            message: "known failure".into(),
            retryable: false,
        }),
        &worker,
    );
    let projection = project(vec![
        base[0].clone(),
        base[1].clone(),
        base[2].clone(),
        cancel,
        error,
    ])
    .expect("projection")
    .pop()
    .expect("job");
    assert_eq!(projection.lifecycle.state, "conflict");
    assert!(projection.lifecycle.conflict);
    assert!(projection
        .lifecycle
        .conflicts
        .iter()
        .any(|conflict| conflict.starts_with("invalid_transition:")));
}

#[test]
fn cancel_requested_rejects_retryable_indeterminate_worker_error() {
    let base = chain();
    let request = &base[0];
    let requester = Keys::parse("0000000000000000000000000000000000000000000000000000000000000011")
        .expect("requester");
    let worker = Keys::parse("0000000000000000000000000000000000000000000000000000000000000012")
        .expect("worker");
    let root_job = JobEvent::parse(request).expect("request");
    let cancel = sign(
        &JobEvent::Control(JobControl {
            followup: JobFollowup {
                common: root_job.common().clone(),
                request_event_id: request.id.to_hex(),
                prior_event_id: Some(base[2].id.to_hex()),
            },
            action: JobControlAction::Cancel,
            reason: "stop".into(),
            handoff_to: None,
        }),
        &requester,
    );
    let error = sign(
        &JobEvent::Error(JobError {
            followup: JobFollowup {
                common: response_common(root_job.common(), &worker, &requester),
                request_event_id: request.id.to_hex(),
                prior_event_id: Some(cancel.id.to_hex()),
            },
            outcome: JobErrorOutcome::Indeterminate,
            code: "retryable_unknown".into(),
            message: "retry would be unsafe".into(),
            retryable: true,
        }),
        &worker,
    );
    let error = project(vec![
        base[0].clone(),
        base[1].clone(),
        base[2].clone(),
        cancel,
        error,
    ])
    .expect_err("retryable indeterminate error must be rejected by the job schema");
    assert!(error
        .to_string()
        .contains("indeterminate errors require retryable=false"));
}

#[test]
fn cursor_is_opaque_complete_history_fingerprint() {
    let events = chain();
    let cursor = history_cursor(&events).expect("cursor");
    assert_eq!(
        decode_cursor(&encode_cursor(&cursor).expect("encode")).unwrap(),
        cursor
    );
    assert!(decode_cursor("not-a-cursor").is_err());
    let mut extended = events;
    extended.push(extended[0].clone());
    assert_ne!(history_cursor(&extended).unwrap(), cursor);
}

#[test]
fn jobs_help_and_machine_contract_freeze_the_surface() {
    let mut command = crate::Cli::command();
    let jobs = command
        .find_subcommand_mut("jobs")
        .expect("jobs command")
        .render_long_help()
        .to_string();
    for name in [
        "submit",
        "list",
        "get",
        "accept",
        "progress",
        "complete",
        "fail",
        "cancel",
        "acknowledge-cancel",
        "release",
        "handoff",
    ] {
        assert!(jobs.contains(name), "jobs help omitted {name}");
    }
    let contract: Value = serde_json::from_str(include_str!("../../../cli-contract-v1.json"))
        .expect("machine contract");
    assert_eq!(contract["schema_version"], "buzz.cli-contract.v1");
    assert_eq!(contract["result_schema_version"], CLI_RESULT_SCHEMA_VERSION);
    let paths = contract["commands"].as_array().expect("commands");
    assert!(paths
        .iter()
        .any(|entry| { entry["path"] == Value::String("buzz jobs acknowledge-cancel".into()) }));
    assert!(contract["outputs"]["capabilities_result"]["agents"].is_string());
    assert!(contract["outputs"]["job_projection"]["events"].is_string());
}
