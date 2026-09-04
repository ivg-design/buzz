use buzz_core::job::{
    semantic_request_digest, JobAccepted, JobClaim, JobClaimStatus, JobCommon, JobControl,
    JobControlAction, JobError, JobErrorOutcome, JobEvent, JobFollowup, JobProgress,
    JobProgressStatus, JobProject, JobRepository, JobRequest, JobSponsor, JOB_SCHEMA_VERSION,
    JOB_TERMINAL_AUDIT_GRACE_SECONDS,
};
use chrono::{TimeZone, Utc};
use nostr::Keys;

use super::authority::validate_superseding_request;
use super::gate::{validate_claim_digest, validate_job_time};
use super::history::is_terminal;
use super::lifecycle::{requires_predecessor, validate_predecessor, validate_transition};
use super::project::parse_project_address;

fn common(sender: &Keys, recipient: &Keys) -> JobCommon {
    JobCommon {
        schema_version: JOB_SCHEMA_VERSION.into(),
        operation_id: "31dbb246-bc79-4ddc-aab0-2773f05b5cb2".into(),
        idempotency_key: "relay-test-1".into(),
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
            worktree_id: "relay-test".into(),
            paths: vec!["crates/buzz-relay".into()],
            contracts: vec!["contract:relay-tests".into()],
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

fn request(requester: &Keys, worker: &Keys) -> JobEvent {
    JobEvent::Request(JobRequest {
        common: common(requester, worker),
        capability: "rust".into(),
        summary: "Build the seam".into(),
        acceptance: vec!["tests pass".into()],
        supersedes_event_id: None,
    })
}

fn worker_common(root: &JobEvent, worker: &Keys, requester: &Keys) -> JobCommon {
    let mut common = root.common().clone();
    common.sender_pubkey = worker.public_key().to_hex();
    common.recipient_pubkey = requester.public_key().to_hex();
    common.sponsor = JobSponsor {
        pubkey: worker.public_key().to_hex(),
        github_login: "worker-owner".into(),
    };
    common
}

fn receipt(
    root: &JobEvent,
    common: JobCommon,
    status: JobClaimStatus,
    prior: Option<&str>,
) -> JobEvent {
    let JobEvent::Request(request) = root else {
        unreachable!()
    };
    JobEvent::Accepted(JobAccepted {
        followup: JobFollowup {
            common,
            request_event_id: "a".repeat(64),
            prior_event_id: prior.map(str::to_owned),
        },
        claim: JobClaim {
            status,
            scope_digest: semantic_request_digest(request).expect("digest"),
            reason: (status == JobClaimStatus::Declined).then(|| "unsupported".into()),
        },
    })
}

#[test]
fn project_address_is_strict_and_canonical() {
    let author = "a".repeat(64);
    assert!(parse_project_address(&format!("30621:{author}:nemo")).is_ok());
    assert!(parse_project_address(&format!("30617:{author}:nemo")).is_err());
    assert!(parse_project_address(&format!("30621:{}:nemo", author.to_uppercase())).is_err());
    assert!(parse_project_address(&format!("30621:{author}:")).is_err());
}

#[test]
fn processed_accepted_progress_order_is_enforced() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let root = request(&requester, &worker);
    let common = worker_common(&root, &worker, &requester);
    let processed = receipt(&root, common.clone(), JobClaimStatus::Processed, None);
    let accepted = receipt(
        &root,
        common.clone(),
        JobClaimStatus::Accepted,
        Some(&"b".repeat(64)),
    );
    let progress = JobEvent::Progress(JobProgress {
        followup: JobFollowup {
            common,
            request_event_id: "a".repeat(64),
            prior_event_id: Some("c".repeat(64)),
        },
        status: JobProgressStatus::Progress,
        message: "working".into(),
        evidence: vec![],
    });
    assert!(validate_transition(&processed, &root).is_ok());
    assert!(validate_claim_digest(&processed, &root).is_ok());
    assert!(validate_predecessor(&accepted, &processed, &"a".repeat(64)).is_ok());
    assert!(validate_predecessor(&progress, &accepted, &"a".repeat(64)).is_ok());
    assert!(validate_predecessor(&progress, &processed, &"a".repeat(64)).is_err());
}

#[test]
fn claim_digest_is_receiver_computed_and_decline_is_terminal_root_slot() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let root = request(&requester, &worker);
    let common = worker_common(&root, &worker, &requester);
    let mut processed = receipt(&root, common.clone(), JobClaimStatus::Processed, None);
    let JobEvent::Accepted(body) = &mut processed else {
        unreachable!()
    };
    body.claim.scope_digest = "f".repeat(64);
    assert!(validate_claim_digest(&processed, &root).is_err());

    let declined = receipt(&root, common, JobClaimStatus::Declined, None);
    assert!(validate_transition(&declined, &root).is_ok());
    assert!(validate_claim_digest(&declined, &root).is_ok());
    assert!(is_terminal(&declined));
    assert!(!requires_predecessor(&declined));
}

#[test]
fn cancel_request_requires_worker_quiescence_after_claim() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let root = request(&requester, &worker);
    let processed = receipt(
        &root,
        worker_common(&root, &worker, &requester),
        JobClaimStatus::Processed,
        None,
    );
    let accepted = receipt(
        &root,
        worker_common(&root, &worker, &requester),
        JobClaimStatus::Accepted,
        Some(&"b".repeat(64)),
    );
    let cancel = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common: root.common().clone(),
            request_event_id: "a".repeat(64),
            prior_event_id: Some("c".repeat(64)),
        },
        action: JobControlAction::Cancel,
        reason: "stop".into(),
        handoff_to: None,
    });
    assert!(validate_transition(&cancel, &root).is_ok());
    assert!(validate_predecessor(&cancel, &processed, &"a".repeat(64)).is_ok());
    assert!(validate_predecessor(&cancel, &accepted, &"a".repeat(64)).is_ok());
    assert!(!is_terminal(&cancel));

    let cancelled = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common: worker_common(&root, &worker, &requester),
            request_event_id: "a".repeat(64),
            prior_event_id: Some("d".repeat(64)),
        },
        action: JobControlAction::Cancelled,
        reason: "quiesced".into(),
        handoff_to: None,
    });
    assert!(validate_transition(&cancelled, &root).is_ok());
    assert!(validate_predecessor(&cancelled, &cancel, &"a".repeat(64)).is_ok());
    assert!(is_terminal(&cancelled));

    let root_cancel = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common: root.common().clone(),
            request_event_id: "a".repeat(64),
            prior_event_id: None,
        },
        action: JobControlAction::Cancel,
        reason: "before claim".into(),
        handoff_to: None,
    });
    assert!(is_terminal(&root_cancel));
}

#[test]
fn superseding_handoff_requires_original_signer_target_and_next_epoch() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let target = Keys::generate();
    let root = request(&requester, &worker);
    let handoff = JobEvent::Control(JobControl {
        followup: JobFollowup {
            common: worker_common(&root, &worker, &requester),
            request_event_id: "a".repeat(64),
            prior_event_id: Some("b".repeat(64)),
        },
        action: JobControlAction::Handoff,
        reason: "capability".into(),
        handoff_to: Some(target.public_key().to_hex()),
    });
    let JobEvent::Request(original) = &root else {
        unreachable!()
    };
    let mut next = original.clone();
    next.common.recipient_pubkey = target.public_key().to_hex();
    next.common.coordinator_epoch = 2;
    next.supersedes_event_id = Some("c".repeat(64));
    assert!(validate_superseding_request(&next, &handoff, &root).is_ok());
    next.common.sender_pubkey = worker.public_key().to_hex();
    assert!(validate_superseding_request(&next, &handoff, &root).is_err());
}

#[test]
fn expiry_allows_only_bounded_worker_terminal_audit() {
    let requester = Keys::generate();
    let worker = Keys::generate();
    let root = request(&requester, &worker);
    let normal = receipt(
        &root,
        worker_common(&root, &worker, &requester),
        JobClaimStatus::Processed,
        None,
    );
    let declined = receipt(
        &root,
        worker_common(&root, &worker, &requester),
        JobClaimStatus::Declined,
        None,
    );
    let failed = JobEvent::Error(JobError {
        followup: JobFollowup {
            common: worker_common(&root, &worker, &requester),
            request_event_id: "a".repeat(64),
            prior_event_id: Some("b".repeat(64)),
        },
        outcome: JobErrorOutcome::Indeterminate,
        code: "side_effect_unknown".into(),
        message: "reconcile before retry".into(),
        retryable: false,
    });
    let now = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();
    let expired = Utc.timestamp_opt(1_799_999_900, 0).single().unwrap();
    assert!(validate_job_time(&normal, now.timestamp(), expired, now).is_err());
    assert!(validate_job_time(&declined, now.timestamp(), expired, now).is_ok());
    assert!(validate_job_time(&failed, now.timestamp(), expired, now).is_ok());
    let too_old = Utc
        .timestamp_opt(now.timestamp() - JOB_TERMINAL_AUDIT_GRACE_SECONDS - 1, 0)
        .single()
        .unwrap();
    assert!(validate_job_time(&failed, now.timestamp(), too_old, now).is_err());
    let future = Utc
        .timestamp_opt(now.timestamp() + 3_600, 0)
        .single()
        .unwrap();
    assert!(validate_job_time(&normal, now.timestamp() - 3_600, future, now).is_err());
}
