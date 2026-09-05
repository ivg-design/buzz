use buzz_core::job::{
    build_job_tags, semantic_request_digest, JobAccepted, JobClaim, JobClaimStatus, JobCommon,
    JobEvent, JobFollowup, JobProgress, JobProgressStatus, JobProject, JobRepository, JobRequest,
    JobResult, JobSponsor, JobSuccessOutcome, JOB_SCHEMA_VERSION,
};
use buzz_core::kind::{KIND_JOB_ACCEPTED, KIND_JOB_PROGRESS, KIND_JOB_REQUEST, KIND_JOB_RESULT};
use buzz_core::CommunityId;
use nostr::{Event, EventBuilder, Keys, Kind, Timestamp};
use sqlx::PgPool;
use uuid::Uuid;

use crate::relay_admin_actions::{self, ClaimResult, LeaseResult};
use crate::Db;

async fn setup() -> (Db, CommunityId) {
    let pool = PgPool::connect(&crate::test_support::database_url())
        .await
        .expect("connect to A2A test database");
    let db = Db::from_pool(pool);
    let community_id = Uuid::new_v4();
    sqlx::query("INSERT INTO communities (id, host) VALUES ($1, $2)")
        .bind(community_id)
        .bind(format!("job-delete-{}.example", community_id.simple()))
        .execute(&db.pool)
        .await
        .expect("insert deletion test community");
    (db, CommunityId::from_uuid(community_id))
}

async fn channel(db: &Db, community: CommunityId, creator: &Keys) -> Uuid {
    let channel_id = Uuid::new_v4();
    sqlx::query("INSERT INTO channels (id, community_id, name, created_by) VALUES ($1,$2,$3,$4)")
        .bind(channel_id)
        .bind(community.as_uuid())
        .bind(format!("job-thread-{}", channel_id.simple()))
        .bind(creator.public_key().to_bytes().to_vec())
        .execute(&db.pool)
        .await
        .expect("insert job thread test channel");
    channel_id
}

fn job_common(requester: &Keys, worker: &Keys, channel_id: Uuid, operation_id: Uuid) -> JobCommon {
    JobCommon {
        schema_version: JOB_SCHEMA_VERSION.into(),
        operation_id: operation_id.to_string(),
        idempotency_key: format!("job-thread-{operation_id}"),
        coordinator_epoch: 1,
        project: JobProject {
            address: format!("30621:{}:thread-test", requester.public_key().to_hex()),
            home_channel: channel_id.to_string(),
        },
        conversation: None,
        repository: JobRepository {
            canonical: "https://github.com/example/thread-test".into(),
            github_issue: None,
            github_pr: None,
            github_run: None,
            base_sha: "a".repeat(40),
            branch: "codex/thread-test".into(),
            worktree_id: "job-thread-test".into(),
            paths: vec!["crates/buzz-db".into()],
            contracts: vec!["contract:job-thread-test".into()],
        },
        sender_pubkey: requester.public_key().to_hex(),
        recipient_pubkey: worker.public_key().to_hex(),
        sponsor: JobSponsor {
            pubkey: requester.public_key().to_hex(),
            github_login: "sponsor".into(),
        },
        expires_at: "2030-01-01T00:00:00Z".into(),
    }
}

fn sign_job(job: &JobEvent, signer: &Keys, created_at: u64) -> Event {
    let kind = match job {
        JobEvent::Request(_) => KIND_JOB_REQUEST,
        JobEvent::Accepted(_) => KIND_JOB_ACCEPTED,
        JobEvent::Progress(_) => KIND_JOB_PROGRESS,
        JobEvent::Result(_) => KIND_JOB_RESULT,
        JobEvent::Control(_) | JobEvent::Error(_) => unreachable!("unused test job kind"),
    };
    EventBuilder::new(
        Kind::Custom(kind as u16),
        job.canonical_json().expect("serialize job fixture"),
    )
    .tags(build_job_tags(job).expect("build job fixture tags"))
    .custom_created_at(Timestamp::from(created_at))
    .sign_with_keys(signer)
    .expect("sign job fixture")
}

fn job_sequence(requester: &Keys, worker: &Keys, channel_id: Uuid) -> Vec<(JobEvent, Event)> {
    let operation_id = Uuid::new_v4();
    let request_common = job_common(requester, worker, channel_id, operation_id);
    let request_job = JobEvent::Request(JobRequest {
        common: request_common.clone(),
        capability: "rust".into(),
        title: None,
        origin: None,
        summary: "Index this job as a conversation".into(),
        acceptance: vec!["One flat task thread".into()],
        supersedes_event_id: None,
    });
    let request = sign_job(&request_job, requester, 1_893_456_000);

    let mut followup_common = request_common;
    followup_common.sender_pubkey = worker.public_key().to_hex();
    followup_common.recipient_pubkey = requester.public_key().to_hex();
    let followup = |prior_event_id: Option<String>| JobFollowup {
        common: followup_common.clone(),
        request_event_id: request.id.to_hex(),
        prior_event_id,
    };
    let accepted_job = JobEvent::Accepted(JobAccepted {
        followup: followup(None),
        claim: JobClaim {
            status: JobClaimStatus::Accepted,
            scope_digest: semantic_request_digest(match &request_job {
                JobEvent::Request(body) => body,
                _ => unreachable!("request fixture"),
            })
            .expect("request digest"),
            reason: None,
        },
    });
    let accepted = sign_job(&accepted_job, worker, 1_893_456_001);
    let progress_job = JobEvent::Progress(JobProgress {
        followup: followup(Some(accepted.id.to_hex())),
        status: JobProgressStatus::Progress,
        message: "Thread metadata is being written".into(),
        evidence: vec!["cargo:test".into()],
    });
    let progress = sign_job(&progress_job, worker, 1_893_456_002);
    let result_job = JobEvent::Result(JobResult {
        followup: followup(Some(progress.id.to_hex())),
        outcome: JobSuccessOutcome::Success,
        summary: None,
        candidate_sha: Some("b".repeat(40)),
        artifacts: vec!["crates/buzz-db/src/store/job.rs".into()],
        evidence: vec!["cargo:test".into()],
        capabilities: vec![],
    });
    let result = sign_job(&result_job, worker, 1_893_456_003);
    vec![
        (request_job, request),
        (accepted_job, accepted),
        (progress_job, progress),
        (result_job, result),
    ]
}

async fn insert_protected_job(
    db: &Db,
    community: CommunityId,
    channel_id: Uuid,
    job: &JobEvent,
    event: &Event,
) -> bool {
    let mut lock = db
        .acquire_job_operation_locks(
            community,
            &[format!("operation:{}", job.common().operation_id)],
        )
        .await
        .expect("acquire job insert lock");
    let inserted = lock
        .insert_event(community, event, channel_id, job)
        .await
        .expect("insert protected job event")
        .1;
    lock.commit().await.expect("commit protected job event");
    inserted
}

async fn assert_flat_job_thread(
    db: &Db,
    community: CommunityId,
    channel_id: Uuid,
    events: &[(JobEvent, Event)],
) {
    let root = &events[0].1;
    let window = db
        .get_channel_window(
            community,
            channel_id,
            20,
            None,
            Some(&[
                KIND_JOB_REQUEST,
                KIND_JOB_ACCEPTED,
                KIND_JOB_PROGRESS,
                KIND_JOB_RESULT,
            ]),
        )
        .await
        .expect("load channel window");
    assert_eq!(window.rows.len(), 1, "follow-ups stay out of main channel");
    assert_eq!(window.rows[0].stored_event.event.id, root.id);
    let window_summary = window.rows[0]
        .thread_summary
        .as_ref()
        .expect("request exposes thread summary");
    assert_eq!(window_summary.reply_count, 3);
    assert_eq!(window_summary.descendant_count, 3);

    let replies = db
        .get_thread_replies(community, root.id.as_bytes(), Some(64), 20, None)
        .await
        .expect("load job thread");
    assert_eq!(replies.len(), 3);
    for reply in replies {
        assert_eq!(
            reply.parent_event_id.as_deref(),
            Some(root.id.as_bytes().as_slice())
        );
        assert_eq!(
            reply.root_event_id.as_deref(),
            Some(root.id.as_bytes().as_slice())
        );
        assert_eq!(reply.depth, 1);
        assert!(!reply.broadcast);
    }

    let summary = db
        .get_thread_summary(community, root.id.as_bytes())
        .await
        .expect("load job thread summary")
        .expect("job thread root metadata");
    assert_eq!(summary.reply_count, 3);
    assert_eq!(summary.descendant_count, 3);
}

fn event(kind: u16, label: &str) -> Event {
    EventBuilder::new(Kind::Custom(kind), label)
        .sign_with_keys(&Keys::generate())
        .expect("sign deletion fixture")
}

async fn store(db: &Db, community: CommunityId, event: &Event) {
    assert!(
        db.insert_event(community, event, None)
            .await
            .expect("insert deletion fixture")
            .1
    );
}

async fn assert_live(db: &Db, community: CommunityId, event: &Event) {
    assert!(
        db.get_event_by_id(community, event.id.as_bytes())
            .await
            .expect("load deletion fixture")
            .is_some(),
        "kind {} must remain live",
        event.kind.as_u16()
    );
}

async fn claimed_delete_action(db: &Db, community: CommunityId, target: &Event) -> (Uuid, Uuid) {
    let report_event_id = Keys::generate().public_key().to_bytes().to_vec();
    let reporter = Keys::generate().public_key().to_bytes().to_vec();
    let report_id: Uuid = sqlx::query_scalar(
        "INSERT INTO moderation_reports \
         (community_id, report_event_id, reporter_pubkey, target_kind, target_event_id, report_type) \
         VALUES ($1,$2,$3,'event',$4,'other') RETURNING id",
    )
    .bind(community.as_uuid())
    .bind(report_event_id)
    .bind(reporter)
    .bind(target.id.as_bytes().as_slice())
    .fetch_one(&db.pool)
    .await
    .expect("insert deletion report");
    let actor = Keys::generate().public_key().to_bytes().to_vec();
    let action_id = match relay_admin_actions::claim_report(
        &db.pool,
        community,
        report_id,
        Uuid::new_v4(),
        &actor,
        "operator",
        "delete",
        Some("immutability test"),
        None,
        "resolve:delete",
        "relay_operator",
        None,
        Some(target.id.as_bytes()),
        None,
    )
    .await
    .expect("claim deletion action")
    {
        ClaimResult::Claimed(action) => action.id,
        other => panic!("expected claimed delete action, got {other:?}"),
    };
    assert!(relay_admin_actions::begin_enforcing(&db.pool, action_id)
        .await
        .expect("begin deletion enforcement"));
    let lease = match relay_admin_actions::acquire_action_lease(
        &db.pool,
        action_id,
        chrono::Utc::now() + chrono::Duration::seconds(60),
    )
    .await
    .expect("acquire deletion lease")
    {
        LeaseResult::Acquired(token) => token,
        other => panic!("expected acquired lease, got {other:?}"),
    };
    (action_id, lease)
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn protected_job_insert_builds_one_flat_thread_without_duplicate_counts() {
    let (db, community) = setup().await;
    let requester = Keys::generate();
    let worker = Keys::generate();
    let channel_id = channel(&db, community, &requester).await;
    let events = job_sequence(&requester, &worker, channel_id);

    for (job, event) in &events {
        assert!(insert_protected_job(&db, community, channel_id, job, event).await);
    }
    assert!(
        !insert_protected_job(&db, community, channel_id, &events[2].0, &events[2].1).await,
        "replaying the exact progress event must be idempotent"
    );

    assert_flat_job_thread(&db, community, channel_id, &events).await;
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn generic_soft_delete_paths_preserve_every_job_protocol_kind() {
    let (db, community) = setup().await;
    for kind in 43_001..=43_006 {
        let direct = event(kind, "direct immutable job");
        store(&db, community, &direct).await;
        assert!(!db
            .soft_delete_event(community, direct.id.as_bytes())
            .await
            .expect("direct soft delete"));
        assert_live(&db, community, &direct).await;

        let threaded = event(kind, "threaded immutable job");
        store(&db, community, &threaded).await;
        assert!(!db
            .soft_delete_event_and_update_thread(community, threaded.id.as_bytes(), None, None,)
            .await
            .expect("thread-aware soft delete"));
        assert_live(&db, community, &threaded).await;
    }

    let ordinary = event(1, "ordinary event remains deletable");
    store(&db, community, &ordinary).await;
    assert!(db
        .soft_delete_event(community, ordinary.id.as_bytes())
        .await
        .expect("ordinary event delete"));
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn admin_delete_path_rejects_every_job_protocol_kind_atomically() {
    let (db, community) = setup().await;
    for kind in 43_001..=43_006 {
        let target = event(kind, "admin immutable job");
        store(&db, community, &target).await;
        let (action_id, lease) = claimed_delete_action(&db, community, &target).await;
        let error = db
            .execute_delete_with_marker(
                action_id,
                lease,
                community,
                target.id.as_bytes(),
                None,
                None,
            )
            .await
            .expect_err("admin path must reject job deletion");
        assert!(error
            .to_string()
            .contains("job protocol events are immutable"));
        assert_live(&db, community, &target).await;
        let action = relay_admin_actions::get_action(&db.pool, action_id)
            .await
            .expect("read rejected action")
            .expect("rejected action exists");
        assert_eq!(action.step_marker, None, "rejected delete must be atomic");
    }

    let ordinary = event(1, "ordinary admin target");
    store(&db, community, &ordinary).await;
    let (action_id, lease) = claimed_delete_action(&db, community, &ordinary).await;
    assert!(db
        .execute_delete_with_marker(
            action_id,
            lease,
            community,
            ordinary.id.as_bytes(),
            None,
            None,
        )
        .await
        .expect("ordinary admin delete"));
}
