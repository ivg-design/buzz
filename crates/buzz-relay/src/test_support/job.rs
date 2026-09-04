use std::sync::Arc;

use buzz_core::channel::{ChannelType, ChannelVisibility, MemberRole};
use buzz_core::job::{
    build_job_tags, semantic_request_digest, JobAccepted, JobClaim, JobClaimStatus, JobCommon,
    JobControl, JobControlAction, JobError, JobErrorOutcome, JobEvent, JobFollowup, JobProgress,
    JobProgressStatus, JobProject, JobRepository, JobRequest, JobResult, JobSponsor,
    JobSuccessOutcome, JOB_SCHEMA_VERSION,
};
use buzz_core::kind::{
    KIND_JOB_ACCEPTED, KIND_JOB_CANCEL, KIND_JOB_ERROR, KIND_JOB_PROGRESS, KIND_JOB_REQUEST,
    KIND_JOB_RESULT, KIND_PROJECT,
};
use buzz_core::TenantContext;
use chrono::{Duration, SecondsFormat, Utc};
use nostr::{Event, EventBuilder, Keys, Kind, Tag};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use crate::state::AppState;

pub(crate) const REPOSITORY_URL: &str = "https://github.com/mysteropodes/nemo";

pub(crate) struct JobFixture {
    pub(crate) state: Arc<AppState>,
    pub(crate) tenant: TenantContext,
    pub(crate) requester: Keys,
    pub(crate) worker: Keys,
    pub(crate) channel_id: Uuid,
    pub(crate) project_address: String,
    pub(crate) repository_coordinate: String,
}

impl JobFixture {
    pub(crate) async fn new(max_connections: u32) -> Self {
        let host = format!("a2a-{}.example", Uuid::new_v4().simple());
        let database_url = super::database_url();
        let redis_url = std::env::var("BUZZ_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(&database_url)
            .await
            .expect("connect to A2A test database");
        let db = buzz_db::Db::from_pool(pool.clone());
        let community = db
            .ensure_configured_community(&host)
            .await
            .expect("seed A2A test community")
            .id;
        let tenant = TenantContext::resolved(community, host.clone());

        let requester = Keys::generate();
        let worker = Keys::generate();
        for keys in [&requester, &worker] {
            db.ensure_user(community, &keys.public_key().to_bytes())
                .await
                .expect("seed A2A user");
        }
        db.set_agent_owner(
            community,
            &worker.public_key().to_bytes(),
            &requester.public_key().to_bytes(),
        )
        .await
        .expect("bind worker owner");
        db.add_relay_member(community, &requester.public_key().to_hex(), "owner", None)
            .await
            .expect("seed requester relay membership");

        let channel_id = Uuid::new_v4();
        db.create_channel_with_id(
            community,
            channel_id,
            "A2A test project",
            ChannelType::Stream,
            ChannelVisibility::Private,
            None,
            &requester.public_key().to_bytes(),
            None,
        )
        .await
        .expect("seed project channel");
        db.add_member(
            community,
            channel_id,
            &requester.public_key().to_bytes(),
            MemberRole::Owner,
            Some(&requester.public_key().to_bytes()),
        )
        .await
        .expect("seed requester channel membership");
        db.add_member(
            community,
            channel_id,
            &worker.public_key().to_bytes(),
            MemberRole::Bot,
            Some(&requester.public_key().to_bytes()),
        )
        .await
        .expect("seed worker channel membership");

        let repo_d = format!("nemo-{}", Uuid::new_v4().simple());
        let repository_coordinate = format!("30617:{}:{repo_d}", requester.public_key().to_hex());
        let repository = EventBuilder::new(Kind::Custom(30_617), "Nemo repository")
            .tags(vec![
                Tag::parse(["d", repo_d.as_str()]).expect("repo d tag"),
                Tag::parse(["web", REPOSITORY_URL]).expect("repo web tag"),
                Tag::parse(["clone", REPOSITORY_URL]).expect("repo clone tag"),
            ])
            .sign_with_keys(&requester)
            .expect("sign repository announcement");
        db.replace_parameterized_event(community, &repository, &repo_d, None)
            .await
            .expect("store repository announcement");

        let project_d = format!("nemo-{}", Uuid::new_v4().simple());
        let project_address = format!(
            "{}:{}:{project_d}",
            KIND_PROJECT,
            requester.public_key().to_hex()
        );
        let channel_text = channel_id.to_string();
        let project = EventBuilder::new(Kind::Custom(KIND_PROJECT as u16), "Nemo project")
            .tags(vec![
                Tag::parse(["d", project_d.as_str()]).expect("project d tag"),
                Tag::parse(["buzz-channel", channel_text.as_str()]).expect("project channel tag"),
                Tag::parse(["a", repository_coordinate.as_str()]).expect("project repository tag"),
            ])
            .sign_with_keys(&requester)
            .expect("sign project");
        db.replace_parameterized_event(community, &project, &project_d, None)
            .await
            .expect("store project");

        let mut config = crate::config::Config::from_env().expect("default config loads");
        config.database_url = database_url;
        config.redis_url = redis_url.clone();
        config.relay_url = format!("wss://{host}");
        config.require_auth_token = false;
        config.require_relay_membership = false;
        let redis_pool = deadpool_redis::Config::from_url(&redis_url)
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))
            .expect("test Redis pool");
        let pubsub = Arc::new(
            buzz_pubsub::PubSubManager::new(&redis_url, redis_pool.clone())
                .await
                .expect("test PubSub manager"),
        );
        let audit = buzz_audit::AuditService::new(pool.clone());
        let auth = buzz_auth::AuthService::new(config.auth.clone());
        let search = buzz_search::SearchService::new(pool.clone());
        let workflow_engine = Arc::new(buzz_workflow::WorkflowEngine::new(
            db.clone(),
            buzz_workflow::WorkflowConfig::default(),
        ));
        let media_storage =
            buzz_media::MediaStorage::new(&config.media).expect("test media storage");
        let (state, _audit_shutdown) = AppState::new(
            config,
            db,
            redis_pool,
            audit,
            pubsub,
            auth,
            search,
            workflow_engine,
            Keys::generate(),
            media_storage,
        );
        Self {
            state: Arc::new(state),
            tenant,
            requester,
            worker,
            channel_id,
            project_address,
            repository_coordinate,
        }
    }

    pub(crate) fn request(&self, operation_id: Uuid, idempotency_key: &str) -> Event {
        let job = JobEvent::Request(JobRequest {
            common: self.request_common(operation_id, idempotency_key),
            capability: "rust".into(),
            summary: "Exercise the durable A2A seam".into(),
            acceptance: vec!["Production seam remains single-writer".into()],
            supersedes_event_id: None,
        });
        signed_job_event(&job, &self.requester, KIND_JOB_REQUEST)
    }

    fn request_common(&self, operation_id: Uuid, idempotency_key: &str) -> JobCommon {
        JobCommon {
            schema_version: JOB_SCHEMA_VERSION.into(),
            operation_id: operation_id.to_string(),
            idempotency_key: idempotency_key.into(),
            coordinator_epoch: 1,
            project: JobProject {
                address: self.project_address.clone(),
                home_channel: self.channel_id.to_string(),
            },
            repository: JobRepository {
                canonical: REPOSITORY_URL.into(),
                github_issue: Some("1".into()),
                github_pr: None,
                github_run: None,
                base_sha: "a".repeat(40),
                branch: "codex/a2a-test".into(),
                worktree_id: "a2a-test".into(),
                paths: vec!["crates/buzz-relay".into()],
                contracts: vec!["contract:a2a-postgres".into()],
            },
            sender_pubkey: self.requester.public_key().to_hex(),
            recipient_pubkey: self.worker.public_key().to_hex(),
            sponsor: JobSponsor {
                pubkey: self.requester.public_key().to_hex(),
                github_login: "requester".into(),
            },
            expires_at: (Utc::now() + Duration::minutes(10))
                .to_rfc3339_opts(SecondsFormat::Secs, true),
        }
    }

    fn worker_common(&self, request: &Event) -> JobCommon {
        let mut common = JobEvent::parse(request)
            .expect("parse fixture request")
            .common()
            .clone();
        common.sender_pubkey = self.worker.public_key().to_hex();
        common.recipient_pubkey = self.requester.public_key().to_hex();
        common.sponsor = JobSponsor {
            pubkey: self.requester.public_key().to_hex(),
            github_login: "requester".into(),
        };
        common
    }

    pub(crate) fn processed(&self, request: &Event) -> Event {
        let JobEvent::Request(request_body) = JobEvent::parse(request).expect("parse request")
        else {
            unreachable!()
        };
        let job = JobEvent::Accepted(JobAccepted {
            followup: JobFollowup {
                common: self.worker_common(request),
                request_event_id: request.id.to_hex(),
                prior_event_id: None,
            },
            claim: JobClaim {
                status: JobClaimStatus::Processed,
                scope_digest: semantic_request_digest(&request_body).expect("request digest"),
                reason: None,
            },
        });
        signed_job_event(&job, &self.worker, KIND_JOB_ACCEPTED)
    }

    pub(crate) fn accepted(&self, request: &Event, processed: &Event) -> Event {
        let JobEvent::Request(request_body) = JobEvent::parse(request).expect("parse request")
        else {
            unreachable!()
        };
        let job = JobEvent::Accepted(JobAccepted {
            followup: JobFollowup {
                common: self.worker_common(request),
                request_event_id: request.id.to_hex(),
                prior_event_id: Some(processed.id.to_hex()),
            },
            claim: JobClaim {
                status: JobClaimStatus::Accepted,
                scope_digest: semantic_request_digest(&request_body).expect("request digest"),
                reason: None,
            },
        });
        signed_job_event(&job, &self.worker, KIND_JOB_ACCEPTED)
    }

    pub(crate) fn progress(&self, request: &Event, prior: &Event, message: &str) -> Event {
        let job = JobEvent::Progress(JobProgress {
            followup: JobFollowup {
                common: self.worker_common(request),
                request_event_id: request.id.to_hex(),
                prior_event_id: Some(prior.id.to_hex()),
            },
            status: JobProgressStatus::Progress,
            message: message.into(),
            evidence: vec![],
        });
        signed_job_event(&job, &self.worker, KIND_JOB_PROGRESS)
    }

    pub(crate) fn result(&self, request: &Event, prior: &Event) -> Event {
        let job = JobEvent::Result(JobResult {
            followup: JobFollowup {
                common: self.worker_common(request),
                request_event_id: request.id.to_hex(),
                prior_event_id: Some(prior.id.to_hex()),
            },
            outcome: JobSuccessOutcome::Success,
            candidate_sha: None,
            artifacts: vec![],
            evidence: vec![],
            capabilities: vec![],
        });
        signed_job_event(&job, &self.worker, KIND_JOB_RESULT)
    }

    pub(crate) fn error(
        &self,
        request: &Event,
        prior: &Event,
        outcome: JobErrorOutcome,
        code: &str,
        message: &str,
    ) -> Event {
        let job = JobEvent::Error(JobError {
            followup: JobFollowup {
                common: self.worker_common(request),
                request_event_id: request.id.to_hex(),
                prior_event_id: Some(prior.id.to_hex()),
            },
            outcome,
            code: code.into(),
            message: message.into(),
            retryable: false,
        });
        signed_job_event(&job, &self.worker, KIND_JOB_ERROR)
    }

    pub(crate) fn cancel(&self, request: &Event, prior: &Event) -> Event {
        let job = JobEvent::Control(JobControl {
            followup: JobFollowup {
                common: self.request_common_from_event(request),
                request_event_id: request.id.to_hex(),
                prior_event_id: Some(prior.id.to_hex()),
            },
            action: JobControlAction::Cancel,
            reason: "stop".into(),
            handoff_to: None,
        });
        signed_job_event(&job, &self.requester, KIND_JOB_CANCEL)
    }

    pub(crate) fn cancelled(&self, request: &Event, prior: &Event) -> Event {
        let job = JobEvent::Control(JobControl {
            followup: JobFollowup {
                common: self.worker_common(request),
                request_event_id: request.id.to_hex(),
                prior_event_id: Some(prior.id.to_hex()),
            },
            action: JobControlAction::Cancelled,
            reason: "quiesced".into(),
            handoff_to: None,
        });
        signed_job_event(&job, &self.worker, KIND_JOB_CANCEL)
    }

    fn request_common_from_event(&self, request: &Event) -> JobCommon {
        JobEvent::parse(request)
            .expect("parse fixture request")
            .common()
            .clone()
    }
}

pub(crate) fn signed_job_event(job: &JobEvent, keys: &Keys, kind: u32) -> Event {
    EventBuilder::new(
        Kind::Custom(kind as u16),
        job.canonical_json().expect("canonical job JSON"),
    )
    .tags(build_job_tags(job).expect("canonical job tags"))
    .sign_with_keys(keys)
    .expect("sign job event")
}

pub(crate) async fn persist(
    fixture: &JobFixture,
    event: &Event,
) -> Result<bool, crate::handlers::job::JobAuthError> {
    let mut validated =
        crate::handlers::job::validate_job_event(&fixture.tenant, &fixture.state, event).await?;
    let (_, inserted) = validated
        .insert_event(&fixture.tenant, event, fixture.channel_id)
        .await?;
    validated.commit().await?;
    Ok(inserted)
}
