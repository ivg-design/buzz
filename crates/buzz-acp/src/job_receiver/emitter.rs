use std::sync::Arc;

use buzz_core::job::{
    build_job_tags, JobAccepted, JobClaim, JobClaimStatus, JobCommon, JobControl, JobControlAction,
    JobError, JobErrorOutcome, JobEvent, JobFollowup, JobProgress, JobProgressStatus, JobRequest,
    JobResult, JobSponsor, JobSuccessOutcome,
};
use nostr::{Event, EventBuilder, Keys, Kind};
use thiserror::Error;
use tokio::sync::Mutex;

use super::lifecycle::{LifecycleError, LifecycleStore};
use super::outcome::TerminalDisposition;
use crate::relay::{RelayError, RestClient};

#[derive(Debug, Error)]
pub enum EmitError {
    #[error("invalid job event: {0}")]
    Protocol(String),
    #[error("signing job event failed: {0}")]
    Signing(String),
    #[error(transparent)]
    Relay(#[from] RelayError),
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
}

#[derive(Debug, Clone)]
pub struct FrozenReceipts {
    pub processed: Event,
    pub accepted: Event,
}

pub fn build_claim_receipts(
    request: &JobRequest,
    request_event_id: &str,
    scope_digest: &str,
    keys: &Keys,
    sponsor: &JobSponsor,
) -> Result<FrozenReceipts, EmitError> {
    let response_common = response_common(request, keys, sponsor);
    let processed_body = JobEvent::Accepted(JobAccepted {
        followup: JobFollowup {
            common: response_common.clone(),
            request_event_id: request_event_id.into(),
            prior_event_id: None,
        },
        claim: JobClaim {
            status: JobClaimStatus::Processed,
            scope_digest: scope_digest.into(),
            reason: None,
        },
    });
    let processed = sign_job(processed_body, keys)?;
    let accepted_body = JobEvent::Accepted(JobAccepted {
        followup: JobFollowup {
            common: response_common,
            request_event_id: request_event_id.into(),
            prior_event_id: Some(processed.id.to_hex()),
        },
        claim: JobClaim {
            status: JobClaimStatus::Accepted,
            scope_digest: scope_digest.into(),
            reason: None,
        },
    });
    let accepted = sign_job(accepted_body, keys)?;
    Ok(FrozenReceipts {
        processed,
        accepted,
    })
}

fn response_common(request: &JobRequest, keys: &Keys, sponsor: &JobSponsor) -> JobCommon {
    let mut common = request.common.clone();
    common.sender_pubkey = keys.public_key().to_hex();
    common.recipient_pubkey = request.common.sender_pubkey.clone();
    common.sponsor = sponsor.clone();
    common
}

fn sign_job(job: JobEvent, keys: &Keys) -> Result<Event, EmitError> {
    let kind = match &job {
        JobEvent::Request(_) => buzz_core::kind::KIND_JOB_REQUEST,
        JobEvent::Accepted(_) => buzz_core::kind::KIND_JOB_ACCEPTED,
        JobEvent::Progress(_) => buzz_core::kind::KIND_JOB_PROGRESS,
        JobEvent::Result(_) => buzz_core::kind::KIND_JOB_RESULT,
        JobEvent::Control(_) => buzz_core::kind::KIND_JOB_CANCEL,
        JobEvent::Error(_) => buzz_core::kind::KIND_JOB_ERROR,
    };
    let content = job
        .canonical_json()
        .map_err(|error| EmitError::Protocol(error.to_string()))?;
    let tags = build_job_tags(&job).map_err(|error| EmitError::Protocol(error.to_string()))?;
    let event = EventBuilder::new(Kind::Custom(kind as u16), content)
        .tags(tags)
        .sign_with_keys(keys)
        .map_err(|error| EmitError::Signing(error.to_string()))?;
    JobEvent::parse(&event).map_err(|error| EmitError::Protocol(error.to_string()))?;
    Ok(event)
}

/// Signed lifecycle publisher bound to one accepted request chain.
#[derive(Clone)]
pub struct JobEmitter {
    common: JobCommon,
    request_event_id: String,
    transition_gate: Arc<Mutex<()>>,
    keys: Keys,
    rest: RestClient,
    lifecycle: LifecycleStore,
    capabilities: Vec<String>,
    scope_digest: String,
}

impl JobEmitter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request: &JobRequest,
        request_event_id: String,
        keys: Keys,
        rest: RestClient,
        lifecycle: LifecycleStore,
        capabilities: Vec<String>,
        scope_digest: String,
        sponsor: JobSponsor,
    ) -> Self {
        Self {
            common: response_common(request, &keys, &sponsor),
            request_event_id,
            transition_gate: Arc::new(Mutex::new(())),
            keys,
            rest,
            lifecycle,
            capabilities,
            scope_digest,
        }
    }

    pub fn scope_digest(&self) -> &str {
        &self.scope_digest
    }

    /// Whether a relay-confirmed terminal has already closed this lifecycle.
    pub async fn is_terminal(&self) -> Result<bool, EmitError> {
        let (_, _, terminal) = self.lifecycle.snapshot().await?;
        Ok(terminal)
    }

    pub async fn progress(
        &self,
        status: JobProgressStatus,
        message: String,
        evidence: Vec<String>,
    ) -> Result<String, EmitError> {
        self.publish_followup(false, |followup| {
            JobEvent::Progress(JobProgress {
                followup,
                status,
                message,
                evidence,
            })
        })
        .await
    }

    pub async fn complete(
        &self,
        candidate_sha: Option<String>,
        artifacts: Vec<String>,
        evidence: Vec<String>,
    ) -> Result<String, EmitError> {
        self.publish_followup(true, |followup| {
            JobEvent::Result(JobResult {
                followup,
                outcome: JobSuccessOutcome::Success,
                candidate_sha,
                artifacts,
                evidence,
                capabilities: self.capabilities.clone(),
            })
        })
        .await
    }

    pub async fn fail(
        &self,
        code: String,
        message: String,
        retryable: bool,
    ) -> Result<String, EmitError> {
        self.error(JobErrorOutcome::Failed, code, message, retryable)
            .await
    }

    pub async fn indeterminate(&self, code: String, message: String) -> Result<String, EmitError> {
        self.error(JobErrorOutcome::Indeterminate, code, message, false)
            .await
    }

    async fn error(
        &self,
        outcome: JobErrorOutcome,
        code: String,
        message: String,
        retryable: bool,
    ) -> Result<String, EmitError> {
        self.publish_followup(true, |followup| {
            JobEvent::Error(JobError {
                followup,
                outcome,
                code,
                message,
                retryable,
            })
        })
        .await
    }

    pub async fn control(
        &self,
        action: JobControlAction,
        reason: String,
        handoff_to: Option<String>,
    ) -> Result<String, EmitError> {
        let terminal = matches!(
            action,
            JobControlAction::Cancelled | JobControlAction::Release | JobControlAction::Handoff
        );
        self.publish_followup(terminal, |followup| {
            JobEvent::Control(JobControl {
                followup,
                action,
                reason,
                handoff_to,
            })
        })
        .await
    }

    pub async fn terminal(&self, disposition: TerminalDisposition) -> Result<String, EmitError> {
        match disposition {
            TerminalDisposition::Success {
                candidate_sha,
                artifacts,
                evidence,
            } => self.complete(candidate_sha, artifacts, evidence).await,
            TerminalDisposition::Failed {
                code,
                message,
                retryable,
            } => self.fail(code, message, retryable).await,
            TerminalDisposition::Indeterminate { code, message } => {
                self.indeterminate(code, message).await
            }
        }
    }

    async fn publish_followup<F>(&self, terminal: bool, build: F) -> Result<String, EmitError>
    where
        F: FnOnce(JobFollowup) -> JobEvent,
    {
        let _guard = self.transition_gate.lock().await;
        let (_, pending, _) = self.lifecycle.snapshot().await?;
        if let Some(pending) = pending {
            self.rest.submit_event_confirmed(&pending).await?;
            self.lifecycle.confirm(pending.id.to_hex()).await?;
        }
        let (prior, _, already_terminal) = self.lifecycle.snapshot().await?;
        if already_terminal {
            return Err(EmitError::Protocol("job is already terminal".into()));
        }
        let followup = JobFollowup {
            common: self.common.clone(),
            request_event_id: self.request_event_id.clone(),
            prior_event_id: Some(prior.clone()),
        };
        let event = sign_job(build(followup), &self.keys)?;
        self.lifecycle.stage(event.clone(), terminal, prior).await?;
        self.rest.submit_event_confirmed(&event).await?;
        let event_id = event.id.to_hex();
        self.lifecycle.confirm(event_id.clone()).await?;
        Ok(event_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::job::{JobProject, JobRepository, JobSponsor, JOB_SCHEMA_VERSION};

    fn request(sender: &Keys, receiver: &Keys) -> JobRequest {
        JobRequest {
            common: JobCommon {
                schema_version: JOB_SCHEMA_VERSION.into(),
                operation_id: "31dbb246-bc79-4ddc-aab0-2773f05b5cb2".into(),
                idempotency_key: "idem".into(),
                coordinator_epoch: 1,
                project: JobProject {
                    address: format!("30621:{}:nemo", sender.public_key().to_hex()),
                    home_channel: "3580ca9b-47b4-4af9-b22a-1068778f26c6".into(),
                },
                repository: JobRepository {
                    canonical: "https://github.com/mysteropodes/nemo".into(),
                    github_issue: None,
                    github_pr: None,
                    github_run: None,
                    base_sha: "a".repeat(40),
                    branch: "codex/a2a".into(),
                    worktree_id: "a2a".into(),
                    paths: vec!["src".into()],
                    contracts: vec![],
                },
                sender_pubkey: sender.public_key().to_hex(),
                recipient_pubkey: receiver.public_key().to_hex(),
                sponsor: JobSponsor {
                    pubkey: sender.public_key().to_hex(),
                    github_login: "owner".into(),
                },
                expires_at: "2030-01-01T00:00:00Z".into(),
            },
            capability: "rust".into(),
            summary: "Do work".into(),
            acceptance: vec!["Tests pass".into()],
            supersedes_event_id: None,
        }
    }

    #[tokio::test]
    async fn result_hook_publishes_one_causally_linked_terminal() {
        let requester = Keys::generate();
        let worker = Keys::generate();
        let request = request(&requester, &worker);
        let sponsor = JobSponsor {
            pubkey: worker.public_key().to_hex(),
            github_login: "worker-owner".into(),
        };
        let receipts = build_claim_receipts(
            &request,
            &"f".repeat(64),
            &"a".repeat(64),
            &worker,
            &sponsor,
        )
        .expect("receipts");
        let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
        let root = std::env::temp_dir().join(format!("buzz-emitter-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create state root");
        let lifecycle = LifecycleStore::new(&root, "job");
        lifecycle
            .initialize(receipts.accepted.id.to_hex())
            .await
            .expect("initialize lifecycle");
        let emitter = JobEmitter::new(
            &request,
            "f".repeat(64),
            worker,
            rest,
            lifecycle,
            vec!["rust".into()],
            "a".repeat(64),
            sponsor,
        );
        let progress_id = emitter
            .progress(
                JobProgressStatus::Progress,
                "Worker prompt admitted".into(),
                Vec::new(),
            )
            .await
            .expect("progress");
        let progress = published.recv().await.expect("published progress");
        assert_eq!(progress.id.to_hex(), progress_id);
        assert!(matches!(
            JobEvent::parse(&progress).expect("valid progress"),
            JobEvent::Progress(progress)
                if progress.status == JobProgressStatus::Progress
                    && progress.message == "Worker prompt admitted"
        ));
        let result_id = emitter
            .complete(None, vec![], vec![format!("git:{}", "a".repeat(40))])
            .await
            .expect("result");
        let event = published.recv().await.expect("published");
        assert_eq!(event.id.to_hex(), result_id);
        assert!(matches!(
            JobEvent::parse(&event).expect("valid event"),
            JobEvent::Result(result)
                if result.capabilities == ["rust"]
                    && result.followup.prior_event_id.as_deref() == Some(progress_id.as_str())
        ));
        assert!(emitter.is_terminal().await.expect("terminal snapshot"));
        server.abort();
        std::fs::remove_dir_all(root).ok();
    }
}
