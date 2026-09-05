use std::sync::Arc;

use buzz_core::job::{
    build_job_tags, JobAccepted, JobClaim, JobClaimStatus, JobCommon, JobControl, JobControlAction,
    JobError, JobErrorOutcome, JobEvent, JobFollowup, JobProgress, JobProgressStatus, JobRequest,
    JobResult, JobSponsor, JobSuccessOutcome,
};
use nostr::{Event, EventBuilder, EventId, Keys, Kind, Tag};
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::human_report::HumanJobReport;
use super::lifecycle::{HumanReportStore, LifecycleError, LifecycleStore};
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

fn validate_task_chat_event(
    event: &Event,
    keys: &Keys,
    channel_id: &str,
    thread_root_id: &str,
    marker: &str,
    marker_event_id: &str,
) -> Result<(), EmitError> {
    event
        .verify()
        .map_err(|error| EmitError::Protocol(format!("invalid frozen report: {error}")))?;
    if event.pubkey != keys.public_key()
        || event.kind != Kind::Custom(9)
        || event.content.is_empty()
        || event.content.len() > 64 * 1024
    {
        return Err(EmitError::Protocol(
            "frozen report does not match the worker identity or message contract".into(),
        ));
    }
    for expected in [
        ["h", channel_id, "", ""],
        ["e", thread_root_id, "", "reply"],
        ["buzz-task", marker, marker_event_id, ""],
    ] {
        let found = event.tags.iter().any(|tag| {
            let values = tag.as_slice();
            values.first().map(String::as_str) == Some(expected[0])
                && values.get(1).map(String::as_str) == Some(expected[1])
                && (expected[2].is_empty()
                    || values.get(2).map(String::as_str) == Some(expected[2]))
                && (expected[3].is_empty()
                    || values.get(3).map(String::as_str) == Some(expected[3]))
        });
        if !found {
            return Err(EmitError::Protocol(
                "frozen report is not bound to the job conversation".into(),
            ));
        }
    }
    if event
        .tags
        .iter()
        .any(|tag| tag.as_slice().first().map(String::as_str) == Some("p"))
    {
        return Err(EmitError::Protocol(
            "task conversation updates must not address another agent".into(),
        ));
    }
    Ok(())
}

fn task_conversation<'a>(
    common: &'a JobCommon,
    legacy_thread_root_id: &'a str,
) -> (&'a str, &'a str) {
    common.conversation.as_ref().map_or(
        (common.project.home_channel.as_str(), legacy_thread_root_id),
        |conversation| {
            (
                conversation.channel_id.as_str(),
                conversation.thread_root_id.as_str(),
            )
        },
    )
}

fn lifecycle_mirror_text(job: &JobEvent) -> String {
    match job {
        JobEvent::Request(_) => "Task created.".into(),
        JobEvent::Accepted(body) => match body.claim.status {
            JobClaimStatus::Processed => "Received the task.".into(),
            JobClaimStatus::Accepted => "Started the task.".into(),
            JobClaimStatus::Declined => "Declined the task.".into(),
        },
        JobEvent::Progress(body) => match body.status {
            JobProgressStatus::Progress => format!("Progress: {}", body.message),
            JobProgressStatus::Blocked => format!("Blocked: {}", body.message),
        },
        JobEvent::Result(_) => "Completed successfully.".into(),
        JobEvent::Control(body) => match body.action {
            JobControlAction::Cancel => format!("Cancellation requested: {}", body.reason),
            JobControlAction::Cancelled => format!("Cancelled: {}", body.reason),
            JobControlAction::Release => format!("Released the task: {}", body.reason),
            JobControlAction::Handoff => format!("Handoff requested: {}", body.reason),
        },
        JobEvent::Error(body) => match body.outcome {
            JobErrorOutcome::Failed => format!("Failed: {}", body.message),
            JobErrorOutcome::Indeterminate => {
                format!("Outcome indeterminate: {}", body.message)
            }
        },
    }
}

fn build_lifecycle_mirror(event: &Event, keys: &Keys) -> Result<Option<Event>, EmitError> {
    let job = JobEvent::parse(event).map_err(|error| EmitError::Protocol(error.to_string()))?;
    let Some(conversation) = &job.common().conversation else {
        return Ok(None);
    };
    if event.pubkey != keys.public_key() {
        return Err(EmitError::Protocol(
            "lifecycle mirror signer does not match the machine event".into(),
        ));
    }
    let machine_event_id = event.id.to_hex();
    let tags = [
        Tag::parse(["h", conversation.channel_id.as_str()]),
        Tag::parse(["e", conversation.thread_root_id.as_str(), "", "reply"]),
        Tag::parse(["buzz-task", "lifecycle", machine_event_id.as_str()]),
    ]
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .map_err(|error| EmitError::Protocol(format!("invalid lifecycle mirror tag: {error}")))?;
    let mirror = EventBuilder::new(Kind::Custom(9), lifecycle_mirror_text(&job))
        .tags(tags)
        .custom_created_at(event.created_at)
        .sign_with_keys(keys)
        .map_err(|error| EmitError::Signing(error.to_string()))?;
    validate_task_chat_event(
        &mirror,
        keys,
        &conversation.channel_id,
        &conversation.thread_root_id,
        "lifecycle",
        &machine_event_id,
    )?;
    Ok(Some(mirror))
}

pub(super) async fn publish_lifecycle_mirror(
    rest: &RestClient,
    event: &Event,
    keys: &Keys,
) -> Result<Option<String>, EmitError> {
    let Some(mirror) = build_lifecycle_mirror(event, keys)? else {
        return Ok(None);
    };
    rest.submit_event_confirmed(&mirror).await?;
    Ok(Some(mirror.id.to_hex()))
}

#[derive(Debug, Clone)]
pub struct FrozenReceipts {
    pub processed: Event,
    pub accepted: Event,
}

pub fn build_declined_receipt(
    request: &JobRequest,
    request_event_id: &str,
    scope_digest: &str,
    keys: &Keys,
    sponsor: &JobSponsor,
    reason: &str,
) -> Result<Event, EmitError> {
    sign_job(
        JobEvent::Accepted(JobAccepted {
            followup: JobFollowup {
                common: response_common(request, keys, sponsor),
                request_event_id: request_event_id.into(),
                prior_event_id: None,
            },
            claim: JobClaim {
                status: JobClaimStatus::Declined,
                scope_digest: scope_digest.into(),
                reason: Some(reason.into()),
            },
        }),
        keys,
    )
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
    report_gate: Arc<Mutex<()>>,
    keys: Keys,
    rest: RestClient,
    lifecycle: LifecycleStore,
    human_report: HumanReportStore,
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
        let human_report = lifecycle.human_report_store(&request_event_id);
        Self {
            common: response_common(request, &keys, &sponsor),
            request_event_id,
            transition_gate: Arc::new(Mutex::new(())),
            report_gate: Arc::new(Mutex::new(())),
            keys,
            rest,
            lifecycle,
            human_report,
            capabilities,
            scope_digest,
        }
    }

    /// Visible conversation bound to this active worker's signed request.
    pub(crate) fn conversation(&self) -> Option<&buzz_core::job::JobConversation> {
        self.common.conversation.as_ref()
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
        summary: String,
        candidate_sha: Option<String>,
        artifacts: Vec<String>,
        evidence: Vec<String>,
    ) -> Result<String, EmitError> {
        self.publish_followup(true, |followup| {
            JobEvent::Result(JobResult {
                followup,
                outcome: JobSuccessOutcome::Success,
                summary: Some(summary),
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
                summary,
                candidate_sha,
                artifacts,
                evidence,
            } => {
                self.complete(summary, candidate_sha, artifacts, evidence)
                    .await
            }
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

    /// Publish or replay the exact worker-signed human report for this task.
    ///
    /// The report has its own outbox and never advances the lifecycle chain.
    /// A confirmed prior report is returned idempotently without signing a
    /// second message.
    pub async fn publish_human_report(&self, report: HumanJobReport) -> Result<String, EmitError> {
        let _guard = self.report_gate.lock().await;
        if let Some(event_id) = self.retry_human_report_outbox_inner().await? {
            return Ok(event_id);
        }
        let (channel, thread_root) = task_conversation(&self.common, &self.request_event_id);
        let channel_id = Uuid::parse_str(channel)
            .map_err(|error| EmitError::Protocol(format!("invalid report channel: {error}")))?;
        let thread_root_id = EventId::from_hex(thread_root)
            .map_err(|error| EmitError::Protocol(format!("invalid report thread: {error}")))?;
        let channel_tag_value = channel_id.to_string();
        let thread_root_hex = thread_root_id.to_hex();
        let tags = [
            Tag::parse(["h", channel_tag_value.as_str()]),
            Tag::parse(["e", thread_root_hex.as_str(), "", "reply"]),
            Tag::parse(["buzz-task", "report", self.request_event_id.as_str()]),
        ]
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| EmitError::Protocol(format!("invalid report tag: {error}")))?;
        let event = EventBuilder::new(Kind::Custom(9), report.content())
            .tags(tags)
            .sign_with_keys(&self.keys)
            .map_err(|error| EmitError::Signing(error.to_string()))?;
        validate_task_chat_event(
            &event,
            &self.keys,
            channel,
            thread_root,
            "report",
            &self.request_event_id,
        )?;
        self.human_report.stage(event.clone()).await?;
        self.rest.submit_event_confirmed(&event).await?;
        let event_id = event.id.to_hex();
        self.human_report.confirm(event_id.clone()).await?;
        Ok(event_id)
    }

    /// Replay a frozen report event after reconnect or process restart.
    pub async fn retry_human_report_outbox(&self) -> Result<Option<String>, EmitError> {
        let _guard = self.report_gate.lock().await;
        self.retry_human_report_outbox_inner().await
    }

    async fn retry_human_report_outbox_inner(&self) -> Result<Option<String>, EmitError> {
        let (pending, confirmed) = self.human_report.snapshot().await?;
        if let Some(event_id) = confirmed {
            return Ok(Some(event_id));
        }
        let Some(event) = pending else {
            return Ok(None);
        };
        let (channel, thread_root) = task_conversation(&self.common, &self.request_event_id);
        validate_task_chat_event(
            &event,
            &self.keys,
            channel,
            thread_root,
            "report",
            &self.request_event_id,
        )?;
        self.rest.submit_event_confirmed(&event).await?;
        let event_id = event.id.to_hex();
        self.human_report.confirm(event_id.clone()).await?;
        Ok(Some(event_id))
    }

    /// Replay a frozen lifecycle event and its deterministic chat mirror.
    pub async fn retry_lifecycle_outbox(&self) -> Result<Option<String>, EmitError> {
        let Some((event, machine_confirmed, conversation_confirmed)) =
            self.lifecycle.pending_delivery().await?
        else {
            return Ok(None);
        };
        let mirror = build_lifecycle_mirror(&event, &self.keys)?;
        if !machine_confirmed {
            self.rest.submit_event_confirmed(&event).await?;
            self.lifecycle.confirm(event.id.to_hex()).await?;
        }
        if let Some(mirror) = mirror {
            if !conversation_confirmed {
                self.rest.submit_event_confirmed(&mirror).await?;
                self.lifecycle
                    .confirm_conversation(event.id.to_hex())
                    .await?;
            }
        }
        Ok(Some(event.id.to_hex()))
    }

    async fn publish_followup<F>(&self, terminal: bool, build: F) -> Result<String, EmitError>
    where
        F: FnOnce(JobFollowup) -> JobEvent,
    {
        let _guard = self.transition_gate.lock().await;
        self.retry_lifecycle_outbox().await?;
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
        let event_id = event.id.to_hex();
        self.retry_lifecycle_outbox().await?;
        Ok(event_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::job::{
        JobConversation, JobProject, JobRepository, JobSponsor, JOB_SCHEMA_VERSION,
    };

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
                conversation: None,
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
            title: None,
            origin: None,
            summary: "Do work".into(),
            acceptance: vec!["Tests pass".into()],
            supersedes_event_id: None,
        }
    }

    fn request_with_conversation(sender: &Keys, receiver: &Keys) -> JobRequest {
        let mut request = request(sender, receiver);
        request.common.conversation = Some(JobConversation {
            channel_id: "6df3d942-e730-4b1c-9742-184bf292fa71".into(),
            thread_root_id: "8".repeat(64),
        });
        request
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
            .complete(
                "Implemented and verified".into(),
                None,
                vec![],
                vec![format!("git:{}", "a".repeat(40))],
            )
            .await
            .expect("result");
        let event = published.recv().await.expect("published");
        assert_eq!(event.id.to_hex(), result_id);
        assert!(matches!(
            JobEvent::parse(&event).expect("valid event"),
            JobEvent::Result(result)
                if result.capabilities == ["rust"]
                    && result.summary.as_deref() == Some("Implemented and verified")
                    && result.followup.prior_event_id.as_deref() == Some(progress_id.as_str())
        ));
        assert!(emitter.is_terminal().await.expect("terminal snapshot"));
        server.abort();
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn human_report_is_worker_signed_threaded_and_idempotent() {
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
        let root = tempfile::tempdir().expect("state root");
        let lifecycle = LifecycleStore::new(root.path(), "job");
        lifecycle
            .initialize(receipts.accepted.id.to_hex())
            .await
            .expect("initialize lifecycle");
        let emitter = JobEmitter::new(
            &request,
            "f".repeat(64),
            worker.clone(),
            rest,
            lifecycle,
            vec!["rust".into()],
            "a".repeat(64),
            sponsor,
        );
        let report = HumanJobReport::from_turn_output(None, Some("Implemented and verified."))
            .expect("report");
        let event_id = emitter
            .publish_human_report(report.clone())
            .await
            .expect("publish report");
        let event = published.recv().await.expect("published report");
        assert_eq!(event.id.to_hex(), event_id);
        assert_eq!(event.pubkey, worker.public_key());
        assert_eq!(event.kind, Kind::Custom(9));
        assert_eq!(event.content, "Implemented and verified.");
        let tags = event
            .tags
            .iter()
            .map(|tag| tag.as_slice())
            .collect::<Vec<_>>();
        assert!(tags.iter().any(|tag| {
            tag.first().map(String::as_str) == Some("h")
                && tag.get(1).map(String::as_str)
                    == Some(request.common.project.home_channel.as_str())
        }));
        assert!(tags.iter().any(|tag| {
            tag.first().map(String::as_str) == Some("e")
                && tag.get(1).map(String::as_str) == Some("f".repeat(64).as_str())
                && tag.get(3).map(String::as_str) == Some("reply")
        }));
        assert!(tags.iter().any(|tag| {
            tag.first().map(String::as_str) == Some("buzz-task")
                && tag.get(1).map(String::as_str) == Some("report")
                && tag.get(2).map(String::as_str) == Some("f".repeat(64).as_str())
        }));
        assert!(!tags
            .iter()
            .any(|tag| tag.first().map(String::as_str) == Some("p")));

        assert_eq!(
            emitter
                .publish_human_report(report)
                .await
                .expect("confirmed report is idempotent"),
            event_id
        );
        assert!(published.try_recv().is_err());
        assert!(!emitter.is_terminal().await.expect("lifecycle remains open"));
        server.abort();
    }

    #[tokio::test]
    async fn conversation_mirror_is_deterministic_and_completes_both_acknowledgements() {
        let requester = Keys::generate();
        let worker = Keys::generate();
        let request = request_with_conversation(&requester, &worker);
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
        assert_eq!(
            build_lifecycle_mirror(&receipts.processed, &worker)
                .expect("processed mirror")
                .expect("conversation mirror")
                .content,
            "Received the task."
        );
        assert_eq!(
            build_lifecycle_mirror(&receipts.accepted, &worker)
                .expect("accepted mirror")
                .expect("conversation mirror")
                .content,
            "Started the task."
        );

        let machine = sign_job(
            JobEvent::Progress(JobProgress {
                followup: JobFollowup {
                    common: response_common(&request, &worker, &sponsor),
                    request_event_id: "f".repeat(64),
                    prior_event_id: Some(receipts.accepted.id.to_hex()),
                },
                status: JobProgressStatus::Progress,
                message: "Checking the implementation.".into(),
                evidence: Vec::new(),
            }),
            &worker,
        )
        .expect("machine progress");
        let first_mirror = build_lifecycle_mirror(&machine, &worker)
            .expect("build mirror")
            .expect("conversation mirror");
        let replayed_mirror = build_lifecycle_mirror(&machine, &worker)
            .expect("rebuild mirror")
            .expect("conversation mirror");
        assert_eq!(first_mirror.id, replayed_mirror.id);
        assert_eq!(first_mirror.created_at, machine.created_at);
        assert_eq!(
            first_mirror.content,
            "Progress: Checking the implementation."
        );
        assert!(first_mirror.tags.iter().any(|tag| {
            let tag = tag.as_slice();
            tag.first().map(String::as_str) == Some("h")
                && tag.get(1).map(String::as_str) == Some("6df3d942-e730-4b1c-9742-184bf292fa71")
        }));
        assert!(first_mirror.tags.iter().any(|tag| {
            let tag = tag.as_slice();
            tag.first().map(String::as_str) == Some("e")
                && tag.get(1).map(String::as_str) == Some("8".repeat(64).as_str())
                && tag.get(3).map(String::as_str) == Some("reply")
        }));
        assert!(first_mirror.tags.iter().any(|tag| {
            let tag = tag.as_slice();
            tag.first().map(String::as_str) == Some("buzz-task")
                && tag.get(1).map(String::as_str) == Some("lifecycle")
                && tag.get(2).map(String::as_str) == Some(machine.id.to_hex().as_str())
        }));
        assert!(!first_mirror
            .tags
            .iter()
            .any(|tag| tag.as_slice().first().map(String::as_str) == Some("p")));

        let root = tempfile::tempdir().expect("state root");
        let lifecycle = LifecycleStore::new(root.path(), "job");
        lifecycle
            .initialize(receipts.accepted.id.to_hex())
            .await
            .expect("initialize lifecycle");
        lifecycle
            .stage(machine.clone(), false, receipts.accepted.id.to_hex())
            .await
            .expect("stage machine event");
        lifecycle
            .confirm(machine.id.to_hex())
            .await
            .expect("machine acknowledgement");
        let (_, machine_confirmed, conversation_confirmed) = lifecycle
            .pending_delivery()
            .await
            .expect("pending delivery")
            .expect("mirror still pending");
        assert!(machine_confirmed);
        assert!(!conversation_confirmed);

        let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
        let emitter = JobEmitter::new(
            &request,
            "f".repeat(64),
            worker,
            rest,
            lifecycle.clone(),
            vec!["rust".into()],
            "a".repeat(64),
            sponsor,
        );
        assert_eq!(
            emitter
                .retry_lifecycle_outbox()
                .await
                .expect("retry mirror"),
            Some(machine.id.to_hex())
        );
        let published_mirror = published.recv().await.expect("published mirror");
        assert_eq!(published_mirror.id, first_mirror.id);
        assert!(published.try_recv().is_err());
        let report_id = emitter
            .publish_human_report(
                HumanJobReport::from_turn_output(None, Some("Finished the task.")).expect("report"),
            )
            .await
            .expect("publish report in conversation");
        let report = published.recv().await.expect("published report");
        assert_eq!(report.id.to_hex(), report_id);
        assert!(report.tags.iter().any(|tag| {
            let tag = tag.as_slice();
            tag.first().map(String::as_str) == Some("h")
                && tag.get(1).map(String::as_str) == Some("6df3d942-e730-4b1c-9742-184bf292fa71")
        }));
        assert!(report.tags.iter().any(|tag| {
            let tag = tag.as_slice();
            tag.first().map(String::as_str) == Some("e")
                && tag.get(1).map(String::as_str) == Some("8".repeat(64).as_str())
                && tag.get(3).map(String::as_str) == Some("reply")
        }));
        assert!(lifecycle
            .pending_delivery()
            .await
            .expect("delivery state")
            .is_none());
        assert!(emitter
            .retry_lifecycle_outbox()
            .await
            .expect("idempotent retry")
            .is_none());
        assert!(published.try_recv().is_err());
        server.abort();
    }
}
