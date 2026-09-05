//! Return a delegated result to the conversation that requested the work.

use std::collections::{HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use buzz_core::job::{JobControlAction, JobEvent};
use nostr::Event;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{relay::RestClient, scope::SessionScope};

const ROUTER_STATE_VERSION: u32 = 1;
const DELIVERED_EVENT_LIMIT: usize = 4_096;
const PENDING_EVENT_LIMIT: usize = 4_096;
const READY_RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NotificationDestination {
    /// Provider session to resume.
    pub scope: SessionScope,
    /// Human chat channel in which the requester expects the continuation.
    pub channel_id: Uuid,
    /// Human chat thread, when the originating message was threaded.
    pub thread_root_id: Option<String>,
}

impl NotificationDestination {
    pub(crate) fn prompt_tag(&self) -> String {
        format!(
            "delegated-result:{}:{}",
            self.channel_id,
            self.thread_root_id.as_deref().unwrap_or_default()
        )
    }
}

pub(crate) fn parse_reply_destination(prompt_tag: &str) -> Option<(Uuid, Option<String>)> {
    let encoded = prompt_tag.strip_prefix("delegated-result:")?;
    let (channel_text, root) = encoded.split_once(':')?;
    let channel = Uuid::parse_str(channel_text).ok()?;
    if channel.to_string() != channel_text {
        return None;
    }
    let root = match root {
        "" => None,
        value
            if value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) =>
        {
            Some(value.to_owned())
        }
        _ => return None,
    };
    Some((channel, root))
}

pub(crate) fn is_terminal(event: &JobEvent) -> bool {
    match event {
        JobEvent::Result(_) | JobEvent::Error(_) => true,
        JobEvent::Accepted(accepted) => {
            accepted.claim.status == buzz_core::job::JobClaimStatus::Declined
        }
        JobEvent::Control(control) => matches!(
            control.action,
            JobControlAction::Cancelled | JobControlAction::Handoff | JobControlAction::Release
        ),
        _ => false,
    }
}

/// Durable delegated-result notification router.
///
/// Pending entries contain only an already-signed terminal event. They remain
/// pending until the provider reports the event ID as delivered, so neither a
/// transient request lookup failure nor a process restart can lose the result.
/// This state never contains or replays native worker instructions.
#[derive(Debug, Clone)]
pub(crate) struct ReadyNotification {
    pub event: Event,
    pub destination: NotificationDestination,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingNotification {
    event: Event,
    destination: Option<NotificationDestination>,
    attempts: u32,
    next_attempt_at_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RouterState {
    version: u32,
    pending: Vec<PendingNotification>,
    delivered: VecDeque<String>,
}

#[derive(Debug)]
pub(crate) struct ResultNotificationRouter {
    path: PathBuf,
    pending: Vec<PendingNotification>,
    delivered: HashSet<String>,
    delivery_order: VecDeque<String>,
}

impl ResultNotificationRouter {
    pub(crate) fn open(path: PathBuf) -> Result<Self, String> {
        let state = if path.exists() {
            serde_json::from_slice::<RouterState>(
                &std::fs::read(&path).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?
        } else {
            RouterState {
                version: ROUTER_STATE_VERSION,
                pending: Vec::new(),
                delivered: VecDeque::new(),
            }
        };
        if state.version != ROUTER_STATE_VERSION
            || state.pending.len() > PENDING_EVENT_LIMIT
            || state.delivered.len() > DELIVERED_EVENT_LIMIT
        {
            return Err("delegated result notification state is invalid".into());
        }
        let mut pending_ids = HashSet::new();
        let mut pending = state.pending;
        for item in &mut pending {
            item.event.verify().map_err(|error| error.to_string())?;
            if !pending_ids.insert(item.event.id.to_hex()) {
                return Err("delegated result notification state has duplicate pending IDs".into());
            }
            // Re-resolve the signed request after restart rather than trusting
            // a cached presentation destination from local state.
            item.destination = None;
            item.next_attempt_at_ms = 0;
        }
        let delivered = state.delivered.iter().cloned().collect::<HashSet<_>>();
        if delivered.len() != state.delivered.len()
            || delivered.iter().any(|id| !is_event_id(id))
            || pending_ids.iter().any(|id| delivered.contains(id))
        {
            return Err("delegated result notification receipts are invalid".into());
        }
        Ok(Self {
            path,
            pending,
            delivered,
            delivery_order: state.delivered,
        })
    }

    pub(crate) async fn route(
        &mut self,
        rest: &RestClient,
        event: &Event,
        agent: &str,
    ) -> Result<Option<ReadyNotification>, String> {
        let event_id = event.id.to_hex();
        if self.delivered.contains(&event_id) {
            return Ok(None);
        }
        if !is_candidate(event, agent)? {
            return Ok(None);
        }
        let newly_pending = if !self
            .pending
            .iter()
            .any(|pending| pending.event.id == event.id)
        {
            if self.pending.len() >= PENDING_EVENT_LIMIT {
                return Err("delegated result notification queue is full".into());
            }
            event.verify().map_err(|error| error.to_string())?;
            self.pending.push(PendingNotification {
                event: event.clone(),
                destination: None,
                attempts: 0,
                next_attempt_at_ms: 0,
            });
            self.persist()?;
            true
        } else {
            false
        };
        // Relay redelivery must not bypass lookup backoff for an existing
        // pending result. A newly observed event is due immediately.
        self.try_event(rest, &event_id, agent, newly_pending).await
    }

    pub(crate) async fn poll_ready(
        &mut self,
        rest: &RestClient,
        agent: &str,
    ) -> Result<Option<ReadyNotification>, String> {
        let now = now_ms();
        let Some(event_id) = self
            .pending
            .iter()
            .filter(|pending| pending.next_attempt_at_ms <= now)
            .min_by_key(|pending| pending.next_attempt_at_ms)
            .map(|pending| pending.event.id.to_hex())
        else {
            return Ok(None);
        };
        self.try_event(rest, &event_id, agent, false).await
    }

    /// Acknowledge exact event IDs only after a provider Result receipt says it
    /// rendered/delivered them. Queue admission alone is not delivery.
    pub(crate) fn ack_delivered(
        &mut self,
        delivered_event_ids: &HashSet<String>,
    ) -> Result<usize, String> {
        let mut acknowledged = Vec::new();
        self.pending.retain(|pending| {
            let event_id = pending.event.id.to_hex();
            if delivered_event_ids.contains(&event_id) {
                acknowledged.push(event_id);
                false
            } else {
                true
            }
        });
        if acknowledged.is_empty() {
            return Ok(0);
        }
        let count = acknowledged.len();
        for event_id in acknowledged {
            self.remember(event_id);
        }
        self.persist()?;
        Ok(count)
    }

    async fn try_event(
        &mut self,
        rest: &RestClient,
        event_id: &str,
        agent: &str,
        force: bool,
    ) -> Result<Option<ReadyNotification>, String> {
        let Some(index) = self
            .pending
            .iter()
            .position(|pending| pending.event.id.to_hex() == event_id)
        else {
            return Ok(None);
        };
        if !force && self.pending[index].next_attempt_at_ms > now_ms() {
            return Ok(None);
        }
        if let Some(destination) = self.pending[index].destination.clone() {
            self.pending[index].next_attempt_at_ms = deadline_ms(READY_RETRY_DELAY);
            let ready = ReadyNotification {
                event: self.pending[index].event.clone(),
                destination,
            };
            self.persist()?;
            return Ok(Some(ready));
        }
        let event = self.pending[index].event.clone();
        match resolve_destination(rest, &event, agent).await {
            Ok(Some(destination)) => {
                self.pending[index].destination = Some(destination.clone());
                self.pending[index].attempts = 0;
                self.pending[index].next_attempt_at_ms = deadline_ms(READY_RETRY_DELAY);
                self.persist()?;
                Ok(Some(ReadyNotification { event, destination }))
            }
            Ok(None) => {
                self.pending.remove(index);
                self.remember(event_id.to_owned());
                self.persist()?;
                Ok(None)
            }
            Err(error) => {
                self.pending[index].attempts = self.pending[index].attempts.saturating_add(1);
                self.pending[index].next_attempt_at_ms =
                    deadline_ms(lookup_retry_delay(self.pending[index].attempts));
                self.persist()?;
                Err(error)
            }
        }
    }

    fn remember(&mut self, event_id: String) {
        if !self.delivered.insert(event_id.clone()) {
            return;
        }
        self.delivery_order.push_back(event_id);
        while self.delivery_order.len() > DELIVERED_EVENT_LIMIT {
            if let Some(expired) = self.delivery_order.pop_front() {
                self.delivered.remove(&expired);
            }
        }
    }

    fn persist(&self) -> Result<(), String> {
        let state = RouterState {
            version: ROUTER_STATE_VERSION,
            pending: self.pending.clone(),
            delivered: self.delivery_order.clone(),
        };
        write_state(&self.path, &state)
    }
}

async fn resolve_destination(
    rest: &RestClient,
    event: &Event,
    agent: &str,
) -> Result<Option<NotificationDestination>, String> {
    if !is_candidate(event, agent)? {
        return Ok(None);
    }
    let parsed = JobEvent::parse(event).map_err(|error| error.to_string())?;
    let Some(request_id) = parsed.request_event_id() else {
        return Ok(None);
    };
    let request = lookup_request(rest, request_id).await?;
    result_destination(event, &request, agent)
}

fn is_candidate(event: &Event, agent: &str) -> Result<bool, String> {
    event.verify().map_err(|error| error.to_string())?;
    let parsed = JobEvent::parse(event).map_err(|error| error.to_string())?;
    Ok(is_terminal(&parsed) && parsed.common().recipient_pubkey == agent)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn deadline_ms(delay: Duration) -> u64 {
    let millis = delay.as_millis().min(u64::MAX as u128) as u64;
    now_ms().saturating_add(millis)
}

fn lookup_retry_delay(attempts: u32) -> Duration {
    let shift = attempts.saturating_sub(1).min(7);
    Duration::from_millis((250_u64 << shift).min(30_000))
}

fn is_event_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn write_state(path: &Path, state: &RouterState) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "delegated result notification path has no parent".to_owned())?;
    std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err("delegated result notification state path is invalid".into());
        }
    }
    let temporary = parent.join(format!(
        ".job-notifications-{}-{}.tmp",
        std::process::id(),
        Uuid::new_v4().simple()
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let result = (|| {
        let mut file = options
            .open(&temporary)
            .map_err(|error| error.to_string())?;
        file.write_all(&serde_json::to_vec(state).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        std::fs::rename(&temporary, path).map_err(|error| error.to_string())?;
        #[cfg(unix)]
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

async fn lookup_request(rest: &RestClient, request_id: &str) -> Result<Event, String> {
    let raw = rest
        .query_raw(&[serde_json::json!({
            "ids": [request_id], "kinds": [43001], "limit": 1,
        })])
        .await
        .map_err(|error| error.to_string())?;
    let Some(request) = raw.as_array().and_then(|events| events.first()) else {
        return Err("delegated result request is unavailable".into());
    };
    serde_json::from_value(request.clone()).map_err(|error| error.to_string())
}

fn result_destination(
    event: &Event,
    request_event: &Event,
    agent: &str,
) -> Result<Option<NotificationDestination>, String> {
    event.verify().map_err(|error| error.to_string())?;
    request_event.verify().map_err(|error| error.to_string())?;
    let terminal = JobEvent::parse(event).map_err(|error| error.to_string())?;
    let JobEvent::Request(request) =
        JobEvent::parse(request_event).map_err(|error| error.to_string())?
    else {
        return Err("delegated result does not reference a request".into());
    };
    if !is_terminal(&terminal)
        || terminal.request_event_id() != Some(request_event.id.to_hex().as_str())
        || request.common.sender_pubkey != agent
        || terminal.common().recipient_pubkey != agent
        || terminal.common().sender_pubkey != request.common.recipient_pubkey
        || terminal.common().schema_version != request.common.schema_version
        || terminal.common().operation_id != request.common.operation_id
        || terminal.common().idempotency_key != request.common.idempotency_key
        || terminal.common().coordinator_epoch != request.common.coordinator_epoch
        || terminal.common().project != request.common.project
        || terminal.common().repository != request.common.repository
        || terminal.common().conversation != request.common.conversation
        || terminal.common().expires_at != request.common.expires_at
    {
        return Err("delegated result does not match the addressed task".into());
    }
    // Old releases did not retain an origin. Do not wake/replay their historical
    // tasks on upgrade or guess a different conversation for them.
    let Some(origin) = request.origin else {
        return Ok(None);
    };
    let channel_id = Uuid::parse_str(&origin.channel_id).map_err(|error| error.to_string())?;
    let session_channel_id = origin
        .session_channel_id
        .as_deref()
        .unwrap_or(&origin.channel_id);
    let session_channel_id =
        Uuid::parse_str(session_channel_id).map_err(|error| error.to_string())?;
    let scope = match origin.session_thread_root_id {
        Some(root_event_id) => SessionScope::Thread {
            channel_id: session_channel_id,
            root_event_id,
        },
        None => SessionScope::Conversation {
            channel_id: session_channel_id,
        },
    };
    Ok(Some(NotificationDestination {
        scope,
        channel_id,
        thread_root_id: origin.thread_root_id,
    }))
}

pub(crate) fn instruction() -> &'static str {
    "A delegated teammate returned a task result. Continue your existing plan from \
     this result; this is not a new assignment. Read its summary and report reference, \
     incorporate the result once, and keep any useful update concise in this \
     conversation. Relay acceptance is not completion. For a failed or indeterminate \
     outcome, preserve existing effects and explain the remaining problem; do not \
     redispatch the same work automatically."
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use axum::extract::State;
    use axum::routing::post;
    use axum::{Json, Router};
    use buzz_core::job::{
        build_job_tags, JobCommon, JobConversation, JobEvent, JobOrigin, JobProject, JobRepository,
        JobRequest, JobResult, JobSponsor, JobSuccessOutcome, JOB_SCHEMA_VERSION,
    };
    use nostr::{EventBuilder, Keys, Kind};

    use super::*;

    const PROVIDER_CHANNEL: &str = "3580ca9b-47b4-4af9-b22a-1068778f26c6";
    const CHAT_CHANNEL: &str = "6df3d942-e730-4b1c-9742-184bf292fa71";

    fn request(requester: &Keys, worker: &Keys) -> (JobRequest, Event) {
        let request = JobRequest {
            common: JobCommon {
                schema_version: JOB_SCHEMA_VERSION.into(),
                operation_id: "31dbb246-bc79-4ddc-aab0-2773f05b5cb2".into(),
                idempotency_key: "notification-test".into(),
                coordinator_epoch: 1,
                project: JobProject {
                    address: format!("30621:{}:nemo", requester.public_key().to_hex()),
                    home_channel: PROVIDER_CHANNEL.into(),
                },
                conversation: Some(JobConversation {
                    channel_id: CHAT_CHANNEL.into(),
                    thread_root_id: "8".repeat(64),
                }),
                repository: JobRepository {
                    canonical: "https://github.com/example/repo".into(),
                    github_issue: None,
                    github_pr: None,
                    github_run: None,
                    base_sha: "a".repeat(40),
                    branch: "codex/notification-test".into(),
                    worktree_id: "notification-test".into(),
                    paths: vec!["crates/buzz-acp".into()],
                    contracts: Vec::new(),
                },
                sender_pubkey: requester.public_key().to_hex(),
                recipient_pubkey: worker.public_key().to_hex(),
                sponsor: JobSponsor {
                    pubkey: requester.public_key().to_hex(),
                    github_login: "requester".into(),
                },
                expires_at: "2030-01-01T00:00:00Z".into(),
            },
            capability: "rust".into(),
            title: Some("Return the result".into()),
            origin: Some(JobOrigin {
                channel_id: CHAT_CHANNEL.into(),
                thread_root_id: Some("9".repeat(64)),
                session_channel_id: Some(PROVIDER_CHANNEL.into()),
                session_thread_root_id: Some("7".repeat(64)),
            }),
            summary: "Exercise result routing".into(),
            acceptance: vec!["Route once".into()],
            supersedes_event_id: None,
        };
        let event = sign(JobEvent::Request(request.clone()), requester);
        (request, event)
    }

    fn terminal(request: &JobRequest, request_event: &Event, worker: &Keys) -> Event {
        let mut common = request.common.clone();
        common.sender_pubkey = worker.public_key().to_hex();
        common.recipient_pubkey = request.common.sender_pubkey.clone();
        common.sponsor = JobSponsor {
            pubkey: worker.public_key().to_hex(),
            github_login: "worker-owner".into(),
        };
        sign(
            JobEvent::Result(JobResult {
                followup: buzz_core::job::JobFollowup {
                    common,
                    request_event_id: request_event.id.to_hex(),
                    prior_event_id: Some("b".repeat(64)),
                },
                outcome: JobSuccessOutcome::Success,
                summary: Some("Finished.".into()),
                candidate_sha: None,
                artifacts: Vec::new(),
                evidence: vec![format!("buzz:event:{}", "c".repeat(64))],
                capabilities: Vec::new(),
            }),
            worker,
        )
    }

    fn sign(job: JobEvent, keys: &Keys) -> Event {
        let kind = match job {
            JobEvent::Request(_) => 43001,
            JobEvent::Result(_) => 43004,
            _ => unreachable!("notification fixture kind"),
        };
        EventBuilder::new(
            Kind::Custom(kind),
            job.canonical_json().expect("canonical job"),
        )
        .tags(build_job_tags(&job).expect("job tags"))
        .sign_with_keys(keys)
        .expect("signed job")
    }

    #[test]
    fn exact_signed_request_routes_provider_scope_and_chat_destination() {
        let requester = Keys::generate();
        let worker = Keys::generate();
        let (request, request_event) = request(&requester, &worker);
        let terminal = terminal(&request, &request_event, &worker);
        let destination =
            result_destination(&terminal, &request_event, &requester.public_key().to_hex())
                .expect("bound destination")
                .expect("addressed destination");
        assert_eq!(
            destination,
            NotificationDestination {
                scope: SessionScope::Thread {
                    channel_id: Uuid::parse_str(PROVIDER_CHANNEL).expect("provider channel"),
                    root_event_id: "7".repeat(64),
                },
                channel_id: Uuid::parse_str(CHAT_CHANNEL).expect("chat channel"),
                thread_root_id: Some("9".repeat(64)),
            }
        );
        let prompt_tag = destination.prompt_tag();
        assert_eq!(
            parse_reply_destination(&prompt_tag),
            Some((
                Uuid::parse_str(CHAT_CHANNEL).expect("chat channel"),
                Some("9".repeat(64)),
            ))
        );
    }

    #[test]
    fn reply_destination_tag_rejects_noncanonical_or_unbound_values() {
        assert_eq!(
            parse_reply_destination(&format!("delegated-result:{CHAT_CHANNEL}:")),
            Some((Uuid::parse_str(CHAT_CHANNEL).expect("channel"), None))
        );
        assert!(parse_reply_destination(&format!(
            "delegated-result:{}:",
            CHAT_CHANNEL.to_ascii_uppercase()
        ))
        .is_none());
        assert!(parse_reply_destination(&format!(
            "delegated-result:{CHAT_CHANNEL}:{}",
            "A".repeat(64)
        ))
        .is_none());
        assert!(parse_reply_destination("delegated-result:not-a-uuid:").is_none());
    }

    #[test]
    fn signed_conversation_drift_is_rejected() {
        let requester = Keys::generate();
        let worker = Keys::generate();
        let (request, request_event) = request(&requester, &worker);
        let mut drifted = request.clone();
        drifted.common.conversation = Some(JobConversation {
            channel_id: CHAT_CHANNEL.into(),
            thread_root_id: "6".repeat(64),
        });
        let terminal = terminal(&drifted, &request_event, &worker);
        assert!(
            result_destination(&terminal, &request_event, &requester.public_key().to_hex())
                .is_err()
        );
    }

    #[test]
    fn legacy_origin_scope_and_missing_origin_are_explicit() {
        let requester = Keys::generate();
        let worker = Keys::generate();
        let (mut request, _) = request(&requester, &worker);
        let origin = request.origin.as_mut().expect("origin");
        origin.session_channel_id = None;
        origin.session_thread_root_id = None;
        let request_event = sign(JobEvent::Request(request.clone()), &requester);
        let routed_terminal = terminal(&request, &request_event, &worker);
        assert_eq!(
            result_destination(
                &routed_terminal,
                &request_event,
                &requester.public_key().to_hex(),
            )
            .expect("conversation destination"),
            Some(NotificationDestination {
                scope: SessionScope::Conversation {
                    channel_id: Uuid::parse_str(CHAT_CHANNEL).expect("fallback provider channel"),
                },
                channel_id: Uuid::parse_str(CHAT_CHANNEL).expect("chat channel"),
                thread_root_id: Some("9".repeat(64)),
            })
        );

        request.origin = None;
        let request_event = sign(JobEvent::Request(request.clone()), &requester);
        let terminal = terminal(&request, &request_event, &worker);
        assert_eq!(
            result_destination(&terminal, &request_event, &requester.public_key().to_hex())
                .expect("legacy request"),
            None
        );
    }

    #[tokio::test]
    async fn request_lookup_retries_then_routes_and_deduplicates_exact_terminal() {
        let requester = Keys::generate();
        let worker = Keys::generate();
        let (request, request_event) = request(&requester, &worker);
        let terminal = terminal(&request, &request_event, &worker);
        let calls = Arc::new(AtomicUsize::new(0));
        let state = (calls.clone(), request_event.clone());
        async fn query(
            State((calls, request)): State<(Arc<AtomicUsize>, Event)>,
        ) -> Json<serde_json::Value> {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Json(serde_json::json!([]))
            } else {
                Json(serde_json::json!([request]))
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind lookup fixture");
        let base_url = format!("http://{}", listener.local_addr().expect("address"));
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/query", post(query)).with_state(state),
            )
            .await
            .expect("serve lookup fixture");
        });
        let rest = RestClient {
            http: reqwest::Client::new(),
            base_url,
            keys: requester.clone(),
            auth_tag_json: None,
        };
        let directory = tempfile::tempdir().expect("state directory");
        let path = directory.path().join("result-notifications.json");
        let mut router = ResultNotificationRouter::open(path.clone()).expect("open router");

        assert!(router
            .route(&rest, &terminal, &requester.public_key().to_hex())
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(path.exists(), "terminal persisted before failed lookup");
        assert!(router
            .route(&rest, &terminal, &requester.public_key().to_hex())
            .await
            .expect("relay duplicate respects retry delay")
            .is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        router.pending[0].next_attempt_at_ms = 0;
        router.persist().expect("make lookup due");
        let ready = router
            .poll_ready(&rest, &requester.public_key().to_hex())
            .await
            .expect("retry lookup")
            .expect("notification ready");
        assert_eq!(ready.event.id, terminal.id);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // Queue admission is not an acknowledgement. Until a provider receipt
        // arrives, the exact signed event remains durable across restart.
        drop(router);
        let mut router = ResultNotificationRouter::open(path.clone()).expect("reopen router");
        let ready = router
            .poll_ready(&rest, &requester.public_key().to_hex())
            .await
            .expect("resolve after restart")
            .expect("recovered notification");
        assert_eq!(ready.event, terminal);
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        let delivered = HashSet::from([terminal.id.to_hex()]);
        assert_eq!(router.ack_delivered(&delivered).expect("delivery ACK"), 1);
        drop(router);
        let mut router = ResultNotificationRouter::open(path).expect("reopen receipts");
        assert!(router
            .route(&rest, &terminal, &requester.public_key().to_hex())
            .await
            .expect("duplicate receipt")
            .is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        server.abort();
    }
}
