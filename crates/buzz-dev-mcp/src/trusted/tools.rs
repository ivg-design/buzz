use std::sync::Arc;

use buzz_core::job::{
    JobCommon, JobControl, JobControlAction, JobEvent, JobFollowup, JobProject, JobRepository,
    JobRequest, JobSponsor, JOB_SCHEMA_VERSION,
};
use chrono::{Duration, SecondsFormat, Utc};
use rmcp::model::{CallToolResult, Content};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{GrantMatch, PublishedEvent, TrustedRelay};

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct A2aDispatchParams {
    pub operation_id: String,
    pub idempotency_key: String,
    #[serde(default = "one")]
    pub coordinator_epoch: u32,
    pub recipient_pubkey: String,
    pub capability: String,
    pub summary: String,
    pub acceptance: Vec<String>,
    pub worktree_id: String,
    pub paths: Vec<String>,
    #[serde(default)]
    pub contracts: Vec<String>,
    #[serde(default)]
    pub github_issue: Option<String>,
    #[serde(default)]
    pub github_pr: Option<String>,
    #[serde(default)]
    pub github_run: Option<String>,
    /// Exact kind-43005 handoff event when continuing an existing operation.
    #[serde(default)]
    pub supersedes_event_id: Option<String>,
    #[serde(default = "default_ttl")]
    pub ttl_seconds: u32,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct A2aInboxParams {
    #[serde(default = "default_inbox_limit")]
    pub limit: u16,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct A2aStatusParams {
    pub request_event_id: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct A2aCancelParams {
    pub request_event_id: String,
    pub reason: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct A2aHandoffParams {
    pub request_event_id: String,
    pub handoff_to: String,
    pub worktree_id: String,
    pub reason: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChatSendParams {
    pub content: String,
}

pub async fn dispatch(
    relay: &Arc<TrustedRelay>,
    params: A2aDispatchParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    if relay.job_operation_id.is_some() || relay.job_request_event_id.is_some() {
        return error_result("A2A dispatch is unavailable inside a one-shot job session".into());
    }
    if !(1..=604_800).contains(&params.ttl_seconds) {
        return error_result("ttl_seconds must be between 1 and 604800".into());
    }
    let result = match params.supersedes_event_id.clone() {
        Some(event_id) => build_superseding_request(relay, params, &event_id, &cancellation).await,
        None => build_request(relay, params).await,
    };
    match result.and_then(|job| ensure_session_channel(relay, &job).map(|_| job)) {
        Ok(job) => publish_result(relay.publish_job(job, &cancellation).await),
        Err(error) => error_result(error),
    }
}

pub async fn inbox(
    relay: &Arc<TrustedRelay>,
    params: A2aInboxParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    if relay.job_operation_id.is_some() || relay.job_request_event_id.is_some() {
        return error_result(
            "A2A inbox is unavailable inside a one-shot job session; use status for the bound request"
                .into(),
        );
    }
    query_result(
        relay
            .query_job_events(None, params.limit, &cancellation)
            .await,
    )
}

pub async fn status(
    relay: &Arc<TrustedRelay>,
    params: A2aStatusParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    query_result(
        relay
            .query_job_events(Some(&params.request_event_id), 100, &cancellation)
            .await,
    )
}

pub async fn cancel(
    relay: &Arc<TrustedRelay>,
    params: A2aCancelParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    let result = async {
        let events = relay
            .query_job_events(Some(&params.request_event_id), 100, &cancellation)
            .await?;
        let chain = JobChain::parse(events, &params.request_event_id)?;
        chain.ensure_requester(relay)?;
        chain.ensure_active()?;
        ensure_control_channel(relay, &chain.request.common)?;
        let control = JobControl {
            followup: JobFollowup {
                common: chain.request.common.clone(),
                request_event_id: params.request_event_id,
                prior_event_id: chain.prior_event_id(),
            },
            action: JobControlAction::Cancel,
            reason: params.reason,
            handoff_to: None,
        };
        relay
            .publish_job(JobEvent::Control(control), &cancellation)
            .await
    }
    .await;
    publish_result(result)
}

pub(super) async fn prepare_handoff(
    relay: &Arc<TrustedRelay>,
    params: A2aHandoffParams,
    cancellation: CancellationToken,
) -> Result<nostr::Event, String> {
    let events = relay
        .query_job_events(Some(&params.request_event_id), 100, &cancellation)
        .await?;
    let chain = JobChain::parse(events, &params.request_event_id)?;
    chain.ensure_recipient(relay)?;
    chain.ensure_active()?;
    ensure_bound_job_session(relay, &chain.request.common, &params.request_event_id)?;
    let target_grant = relay
        .grants
        .outbound(
            &params.handoff_to,
            &chain.request.capability,
            &chain.request.common.repository.paths,
            &params.worktree_id,
        )
        .await?;
    if target_grant.repository != chain.request.common.repository.canonical
        || target_grant.project_address != chain.request.common.project.address
        || target_grant.home_channel != chain.request.common.project.home_channel
        || target_grant.branch != chain.request.common.repository.branch
        || params.worktree_id != chain.request.common.repository.worktree_id
    {
        return Err("handoff must preserve the original repository checkout scope".into());
    }
    let mut common = chain.request.common.clone();
    common.sender_pubkey = relay.signer_pubkey();
    common.recipient_pubkey = chain.request.common.sender_pubkey.clone();
    relay.prepare_job_event(JobEvent::Control(JobControl {
        followup: JobFollowup {
            common,
            request_event_id: params.request_event_id,
            prior_event_id: chain.prior_event_id(),
        },
        action: JobControlAction::Handoff,
        reason: params.reason,
        handoff_to: Some(params.handoff_to),
    }))
}

pub(super) async fn publish_prepared_handoff(
    relay: &Arc<TrustedRelay>,
    event: nostr::Event,
    cancellation: CancellationToken,
) -> CallToolResult {
    match relay.publish_prepared_job_event(event, &cancellation).await {
        Ok(event) => json_result(&serde_json::json!({
            "event_id": event.event_id,
            "accepted": event.accepted,
            "requires_superseding_request": true,
            "required_next_coordinator_epoch": "previous_epoch_plus_one",
        })),
        Err(error) => error_result(error),
    }
}

pub async fn send_chat(
    relay: &Arc<TrustedRelay>,
    params: ChatSendParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    if relay.job_operation_id.is_some() || relay.job_request_event_id.is_some() {
        return error_result(
            "chat send is unavailable inside a one-shot job session; return the required job outcome instead"
                .into(),
        );
    }
    publish_result(relay.publish_chat(&params.content, &cancellation).await)
}

async fn build_request(
    relay: &TrustedRelay,
    params: A2aDispatchParams,
) -> Result<JobEvent, String> {
    if params.coordinator_epoch != 1 {
        return Err("initial A2A requests require coordinator_epoch=1".into());
    }
    let grant = relay
        .grants
        .outbound(
            &params.recipient_pubkey,
            &params.capability,
            &params.paths,
            &params.worktree_id,
        )
        .await?;
    let github_login = relay
        .owner_github_login
        .clone()
        .ok_or_else(|| "outbound A2A requires BUZZ_ACP_OWNER_GITHUB_LOGIN".to_owned())?;
    let request = JobRequest {
        common: common_from(grant, relay, &params, github_login),
        capability: params.capability,
        summary: params.summary,
        acceptance: params.acceptance,
        supersedes_event_id: None,
    };
    Ok(JobEvent::Request(request))
}

async fn build_superseding_request(
    relay: &TrustedRelay,
    params: A2aDispatchParams,
    handoff_event_id: &str,
    cancellation: &CancellationToken,
) -> Result<JobEvent, String> {
    let handoff_event = relay
        .query_handoff_event(handoff_event_id, cancellation)
        .await?;
    let handoff = match JobEvent::parse(&handoff_event).map_err(|error| error.to_string())? {
        JobEvent::Control(control) if control.action == JobControlAction::Handoff => control,
        _ => return Err("supersedes_event_id must reference a handoff control".into()),
    };
    let target = handoff
        .handoff_to
        .as_deref()
        .ok_or_else(|| "handoff has no target".to_owned())?;
    if target != params.recipient_pubkey {
        return Err("dispatch recipient does not match the signed handoff target".into());
    }
    let old_id = handoff.followup.request_event_id.clone();
    let chain = JobChain::parse(
        relay
            .query_job_events(Some(&old_id), 100, cancellation)
            .await?,
        &old_id,
    )?;
    chain.ensure_requester(relay)?;
    if handoff.followup.common.sender_pubkey != chain.request.common.recipient_pubkey
        || handoff.followup.common.recipient_pubkey != chain.request.common.sender_pubkey
        || !same_operation_scope(&handoff.followup.common, &chain.request.common)
    {
        return Err("handoff actors or scope do not match the original request".into());
    }
    let old = &chain.request;
    let expected_epoch = old
        .common
        .coordinator_epoch
        .checked_add(1)
        .ok_or_else(|| "coordinator epoch overflow".to_owned())?;
    if params.operation_id != old.common.operation_id
        || params.idempotency_key != old.common.idempotency_key
        || params.coordinator_epoch != expected_epoch
        || params.capability != old.capability
        || params.summary != old.summary
        || params.acceptance != old.acceptance
        || params.worktree_id != old.common.repository.worktree_id
        || params.paths != old.common.repository.paths
        || params.contracts != old.common.repository.contracts
        || params.github_issue != old.common.repository.github_issue
        || params.github_pr != old.common.repository.github_pr
        || params.github_run != old.common.repository.github_run
    {
        return Err("superseding dispatch must exactly match the old scope and next epoch".into());
    }
    if !relay.grants.allows_superseding_request(
        &old.common.project,
        &old.common.repository,
        target,
        &old.capability,
    ) {
        return Err("handoff target is outside the current local grant".into());
    }
    let mut next = old.clone();
    next.common.coordinator_epoch = expected_epoch;
    next.common.recipient_pubkey = target.to_owned();
    next.supersedes_event_id = Some(handoff_event_id.to_owned());
    Ok(JobEvent::Request(next))
}

fn same_operation_scope(left: &JobCommon, right: &JobCommon) -> bool {
    left.operation_id == right.operation_id
        && left.idempotency_key == right.idempotency_key
        && left.coordinator_epoch == right.coordinator_epoch
        && left.project == right.project
        && left.repository == right.repository
        && left.sponsor == right.sponsor
        && left.expires_at == right.expires_at
}

fn common_from(
    grant: GrantMatch,
    relay: &TrustedRelay,
    params: &A2aDispatchParams,
    github_login: String,
) -> JobCommon {
    JobCommon {
        schema_version: JOB_SCHEMA_VERSION.into(),
        operation_id: params.operation_id.clone(),
        idempotency_key: params.idempotency_key.clone(),
        coordinator_epoch: params.coordinator_epoch,
        project: JobProject {
            address: grant.project_address,
            home_channel: grant.home_channel,
        },
        repository: JobRepository {
            canonical: grant.repository,
            github_issue: params.github_issue.clone(),
            github_pr: params.github_pr.clone(),
            github_run: params.github_run.clone(),
            base_sha: grant.base_sha,
            branch: grant.branch,
            worktree_id: grant.worktree_id,
            paths: params.paths.clone(),
            contracts: params.contracts.clone(),
        },
        sender_pubkey: relay.signer_pubkey(),
        recipient_pubkey: params.recipient_pubkey.clone(),
        sponsor: JobSponsor {
            pubkey: relay.owner_pubkey.clone(),
            github_login,
        },
        expires_at: (Utc::now() + Duration::seconds(i64::from(params.ttl_seconds)))
            .to_rfc3339_opts(SecondsFormat::Secs, true),
    }
}

fn ensure_session_channel(relay: &TrustedRelay, job: &JobEvent) -> Result<(), String> {
    let bound = relay
        .session_channel_id
        .as_deref()
        .ok_or_else(|| "A2A dispatch requires a channel-bound session".to_owned())?;
    if bound != job.common().project.home_channel {
        return Err("session channel does not match the locally granted project channel".into());
    }
    Ok(())
}

fn ensure_control_channel(relay: &TrustedRelay, common: &JobCommon) -> Result<(), String> {
    if relay.session_channel_id.as_deref() != Some(common.project.home_channel.as_str()) {
        return Err("session channel binding does not match the request".into());
    }
    if relay
        .job_operation_id
        .as_deref()
        .is_some_and(|operation| operation != common.operation_id)
    {
        return Err("job operation binding does not match the request".into());
    }
    Ok(())
}

fn ensure_bound_job_session(
    relay: &TrustedRelay,
    common: &JobCommon,
    request_event_id: &str,
) -> Result<(), String> {
    let operation = relay
        .job_operation_id
        .as_deref()
        .ok_or_else(|| "control operations require a job-bound session".to_owned())?;
    let request = relay
        .job_request_event_id
        .as_deref()
        .ok_or_else(|| "control operations require a job-bound session".to_owned())?;
    if operation != common.operation_id {
        return Err("job session operation binding does not match the request".into());
    }
    ensure_control_channel(relay, common)?;
    if request != request_event_id {
        return Err("job session request binding does not match the request".into());
    }
    Ok(())
}

struct JobChain {
    request: JobRequest,
    followups: Vec<(nostr::Event, JobEvent)>,
}

impl JobChain {
    fn parse(events: Vec<nostr::Event>, request_id: &str) -> Result<Self, String> {
        let mut request = None;
        let mut followups = Vec::new();
        for event in events {
            let parsed = JobEvent::parse(&event).map_err(|error| error.to_string())?;
            match &parsed {
                JobEvent::Request(body) if event.id.to_hex() == request_id => {
                    if request.replace(body.clone()).is_some() {
                        return Err("relay returned duplicate root job requests".into());
                    }
                }
                JobEvent::Request(_) => {
                    return Err("relay returned an unrelated job request".into())
                }
                _ if parsed.request_event_id() == Some(request_id) => {
                    followups.push((event, parsed))
                }
                _ => return Err("relay returned an unrelated job follow-up".into()),
            }
        }
        Ok(Self {
            request: request.ok_or_else(|| "job request was not found".to_owned())?,
            followups,
        })
        .and_then(Self::validate_scope)
    }

    fn validate_scope(self) -> Result<Self, String> {
        let root = &self.request.common;
        for (_, event) in &self.followups {
            let candidate = event.common();
            let request_actors = (&root.sender_pubkey, &root.recipient_pubkey);
            let event_actors = (&candidate.sender_pubkey, &candidate.recipient_pubkey);
            let actors_match = event_actors == request_actors
                || event_actors == (request_actors.1, request_actors.0);
            if !same_operation_scope(candidate, root) || !actors_match {
                return Err("job follow-up escaped the immutable request scope".into());
            }
        }
        Ok(self)
    }

    fn ensure_requester(&self, relay: &TrustedRelay) -> Result<(), String> {
        if self.request.common.sender_pubkey != relay.signer_pubkey() {
            return Err("only the original requester may cancel through this tool".into());
        }
        Ok(())
    }

    fn ensure_recipient(&self, relay: &TrustedRelay) -> Result<(), String> {
        if self.request.common.recipient_pubkey != relay.signer_pubkey() {
            return Err("only the current recipient may request handoff through this tool".into());
        }
        Ok(())
    }

    fn ensure_active(&self) -> Result<(), String> {
        if self.followups.iter().any(|(_, job)| {
            matches!(job, JobEvent::Result(_) | JobEvent::Error(_))
                || matches!(job, JobEvent::Control(control) if matches!(control.action, JobControlAction::Cancelled | JobControlAction::Release | JobControlAction::Handoff))
        }) {
            return Err("job already has a terminal or ownership-transfer event".into());
        }
        Ok(())
    }

    fn prior_event_id(&self) -> Option<String> {
        self.followups
            .iter()
            .max_by_key(|(event, _)| (event.created_at.as_secs(), event.id.to_hex()))
            .map(|(event, _)| event.id.to_hex())
    }
}

#[derive(Serialize)]
struct SafeJobEvent {
    event_id: String,
    kind: u32,
    author: String,
    created_at: u64,
    body: serde_json::Value,
}

fn query_result(result: Result<Vec<nostr::Event>, String>) -> CallToolResult {
    match result.and_then(|events| {
        events
            .into_iter()
            .map(|event| {
                let job = JobEvent::parse(&event).map_err(|error| error.to_string())?;
                let body =
                    serde_json::from_str(&job.canonical_json().map_err(|error| error.to_string())?)
                        .map_err(|_| "canonical job body was not JSON".to_owned())?;
                Ok(SafeJobEvent {
                    event_id: event.id.to_hex(),
                    kind: u32::from(event.kind.as_u16()),
                    author: event.pubkey.to_hex(),
                    created_at: event.created_at.as_secs(),
                    body,
                })
            })
            .collect::<Result<Vec<_>, String>>()
    }) {
        Ok(events) => json_result(&serde_json::json!({"events": events})),
        Err(error) => error_result(error),
    }
}

fn publish_result(result: Result<PublishedEvent, String>) -> CallToolResult {
    match result {
        Ok(event) => json_result(&event),
        Err(error) => error_result(error),
    }
}

fn json_result(value: &impl Serialize) -> CallToolResult {
    match serde_json::to_string(value) {
        Ok(json) => CallToolResult::success(vec![Content::text(json)]),
        Err(_) => error_result("tool response serialization failed".into()),
    }
}

fn error_result(error: String) -> CallToolResult {
    CallToolResult::error(vec![Content::text(error)])
}

const fn one() -> u32 {
    1
}

const fn default_ttl() -> u32 {
    7_200
}

const fn default_inbox_limit() -> u16 {
    50
}

#[cfg(test)]
#[path = "tools_tests.rs"]
mod tests;
