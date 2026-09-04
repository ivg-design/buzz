use std::collections::{HashMap, HashSet};

use buzz_core::job::{
    JobClaimStatus, JobControlAction, JobEvent, JobProgressStatus, JOB_SCHEMA_VERSION,
};
use nostr::Event;
use serde::Serialize;
use serde_json::{json, Value};

use crate::error::CliError;

use super::publish::control_name;

#[derive(Debug, Clone, Serialize)]
pub(super) struct Milestone {
    pub(super) event_id: String,
    pub(super) created_at: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct LifecycleProjection {
    pub(super) state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) processed: Option<Milestone>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) accepted: Option<Milestone>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) completed: Option<Milestone>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) terminal: Option<Milestone>,
    pub(super) conflict: bool,
    pub(super) conflicts: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct JobProjection {
    pub(super) job_schema_version: &'static str,
    pub(super) operation_id: String,
    pub(super) coordinator_epoch: u32,
    pub(super) request_event_id: String,
    project_address: String,
    home_channel: String,
    repository: String,
    sender_pubkey: String,
    recipient_pubkey: String,
    relay: Value,
    pub(super) lifecycle: LifecycleProjection,
    pub(super) event_ids: Vec<String>,
    pub(super) events: Vec<Event>,
}

pub(super) fn project(events: Vec<Event>) -> Result<Vec<JobProjection>, CliError> {
    let mut parsed = Vec::new();
    for event in events {
        let job = JobEvent::parse(&event)
            .map_err(|error| CliError::Other(format!("stored job event is invalid: {error}")))?;
        parsed.push((event, job));
    }
    let mut by_operation: HashMap<String, Vec<(Event, JobEvent)>> = HashMap::new();
    for item in parsed {
        by_operation
            .entry(item.1.common().operation_id.clone())
            .or_default()
            .push(item);
    }
    let mut output = Vec::new();
    for (_, operation_events) in by_operation {
        let max_epoch = operation_events
            .iter()
            .map(|(_, job)| job.common().coordinator_epoch)
            .max()
            .unwrap_or(1);
        let requests: Vec<&(Event, JobEvent)> = operation_events
            .iter()
            .filter(|(_, job)| {
                matches!(job, JobEvent::Request(_)) && job.common().coordinator_epoch == max_epoch
            })
            .collect();
        if requests.is_empty() {
            continue;
        }
        let root_conflict = requests.len() > 1;
        for request in requests {
            output.push(project_root(request, &operation_events, root_conflict)?);
        }
    }
    output.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
    Ok(output)
}

fn project_root(
    request: &(Event, JobEvent),
    events: &[(Event, JobEvent)],
    root_conflict: bool,
) -> Result<JobProjection, CliError> {
    let root_id = request.0.id.to_hex();
    let common = request.1.common();
    let mut state = "requested".to_owned();
    let mut processed = None;
    let mut accepted = None;
    let mut completed = None;
    let mut terminal = None;
    let mut conflicts = if root_conflict {
        vec!["multiple_request_roots_for_epoch".to_owned()]
    } else {
        Vec::new()
    };
    let mut head = root_id.clone();
    let mut seen_ids = HashSet::from([root_id.clone()]);
    let mut event_ids = vec![root_id.clone()];

    let followers: Vec<&(Event, JobEvent)> = events
        .iter()
        .filter(|(_, job)| job.request_event_id() == Some(root_id.as_str()))
        .collect();
    let mut by_predecessor: HashMap<String, Vec<&(Event, JobEvent)>> = HashMap::new();
    for item in &followers {
        let parent = item
            .1
            .prior_event_id()
            .unwrap_or(root_id.as_str())
            .to_owned();
        by_predecessor.entry(parent).or_default().push(*item);
    }
    loop {
        let mut successors = by_predecessor.remove(&head).unwrap_or_default();
        successors.sort_by_key(|(event, _)| event.id.to_hex());
        if successors.is_empty() {
            break;
        }
        if successors.len() != 1 {
            conflicts.push(format!(
                "multiple_successors:{}:{}",
                head,
                successors
                    .iter()
                    .map(|(event, _)| event.id.to_hex())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
            break;
        }
        let (event, job) = successors[0];
        let id = event.id.to_hex();
        if !seen_ids.insert(id.clone()) {
            conflicts.push(format!("cycle:{id}"));
            break;
        }
        event_ids.push(id.clone());
        if !scope_matches_root(&request.1, job) || !valid_successor(&state, job) {
            conflicts.push(format!("invalid_transition:{id}"));
            break;
        }
        let milestone = Milestone {
            event_id: id.clone(),
            created_at: event.created_at.as_secs(),
        };
        match job {
            JobEvent::Accepted(body) if body.claim.status == JobClaimStatus::Processed => {
                processed = Some(milestone);
                state = "processed".into();
            }
            JobEvent::Accepted(body) if body.claim.status == JobClaimStatus::Accepted => {
                accepted = Some(milestone);
                state = "accepted".into();
            }
            JobEvent::Accepted(_) => {
                terminal = Some(milestone);
                state = "declined".into();
            }
            JobEvent::Progress(body) => {
                state = match body.status {
                    JobProgressStatus::Progress => "progress",
                    JobProgressStatus::Blocked => "blocked",
                }
                .into();
            }
            JobEvent::Result(_) => {
                completed = Some(milestone.clone());
                terminal = Some(milestone);
                state = "completed".into();
            }
            JobEvent::Error(body) => {
                terminal = Some(milestone);
                state = match body.outcome {
                    buzz_core::job::JobErrorOutcome::Failed => "failed",
                    buzz_core::job::JobErrorOutcome::Indeterminate => "indeterminate",
                }
                .into();
            }
            JobEvent::Control(body) => {
                if body.action == JobControlAction::Cancel && body.followup.prior_event_id.is_some()
                {
                    state = "cancel_requested".into();
                } else {
                    terminal = Some(milestone);
                    state = if body.action == JobControlAction::Cancel {
                        "cancelled"
                    } else {
                        control_name(body.action)
                    }
                    .into();
                }
            }
            JobEvent::Request(_) => conflicts.push("request_in_followup_set".into()),
        }
        head = id;
    }
    let mut disconnected: Vec<String> = followers
        .iter()
        .map(|(event, _)| event.id.to_hex())
        .filter(|id| !seen_ids.contains(id))
        .collect();
    disconnected.sort();
    if !disconnected.is_empty() {
        conflicts.push(format!("disconnected_or_forked:{}", disconnected.join(",")));
    }
    event_ids.extend(disconnected);
    let signed_events: Vec<Event> = event_ids
        .iter()
        .map(|id| {
            if id == &root_id {
                Ok(request.0.clone())
            } else {
                events
                    .iter()
                    .find(|(event, _)| event.id.to_hex() == *id)
                    .map(|(event, _)| event.clone())
                    .ok_or_else(|| {
                        CliError::Other(format!(
                            "job projection lost signed event material for {id}"
                        ))
                    })
            }
        })
        .collect::<Result<_, _>>()?;
    if !conflicts.is_empty() {
        state = "conflict".into();
    }
    Ok(JobProjection {
        job_schema_version: JOB_SCHEMA_VERSION,
        operation_id: common.operation_id.clone(),
        coordinator_epoch: common.coordinator_epoch,
        request_event_id: root_id.clone(),
        project_address: common.project.address.clone(),
        home_channel: common.project.home_channel.clone(),
        repository: common.repository.canonical.clone(),
        sender_pubkey: common.sender_pubkey.clone(),
        recipient_pubkey: common.recipient_pubkey.clone(),
        relay: json!({"state": "stored", "event_id": root_id}),
        lifecycle: LifecycleProjection {
            state,
            processed,
            accepted,
            completed,
            terminal,
            conflict: !conflicts.is_empty(),
            conflicts,
        },
        event_ids,
        events: signed_events,
    })
}

fn scope_matches_root(root: &JobEvent, next: &JobEvent) -> bool {
    let root_common = root.common();
    let next_common = next.common();
    let scope_matches = root_common.operation_id == next_common.operation_id
        && root_common.idempotency_key == next_common.idempotency_key
        && root_common.coordinator_epoch == next_common.coordinator_epoch
        && root_common.project == next_common.project
        && root_common.repository == next_common.repository
        && root_common.expires_at == next_common.expires_at;
    let worker_to_requester = next_common.sender_pubkey == root_common.recipient_pubkey
        && next_common.recipient_pubkey == root_common.sender_pubkey;
    let requester_to_worker = next_common.sender_pubkey == root_common.sender_pubkey
        && next_common.recipient_pubkey == root_common.recipient_pubkey;
    scope_matches
        && match next {
            JobEvent::Accepted(_)
            | JobEvent::Progress(_)
            | JobEvent::Result(_)
            | JobEvent::Error(_) => worker_to_requester,
            JobEvent::Control(body) => match body.action {
                JobControlAction::Cancel => requester_to_worker,
                JobControlAction::Cancelled
                | JobControlAction::Release
                | JobControlAction::Handoff => worker_to_requester,
            },
            JobEvent::Request(_) => false,
        }
}

fn valid_successor(state: &str, next: &JobEvent) -> bool {
    match (state, next) {
        ("requested", JobEvent::Accepted(body))
            if body.claim.status == JobClaimStatus::Processed =>
        {
            true
        }
        ("requested", JobEvent::Accepted(body))
            if body.claim.status == JobClaimStatus::Declined =>
        {
            true
        }
        ("requested", JobEvent::Control(body))
            if body.action == JobControlAction::Cancel
                && body.followup.prior_event_id.is_none() =>
        {
            true
        }
        ("processed", JobEvent::Accepted(body))
            if body.claim.status == JobClaimStatus::Accepted =>
        {
            true
        }
        ("processed", JobEvent::Control(body)) if body.action == JobControlAction::Cancel => true,
        ("cancel_requested", JobEvent::Control(body))
            if body.action == JobControlAction::Cancelled =>
        {
            true
        }
        ("accepted" | "progress" | "blocked", JobEvent::Progress(_))
        | ("accepted" | "progress" | "blocked", JobEvent::Result(_))
        | ("accepted" | "progress" | "blocked", JobEvent::Error(_)) => true,
        ("accepted" | "progress" | "blocked", JobEvent::Control(body))
            if matches!(
                body.action,
                JobControlAction::Cancel | JobControlAction::Release | JobControlAction::Handoff
            ) =>
        {
            true
        }
        _ => false,
    }
}
