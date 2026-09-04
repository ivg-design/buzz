use buzz_core::job::{
    build_job_tags, semantic_request_digest, JobControl, JobControlAction, JobEvent,
};
use buzz_core::kind::{
    KIND_JOB_ACCEPTED, KIND_JOB_CANCEL, KIND_JOB_ERROR, KIND_JOB_PROGRESS, KIND_JOB_REQUEST,
    KIND_JOB_RESULT,
};
use buzz_core::CommunityContext;
use nostr::{Event, EventBuilder, Kind};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use crate::client::BuzzClient;
use crate::error::CliError;
use crate::validate::read_or_stdin;

use super::query::query_events;
use super::{ParsedInput, CLI_RESULT_SCHEMA_VERSION};

pub(super) async fn publish_control(
    client: &BuzzClient,
    operation_id: &str,
    input: &str,
    expected: JobControlAction,
) -> Result<(), CliError> {
    let parsed: ParsedInput<JobControl> = parse_input(input, KIND_JOB_CANCEL)?;
    let body = parsed.body;
    require_operation(operation_id, &body.followup.common.operation_id)?;
    if body.action != expected {
        return Err(CliError::Usage(format!(
            "command requires action={}, got {}",
            control_name(expected),
            control_name(body.action)
        )));
    }
    publish(
        client,
        JobEvent::Control(body),
        &parsed.raw,
        Some(operation_id),
    )
    .await
}

pub(super) fn control_name(action: JobControlAction) -> &'static str {
    match action {
        JobControlAction::Cancel => "cancel",
        JobControlAction::Cancelled => "cancelled",
        JobControlAction::Release => "release",
        JobControlAction::Handoff => "handoff",
    }
}

pub(super) fn parse_input<T: DeserializeOwned>(
    input: &str,
    kind: u32,
) -> Result<ParsedInput<T>, CliError> {
    let raw = read_or_stdin(input)?;
    JobEvent::parse_content(kind, &raw)
        .map_err(|error| CliError::Usage(format!("invalid job JSON input: {error}")))?;
    let body = serde_json::from_str(&raw)
        .map_err(|error| CliError::Usage(format!("invalid job JSON input: {error}")))?;
    Ok(ParsedInput { body, raw })
}

pub(super) fn require_operation(flag: &str, body: &str) -> Result<(), CliError> {
    if flag != body {
        return Err(CliError::Usage(format!(
            "--operation-id {flag} does not match body operation_id {body}"
        )));
    }
    Ok(())
}

pub(super) async fn publish(
    client: &BuzzClient,
    job: JobEvent,
    raw_input: &str,
    operation_flag: Option<&str>,
) -> Result<(), CliError> {
    if let Some(flag) = operation_flag {
        require_operation(flag, &job.common().operation_id)?;
    }
    if job.common().sender_pubkey != client.keys().public_key().to_hex() {
        return Err(CliError::Auth(
            "sender_pubkey must match the CLI signing identity".into(),
        ));
    }
    let tags = build_job_tags(&job).map_err(|error| CliError::Usage(error.to_string()))?;
    let probe = client.sign_job_event(
        EventBuilder::new(Kind::Custom(job_kind(&job) as u16), raw_input).tags(tags.clone()),
    )?;
    JobEvent::parse(&probe).map_err(|error| CliError::Usage(error.to_string()))?;
    let canonical = job
        .canonical_json()
        .map_err(|error| CliError::Usage(error.to_string()))?;
    let event = client.sign_job_event(
        EventBuilder::new(Kind::Custom(job_kind(&job) as u16), canonical).tags(tags),
    )?;
    JobEvent::parse(&event).map_err(|error| CliError::Usage(error.to_string()))?;
    let community = client.community_context().await?;

    if let Some(existing) = find_existing_disposition(client, &event, &job).await? {
        print_delivery(&community, &job.common().operation_id, &existing, true);
        return Ok(());
    }

    let event_id = event.id.to_hex();
    let response = client.submit_event(event.clone()).await?;
    ensure_delivered(&response, &event_id)?;
    print_delivery(&community, &job.common().operation_id, &event, false);
    Ok(())
}

pub(super) fn ensure_delivered(response: &str, expected_event_id: &str) -> Result<(), CliError> {
    let value: Value = serde_json::from_str(response)
        .map_err(|error| CliError::Other(format!("invalid relay response: {error}")))?;
    if value.get("accepted").and_then(Value::as_bool) != Some(true) {
        let message = value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("relay rejected job event");
        return Err(CliError::Conflict(message.to_owned()));
    }
    if value.get("event_id").and_then(Value::as_str) != Some(expected_event_id) {
        return Err(CliError::DeliveryUnknown(
            "relay response did not confirm the submitted event ID".into(),
        ));
    }
    Ok(())
}

fn print_delivery(community: &CommunityContext, operation_id: &str, event: &Event, replayed: bool) {
    println!(
        "{}",
        json!({
            "schema_version": CLI_RESULT_SCHEMA_VERSION,
            "operation_id": operation_id,
            "community": community,
            "relay": {"state": "stored", "event_id": event.id.to_hex()},
            "lifecycle": Value::Null,
            "replayed": replayed,
            "event": event,
            "authority": repository_authority_boundary(),
        })
    );
}

pub(super) fn repository_authority_boundary() -> Value {
    json!({
        "repository_scope": "unverified",
        "note": "relay storage does not authorize repository scope; receivers must allowlist project.address, project.home_channel, and repository.canonical",
    })
}

async fn find_existing_disposition(
    client: &BuzzClient,
    event: &Event,
    job: &JobEvent,
) -> Result<Option<Event>, CliError> {
    let mut filter = json!({
        "kinds": [job_kind(job)],
        "authors": [event.pubkey.to_hex()],
        "limit": 500,
    });
    if let Some(root) = job.request_event_id() {
        filter["#e"] = json!([root]);
    } else {
        filter["#k"] = json!([job.common().idempotency_key]);
    }
    let events = query_events(client, filter).await?;
    let canonical = job
        .canonical_json()
        .map_err(|error| CliError::Usage(error.to_string()))?;
    let mut parsed_events = Vec::new();
    for candidate in events {
        if let Ok(parsed) = JobEvent::parse(&candidate) {
            if parsed.common().idempotency_key == job.common().idempotency_key
                && parsed.common().coordinator_epoch == job.common().coordinator_epoch
            {
                parsed_events.push((candidate, parsed));
            }
        }
    }
    for (candidate, parsed) in &parsed_events {
        let candidate_json = parsed
            .canonical_json()
            .map_err(|error| CliError::Other(error.to_string()))?;
        if candidate_json == canonical {
            return Ok(Some(candidate.clone()));
        }
    }
    if parsed_events
        .iter()
        .any(|(_, parsed)| same_transition_slot(parsed, job))
    {
        return Err(CliError::Conflict(
            "transition slot already has a different canonical job payload".into(),
        ));
    }
    Ok(None)
}

pub(super) fn same_transition_slot(left: &JobEvent, right: &JobEvent) -> bool {
    match (left, right) {
        (JobEvent::Request(a), JobEvent::Request(b)) => {
            a.common.coordinator_epoch == b.common.coordinator_epoch
        }
        (JobEvent::Accepted(a), JobEvent::Accepted(b)) => {
            a.followup.request_event_id == b.followup.request_event_id
                && a.followup.prior_event_id == b.followup.prior_event_id
                && a.claim.status == b.claim.status
        }
        (JobEvent::Progress(a), JobEvent::Progress(b)) => {
            a.followup.request_event_id == b.followup.request_event_id
                && a.followup.prior_event_id == b.followup.prior_event_id
        }
        (JobEvent::Result(a), JobEvent::Result(b)) => {
            a.followup.request_event_id == b.followup.request_event_id
                && a.followup.prior_event_id == b.followup.prior_event_id
        }
        (JobEvent::Control(a), JobEvent::Control(b)) => {
            a.followup.request_event_id == b.followup.request_event_id
                && a.followup.prior_event_id == b.followup.prior_event_id
                && a.action == b.action
        }
        (JobEvent::Error(a), JobEvent::Error(b)) => {
            a.followup.request_event_id == b.followup.request_event_id
                && a.followup.prior_event_id == b.followup.prior_event_id
        }
        _ => false,
    }
}

pub(super) async fn request_scope_digest(
    client: &BuzzClient,
    request_id: &str,
) -> Result<String, CliError> {
    let filter = json!({"ids": [request_id], "kinds": [KIND_JOB_REQUEST], "limit": 2});
    let events = query_events(client, filter).await?;
    let request = events
        .first()
        .ok_or_else(|| CliError::NotFound(format!("job request {request_id} not found")))?;
    let parsed = JobEvent::parse(request)
        .map_err(|error| CliError::Other(format!("stored request is invalid: {error}")))?;
    let JobEvent::Request(request) = parsed else {
        return Err(CliError::Other(
            "request_event_id did not resolve to kind 43001".into(),
        ));
    };
    semantic_request_digest(&request).map_err(|error| CliError::Other(error.to_string()))
}

pub(super) fn job_kind(job: &JobEvent) -> u32 {
    match job {
        JobEvent::Request(_) => KIND_JOB_REQUEST,
        JobEvent::Accepted(_) => KIND_JOB_ACCEPTED,
        JobEvent::Progress(_) => KIND_JOB_PROGRESS,
        JobEvent::Result(_) => KIND_JOB_RESULT,
        JobEvent::Control(_) => KIND_JOB_CANCEL,
        JobEvent::Error(_) => KIND_JOB_ERROR,
    }
}
