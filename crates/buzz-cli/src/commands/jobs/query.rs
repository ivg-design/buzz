use std::collections::BTreeMap;

use base64::Engine;
use buzz_core::job::JobEvent;
use buzz_core::kind::{
    KIND_JOB_ACCEPTED, KIND_JOB_CANCEL, KIND_JOB_ERROR, KIND_JOB_PROGRESS, KIND_JOB_REQUEST,
    KIND_JOB_RESULT,
};
use nostr::Event;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::client::BuzzClient;
use crate::error::CliError;

use super::projection::project;
use super::publish::repository_authority_boundary;
use super::{CAPABILITY_DISCOVERY, CLI_RESULT_SCHEMA_VERSION, JOB_QUERY_BOUND};

pub(super) async fn list(
    client: &BuzzClient,
    project_address: Option<String>,
    recipient: Option<String>,
    state: Option<String>,
    cursor: Option<String>,
) -> Result<(), CliError> {
    let community = client.community_context().await?;
    let events = if let Some(recipient) = recipient {
        let pubkey = if recipient == "self" {
            client.keys().public_key().to_hex()
        } else {
            let parsed = nostr::PublicKey::parse(&recipient)
                .map_err(|_| CliError::Usage("--recipient must be self or a public key".into()))?;
            parsed.to_hex()
        };
        participant_job_events(client, project_address.as_deref(), &pubkey).await?
    } else {
        let mut filter = all_job_filter();
        if let Some(address) = &project_address {
            filter["#a"] = json!([address]);
        }
        query_events(client, filter).await?
    };
    let next_cursor = history_cursor(&events)?;
    let unchanged = match cursor {
        Some(cursor) => decode_cursor(&cursor)? == next_cursor,
        None => false,
    };
    let mut jobs = project(events)?;
    if unchanged {
        jobs.clear();
    }
    if let Some(expected) = state {
        jobs.retain(|job| job.lifecycle.state == expected);
    }
    println!(
        "{}",
        json!({
            "schema_version": CLI_RESULT_SCHEMA_VERSION,
            "community": community,
            "jobs": jobs,
            "count": jobs.len(),
            "unchanged": unchanged,
            "next_cursor": encode_cursor(&next_cursor)?,
            "authority": repository_authority_boundary(),
        })
    );
    Ok(())
}

async fn participant_job_events(
    client: &BuzzClient,
    project_address: Option<&str>,
    pubkey: &str,
) -> Result<Vec<Event>, CliError> {
    let mut addressed = json!({
        "kinds": [KIND_JOB_REQUEST],
        "#p": [pubkey],
        "limit": 500,
    });
    let mut authored = json!({
        "kinds": [KIND_JOB_REQUEST],
        "authors": [pubkey],
        "limit": 500,
    });
    if let Some(address) = project_address {
        addressed["#a"] = json!([address]);
        authored["#a"] = json!([address]);
    }
    let mut roots = query_events(client, addressed).await?;
    roots.extend(query_events(client, authored).await?);
    roots.sort_by_key(|event| event.id.to_hex());
    roots.dedup_by_key(|event| event.id);
    if roots.is_empty() {
        return Ok(Vec::new());
    }
    let root_ids: Vec<String> = roots.iter().map(|event| event.id.to_hex()).collect();
    let mut followups = query_events(
        client,
        json!({
            "kinds": [
                KIND_JOB_ACCEPTED,
                KIND_JOB_PROGRESS,
                KIND_JOB_RESULT,
                KIND_JOB_CANCEL,
                KIND_JOB_ERROR,
            ],
            "#e": root_ids,
            "limit": 500,
        }),
    )
    .await?;
    roots.append(&mut followups);
    Ok(roots)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub(super) struct HistoryCursor {
    event_count: usize,
    event_ids_sha256: String,
}

pub(super) fn history_cursor(events: &[Event]) -> Result<HistoryCursor, CliError> {
    let mut ids: Vec<String> = events.iter().map(|event| event.id.to_hex()).collect();
    ids.sort();
    let bytes = serde_json::to_vec(&ids)
        .map_err(|error| CliError::Other(format!("encoding history cursor: {error}")))?;
    Ok(HistoryCursor {
        event_count: ids.len(),
        event_ids_sha256: hex::encode(Sha256::digest(bytes)),
    })
}

pub(super) fn encode_cursor(cursor: &HistoryCursor) -> Result<String, CliError> {
    let bytes = serde_json::to_vec(cursor)
        .map_err(|error| CliError::Other(format!("encoding history cursor: {error}")))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

pub(super) fn decode_cursor(cursor: &str) -> Result<HistoryCursor, CliError> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| CliError::Usage("--cursor is not a valid Buzz jobs cursor".into()))?;
    let decoded: HistoryCursor = serde_json::from_slice(&bytes)
        .map_err(|_| CliError::Usage("--cursor is not a valid Buzz jobs cursor".into()))?;
    if decoded.event_ids_sha256.len() != 64
        || !decoded
            .event_ids_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CliError::Usage(
            "--cursor is not a valid Buzz jobs cursor".into(),
        ));
    }
    Ok(decoded)
}

pub(super) async fn get(client: &BuzzClient, operation_id: &str) -> Result<(), CliError> {
    let community = client.community_context().await?;
    let operation = uuid::Uuid::parse_str(operation_id)
        .map_err(|_| CliError::Usage("--operation-id must be a UUID".into()))?;
    if operation.to_string() != operation_id {
        return Err(CliError::Usage(
            "--operation-id must use canonical UUID spelling".into(),
        ));
    }
    let mut filter = all_job_filter();
    filter["#i"] = json!([operation_id]);
    let events = query_events(client, filter).await?;
    let mut jobs = project(events)?;
    let job = jobs
        .pop()
        .ok_or_else(|| CliError::NotFound(format!("job operation {operation_id} not found")))?;
    if jobs.iter().any(|other| other.operation_id == operation_id) {
        return Err(CliError::Conflict(
            "operation has multiple unlinked request roots".into(),
        ));
    }
    println!(
        "{}",
        json!({
            "schema_version": CLI_RESULT_SCHEMA_VERSION,
            "community": community,
            "job": job,
            "authority": repository_authority_boundary(),
        })
    );
    Ok(())
}

/// Query capability advertisements from successful capability-discovery jobs.
pub async fn capabilities(client: &BuzzClient, project_address: String) -> Result<(), CliError> {
    let community = client.community_context().await?;
    let mut filter = json!({"kinds": [KIND_JOB_RESULT], "limit": 500});
    filter["#a"] = json!([project_address]);
    let events = query_events(client, filter).await?;
    let mut by_agent: BTreeMap<String, (u64, String, Value)> = BTreeMap::new();
    for event in events {
        let parsed = JobEvent::parse(&event)
            .map_err(|error| CliError::Other(format!("stored job result is invalid: {error}")))?;
        let JobEvent::Result(result) = parsed else {
            return Err(CliError::Other(
                "result query returned a non-result job event".into(),
            ));
        };
        if result.capabilities.is_empty() {
            continue;
        }
        let expiry = chrono::DateTime::parse_from_rfc3339(&result.followup.common.expires_at)
            .map_err(|_| CliError::Other("stored capability result has invalid expiry".into()))?
            .with_timezone(&chrono::Utc);
        if expiry <= chrono::Utc::now() {
            continue;
        }
        let request_id = &result.followup.request_event_id;
        let roots = query_events(
            client,
            json!({"ids": [request_id], "kinds": [KIND_JOB_REQUEST], "limit": 2}),
        )
        .await?;
        if roots.len() != 1 {
            return Err(CliError::Conflict(format!(
                "capability result {} has no unique request root",
                event.id.to_hex()
            )));
        }
        let root = JobEvent::parse(&roots[0]).map_err(|error| {
            CliError::Other(format!("capability request root is invalid: {error}"))
        })?;
        let JobEvent::Request(request) = root else {
            return Err(CliError::Other(
                "capability result root is not a job request".into(),
            ));
        };
        if request.capability != CAPABILITY_DISCOVERY
            || request.common.operation_id != result.followup.common.operation_id
            || request.common.idempotency_key != result.followup.common.idempotency_key
            || request.common.coordinator_epoch != result.followup.common.coordinator_epoch
            || request.common.project != result.followup.common.project
            || request.common.repository != result.followup.common.repository
            || request.common.expires_at != result.followup.common.expires_at
            || request.common.recipient_pubkey != result.followup.common.sender_pubkey
            || request.common.sender_pubkey != result.followup.common.recipient_pubkey
        {
            return Err(CliError::Conflict(format!(
                "capability result {} is not an authorized capability-discovery completion",
                event.id.to_hex()
            )));
        }
        let mut closure = roots;
        closure.extend(
            query_events(
                client,
                json!({
                    "kinds": [
                        KIND_JOB_ACCEPTED,
                        KIND_JOB_PROGRESS,
                        KIND_JOB_RESULT,
                        KIND_JOB_CANCEL,
                        KIND_JOB_ERROR,
                    ],
                    "#e": [request_id],
                    "limit": 500,
                }),
            )
            .await?,
        );
        let projections = project(closure)?;
        let projection = projections
            .iter()
            .find(|projection| projection.request_event_id == *request_id)
            .ok_or_else(|| {
                CliError::Conflict(format!(
                    "capability result {} has no complete lifecycle projection",
                    event.id.to_hex()
                ))
            })?;
        if projection.lifecycle.conflict
            || projection.lifecycle.state != "completed"
            || projection
                .lifecycle
                .completed
                .as_ref()
                .map(|milestone| milestone.event_id.as_str())
                != Some(event.id.to_hex().as_str())
        {
            return Err(CliError::Conflict(format!(
                "capability result {} is not the verified terminal completion",
                event.id.to_hex()
            )));
        }
        let mut values = result.capabilities;
        values.sort();
        values.dedup();
        let event_id = event.id.to_hex();
        let epoch = result.followup.common.coordinator_epoch;
        let created_at = event.created_at.as_secs();
        let agent_pubkey = result.followup.common.sender_pubkey.clone();
        let advertisement = json!({
            "pubkey": agent_pubkey,
            "event_id": event_id,
            "request_event_id": request_id,
            "coordinator_epoch": epoch,
            "project": result.followup.common.project,
            "repository": result.followup.common.repository,
            "expires_at": result.followup.common.expires_at,
            "capabilities": values,
            "dispatch_eligible": false,
            "authority_state": "historical_unverified",
        });
        let key = agent_pubkey;
        let replace = by_agent
            .get(&key)
            .is_none_or(|(prior_created_at, prior_id, _)| {
                (created_at, &event_id) > (*prior_created_at, prior_id)
            });
        if replace {
            by_agent.insert(key, (created_at, event_id, advertisement));
        }
    }
    let agents: Vec<Value> = by_agent
        .into_iter()
        .map(|(_, (_, _, advertisement))| advertisement)
        .collect();
    println!(
        "{}",
        json!({
            "schema_version": CLI_RESULT_SCHEMA_VERSION,
            "community": community,
            "agents": agents,
            "authority": repository_authority_boundary(),
        })
    );
    Ok(())
}

pub(super) async fn query_events(
    client: &BuzzClient,
    filter: Value,
) -> Result<Vec<Event>, CliError> {
    let raw = client.query_all_bounded(filter, JOB_QUERY_BOUND).await?;
    raw.into_iter()
        .map(|value| {
            let event: Event = serde_json::from_value(value)
                .map_err(|error| CliError::Other(format!("invalid event from relay: {error}")))?;
            buzz_core::verify_event(&event).map_err(|error| {
                CliError::Other(format!("relay returned invalid event: {error}"))
            })?;
            Ok(event)
        })
        .collect()
}

pub(super) fn all_job_filter() -> Value {
    json!({
        "kinds": [
            KIND_JOB_REQUEST,
            KIND_JOB_ACCEPTED,
            KIND_JOB_PROGRESS,
            KIND_JOB_RESULT,
            KIND_JOB_CANCEL,
            KIND_JOB_ERROR,
        ],
        "limit": 500,
    })
}
