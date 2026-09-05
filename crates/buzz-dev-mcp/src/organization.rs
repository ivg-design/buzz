//! Typed, user-directed conversation organization. Registered inside `trusted`
//! with `#[path = "../organization.rs"] mod organization`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use buzz_core::organization::{self, OrganizationChange, OrganizationProjection};
use nostr::{Event, Timestamp};
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::tools::{error_result, json_result};
use super::TrustedRelay;

/// Paged history/search input; all reads use current authenticated channel rights.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OrganizationReadParams {
    /// Omit to use this agent's current conversation.
    pub channel_id: Option<String>,
    /// Read original messages or the separate reversible organization history.
    #[serde(default)]
    pub source: OrganizationHistorySource,
    /// Optional full-text search over original messages.
    pub search: Option<String>,
    /// Search uses relevance-ordered pages; omit for page one.
    pub search_page: Option<u32>,
    /// Optional original thread root, for message history only.
    pub thread_root_id: Option<String>,
    /// Include the effective persistent agent participants for thread_root_id.
    #[serde(default)]
    pub include_participants: bool,
    /// Cursor returned by an earlier page. Supply both cursor fields together.
    pub before_created_at: Option<u64>,
    /// Cursor returned by an earlier page.
    pub before_event_id: Option<String>,
    /// Page size, between 1 and 100.
    #[serde(default = "default_limit")]
    pub limit: u16,
}

/// Separate readable content from organization change records.
#[derive(Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrganizationHistorySource {
    /// Original signed messages, including their attachment and reply tags.
    #[default]
    Messages,
    /// Signed organization records, including all undo events.
    Changes,
}

/// One atomic organization action requested by the user.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OrganizationApplyParams {
    /// Omit to use this agent's current conversation.
    pub channel_id: Option<String>,
    /// Reversible display change. Original messages are never rewritten.
    pub action: OrganizationActionInput,
}

/// Closed tool schema matching the shared core wire contract.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OrganizationActionInput {
    /// Group selected messages and replies under an existing top-level root.
    Group {
        message_ids: Vec<String>,
        thread_root_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
    },
    /// Rename or summarize a thread separately from its original message.
    ThreadMetadata {
        thread_root_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
    },
    /// Replace the complete persistent agent participant list for a thread.
    Participants {
        thread_root_id: String,
        /// Verified enrolled agent public keys. An empty list removes all agents.
        agent_pubkeys: Vec<String>,
    },
    /// Archive/hide or restore chat, forum and legacy A2A entries and their replies.
    Hide {
        message_ids: Vec<String>,
        hidden: bool,
    },
    /// Undo the exact organization change, preserving all other changes.
    Undo { change_event_id: String },
}

/// Inspect/search history without losing original identity, attachments or links.
pub async fn organization_read(
    relay: &Arc<TrustedRelay>,
    params: OrganizationReadParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    match read(relay, params, &cancellation).await {
        Ok(value) => json_result(&value),
        Err(error) => error_result(error),
    }
}

/// Apply a user-requested reversible cleanup with normal channel authority.
/// There is intentionally no orchestrator tier, extra grant or approval step.
pub async fn organization_apply(
    relay: &Arc<TrustedRelay>,
    params: OrganizationApplyParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    let result = async {
        let channel = channel(relay, params.channel_id.as_deref())?;
        let action = serde_json::from_value(
            serde_json::to_value(params.action).map_err(|error| error.to_string())?,
        )
        .map_err(|error| format!("invalid organization action: {error}"))?;
        let change = OrganizationChange { version: 1, action };
        change.validate()?;
        let requested_participants = match &change.action {
            buzz_core::organization::OrganizationAction::Participants {
                thread_root_id,
                agent_pubkeys,
            } => {
                if !agent_pubkeys.is_empty() {
                    let peers = super::peers::discover_including_self(relay, &cancellation).await?;
                    validate_participant_roster(agent_pubkeys, &peers)?;
                }
                Some(thread_root_id.clone())
            }
            _ => None,
        };
        let latest = relay.query_signed_events(
            vec![serde_json::json!({
                "#h": [channel], "kinds": [buzz_core::kind::KIND_CONVERSATION_ORGANIZATION], "limit": 1,
            })],
            &cancellation,
        ).await?;
        let event = organization::build_change_event(
            channel, &change, &relay.keys, Timestamp::now().as_secs(), &latest,
        )?;
        let targets = relay
            .query_signed_events(
                vec![serde_json::json!({
                    "#h": [channel], "ids": change.references(),
                    "kinds": organization::ORGANIZABLE_MESSAGE_KINDS.iter().copied().chain(std::iter::once(buzz_core::kind::KIND_CONVERSATION_ORGANIZATION)).collect::<Vec<_>>(),
                    "limit": change.references().len(),
                })],
                &cancellation,
            )
            .await?;
        organization::validate_references(&event, &targets)?;
        let published = relay
            .publish_organization_event(event, &cancellation)
            .await?;
        let participants = if let Some(thread_root_id) = requested_participants {
            let changes = load_organization_history(relay, channel, &cancellation).await?;
            Some(effective_participants(
                channel,
                &thread_root_id,
                &changes,
                &targets,
            )?)
        } else {
            None
        };
        Ok::<_, String>(serde_json::json!({
            "event_id": published.event_id,
            "accepted": published.accepted,
            "channel_id": channel,
            "change": change,
            "participants": participants,
        }))
    }
    .await;
    match result {
        Ok(value) => json_result(&value),
        Err(error) => error_result(error),
    }
}

async fn read(
    relay: &TrustedRelay,
    params: OrganizationReadParams,
    cancellation: &CancellationToken,
) -> Result<serde_json::Value, String> {
    let channel = channel(relay, params.channel_id.as_deref())?;
    if params.include_participants && params.thread_root_id.is_none() {
        return Err("include_participants requires thread_root_id".into());
    }
    let filter = history_filter(channel, &params)?;
    relay.fresh_context(cancellation).await?;
    let mut filters = vec![filter];
    if let Some(root) = params.thread_root_id.as_deref() {
        filters.push(serde_json::json!({
            "#h": [channel], "ids": [root], "kinds": organization::ORGANIZABLE_MESSAGE_KINDS, "limit": 1,
        }));
    }
    let mut events = relay.query_signed_events(filters, cancellation).await?;
    // The relay owns access control; also reject an invalid/misrouted response
    // before passing it to an agent as the requested channel's history.
    for event in &events {
        if organization::event_channel(event)? != channel {
            return Err("relay history contained an unexpected channel".into());
        }
        match params.source {
            OrganizationHistorySource::Messages if !organization::is_organizable_message(event) => {
                return Err("relay history contained an unexpected event kind".into());
            }
            OrganizationHistorySource::Changes => {
                organization::parse_change(event)?;
            }
            _ => {}
        }
    }
    let thread_root = if let Some(root) = params.thread_root_id.as_deref() {
        let index = events
            .iter()
            .position(|event| event.id.to_hex() == root)
            .ok_or("thread root is unavailable in this channel")?;
        let event = events.remove(index);
        if buzz_core::nip10::parse_thread_markers(&event.tags)
            .resolve()
            .is_some()
        {
            return Err("thread_root_id must be an original top-level message".into());
        }
        Some(event)
    } else {
        None
    };
    if params.search.is_none() {
        events.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| a.id.cmp(&b.id))
        });
    }
    let has_more = if params.search.is_some() {
        events.len() == usize::from(params.limit)
    } else {
        events.len() > usize::from(params.limit)
    };
    events.truncate(usize::from(params.limit));
    let next_cursor = if has_more && params.search.is_none() {
        events.last().map(|event| {
            serde_json::json!({
                "before_created_at": event.created_at.as_secs(),
                "before_event_id": event.id.to_hex(),
            })
        })
    } else {
        None
    };
    let participants = if params.include_participants {
        let root = thread_root
            .as_ref()
            .ok_or("thread root is unavailable in this channel")?;
        let changes = load_organization_history(relay, channel, cancellation).await?;
        Some(effective_participants(
            channel,
            &root.id.to_hex(),
            &changes,
            std::slice::from_ref(root),
        )?)
    } else {
        None
    };
    Ok(serde_json::json!({
        "channel_id": channel,
        // Full signed source fields preserve author/time/id/imeta/reply links.
        "events": events,
        "thread_root": thread_root,
        "participants": participants,
        "has_more": has_more,
        "next_cursor": next_cursor,
        "next_search_page": if has_more && params.search.is_some() && params.search_page.unwrap_or(1) < 1000 {
            Some(params.search_page.unwrap_or(1) + 1)
        } else { None },
        "search_limit_reached": has_more && params.search.is_some() && params.search_page == Some(1000),
    }))
}

const ORGANIZATION_PAGE_SIZE: usize = 200;
const MAX_ORGANIZATION_PAGES: usize = 500;

/// Load complete signed change history so an effective policy cannot silently
/// ignore an older Participants record or its later Undo.
async fn load_organization_history(
    relay: &TrustedRelay,
    channel: uuid::Uuid,
    cancellation: &CancellationToken,
) -> Result<Vec<Event>, String> {
    let mut all = BTreeMap::new();
    let mut cursor: Option<(u64, String)> = None;
    for _ in 0..MAX_ORGANIZATION_PAGES {
        let mut filter = serde_json::json!({
            "#h": [channel],
            "kinds": [buzz_core::kind::KIND_CONVERSATION_ORGANIZATION],
            "limit": ORGANIZATION_PAGE_SIZE,
        });
        if let Some((created_at, event_id)) = cursor.as_ref() {
            filter["until"] = serde_json::json!(created_at);
            filter["before_id"] = serde_json::json!(event_id);
        }
        let batch = relay
            .query_signed_events(vec![filter], cancellation)
            .await?;
        for event in &batch {
            if organization::event_channel(event)? != channel {
                return Err("relay history contained an unexpected channel".into());
            }
            organization::parse_change(event)?;
            all.insert(event.id, event.clone());
        }
        if batch.len() < ORGANIZATION_PAGE_SIZE {
            return Ok(all.into_values().collect());
        }
        let oldest = batch
            .iter()
            .min_by(|a, b| {
                a.created_at
                    .cmp(&b.created_at)
                    .then_with(|| b.id.cmp(&a.id))
            })
            .ok_or("organization history page was unexpectedly empty")?;
        let next = (oldest.created_at.as_secs(), oldest.id.to_hex());
        if cursor.as_ref() == Some(&next) {
            return Err("organization history did not advance; retry the read".into());
        }
        cursor = Some(next);
    }
    Err("organization history exceeded the supported page limit".into())
}

fn effective_participants(
    channel: uuid::Uuid,
    thread_root_id: &str,
    changes: &[Event],
    messages: &[Event],
) -> Result<serde_json::Value, String> {
    let projection = OrganizationProjection::from_events(channel, changes, messages)?;
    let effective_thread_root_id = projection.effective_root(thread_root_id);
    let participants = projection.participants(&effective_thread_root_id);
    Ok(serde_json::json!({
        "thread_root_id": effective_thread_root_id,
        "configured": participants.is_some(),
        "agent_pubkeys": participants.unwrap_or_default(),
    }))
}

fn validate_participant_roster(
    requested: &[String],
    peers: &[super::peers::VerifiedPeer],
) -> Result<(), String> {
    let verified: BTreeSet<_> = peers.iter().map(|peer| peer.pubkey.as_str()).collect();
    if let Some(unknown) = requested
        .iter()
        .find(|pubkey| !verified.contains(pubkey.as_str()))
    {
        return Err(format!(
            "participant agent is not in the verified Nemo roster: {unknown}"
        ));
    }
    Ok(())
}

fn history_filter(
    channel: uuid::Uuid,
    params: &OrganizationReadParams,
) -> Result<serde_json::Value, String> {
    if !(1..=100).contains(&params.limit) {
        return Err("history limit must be between 1 and 100".into());
    }
    if params.before_created_at.is_some() != params.before_event_id.is_some() {
        return Err("provide both history cursor fields".into());
    }
    let mut filter = serde_json::json!({
        "#h": [channel],
        "kinds": if params.source == OrganizationHistorySource::Changes {
            vec![buzz_core::kind::KIND_CONVERSATION_ORGANIZATION]
        } else {
            organization::ORGANIZABLE_MESSAGE_KINDS.to_vec()
        },
        "limit": params.limit + 1,
    });
    if let Some(id) = params.before_event_id.as_deref() {
        validate_event_id(id)?;
        filter["before_id"] = serde_json::json!(id);
        filter["until"] = serde_json::json!(params.before_created_at);
    }
    if let Some(search) = params.search.as_deref() {
        if params.source == OrganizationHistorySource::Changes {
            return Err("search applies to messages; read changes without a search filter".into());
        }
        if search.trim().is_empty() || search.chars().count() > 512 {
            return Err("search must contain 1 to 512 characters".into());
        }
        if params.thread_root_id.is_some() || params.before_created_at.is_some() {
            return Err(
                "search uses channel relevance pages; omit thread and history cursor fields".into(),
            );
        }
        let page = params.search_page.unwrap_or(1);
        if !(1..=1000).contains(&page) {
            return Err("search_page must be between 1 and 1000".into());
        }
        filter["page"] = serde_json::json!(page);
        filter["limit"] = serde_json::json!(params.limit);
        filter["search"] = serde_json::json!(search);
    } else if params.search_page.is_some() {
        return Err("search_page requires search text".into());
    }
    if let Some(root) = params.thread_root_id.as_deref() {
        if params.source == OrganizationHistorySource::Changes {
            return Err("organization changes are read for the whole channel".into());
        }
        validate_event_id(root)?;
        filter["#e"] = serde_json::json!([root]);
    }
    Ok(filter)
}

fn channel(relay: &TrustedRelay, requested: Option<&str>) -> Result<uuid::Uuid, String> {
    match requested {
        Some(value) => uuid::Uuid::parse_str(value).map_err(|_| "invalid channel ID".into()),
        None => relay.bound_chat_channel(),
    }
}

fn validate_event_id(value: &str) -> Result<(), String> {
    let id = nostr::EventId::parse(value).map_err(|_| "invalid event ID".to_owned())?;
    if id.to_hex() != value {
        return Err("event ID must be lowercase hexadecimal".into());
    }
    Ok(())
}

fn default_limit() -> u16 {
    50
}

#[cfg(test)]
#[path = "organization_tests.rs"]
mod tests;
