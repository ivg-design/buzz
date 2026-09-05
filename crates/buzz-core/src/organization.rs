//! Reversible conversation organization over immutable signed messages.
//!
//! Each event is one atomic change. Clients replay changes in `(created_at, id)`
//! order after excluding undo targets. No original event, attachment or NIP-10
//! relationship is rewritten. Grouping and visibility inherit through original
//! reply ancestry, including replies that arrive after the organization change.

use std::collections::BTreeSet;

use nostr::{Event, EventBuilder, Keys, Kind, Tag, Timestamp};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::kind::{
    event_kind_u32, KIND_CONVERSATION_ORGANIZATION, KIND_FORUM_COMMENT, KIND_FORUM_POST,
    KIND_STREAM_MESSAGE, KIND_STREAM_MESSAGE_V2,
};

/// Maximum explicit message references in one atomic change.
pub const MAX_ORGANIZATION_MESSAGES: usize = 100;

/// Build and sign a change after the latest authoritative channel history.
/// Sequential actions must not depend on random event-ID ordering within a
/// second. Independent concurrent actions can still share a timestamp and use
/// the deterministic ID tie-break. Both desktop and agent producers use this.
pub fn build_change_event(
    channel: Uuid,
    change: &OrganizationChange,
    keys: &Keys,
    now: u64,
    observed_changes: &[Event],
) -> Result<Event, String> {
    change.validate()?;
    let mut created_at = now;
    for previous in observed_changes {
        crate::verify_event(previous).map_err(|_| "invalid organization history signature")?;
        if parse_change(previous)?.0 != channel {
            return Err("organization history contained an unexpected channel".into());
        }
        let next = previous
            .created_at
            .as_secs()
            .checked_add(1)
            .ok_or("organization history timestamp is out of range")?;
        created_at = created_at.max(next);
    }
    // The relay accepts +/-900s. Keep generated logical timestamps comfortably
    // inside that existing window, rather than signing arbitrary future dates.
    if created_at > now.saturating_add(300) {
        return Err(
            "cannot order organization change within the relay timestamp window; try again shortly"
                .into(),
        );
    }
    let content = serde_json::to_string(change).map_err(|error| error.to_string())?;
    let event = EventBuilder::new(Kind::Custom(KIND_CONVERSATION_ORGANIZATION as u16), content)
        .tags([Tag::parse(["h", &channel.to_string()]).map_err(|error| error.to_string())?])
        .custom_created_at(Timestamp::from(created_at))
        .sign_with_keys(keys)
        .map_err(|error| format!("failed to sign organization change: {error}"))?;
    parse_change(&event)?;
    Ok(event)
}

/// Versioned content of a channel-scoped organization event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrganizationChange {
    /// Wire schema version; currently one.
    pub version: u8,
    /// One reversible operation, applied atomically.
    pub action: OrganizationAction,
}

/// A display projection operation; signed source events remain untouched.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OrganizationAction {
    /// Group messages and their replies under an existing top-level message.
    Group {
        /// Source message IDs, without the destination root.
        message_ids: Vec<String>,
        /// Existing top-level destination; it becomes visible as a thread root.
        thread_root_id: String,
        /// Optional display title, separate from original message content.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// Optional readable summary, separate from original message content.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
    },
    /// Set a thread's display title and/or summary without editing its author.
    ThreadMetadata {
        /// Original top-level thread message.
        thread_root_id: String,
        /// New title; omission preserves the current title.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// New summary; an empty string clears it, omission preserves it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
    },
    /// Hide or restore messages and their descendants in the organized view.
    Hide {
        /// Explicit messages whose original reply descendants inherit the value.
        message_ids: Vec<String>,
        /// True hides clutter; false restores it.
        hidden: bool,
    },
    /// Undo one earlier change; other and later changes remain in effect.
    Undo {
        /// Existing organization change in this channel, excluding another undo.
        change_event_id: String,
    },
}

impl OrganizationChange {
    /// Validate the bounded wire shape before any storage/network lookup.
    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1 {
            return Err("unsupported organization version".into());
        }
        match &self.action {
            OrganizationAction::Group {
                message_ids,
                thread_root_id,
                title,
                summary,
            } => {
                validate_ids(message_ids)?;
                validate_id(thread_root_id)?;
                if message_ids.contains(thread_root_id) {
                    return Err("the destination root must not be in message_ids".into());
                }
                validate_metadata(title.as_deref(), summary.as_deref())?;
            }
            OrganizationAction::ThreadMetadata {
                thread_root_id,
                title,
                summary,
            } => {
                validate_id(thread_root_id)?;
                if title.is_none() && summary.is_none() {
                    return Err("provide a title or summary".into());
                }
                validate_metadata(title.as_deref(), summary.as_deref())?;
            }
            OrganizationAction::Hide { message_ids, .. } => validate_ids(message_ids)?,
            OrganizationAction::Undo { change_event_id } => validate_id(change_event_id)?,
        }
        Ok(())
    }

    /// Exact event IDs a relay must resolve in its current community/channel.
    pub fn references(&self) -> Vec<&str> {
        match &self.action {
            OrganizationAction::Group {
                message_ids,
                thread_root_id,
                ..
            } => message_ids
                .iter()
                .map(String::as_str)
                .chain(std::iter::once(thread_root_id.as_str()))
                .collect(),
            OrganizationAction::ThreadMetadata { thread_root_id, .. } => vec![thread_root_id],
            OrganizationAction::Hide { message_ids, .. } => {
                message_ids.iter().map(String::as_str).collect()
            }
            OrganizationAction::Undo { change_event_id } => vec![change_event_id],
        }
    }
}

/// Parse and validate a signed organization event, including exact channel tags.
/// The ingest/auth layer verifies the signature and authenticated channel rights.
pub fn parse_change(event: &Event) -> Result<(Uuid, OrganizationChange), String> {
    if event_kind_u32(event) != KIND_CONVERSATION_ORGANIZATION {
        return Err("expected an organization change".into());
    }
    // Organization references live in the body, not NIP-10 reply tags. Prevent
    // routing/expiration tags from turning durable change history into messages
    // or silently removing the only undo record.
    if event.tags.iter().any(|tag| {
        !matches!(
            tag.as_slice().first().map(String::as_str),
            Some("h" | "auth" | "delegation")
        )
    }) {
        return Err("organization changes allow only channel and authentication tags".into());
    }
    let channel = event_channel(event)?;
    if event.content.len() > 24_000 {
        return Err("organization change exceeds 24000 bytes".into());
    }
    let change: OrganizationChange = serde_json::from_str(&event.content)
        .map_err(|_| "invalid organization change JSON".to_owned())?;
    change.validate()?;
    Ok((channel, change))
}

/// Return the exact single canonical channel tag, rejecting ambiguous scopes.
pub fn event_channel(event: &Event) -> Result<Uuid, String> {
    let mut channels = event
        .tags
        .iter()
        .filter(|tag| tag.as_slice().first().is_some_and(|name| name == "h"));
    let tag = channels.next().ok_or("missing organization channel")?;
    if tag.as_slice().len() != 2 || channels.next().is_some() {
        return Err("expected exactly one channel tag".into());
    }
    let raw = &tag.as_slice()[1];
    let channel = Uuid::parse_str(raw).map_err(|_| "invalid channel ID".to_owned())?;
    if channel.to_string() != *raw {
        return Err("channel ID must be canonical".into());
    }
    Ok(channel)
}

/// Supported original message kinds; routing/job/tool events are not clutter.
pub fn is_organizable_message(event: &Event) -> bool {
    matches!(
        event_kind_u32(event),
        KIND_STREAM_MESSAGE | KIND_STREAM_MESSAGE_V2 | KIND_FORUM_POST | KIND_FORUM_COMMENT
    )
}

/// Validate resolved references without mutating them. Reads must come from the
/// authenticated relay's current community, never from caller-supplied events.
pub fn validate_references(
    event: &Event,
    references: &[Event],
) -> Result<OrganizationChange, String> {
    let (channel, change) = parse_change(event)?;
    for id in change.references() {
        let target = references
            .iter()
            .find(|candidate| candidate.id.to_hex() == id)
            .ok_or("organization target is unavailable in this channel")?;
        if event_channel(target)? != channel {
            return Err("organization target is unavailable in this channel".into());
        }
        match &change.action {
            OrganizationAction::Undo { .. } => {
                let (_, previous) = parse_change(target)?;
                if matches!(previous.action, OrganizationAction::Undo { .. }) {
                    return Err("an undo record cannot itself be undone; apply a new change".into());
                }
                if target.created_at > event.created_at {
                    return Err("cannot undo a change from the future".into());
                }
            }
            _ => {
                if !is_organizable_message(target) {
                    return Err("organization targets must be conversation messages".into());
                }
                let root = match &change.action {
                    OrganizationAction::Group { thread_root_id, .. }
                    | OrganizationAction::ThreadMetadata { thread_root_id, .. } => {
                        Some(thread_root_id.as_str())
                    }
                    _ => None,
                };
                if root == Some(id)
                    && crate::nip10::parse_thread_markers(&target.tags)
                        .resolve()
                        .is_some()
                {
                    return Err("the destination must be a top-level thread message".into());
                }
            }
        }
    }
    Ok(change)
}

fn validate_id(id: &str) -> Result<(), String> {
    if id.len() != 64
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("event IDs must be 64 lowercase hexadecimal characters".into());
    }
    Ok(())
}

fn validate_ids(ids: &[String]) -> Result<(), String> {
    if ids.is_empty() || ids.len() > MAX_ORGANIZATION_MESSAGES {
        return Err("select between 1 and 100 messages".into());
    }
    let mut unique = BTreeSet::new();
    for id in ids {
        validate_id(id)?;
        if !unique.insert(id) {
            return Err("message_ids must not contain duplicates".into());
        }
    }
    Ok(())
}

fn validate_metadata(title: Option<&str>, summary: Option<&str>) -> Result<(), String> {
    if title.is_some_and(|title| title.trim().is_empty() || title.chars().count() > 160) {
        return Err("thread title must contain 1 to 160 characters".into());
    }
    if summary.is_some_and(|summary| summary.chars().count() > 8000) {
        return Err("thread summary must contain at most 8000 characters".into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "organization_tests.rs"]
mod tests;
