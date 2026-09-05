//! Ordinary kind-9 conversation events, with routing metadata outside the instruction.

use super::types::{TaskInput, TimedTask};
use nostr::{Event, EventId, Keys, Tag};

fn tag(parts: &[&str]) -> Result<Tag, String> {
    Tag::parse(parts.iter().copied()).map_err(|e| e.to_string())
}

pub fn root(id: &str, input: &TaskInput, keys: &Keys, relay: &str) -> Result<Event, String> {
    let summary: String = input
        .instruction
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("Scheduled instruction")
        .trim()
        .chars()
        .take(100)
        .collect();
    let mut builder = crate::events::build_message(
        uuid::Uuid::parse_str(&input.channel_id).map_err(|e| e.to_string())?,
        &format!("Timed task: {summary}"),
        None,
        &[],
        &[],
        &[],
        &[],
        &[],
        None,
        relay,
    )?
    .tag(tag(&["buzz-task", "schedule", id])?);
    if let Some(origin) = &input.origin_event_id {
        builder = builder.tag(tag(&["buzz-origin", origin])?);
    }
    builder.sign_with_keys(keys).map_err(|e| e.to_string())
}

pub fn occurrence(task: &TimedTask, id: &str, keys: &Keys) -> Result<Event, String> {
    let thread = EventId::from_hex(&task.thread_id).map_err(|e| e.to_string())?;
    crate::events::build_message(
        uuid::Uuid::parse_str(&task.input.channel_id).map_err(|e| e.to_string())?,
        &task.input.instruction,
        Some(&crate::events::ThreadRef {
            root_event_id: thread,
            parent_event_id: thread,
        }),
        &[&task.input.recipient_pubkey],
        &[],
        &[],
        &[],
        &[],
        None,
        &task.relay_url,
    )?
    .tag(tag(&["buzz-task", "scheduled", &task.id, id])?)
    .sign_with_keys(keys)
    .map_err(|e| e.to_string())
}
