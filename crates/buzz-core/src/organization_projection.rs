//! Shared organization replay for consumers of the immutable event history.

use std::collections::{BTreeMap, BTreeSet};

use nostr::{Event, EventId};
use uuid::Uuid;

use super::{event_channel, is_organizable_message, parse_change, OrganizationAction};

#[derive(Debug, Clone)]
struct RankedRoot {
    value: String,
    rank: usize,
}

#[derive(Debug, Clone)]
struct Ancestry {
    root: String,
    parent: String,
}

/// Effective grouping and agent participation after ordered replay and Undo.
///
/// This projection never modifies or republishes a source event. Hidden content
/// remains in its conversation: visibility is a display choice, not a change to
/// participant authority or a request to interrupt already running work.
#[derive(Debug, Clone, Default)]
pub struct OrganizationProjection {
    channel_id: Option<Uuid>,
    groups: BTreeMap<String, RankedRoot>,
    participants: BTreeMap<String, Vec<String>>,
    ancestry: BTreeMap<String, Ancestry>,
    message_ids: BTreeSet<EventId>,
}

impl OrganizationProjection {
    /// Replay the authenticated relay's complete channel organization history.
    ///
    /// Signatures, wire shape, channel scope and Undo references are checked
    /// here. The relay must already have validated actor/channel rights,
    /// participant enrollment and original message references at ingress.
    /// Supply original messages including ancestry and grouping destinations;
    /// root tags bridge missing intermediate parents just as in the desktop.
    /// Missing history or ancestry can produce an incomplete projection.
    pub fn from_events(
        channel_id: Uuid,
        changes: &[Event],
        messages: &[Event],
    ) -> Result<Self, String> {
        let mut records = BTreeMap::new();
        for event in changes {
            crate::verify_event(event).map_err(|_| "invalid organization history signature")?;
            let (channel, change) = parse_change(event)?;
            if channel != channel_id {
                return Err("organization history contained an unexpected channel".into());
            }
            if matches!(change.action, OrganizationAction::Undo { .. }) {
                super::validate_references(event, changes)?;
            }
            records.insert((event.created_at, event.id), change);
        }
        let undone: BTreeSet<_> = records
            .values()
            .filter_map(|change| match &change.action {
                OrganizationAction::Undo { change_event_id } => Some(change_event_id.as_str()),
                _ => None,
            })
            .collect();
        let mut projection = Self::default();
        for (rank, ((_, id), change)) in records.iter().enumerate() {
            if undone.contains(id.to_hex().as_str()) {
                continue;
            }
            match &change.action {
                OrganizationAction::Group {
                    message_ids,
                    thread_root_id,
                    ..
                } => {
                    // The destination's self-write detaches any older move of
                    // that root and makes chained moves converge without cycles.
                    for id in message_ids.iter().chain(std::iter::once(thread_root_id)) {
                        projection.groups.insert(
                            id.clone(),
                            RankedRoot {
                                value: thread_root_id.clone(),
                                rank,
                            },
                        );
                    }
                }
                OrganizationAction::Participants {
                    thread_root_id,
                    agent_pubkeys,
                } => {
                    projection
                        .participants
                        .insert(thread_root_id.clone(), agent_pubkeys.clone());
                }
                _ => {}
            }
        }
        projection.add_messages(channel_id, messages)?;
        Ok(projection)
    }

    /// Add newly received original messages without replaying organization changes.
    ///
    /// Only unseen event IDs are verified and indexed; already verified sources
    /// are immutable. The entire batch must have valid signatures, message kinds
    /// and this projection's channel before any ancestry is inserted. An error
    /// leaves the projection unchanged, including its previously resolved policy.
    pub fn add_messages(&mut self, channel_id: Uuid, messages: &[Event]) -> Result<(), String> {
        if self
            .channel_id
            .is_some_and(|expected| expected != channel_id)
        {
            return Err("organization sources belong to an unexpected projection channel".into());
        }
        let mut message_ids = BTreeSet::new();
        let mut ancestry = BTreeMap::new();
        for event in messages {
            if self.message_ids.contains(&event.id) || !message_ids.insert(event.id) {
                continue;
            }
            crate::verify_event(event).map_err(|_| "invalid organization source signature")?;
            if event_channel(event)? != channel_id || !is_organizable_message(event) {
                return Err(
                    "organization sources must be conversation messages in this channel".into(),
                );
            }
            if let Some((root, parent)) = crate::nip10::parse_thread_markers(&event.tags).resolve()
            {
                ancestry.insert(event.id.to_hex(), Ancestry { root, parent });
            }
        }
        self.channel_id = Some(channel_id);
        self.message_ids.extend(message_ids);
        self.ancestry.extend(ancestry);
        Ok(())
    }

    /// Return the effective grouping root using original reply ancestry.
    ///
    /// A newer move of any ancestor wins for the subtree; destination moves are
    /// followed transitively. With no move, this returns the original root tag
    /// or the message itself. This matches the desktop organization projection,
    /// independently of message arrival order and display visibility.
    pub fn effective_root(&self, message_id: &str) -> String {
        let mut current = self
            .newest_group(message_id)
            .map(|entry| entry.value.as_str())
            .or_else(|| {
                self.ancestry
                    .get(message_id)
                    .map(|ancestry| ancestry.root.as_str())
            })
            .unwrap_or(message_id);
        let mut visited = BTreeSet::from([message_id]);
        while visited.insert(current) {
            let Some(next) = self.newest_group(current) else {
                break;
            };
            if next.value == current {
                break;
            }
            current = &next.value;
        }
        current.to_owned()
    }

    /// Read the complete policy for an already resolved effective thread root.
    ///
    /// `None` means no policy was set (or its changes were undone); `Some(&[])`
    /// explicitly removes all joined agents. Call [`Self::effective_root`] first
    /// so a moved subtree uses its destination's policy, not its source's policy.
    pub fn participants(&self, effective_thread_root_id: &str) -> Option<&[String]> {
        self.participants
            .get(effective_thread_root_id)
            .map(Vec::as_slice)
    }

    fn newest_group<'a>(&'a self, message_id: &'a str) -> Option<&'a RankedRoot> {
        let mut winner: Option<&RankedRoot> = None;
        let mut visited = BTreeSet::new();
        let mut current = Some(message_id);
        while let Some(id) = current.filter(|id| visited.insert(*id)) {
            if let Some(entry) = self.groups.get(id) {
                if winner.is_none_or(|previous| entry.rank > previous.rank) {
                    winner = Some(entry);
                }
            }
            current = match self.ancestry.get(id) {
                Some(ancestry) if !visited.contains(ancestry.parent.as_str()) => {
                    // Preserve the root's move even if the parent is not loaded.
                    if let Some(entry) = self.groups.get(&ancestry.root) {
                        if winner.is_none_or(|previous| entry.rank > previous.rank) {
                            winner = Some(entry);
                        }
                    }
                    Some(ancestry.parent.as_str())
                }
                Some(ancestry) if ancestry.root != id => Some(ancestry.root.as_str()),
                _ => None,
            };
        }
        winner
    }
}
