//! Persistent agent participation in ordinary human conversation threads.

use std::collections::{HashMap, HashSet, VecDeque};

use buzz_core::organization::{is_organizable_message, OrganizationProjection};
use nostr::Event;
use serde_json::json;
use uuid::Uuid;

use crate::relay::RestClient;

const MAX_CACHED_CHANNELS: usize = 32;
const MAX_RECENT_ANCESTRY: usize = 512;
const MAX_ANCESTRY_DEPTH: usize = 128;
const MAX_ORGANIZATION_CHANGES: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParticipationDecision {
    pub effective_root: String,
    pub grouped: bool,
    /// `None` is legacy/unconfigured. `Some(false)` is an explicit policy that
    /// does not include this agent; direct mentions still use normal routing.
    pub included: Option<bool>,
}

#[derive(Debug)]
struct CachedChannel {
    /// Complete organization history, bounded independently from chat volume.
    changes: Vec<Event>,
    /// Only recent ancestry sources. Older parents are fetched by exact id when
    /// a future reply needs them; fetched history is never enqueued as a turn.
    messages: HashMap<String, Event>,
    message_order: VecDeque<String>,
    projection: OrganizationProjection,
    touched: u64,
}

/// Bounded, authenticated channel-history projection.
///
/// The initial read loads organization records only. A live message adds itself
/// and the exact parent chain needed to resolve its subtree, so unrelated channel
/// history cannot disable participant routing or be replayed to the provider.
#[derive(Debug, Default)]
pub(crate) struct ThreadParticipation {
    channels: HashMap<Uuid, CachedChannel>,
    clock: u64,
}

impl ThreadParticipation {
    /// Process a live organization event before the passive-record gate.
    pub(crate) async fn observe_change(
        &mut self,
        channel_id: Uuid,
        event: &Event,
        rest: &RestClient,
    ) -> Result<(), String> {
        self.ensure_loaded(channel_id, rest).await?;
        let cached = self
            .channels
            .get_mut(&channel_id)
            .ok_or("thread participation history is unavailable")?;
        if cached
            .changes
            .iter()
            .any(|existing| existing.id == event.id)
        {
            return Ok(());
        }
        if cached.changes.len() >= MAX_ORGANIZATION_CHANGES {
            return Err(format!(
                "conversation organization history exceeds the {MAX_ORGANIZATION_CHANGES}-event runtime bound"
            ));
        }
        let mut changes = cached.changes.clone();
        changes.push(event.clone());
        let messages = cached.messages.values().cloned().collect::<Vec<_>>();
        let projection = OrganizationProjection::from_events(channel_id, &changes, &messages)?;
        cached.changes = changes;
        cached.projection = projection;
        cached.touched = self.clock;
        Ok(())
    }

    /// Resolve a newly received kind-9 message against current grouping and
    /// participant policy. Top-level messages update sparse ancestry but do not
    /// wake persistent participants by themselves.
    pub(crate) async fn resolve_message(
        &mut self,
        channel_id: Uuid,
        event: &Event,
        agent_pubkey: &str,
        rest: &RestClient,
    ) -> Result<Option<ParticipationDecision>, String> {
        if event.kind.as_u16() != buzz_core::kind::KIND_STREAM_MESSAGE as u16 {
            return Ok(None);
        }
        self.ensure_loaded(channel_id, rest).await?;
        let original_root = original_thread_root(event);
        let ancestry = self.hydrate_ancestry(channel_id, event, rest).await?;
        self.add_messages(channel_id, ancestry)?;
        let Some(original_root) = original_root else {
            return Ok(None);
        };
        self.resolve_cached(channel_id, &event.id.to_hex(), &original_root, agent_pubkey)
            .map(Some)
    }

    fn resolve_cached(
        &self,
        channel_id: Uuid,
        message_id: &str,
        original_root: &str,
        agent_pubkey: &str,
    ) -> Result<ParticipationDecision, String> {
        let cached = self
            .channels
            .get(&channel_id)
            .ok_or("thread participation cache was evicted")?;
        // Resolve the incoming message, not its original root: an organization
        // move may select an intermediate child and therefore its whole subtree.
        let effective_root = cached.projection.effective_root(message_id);
        let grouped = effective_root != original_root;
        let included = cached
            .projection
            .participants(&effective_root)
            .map(|participants| participants.iter().any(|key| key == agent_pubkey));
        Ok(ParticipationDecision {
            effective_root,
            grouped,
            included,
        })
    }

    async fn ensure_loaded(&mut self, channel_id: Uuid, rest: &RestClient) -> Result<(), String> {
        self.clock = self.clock.wrapping_add(1);
        if let Some(cached) = self.channels.get_mut(&channel_id) {
            cached.touched = self.clock;
            return Ok(());
        }
        let raw = rest
            .query_raw_all(organization_history_filter(channel_id))
            .await
            .map_err(|error| error.to_string())?;
        if raw.len() > MAX_ORGANIZATION_CHANGES {
            return Err(format!(
                "conversation organization history exceeds the {MAX_ORGANIZATION_CHANGES}-event runtime bound"
            ));
        }
        let mut changes = Vec::with_capacity(raw.len());
        for value in raw {
            let event: Event = serde_json::from_value(value).map_err(|error| error.to_string())?;
            if event.kind.as_u16() != buzz_core::kind::KIND_CONVERSATION_ORGANIZATION as u16 {
                return Err("organization history query returned an unexpected event kind".into());
            }
            changes.push(event);
        }
        let projection = OrganizationProjection::from_events(channel_id, &changes, &[])?;
        self.evict_channel_if_needed();
        self.channels.insert(
            channel_id,
            CachedChannel {
                changes,
                messages: HashMap::new(),
                message_order: VecDeque::new(),
                projection,
                touched: self.clock,
            },
        );
        Ok(())
    }

    async fn hydrate_ancestry(
        &self,
        channel_id: Uuid,
        event: &Event,
        rest: &RestClient,
    ) -> Result<Vec<Event>, String> {
        let mut ancestry = vec![event.clone()];
        let Some((_, mut parent_id)) = thread_markers(event) else {
            return Ok(ancestry);
        };
        let mut visited = HashSet::from([event.id.to_hex()]);
        for _ in 0..MAX_ANCESTRY_DEPTH {
            if !visited.insert(parent_id.clone()) {
                break;
            }
            let parent = match self
                .channels
                .get(&channel_id)
                .and_then(|cached| cached.messages.get(&parent_id))
            {
                Some(event) => event.clone(),
                None => match fetch_message(channel_id, &parent_id, rest).await? {
                    Some(event) => event,
                    // Root tags let the shared projection resolve a root move
                    // even when relay retention no longer has every parent.
                    None => break,
                },
            };
            let next_parent = thread_markers(&parent).map(|(_, parent)| parent);
            ancestry.push(parent);
            let Some(next_parent) = next_parent else {
                break;
            };
            parent_id = next_parent;
        }
        Ok(ancestry)
    }

    fn add_messages(&mut self, channel_id: Uuid, messages: Vec<Event>) -> Result<(), String> {
        let cached = self
            .channels
            .get_mut(&channel_id)
            .ok_or("thread participation history is unavailable")?;
        let new_messages = messages
            .into_iter()
            .filter(|event| !cached.messages.contains_key(&event.id.to_hex()))
            .collect::<Vec<_>>();
        if new_messages.is_empty() {
            return Ok(());
        }
        cached.projection.add_messages(channel_id, &new_messages)?;
        let mut evicted = false;
        for event in new_messages {
            let id = event.id.to_hex();
            cached.message_order.push_back(id.clone());
            cached.messages.insert(id, event);
            while cached.messages.len() > MAX_RECENT_ANCESTRY {
                if let Some(oldest) = cached.message_order.pop_front() {
                    cached.messages.remove(&oldest);
                    evicted = true;
                }
            }
        }
        if evicted {
            let retained = cached.messages.values().cloned().collect::<Vec<_>>();
            cached.projection =
                OrganizationProjection::from_events(channel_id, &cached.changes, &retained)?;
        }
        cached.touched = self.clock;
        Ok(())
    }

    fn evict_channel_if_needed(&mut self) {
        if self.channels.len() < MAX_CACHED_CHANNELS {
            return;
        }
        if let Some(oldest) = self
            .channels
            .iter()
            .min_by_key(|(_, cached)| cached.touched)
            .map(|(channel, _)| *channel)
        {
            self.channels.remove(&oldest);
        }
    }
}

fn organization_history_filter(channel_id: Uuid) -> serde_json::Value {
    json!({
        "kinds": [buzz_core::kind::KIND_CONVERSATION_ORGANIZATION],
        "#h": [channel_id.to_string()],
    })
}

async fn fetch_message(
    channel_id: Uuid,
    event_id: &str,
    rest: &RestClient,
) -> Result<Option<Event>, String> {
    let filter = json!({
        "ids": [event_id],
        "kinds": [
            buzz_core::kind::KIND_STREAM_MESSAGE,
            buzz_core::kind::KIND_STREAM_MESSAGE_V2,
            buzz_core::kind::KIND_FORUM_POST,
            buzz_core::kind::KIND_FORUM_COMMENT,
        ],
        "#h": [channel_id.to_string()],
        "limit": 2,
    });
    let raw = rest
        .query_raw(std::slice::from_ref(&filter))
        .await
        .map_err(|error| error.to_string())?;
    let values = raw
        .as_array()
        .ok_or("message ancestry query response is not an array")?;
    if values.len() > 1 {
        return Err("message ancestry query returned duplicate event ids".into());
    }
    let Some(value) = values.first() else {
        return Ok(None);
    };
    let event: Event = serde_json::from_value(value.clone()).map_err(|error| error.to_string())?;
    if event.id.to_hex() != event_id.to_ascii_lowercase() || !is_organizable_message(&event) {
        return Err("message ancestry query returned an unexpected event".into());
    }
    Ok(Some(event))
}

fn thread_markers(event: &Event) -> Option<(String, String)> {
    buzz_core::nip10::parse_thread_markers(&event.tags)
        .resolve()
        .map(|(root, parent)| (root.to_ascii_lowercase(), parent.to_ascii_lowercase()))
}

fn original_thread_root(event: &Event) -> Option<String> {
    thread_markers(event).map(|(root, _)| root)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;
    use axum::extract::State;
    use axum::routing::post;
    use axum::{Json, Router};
    use buzz_core::organization::{build_change_event, OrganizationAction, OrganizationChange};
    use nostr::{EventBuilder, Keys, Kind, Tag};

    fn message(
        channel: Uuid,
        keys: &Keys,
        content: &str,
        root_and_parent: Option<(&str, &str)>,
    ) -> Event {
        let mut tags = vec![Tag::parse(["h", channel.to_string().as_str()]).unwrap()];
        if let Some((root, parent)) = root_and_parent {
            tags.push(Tag::parse(["e", root, "", "root"]).unwrap());
            tags.push(Tag::parse(["e", parent, "", "reply"]).unwrap());
        }
        EventBuilder::new(Kind::Custom(9), content)
            .tags(tags)
            .sign_with_keys(keys)
            .unwrap()
    }

    fn change(channel: Uuid, keys: &Keys, action: OrganizationAction, observed: &[Event]) -> Event {
        build_change_event(
            channel,
            &OrganizationChange { version: 1, action },
            keys,
            1_000,
            observed,
        )
        .unwrap()
    }

    fn cached(channel: Uuid, changes: Vec<Event>, messages: Vec<Event>) -> ThreadParticipation {
        let projection = OrganizationProjection::from_events(channel, &changes, &messages).unwrap();
        let message_order = messages.iter().map(|event| event.id.to_hex()).collect();
        let messages = messages
            .into_iter()
            .map(|event| (event.id.to_hex(), event))
            .collect();
        ThreadParticipation {
            channels: HashMap::from([(
                channel,
                CachedChannel {
                    changes,
                    messages,
                    message_order,
                    projection,
                    touched: 1,
                },
            )]),
            clock: 1,
        }
    }

    #[test]
    fn grouped_child_routes_future_subtree_to_destination_policy() {
        let channel = Uuid::new_v4();
        let human = Keys::generate();
        let organizer = Keys::generate();
        let agent = Keys::generate().public_key().to_hex();
        let original = message(channel, &human, "original", None);
        let selected_child = message(
            channel,
            &human,
            "selected child",
            Some((&original.id.to_hex(), &original.id.to_hex())),
        );
        let destination = message(channel, &human, "destination", None);
        let future_reply = message(
            channel,
            &human,
            "future reply",
            Some((&original.id.to_hex(), &selected_child.id.to_hex())),
        );
        let group = change(
            channel,
            &organizer,
            OrganizationAction::Group {
                message_ids: vec![selected_child.id.to_hex()],
                thread_root_id: destination.id.to_hex(),
                title: None,
                summary: None,
            },
            &[],
        );
        let joined = change(
            channel,
            &organizer,
            OrganizationAction::Participants {
                thread_root_id: destination.id.to_hex(),
                agent_pubkeys: vec![agent.clone()],
            },
            std::slice::from_ref(&group),
        );
        let state = cached(
            channel,
            vec![group, joined],
            vec![
                original.clone(),
                selected_child,
                destination.clone(),
                future_reply.clone(),
            ],
        );
        assert_eq!(
            state
                .resolve_cached(
                    channel,
                    &future_reply.id.to_hex(),
                    &original.id.to_hex(),
                    &agent,
                )
                .unwrap(),
            ParticipationDecision {
                effective_root: destination.id.to_hex(),
                grouped: true,
                included: Some(true),
            }
        );
    }

    #[derive(Clone)]
    struct RelayState {
        changes: Arc<Vec<Event>>,
        messages: Arc<HashMap<String, Event>>,
        broad_message_query: Arc<AtomicBool>,
        exact_message_queries: Arc<AtomicUsize>,
        unrelated_message_count: usize,
    }

    async fn query(
        State(state): State<RelayState>,
        Json(filters): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        let filter = &filters[0];
        let kinds = filter["kinds"].as_array().unwrap();
        let ids = filter.get("ids").and_then(serde_json::Value::as_array);
        if kinds.iter().any(|kind| kind.as_u64() == Some(9)) && ids.is_none() {
            state.broad_message_query.store(true, Ordering::SeqCst);
            assert!(state.unrelated_message_count > 4_096);
            return Json(json!([]));
        }
        if let Some(ids) = ids {
            state.exact_message_queries.fetch_add(1, Ordering::SeqCst);
            let events = ids
                .iter()
                .filter_map(serde_json::Value::as_str)
                .filter_map(|id| state.messages.get(id))
                .collect::<Vec<_>>();
            return Json(serde_json::to_value(events).unwrap());
        }
        Json(serde_json::to_value(state.changes.as_ref()).unwrap())
    }

    #[tokio::test]
    async fn large_unrelated_chat_history_does_not_block_sparse_subtree_routing() {
        let channel = Uuid::new_v4();
        let human = Keys::generate();
        let organizer = Keys::generate();
        let agent = Keys::generate().public_key().to_hex();
        let original = message(channel, &human, "original", None);
        let selected_child = message(
            channel,
            &human,
            "selected child",
            Some((&original.id.to_hex(), &original.id.to_hex())),
        );
        let destination = message(channel, &human, "destination", None);
        let future_reply = message(
            channel,
            &human,
            "future reply",
            Some((&original.id.to_hex(), &selected_child.id.to_hex())),
        );
        let group = change(
            channel,
            &organizer,
            OrganizationAction::Group {
                message_ids: vec![selected_child.id.to_hex()],
                thread_root_id: destination.id.to_hex(),
                title: None,
                summary: None,
            },
            &[],
        );
        let joined = change(
            channel,
            &organizer,
            OrganizationAction::Participants {
                thread_root_id: destination.id.to_hex(),
                agent_pubkeys: vec![agent.clone()],
            },
            std::slice::from_ref(&group),
        );
        let state = RelayState {
            changes: Arc::new(vec![group, joined]),
            messages: Arc::new(HashMap::from([
                (original.id.to_hex(), original),
                (selected_child.id.to_hex(), selected_child),
                (destination.id.to_hex(), destination.clone()),
            ])),
            broad_message_query: Arc::new(AtomicBool::new(false)),
            exact_message_queries: Arc::new(AtomicUsize::new(0)),
            unrelated_message_count: 4_097,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_state = state.clone();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/query", post(query))
                    .with_state(server_state),
            )
            .await
            .unwrap();
        });
        let rest = RestClient {
            http: reqwest::Client::new(),
            base_url: format!("http://{address}"),
            keys: Keys::generate(),
            auth_tag_json: None,
        };
        let decision = ThreadParticipation::default()
            .resolve_message(channel, &future_reply, &agent, &rest)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            decision,
            ParticipationDecision {
                effective_root: destination.id.to_hex(),
                grouped: true,
                included: Some(true),
            }
        );
        assert!(!state.broad_message_query.load(Ordering::SeqCst));
        assert_eq!(state.exact_message_queries.load(Ordering::SeqCst), 2);
    }
}
