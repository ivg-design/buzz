//! Session-bound chat threads and correlated peer questions.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use buzz_core::job::{JobConversation, JobEvent};
use nostr::{Event, EventId, Tag};
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::tools::{error_result, json_result, publish_result};
use super::{TrustedRelay, VerifiedPeer};

const MAX_RECIPIENTS: usize = 50;
const MAX_HISTORY_EVENTS: u16 = 50;
const MAX_WAIT_SECONDS: u8 = 60;
const DEFAULT_HISTORY_LIMIT: u16 = 20;
const DEFAULT_WAIT_SECONDS: u8 = 30;
const PEER_TAG: &str = "buzz-peer";
const QUESTION_TAG: &str = "question";
const REPLY_TAG: &str = "reply";
const TASK_TAG: &str = "buzz-task";
const TASK_ASSIGNMENT_TAG: &str = "assignment";
const MAX_TASK_ROOT_LOOKBACK: usize = 100;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChatSendParams {
    pub content: String,
    /// Optional existing channel. Omit to use the ACP session channel.
    #[serde(default)]
    pub channel_id: Option<String>,
    /// Optional canonical thread root. Omit to use the current task thread (or
    /// the current timeline when this session has no thread binding).
    #[serde(default)]
    pub thread_root_id: Option<String>,
    /// Enrolled agent public keys to address with signed `p` tags.
    #[serde(default)]
    pub recipient_pubkeys: Vec<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChatThreadCreateParams {
    pub content: String,
    /// Optional existing channel. Omit to use the ACP session channel.
    #[serde(default)]
    pub channel_id: Option<String>,
    /// Enrolled agent public keys to address on the new thread root.
    #[serde(default)]
    pub recipient_pubkeys: Vec<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChatReadParams {
    /// Optional existing channel. Omit to use the ACP session channel.
    #[serde(default)]
    pub channel_id: Option<String>,
    /// Optional canonical root. Omit to read the current task thread.
    #[serde(default)]
    pub thread_root_id: Option<String>,
    #[serde(default = "default_history_limit")]
    pub limit: u16,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PeerAskParams {
    pub recipient_pubkey: String,
    pub question: String,
    /// Optional existing channel. Omit to use the ACP session channel.
    #[serde(default)]
    pub channel_id: Option<String>,
    /// Optional canonical task thread. Omit to use the current task thread.
    #[serde(default)]
    pub thread_root_id: Option<String>,
    /// Await a correlated answer for at most this many seconds. Zero publishes
    /// the question and returns pending immediately.
    #[serde(default = "default_wait_seconds")]
    pub wait_seconds: u8,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PeerWaitParams {
    pub request_event_id: String,
    #[serde(default = "default_wait_seconds")]
    pub wait_seconds: u8,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PeerReplyParams {
    pub request_event_id: String,
    pub answer: String,
}

#[derive(Serialize)]
struct SafeChatEvent {
    event_id: String,
    kind: u32,
    author: String,
    created_at: u64,
    content: String,
    thread_root_id: String,
    parent_event_id: Option<String>,
    recipient_pubkeys: Vec<String>,
}

#[derive(Clone)]
struct PeerQuestion {
    request_id: String,
    event_id: String,
    author: String,
    recipient: String,
    channel: uuid::Uuid,
    thread_root_id: String,
}

#[derive(Serialize)]
struct PeerAnswer {
    request_id: String,
    request_event_id: String,
    response_event_id: String,
    author: String,
    answer: String,
    thread_root_id: String,
}

impl TrustedRelay {
    /// Resolve one existing oversight thread or publish a visible, idempotent
    /// task root before its machine-readable job request can be sent.
    pub async fn prepare_visible_task_thread(
        &self,
        channel_id: Option<&str>,
        thread_root_id: Option<&str>,
        operation_id: &str,
        content: &str,
        cancellation: &CancellationToken,
    ) -> Result<JobConversation, String> {
        let channel = validated_channel(self, channel_id, &[], cancellation).await?;
        if let Some(root) = thread_root_id {
            load_thread_root(self, root, Some(channel), cancellation).await?;
            return Ok(JobConversation {
                channel_id: channel.to_string(),
                thread_root_id: root.to_owned(),
            });
        }
        validate_operation_id(operation_id)?;
        self.fresh_context(cancellation).await?;
        let mut existing = self
            .query_signed_events(
                vec![serde_json::json!({
                    "authors": [self.signer_pubkey()],
                    "kinds": [buzz_core::kind::KIND_STREAM_MESSAGE],
                    "#h": [channel.to_string()],
                    "#i": [operation_id],
                    "limit": MAX_TASK_ROOT_LOOKBACK,
                })],
                cancellation,
            )
            .await?
            .into_iter()
            .filter(|event| valid_task_assignment_root(event, self, channel, operation_id))
            .collect::<Vec<_>>();
        existing.sort_by_key(|event| (event.created_at.as_secs(), event.id.to_hex()));
        if let Some(root) = existing.first() {
            return Ok(JobConversation {
                channel_id: channel.to_string(),
                thread_root_id: root.id.to_hex(),
            });
        }

        let markers = task_assignment_tags(operation_id)?;
        let event = self.build_scoped_chat_event(channel, content, None, &[], &markers)?;
        let thread_root_id = event.id.to_hex();
        self.publish_chat_event(event, cancellation).await?;
        Ok(JobConversation {
            channel_id: channel.to_string(),
            thread_root_id,
        })
    }
}

pub(super) async fn validate_existing_conversation(
    relay: &TrustedRelay,
    channel_id: &str,
    thread_root_id: Option<&str>,
    cancellation: &CancellationToken,
) -> Result<(), String> {
    let channel = validated_channel(relay, Some(channel_id), &[], cancellation).await?;
    if let Some(root) = thread_root_id {
        load_thread_root(relay, root, Some(channel), cancellation).await?;
    }
    Ok(())
}

pub async fn send_chat(
    relay: &Arc<TrustedRelay>,
    params: ChatSendParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    let result = async {
        let recipients =
            verified_recipients(relay, &params.recipient_pubkeys, &cancellation).await?;
        let channel = validated_channel(
            relay,
            params.channel_id.as_deref(),
            &recipients,
            &cancellation,
        )
        .await?;
        let (default_channel, default_root) = relay.current_chat_destination_parts()?;
        let channel_id = channel.to_string();
        let root = match params.thread_root_id {
            Some(root) => Some(root),
            None if default_channel.as_deref() == Some(channel_id.as_str()) => default_root,
            None => None,
        };
        if let Some(root) = root.as_deref() {
            load_thread_root(relay, root, Some(channel), &cancellation).await?;
        }
        let thread = root.as_deref().map(direct_thread_ref).transpose()?;
        let mention_refs = recipients
            .iter()
            .map(|peer| peer.pubkey.as_str())
            .collect::<Vec<_>>();
        let event = relay.build_scoped_chat_event(
            channel,
            &params.content,
            thread.as_ref(),
            &mention_refs,
            &[],
        )?;
        relay.publish_chat_event(event, &cancellation).await
    }
    .await;
    publish_result(result)
}

pub async fn create_thread(
    relay: &Arc<TrustedRelay>,
    params: ChatThreadCreateParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    let result = async {
        let recipients =
            verified_recipients(relay, &params.recipient_pubkeys, &cancellation).await?;
        let channel = validated_channel(
            relay,
            params.channel_id.as_deref(),
            &recipients,
            &cancellation,
        )
        .await?;
        let mention_refs = recipients
            .iter()
            .map(|peer| peer.pubkey.as_str())
            .collect::<Vec<_>>();
        let event =
            relay.build_scoped_chat_event(channel, &params.content, None, &mention_refs, &[])?;
        let root_event_id = event.id.to_hex();
        let published = relay.publish_chat_event(event, &cancellation).await?;
        Ok::<_, String>((channel, root_event_id, published))
    }
    .await;
    match result {
        Ok((channel, root_event_id, published)) => json_result(&serde_json::json!({
            "channel_id": channel,
            "root_event_id": root_event_id,
            "event_id": published.event_id,
            "accepted": published.accepted,
        })),
        Err(error) => error_result(error),
    }
}

pub async fn read_chat(
    relay: &Arc<TrustedRelay>,
    params: ChatReadParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    let result = read_thread(relay, params, &cancellation).await;
    match result {
        Ok((root_event_id, events)) => json_result(&serde_json::json!({
            "thread_root_id": root_event_id,
            "events": events,
        })),
        Err(error) => error_result(error),
    }
}

pub async fn ask_peer(
    relay: &Arc<TrustedRelay>,
    params: PeerAskParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    if params.wait_seconds > MAX_WAIT_SECONDS {
        return error_result("wait_seconds must be between 0 and 60".into());
    }
    let result = async {
        let recipients = verified_recipients(
            relay,
            std::slice::from_ref(&params.recipient_pubkey),
            &cancellation,
        )
        .await?;
        let recipient = recipients
            .first()
            .ok_or_else(|| "peer question requires one recipient".to_owned())?;
        let channel = validated_channel(
            relay,
            params.channel_id.as_deref(),
            &recipients,
            &cancellation,
        )
        .await?;
        let (default_channel, default_root) = relay.current_chat_destination_parts()?;
        let channel_id = channel.to_string();
        let current_root = match params.thread_root_id {
            Some(root) => Some(root),
            None if default_channel.as_deref() == Some(channel_id.as_str()) => default_root,
            None => None,
        };
        if let Some(root) = current_root.as_deref() {
            load_thread_root(relay, root, Some(channel), &cancellation).await?;
        }
        let thread = current_root.as_deref().map(direct_thread_ref).transpose()?;
        let request_id = uuid::Uuid::new_v4().to_string();
        let peer_tag = peer_tag(QUESTION_TAG, &request_id)?;
        let event = relay.build_scoped_chat_event(
            channel,
            &params.question,
            thread.as_ref(),
            &[recipient.pubkey.as_str()],
            &[peer_tag],
        )?;
        let request_event_id = event.id.to_hex();
        let thread_root_id = current_root.unwrap_or_else(|| request_event_id.clone());
        relay.publish_chat_event(event, &cancellation).await?;
        let answer =
            await_peer_answer(relay, &request_event_id, params.wait_seconds, &cancellation).await?;
        Ok::<_, String>((request_id, request_event_id, thread_root_id, answer))
    }
    .await;
    match result {
        Ok((request_id, request_event_id, thread_root_id, Some(answer))) => {
            json_result(&serde_json::json!({
                "status": "answered",
                "request_id": request_id,
                "request_event_id": request_event_id,
                "thread_root_id": thread_root_id,
                "response": answer,
            }))
        }
        Ok((request_id, request_event_id, thread_root_id, None)) => {
            json_result(&serde_json::json!({
                "status": "pending",
                "request_id": request_id,
                "request_event_id": request_event_id,
                "thread_root_id": thread_root_id,
            }))
        }
        Err(error) => error_result(error),
    }
}

pub async fn wait_for_peer(
    relay: &Arc<TrustedRelay>,
    params: PeerWaitParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    if params.wait_seconds > MAX_WAIT_SECONDS {
        return error_result("wait_seconds must be between 0 and 60".into());
    }
    match await_peer_answer(
        relay,
        &params.request_event_id,
        params.wait_seconds,
        &cancellation,
    )
    .await
    {
        Ok(Some(answer)) => json_result(&serde_json::json!({
            "status": "answered",
            "request_event_id": params.request_event_id,
            "response": answer,
        })),
        Ok(None) => json_result(&serde_json::json!({
            "status": "pending",
            "request_event_id": params.request_event_id,
        })),
        Err(error) => error_result(error),
    }
}

pub async fn reply_to_peer(
    relay: &Arc<TrustedRelay>,
    params: PeerReplyParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    let result = async {
        let question = load_peer_question(relay, &params.request_event_id, &cancellation).await?;
        if question.recipient != relay.signer_pubkey() {
            return Err("peer question is not addressed to this agent".into());
        }
        ensure_enrolled_peer(relay, &question.author, &cancellation).await?;
        let thread = thread_ref(&question.thread_root_id, &question.event_id)?;
        let event = relay.build_scoped_chat_event(
            question.channel,
            &params.answer,
            Some(&thread),
            &[question.author.as_str()],
            &[peer_tag(REPLY_TAG, &question.request_id)?],
        )?;
        relay.publish_chat_event(event, &cancellation).await
    }
    .await;
    publish_result(result)
}

async fn read_thread(
    relay: &TrustedRelay,
    params: ChatReadParams,
    cancellation: &CancellationToken,
) -> Result<(String, Vec<SafeChatEvent>), String> {
    if !(1..=MAX_HISTORY_EVENTS).contains(&params.limit) {
        return Err("limit must be between 1 and 50".into());
    }
    let root_id = match params.thread_root_id {
        Some(root) => root,
        None => relay
            .current_chat_thread_root()?
            .ok_or_else(|| "this session has no current task thread".to_owned())?,
    };
    let channel = validated_channel(relay, params.channel_id.as_deref(), &[], cancellation).await?;
    let root = load_thread_root(relay, &root_id, Some(channel), cancellation).await?;
    let reply_limit = params.limit.saturating_sub(1);
    let mut replies = if reply_limit == 0 {
        Vec::new()
    } else {
        relay
            .query_signed_events(
                vec![serde_json::json!({
                    "#e": [root_id],
                    "#h": [channel.to_string()],
                    "kinds": [9, 43002, 43003, 43004, 43005, 43006],
                    "limit": reply_limit,
                })],
                cancellation,
            )
            .await?
    };
    replies.retain(|event| valid_thread_reply(event, channel, &root_id));
    replies.sort_by_key(|event| (event.created_at.as_secs(), event.id.to_hex()));
    if replies.len() > usize::from(reply_limit) {
        replies = replies.split_off(replies.len() - usize::from(reply_limit));
    }
    let mut events = Vec::with_capacity(replies.len() + 1);
    events.push(safe_chat_event(&root, &root_id)?);
    for event in replies {
        events.push(safe_chat_event(&event, &root_id)?);
    }
    Ok((root_id, events))
}

async fn await_peer_answer(
    relay: &TrustedRelay,
    request_event_id: &str,
    wait_seconds: u8,
    cancellation: &CancellationToken,
) -> Result<Option<PeerAnswer>, String> {
    let question = load_peer_question(relay, request_event_id, cancellation).await?;
    if question.author != relay.signer_pubkey() {
        return Err("only the question author may wait for its answer".into());
    }
    ensure_enrolled_peer(relay, &question.recipient, cancellation).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(u64::from(wait_seconds));
    loop {
        if let Some(answer) = query_peer_answer(relay, &question, cancellation).await? {
            return Ok(Some(answer));
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Ok(None);
        }
        let pause = (deadline - now).min(Duration::from_secs(1));
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err("peer answer wait was cancelled".into()),
            _ = tokio::time::sleep(pause) => {}
        }
    }
}

async fn query_peer_answer(
    relay: &TrustedRelay,
    question: &PeerQuestion,
    cancellation: &CancellationToken,
) -> Result<Option<PeerAnswer>, String> {
    relay.fresh_context(cancellation).await?;
    let mut events = relay
        .query_signed_events(
            vec![serde_json::json!({
                "#e": [question.event_id],
                "#h": [question.channel.to_string()],
                "#p": [relay.signer_pubkey()],
                "kinds": [9],
                "limit": 20,
            })],
            cancellation,
        )
        .await?;
    events.sort_by_key(|event| (event.created_at.as_secs(), event.id.to_hex()));
    for event in events {
        if event.pubkey.to_hex() != question.recipient
            || event_channel(&event) != Some(question.channel)
            || single_recipient(&event) != Some(relay.signer_pubkey())
            || peer_marker(&event, REPLY_TAG).as_deref() != Some(&question.request_id)
        {
            continue;
        }
        let Some((root, parent)) = buzz_core::nip10::parse_thread_markers(&event.tags).resolve()
        else {
            continue;
        };
        if root != question.thread_root_id || parent != question.event_id {
            continue;
        }
        return Ok(Some(PeerAnswer {
            request_id: question.request_id.clone(),
            request_event_id: question.event_id.clone(),
            response_event_id: event.id.to_hex(),
            author: event.pubkey.to_hex(),
            answer: event.content.to_string(),
            thread_root_id: root,
        }));
    }
    Ok(None)
}

async fn load_peer_question(
    relay: &TrustedRelay,
    event_id: &str,
    cancellation: &CancellationToken,
) -> Result<PeerQuestion, String> {
    let event = load_exact_channel_event(relay, event_id, None, cancellation).await?;
    parse_peer_question(&event)
}

fn parse_peer_question(event: &Event) -> Result<PeerQuestion, String> {
    if u32::from(event.kind.as_u16()) != buzz_core::kind::KIND_STREAM_MESSAGE {
        return Err("peer request is not a chat message".into());
    }
    let request_id = peer_marker(&event, QUESTION_TAG)
        .ok_or_else(|| "peer request has no unique question correlation tag".to_owned())?;
    let parsed_request = uuid::Uuid::parse_str(&request_id)
        .map_err(|_| "peer request correlation ID is invalid".to_owned())?;
    if parsed_request.to_string() != request_id {
        return Err("peer request correlation ID is invalid".into());
    }
    let recipient = single_recipient(&event)
        .ok_or_else(|| "peer request must address exactly one agent".to_owned())?;
    let channel =
        event_channel(&event).ok_or_else(|| "peer request channel is invalid".to_owned())?;
    let thread_root_id = buzz_core::nip10::parse_thread_markers(&event.tags)
        .resolve()
        .map(|(root, _)| root)
        .unwrap_or_else(|| event.id.to_hex());
    Ok(PeerQuestion {
        request_id,
        event_id: event.id.to_hex(),
        author: event.pubkey.to_hex(),
        recipient,
        channel,
        thread_root_id,
    })
}

async fn load_thread_root(
    relay: &TrustedRelay,
    event_id: &str,
    expected_channel: Option<uuid::Uuid>,
    cancellation: &CancellationToken,
) -> Result<Event, String> {
    let event = load_exact_channel_event(relay, event_id, expected_channel, cancellation).await?;
    let kind = u32::from(event.kind.as_u16());
    if kind == buzz_core::kind::KIND_STREAM_MESSAGE {
        if event
            .tags
            .iter()
            .any(|tag| tag.as_slice().first().map(String::as_str) == Some("e"))
        {
            return Err("requested event is a reply, not a canonical thread root".into());
        }
    } else if kind == buzz_core::kind::KIND_JOB_REQUEST {
        let JobEvent::Request(request) = JobEvent::parse(&event)
            .map_err(|_| "requested job thread root is invalid".to_owned())?
        else {
            return Err("requested event is not a supported thread root".into());
        };
        if request.common.project.home_channel
            != event_channel(&event)
                .ok_or_else(|| "requested job thread root has no channel".to_owned())?
                .to_string()
        {
            return Err("requested job thread root belongs to another channel".into());
        }
    } else {
        return Err("requested event is not a supported thread root".into());
    }
    Ok(event)
}

async fn load_exact_channel_event(
    relay: &TrustedRelay,
    event_id: &str,
    expected_channel: Option<uuid::Uuid>,
    cancellation: &CancellationToken,
) -> Result<Event, String> {
    validate_event_id("event_id", event_id)?;
    relay.fresh_context(cancellation).await?;
    let mut filter = serde_json::json!({"ids": [event_id], "limit": 2});
    if let Some(channel) = expected_channel {
        filter["#h"] = serde_json::json!([channel.to_string()]);
    }
    let mut events = relay
        .query_signed_events(vec![filter], cancellation)
        .await?;
    if events.len() != 1 || events[0].id.to_hex() != event_id {
        return Err("exact event was not found in the session channel".into());
    }
    let event = events.remove(0);
    let channel =
        event_channel(&event).ok_or_else(|| "event channel binding is invalid".to_owned())?;
    if expected_channel.is_some_and(|expected| expected != channel) {
        return Err("event channel binding is invalid".into());
    }
    validated_channel(relay, Some(&channel.to_string()), &[], cancellation).await?;
    Ok(event)
}

async fn validated_channel(
    relay: &TrustedRelay,
    requested: Option<&str>,
    addressed_peers: &[VerifiedPeer],
    cancellation: &CancellationToken,
) -> Result<uuid::Uuid, String> {
    let channel = match requested {
        Some(value) => {
            let parsed = uuid::Uuid::parse_str(value)
                .map_err(|_| "channel_id must be a canonical non-nil UUID".to_owned())?;
            if parsed.is_nil() || parsed.to_string() != value {
                return Err("channel_id must be a canonical non-nil UUID".into());
            }
            parsed
        }
        None => relay.bound_chat_channel()?,
    };
    relay.fresh_context(cancellation).await?;
    if relay.grants.is_managed_nemo() && channel.to_string() == buzz_core::nemo::HOME_CHANNEL {
        return Ok(channel);
    }
    let relay_pubkey = relay.relay_signer_pubkey(cancellation).await?;
    let events = relay
        .query_signed_events(
            vec![serde_json::json!({
                "kinds": [buzz_core::kind::KIND_NIP29_GROUP_MEMBERS],
                "#d": [channel.to_string()],
                "authors": [relay_pubkey],
                "limit": 2,
            })],
            cancellation,
        )
        .await?;
    let roster = events
        .iter()
        .filter(|event| event.kind.as_u16() as u32 == buzz_core::kind::KIND_NIP29_GROUP_MEMBERS)
        .max_by_key(|event| (event.created_at.as_secs(), event.id.to_hex()))
        .ok_or_else(|| "relay did not return the current channel membership".to_owned())?;
    if single_tag_value(roster, "d").as_deref() != Some(channel.to_string().as_str()) {
        return Err("channel membership binding is invalid".into());
    }
    let members = recipients(roster)?.into_iter().collect::<BTreeSet<_>>();
    let signer = relay.signer_pubkey();
    if !members.contains(&signer) && !members.contains(&relay.owner_pubkey) {
        return Err("this agent is not a current member of the requested channel".into());
    }
    if let Some(missing) = addressed_peers
        .iter()
        .find(|peer| !members.contains(&peer.pubkey) && !members.contains(&peer.owner_pubkey))
    {
        return Err(format!(
            "recipient_pubkey {} is not a current member of the requested channel",
            missing.pubkey
        ));
    }
    Ok(channel)
}

async fn verified_recipients(
    relay: &TrustedRelay,
    recipients: &[String],
    cancellation: &CancellationToken,
) -> Result<Vec<VerifiedPeer>, String> {
    if recipients.len() > MAX_RECIPIENTS {
        return Err("recipient_pubkeys exceeds the 50-recipient limit".into());
    }
    let mut requested = BTreeSet::new();
    for recipient in recipients {
        validate_pubkey("recipient_pubkey", recipient)?;
        requested.insert(recipient.clone());
    }
    if requested.is_empty() {
        return Ok(Vec::new());
    }
    let enrolled = super::peers::discover(relay, cancellation).await?;
    if let Some(unknown) = requested.iter().find(|peer| {
        !enrolled
            .iter()
            .any(|enrolled| enrolled.pubkey.as_str() == peer.as_str())
    }) {
        return Err(format!(
            "recipient_pubkey {unknown} is not an enrolled community agent"
        ));
    }
    Ok(enrolled
        .into_iter()
        .filter(|peer| requested.contains(&peer.pubkey))
        .collect())
}

async fn ensure_enrolled_peer(
    relay: &TrustedRelay,
    recipient: &str,
    cancellation: &CancellationToken,
) -> Result<(), String> {
    verified_recipients(relay, &[recipient.to_owned()], cancellation)
        .await
        .map(|_| ())
}

fn safe_chat_event(event: &Event, root: &str) -> Result<SafeChatEvent, String> {
    let markers = buzz_core::nip10::parse_thread_markers(&event.tags).resolve();
    Ok(SafeChatEvent {
        event_id: event.id.to_hex(),
        kind: u32::from(event.kind.as_u16()),
        author: event.pubkey.to_hex(),
        created_at: event.created_at.as_secs(),
        content: event.content.to_string(),
        thread_root_id: markers
            .as_ref()
            .map(|(thread_root, _)| thread_root.clone())
            .unwrap_or_else(|| root.to_owned()),
        parent_event_id: markers.map(|(_, parent)| parent),
        recipient_pubkeys: recipients(event)?,
    })
}

fn valid_thread_reply(event: &Event, channel: uuid::Uuid, root: &str) -> bool {
    if event_channel(event) != Some(channel) {
        return false;
    }
    let kind = u32::from(event.kind.as_u16());
    if !matches!(kind, 9 | 43002 | 43003 | 43004 | 43005 | 43006) {
        return false;
    }
    buzz_core::nip10::parse_thread_markers(&event.tags)
        .resolve()
        .is_some_and(|(thread_root, _)| thread_root == root)
}

fn event_channel(event: &Event) -> Option<uuid::Uuid> {
    let tags = event
        .tags
        .iter()
        .filter(|tag| tag.as_slice().first().map(String::as_str) == Some("h"))
        .collect::<Vec<_>>();
    if tags.len() != 1 {
        return None;
    }
    let value = tags[0].as_slice().get(1)?;
    let channel = uuid::Uuid::parse_str(value).ok()?;
    (channel.to_string() == *value).then_some(channel)
}

fn recipients(event: &Event) -> Result<Vec<String>, String> {
    let mut recipients = BTreeSet::new();
    for tag in event.tags.iter() {
        let values = tag.as_slice();
        if values.first().map(String::as_str) != Some("p") {
            continue;
        }
        let recipient = values
            .get(1)
            .ok_or_else(|| "chat event contains a malformed recipient tag".to_owned())?;
        validate_pubkey("chat recipient", recipient)?;
        recipients.insert(recipient.clone());
    }
    Ok(recipients.into_iter().collect())
}

fn single_recipient(event: &Event) -> Option<String> {
    let recipients = recipients(event).ok()?;
    (recipients.len() == 1).then(|| recipients[0].clone())
}

fn peer_marker(event: &Event, marker: &str) -> Option<String> {
    let mut matching = event.tags.iter().filter_map(|tag| {
        let values = tag.as_slice();
        (values.len() == 3 && values[0] == PEER_TAG && values[1] == marker)
            .then(|| values[2].clone())
    });
    let value = matching.next()?;
    matching.next().is_none().then_some(value)
}

fn single_tag_value(event: &Event, name: &str) -> Option<String> {
    let mut matching = event.tags.iter().filter_map(|tag| {
        let values = tag.as_slice();
        (values.len() >= 2 && values[0] == name).then(|| values[1].clone())
    });
    let value = matching.next()?;
    matching.next().is_none().then_some(value)
}

fn peer_tag(marker: &str, request_id: &str) -> Result<Tag, String> {
    Tag::parse([PEER_TAG, marker, request_id])
        .map_err(|_| "failed to encode peer correlation tag".to_owned())
}

fn task_assignment_tag(operation_id: &str) -> Result<Tag, String> {
    validate_operation_id(operation_id)?;
    Tag::parse([TASK_TAG, TASK_ASSIGNMENT_TAG, operation_id])
        .map_err(|_| "failed to encode task assignment tag".to_owned())
}

fn task_assignment_tags(operation_id: &str) -> Result<Vec<Tag>, String> {
    Ok(vec![
        task_assignment_tag(operation_id)?,
        Tag::parse(["i", operation_id])
            .map_err(|_| "failed to encode task operation index".to_owned())?,
    ])
}

fn valid_task_assignment_root(
    event: &Event,
    relay: &TrustedRelay,
    channel: uuid::Uuid,
    operation_id: &str,
) -> bool {
    event.pubkey == relay.keys.public_key()
        && u32::from(event.kind.as_u16()) == buzz_core::kind::KIND_STREAM_MESSAGE
        && event_channel(event) == Some(channel)
        && !event
            .tags
            .iter()
            .any(|tag| tag.as_slice().first().map(String::as_str) == Some("e"))
        && recipients(event).is_ok_and(|recipients| recipients.is_empty())
        && single_tag_value(event, "i").as_deref() == Some(operation_id)
        && task_assignment_operation(event).as_deref() == Some(operation_id)
}

fn task_assignment_operation(event: &Event) -> Option<String> {
    let mut markers = event.tags.iter().filter_map(|tag| {
        let values = tag.as_slice();
        (values.len() == 3 && values[0] == TASK_TAG && values[1] == TASK_ASSIGNMENT_TAG)
            .then(|| values[2].clone())
    });
    let operation_id = markers.next()?;
    if markers.next().is_some() || validate_operation_id(&operation_id).is_err() {
        return None;
    }
    Some(operation_id)
}

fn direct_thread_ref(root: &str) -> Result<buzz_sdk::ThreadRef, String> {
    thread_ref(root, root)
}

fn thread_ref(root: &str, parent: &str) -> Result<buzz_sdk::ThreadRef, String> {
    Ok(buzz_sdk::ThreadRef {
        root_event_id: EventId::parse(root)
            .map_err(|_| "thread root event ID is invalid".to_owned())?,
        parent_event_id: EventId::parse(parent)
            .map_err(|_| "thread parent event ID is invalid".to_owned())?,
    })
}

fn validate_pubkey(name: &str, value: &str) -> Result<(), String> {
    validate_event_id(name, value)?;
    let pubkey = nostr::PublicKey::parse(value).map_err(|_| format!("{name} is invalid"))?;
    if pubkey.to_hex() != value {
        return Err(format!("{name} must be canonical lowercase hexadecimal"));
    }
    Ok(())
}

fn validate_event_id(name: &str, value: &str) -> Result<(), String> {
    if value.len() != 64
        || value != value.to_ascii_lowercase()
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(format!(
            "{name} must be 64 lowercase hexadecimal characters"
        ));
    }
    Ok(())
}

fn validate_operation_id(value: &str) -> Result<(), String> {
    let operation = uuid::Uuid::parse_str(value)
        .map_err(|_| "operation_id must be a canonical UUID".to_owned())?;
    if operation.is_nil() || operation.to_string() != value {
        return Err("operation_id must be a canonical UUID".into());
    }
    Ok(())
}

const fn default_history_limit() -> u16 {
    DEFAULT_HISTORY_LIMIT
}

const fn default_wait_seconds() -> u8 {
    DEFAULT_WAIT_SECONDS
}

#[cfg(test)]
#[path = "chat_tests.rs"]
mod tests;
