use base64::Engine;
use nostr::{Event, EventBuilder, JsonUtil, Kind, Tag};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::RwLock;
use tokio_util::sync::CancellationToken;

struct ChatDestination {
    channel_id: Option<String>,
    thread_root_id: Option<String>,
}

use buzz_core::job::{build_job_tags, JobEvent};
use buzz_core::{CommunityContext, COMMUNITY_CONTEXT_SCHEMA_VERSION};

use super::TrustedConfig;

const MAX_HTTP_BODY_BYTES: usize = 256 * 1024;

/// Minimal acknowledgement returned to model-facing tools.
#[derive(Clone, serde::Serialize)]
pub struct PublishedEvent {
    pub event_id: String,
    pub accepted: bool,
}

/// Authenticated relay client reachable only through bounded typed methods.
///
/// Deliberately not `Debug`: this object owns the signing key and auth tag.
pub struct TrustedRelay {
    pub(super) http: reqwest::Client,
    pub(super) base_url: String,
    pub(super) relay_host: String,
    pub(super) keys: nostr::Keys,
    auth_tag: Option<Tag>,
    pub(super) auth_tag_json: Option<String>,
    pub(super) owner_pubkey: String,
    pub(super) owner_github_login: Option<String>,
    pub(super) grants: super::GrantSet,
    pub(super) a2a_channel_id: Option<String>,
    /// Immutable provider-session channel captured at construction. Chat
    /// reply placement may change independently during later turns.
    pub(super) provider_channel_id: Option<String>,
    /// Immutable provider-session root captured at construction. Conversation
    /// reply placement may change independently during later turns.
    pub(super) provider_thread_root_id: Option<String>,
    chat_destination: RwLock<ChatDestination>,
    pub(super) job_operation_id: Option<String>,
    pub(super) job_request_event_id: Option<String>,
    pub(super) session_working_directory: Option<std::path::PathBuf>,
    pub(super) github_credentials: super::git::GitHubCredentialStore,
}

impl TrustedRelay {
    pub fn new(config: TrustedConfig) -> Result<Self, String> {
        let (base_url, relay_host) =
            normalize_relay_url(&config.relay_url, config.allow_insecure_loopback)?;
        let http = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(15))
            .timeout(std::time::Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| "failed to initialize trusted relay client".to_owned())?;
        validate_optional_uuid(
            "BUZZ_MCP_SESSION_CHANNEL_ID",
            config.session_channel_id.as_deref(),
        )?;
        validate_optional_hex(
            "BUZZ_MCP_SESSION_THREAD_ROOT_ID",
            config.session_thread_root_id.as_deref(),
        )?;
        validate_optional_uuid(
            "BUZZ_MCP_JOB_OPERATION_ID",
            config.job_operation_id.as_deref(),
        )?;
        validate_optional_hex(
            "BUZZ_MCP_JOB_REQUEST_EVENT_ID",
            config.job_request_event_id.as_deref(),
        )?;
        let provider_channel_id = config.session_channel_id.clone();
        let provider_thread_root_id = config.session_thread_root_id.clone();
        let chat_destination = ChatDestination {
            channel_id: config.session_channel_id.clone(),
            thread_root_id: config.session_thread_root_id.clone(),
        };
        Ok(Self {
            http,
            base_url,
            relay_host,
            keys: config.keys,
            auth_tag: config.auth_tag,
            auth_tag_json: config.auth_tag_json,
            owner_pubkey: config.owner_pubkey,
            owner_github_login: config.owner_github_login,
            grants: config.grants,
            a2a_channel_id: config.a2a_channel_id,
            provider_channel_id,
            provider_thread_root_id,
            chat_destination: RwLock::new(chat_destination),
            job_operation_id: config.job_operation_id,
            job_request_event_id: config.job_request_event_id,
            session_working_directory: config.session_working_directory,
            github_credentials: config.github_credentials,
        })
    }

    pub fn signer_pubkey(&self) -> String {
        self.keys.public_key().to_hex()
    }

    /// Verify that a public key is a currently enrolled peer in this trusted
    /// session's community. This reuses the same signed three-layer directory
    /// evidence as the model-facing peer list.
    pub async fn is_enrolled_peer(
        &self,
        pubkey: &str,
        cancellation: &CancellationToken,
    ) -> Result<bool, String> {
        let parsed =
            nostr::PublicKey::parse(pubkey).map_err(|_| "peer pubkey is invalid".to_owned())?;
        if parsed.to_hex() != pubkey {
            return Err("peer pubkey must be canonical lowercase hexadecimal".into());
        }
        Ok(super::peers::discover(self, cancellation)
            .await?
            .iter()
            .any(|peer| peer.pubkey == pubkey))
    }

    pub async fn fresh_context(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<CommunityContext, String> {
        let url = format!("{}/api/context", self.base_url);
        let auth = sign_nip98(&self.keys, "GET", &url, None)?;
        let request = self.with_auth(self.http.get(&url).header("Authorization", auth));
        let bytes =
            send_bounded_cancellable(request, cancellation, "relay context", MAX_HTTP_BODY_BYTES)
                .await?;
        let context: CommunityContext = serde_json::from_slice(&bytes)
            .map_err(|_| "relay returned an invalid context document".to_owned())?;
        if context.schema_version != COMMUNITY_CONTEXT_SCHEMA_VERSION {
            return Err("relay returned an unsupported context document".into());
        }
        context
            .validate_binding(&self.relay_host, &self.keys.public_key())
            .map_err(|_| {
                "relay context did not match the configured tenant and signer".to_owned()
            })?;
        Ok(context)
    }

    /// Resolve the relay's event-signing identity from its NIP-11 document.
    ///
    /// Peer discovery uses this key to distinguish the relay-authored current
    /// membership snapshots from user-authored lookalikes returned by a broad
    /// event query. The request is fixed to the already normalized relay
    /// origin and redirects are disabled on the shared client.
    pub(super) async fn relay_signer_pubkey(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<String, String> {
        #[derive(Deserialize)]
        struct RelayInformationDocument {
            #[serde(default, rename = "self")]
            relay_self: Option<String>,
        }

        let request = self
            .http
            .get(&self.base_url)
            .header("Accept", "application/nostr+json");
        let bytes =
            send_bounded_cancellable(request, cancellation, "relay identity", MAX_HTTP_BODY_BYTES)
                .await?;
        let document: RelayInformationDocument = serde_json::from_slice(&bytes)
            .map_err(|_| "relay returned an invalid NIP-11 document".to_owned())?;
        let relay_self = document
            .relay_self
            .ok_or_else(|| "relay did not advertise its signing identity".to_owned())?;
        validate_hex64("relay NIP-11 self", &relay_self)?;
        let parsed = nostr::PublicKey::parse(&relay_self)
            .map_err(|_| "relay advertised an invalid signing identity".to_owned())?;
        if parsed.to_hex() != relay_self {
            return Err("relay advertised a non-canonical signing identity".into());
        }
        Ok(relay_self)
    }

    pub(super) async fn publish_job(
        &self,
        job: JobEvent,
        cancellation: &CancellationToken,
    ) -> Result<PublishedEvent, String> {
        let event = self.prepare_job_event(job)?;
        self.publish_prepared_job_event(event, cancellation).await
    }

    /// Sign one fully validated model-owned job event without crossing the
    /// network boundary. Handoff uses this so ACP can durably freeze the exact
    /// bytes before publication.
    pub(super) fn prepare_job_event(&self, job: JobEvent) -> Result<Event, String> {
        let kind = job_kind(&job);
        if !model_owned_job(&job) {
            return Err(
                "model-facing tools may publish only job requests or requester controls".into(),
            );
        }
        let content = job.canonical_json().map_err(|error| error.to_string())?;
        let tags = build_job_tags(&job).map_err(|error| error.to_string())?;
        // The owner authorization travels as the trusted `x-auth-tag` HTTP
        // header. Job envelopes have a closed routing-tag schema and must not
        // inherit that authorization tag from ordinary chat signing.
        let event = EventBuilder::new(Kind::Custom(kind as u16), content)
            .tags(tags)
            .sign_with_keys(&self.keys)
            .map_err(|_| "event signing failed".to_owned())?;
        JobEvent::parse(&event).map_err(|error| error.to_string())?;
        Ok(event)
    }

    /// Revalidate current tenant authority and publish only the exact event
    /// previously returned by `prepare_job_event`.
    pub(super) async fn publish_prepared_job_event(
        &self,
        event: Event,
        cancellation: &CancellationToken,
    ) -> Result<PublishedEvent, String> {
        self.fresh_context(cancellation).await?;
        buzz_core::verify_event(&event)
            .map_err(|_| "prepared job event signature is invalid".to_owned())?;
        if event.pubkey != self.keys.public_key() {
            return Err("prepared job event signer does not match this session".into());
        }
        let job =
            JobEvent::parse(&event).map_err(|_| "prepared job event is invalid".to_owned())?;
        if !model_owned_job(&job)
            || !self.grants.allows_event(&job, &self.signer_pubkey())
            || self.bound_a2a_channel()? != job.common().project.home_channel
        {
            return Err("prepared job event escaped the bound A2A session".into());
        }
        self.submit(event, PublishClass::ModelJob, cancellation)
            .await
    }

    pub async fn publish_chat(
        &self,
        content: &str,
        cancellation: &CancellationToken,
    ) -> Result<PublishedEvent, String> {
        let (channel, thread_root_id) = self.current_chat_destination()?;
        let event =
            self.build_chat_event_for_destination(channel, thread_root_id.as_deref(), content)?;
        self.publish_chat_event(event, cancellation).await
    }

    pub(super) async fn publish_chat_event(
        &self,
        event: Event,
        cancellation: &CancellationToken,
    ) -> Result<PublishedEvent, String> {
        self.fresh_context(cancellation).await?;
        self.submit(event, PublishClass::Chat, cancellation).await
    }

    /// Publish one self-signed, validated organization change after refreshing
    /// this session's authenticated community binding.
    pub(super) async fn publish_organization_event(
        &self,
        event: Event,
        cancellation: &CancellationToken,
    ) -> Result<PublishedEvent, String> {
        buzz_core::verify_event(&event)
            .map_err(|_| "organization event signature is invalid".to_owned())?;
        if event.pubkey != self.keys.public_key() {
            return Err("organization event signer does not match this session".into());
        }
        buzz_core::organization::parse_change(&event)?;
        self.fresh_context(cancellation).await?;
        self.submit(event, PublishClass::Organization, cancellation)
            .await
    }

    pub(super) fn bound_chat_channel(&self) -> Result<uuid::Uuid, String> {
        self.current_chat_destination().map(|(channel, _)| channel)
    }

    pub(super) fn current_chat_thread_root(&self) -> Result<Option<String>, String> {
        self.chat_destination
            .read()
            .map(|destination| destination.thread_root_id.clone())
            .map_err(|_| "session chat destination is unavailable".to_owned())
    }

    pub(super) fn current_chat_destination_parts(
        &self,
    ) -> Result<(Option<String>, Option<String>), String> {
        self.chat_destination
            .read()
            .map(|destination| {
                (
                    destination.channel_id.clone(),
                    destination.thread_root_id.clone(),
                )
            })
            .map_err(|_| "session chat destination is unavailable".to_owned())
    }

    pub(super) fn current_chat_destination(&self) -> Result<(uuid::Uuid, Option<String>), String> {
        let (channel_id, thread_root_id) = self.current_chat_destination_parts()?;
        let channel = channel_id.as_deref().ok_or_else(|| {
            "typed chat is unavailable outside a channel-bound session".to_owned()
        })?;
        let channel = uuid::Uuid::parse_str(channel)
            .map_err(|_| "session channel binding is invalid".to_owned())?;
        Ok((channel, thread_root_id))
    }

    #[cfg(test)]
    fn build_session_chat_event(
        &self,
        channel: uuid::Uuid,
        content: &str,
    ) -> Result<Event, String> {
        let thread_root_id = self.current_chat_thread_root()?;
        self.build_chat_event_for_destination(channel, thread_root_id.as_deref(), content)
    }

    fn build_chat_event_for_destination(
        &self,
        channel: uuid::Uuid,
        thread_root_id: Option<&str>,
        content: &str,
    ) -> Result<Event, String> {
        let thread = thread_root_id
            .map(|root| {
                let event = nostr::EventId::parse(root)
                    .map_err(|_| "session thread binding is invalid".to_owned())?;
                Ok::<_, String>(buzz_sdk::ThreadRef {
                    root_event_id: event,
                    parent_event_id: event,
                })
            })
            .transpose()?;
        self.build_chat_event(channel, content, thread.as_ref())
    }

    pub(super) fn build_scoped_chat_event(
        &self,
        channel: uuid::Uuid,
        content: &str,
        thread: Option<&buzz_sdk::ThreadRef>,
        mentions: &[&str],
        extra_tags: &[Tag],
    ) -> Result<Event, String> {
        let mut builder =
            buzz_sdk::build_message(channel, content, thread, mentions, false, &[], &[])
                .map_err(|error| format!("invalid chat message: {error}"))?;
        for tag in extra_tags {
            builder = builder.tag(tag.clone());
        }
        self.sign(builder)
    }

    /// Atomically replace the conversational reply destination for the next turn.
    /// The immutable provider scope, signer, repository and A2A scope remain fixed.
    pub(super) fn set_chat_destination(
        &self,
        channel_id: &str,
        thread_root_id: Option<&str>,
    ) -> Result<(), String> {
        validate_optional_uuid("session channel", Some(channel_id))?;
        validate_optional_hex("session thread root", thread_root_id)?;
        let mut destination = self
            .chat_destination
            .write()
            .map_err(|_| "session chat destination is unavailable".to_owned())?;
        destination.channel_id = Some(channel_id.to_owned());
        destination.thread_root_id = thread_root_id.map(str::to_owned);
        Ok(())
    }

    /// Replace only the thread while retaining the current destination channel.
    pub(super) fn set_chat_thread_root_id(
        &self,
        thread_root_id: Option<&str>,
    ) -> Result<(), String> {
        validate_optional_hex("session thread root", thread_root_id)?;
        let mut destination = self
            .chat_destination
            .write()
            .map_err(|_| "session chat destination is unavailable".to_owned())?;
        if destination.channel_id.is_none() && thread_root_id.is_some() {
            return Err("session thread root requires a channel destination".into());
        }
        destination.thread_root_id = thread_root_id.map(str::to_owned);
        Ok(())
    }

    /// Fetch relay-hosted private media with a narrow Blossom read token.
    /// Third-party URLs return `Ok(None)` and remain on the unauthenticated
    /// generic image path.
    pub async fn fetch_private_media(
        &self,
        source: &str,
        cancellation: &CancellationToken,
    ) -> Result<Option<Vec<u8>>, String> {
        super::media::fetch(self, source, cancellation).await
    }

    pub async fn query_job_events(
        &self,
        request_event_id: Option<&str>,
        inbox_limit: u16,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Event>, String> {
        self.fresh_context(cancellation).await?;
        let signer = self.signer_pubkey();
        let channel = self.bound_a2a_channel()?;
        let filters = if let Some(request) = request_event_id {
            validate_hex64("request_event_id", request)?;
            if self
                .job_request_event_id
                .as_deref()
                .is_some_and(|bound| bound != request)
            {
                return Err("job session request binding does not match the query".into());
            }
            vec![
                serde_json::json!({"ids":[request],"#h":[channel],"kinds":[43001],"limit":1}),
                serde_json::json!({"#e":[request],"#h":[channel],"kinds":[43002,43003,43004,43005,43006],"limit":200}),
            ]
        } else {
            vec![serde_json::json!({
                "#h": [channel],
                "#p": [signer],
                "kinds": [43001,43005],
                "limit": inbox_limit.clamp(1, 100),
            })]
        };
        self.run_job_query(filters, cancellation).await
    }

    pub(super) async fn query_handoff_event(
        &self,
        event_id: &str,
        cancellation: &CancellationToken,
    ) -> Result<Event, String> {
        validate_hex64("handoff_event_id", event_id)?;
        self.fresh_context(cancellation).await?;
        let mut events = self
            .run_job_query(
                vec![serde_json::json!({
                    "ids": [event_id],
                    "kinds": [43005],
                    "limit": 2,
                })],
                cancellation,
            )
            .await?;
        if events.len() != 1 || events[0].id.to_hex() != event_id {
            return Err("exact handoff event was not found in local scope".into());
        }
        Ok(events.remove(0))
    }

    async fn run_job_query(
        &self,
        filters: Vec<serde_json::Value>,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Event>, String> {
        let signer = self.signer_pubkey();
        let channel = self.bound_a2a_channel()?;
        let events = self.query_signed_events(filters, cancellation).await?;
        let mut validated = Vec::with_capacity(events.len());
        for event in events {
            let job = JobEvent::parse(&event)
                .map_err(|_| "relay returned an invalid job event".to_owned())?;
            if job.common().project.home_channel == channel
                && self.grants.allows_event(&job, &signer)
            {
                validated.push(event);
            }
        }
        Ok(validated)
    }

    /// Run a signed, authenticated event query and verify every returned
    /// envelope. Callers remain responsible for validating event kinds,
    /// authors, tags and application-specific scope.
    pub(super) async fn query_signed_events(
        &self,
        filters: Vec<serde_json::Value>,
        cancellation: &CancellationToken,
    ) -> Result<Vec<Event>, String> {
        let url = format!("{}/query", self.base_url);
        let body =
            serde_json::to_vec(&filters).map_err(|_| "query serialization failed".to_owned())?;
        let auth = sign_nip98(&self.keys, "POST", &url, Some(&body))?;
        let request = self.with_auth(
            self.http
                .post(&url)
                .header("Authorization", auth)
                .header("Content-Type", "application/json")
                .body(body),
        );
        let bytes =
            send_bounded_cancellable(request, cancellation, "relay query", MAX_HTTP_BODY_BYTES)
                .await?;
        let events: Vec<Event> = serde_json::from_slice(&bytes)
            .map_err(|_| "relay returned an invalid event list".to_owned())?;
        for event in &events {
            buzz_core::verify_event(event)
                .map_err(|_| "relay returned an invalid event signature".to_owned())?;
        }
        Ok(events)
    }

    pub(super) fn bound_a2a_channel(&self) -> Result<&str, String> {
        let channel = self
            .a2a_channel_id
            .as_deref()
            .or(self.provider_channel_id.as_deref())
            .ok_or_else(|| {
                "A2A reads are unavailable outside a channel-bound session".to_owned()
            })?;
        if !self
            .grants
            .channels()
            .iter()
            .any(|allowed| allowed == channel)
        {
            return Err("session channel is outside the local A2A grants".into());
        }
        Ok(channel)
    }

    fn sign(&self, builder: EventBuilder) -> Result<Event, String> {
        let builder = if let Some(tag) = &self.auth_tag {
            builder.tags([tag.clone()])
        } else {
            builder
        };
        builder
            .sign_with_keys(&self.keys)
            .map_err(|_| "event signing failed".to_owned())
    }

    fn build_chat_event(
        &self,
        channel: uuid::Uuid,
        content: &str,
        thread: Option<&buzz_sdk::ThreadRef>,
    ) -> Result<Event, String> {
        self.build_scoped_chat_event(channel, content, thread, &[], &[])
    }

    async fn submit(
        &self,
        event: Event,
        class: PublishClass,
        cancellation: &CancellationToken,
    ) -> Result<PublishedEvent, String> {
        let kind = u32::from(event.kind.as_u16());
        if !class.accepts(kind) {
            return Err("typed publisher rejected an out-of-surface event kind".into());
        }
        let event_id = event.id.to_hex();
        let body =
            serde_json::to_vec(&event).map_err(|_| "event serialization failed".to_owned())?;
        let url = format!("{}/events", self.base_url);
        let auth = sign_nip98(&self.keys, "POST", &url, Some(&body))?;
        let request = self.with_auth(
            self.http
                .post(&url)
                .header("Authorization", auth)
                .header("Content-Type", "application/json")
                .body(body),
        );
        let bytes = send_bounded_cancellable(
            request,
            cancellation,
            "event submission",
            MAX_HTTP_BODY_BYTES,
        )
        .await?;
        let ack: RelayAck = serde_json::from_slice(&bytes)
            .map_err(|_| "relay returned an invalid event acknowledgement".to_owned())?;
        if !ack.accepted || ack.event_id != event_id {
            return Err("relay did not acknowledge the exact signed event".into());
        }
        Ok(PublishedEvent {
            event_id,
            accepted: true,
        })
    }

    pub(super) fn with_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth_tag_json {
            Some(tag) => request.header("x-auth-tag", tag),
            None => request,
        }
    }
}

pub(super) async fn send_bounded_cancellable(
    request: reqwest::RequestBuilder,
    cancellation: &CancellationToken,
    operation: &str,
    limit: usize,
) -> Result<Vec<u8>, String> {
    if cancellation.is_cancelled() {
        return Err(format!("{operation} cancelled before relay access"));
    }
    let mut response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(format!("{operation} cancelled before confirmation")),
        response = request.send() => response.map_err(|_| format!("{operation} outcome is unknown"))?,
    };
    if !response.status().is_success() {
        return Err(format!(
            "{operation} rejected with HTTP {}",
            response.status().as_u16()
        ));
    }
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(format!("{operation} response exceeded the size limit"));
    }
    let mut body = Vec::new();
    loop {
        let chunk = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(format!("{operation} cancelled before confirmation"));
            }
            chunk = response.chunk() => {
                chunk.map_err(|_| format!("{operation} response failed"))?
            }
        };
        let Some(chunk) = chunk else {
            break;
        };
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| format!("{operation} response exceeded the size limit"))?;
        if next_len > limit {
            return Err(format!("{operation} response exceeded the size limit"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[derive(Clone, Copy)]
enum PublishClass {
    ModelJob,
    Chat,
    Organization,
}

impl PublishClass {
    fn accepts(self, kind: u32) -> bool {
        match self {
            Self::ModelJob => matches!(kind, 43001 | 43005),
            Self::Chat => kind == buzz_core::kind::KIND_STREAM_MESSAGE,
            Self::Organization => kind == buzz_core::kind::KIND_CONVERSATION_ORGANIZATION,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RelayAck {
    event_id: String,
    accepted: bool,
    #[serde(rename = "message")]
    _message: String,
}

fn job_kind(job: &JobEvent) -> u32 {
    match job {
        JobEvent::Request(_) => 43001,
        JobEvent::Accepted(_) => 43002,
        JobEvent::Progress(_) => 43003,
        JobEvent::Result(_) => 43004,
        JobEvent::Control(_) => 43005,
        JobEvent::Error(_) => 43006,
    }
}

fn model_owned_job(job: &JobEvent) -> bool {
    matches!(job, JobEvent::Request(_))
        || matches!(job, JobEvent::Control(control) if model_control_action_allowed(control.action))
}

fn model_control_action_allowed(action: buzz_core::job::JobControlAction) -> bool {
    matches!(
        action,
        buzz_core::job::JobControlAction::Cancel | buzz_core::job::JobControlAction::Handoff
    )
}

fn sign_nip98(
    keys: &nostr::Keys,
    method: &str,
    url: &str,
    body: Option<&[u8]>,
) -> Result<String, String> {
    let mut tags = vec![
        Tag::parse(["u", url]).map_err(|_| "NIP-98 URL binding failed".to_owned())?,
        Tag::parse(["method", method]).map_err(|_| "NIP-98 method binding failed".to_owned())?,
        Tag::parse(["nonce", &uuid::Uuid::new_v4().to_string()])
            .map_err(|_| "NIP-98 nonce binding failed".to_owned())?,
    ];
    if let Some(body) = body {
        let digest = hex::encode(Sha256::digest(body));
        tags.push(
            Tag::parse(["payload", digest.as_str()])
                .map_err(|_| "NIP-98 payload binding failed".to_owned())?,
        );
    }
    let event = EventBuilder::new(Kind::Custom(27235), "")
        .tags(tags)
        .sign_with_keys(keys)
        .map_err(|_| "NIP-98 signing failed".to_owned())?;
    Ok(format!(
        "Nostr {}",
        base64::engine::general_purpose::STANDARD.encode(event.as_json().as_bytes())
    ))
}

fn normalize_relay_url(raw: &str, allow_loopback: bool) -> Result<(String, String), String> {
    let mut url = reqwest::Url::parse(raw).map_err(|_| "BUZZ_RELAY_URL is invalid".to_owned())?;
    let mapped = match url.scheme() {
        "https" | "http" => url.scheme().to_owned(),
        "wss" => "https".to_owned(),
        "ws" => "http".to_owned(),
        _ => return Err("BUZZ_RELAY_URL must use https/wss or explicit loopback http/ws".into()),
    };
    url.set_scheme(&mapped)
        .map_err(|_| "BUZZ_RELAY_URL scheme is invalid".to_owned())?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("BUZZ_RELAY_URL must not contain credentials, query, or fragment".into());
    }
    if url.path() != "/" && !url.path().is_empty() {
        return Err("BUZZ_RELAY_URL must not contain a path".into());
    }
    let loopback = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    if url.scheme() == "http" && !(allow_loopback && loopback) {
        return Err(
            "insecure relay transport is allowed only for explicitly enabled loopback development"
                .into(),
        );
    }
    let host = buzz_core::tenant::relay_url_authority(url.as_str());
    if host.is_empty() {
        return Err("BUZZ_RELAY_URL has no canonical authority".into());
    }
    Ok((url.as_str().trim_end_matches('/').to_owned(), host))
}

fn validate_optional_uuid(name: &str, value: Option<&str>) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    let parsed = uuid::Uuid::parse_str(value).map_err(|_| format!("{name} must be a UUID"))?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(format!("{name} must be a canonical non-nil UUID"));
    }
    Ok(())
}

fn validate_optional_hex(name: &str, value: Option<&str>) -> Result<(), String> {
    match value {
        Some(value) => validate_hex64(name, value),
        None => Ok(()),
    }
}

fn validate_hex64(name: &str, value: &str) -> Result<(), String> {
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

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
