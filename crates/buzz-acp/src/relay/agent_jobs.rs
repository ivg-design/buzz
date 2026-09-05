use std::collections::{HashMap, HashSet};

use buzz_core::job::MAX_JOB_TTL_SECONDS;
use serde_json::{json, Value};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, warn};
use uuid::Uuid;

use super::{ws_send_timeout, WsStream, SINCE_SKEW_SECS, WS_SEND_TIMEOUT_SECS};

const SUBSCRIPTION_PREFIX: &str = "agent-job-";

#[derive(Debug, Default)]
struct Cursor {
    last_seen: Option<u64>,
    dropped_since: Option<u64>,
}

/// Grant-derived job subscription state, deliberately isolated from chat.
#[derive(Debug, Default)]
pub(super) struct JobSubscriptions {
    active: HashMap<Uuid, Cursor>,
    rate_limited_pending: HashMap<Uuid, Instant>,
    resubscribe_retry: HashSet<Uuid>,
}

impl JobSubscriptions {
    pub(super) fn subscribe(&mut self, channel_id: Uuid) {
        self.active.entry(channel_id).or_default();
    }

    pub(super) fn unsubscribe(&mut self, channel_id: &Uuid) {
        self.active.remove(channel_id);
        self.rate_limited_pending.remove(channel_id);
        self.resubscribe_retry.remove(channel_id);
    }

    pub(super) fn contains(&self, channel_id: &Uuid) -> bool {
        self.active.contains_key(channel_id)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    pub(super) fn channels(&self) -> Vec<Uuid> {
        self.active.keys().copied().collect()
    }

    pub(super) fn record_seen(&mut self, channel_id: &Uuid, timestamp: u64) -> bool {
        let Some(cursor) = self.active.get_mut(channel_id) else {
            return false;
        };
        cursor.last_seen = Some(cursor.last_seen.unwrap_or(0).max(timestamp));
        true
    }

    pub(super) fn record_dropped(&mut self, channel_id: &Uuid, timestamp: u64) {
        if let Some(cursor) = self.active.get_mut(channel_id) {
            cursor.dropped_since = Some(
                cursor
                    .dropped_since
                    .map_or(timestamp, |oldest| oldest.min(timestamp)),
            );
        }
    }

    pub(super) fn clear_dropped(&mut self, channel_id: &Uuid) {
        if let Some(cursor) = self.active.get_mut(channel_id) {
            cursor.dropped_since = None;
        }
    }

    /// Return a rolling TTL-bounded backfill floor with reconnect overlap.
    pub(super) fn since(&self, channel_id: &Uuid) -> u64 {
        self.since_at(channel_id, unix_now_secs())
    }

    fn since_at(&self, channel_id: &Uuid, now: u64) -> u64 {
        let ttl = u64::try_from(MAX_JOB_TTL_SECONDS).unwrap_or_default();
        let ttl_floor = now.saturating_sub(ttl);
        let Some(cursor) = self.active.get(channel_id) else {
            return ttl_floor;
        };
        match (cursor.last_seen, cursor.dropped_since) {
            (Some(last), Some(dropped)) => last.min(dropped).saturating_sub(SINCE_SKEW_SECS),
            (Some(last), None) => last.saturating_sub(SINCE_SKEW_SECS),
            (None, Some(dropped)) => dropped.saturating_sub(SINCE_SKEW_SECS),
            (None, None) => ttl_floor,
        }
        .max(ttl_floor)
    }

    pub(super) fn clear_derived_queues(&mut self) {
        self.rate_limited_pending.clear();
        self.resubscribe_retry.clear();
    }

    pub(super) fn park_rate_limited(&mut self, channel_id: Uuid, deadline: Instant) {
        if self.contains(&channel_id) {
            self.rate_limited_pending.insert(channel_id, deadline);
        }
    }

    pub(super) fn ready_rate_limited(&self, now: Instant, budget: usize) -> Vec<Uuid> {
        self.rate_limited_pending
            .iter()
            .filter(|(_, deadline)| now >= **deadline)
            .map(|(channel_id, _)| *channel_id)
            .take(budget)
            .collect()
    }

    pub(super) fn has_rate_limited_pending(&self) -> bool {
        !self.rate_limited_pending.is_empty()
    }

    pub(super) fn remove_rate_limited(&mut self, channel_id: &Uuid) {
        self.rate_limited_pending.remove(channel_id);
    }

    pub(super) fn mark_retry(&mut self, channel_id: Uuid) {
        if self.contains(&channel_id) {
            self.resubscribe_retry.insert(channel_id);
        }
    }

    pub(super) fn retry_channels(&self, budget: usize) -> Vec<Uuid> {
        self.resubscribe_retry
            .iter()
            .copied()
            .take(budget)
            .collect()
    }

    pub(super) fn has_retries(&self) -> bool {
        !self.resubscribe_retry.is_empty()
    }

    pub(super) fn remove_retry(&mut self, channel_id: &Uuid) {
        self.resubscribe_retry.remove(channel_id);
    }

    pub(super) async fn send(
        &self,
        ws: &mut WsStream,
        channel_id: Uuid,
        agent_pubkey_hex: &str,
    ) -> bool {
        send_frame(
            ws,
            request_frame(channel_id, agent_pubkey_hex, self.since(&channel_id)),
            channel_id,
        )
        .await
    }

    pub(super) async fn drain_rate_limited(
        &mut self,
        ws: &mut WsStream,
        agent_pubkey_hex: &str,
        gate: Option<Instant>,
        budget: usize,
    ) -> usize {
        let ready = self.ready_rate_limited(Instant::now(), budget);
        let mut sent = 0;
        for channel_id in ready {
            if !self.contains(&channel_id) {
                self.remove_rate_limited(&channel_id);
                continue;
            }
            if let Some(deadline) = gate {
                self.park_rate_limited(channel_id, deadline);
                continue;
            }
            if self.send(ws, channel_id, agent_pubkey_hex).await {
                self.remove_rate_limited(&channel_id);
                self.clear_dropped(&channel_id);
                sent += 1;
            } else {
                self.park_rate_limited(
                    channel_id,
                    Instant::now() + std::time::Duration::from_secs(5),
                );
            }
        }
        sent
    }

    pub(super) async fn drain_retries(
        &mut self,
        ws: &mut WsStream,
        agent_pubkey_hex: &str,
        gate: Option<Instant>,
        budget: usize,
    ) -> usize {
        let channels = self.retry_channels(budget);
        let mut sent = 0;
        for channel_id in channels {
            if !self.contains(&channel_id) {
                self.remove_retry(&channel_id);
                continue;
            }
            if let Some(deadline) = gate {
                self.park_rate_limited(channel_id, deadline);
                self.remove_retry(&channel_id);
                continue;
            }
            if self.send(ws, channel_id, agent_pubkey_hex).await {
                self.remove_retry(&channel_id);
                self.clear_dropped(&channel_id);
                sent += 1;
            }
        }
        sent
    }
}

pub(super) fn subscription_id(channel_id: Uuid) -> String {
    format!("{SUBSCRIPTION_PREFIX}{channel_id}")
}

pub(super) fn channel_from_subscription_id(sub_id: &str) -> Option<Uuid> {
    sub_id
        .strip_prefix(SUBSCRIPTION_PREFIX)
        .and_then(|value| value.parse::<Uuid>().ok())
}

pub(super) fn request_frame(channel_id: Uuid, agent_pubkey_hex: &str, since: u64) -> Value {
    json!(["REQ", subscription_id(channel_id), {
        "kinds": [buzz_core::kind::KIND_JOB_REQUEST, buzz_core::kind::KIND_JOB_CANCEL],
        "#h": [channel_id.to_string()],
        "#p": [agent_pubkey_hex],
        "since": since,
    }])
}

pub(super) fn is_inbound_kind(kind: u32) -> bool {
    matches!(
        kind,
        buzz_core::kind::KIND_JOB_REQUEST | buzz_core::kind::KIND_JOB_CANCEL
    )
}

async fn send_frame(ws: &mut WsStream, frame: Value, channel_id: Uuid) -> bool {
    let Ok(text) = serde_json::to_string(&frame) else {
        warn!(channel_id = %channel_id, "failed to serialize agent-job REQ");
        return false;
    };
    match ws_send_timeout(ws, Message::Text(text.into()), WS_SEND_TIMEOUT_SECS).await {
        Ok(()) => {
            debug!(channel_id = %channel_id, "subscribed to addressed agent jobs");
            true
        }
        Err(error) => {
            warn!(channel_id = %channel_id, "failed to send agent-job REQ: {error}");
            false
        }
    }
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
impl super::RestClient {
    /// Test transport that observes event bodies but returns a transient error
    /// for the first complete submit attempt, modeling a lost acknowledgement.
    pub(crate) async fn uncertain_then_accepting_job_test_pair(
        keys: nostr::Keys,
    ) -> (
        Self,
        tokio::sync::mpsc::Receiver<nostr::Event>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind uncertain event test server");
        let base_url = format!("http://{}", listener.local_addr().expect("test address"));
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(64);
        let server = tokio::spawn(async move {
            let mut uncertain_submits = 4_usize;
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = Vec::with_capacity(4096);
                let mut chunk = [0_u8; 4096];
                let header_end = loop {
                    let read = socket.read(&mut chunk).await.unwrap_or_default();
                    if read == 0 {
                        break None;
                    }
                    request.extend_from_slice(&chunk[..read]);
                    if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                        break Some(end + 4);
                    }
                };
                let Some(header_end) = header_end else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let first_line = headers.lines().next().unwrap_or_default().to_owned();
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                while request.len() < header_end + content_length {
                    let read = socket.read(&mut chunk).await.unwrap_or_default();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..read]);
                }
                let body = &request[header_end..header_end.saturating_add(content_length)];
                if first_line.contains(" /api/jobs/authorize ") {
                    let Ok(authorization) = serde_json::from_slice::<
                        buzz_core::job_authorization::JobAuthorizationRequest,
                    >(body) else {
                        continue;
                    };
                    let now = chrono::Utc::now();
                    let response = buzz_core::job_authorization::JobAuthorizationResponse {
                        schema_version:
                            buzz_core::job_authorization::JOB_AUTHORIZATION_SCHEMA_VERSION.into(),
                        authorized: true,
                        authorization_id: uuid::Uuid::new_v4().to_string(),
                        issued_at: now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        expires_at: (now + chrono::Duration::seconds(5))
                            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        binding: buzz_core::job_authorization::JobAuthorizationBinding::from(
                            &authorization,
                        ),
                        project_head_event_id: "a".repeat(64),
                        repository_coordinate: format!(
                            "30617:{}:nemo",
                            authorization.requester_pubkey
                        ),
                        repository_announcement_event_id: "b".repeat(64),
                        requester_owner_pubkey: authorization.requester_pubkey.clone(),
                        recipient_owner_pubkey: authorization.recipient_pubkey.clone(),
                    };
                    let body = serde_json::to_string(&response).expect("authorization JSON");
                    write_test_response(&mut socket, "200 OK", &body).await;
                    continue;
                }
                let Ok(event) = serde_json::from_slice::<nostr::Event>(body) else {
                    continue;
                };
                let _ = event_tx.send(event.clone()).await;
                if uncertain_submits > 0 {
                    uncertain_submits -= 1;
                    write_test_response(&mut socket, "503 Service Unavailable", "").await;
                } else {
                    let body = serde_json::json!({
                        "event_id": event.id.to_hex(),
                        "accepted": true,
                        "message": ""
                    })
                    .to_string();
                    write_test_response(&mut socket, "200 OK", &body).await;
                }
            }
        });
        (
            Self {
                http: reqwest::Client::new(),
                base_url,
                keys,
                auth_tag_json: None,
            },
            event_rx,
            server,
        )
    }
}

#[cfg(test)]
async fn write_test_response(socket: &mut tokio::net::TcpStream, status: &str, body: &str) {
    use tokio::io::AsyncWriteExt;
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = socket.write_all(response.as_bytes()).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_identity_is_disjoint_from_chat() {
        let channel = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let id = subscription_id(channel);
        assert_eq!(id, "agent-job-550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(channel_from_subscription_id(&id), Some(channel));
        assert!(channel_from_subscription_id(&format!("ch-{channel}")).is_none());
    }

    #[test]
    fn request_filter_is_exact() {
        let channel = Uuid::new_v4();
        let frame = request_frame(channel, "agent-pubkey", 42);
        let filter = frame[2].as_object().unwrap();
        assert_eq!(filter.len(), 4);
        assert_eq!(
            filter["kinds"],
            json!([
                buzz_core::kind::KIND_JOB_REQUEST,
                buzz_core::kind::KIND_JOB_CANCEL
            ])
        );
        assert_eq!(filter["#h"], json!([channel.to_string()]));
        assert_eq!(filter["#p"], json!(["agent-pubkey"]));
        assert_eq!(filter["since"], json!(42));
    }

    #[test]
    fn only_requests_and_controls_are_inbound() {
        assert!(is_inbound_kind(buzz_core::kind::KIND_JOB_REQUEST));
        assert!(is_inbound_kind(buzz_core::kind::KIND_JOB_CANCEL));
        assert!(!is_inbound_kind(buzz_core::kind::KIND_JOB_ACCEPTED));
        assert!(!is_inbound_kind(buzz_core::kind::KIND_JOB_RESULT));
    }

    #[test]
    fn cursor_is_ttl_bounded_and_channel_local() {
        let left = Uuid::new_v4();
        let right = Uuid::new_v4();
        let ttl = u64::try_from(MAX_JOB_TTL_SECONDS).unwrap();
        let now = ttl + 10_000;
        let mut jobs = JobSubscriptions::default();
        jobs.subscribe(left);
        jobs.subscribe(right);
        assert_eq!(jobs.since_at(&left, now), 10_000);
        jobs.record_seen(&left, now - 60);
        jobs.record_seen(&right, now - 20);
        assert_eq!(jobs.since_at(&left, now), now - 65);
        assert_eq!(jobs.since_at(&right, now), now - 25);
        jobs.record_dropped(&left, now - 90);
        assert_eq!(jobs.since_at(&left, now), now - 95);
        assert_eq!(jobs.since_at(&right, now), now - 25);
    }

    #[test]
    fn unsubscribe_clears_only_the_named_job_channel() {
        let left = Uuid::new_v4();
        let right = Uuid::new_v4();
        let mut jobs = JobSubscriptions::default();
        jobs.subscribe(left);
        jobs.subscribe(right);
        jobs.unsubscribe(&left);
        assert!(!jobs.contains(&left));
        assert!(jobs.contains(&right));
    }
}
