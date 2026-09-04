//! Harness-owned, session-scoped Streamable HTTP MCP transport.
//!
//! The ACP adapter receives only a loopback URL and an ephemeral bearer
//! capability. Buzz signing keys, owner attestations, and grant documents stay
//! in this process and are baked into the typed handler directly.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{ConnectInfo, Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    Router,
};
use buzz_dev_mcp::{HarnessTrustedIdentity, TrustedSessionMcp, TrustedSessionScope};
use rmcp::transport::{
    streamable_http_server::session::local::LocalSessionManager, StreamableHttpServerConfig,
    StreamableHttpService,
};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::acp::{HttpHeader, McpServer};
use crate::scope::SessionScope;

const SERVER_NAME: &str = "buzz-trusted-session";
const MIN_TOKEN_BYTES: usize = 32;

/// Immutable harness identity used to create a fresh capability for each ACP
/// provider session.
#[derive(Clone)]
pub struct TrustedMcpFactory {
    identity: HarnessTrustedIdentity,
    lifetime: Duration,
}

impl TrustedMcpFactory {
    /// Create a factory. `lifetime` must cover a complete maximum-length turn;
    /// callers rotate provider sessions before the remaining lifetime is short.
    pub fn new(identity: HarnessTrustedIdentity, lifetime: Duration) -> Result<Self, String> {
        if lifetime.is_zero() {
            return Err("trusted MCP capability lifetime must be non-zero".into());
        }
        Ok(Self { identity, lifetime })
    }

    /// Start one loopback-only MCP server with an independently generated
    /// bearer capability and a handler fixed to `scope`.
    pub async fn start(&self, scope: &SessionScope) -> Result<TrustedMcpSession, String> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| "failed to bind trusted MCP loopback listener".to_owned())?;
        let address = listener
            .local_addr()
            .map_err(|_| "failed to inspect trusted MCP loopback listener".to_owned())?;
        if !address.ip().is_loopback() {
            return Err("trusted MCP listener was not bound to loopback".into());
        }

        let relay = self.identity.scoped_relay(scope_binding(scope))?;
        let cancellation = CancellationToken::new();
        let handler = TrustedSessionMcp::new(relay, cancellation.clone());
        let expires_at = tokio::time::Instant::now() + self.lifetime;
        let token = Arc::new(SessionSecret::generate()?);
        let auth = AuthState {
            token: token.clone(),
            expires_at,
        };
        let authority = address.to_string();
        let service: StreamableHttpService<TrustedSessionMcp, LocalSessionManager> =
            StreamableHttpService::new(
                move || Ok(handler.clone()),
                Default::default(),
                StreamableHttpServerConfig::default()
                    .with_sse_keep_alive(None)
                    .with_allowed_hosts([authority])
                    .with_cancellation_token(cancellation.child_token()),
            );
        let router = Router::new()
            .nest_service("/mcp", service)
            .layer(middleware::from_fn_with_state(auth, authorize));
        let shutdown = cancellation.clone();
        tokio::spawn(async move {
            let _ = axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move { shutdown.cancelled_owned().await })
            .await;
        });
        let expiry = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(expires_at).await;
            expiry.cancel();
        });

        Ok(TrustedMcpSession {
            address,
            token,
            expires_at,
            cancellation,
        })
    }
}

/// Lifetime handle for one provider session's trusted MCP capability.
/// Dropping it closes the listener and invalidates all transport sessions.
pub struct TrustedMcpSession {
    address: SocketAddr,
    token: Arc<SessionSecret>,
    expires_at: tokio::time::Instant,
    cancellation: CancellationToken,
}

impl TrustedMcpSession {
    /// ACP wire configuration containing only a loopback URL and ephemeral
    /// bearer. The wire layer redacts the header from logs and observers.
    pub fn mcp_server(&self) -> McpServer {
        McpServer::http(
            SERVER_NAME,
            format!("http://{}/mcp", self.address),
            vec![HttpHeader {
                name: "Authorization".into(),
                value: format!("Bearer {}", self.token.expose()),
            }],
        )
    }

    /// Whether the capability remains valid for an entire upcoming turn.
    pub fn is_valid_for(&self, duration: Duration) -> bool {
        !self.cancellation.is_cancelled()
            && self
                .expires_at
                .checked_duration_since(tokio::time::Instant::now())
                .is_some_and(|remaining| remaining >= duration)
    }

    #[cfg(test)]
    pub(crate) fn url(&self) -> String {
        format!("http://{}/mcp", self.address)
    }

    #[cfg(test)]
    fn authorization(&self) -> String {
        format!("Bearer {}", self.token.expose())
    }
}

impl Drop for TrustedMcpSession {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

struct SessionSecret(Zeroizing<String>);

impl SessionSecret {
    fn generate() -> Result<Self, String> {
        let secret = nostr::Keys::generate().secret_key().to_secret_hex();
        if secret.len() < MIN_TOKEN_BYTES * 2 {
            return Err("trusted MCP capability generator returned a short token".into());
        }
        Ok(Self(Zeroizing::new(secret)))
    }

    fn expose(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Clone)]
struct AuthState {
    token: Arc<SessionSecret>,
    expires_at: tokio::time::Instant,
}

async fn authorize(State(state): State<AuthState>, request: Request, next: Next) -> Response {
    let peer_is_loopback = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .is_some_and(|ConnectInfo(address)| address.ip().is_loopback());
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if !peer_is_loopback
        || tokio::time::Instant::now() >= state.expires_at
        || supplied.is_none_or(|value| !constant_time_eq(value, state.token.expose()))
    {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    next.run(request).await
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.as_bytes()
        .iter()
        .zip(right.as_bytes())
        .fold(0_u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

fn scope_binding(scope: &SessionScope) -> TrustedSessionScope {
    let mut binding = TrustedSessionScope {
        channel_id: Some(scope.channel_id().to_string()),
        thread_root_id: scope.root_event_id().map(str::to_owned),
        ..TrustedSessionScope::default()
    };
    if let SessionScope::Job {
        operation_id,
        request_event_id,
        ..
    } = scope
    {
        binding.job_operation_id = Some(operation_id.clone());
        binding.job_request_event_id = Some(request_event_id.clone());
    }
    binding
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::{extract::State as AxumState, routing::get, Json};
    use tokio::sync::Notify;

    fn identity() -> HarnessTrustedIdentity {
        HarnessTrustedIdentity::new(
            std::path::Path::new("."),
            "http://127.0.0.1:9".into(),
            nostr::Keys::generate(),
            None,
            None,
            None,
            None,
            true,
        )
        .expect("identity")
    }

    fn scope() -> SessionScope {
        SessionScope::Conversation {
            channel_id: uuid::Uuid::new_v4(),
        }
    }

    async fn post(session: &TrustedMcpSession, auth: &str) -> reqwest::Result<reqwest::Response> {
        reqwest::Client::new()
            .post(session.url())
            .header("Authorization", auth)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream")
            .body(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#)
            .send()
            .await
    }

    #[tokio::test]
    async fn bearer_is_unique_scope_bound_and_required() {
        let factory = TrustedMcpFactory::new(identity(), Duration::from_secs(60)).unwrap();
        let first = factory.start(&scope()).await.unwrap();
        let second = factory.start(&scope()).await.unwrap();
        assert_eq!(post(&first, "Bearer wrong").await.unwrap().status(), 401);
        assert_eq!(
            post(&second, &first.authorization())
                .await
                .unwrap()
                .status(),
            401
        );
        assert_eq!(
            post(&first, &first.authorization()).await.unwrap().status(),
            200
        );
    }

    #[tokio::test]
    async fn exact_host_is_required() {
        let factory = TrustedMcpFactory::new(identity(), Duration::from_secs(60)).unwrap();
        let session = factory.start(&scope()).await.unwrap();
        let response = reqwest::Client::new()
            .post(session.url())
            .header("Authorization", session.authorization())
            .header("Host", "attacker.example")
            .header("Content-Type", "application/json")
            .body("{}")
            .send()
            .await
            .expect("wrong-Host response");
        assert!(!response.status().is_success());
    }

    #[tokio::test]
    async fn expiry_and_drop_close_capability() {
        let factory = TrustedMcpFactory::new(identity(), Duration::from_millis(30)).unwrap();
        let session = factory.start(&scope()).await.unwrap();
        let url = session.url();
        let auth = session.authorization();
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(!session.is_valid_for(Duration::ZERO));
        let expired = reqwest::Client::new()
            .post(&url)
            .header("Authorization", &auth)
            .send()
            .await;
        assert!(expired.is_err() || expired.unwrap().status() == 401);
        drop(session);
    }

    #[tokio::test]
    async fn drop_closes_listener_before_expiry() {
        let factory = TrustedMcpFactory::new(identity(), Duration::from_secs(60)).unwrap();
        let session = factory.start(&scope()).await.unwrap();
        let url = session.url();
        let auth = session.authorization();
        assert_eq!(post(&session, &auth).await.unwrap().status(), 200);
        drop(session);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let response = reqwest::Client::new()
                .post(&url)
                .header("Authorization", &auth)
                .send()
                .await;
            if response.is_err() {
                break;
            }
            assert!(
                !response.unwrap().status().is_success(),
                "dropped capability remained usable"
            );
            assert!(
                tokio::time::Instant::now() < deadline,
                "listener stayed open"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[derive(Clone)]
    struct DelayedRelay {
        context_started: Arc<Notify>,
        release_context: Arc<Notify>,
        event_submissions: Arc<AtomicUsize>,
        context: serde_json::Value,
    }

    async fn delayed_context(AxumState(state): AxumState<DelayedRelay>) -> Json<serde_json::Value> {
        state.context_started.notify_one();
        state.release_context.notified().await;
        Json(state.context)
    }

    async fn count_event(
        AxumState(state): AxumState<DelayedRelay>,
        Json(event): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        state.event_submissions.fetch_add(1, Ordering::SeqCst);
        Json(serde_json::json!({
            "event_id": event["id"],
            "accepted": true,
            "message": "ok"
        }))
    }

    #[tokio::test]
    async fn dropping_session_cancels_inflight_tool_before_publish() {
        let relay_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let relay_address = relay_listener.local_addr().unwrap();
        let keys = nostr::Keys::generate();
        let relay_state = DelayedRelay {
            context_started: Arc::new(Notify::new()),
            release_context: Arc::new(Notify::new()),
            event_submissions: Arc::new(AtomicUsize::new(0)),
            context: serde_json::json!({
                "schema_version": "buzz.context.v1",
                "community_id": uuid::Uuid::new_v4().to_string(),
                "host": relay_address.to_string(),
                "pubkey": keys.public_key().to_hex(),
            }),
        };
        let relay_router = Router::new()
            .route("/api/context", get(delayed_context))
            .route("/events", axum::routing::post(count_event))
            .with_state(relay_state.clone());
        let relay_task = tokio::spawn(async move {
            axum::serve(relay_listener, relay_router).await.unwrap();
        });
        let identity = HarnessTrustedIdentity::new(
            std::path::Path::new("."),
            format!("http://{relay_address}"),
            keys,
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap();
        let factory = TrustedMcpFactory::new(identity, Duration::from_secs(60)).unwrap();
        let session = factory.start(&scope()).await.unwrap();
        let initialized = post(&session, &session.authorization()).await.unwrap();
        assert_eq!(initialized.status(), 200);
        let mcp_session = initialized
            .headers()
            .get("mcp-session-id")
            .expect("MCP session header")
            .to_str()
            .unwrap()
            .to_owned();

        let request = reqwest::Client::new()
            .post(session.url())
            .header("Authorization", session.authorization())
            .header("mcp-session-id", mcp_session)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream")
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "buzz_chat_send", "arguments": {"content": "hello"}}
            }));
        let call_task = tokio::spawn(async move { request.send().await });
        tokio::time::timeout(
            Duration::from_secs(2),
            relay_state.context_started.notified(),
        )
        .await
        .expect("tool reached delayed context check");
        drop(session);
        relay_state.release_context.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(2), call_task).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(relay_state.event_submissions.load(Ordering::SeqCst), 0);
        relay_task.abort();
    }
}
