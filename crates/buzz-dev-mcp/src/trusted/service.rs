//! Session-scoped MCP surface for harness-owned Buzz credentials.

use std::sync::Arc;

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ErrorData, ServerHandler,
};
use schemars::JsonSchema;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use super::{
    cancel, dispatch, handoff, inbox, send_chat, status, A2aCancelParams, A2aDispatchParams,
    A2aHandoffParams, A2aInboxParams, A2aStatusParams, ChatSendParams, TrustedRelay,
};

/// Typed Buzz tools backed by a harness-owned signer.
///
/// The HTTP transport and bearer capability live in `buzz-acp`; this handler
/// receives only an already validated, session-bound relay client. It exposes
/// no generic shell and no raw signing or authorization material.
#[derive(Clone)]
pub struct TrustedSessionMcp {
    relay: Arc<TrustedRelay>,
    session_cancellation: CancellationToken,
    tool_router: ToolRouter<Self>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PrivateMediaParams {
    /// Exact relay-hosted `/media/` URL. Other origins and URL credentials,
    /// queries, or fragments are rejected before a signed read is attempted.
    source: String,
    /// Optional longest-edge cap, clamped to the same range as `view_image`.
    #[serde(default)]
    max_dim: Option<u32>,
}

#[tool_router]
impl TrustedSessionMcp {
    /// Create a typed MCP handler for one immutable ACP session scope.
    pub fn new(relay: TrustedRelay, session_cancellation: CancellationToken) -> Self {
        Self {
            relay: Arc::new(relay),
            session_cancellation,
            tool_router: Self::tool_router(),
        }
    }

    #[cfg(test)]
    pub(crate) fn tool_names() -> Vec<String> {
        Self::tool_router()
            .list_all()
            .into_iter()
            .map(|tool| tool.name.into_owned())
            .collect()
    }

    #[tool(
        name = "buzz_a2a_dispatch",
        description = "Dispatch one signed A2A request within the exact local project, repository, peer, capability, path, branch, and worktree grant."
    )]
    async fn buzz_a2a_dispatch(
        &self,
        Parameters(params): Parameters<A2aDispatchParams>,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let cancellation = combine_cancellation(&self.session_cancellation, context.ct);
        let result = dispatch(&self.relay, params, cancellation.clone()).await;
        cancellation.cancel();
        Ok(result)
    }

    #[tool(
        name = "buzz_a2a_inbox",
        description = "Read validated A2A events addressed to this agent within locally granted project channels."
    )]
    async fn buzz_a2a_inbox(
        &self,
        Parameters(params): Parameters<A2aInboxParams>,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let cancellation = combine_cancellation(&self.session_cancellation, context.ct);
        let result = inbox(&self.relay, params, cancellation.clone()).await;
        cancellation.cancel();
        Ok(result)
    }

    #[tool(
        name = "buzz_a2a_status",
        description = "Read one granted A2A request and its lifecycle chain."
    )]
    async fn buzz_a2a_status(
        &self,
        Parameters(params): Parameters<A2aStatusParams>,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let cancellation = combine_cancellation(&self.session_cancellation, context.ct);
        let result = status(&self.relay, params, cancellation.clone()).await;
        cancellation.cancel();
        Ok(result)
    }

    #[tool(
        name = "buzz_a2a_cancel",
        description = "Request cancellation of an active A2A job as its original requester."
    )]
    async fn buzz_a2a_cancel(
        &self,
        Parameters(params): Parameters<A2aCancelParams>,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let cancellation = combine_cancellation(&self.session_cancellation, context.ct);
        let result = cancel(&self.relay, params, cancellation.clone()).await;
        cancellation.cancel();
        Ok(result)
    }

    #[tool(
        name = "buzz_a2a_handoff",
        description = "Request a bounded handoff for an active A2A job as its current recipient."
    )]
    async fn buzz_a2a_handoff(
        &self,
        Parameters(params): Parameters<A2aHandoffParams>,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let cancellation = combine_cancellation(&self.session_cancellation, context.ct);
        let result = handoff(&self.relay, params, cancellation.clone()).await;
        cancellation.cancel();
        Ok(result)
    }

    #[tool(
        name = "buzz_chat_send",
        description = "Send a normal Buzz message to this ACP session's fixed channel and thread."
    )]
    async fn buzz_chat_send(
        &self,
        Parameters(params): Parameters<ChatSendParams>,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let cancellation = combine_cancellation(&self.session_cancellation, context.ct);
        let result = send_chat(&self.relay, params, cancellation.clone()).await;
        cancellation.cancel();
        Ok(result)
    }

    #[tool(
        name = "buzz_private_media_get",
        description = "Load a relay-private image through the harness-owned signer. The URL must match this session's exact relay origin and /media/ path; credentials are never returned."
    )]
    async fn buzz_private_media_get(
        &self,
        Parameters(params): Parameters<PrivateMediaParams>,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let cancellation = combine_cancellation(&self.session_cancellation, context.ct);
        let result = match self
            .relay
            .fetch_private_media(&params.source, &cancellation)
            .await
        {
            Ok(Some(bytes)) => crate::view_image::prepare_result(
                &bytes,
                params.max_dim.unwrap_or(crate::view_image::DEFAULT_MAX_DIM),
                "private relay media",
            ),
            Ok(None) => Ok(CallToolResult::error(vec![rmcp::model::Content::text(
                "source is not an exact relay-private /media/ URL",
            )])),
            Err(error) => Ok(CallToolResult::error(vec![rmcp::model::Content::text(
                error,
            )])),
        };
        cancellation.cancel();
        result
    }
}

fn combine_cancellation(
    session: &CancellationToken,
    request: CancellationToken,
) -> CancellationToken {
    let combined = CancellationToken::new();
    let trigger = combined.clone();
    let session = session.clone();
    tokio::spawn(async move {
        tokio::select! {
            biased;
            _ = session.cancelled() => trigger.cancel(),
            _ = request.cancelled() => trigger.cancel(),
            _ = trigger.cancelled() => {},
        }
    });
    combined
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for TrustedSessionMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(rmcp::model::Implementation::new(
                "buzz-trusted-session",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Typed, session-scoped Buzz collaboration tools. Signing credentials are held by the harness and are never model-visible.",
            )
    }
}
