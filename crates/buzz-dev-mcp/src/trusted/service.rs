//! Session-scoped MCP surface for harness-owned Buzz credentials.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ErrorData, ServerHandler,
};
use schemars::JsonSchema;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use super::tools::{prepare_handoff, publish_prepared_handoff};
use super::{
    cancel, dispatch, git, inbox, peers, send_chat, status, A2aCancelParams, A2aDispatchParams,
    A2aHandoffParams, A2aInboxParams, A2aPeersParams, A2aStatusParams, ChatSendParams,
    JobPrivilegeGate, PrivilegedGitOperationReceipt, PrivilegedOperationOutcome,
    ProjectGitCommitParams, ProjectGitOperation, ProjectGitParams, TrustedRelay,
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
    git_operation_lock: Arc<tokio::sync::Mutex<()>>,
    privilege_gate: Option<Arc<dyn JobPrivilegeGate>>,
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
        Self::new_with_privilege_gate(relay, session_cancellation, None)
    }

    /// Create a handler with an ACP-owned lifecycle fence. Job Git and
    /// handoff operations fail closed when this capability is absent.
    pub fn new_with_privilege_gate(
        relay: TrustedRelay,
        session_cancellation: CancellationToken,
        privilege_gate: Option<Arc<dyn JobPrivilegeGate>>,
    ) -> Self {
        Self {
            relay: Arc::new(relay),
            session_cancellation,
            git_operation_lock: Arc::new(tokio::sync::Mutex::new(())),
            privilege_gate,
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
        description = "Dispatch one signed A2A request within the exact local project, repository, peer, capability, branch, and worktree scope. Supply repository-relative paths to coordinate file ownership, or an empty paths list for an information-only request. GitHub references accept a positive number or canonical same-repository issue, pull, or Actions run URL."
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
        name = "buzz_a2a_peers",
        description = "List or resolve enrolled agents in the verified Nemo peer roster. Use the returned public key as recipient_pubkey for buzz_a2a_dispatch; duplicate names remain explicit."
    )]
    async fn buzz_a2a_peers(
        &self,
        Parameters(params): Parameters<A2aPeersParams>,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let cancellation = combine_cancellation(&self.session_cancellation, context.ct);
        let result = peers(&self.relay, params, cancellation.clone()).await;
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
        let invocation_id = uuid::Uuid::new_v4();
        let result = match self
            .begin_privileged_operation(ProjectGitOperation::Handoff, invocation_id, &cancellation)
            .await
        {
            Ok(mut lease) => {
                let operation_cancellation =
                    combine_cancellation(&cancellation, lease.cancellation_token());
                let result = match prepare_handoff(
                    &self.relay,
                    params,
                    operation_cancellation.clone(),
                )
                .await
                {
                    Ok(event) => match lease
                        .stage_handoff(event.clone(), operation_cancellation.clone())
                        .await
                    {
                        Ok(()) => {
                            publish_prepared_handoff(
                                &self.relay,
                                event,
                                operation_cancellation.clone(),
                            )
                            .await
                        }
                        Err(error) => tool_error(error),
                    },
                    Err(error) => tool_error(error),
                };
                let outcome = operation_outcome(&result, &operation_cancellation);
                operation_cancellation.cancel();
                let handoff_event_id = successful_event_id(&result);
                finish_privileged_operation(lease, outcome, None, handoff_event_id, result).await
            }
            Err(error) => tool_error(error),
        };
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

    #[tool(
        name = "buzz_project_git_commit",
        description = "Create one DCO and NIP-GS signed commit from already-staged changes inside this job session's exact receiver-verified Project checkout and path grant."
    )]
    async fn buzz_project_git_commit(
        &self,
        Parameters(params): Parameters<ProjectGitCommitParams>,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let cancellation = combine_cancellation(&self.session_cancellation, context.ct);
        let result = self
            .run_git_operation(
                ProjectGitOperation::Commit,
                cancellation.clone(),
                |operation_cancellation, receipt| async move {
                    git::commit(&self.relay, params, operation_cancellation, receipt).await
                },
            )
            .await;
        cancellation.cancel();
        Ok(result)
    }

    #[tool(
        name = "buzz_project_git_fetch",
        description = "Fetch only this job session's exact granted branch from its fixed Project origin. No caller URL, refspec, or helper is accepted."
    )]
    async fn buzz_project_git_fetch(
        &self,
        Parameters(params): Parameters<ProjectGitParams>,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let cancellation = combine_cancellation(&self.session_cancellation, context.ct);
        let result = self
            .run_git_operation(
                ProjectGitOperation::Fetch,
                cancellation.clone(),
                |operation_cancellation, receipt| async move {
                    git::fetch(&self.relay, params, operation_cancellation, receipt).await
                },
            )
            .await;
        cancellation.cancel();
        Ok(result)
    }

    #[tool(
        name = "buzz_project_git_push",
        description = "Non-force push HEAD to this job session's exact granted Project branch after verifying path scope, DCO trailers, and NIP-GS signatures."
    )]
    async fn buzz_project_git_push(
        &self,
        Parameters(params): Parameters<ProjectGitParams>,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let cancellation = combine_cancellation(&self.session_cancellation, context.ct);
        let result = self
            .run_git_operation(
                ProjectGitOperation::Push,
                cancellation.clone(),
                |operation_cancellation, receipt| async move {
                    git::push(&self.relay, params, operation_cancellation, receipt).await
                },
            )
            .await;
        cancellation.cancel();
        Ok(result)
    }
}

impl TrustedSessionMcp {
    /// Update the harness-owned reply destination before dispatching a turn.
    /// This mutates no model-visible authority or A2A scope.
    pub fn set_chat_thread_root_id(&self, thread_root_id: Option<&str>) -> Result<(), String> {
        self.relay.set_chat_thread_root_id(thread_root_id)
    }

    async fn begin_privileged_operation(
        &self,
        operation: ProjectGitOperation,
        invocation_id: uuid::Uuid,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn super::TrustedGitOperationLease>, String> {
        let gate = self.privilege_gate.as_ref().ok_or_else(|| {
            "job privilege lifecycle capability is unavailable; operation denied".to_owned()
        })?;
        gate.begin(operation, invocation_id, cancellation.clone())
            .await
    }

    async fn run_git_operation<F, Fut>(
        &self,
        operation: ProjectGitOperation,
        cancellation: CancellationToken,
        run: F,
    ) -> CallToolResult
    where
        F: FnOnce(CancellationToken, PrivilegedGitOperationReceipt) -> Fut,
        Fut: std::future::Future<Output = git::GitOperationExecution>,
    {
        let session_lock = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return tool_error("trusted Git operation was cancelled".into());
            }
            lock = self.git_operation_lock.lock() => lock,
        };
        let lock_key = match self
            .relay
            .grants
            .git_lock_key(self.relay.session_working_directory.as_deref())
        {
            Ok(key) => key,
            Err(error) => return tool_error(error),
        };
        let checkout_lock = match shared_checkout_lock(lock_key) {
            Ok(lock) => lock,
            Err(error) => return tool_error(error),
        };
        let checkout_guard = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return tool_error("trusted Git operation was cancelled".into());
            }
            lock = checkout_lock.lock_owned() => lock,
        };
        let invocation_id = uuid::Uuid::new_v4();
        let receipt = match git::operation_receipt(&self.relay, operation, invocation_id) {
            Ok(receipt) => receipt,
            Err(error) => return tool_error(error),
        };
        let lease = match self
            .begin_privileged_operation(operation, invocation_id, &cancellation)
            .await
        {
            Ok(lease) => lease,
            Err(error) => return tool_error(error),
        };
        let operation_cancellation =
            combine_cancellation(&cancellation, lease.cancellation_token());
        let execution = run(operation_cancellation.clone(), receipt).await;
        operation_cancellation.cancel();
        let result = finish_privileged_operation(
            lease,
            execution.outcome,
            Some(execution.receipt),
            None,
            execution.result,
        )
        .await;
        drop(checkout_guard);
        drop(session_lock);
        result
    }
}

async fn finish_privileged_operation(
    lease: Box<dyn super::TrustedGitOperationLease>,
    outcome: PrivilegedOperationOutcome,
    git_receipt: Option<PrivilegedGitOperationReceipt>,
    terminal_event_id: Option<String>,
    result: CallToolResult,
) -> CallToolResult {
    match lease.finish(outcome, git_receipt, terminal_event_id).await {
        Ok(()) => result,
        Err(error) => tool_error(error),
    }
}

fn successful_event_id(result: &CallToolResult) -> Option<String> {
    if result.is_error == Some(true) {
        return None;
    }
    let text = result.content.first()?.as_text()?.text.as_str();
    serde_json::from_str::<serde_json::Value>(text)
        .ok()?
        .get("event_id")?
        .as_str()
        .map(str::to_owned)
}

fn operation_outcome(
    result: &CallToolResult,
    cancellation: &CancellationToken,
) -> PrivilegedOperationOutcome {
    if cancellation.is_cancelled() {
        PrivilegedOperationOutcome::Cancelled
    } else if result.is_error == Some(true) {
        PrivilegedOperationOutcome::Failed
    } else {
        PrivilegedOperationOutcome::Completed
    }
}

fn tool_error(error: String) -> CallToolResult {
    CallToolResult::error(vec![rmcp::model::Content::text(error)])
}

type CheckoutLocks = Mutex<HashMap<PathBuf, Weak<tokio::sync::Mutex<()>>>>;

fn shared_checkout_lock(path: PathBuf) -> Result<Arc<tokio::sync::Mutex<()>>, String> {
    static LOCKS: OnceLock<CheckoutLocks> = OnceLock::new();
    let mut locks = LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| "trusted Git checkout lock is unavailable".to_owned())?;
    if let Some(lock) = locks.get(&path).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    locks.retain(|_, lock| lock.strong_count() > 0);
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    locks.insert(path, Arc::downgrade(&lock));
    Ok(lock)
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
