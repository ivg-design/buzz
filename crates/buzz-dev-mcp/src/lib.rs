#![cfg_attr(not(windows), forbid(unsafe_code))]
#![cfg_attr(windows, deny(unsafe_code))]
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
    transport::stdio,
    ErrorData, ServerHandler, ServiceExt,
};
use std::path::Path;
use std::sync::Arc;

mod paths;
mod read_file;
mod rg;
mod shell;
mod shim;
mod str_replace;
mod todo;
mod tree;
mod trusted;
mod view_image;

#[derive(Clone)]
struct DevMcp {
    state: Arc<shell::SharedState>,
    todos: Arc<todo::TodoState>,
    trusted: Option<Arc<trusted::TrustedRelay>>,
    tool_router: ToolRouter<DevMcp>,
}

#[tool_router]
impl DevMcp {
    fn new(state: Arc<shell::SharedState>, trusted: Option<Arc<trusted::TrustedRelay>>) -> Self {
        Self {
            state,
            todos: Arc::new(todo::TodoState::new()),
            trusted,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        name = "buzz_a2a_dispatch",
        description = "Dispatch one signed A2A job request inside the exact local project, peer, capability, path, branch, and worktree grant. Repository URL, current base SHA, project, channel, signer, sponsor, and tenant come from trusted local state; they cannot be supplied by the caller."
    )]
    async fn buzz_a2a_dispatch(
        &self,
        Parameters(params): Parameters<trusted::A2aDispatchParams>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(match &self.trusted {
            Some(relay) => trusted::dispatch(relay, params).await,
            None => trusted_unavailable(),
        })
    }

    #[tool(
        name = "buzz_a2a_inbox",
        description = "Read validated A2A requests and controls addressed to this agent in locally granted project channels. Auth tags and signing credentials are never returned."
    )]
    async fn buzz_a2a_inbox(
        &self,
        Parameters(params): Parameters<trusted::A2aInboxParams>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(match &self.trusted {
            Some(relay) => trusted::inbox(relay, params).await,
            None => trusted_unavailable(),
        })
    }

    #[tool(
        name = "buzz_a2a_status",
        description = "Read one validated A2A request and its receipts, progress, controls, and terminal result. The request must match local project and peer grants."
    )]
    async fn buzz_a2a_status(
        &self,
        Parameters(params): Parameters<trusted::A2aStatusParams>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(match &self.trusted {
            Some(relay) => trusted::status(relay, params).await,
            None => trusted_unavailable(),
        })
    }

    #[tool(
        name = "buzz_a2a_cancel",
        description = "Ask the addressed worker to cancel an active A2A job. Available only to the original requester in the exact channel-bound session; it cannot publish a cancellation acknowledgement."
    )]
    async fn buzz_a2a_cancel(
        &self,
        Parameters(params): Parameters<trusted::A2aCancelParams>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(match &self.trusted {
            Some(relay) => trusted::cancel(relay, params).await,
            None => trusted_unavailable(),
        })
    }

    #[tool(
        name = "buzz_a2a_handoff",
        description = "Publish the current recipient's signed handoff request for an active A2A job. This never creates or executes the superseding request; the requester must dispatch that separately at a higher coordinator epoch."
    )]
    async fn buzz_a2a_handoff(
        &self,
        Parameters(params): Parameters<trusted::A2aHandoffParams>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(match &self.trusted {
            Some(relay) => trusted::handoff(relay, params).await,
            None => trusted_unavailable(),
        })
    }

    #[tool(
        name = "buzz_chat_send",
        description = "Send a normal Buzz chat message to this ACP session's fixed channel and, when present, fixed thread. The caller cannot choose a channel, thread, recipient, event kind, or signing identity."
    )]
    async fn buzz_chat_send(
        &self,
        Parameters(params): Parameters<trusted::ChatSendParams>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(match &self.trusted {
            Some(relay) => trusted::send_chat(relay, params).await,
            None => trusted_unavailable(),
        })
    }

    #[tool(
        name = "shell",
        description = "Run a shell command (bash by default; set `BUZZ_SHELL` to use cmd, PowerShell, or another shell). Ephemeral process per call. Output tail-truncated to ~8KB for the LLM; full output (first 10MB) saved to artifact file. timeout_ms defaults to 120000 (2 min) if omitted; capped at 1,200,000 (20 min). For long-running commands (git push with hooks, cargo build, test suites), use 300000+. On PATH: rg (prefer over grep; flags: -n -i -l -g <glob> -C <n> --files) and tree (flags: -d <depth>; shows line counts)."
    )]
    async fn shell(
        &self,
        Parameters(p): Parameters<shell::ShellParams>,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        shell::run(&self.state, p, context.ct).await
    }

    #[tool(
        name = "read_file",
        description = "Read a text file and return its contents with line numbers. Returns lines in `{number}:{content}` format. Use `offset` (0-based) and `limit` (default 2000) to window into large files. Path resolved relative to workdir (defaults to server cwd). Prefer over cat/head/tail."
    )]
    async fn read_file(
        &self,
        Parameters(p): Parameters<read_file::ReadFileParams>,
    ) -> Result<String, ErrorData> {
        read_file::run(&self.state, p)
    }

    #[tool(
        name = "view_image",
        description = "Load an image from a file path, http(s) URL, or data: URL and return it as an MCP image content block that multimodal LLMs (Anthropic, OpenAI-compatible, etc.) can see. Resizes to a longest-edge of 1568px by default (override with `max_dim`, range 64..=2048). Pass-through for already-small PNG/JPEG; transcodes oversize input to PNG (if alpha) or JPEG q85. Animated GIF/WebP rejected — provide a still frame. Hard cap 20 MiB source, ~4 MiB on the wire. Relative paths resolve under `workdir` (defaults to server cwd) and may not escape it."
    )]
    async fn view_image(
        &self,
        Parameters(mut p): Parameters<view_image::ViewImageParams>,
    ) -> Result<CallToolResult, ErrorData> {
        if let Some(relay) = &self.trusted {
            match relay.fetch_private_media(&p.source).await {
                Ok(Some(bytes)) => {
                    use base64::Engine as _;
                    p.source = format!(
                        "data:image/png;base64,{}",
                        base64::engine::general_purpose::STANDARD.encode(bytes)
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    return Ok(CallToolResult::error(vec![rmcp::model::Content::text(
                        error,
                    )]));
                }
            }
        }
        view_image::run(&self.state, p).await
    }

    #[tool(
        name = "str_replace",
        description = "Atomic find-and-replace in a file. old_str must occur exactly once unless replace_all is true, in which case all occurrences are replaced. Returns a unified diff. Path resolved relative to workdir (defaults to server cwd). Prefer over sed/awk."
    )]
    async fn str_replace(
        &self,
        Parameters(p): Parameters<str_replace::StrReplaceParams>,
    ) -> Result<String, ErrorData> {
        str_replace::run(&self.state, p)
    }

    #[tool(
        name = "todo",
        description = "Session checklist only for work that must continue across turns or survive context compaction. Do not use for work you can finish in the current turn. Omit `todos` to read; provide the full {text, done} list to replace it. Open items let the _Stop hook advise against ending."
    )]
    async fn todo(
        &self,
        Parameters(p): Parameters<todo::TodoParams>,
    ) -> Result<CallToolResult, ErrorData> {
        match self.todos.handle_todo(p) {
            Ok(text) => todo::text_result(text),
            Err(e) => todo::error_result(format!("Error: {e}")),
        }
    }

    /// Hook: called by the agent before honoring end_turn. Returns
    /// non-empty objection text iff items remain open.
    #[tool(
        name = "_Stop",
        description = "Returns open todo items if any exist. Used by the agent's _Stop lifecycle hook to advise against ending with incomplete work."
    )]
    async fn stop_hook(
        &self,
        Parameters(_): Parameters<todo::HookParams>,
    ) -> Result<CallToolResult, ErrorData> {
        todo::text_result(self.todos.stop_objection())
    }

    /// Hook: called by the agent after context compaction/handoff so the
    /// todo list survives history truncation.
    #[tool(
        name = "_PostCompact",
        description = "Internal hook. Agent invokes after handoff; returns todo state for re-injection."
    )]
    async fn post_compact_hook(
        &self,
        Parameters(_): Parameters<todo::HookParams>,
    ) -> Result<CallToolResult, ErrorData> {
        todo::text_result(self.todos.post_compact())
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for DevMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(rmcp::model::Implementation::new(
                "buzz-dev-mcp",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(self.state.bootstrap_instructions.clone())
    }
}

pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let argv0 = std::env::args().next().unwrap_or_default();
    let cmd = Path::new(&argv0)
        .file_stem()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    // Multicall dispatch — sync personalities exit before any runtime is built.
    // No tracing, no tokio, no allocations beyond argv parsing.
    match cmd.as_str() {
        "rg" => std::process::exit(rg::run(std::env::args().skip(1).collect())),
        "tree" => std::process::exit(tree::run(std::env::args().skip(1).collect())),
        _ => {}
    }

    // Async personalities and MCP server mode — build the runtime.
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async_main(cmd))
}

async fn async_main(_cmd: String) -> Result<(), Box<dyn std::error::Error>> {
    // HTTPS clients invoked through this MCP process need a Rustls provider;
    // repeated installation is harmless.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cwd = std::env::current_dir()?;
    let trusted = trusted::TrustedConfig::capture(&cwd)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?
        .map(trusted::TrustedRelay::new)
        .transpose()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?
        .map(Arc::new);

    // Credential capture and process-environment scrubbing precede tracing.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let shim = shim::Shim::install()?;
    let state = Arc::new(shell::SharedState::new(cwd, shim)?);

    let service = DevMcp::new(state, trusted).serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}

fn trusted_unavailable() -> CallToolResult {
    CallToolResult::error(vec![rmcp::model::Content::text(
        "trusted Buzz tools are unavailable because no signing identity was configured",
    )])
}

/// Suppress the console window that Windows otherwise allocates for every
/// console-subsystem child process spawned from a non-console parent.
/// No-op on non-Windows platforms.
pub(crate) fn configure_no_window(cmd: &mut std::process::Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt as _;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    #[cfg(not(windows))]
    let _ = cmd;
}

/// Suppress the console window for async (`tokio::process::Command`) spawns.
/// Equivalent to `configure_no_window` but accepts a tokio command.
/// No-op on non-Windows platforms.
pub(crate) fn configure_no_window_async(cmd: &mut tokio::process::Command) {
    #[cfg(windows)]
    {
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    #[cfg(not(windows))]
    let _ = cmd;
}
