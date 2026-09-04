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

pub use trusted::{
    HarnessTrustedIdentity, JobPrivilegeGate, PrivilegeFuture, PrivilegedGitDisposition,
    PrivilegedGitOperationReceipt, PrivilegedOperationOutcome, ProjectGitOperation,
    TrustedGitOperationLease, TrustedSessionMcp, TrustedSessionScope,
};

#[derive(Clone)]
struct DevMcp {
    state: Arc<shell::SharedState>,
    todos: Arc<todo::TodoState>,
    tool_router: ToolRouter<DevMcp>,
}

#[tool_router]
impl DevMcp {
    fn new(state: Arc<shell::SharedState>) -> Self {
        Self {
            state,
            todos: Arc::new(todo::TodoState::new()),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        name = "shell",
        description = "Run a checkout-confined shell command on macOS. Ephemeral process per call with an explicit non-secret environment and no access to operator HOME or protected authority paths. This shell never receives the managed Nostr signer or relay credentials; use the typed Project Git tools for authorized commit/fetch/push. Output tail-truncated to ~8KB for the LLM; full output (first 10MB) saved to an isolated session artifact. timeout_ms defaults to 120000 (2 min) if omitted; capped at 1,200,000 (20 min). For long-running commands (cargo build, test suites), use 300000+. On PATH: rg and tree."
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
        description = "Read a regular text file inside the session checkout without following symlinks. Returns lines in `{number}:{content}` format. Use `offset` (0-based) and `limit` (default 2000) to window into large files. `workdir` may only select a directory inside the checkout. Prefer over cat/head/tail."
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
        Parameters(p): Parameters<view_image::ViewImageParams>,
    ) -> Result<CallToolResult, ErrorData> {
        view_image::run(&self.state, p).await
    }

    #[tool(
        name = "str_replace",
        description = "Descriptor-safe atomic find-and-replace in an ordinary checkout file. Symlinks, protected authority inputs, and every `.git` component are rejected. old_str must occur exactly once unless replace_all is true. `workdir` may only select a directory inside the checkout. Returns a unified diff."
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
    let protected_paths = paths::ProtectedPathPolicy::take_from_environment()?;
    // A standalone/generic MCP never captures signing material. Typed Buzz
    // publishing is hosted in-process by buzz-acp on a loopback-only server.
    trusted::scrub_harness_environment();

    // Credential capture and process-environment scrubbing precede tracing.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let shim = shim::Shim::install()?;
    let state = Arc::new(shell::SharedState::new_with_protected_paths(
        cwd,
        shim,
        protected_paths,
    )?);

    let service = DevMcp::new(state).serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
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

#[cfg(test)]
mod tool_inventory_tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn generic_and_trusted_tool_names_are_separate_and_secret_free() {
        let generic: HashSet<String> = DevMcp::tool_router()
            .list_all()
            .into_iter()
            .map(|tool| tool.name.into_owned())
            .collect();
        let trusted: HashSet<String> = trusted::TrustedSessionMcp::tool_names()
            .into_iter()
            .collect();
        assert!(generic.is_disjoint(&trusted));
        assert_eq!(
            trusted,
            HashSet::from_iter(
                [
                    "buzz_a2a_dispatch",
                    "buzz_a2a_inbox",
                    "buzz_a2a_status",
                    "buzz_a2a_cancel",
                    "buzz_a2a_handoff",
                    "buzz_chat_send",
                    "buzz_private_media_get",
                    "buzz_project_git_commit",
                    "buzz_project_git_fetch",
                    "buzz_project_git_push",
                ]
                .map(str::to_owned)
            )
        );
        assert!(generic.contains("view_image"));
        assert!(!generic.iter().any(|name| name.starts_with("buzz_")));
        let inventory = serde_json::to_string(&DevMcp::tool_router().list_all())
            .expect("serialize generic MCP tool inventory");
        for forbidden in [
            "BUZZ_PRIVATE_KEY",
            "BUZZ_PRIVATE_KEY_FILE",
            "BUZZ_AUTH_TAG",
            "BUZZ_AUTH_TAG_FILE",
            "`buzz ",
        ] {
            assert!(
                !inventory.contains(forbidden),
                "MCP tool inventory exposed forbidden guidance {forbidden:?}"
            );
        }
    }
}
