//! Agent pool — owns N AcpClient instances and dispatches prompt tasks.
//!
//! # Mental model
//!
//! ```text
//!   AgentPool
//!   ├── agents: Vec<Option<OwnedAgent>>   ← idle agents sit here
//!   ├── join_set: JoinSet<()>             ← in-flight tasks
//!   ├── task_map: HashMap<Id, TaskMeta>   ← panic recovery metadata
//!   └── result_tx/rx: mpsc channel        ← tasks return agents here
//!
//!   Dispatch:
//!     try_claim() → OwnedAgent (removed from slot)
//!     spawn run_prompt_task(agent, ...) into join_set
//!     task sends PromptResult { agent, outcome } via result_tx
//!     rx_and_join_set() → poll result_rx for PromptResult
//!     return_agent(agent) → puts agent back in slot
//! ```
//!
//! `AcpClient` is NOT Clone — ownership moves out on claim and back on return.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;
use uuid::Uuid;

use crate::acp::{
    extract_model_config_options, extract_model_state, extract_thought_level_config_id,
    model_in_catalog, resolve_model_switch_method, AcpClient, AcpError, EnvVar, JobSessionPolicy,
    McpServer, ModelSwitchMethod, StopReason, SystemPromptTransport,
};
use crate::config::{compose_scoped_session_title, DedupMode, PermissionMode};
use crate::observer;
use crate::prompt_project::{pick_authoritative_project_home, PromptProjectInfo};
use crate::queue::{
    CancelReason, ContextMessage, ConversationContext, FlushBatch, PromptChannelInfo,
    PromptProfile, PromptProfileLookup, ThreadTags,
};
use crate::relay::{ChannelInfo, RestClient};
use crate::scope::SessionScope;
use crate::trusted_mcp::{TrustedMcpFactory, TrustedMcpSession};

/// Window within which agent activity before a hard-cap death qualifies
/// the turn as "recently active" (eligible for requeue instead of dead-letter).
const RECENT_ACTIVITY_WINDOW: Duration = Duration::from_secs(60);

// FlushBatch and BatchEvent derive Clone (added in queue.rs) so legacy chat
// harnesses can store a recoverable copy in TaskMeta for panic recovery. A
// harness with durable provider-session recovery leaves this empty because a
// task panic cannot prove the provider did not already act.

/// Metadata stored per in-flight task for panic recovery.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SuccessfulSteerDelivery {
    pub event_id: String,
    pub session_id: String,
}

pub struct TaskMeta {
    pub agent_index: usize,
    pub channel_id: Option<Uuid>,
    /// Session scope of the in-flight turn (mid-turn steer/signal routing and
    /// scope-to-worker affinity target this). `None` for heartbeat tasks.
    /// Invariant when `Some`: `scope.channel_id() == channel_id.unwrap()`.
    pub scope: Option<SessionScope>,
    /// Identifies terminal events when the task panics before returning a result.
    pub turn_id: String,
    /// Clone of batch for Queue mode panic recovery.
    pub recoverable_batch: Option<FlushBatch>,
    /// Control signal for the in-flight prompt task.
    /// `None` for heartbeat tasks (not controllable) and after signal is consumed.
    pub control_tx: Option<tokio::sync::oneshot::Sender<ControlSignal>>,
    /// Steer request channel for non-cancelling mid-turn delivery.
    /// Capacity-1; `try_send` from the main loop fails on `Full`/`Closed`,
    /// in which case the caller must fall back to the universal
    /// `ControlSignal::Steer` cancel+merge path. `None` for heartbeat
    /// tasks only — all prompt tasks install a steer channel regardless
    /// of the agent's name.
    pub steer_tx: Option<tokio::sync::mpsc::Sender<SteerRequest>>,
    /// Successful non-cancelling steers acknowledged while this task owned the
    /// live session. The session ID prevents a late ack from contaminating a
    /// replacement session after task return.
    pub successful_steer_deliveries: HashSet<SuccessfulSteerDelivery>,
}

/// Agent-level model capabilities. Populated on first session creation.
/// The catalog is the same across all sessions for a given agent process.
/// Fields are read by the desktop's `get_agent_models` Tauri command (Phase 3).
#[allow(dead_code)] // Scaffolding for desktop integration — fields read via serde.
pub struct AgentModelCapabilities {
    /// Stable: configOptions with category "model" from session/new.
    pub config_options_raw: Vec<serde_json::Value>,
    /// Unstable: SessionModelState from session/new.
    pub available_models_raw: Option<serde_json::Value>,
    /// B5: configId for the `thought_level` category option, if the adapter
    /// advertised one in session/new. Resolved at session time so the
    /// spawn-scoped effort application forwards the adapter's real configId
    /// instead of hardcoding it. `None` when the adapter advertises no
    /// `thought_level` option.
    pub thought_level_config_id: Option<String>,
}

/// Successful deliveries associated with one live channel session.
#[derive(Default)]
pub struct ChannelDeliveryState {
    /// Whether a legacy user message has successfully carried standing context.
    pub standing_context_sent: bool,
    /// Buzz event IDs already delivered to this ACP session, either as trigger
    /// events or conversation context.
    pub delivered_event_ids: HashSet<String>,
}

/// Immutable workspace policy identity bound to one live provider session.
///
/// The relay can republish a Project home while the harness is running.  A
/// provider session created for the old Project must never continue under the
/// new Project's authority without being recreated and receiving the newly
/// pinned instructions.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspaceInstructionBinding {
    home_channel: Uuid,
    project: PromptProjectInfo,
    revision: String,
}

/// Per-channel session IDs, turn counters, and delivery state.
///
/// Separated from `OwnedAgent` so the state machine is testable without
/// spawning a real agent subprocess.
#[derive(Default)]
pub struct SessionState {
    /// session scope → session_id
    pub sessions: HashMap<SessionScope, String>,
    pub heartbeat_session: Option<String>,
    /// Per-scope turn counters for proactive session rotation.
    /// Incremented on each successful prompt; reset when the session is rotated.
    pub turn_counts: HashMap<SessionScope, u32>,
    /// Turn counter for the heartbeat session.
    pub heartbeat_turn_count: u32,
    /// Whether the live heartbeat session has successfully received `<base>`.
    pub heartbeat_standing_context_sent: bool,
    /// session scope → rendered NIP-AE core prompt section, populated once at
    /// session creation per Tyler's spec (no mid-session refresh).
    pub core_sections: HashMap<SessionScope, String>,
    /// session scope → rendered `<channel-canvas>` metadata section.
    ///
    /// Populated once before session creation (same lifecycle as `core_sections`).
    /// Absent when the channel has no canvas, the canvas content is blank, or the
    /// fetch fails — all fail open. Cleared on session invalidation alongside
    /// `core_sections` so the next session picks up any canvas change.
    pub canvas_sections: HashMap<SessionScope, String>,
    /// Per-scope successful-delivery state. Created with the ACP session and
    /// cleared atomically with every invalidation path.
    pub deliveries: HashMap<SessionScope, ChannelDeliveryState>,
    /// Workspace Project/revision used when each provider session was created.
    workspace_instruction_bindings: HashMap<SessionScope, WorkspaceInstructionBinding>,
    /// Workspace Project/revision used when the heartbeat session was created.
    heartbeat_workspace_instruction_binding: Option<WorkspaceInstructionBinding>,
    /// Harness-owned trusted MCP capability for each live provider session.
    /// Removing an entry cancels its loopback listener and invalidates its
    /// bearer token.
    trusted_mcp: HashMap<SessionScope, TrustedMcpSession>,
}

impl SessionState {
    /// Invalidate the session (and turn counter) for a specific prompt source.
    pub fn invalidate(&mut self, source: &PromptSource) {
        match source {
            PromptSource::Channel(scope) => {
                self.invalidate_scope(scope);
            }
            PromptSource::Heartbeat => {
                self.heartbeat_session = None;
                self.heartbeat_turn_count = 0;
                self.heartbeat_standing_context_sent = false;
                self.heartbeat_workspace_instruction_binding = None;
            }
        }
    }

    /// Invalidate a single session scope's session and turn counter.
    /// Returns `true` if the scope had an active session.
    pub fn invalidate_scope(&mut self, scope: &SessionScope) -> bool {
        self.turn_counts.remove(scope);
        self.core_sections.remove(scope);
        self.canvas_sections.remove(scope);
        self.deliveries.remove(scope);
        self.workspace_instruction_bindings.remove(scope);
        self.trusted_mcp.remove(scope);
        self.sessions.remove(scope).is_some()
    }

    /// Invalidate every session scope belonging to `channel_id` (channel-wide
    /// cleanup, e.g. when the agent is removed from a channel). Returns the
    /// number of scopes that had an active session.
    pub fn invalidate_channel(&mut self, channel_id: &Uuid) -> usize {
        let scopes: Vec<SessionScope> = self
            .sessions
            .keys()
            .chain(self.turn_counts.keys())
            .chain(self.core_sections.keys())
            .chain(self.canvas_sections.keys())
            .chain(self.deliveries.keys())
            .chain(self.workspace_instruction_bindings.keys())
            .chain(self.trusted_mcp.keys())
            .filter(|s| s.channel_id() == *channel_id)
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let mut count = 0;
        for scope in scopes {
            if self.invalidate_scope(&scope) {
                count += 1;
            }
        }
        count
    }

    /// Invalidate all sessions and turn counters (e.g. after agent exit).
    pub fn invalidate_all(&mut self) {
        self.sessions.clear();
        self.turn_counts.clear();
        self.heartbeat_session = None;
        self.heartbeat_turn_count = 0;
        self.heartbeat_standing_context_sent = false;
        self.core_sections.clear();
        self.canvas_sections.clear();
        self.deliveries.clear();
        self.workspace_instruction_bindings.clear();
        self.heartbeat_workspace_instruction_binding = None;
        self.trusted_mcp.clear();
    }

    fn trusted_mcp_valid_for(&self, scope: &SessionScope, duration: Duration) -> bool {
        self.trusted_mcp
            .get(scope)
            .is_some_and(|session| session.is_valid_for(duration))
    }

    pub(crate) fn mark_scope_delivery_success(
        &mut self,
        scope: SessionScope,
        standing_context_sent: bool,
        event_ids: impl IntoIterator<Item = String>,
    ) {
        let delivery = self.deliveries.entry(scope).or_default();
        delivery.standing_context_sent |= standing_context_sent;
        delivery.delivered_event_ids.extend(event_ids);
    }

    #[cfg(test)]
    fn has_channel_state(&self, channel_id: &Uuid) -> bool {
        let matches = |s: &SessionScope| s.channel_id() == *channel_id;
        self.sessions.keys().any(matches)
            || self.turn_counts.keys().any(matches)
            || self.core_sections.keys().any(matches)
            || self.canvas_sections.keys().any(matches)
            || self.deliveries.keys().any(matches)
            || self.workspace_instruction_bindings.keys().any(matches)
            || self.trusted_mcp.keys().any(matches)
    }
}

/// An agent with its session state, owned by the pool or a running task.
pub struct OwnedAgent {
    pub index: usize,
    pub acp: AcpClient,
    pub state: SessionState,
    /// Model catalog from first session/new. None until first session created.
    pub model_capabilities: Option<AgentModelCapabilities>,
    /// Desired model ID (from `Config.model`). Applied after every `session_new_full()`.
    pub desired_model: Option<String>,
    /// Whether `desired_model` was set by a live `SwitchModel` control signal
    /// (as opposed to being derived from config/persona at spawn). Used by the
    /// desktop reader to distinguish a genuine runtime override from a stale
    /// session whose persona model was edited. Reset on spawn/restart.
    pub model_overridden: bool,
    /// Opaque per-pick `request_id` from the live `SwitchModel` that set
    /// `desired_model`, echoed on the late `control_result` frame so the
    /// Desktop ModelPicker can correlate it to the pick that fired the switch.
    /// `None` for config/persona-derived models (no live pick to correlate).
    pub desired_model_request_id: Option<String>,
    /// True when a busy-path live switch is awaiting its deferred apply: the
    /// switch was delivered to an in-flight turn (`sent` ack), the turn was
    /// cancelled+requeued, and the real apply runs at the next session. On that
    /// apply, `create_session_and_apply_model` emits a positive terminal
    /// `control_result` (success) so the Desktop learns the outcome instead of
    /// inferring it from timeout silence. The idle path never sets this — it
    /// already emits its terminal immediately — so this gate prevents a
    /// double-emit there. Consumed (reset) at apply time.
    pub desired_model_pending_ack: bool,
    /// Persisted startup effort value from `BUZZ_ACP_EFFORT_LEVEL` (carried from
    /// the Desktop record via `Config.effort_level`). Held per-worker and applied
    /// once, at the first session creation, by pairing with the adapter's
    /// advertised `thought_level` configId. This is spawn-scoped only — there is
    /// no pool-level effort state and no live mid-conversation effort switching.
    /// Non-fatal when absent or when the adapter does not advertise
    /// `thought_level`.
    pub startup_effort: Option<String>,
    /// Normalized agent name from initialize (`agentInfo.name`/`serverInfo.name`).
    pub agent_name: String,
    /// Whether Goose accepted its custom system-prompt method. `None` probes on
    /// the first session; method-not-found is cached as `Some(false)` so legacy
    /// user-message framing is used for this process thereafter.
    pub goose_system_prompt_supported: Option<bool>,
    /// Protocol version reported by the agent in its initialize response.
    pub protocol_version: u32,
}

/// Package name reported by `claude-agent-acp` in its `initialize` response.
/// Any adapter reporting this name supports `_meta.systemPrompt: {append: ...}`
/// on `session/new` — the feature landed in v0.6.0 (Oct 2025), before the
/// `@zed-industries/claude-code-acp` → `@agentclientprotocol/claude-agent-acp`
/// rename, so the new name is a reliable capability gate.
const CLAUDE_AGENT_ACP_NAME: &str = "@agentclientprotocol/claude-agent-acp";

fn has_system_prompt_support(
    protocol_version: u32,
    agent_name: &str,
    goose_system_prompt_supported: Option<bool>,
    developer_instructions_append_supported: bool,
) -> bool {
    if agent_name == "goose" {
        goose_system_prompt_supported == Some(true)
    } else if developer_instructions_append_supported || agent_name == CLAUDE_AGENT_ACP_NAME {
        true
    } else {
        protocol_version >= 2
    }
}

fn session_new_system_prompt<'a>(
    is_goose: bool,
    protocol_version: u32,
    agent_name: &str,
    developer_instructions_append_supported: bool,
    prompt: Option<&'a str>,
) -> Option<SystemPromptTransport<'a>> {
    if is_goose {
        None
    } else if developer_instructions_append_supported || agent_name == CLAUDE_AGENT_ACP_NAME {
        prompt.map(SystemPromptTransport::MetaAppend)
    } else if protocol_version < 2 {
        None
    } else {
        prompt.map(SystemPromptTransport::Field)
    }
}

impl OwnedAgent {
    pub(crate) fn has_system_prompt_support(&self) -> bool {
        has_system_prompt_support(
            self.protocol_version,
            &self.agent_name,
            self.goose_system_prompt_supported,
            self.acp.developer_instructions_append_supported(),
        )
    }
}

/// Pool of agents with take-and-return ownership semantics.
///
/// Agents are either idle (sitting in `agents[i]`) or checked out
/// (running inside a spawned task). The `task_map` tracks in-flight
/// tasks for panic recovery.
pub struct AgentPool {
    agents: Vec<Option<OwnedAgent>>,
    result_tx: mpsc::UnboundedSender<PromptResult>,
    result_rx: mpsc::UnboundedReceiver<PromptResult>,
    pub join_set: JoinSet<()>,
    task_map: HashMap<tokio::task::Id, TaskMeta>,
    /// Authoritative directory of which worker most recently owned each session
    /// scope's provider session. Survives while a worker is checked out (its
    /// `SessionState` is invisible to the pool then), so a busy owner does not
    /// cause another worker to open a duplicate session for the same thread.
    /// Best-effort: stale entries (rotation, crash/respawn) self-heal on the
    /// next dispatch and are pruned on channel-wide session invalidation.
    session_owners: HashMap<SessionScope, usize>,
}

/// Result returned by a completed prompt task.
pub struct PromptResult {
    pub agent: OwnedAgent,
    pub source: PromptSource,
    /// Identifies the completed turn for observer terminal events.
    pub turn_id: String,
    pub outcome: PromptOutcome,
    /// Present on failure in Queue mode, for requeue.
    pub batch: Option<FlushBatch>,
}

/// Whether the prompt came from a channel event or a heartbeat.
///
/// The channel variant carries the full [`SessionScope`] resolved at admission
/// (conversation or thread), not just the channel id, so completion and
/// invalidation target the exact session. Use [`channel_id`](PromptSource::channel_id)
/// where only the channel is needed.
#[derive(Debug)]
pub enum PromptSource {
    Channel(SessionScope),
    Heartbeat,
}

impl PromptSource {
    /// The channel this prompt belongs to, or `None` for heartbeats.
    pub fn channel_id(&self) -> Option<Uuid> {
        match self {
            Self::Channel(scope) => Some(scope.channel_id()),
            Self::Heartbeat => None,
        }
    }

    /// The exact session scope this prompt belongs to, or `None` for
    /// heartbeats. Callers that must target the precise thread (e.g. clearing a
    /// typing indicator on completion) use this rather than [`channel_id`], so a
    /// finishing turn never disturbs a sibling thread in the same channel.
    ///
    /// [`channel_id`]: PromptSource::channel_id
    pub fn scope(&self) -> Option<&SessionScope> {
        match self {
            Self::Channel(scope) => Some(scope),
            Self::Heartbeat => None,
        }
    }
}

/// Apply state effects for Race 1, where a control signal arrives just after the
/// prompt completed naturally. The prompt result has already been consumed by
/// `select!`, so the harness must synthesize a successful result while still
/// honoring any load-bearing control signal semantics.
fn apply_completed_before_control_signal(
    state: &mut SessionState,
    source: &PromptSource,
    control_signal: &ControlSignal,
) {
    // Rotate and SwitchModel both invalidate so the next turn creates a fresh
    // session. For SwitchModel the caller has already set `desired_model`, so
    // the fresh session applies the new model on its next creation.
    if matches!(
        control_signal,
        ControlSignal::Rotate | ControlSignal::SwitchModel { .. }
    ) {
        state.invalidate(source);
    }
}

/// Control signal for an in-flight channel turn.
///
/// Not `Copy`: `SwitchModel` carries owned `String`s. Callers must clone when
/// a value is needed after a move, or match by reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlSignal {
    /// Stop the current turn and drop its triggering batch.
    Cancel,
    /// Stop the current turn and requeue its triggering batch for a merged
    /// re-prompt framed as a **supersede**: the new request replaces the old.
    Interrupt,
    /// Stop the current turn and requeue its triggering batch for a merged
    /// re-prompt framed as a **steer**: a message arrived while the agent was
    /// working; it should continue its work and incorporate the message if
    /// relevant, not treat it as a replacement task. This is the default
    /// mid-turn delivery path (see [`MultipleEventHandling::Steer`]).
    Steer,
    /// Stop the current turn and drop its triggering batch. The session is
    /// invalidated just like cancel; the next turn creates a fresh session.
    Rotate,
    /// Switch the agent's model, then requeue the triggering batch so it
    /// re-runs on a fresh session under the new model. The model lands by
    /// setting `OwnedAgent::desired_model` before invalidation; the requeued
    /// turn re-creates the session and re-applies `desired_model`. Runtime-only
    /// — never persisted, gone on restart/respawn.
    ///
    /// Carries `(model_id, request_id)`: the opaque per-pick `request_id`
    /// originates in the Desktop ModelPicker and is echoed on every
    /// `control_result` frame so a replayed result cannot settle a later pick.
    SwitchModel {
        model_id: String,
        request_id: Option<String>,
    },
}

/// Goose-native non-cancelling steer request, sent from the main loop to an
/// in-flight prompt task's read loop via a capacity-1 mpsc channel.
///
/// The read loop owns the `AcpClient`'s reader/writer for the duration of the
/// turn, so we cannot drive a steer write from the main thread directly. The
/// main loop carries the steer prompt body (already framed by
/// `queue::native_steer_framing()` + `queue::format_event_block`); the read
/// loop completes `sessionId` (lexical) and `expectedRunId`
/// (`AcpClient::active_run_id` at write time) when it actually emits the
/// JSON-RPC request. The main loop awaits a `SteerAck` on the `ack_tx`
/// oneshot.
///
/// ## Why the read loop fills params, not the main loop
///
/// `expectedRunId` is a *moving target*: the read loop updates
/// `self.active_run_id` as goose emits `session/update` notifications, and
/// the steer is rejected if the supplied id doesn't match the *current* run.
/// A snapshot taken at dispatch (or at mode-gate time) can be stale by the
/// time the read loop actually writes the steer line. Filling params at
/// write time uses the freshest possible run id and is correct-by-
/// construction on the one field whose freshness the protocol checks.
/// `sessionId` is in lexical scope inside the read loop's caller
/// (`session_prompt_blocks_with_idle_timeout`), so no plumbing is required
/// for that — only a function parameter pass-through.
///
/// If `active_run_id` is `None` at write time (no `session/update` seen yet
/// — e.g. agents that never emit run-id metadata), the goose-native method
/// cannot form a valid `expectedRunId`, and the read loop falls back to the
/// cross-adapter `_session/steering` method when the agent advertised
/// `_meta.steering.supported` at `initialize`. That method takes no run id, so
/// no freshness concern applies to it. When neither transport is available the
/// read loop acks [`SteerError::ExpectedRunIdMissing`]. The main loop maps that
/// to the "Err-before-pending" bucket: no withhold/mark was established at
/// `pool::send_steer` time because the request was rejected before any
/// write, so the watcher only needs to release nothing and fall back to the
/// universal `ControlSignal::Steer` cancel+merge path.
pub struct SteerRequest {
    /// Prompt body text blocks. Each entry becomes one `text` content
    /// block in `params.prompt`. Built by the main loop via
    /// `queue::native_steer_framing()` + `queue::format_event_block` so
    /// the wording cannot drift from the cancel+merge fallback path.
    pub prompt_blocks: Vec<String>,
    /// Oneshot for the read loop to report the outcome.
    pub ack_tx: tokio::sync::oneshot::Sender<SteerAck>,
}

/// Why a mid-turn steer failed, on either transport
/// (`_goose/unstable/session/steer` or `_session/steering`).
///
/// String and integer fields are intentionally `Debug`-only — read by
/// `tracing` macros in the main loop's `PoolEvent::SteerAck` arm via
/// `?ack`. The dead-code lint can't see that path because it doesn't
/// trace through `Debug` derives, hence the `#[allow]`.
#[allow(dead_code)]
#[derive(Debug)]
pub enum SteerError {
    /// The agent returned a JSON-RPC error response to the steer request.
    ///
    /// `code` is the JSON-RPC error code:
    /// - `-32601` (`method_not_found`): the agent does not implement the
    ///   steer extension. The main loop should fire the cancel+merge
    ///   fallback so the message still reaches the agent.
    /// - Any other code: the write landed and the agent rejected it at the
    ///   application level (e.g. wrong run id). Release the withheld event
    ///   for normal dispatch; do NOT fire the fallback — the turn is still
    ///   running or just ended.
    AgentError { code: i64, message: String },
    /// Transport-level failure: write error, read EOF, JSON-RPC framing
    /// violation, etc. The string carries the underlying `AcpError`'s display.
    Transport(String),
    /// At steer-write time neither steer transport was available: no
    /// `expectedRunId` (`AcpClient::active_run_id` was `None`, so the
    /// goose-native method could not be formed) and the agent did not
    /// advertise the cross-adapter `_session/steering` extension. The read
    /// loop drops the request without writing anything; the main loop should
    /// release any withheld event and fall back to the universal cancel+merge
    /// `ControlSignal::Steer` path. This is in the same "Err-before-pending"
    /// bucket as `Transport` write failures: no in-process state was
    /// established, so no in-process cleanup is needed.
    ExpectedRunIdMissing,
    /// A `_session/steering` request returned a JSON-RPC *success* whose
    /// `outcome` was not one of the two recognized delivery outcomes
    /// (`injected`, `startedNewTurn`) — including `failed` (codex-acp) and a
    /// missing `outcome` entirely. `outcome` carries what the agent actually
    /// reported, for logs.
    ///
    /// The steer did NOT land, so the main loop must release the withheld
    /// event and fire the cancel+merge fallback — exactly like a write that
    /// never happened. Treating an unrecognized success as delivery would
    /// drop the user's message: codex-acp answers unrecognized extension
    /// methods with a bare `{}` success rather than `-32601`.
    OutcomeRejected { outcome: String },
    /// The read loop never got to dispatch the steer because the prompt
    /// completed first. Delivery state for the underlying message is
    /// unknown after prompt completion — the main loop must treat this as
    /// "release the withheld event so normal dispatch handles it" with no
    /// claims that the agent did or did not incorporate it.
    ///
    /// Returned synchronously by `send_steer` when no task is in flight
    /// for the channel. Never sent through the ack channel — the ack
    /// watcher is only spawned on `send_steer` success.
    PromptCompleted,
}

/// Outcome of a mid-turn steer, sent from the read loop back to the
/// main loop's ack watcher.
#[derive(Debug)]
pub enum SteerAck {
    /// The agent returned a successful response to the steer request.
    /// The main loop must drop the withheld event (`remove_event`) — it
    /// has been delivered via the non-cancelling path.
    Success { session_id: String },
    /// The steer was attempted but failed. Delivery state for the
    /// underlying message is unknown after prompt completion; the main
    /// loop must release the withheld event and fall back to the
    /// universal `Steer` cancel+merge path so the message still reaches
    /// the agent.
    Err(SteerError),
    /// The prompt completed before the read loop selected the steer arm.
    /// Treated as a benign no-op: release the withheld event for normal
    /// dispatch. Do not fire the fallback `Steer` signal — there is no
    /// in-flight turn to signal, and normal dispatch handles delivery.
    PromptCompletedNeutral,
}

/// Whether a turn was cut by the idle clock or the hard wall-clock cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutKind {
    /// No ACP wire activity for `idle_timeout` seconds.
    Idle,
    /// Turn ran for `max_turn_duration` seconds of wall-clock time.
    /// `recently_active` is true when the agent produced output within
    /// `RECENT_ACTIVITY_WINDOW` of the hard-cap firing.
    Hard { recently_active: bool },
}

/// Outcome of a prompt task.
#[allow(dead_code)]
pub enum PromptOutcome {
    Ok(StopReason),
    Error(AcpError),
    /// Local relay state could not establish project authority. The ACP
    /// process is healthy; preserve the batch for bounded retry.
    ProjectContextIndeterminate(String),
    AgentExited,
    Timeout(TimeoutKind),
    /// Intentional cancel via `!cancel` command or interrupt mode.
    /// Agent is healthy — no respawn, no retry penalty.
    Cancelled,
    /// The agent did not stop within `grace` after `session/cancel` was sent
    /// for a control-signal cancellation (steer fallback, interrupt, or
    /// explicit stop). Distinct from [`TimeoutKind::Hard`]: this is a bounded
    /// cleanup deadline, not the turn's configured max-turn wall clock, so it
    /// must never be reported or dead-lettered as a hard-cap breach. The
    /// agent process is uncertain — treated as poisoned and respawned, same
    /// as a hard timeout, but the triggering batch's fate follows the
    /// `CancelReason` on the batch (steer/interrupt requeue, explicit cancel
    /// drops) rather than the hard-cap's unconditional dead-letter.
    CancelDrainTimeout(Duration),
}

/// Immutable config subset shared (via `Arc`) by all spawned prompt tasks.
///
/// Built once from `Config` at startup. Avoids cloning the full config
/// into every task.
/// Shared channel-metadata resolver for startup-known and dynamically joined channels.
///
/// Successful lazy lookups are cached for fail-closed classification and as a
/// fallback during relay degradation. Prompt turns refresh metadata through
/// [`ChannelInfoResolver::resolve`] so edits reach a running harness. Unknown
/// metadata is never cached as a non-DM: callers can fail closed and a later
/// event retries resolution.
#[derive(Debug, Clone)]
struct CachedProjectInfo {
    fetched_at: std::time::Instant,
    value: Option<PromptProjectInfo>,
}

#[derive(Debug)]
pub(crate) struct ProjectLookupError(String);

const PROJECT_INFO_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct ChannelInfoResolver {
    cache: std::sync::Arc<std::sync::RwLock<std::collections::HashMap<Uuid, PromptChannelInfo>>>,
    projects: std::sync::Arc<std::sync::RwLock<std::collections::HashMap<Uuid, CachedProjectInfo>>>,
    rest_client: RestClient,
}

impl ChannelInfoResolver {
    pub fn new(
        startup: std::collections::HashMap<Uuid, ChannelInfo>,
        rest_client: RestClient,
    ) -> Self {
        let cache = startup
            .into_iter()
            .filter_map(|(id, info)| {
                (info.channel_type != "unknown").then_some((
                    id,
                    PromptChannelInfo {
                        name: info.name,
                        channel_type: info.channel_type,
                        description: info.description,
                        project: None,
                    },
                ))
            })
            .collect();
        Self {
            cache: std::sync::Arc::new(std::sync::RwLock::new(cache)),
            projects: std::sync::Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            rest_client,
        }
    }

    pub async fn resolve_channel_metadata(&self, channel_id: Uuid) -> Option<PromptChannelInfo> {
        if let Some(info) = self
            .cache
            .read()
            .ok()
            .and_then(|cache| cache.get(&channel_id).cloned())
        {
            return Some(info);
        }
        let info = fetch_channel_info(channel_id, &self.rest_client).await?;
        if let Ok(mut cache) = self.cache.write() {
            cache.insert(channel_id, info.clone());
        }
        Some(info)
    }

    /// Resolve channel context for a prompt turn.
    ///
    /// Prompt-visible metadata is refreshed on every turn rather than served
    /// indefinitely from startup discovery. Channel descriptions and names can
    /// be edited while the harness is running; the next prompt must use the
    /// relay's current kind-39000 event. On a transient refresh failure, retain
    /// the last known metadata so an otherwise healthy turn can still proceed.
    pub async fn resolve(
        &self,
        channel_id: Uuid,
    ) -> Result<Option<PromptChannelInfo>, ProjectLookupError> {
        let cached = self
            .cache
            .read()
            .ok()
            .and_then(|cache| cache.get(&channel_id).cloned());
        // A cached value makes this a refresh, not first-time discovery: use
        // one bounded attempt so relay degradation cannot add the full retry
        // window to every prompt. Unknown channels still use the retrying lazy
        // fetch below because callers must fail closed without metadata.
        let refreshed = if cached.is_some() {
            fetch_channel_info_once(channel_id, &self.rest_client).await
        } else {
            fetch_channel_info(channel_id, &self.rest_client).await
        };
        let mut info = match refreshed {
            Some(fresh) => {
                if let Ok(mut cache) = self.cache.write() {
                    cache.insert(channel_id, fresh.clone());
                }
                fresh
            }
            None => match cached {
                Some(cached) => cached,
                None => return Ok(None),
            },
        };
        info.project = self.lookup_project(channel_id).await?;
        Ok(Some(info))
    }

    async fn lookup_project(
        &self,
        channel_id: Uuid,
    ) -> Result<Option<PromptProjectInfo>, ProjectLookupError> {
        let cached = self
            .projects
            .read()
            .ok()
            .and_then(|cache| cache.get(&channel_id).cloned());
        if let Some(fresh) = cached
            .as_ref()
            .filter(|cached| cached.fetched_at.elapsed() < PROJECT_INFO_CACHE_TTL)
        {
            return Ok(fresh.value.clone());
        }
        let fetched = match fetch_project_home_for_channel(channel_id, &self.rest_client).await {
            Ok(fetched) => fetched,
            Err(error) => {
                if let Some(project) = cached.and_then(|stale| stale.value) {
                    tracing::warn!(
                        channel_id = %channel_id,
                        "project context refresh failed; retaining stale project: {}",
                        error.0
                    );
                    return Ok(Some(project));
                }
                return Err(error);
            }
        };
        if let Ok(mut cache) = self.projects.write() {
            cache.insert(
                channel_id,
                CachedProjectInfo {
                    fetched_at: std::time::Instant::now(),
                    value: fetched.clone(),
                },
            );
        }
        Ok(fetched)
    }

    /// Fetch the workspace Project directly from the relay without accepting
    /// cached or stale authority. Workspace instructions are privileged and
    /// apply beyond the Project home, so every provider turn must confirm that
    /// the selected home still resolves to the same signed Project/repository.
    async fn lookup_project_strict(
        &self,
        channel_id: Uuid,
    ) -> Result<Option<PromptProjectInfo>, ProjectLookupError> {
        let fetched = fetch_project_home_for_channel_strict(channel_id, &self.rest_client).await?;
        if let Ok(mut cache) = self.projects.write() {
            cache.insert(
                channel_id,
                CachedProjectInfo {
                    fetched_at: std::time::Instant::now(),
                    value: fetched.clone(),
                },
            );
        }
        Ok(fetched)
    }
}

pub struct PromptContext {
    pub mcp_servers: Vec<McpServer>,
    /// Creates the typed, signer-bearing MCP service after exact session scope
    /// is known. Static stdio MCP entries remain credential-free.
    pub trusted_mcp_factory: Option<TrustedMcpFactory>,
    /// Pair-scoped provider-session bindings. Absent for standalone harnesses
    /// that did not opt into durable recovery.
    pub session_recovery: Option<crate::session_recovery::SessionRecoveryStore>,
    pub initial_message: Option<String>,
    pub idle_timeout: Duration,
    pub max_turn_duration: Duration,
    /// Interval between per-turn `turn_liveness` observer pings. `Duration::ZERO`
    /// disables emission. This is the desktop crash-backstop signal — distinct
    /// from `heartbeat_prompt` (agent self-prompting).
    pub turn_liveness_interval: Duration,
    pub dedup_mode: DedupMode,
    pub reply_placement: crate::reply_placement::ReplyPlacement,
    pub system_prompt: Option<String>,
    /// Sanitized agent name used to compose `_meta.sessionTitle` on session/new.
    /// Channel sessions add the channel name; thread sessions also add the root
    /// ID prefix. Never part of the prompt.
    pub session_title: Option<String>,
    pub team_instructions: Option<String>,
    /// Owner-selected Project whose repository instructions apply throughout
    /// this Buzz workspace. This is a prompt source, never an authorization
    /// substitute for channel membership or an A2A checkout grant.
    pub workspace_project_channel: Option<Uuid>,
    /// Exact owner-selected NIP-MP Project coordinate and canonical repository.
    /// These are re-bound to every strict relay lookup before ACP delivery.
    pub workspace_project_address: Option<String>,
    pub workspace_project_repository: Option<String>,
    /// Exact reviewed commit containing the immutable workspace preload.
    pub workspace_project_revision: Option<String>,
    pub heartbeat_prompt: Option<String>,
    /// Base instructions with the configured policy's Session Model appended,
    /// assembled once and shared by modern and legacy ACP standing context.
    /// `None` when `--no-base-prompt` was passed.
    pub base_prompt: Option<String>,
    pub cwd: String,
    /// REST client for pre-prompt context fetches (thread/DM history).
    pub rest_client: RestClient,
    /// Shared channel metadata for startup-known and dynamically joined channels.
    pub channel_info: ChannelInfoResolver,
    /// Max messages to include in thread/DM context. 0 = disabled.
    pub context_message_limit: u32,
    /// Max turns per session before proactive rotation. 0 = disabled.
    pub max_turns_per_session: u32,
    /// Permission mode to apply after session creation. `Default` = skip.
    pub permission_mode: PermissionMode,
    /// Agent identity — used to derive the NIP-AE conversation key at
    /// session creation for core injection.
    pub agent_keys: nostr::Keys,
    /// Owner pubkey (hex), if resolved at startup. When unset, NIP-AE core
    /// injection is skipped entirely (no owner = no `(agent, owner)` pair).
    pub agent_owner_pubkey: Option<nostr::PublicKey>,
    /// Whether NIP-AE agent core memory injection is enabled. When false,
    /// the per-session core engram fetch is skipped and `core_sections`
    /// remains empty for every channel, so `format_prompt` renders no
    /// `<core-memory>` section. On by default; disabled via
    /// `--no-memory` / `BUZZ_ACP_NO_MEMORY`.
    pub memory_enabled: bool,
    /// Harness identity string for NIP-AM `harness` field. Derived from the
    /// configured `agent_command` at startup (e.g. `"goose"`, `"buzz-agent"`).
    pub harness_name: String,
    /// Relay URL this harness is connected to. Rides in observer payloads that
    /// the desktop keys per (agent, relay) pair, e.g. `session_config_captured`,
    /// mirroring the `managed_agent_runtime_lifecycle` frames.
    pub relay_url: String,
}

/// Select the repository instruction source for a new provider session.
///
/// With no workspace Project configured, preserve the legacy behavior: only a
/// Project home channel receives that Project's instructions. Once the owner
/// selects a workspace Project, its instructions apply in every conversation,
/// DM, ordinary channel, and heartbeat. Entering a different Project home is
/// rejected rather than silently mixing two repositories' system policy.
async fn resolve_workspace_prompt_project(
    ctx: &PromptContext,
    source: &PromptSource,
    current: Option<&PromptChannelInfo>,
) -> Result<Option<PromptProjectInfo>, ProjectLookupError> {
    let current_project = current.and_then(|info| info.project.as_ref());
    let Some(workspace_home) = ctx.workspace_project_channel else {
        return Ok(current_project.cloned());
    };

    if let PromptSource::Channel(scope) = source {
        if current_project.is_some() && scope.channel_id() != workspace_home {
            return Err(ProjectLookupError(format!(
                "channel {} belongs to a different Project than configured workspace home {}",
                scope.channel_id(),
                workspace_home
            )));
        }
        if scope.channel_id() != workspace_home {
            let (current, workspace) = tokio::join!(
                ctx.channel_info.lookup_project_strict(scope.channel_id()),
                ctx.channel_info.lookup_project_strict(workspace_home)
            );
            if current?.is_some() {
                return Err(ProjectLookupError(format!(
                    "channel {} belongs to a different Project than configured workspace home {}",
                    scope.channel_id(),
                    workspace_home
                )));
            }
            let workspace = workspace?.ok_or_else(|| {
                ProjectLookupError(format!(
                    "configured workspace home {workspace_home} has no authoritative Project"
                ))
            })?;
            validate_selected_workspace_identity(ctx, &workspace)?;
            return Ok(Some(workspace));
        }
    }

    let workspace = ctx
        .channel_info
        .lookup_project_strict(workspace_home)
        .await?
        .ok_or_else(|| {
            ProjectLookupError(format!(
                "configured workspace home {workspace_home} has no authoritative Project"
            ))
        })?;
    validate_selected_workspace_identity(ctx, &workspace)?;
    Ok(Some(workspace))
}

fn validate_selected_workspace_identity(
    ctx: &PromptContext,
    project: &PromptProjectInfo,
) -> Result<(), ProjectLookupError> {
    let address = ctx.workspace_project_address.as_deref().ok_or_else(|| {
        ProjectLookupError("configured workspace has no expected Project address".into())
    })?;
    let repository = ctx.workspace_project_repository.as_deref().ok_or_else(|| {
        ProjectLookupError("configured workspace has no expected repository".into())
    })?;
    let repository_matches = project.default_repo_clone_urls.iter().any(|candidate| {
        crate::project_preload::canonical_github_repository(candidate).as_deref()
            == Some(repository)
    });
    if project.coordinate != address || !repository_matches {
        return Err(ProjectLookupError(format!(
            "configured workspace identity changed (expected {address} at {repository}, found {})",
            project.coordinate
        )));
    }
    Ok(())
}

impl AgentPool {
    /// Create a pool from pre-indexed slots (may contain None for failed startups).
    ///
    /// Slot positions are preserved so that `agent.index` always matches the
    /// index into `self.agents`. Use this instead of `new()` when the startup
    /// loop skips failed agents — `new()` would pack agents densely and break
    /// the index invariant.
    pub fn from_slots(slots: Vec<Option<OwnedAgent>>) -> Self {
        let (result_tx, result_rx) = mpsc::unbounded_channel();
        Self {
            agents: slots,
            result_tx,
            result_rx,
            join_set: JoinSet::new(),
            task_map: HashMap::new(),
            session_owners: HashMap::new(),
        }
    }

    /// Record which worker is handling `scope` so a later dispatch can detect a
    /// busy owner and avoid opening a duplicate session on another worker.
    pub fn record_scope_owner(&mut self, scope: SessionScope, agent_index: usize) {
        self.session_owners.insert(scope, agent_index);
    }

    /// True when this scope should be **held** (left queued) rather than
    /// dispatched to a fresh worker, because the worker that owns its provider
    /// session is currently checked out (busy on another turn).
    ///
    /// Only holds when no idle worker already holds the session
    /// ([`has_session_for`](Self::has_session_for) is false): if an idle owner
    /// exists, [`try_claim`](Self::try_claim) reuses it directly. Holding waits
    /// for the busy owner to return so its exact session (and tool/turn
    /// context) is reused, instead of forking a second session for the thread.
    pub fn should_hold_for_busy_owner(&self, scope: &SessionScope) -> bool {
        if self.has_session_for(scope) {
            return false;
        }
        match self.session_owners.get(scope) {
            Some(&owner_idx) => self.task_map.values().any(|m| m.agent_index == owner_idx),
            None => false,
        }
    }

    /// Try to claim an idle agent for the given session scope (or heartbeat if
    /// `None`).
    ///
    /// Pass 1: prefer an agent that already has a session for this exact scope
    /// (thread affinity — repeated activity in a thread reuses that thread's
    /// provider session).
    /// Pass 2: any idle agent.
    ///
    /// Returns `None` if all agents are checked out.
    pub fn try_claim(&mut self, scope: Option<&SessionScope>) -> Option<OwnedAgent> {
        // Pass 1: prefer agent with existing session for this scope.
        if let Some(scope) = scope {
            let idx = self.agents.iter().position(|slot| {
                slot.as_ref()
                    .map(|a| a.state.sessions.contains_key(scope))
                    .unwrap_or(false)
            });
            if let Some(i) = idx {
                return self.agents[i].take();
            }
        }

        // Pass 2: first idle agent.
        let idx = self.agents.iter().position(|slot| slot.is_some());
        idx.map(|i| self.agents[i].take().unwrap())
    }

    /// Return an agent to its slot after a task completes.
    pub fn return_agent(&mut self, agent: OwnedAgent) {
        let idx = agent.index;
        if self.agents[idx].is_some() {
            // This is a bug: two tasks returned the same agent index. Log it
            // loudly so it shows up in production logs, then overwrite — the
            // alternative (dropping the incoming agent) would permanently leak
            // the slot.
            tracing::error!(
                idx,
                "BUG: return_agent called for slot {idx} which is already occupied — overwriting"
            );
        }
        self.agents[idx] = Some(agent);
    }

    /// Whether any agent is currently idle (sitting in its slot).
    pub fn any_idle(&self) -> bool {
        self.agents.iter().any(|slot| slot.is_some())
    }

    /// Whether any idle agent already has a session for `scope`.
    /// Used to compute `affinity_hit` before calling `try_claim`.
    pub fn has_session_for(&self, scope: &SessionScope) -> bool {
        self.agents.iter().any(|slot| {
            slot.as_ref()
                .map(|a| a.state.sessions.contains_key(scope))
                .unwrap_or(false)
        })
    }

    /// Count of agents that are alive: idle OR checked out (have a task_map entry).
    ///
    /// Used to detect when all agents have exited so the caller can respawn.
    pub fn live_count(&self) -> usize {
        let idle = self.agents.iter().filter(|s| s.is_some()).count();
        let checked_out = self.task_map.len();
        idle + checked_out
    }

    pub fn task_map(&self) -> &HashMap<tokio::task::Id, TaskMeta> {
        &self.task_map
    }

    pub fn task_map_mut(&mut self) -> &mut HashMap<tokio::task::Id, TaskMeta> {
        &mut self.task_map
    }

    /// Try to send a goose-native steer request to the in-flight task for
    /// `channel_id`.
    ///
    /// Returns `Ok(())` if the request was accepted by the read loop's
    /// receiver (capacity-1 mpsc; one slot is the single in-flight steer
    /// write). Returns `Err(SteerError::Transport(_))` on `Full`/`Closed`
    /// (already-in-flight write, or read loop torn down). Callers must
    /// fall back to the universal `ControlSignal::Steer` cancel+merge path
    /// on `Err`.
    ///
    /// This does **not** spawn the ack watcher — the caller owns the
    /// oneshot `ack_tx` inside `SteerRequest` and is responsible for
    /// awaiting it and applying the locked Success / Err / PromptCompletedNeutral
    /// semantics. Caller is also responsible for the synchronous
    /// `queue.mark_native_steer_pending(...)` *before* spawning the
    /// watcher, to close the result-vs-ack race.
    ///
    /// Returns `Err(SteerError::PromptCompleted)` if no task is in flight
    /// for `channel_id` (the prompt completed between the mode-gate check
    /// and this call, or the channel was never in flight). This is
    /// semantically a soft no-op — the caller should release any withheld
    /// event and let normal dispatch handle delivery.
    pub fn send_steer(
        &mut self,
        scope: &SessionScope,
        request: SteerRequest,
    ) -> Result<(), SteerError> {
        let meta = self
            .task_map
            .values_mut()
            .find(|m| m.scope.as_ref() == Some(scope))
            .ok_or(SteerError::PromptCompleted)?;
        let tx = meta
            .steer_tx
            .as_ref()
            .ok_or_else(|| SteerError::Transport("steer_tx not installed".into()))?;
        tx.try_send(request)
            .map_err(|e| SteerError::Transport(e.to_string()))
    }

    /// Whether a non-cancelling steer can keep using the trusted chat route
    /// already bound to the in-flight turn.
    ///
    /// A channel-scoped provider session can receive both top-level messages
    /// and explicit thread follow-ups. If those destinations differ, the
    /// current turn must take the cancel+merge path so `run_prompt_task` can
    /// bind the verified destination before the agent can call
    /// `buzz_chat_send` again.
    ///
    /// When the old turn is still active, the fallback waits for
    /// `cancel_with_cleanup_grace`, then invalidates and drops its trusted MCP
    /// capability before returning the agent to the pool. A timed-out drain
    /// also invalidates that capability, so an old tool call cannot observe
    /// the next turn's mutable route.
    pub fn native_steer_preserves_chat_destination(
        &self,
        scope: &SessionScope,
        incoming_event: &nostr::Event,
        reply_placement: crate::reply_placement::ReplyPlacement,
    ) -> bool {
        let Some(active_batch) = self
            .task_map
            .values()
            .find(|meta| meta.scope.as_ref() == Some(scope))
            .and_then(|meta| meta.recoverable_batch.as_ref())
        else {
            return false;
        };
        native_steer_preserves_chat_destination(active_batch, incoming_event, reply_placement)
    }

    /// Durably associate a successful steer with the exact ACP session that
    /// accepted it. Acks may arrive before or after the prompt result: while
    /// the task is in flight we stage the delivery in `TaskMeta`; after return
    /// we write directly to the idle agent's matching live-session ledger.
    pub fn record_successful_steer(
        &mut self,
        scope: &SessionScope,
        event_id: String,
        session_id: String,
    ) -> bool {
        if let Some(meta) = self
            .task_map
            .values_mut()
            .find(|meta| meta.scope.as_ref() == Some(scope))
        {
            meta.successful_steer_deliveries
                .insert(SuccessfulSteerDelivery {
                    event_id,
                    session_id,
                });
            return true;
        }

        let Some(agent) = self.agents.iter_mut().flatten().find(|agent| {
            agent.state.sessions.get(scope).map(String::as_str) == Some(session_id.as_str())
        }) else {
            return false;
        };
        agent
            .state
            .mark_scope_delivery_success(scope.clone(), false, [event_id]);
        true
    }

    pub fn result_tx(&self) -> mpsc::UnboundedSender<PromptResult> {
        self.result_tx.clone()
    }

    /// Split-borrow: returns mutable refs to `result_rx` and `join_set`
    /// simultaneously. This lets callers poll both in a single `select!`
    /// without a double-borrow error on `&mut AgentPool`.
    pub fn rx_and_join_set(
        &mut self,
    ) -> (&mut mpsc::UnboundedReceiver<PromptResult>, &mut JoinSet<()>) {
        (&mut self.result_rx, &mut self.join_set)
    }

    /// Non-blocking drain of the result channel. Used during shutdown to
    /// collect agents that completed while join_set was being drained.
    pub fn result_rx_try_recv(&mut self) -> Result<PromptResult, mpsc::error::TryRecvError> {
        self.result_rx.try_recv()
    }

    /// Check whether a slot is alive: either idle in the pool or checked out
    /// for an in-flight task. Returns `false` only when the slot is truly
    /// empty and available for refill.
    pub fn slot_alive(&self, index: usize) -> bool {
        let idle = self.agents.get(index).is_some_and(|s| s.is_some());
        if idle {
            return true;
        }
        // Check if the agent is checked out (in-flight on a task).
        self.task_map.values().any(|m| m.agent_index == index)
    }

    pub fn agents_mut(&mut self) -> &mut Vec<Option<OwnedAgent>> {
        &mut self.agents
    }

    /// Remove the session for `channel_id` from all idle agents.
    ///
    /// Called when the agent is removed from a channel — stale sessions
    /// should not be reused. Checked-out agents (in-flight) are not
    /// modified; their sessions will fail naturally on the next prompt
    /// if the relay rejects the request.
    ///
    /// Returns the number of sessions invalidated.
    pub fn invalidate_channel_sessions(&mut self, channel_id: Uuid) -> usize {
        let mut count = 0;
        for slot in &mut self.agents {
            if let Some(agent) = slot.as_mut() {
                // Channel-wide: clears every child thread scope for the channel.
                count += agent.state.invalidate_channel(&channel_id);
            }
        }
        // Drop every scope-owner entry for this channel so the directory does
        // not grow without bound and cannot strand a held batch behind a stale
        // owner after the channel's sessions are gone.
        self.session_owners
            .retain(|scope, _| scope.channel_id() != channel_id);
        count
    }

    /// Invalidate the session for one exact scope across every worker, and drop
    /// its scope-owner entry. The scope-precise counterpart of
    /// [`invalidate_channel_sessions`](Self::invalidate_channel_sessions): under
    /// thread policy an idle `!rotate` in thread A must rotate only thread A's
    /// session, leaving sibling threads in the same channel untouched. Under the
    /// default channel policy the scope is `Conversation(channel_id)` — the sole
    /// scope for the channel — so this matches the channel-wide behavior.
    /// Returns the number of workers that held a session for the scope.
    pub fn invalidate_scope_session(&mut self, scope: &SessionScope) -> usize {
        let mut count = 0;
        for slot in &mut self.agents {
            if let Some(agent) = slot.as_mut() {
                if agent.state.invalidate_scope(scope) {
                    count += 1;
                }
            }
        }
        self.session_owners.remove(scope);
        count
    }

    /// Whether a channel-only control could name more than one session scope.
    ///
    /// Include idle and checked-out sessions, not just active turns: selecting
    /// the first worker for an idle model switch is equally ambiguous. Stale
    /// ownership entries may conservatively reject a control until reconciled.
    pub fn channel_control_is_ambiguous(&self, channel_id: Uuid) -> bool {
        let mut scopes = self
            .session_owners
            .keys()
            .chain(
                self.agents
                    .iter()
                    .flatten()
                    .flat_map(|a| a.state.sessions.keys()),
            )
            .chain(self.task_map.values().filter_map(|m| m.scope.as_ref()))
            .filter(|scope| scope.channel_id() == channel_id);
        let Some(first) = scopes.next() else {
            return false;
        };
        scopes.any(|scope| scope != first)
    }

    /// Cancellation targets active tasks only; idle session history is not work.
    /// Count tasks rather than scopes so duplicate tasks never select arbitrarily.
    pub fn channel_cancel_is_ambiguous(&self, channel_id: Uuid) -> bool {
        self.task_map
            .values()
            .filter(|meta| meta.channel_id == Some(channel_id))
            .take(2)
            .count()
            > 1
    }

    /// Idle-path model switch: set `desired_model` on the idle agent for
    /// `channel_id` and invalidate its exact session scope so the next turn
    /// re-creates that session under the new model.
    ///
    /// Pre-cancel guard: the desired model is validated against the agent's
    /// cached catalog *before* the session is invalidated, so an unsupported
    /// pick is rejected without disturbing the existing session.
    ///
    /// Returns [`IdleSwitchResult`] describing what happened. The model does not
    /// take effect — and the panel does not reflect it — until the agent next
    /// runs a turn (no live session exists to re-emit `session_config_captured`
    /// from an idle agent). This lag is intentional: faking the emit would
    /// surface an override the session has not actually applied.
    pub fn switch_idle_agent_model(
        &mut self,
        channel_id: Uuid,
        model_id: &str,
        request_id: Option<String>,
    ) -> IdleSwitchResult {
        if self.channel_control_is_ambiguous(channel_id) {
            return IdleSwitchResult::AmbiguousTarget;
        }
        let Some((agent_index, scope)) =
            self.agents.iter().enumerate().find_map(|(index, slot)| {
                slot.as_ref().and_then(|agent| {
                    agent
                        .state
                        .sessions
                        .keys()
                        .find(|scope| scope.channel_id() == channel_id)
                        .cloned()
                        .map(|scope| (index, scope))
                })
            })
        else {
            return IdleSwitchResult::NoIdleAgent;
        };
        let Some(agent) = self.agents.get_mut(agent_index).and_then(Option::as_mut) else {
            return IdleSwitchResult::NoIdleAgent;
        };

        // Pre-cancel guard against the cached catalog. None = catalog not yet
        // populated (no session ever created); defer validation to apply time.
        if let Some(caps) = agent.model_capabilities.as_ref() {
            if !model_in_catalog(
                &caps.config_options_raw,
                caps.available_models_raw.as_ref(),
                model_id,
            ) {
                return IdleSwitchResult::UnsupportedModel;
            }
        }

        agent.desired_model = Some(model_id.to_string());
        agent.model_overridden = true;
        // Carry the pick's correlator so a deferred-validation miss on the next
        // turn's session creation emits a late frame the Desktop can match.
        agent.desired_model_request_id = request_id;
        agent.state.invalidate_scope(&scope);
        self.session_owners.remove(&scope);
        IdleSwitchResult::Switched
    }
}

fn native_steer_preserves_chat_destination(
    active_batch: &FlushBatch,
    incoming_event: &nostr::Event,
    reply_placement: crate::reply_placement::ReplyPlacement,
) -> bool {
    if active_batch.scope.is_job() {
        return false;
    }

    let Some(active_event) = active_batch.events.last() else {
        return false;
    };
    let active_root = crate::queue::parse_thread_tags(&active_event.event).root_event_id;
    let incoming_root = crate::queue::parse_thread_tags(incoming_event).root_event_id;

    match reply_placement {
        // Top-level timeline turns always share the unthreaded destination.
        // Any explicit thread may resolve either to its root (human-facing) or
        // the timeline (agent-only), based on the async profile lookup that is
        // deliberately unavailable at ingress. Cancel+merge is therefore the
        // only route-safe choice for a threaded delta.
        crate::reply_placement::ReplyPlacement::Timeline => {
            active_root.is_none() && incoming_root.is_none()
        }
        // Under thread placement even top-level human turns use their own
        // event ID, while agent-only turns may use the timeline. The ingress
        // path cannot prove equality without the prompt's profile lookup.
        crate::reply_placement::ReplyPlacement::Thread => false,
    }
}

/// Outcome of [`AgentPool::switch_idle_agent_model`].
#[derive(Debug, PartialEq, Eq)]
pub enum IdleSwitchResult {
    /// More than one session scope belongs to this channel; nothing changed.
    AmbiguousTarget,
    /// `desired_model` set and the selected session invalidated.
    Switched,
    /// Desired model is not in the agent's cached catalog — pick rejected,
    /// session untouched.
    UnsupportedModel,
    /// No idle agent available (all checked out / none spawned).
    NoIdleAgent,
}

/// Timeout for a single pre-prompt context fetch attempt (thread/DM history).
/// Each call gets this budget; with one retry the total worst-case is
/// 2 × CONTEXT_FETCH_TIMEOUT + CONTEXT_FETCH_RETRY_DELAY ≈ 6.5 s.
const CONTEXT_FETCH_TIMEOUT: Duration = Duration::from_millis(3_000);

/// Short, single-attempt timeout for best-effort exact truncated-thread counts.
const CONTEXT_COUNT_TIMEOUT: Duration = Duration::from_millis(500);

/// Delay between the first failed context fetch and the single retry.
const CONTEXT_FETCH_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Timeout for model-switch requests (`session/set_config_option`, `session/set_model`).
const MODEL_SWITCH_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounded grace window for the post-cancel drain after a control-signal
/// cancellation (steer fallback, interrupt, or explicit stop). This is a
/// cleanup deadline, not the turn's configured max-turn wall clock — see
/// [`AcpClient::cancel_with_cleanup_grace`] and
/// [`classify_control_cancel_failure`].
const CONTROL_CANCEL_GRACE: Duration = Duration::from_secs(5);

/// Timeout for permission-mode requests (`session/set_config_option` with `configId: "mode"`).
const PERMISSION_MODE_TIMEOUT: Duration = Duration::from_secs(5);

/// Placeholder [`fetch_channel_info`] substitutes when a channel's metadata
/// event carries no `name` tag. Not a real channel name — consumers that need
/// an identifying name must treat it as absent.
const UNKNOWN_CHANNEL_NAME: &str = "unknown";

/// Channel-derived inputs for a new session — `(is_dm, title_channel)` — from
/// **one** metadata resolve.
///
/// Both new-session consumers need the same lookup: the canvas block skips DMs
/// (and fails closed when the channel type can't be determined), and the
/// session title is qualified with the channel name. Resolving once is
/// load-bearing rather than tidy: [`ChannelInfoResolver`] caches only `Some`,
/// so two calls against an unresolvable channel pay the whole
/// [`fetch_channel_info`] retry sequence twice — two `CONTEXT_FETCH_TIMEOUT`
/// attempts plus `CONTEXT_FETCH_RETRY_DELAY` each, in front of `session/new`,
/// precisely when the relay is already degraded.
///
/// `title_channel` is `None` whenever the channel can't usefully identify the
/// session: an unresolved channel, a DM (no meaningful name), or the literal
/// `"unknown"` that [`fetch_channel_info`] substitutes for a metadata event
/// with no `name` tag. Composing that sentinel would title every unnamed
/// channel identically (`Agent · #unknown`) — reintroducing the collision the
/// suffix exists to remove, while naming a channel something it isn't. The
/// startup cache already refuses `channel_type == "unknown"` for the same
/// reason.
///
/// Renames do not retitle an already-live session. Prompt-turn resolution does
/// refresh channel metadata, so a later session spawn uses the current channel
/// name without requiring a harness restart. An agent rename lands on the next
/// spawn (the desktop restart badge covers it — see `spawn_config_hash`).
async fn resolve_new_session_channel_context(
    channel_info: Option<&PromptChannelInfo>,
) -> (bool, Option<String>, Option<String>) {
    let Some(info) = channel_info else {
        return (true, None, None);
    };
    let is_dm = info.channel_type == "dm";
    let title_channel = (!is_dm && info.name != UNKNOWN_CHANNEL_NAME).then(|| info.name.clone());
    (is_dm, title_channel, Some(info.channel_type.clone()))
}

/// Create a new ACP session via `session_new_full()`, populate model capabilities
/// on the agent (first session only), and apply `desired_model` if set.
///
/// On error from `session_new_full()`, returns the `AcpError` — caller handles
/// error reporting. Model-switch failures are logged and gracefully ignored
/// (the agent proceeds with its default model).
struct NewSessionChannelContext<'a> {
    huddle_instructions: Option<&'a str>,
    canvas: Option<&'a str>,
    name: Option<&'a str>,
    scope: Option<&'a SessionScope>,
    channel_type: Option<&'a str>,
}

#[cfg(test)]
async fn create_session_and_apply_model(
    agent: &mut OwnedAgent,
    ctx: &PromptContext,
    agent_core: Option<&str>,
    channel: NewSessionChannelContext<'_>,
) -> Result<String, AcpError> {
    Ok(
        resolve_provider_session_at(agent, ctx, &ctx.cwd, None, agent_core, channel)
            .await?
            .session_id,
    )
}

#[cfg(test)]
async fn create_session_and_apply_model_at(
    agent: &mut OwnedAgent,
    ctx: &PromptContext,
    working_directory: &str,
    project_instructions: Option<&str>,
    agent_core: Option<&str>,
    channel: NewSessionChannelContext<'_>,
) -> Result<String, AcpError> {
    Ok(resolve_provider_session_at(
        agent,
        ctx,
        working_directory,
        project_instructions,
        agent_core,
        channel,
    )
    .await?
    .session_id)
}

struct ProviderSessionResolution {
    session_id: String,
    resumed: bool,
}

async fn resolve_provider_session_at(
    agent: &mut OwnedAgent,
    ctx: &PromptContext,
    working_directory: &str,
    project_instructions: Option<&str>,
    agent_core: Option<&str>,
    channel: NewSessionChannelContext<'_>,
) -> Result<ProviderSessionResolution, AcpError> {
    if ctx.workspace_project_channel.is_some()
        && agent.acp.is_codex_adapter()
        && !agent.acp.developer_instructions_append_supported()
    {
        return Err(AcpError::Protocol(format!(
            "adapter '{}' does not advertise the required Workspace Project developerInstructions capability",
            agent.agent_name
        )));
    }
    // Build base_prompt + system_prompt + agent core + canvas metadata into a
    // single prompt. Standard protocol-v2 agents receive it in `session/new`;
    // Goose receives it through the custom request below. Legacy agents receive
    // the same content as user-message sections via `format_prompt`. Core carries
    // its own `<core-memory>` boundary, and canvas carries its own
    // `<channel-canvas>` boundary; both are appended with a blank-line separator.
    let is_goose = agent.agent_name == "goose";
    let ordinary_instructions = with_team(
        framed_system_prompt(
            working_directory,
            ctx.base_prompt.as_deref(),
            ctx.system_prompt.as_deref(),
        ),
        ctx.team_instructions.as_deref(),
    );
    let managed_nemo = ctx.workspace_project_address.as_deref()
        == Some(buzz_core::nemo::PROJECT_ADDRESS)
        && ctx.workspace_project_repository.as_deref() == Some(buzz_core::nemo::REPOSITORY)
        && project_instructions.is_some();
    let standing_instructions = if managed_nemo {
        with_project_instructions_first(ordinary_instructions, project_instructions)
    } else {
        with_project_instructions(ordinary_instructions, project_instructions)
    };
    let combined_system_prompt = with_canvas(
        with_huddle_instructions(
            with_core(standing_instructions, agent_core),
            channel.huddle_instructions,
        ),
        channel.canvas,
    );

    let session_title = ctx.session_title.as_deref().map(|agent_name| {
        compose_scoped_session_title(
            agent_name,
            channel.name,
            channel.scope.and_then(SessionScope::root_event_id),
        )
    });
    let job_policy = channel
        .scope
        .filter(|scope| scope.is_job())
        .map(JobSessionPolicy::for_scope)
        .transpose()?;
    if job_policy.is_some() && !agent.acp.job_policy_supported() {
        return Err(AcpError::Protocol(format!(
            "agent '{}' is chat-only: no checksum-qualified native-tool-off JobPolicyV1 executor is available",
            agent.agent_name
        )));
    }
    // A Job receives no ambient or generic stdio MCP. Its one allowed server
    // is appended below only after the signer-bound scope starts a fresh,
    // ephemeral trusted HTTP capability.
    let mut mcp_servers = mcp_servers_for_scope(
        &ctx.mcp_servers,
        channel.scope,
        channel.channel_type,
        ctx.session_title.as_deref(),
    );
    let trusted_mcp = match (
        &ctx.trusted_mcp_factory,
        channel.scope,
        agent.acp.http_mcp_supported(),
    ) {
        (Some(factory), Some(scope), true) => {
            let session = factory
                .start(scope, std::path::Path::new(working_directory))
                .await
                .map_err(AcpError::Protocol)?;
            mcp_servers.push(session.mcp_server());
            Some((scope.clone(), session))
        }
        (Some(_), Some(scope), false) => {
            return Err(AcpError::Protocol(format!(
                "adapter '{}' does not support the required HTTP MCP for scoped Buzz collaboration ({}); use a Codex or Claude adapter that advertises mcpCapabilities.http=true",
                agent.agent_name,
                scope.telemetry_label()
            )));
        }
        (None, Some(scope), _) if scope.is_job() => {
            return Err(AcpError::Protocol(
                "job sessions require the harness-owned trusted MCP factory".into(),
            ));
        }
        _ => None,
    };

    let system_prompt = session_new_system_prompt(
        is_goose,
        agent.protocol_version,
        &agent.agent_name,
        agent.acp.developer_instructions_append_supported(),
        combined_system_prompt.as_deref(),
    );
    let recovery_binding = match (ctx.session_recovery.as_ref(), channel.scope) {
        (Some(store), Some(scope)) if !scope.is_job() => store
            .binding(scope, &agent.agent_name, working_directory)
            .map_err(|error| {
                AcpError::Protocol(format!(
                    "failed to read provider-session recovery state: {error}"
                ))
            })?,
        _ => None,
    };
    if recovery_binding.is_some() && !agent.acp.session_resume_supported() {
        return Err(AcpError::Protocol(
            "persisted provider session exists but adapter does not advertise session/resume"
                .into(),
        ));
    }
    let prior_recovery_phase = recovery_binding
        .as_ref()
        .map(|binding| binding.phase.clone());
    let mut resumed = false;
    let resp = if let Some(binding) = recovery_binding {
        if matches!(
            binding.phase,
            crate::session_recovery::RecoveryPhase::TurnStarted { .. }
        ) {
            tracing::warn!(
                target: "pool::session",
                scope = %binding.scope.telemetry_label(),
                session_id = %binding.provider_session_id,
                "resuming provider context after an interrupted turn; the prior prompt will not be replayed"
            );
            agent.acp.observe(
                "session_recovery_needs_reconciliation",
                serde_json::json!({
                    "sessionId": binding.provider_session_id,
                    "scope": binding.scope.telemetry_label(),
                }),
            );
        }
        match agent
            .acp
            .session_resume_full(
                &binding.provider_session_id,
                working_directory,
                mcp_servers.clone(),
                system_prompt.clone(),
            )
            .await
        {
            Ok(response) => {
                resumed = true;
                response
            }
            Err(AcpError::AgentError { code, message }) => {
                tracing::warn!(
                    target: "pool::session",
                    scope = %binding.scope.telemetry_label(),
                    session_id = %binding.provider_session_id,
                    code,
                    "provider session could not be resumed ({message}); creating a fresh session"
                );
                agent
                    .acp
                    .session_new_full(
                        working_directory,
                        mcp_servers,
                        system_prompt,
                        session_title.as_deref(),
                    )
                    .await?
            }
            Err(error) => return Err(error),
        }
    } else if let Some(policy) = job_policy.as_ref() {
        agent
            .acp
            .session_new_job_full(
                working_directory,
                mcp_servers,
                system_prompt,
                session_title.as_deref(),
                policy,
            )
            .await?
    } else {
        agent
            .acp
            .session_new_full(
                working_directory,
                mcp_servers,
                system_prompt,
                session_title.as_deref(),
            )
            .await?
    };

    if is_goose && agent.goose_system_prompt_supported != Some(false) {
        if let Some(prompt) = combined_system_prompt.as_deref() {
            match agent
                .acp
                .session_set_goose_system_prompt(&resp.session_id, prompt)
                .await
            {
                Ok(_) => agent.goose_system_prompt_supported = Some(true),
                Err(AcpError::AgentError { code: -32601, .. }) => {
                    agent.goose_system_prompt_supported = Some(false);
                    tracing::warn!(
                        target: "pool::session",
                        "Goose does not support its system-prompt extension; using user-message framing"
                    );
                }
                Err(error) => return Err(error),
            }
        }
    }

    // Populate model capabilities on first session creation.
    if agent.model_capabilities.is_none() {
        agent.model_capabilities = Some(AgentModelCapabilities {
            config_options_raw: extract_model_config_options(&resp.raw),
            available_models_raw: extract_model_state(&resp.raw),
            thought_level_config_id: extract_thought_level_config_id(&resp.raw),
        });
    }

    // Apply desired_model if set, matching against the fresh session/new
    // response. `post_switch_snapshot` drives everything downstream:
    //   `Some(value)` → a switch applied; `value` is the adapter's post-switch
    //                   RPC response, whose `configOptions` describe the target
    //                   model. Effort resolution and the Desktop capture both
    //                   read it so they converge on the model the session is
    //                   actually running, not the pre-switch default.
    //   `None`        → no switch, or the adapter rejected/does-not-know the
    //                   model; the session/new snapshot is cached as-is and
    //                   `switch_succeeded` stays false.
    let post_switch_snapshot: Option<serde_json::Value> = if let Some(ref desired) =
        agent.desired_model
    {
        // Consume the busy-path pending-ack once for this apply: only the
        // `Applied` arm turns it into a positive terminal; the rejection and
        // unsupported arms already emit their own correlated failure frame, so
        // taking it here keeps a leftover flag from firing a spurious success
        // on some later unrelated session.
        let pending_ack = std::mem::take(&mut agent.desired_model_pending_ack);
        match resolve_model_switch_method(&resp.raw, desired) {
            Some(method) => {
                match apply_model_switch(&mut agent.acp, &resp.session_id, desired, &method).await?
                {
                    ModelSwitchOutcome::Applied(switch_result) => {
                        // The adapter rebuilds `session.configOptions` for the
                        // target model and echoes them here. Refresh capabilities
                        // from that authoritative snapshot when present so the
                        // idle-switch guard and the panel reflect the target
                        // model; drop to `None` (re-derive next session) when the
                        // adapter returned no options so a pre-switch snapshot is
                        // never mistaken for the target model's.
                        if switch_result
                            .get("configOptions")
                            .is_some_and(|v| !v.is_null())
                        {
                            agent.model_capabilities = Some(AgentModelCapabilities {
                                config_options_raw: extract_model_config_options(&switch_result),
                                available_models_raw: extract_model_state(&switch_result),
                                thought_level_config_id: extract_thought_level_config_id(
                                    &switch_result,
                                ),
                            });
                        } else {
                            agent.model_capabilities = None;
                        }
                        // Busy-path deferred switch: emit a positive terminal so
                        // the Desktop confirms success from a real frame instead
                        // of inferring it from timeout silence. Gated on the
                        // pending-ack flag so the idle path (which already acked
                        // `switched` immediately) does not double-emit.
                        if pending_ack {
                            agent.acp.observe(
                                "control_result",
                                serde_json::json!({
                                    "type": "switch_model",
                                    "status": "switched",
                                    "modelId": desired,
                                    "requestId": agent.desired_model_request_id,
                                }),
                            );
                        }
                        Some(switch_result)
                    }
                    ModelSwitchOutcome::Rejected => {
                        // The adapter explicitly rejected the switch: the session
                        // is still on its default model. Surface a terminal
                        // failure so the Desktop ModelPicker rejects the live pick
                        // instead of falsely reporting success, and preserve the
                        // pre-switch capabilities the session is really running.
                        agent.acp.observe(
                            "control_result",
                            serde_json::json!({
                                "type": "switch_model",
                                "status": "failure",
                                "modelId": desired,
                                // Echo the pick's request_id so the Desktop can
                                // correlate this late frame to the operation
                                // that fired it, and ignore replayed results.
                                "requestId": agent.desired_model_request_id,
                            }),
                        );
                        None
                    }
                }
            }
            None => {
                tracing::warn!(
                    target: "pool::model",
                    "desired model {desired} not found in agent's available models — proceeding with agent default"
                );
                // Surface the miss so the desktop ModelPicker can reject a live
                // pick rather than silently no-op. On the busy path the turn has
                // already been cancelled+requeued by the time we get here, so the
                // turn restarts on the unchanged model and the user is told no.
                agent.acp.observe(
                    "control_result",
                    serde_json::json!({
                        "type": "switch_model",
                        "status": "unsupported_model",
                        "modelId": desired,
                        // Echo the pick's request_id (see the failure arm).
                        "requestId": agent.desired_model_request_id,
                    }),
                );
                None
            }
        }
    } else {
        None
    };
    let switch_succeeded = post_switch_snapshot.is_some();

    // Apply the worker's spawn-scoped startup effort, if configured and the
    // running model advertises a `thought_level` option. Runs on every session
    // creation (config options are per-session), mirroring the model-switch
    // application above. The held value comes from `BUZZ_ACP_EFFORT_LEVEL` and
    // never mutates — there is no pool-level effort state and no live switching.
    // Reads the post-switch snapshot so the configId is discovered on the model
    // the session is actually running; computed BEFORE the capture emission so
    // the cached configOptions tell the truth about the running session.
    let effort_snapshot = post_switch_snapshot.as_ref().unwrap_or(&resp.raw);
    let effort_outcome = apply_startup_effort(agent, effort_snapshot, &resp.session_id).await?;

    // Resolve the saved autonomy preference to an adapter-native ACP mode.
    // Claude calls unrestricted execution `bypassPermissions`; Codex calls
    // the equivalent mode `agent-full-access`. Immutable JobPolicyV1 sessions
    // already enforce native-tools-off, one explicit trusted MCP, and denied
    // permission requests. Mutating their provider mode afterward would
    // conflict with that acknowledged policy (and Claude deliberately rejects
    // bypassPermissions because dangerous permission skipping is disabled).
    let applied_permission_mode = apply_configured_permission_mode(
        &mut agent.acp,
        &resp.session_id,
        &ctx.permission_mode,
        &resp.raw,
        job_policy.as_ref(),
    )
    .await?;

    // Emit session config for desktop consumption (config bridge tier 1b).
    // Emitted AFTER desired_model resolution so the desktop caches the
    // post-switch state. modelOverridden reflects whether the switch actually
    // applied — false on the rejected/unsupported arms so the panel doesn't show
    // a stale override badge.
    //
    // configOptions come from the post-switch snapshot on a successful switch
    // (the target model's option set) and the session/new snapshot otherwise.
    // Truthful capture: after a successful effort application the snapshot still
    // carries the pre-set `currentValue`, so patch the applied option to the
    // value the session is actually running. A rejected effort or a model with
    // no `thought_level` option leaves the snapshot untouched.
    let config_options_for_cache = {
        let mut opts = applied_permission_mode
            .as_ref()
            .and_then(|(_, application)| application.response.get("configOptions"))
            .or_else(|| effort_snapshot.get("configOptions"))
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        if let Some(StartupEffortOutcome::Applied { config_id, value }) = &effort_outcome {
            patch_config_option_current_value(&mut opts, config_id, value);
        }
        if let Some((wire, _)) = &applied_permission_mode {
            patch_config_option_current_value(&mut opts, "mode", wire);
        }
        opts
    };
    let modes_for_cache = {
        let mut modes = applied_permission_mode
            .as_ref()
            .and_then(|(_, application)| application.response.get("modes"))
            .or_else(|| resp.raw.get("modes"))
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        if let Some((wire, _)) = &applied_permission_mode {
            patch_session_mode_current_value(&mut modes, wire);
        }
        modes
    };
    agent.acp.observe(
        "session_config_captured",
        serde_json::json!({
            "configOptions": config_options_for_cache,
            "modes": modes_for_cache,
            // `models` must come from the SAME snapshot as configOptions — the
            // post-switch snapshot on a successful switch, session/new otherwise.
            // Taking it from `resp.raw` here would emit the target model's option
            // set alongside the pre-switch model identity, so the desktop panel
            // would report the old model as live after an applied switch. When a
            // successful target response omits `models`, this emits Null rather
            // than falling back to the pre-switch `resp.raw.models`.
            "models": effort_snapshot.get("models").cloned().unwrap_or(serde_json::Value::Null),
            "modelOverridden": agent.model_overridden && switch_succeeded,
            "permissionMode": applied_permission_mode.as_ref().map(|(wire, application)| serde_json::json!({
                "requested": wire,
                "status": if application.independently_verified {
                    "verified"
                } else {
                    "accepted_unverified"
                },
            })),
            // Pair identity for the desktop session-config cache, which is
            // keyed by (agent, relay) like the lifecycle frames.
            "relayUrl": ctx.relay_url,
        }),
    );

    if let Some((scope, session)) = trusted_mcp {
        agent.state.trusted_mcp.insert(scope, session);
    }

    if let (Some(store), Some(scope)) = (&ctx.session_recovery, channel.scope) {
        if !scope.is_job() {
            store
                .record_binding(crate::session_recovery::PersistedSessionBinding {
                    scope: scope.clone(),
                    provider: agent.agent_name.clone(),
                    provider_session_id: resp.session_id.clone(),
                    cwd: working_directory.to_owned(),
                    // Keep an interrupted boundary until the next distinct
                    // prompt durably records its own boundary. If this process
                    // dies between resume and prompt delivery, recovery still
                    // reports the original ambiguous turn.
                    phase: if resumed {
                        prior_recovery_phase
                            .clone()
                            .unwrap_or(crate::session_recovery::RecoveryPhase::Idle)
                    } else {
                        crate::session_recovery::RecoveryPhase::Idle
                    },
                })
                .map_err(|error| {
                    AcpError::Protocol(format!(
                        "failed to persist provider-session binding: {error}"
                    ))
                })?;
        }
    }

    Ok(ProviderSessionResolution {
        session_id: resp.session_id,
        resumed,
    })
}

fn mcp_servers_with_git_origin(
    servers: &[McpServer],
    scope: Option<&SessionScope>,
    channel_type: Option<&str>,
    agent_name: Option<&str>,
) -> Vec<McpServer> {
    let mut servers = servers.to_vec();
    let channel_id = scope.map(SessionScope::channel_id);
    let origin = match (channel_id, channel_type) {
        (Some(channel_id), Some("stream")) => Some(EnvVar {
            name: "BUZZ_GIT_ORIGIN_CHANNEL_ID".into(),
            value: channel_id.to_string(),
        }),
        (Some(_), _) => agent_name
            .filter(|name| !name.trim().is_empty())
            .map(|name| EnvVar {
                name: "BUZZ_GIT_ORIGIN_AGENT_NAME".into(),
                value: name.trim().to_string(),
            }),
        (None, _) => None,
    };
    if let Some(origin) = origin {
        for server in &mut servers {
            if let Some(env) = server.stdio_env_mut() {
                env.push(origin.clone());
            }
        }
    }
    servers
}

fn mcp_servers_for_scope(
    servers: &[McpServer],
    scope: Option<&SessionScope>,
    channel_type: Option<&str>,
    agent_name: Option<&str>,
) -> Vec<McpServer> {
    if scope.is_some_and(SessionScope::is_job) {
        Vec::new()
    } else {
        mcp_servers_with_git_origin(servers, scope, channel_type, agent_name)
    }
}

fn resolve_permission_mode(
    permission_mode: &PermissionMode,
    session_new: &serde_json::Value,
) -> Result<Option<&'static str>, AcpError> {
    if permission_mode.is_default() {
        return Ok(None);
    }
    permission_mode
        .provider_wire_modes()
        .iter()
        .copied()
        .find(|wire| agent_supports_mode(session_new, wire))
        .map(Some)
        .ok_or_else(|| {
            let available = advertised_agent_modes(session_new);
            AcpError::Protocol(format!(
                "configured permission mode {permission_mode} is unsupported by this agent; advertised modes: {}",
                if available.is_empty() {
                    "none".to_owned()
                } else {
                    available.join(", ")
                }
            ))
        })
}

async fn apply_configured_permission_mode(
    acp: &mut AcpClient,
    session_id: &str,
    permission_mode: &PermissionMode,
    session_new: &serde_json::Value,
    job_policy: Option<&JobSessionPolicy>,
) -> Result<Option<(&'static str, PermissionModeApplication)>, AcpError> {
    if job_policy.is_some() {
        return Ok(None);
    }
    match resolve_permission_mode(permission_mode, session_new)? {
        Some(wire) => Ok(Some((
            wire,
            apply_permission_mode(acp, session_id, wire).await?,
        ))),
        None => Ok(None),
    }
}

fn should_send_initial_message(source: &PromptSource, is_new_session: bool) -> bool {
    is_new_session && !source.scope().is_some_and(SessionScope::is_job)
}

/// Outcome of a live model-switch RPC returned by [`apply_model_switch`].
///
/// `Applied` and `Rejected` are distinct outcomes and must not be collapsed:
/// the caller needs to know whether the session is now on the target model
/// before deciding what capabilities to cache and whether to surface a failure.
#[derive(Debug)]
enum ModelSwitchOutcome {
    /// The adapter accepted the switch. Carries the RPC response value, which
    /// may include refreshed `configOptions` for the target model.
    Applied(serde_json::Value),
    /// The adapter returned an application-level error (e.g. JSON error,
    /// unrecognised model). The session is still on its default model;
    /// pre-switch capabilities must be preserved.
    Rejected,
}

/// Send the appropriate ACP model-switch request with a timeout.
///
/// Transport-class errors propagate as `Err` so the caller respawns the agent
/// rather than reuse a poisoned stdio stream. An application-level rejection is
/// non-fatal but distinct from success: it returns [`ModelSwitchOutcome::Rejected`]
/// so the caller preserves pre-switch capabilities and tells Desktop the pick
/// failed instead of silently claiming the switch landed.
async fn apply_model_switch(
    acp: &mut AcpClient,
    session_id: &str,
    desired: &str,
    method: &ModelSwitchMethod,
) -> Result<ModelSwitchOutcome, AcpError> {
    let method_label = match method {
        ModelSwitchMethod::ConfigOption { config_id, .. } => {
            format!("configOption (configId={config_id})")
        }
        ModelSwitchMethod::SetModel { .. } => "set_model".to_string(),
    };

    let result = tokio::time::timeout(MODEL_SWITCH_TIMEOUT, async {
        match method {
            ModelSwitchMethod::ConfigOption {
                config_id,
                option_value,
            } => {
                acp.session_set_config_option(session_id, config_id, option_value)
                    .await
            }
            ModelSwitchMethod::SetModel { model_id } => {
                acp.session_set_model(session_id, model_id).await
            }
        }
    })
    .await;

    match result {
        // Return the RPC result so the caller can consume the post-switch
        // capability snapshot the adapter echoes (claude-agent-acp rebuilds
        // `session.configOptions` on a model change and returns them here).
        Ok(Ok(value)) => {
            tracing::info!(
                target: "pool::model",
                "applied model {desired} via {method_label} on session {session_id}"
            );
            Ok(ModelSwitchOutcome::Applied(value))
        }
        // Transport-class errors may have corrupted the stdio stream — propagate
        // so the caller can respawn the agent instead of reusing a poisoned one.
        Ok(Err(e @ AcpError::Io(_)))
        | Ok(Err(e @ AcpError::WriteTimeout(_)))
        | Ok(Err(e @ AcpError::Timeout(_)))
        | Ok(Err(e @ AcpError::Protocol(_)))
        | Ok(Err(e @ AcpError::AgentExited)) => {
            tracing::error!(
                target: "pool::model",
                "fatal error setting model {desired} via {method_label}: {e}"
            );
            Err(e)
        }
        // Application-level errors (Json, etc.) — the adapter explicitly
        // rejected the switch; the session is still on its default model.
        // Distinct from a successful switch that returned no configOptions:
        // the caller must preserve pre-switch capabilities here.
        Ok(Err(e)) => {
            tracing::warn!(
                target: "pool::model",
                "failed to set model {desired} via {method_label}: {e} — proceeding with agent default"
            );
            Ok(ModelSwitchOutcome::Rejected)
        }
        Err(_) => {
            // Outer timeout fired — the inner send_request may have left the
            // stream in an unknown state. Treat as transport error.
            tracing::error!(
                target: "pool::model",
                "model set via {method_label} timed out ({MODEL_SWITCH_TIMEOUT:?}) — treating as fatal"
            );
            Err(AcpError::Timeout(MODEL_SWITCH_TIMEOUT))
        }
    }
}

/// Outcome of applying a worker's spawn-scoped startup effort at session creation.
///
/// Drives truthful capture: only `Applied` patches the cached `currentValue`.
/// `Rejected` (adapter refused) and the `None` return (model advertises no
/// `thought_level` option, or no effort was configured) leave the session/new
/// snapshot untouched so the panel reflects the session's real state.
enum StartupEffortOutcome {
    Applied { config_id: String, value: String },
    Rejected,
}

/// Apply the worker's held `startup_effort` via `session/set_config_option`, if
/// set and the current model advertises a `thought_level` option.
///
/// Returns `Ok(None)` when there is nothing to apply (no configured effort, or
/// the model has no `thought_level` option) or `Ok(Some(_))` describing whether
/// the adapter accepted the value. Transport-class errors propagate as `Err` so
/// the caller respawns the worker rather than reuse a poisoned stream — mirroring
/// [`apply_model_switch`]'s classification. Application-level rejection is
/// non-fatal: the session proceeds on the model's default effort.
async fn apply_startup_effort(
    agent: &mut OwnedAgent,
    session_new_result: &serde_json::Value,
    session_id: &str,
) -> Result<Option<StartupEffortOutcome>, AcpError> {
    let Some(value) = agent.startup_effort.clone() else {
        return Ok(None);
    };
    let Some(config_id) = extract_thought_level_config_id(session_new_result) else {
        tracing::info!(
            target: "pool::effort",
            "startup effort {value} configured but model advertises no thought_level option — leaving agent default"
        );
        return Ok(None);
    };

    let result = tokio::time::timeout(MODEL_SWITCH_TIMEOUT, async {
        agent
            .acp
            .session_set_config_option(session_id, &config_id, &value)
            .await
    })
    .await;

    match result {
        Ok(Ok(_)) => {
            tracing::info!(
                target: "pool::effort",
                "applied startup effort {value} via configId={config_id} on session {session_id}"
            );
            Ok(Some(StartupEffortOutcome::Applied { config_id, value }))
        }
        // Transport-class errors may have corrupted the stdio stream — propagate
        // so the caller can respawn the agent instead of reusing a poisoned one.
        Ok(Err(e @ AcpError::Io(_)))
        | Ok(Err(e @ AcpError::WriteTimeout(_)))
        | Ok(Err(e @ AcpError::Timeout(_)))
        | Ok(Err(e @ AcpError::Protocol(_)))
        | Ok(Err(e @ AcpError::AgentExited)) => {
            tracing::error!(
                target: "pool::effort",
                "fatal error applying startup effort {value} via configId={config_id}: {e}"
            );
            Err(e)
        }
        // Application-level rejection (e.g. Json) — agent is fine, uses default effort.
        Ok(Err(e)) => {
            tracing::warn!(
                target: "pool::effort",
                "adapter rejected startup effort {value} via configId={config_id}: {e} — proceeding with agent default"
            );
            Ok(Some(StartupEffortOutcome::Rejected))
        }
        Err(_) => {
            // Outer timeout fired — the inner send_request may have left the
            // stream in an unknown state. Treat as transport error.
            tracing::error!(
                target: "pool::effort",
                "startup effort {value} via configId={config_id} timed out ({MODEL_SWITCH_TIMEOUT:?}) — treating as fatal"
            );
            Err(AcpError::Timeout(MODEL_SWITCH_TIMEOUT))
        }
    }
}

/// Patch the `currentValue` of the configOption whose `configId`/`id` matches
/// `config_id` in a session/new `configOptions` array, in place.
///
/// Used by truthful capture: a successful `session/set_config_option` is not
/// reflected in the original session/new snapshot, so the accepted value is
/// written back before the snapshot is cached. A no-op when `options` is not an
/// array or no entry matches (the id came from the same array, so a match is
/// expected in practice).
fn patch_config_option_current_value(
    options: &mut serde_json::Value,
    config_id: &str,
    value: &str,
) {
    let Some(arr) = options.as_array_mut() else {
        return;
    };
    for opt in arr {
        let matches = opt
            .get("configId")
            .or_else(|| opt.get("id"))
            .and_then(|v| v.as_str())
            == Some(config_id);
        if matches {
            opt["currentValue"] = serde_json::Value::String(value.to_string());
            return;
        }
    }
}

/// Check if `session/new` advertised a provider-native mode ID.
fn agent_supports_mode(session_new_result: &serde_json::Value, mode_wire: &str) -> bool {
    session_new_result
        .get("modes")
        .and_then(|m| m.get("availableModes"))
        .and_then(|a| a.as_array())
        .map(|modes| {
            modes
                .iter()
                .any(|m| m.get("id").and_then(|v| v.as_str()) == Some(mode_wire))
        })
        .unwrap_or(false)
}

fn advertised_agent_modes(session_new_result: &serde_json::Value) -> Vec<&str> {
    session_new_result
        .get("modes")
        .and_then(|m| m.get("availableModes"))
        .and_then(|modes| modes.as_array())
        .into_iter()
        .flatten()
        .filter_map(|mode| mode.get("id").and_then(|id| id.as_str()))
        .collect()
}

#[derive(Debug)]
struct PermissionModeApplication {
    response: serde_json::Value,
    independently_verified: bool,
}

/// Reject a successful mode RPC if the adapter explicitly reports a different
/// effective mode. Some adapters return `{}` on success; that is an accepted
/// ACP operation, but is recorded separately from an independently verified
/// state echo.
fn verify_permission_mode_response(
    response: &serde_json::Value,
    requested: &str,
) -> Result<bool, AcpError> {
    let mode_from_state = response
        .pointer("/modes/currentModeId")
        .and_then(serde_json::Value::as_str);
    let mode_from_options = response
        .get("configOptions")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|option| {
            option.get("category").and_then(serde_json::Value::as_str) == Some("mode")
                || option.get("configId").and_then(serde_json::Value::as_str) == Some("mode")
                || option.get("id").and_then(serde_json::Value::as_str) == Some("mode")
        })
        .filter_map(|option| {
            option
                .get("currentValue")
                .and_then(serde_json::Value::as_str)
        });
    let reported: Vec<&str> = mode_from_state
        .into_iter()
        .chain(mode_from_options)
        .collect();
    if let Some(actual) = reported.iter().copied().find(|actual| *actual != requested) {
        return Err(AcpError::Protocol(format!(
            "adapter accepted permission mode {requested:?} but reported effective mode {actual:?}"
        )));
    }
    Ok(!reported.is_empty())
}

/// Set and require the provider-native permission mode for this session.
async fn apply_permission_mode(
    acp: &mut AcpClient,
    session_id: &str,
    wire: &str,
) -> Result<PermissionModeApplication, AcpError> {
    let result = tokio::time::timeout(PERMISSION_MODE_TIMEOUT, async {
        acp.session_set_config_option(session_id, "mode", wire)
            .await
    })
    .await;

    match result {
        Ok(Ok(response)) => {
            let independently_verified = verify_permission_mode_response(&response, wire)?;
            if independently_verified {
                tracing::info!(
                    target: "pool::permission",
                    "applied and verified permission mode {wire:?} on session {session_id}"
                );
            } else {
                tracing::info!(
                    target: "pool::permission",
                    "adapter accepted permission mode {wire:?} on session {session_id} without echoing effective state"
                );
            }
            Ok(PermissionModeApplication {
                response,
                independently_verified,
            })
        }
        // Transport-class errors may have corrupted the stdio stream — propagate
        // so the caller can respawn the agent.
        Ok(Err(e @ AcpError::Io(_)))
        | Ok(Err(e @ AcpError::WriteTimeout(_)))
        | Ok(Err(e @ AcpError::Timeout(_)))
        | Ok(Err(e @ AcpError::Protocol(_)))
        | Ok(Err(e @ AcpError::AgentExited)) => {
            tracing::error!(
                target: "pool::permission",
                "fatal error setting permission mode {wire:?}: {e}"
            );
            Err(e)
        }
        // A rejected provider-native mode would silently restore the adapter's
        // default approval/sandbox behavior, so fail session creation visibly.
        Ok(Err(e)) => {
            tracing::error!(
                target: "pool::permission",
                "failed to set permission mode {wire:?}: {e}"
            );
            Err(e)
        }
        Err(_) => {
            // Outer timeout fired — stream may be in unknown state.
            tracing::error!(
                target: "pool::permission",
                "permission mode set timed out ({PERMISSION_MODE_TIMEOUT:?}) — treating as fatal"
            );
            Err(AcpError::Timeout(PERMISSION_MODE_TIMEOUT))
        }
    }
}

fn patch_session_mode_current_value(modes: &mut serde_json::Value, value: &str) {
    if modes.is_object() {
        modes["currentModeId"] = serde_json::Value::String(value.to_owned());
    }
}

/// Prepend a legacy agent's standing context to a user-message body.
///
/// Legacy agents (`protocol_version < 2`) don't receive standing context via
/// the system role in `session/new`, so it must ride along in the user message
/// — in the session's *first* one, and never again. Agents with
/// `protocol_version >= 2`, or an empty [`StandingContext`], get `body`
/// unchanged. Both legacy dispatch paths (initial message, heartbeat) go
/// through this one gate so they can't drift apart again.
///
/// A heartbeat passes base only: it has no channel, so there is no core or
/// canvas to carry, and it has never been given the persona.
pub(crate) fn prepend_standing_for_legacy(
    protocol_version: u32,
    standing: &crate::queue::StandingContext<'_>,
    body: &str,
) -> String {
    if protocol_version >= 2 {
        return body.to_string();
    }
    let sections = standing.sections();
    if sections.is_empty() {
        return body.to_string();
    }
    format!("{}\n\n{body}", sections.join("\n\n"))
}

/// Frame the `session/new` `systemPrompt` so each present prompt carries its own
/// paired tag, keeping the base/workspace/persona boundaries recoverable downstream.
///
/// The static base remains first for prompt-prefix caching. When a base is
/// present, the dynamic workspace anchor follows it and precedes the user-owned
/// agent instructions. A persona-only agent still yields
/// `<system>…</system>` rather than an unlabeled blob that would be mistaken
/// for `<base>`.
fn framed_system_prompt(
    cwd: &str,
    base_prompt: Option<&str>,
    system_prompt: Option<&str>,
) -> Option<String> {
    match (base_prompt, system_prompt) {
        (Some(bp), Some(sp)) => Some(format!(
            "{}\n\n{}\n\n{}",
            crate::queue::base_section(bp),
            workspace_section(cwd),
            crate::prompt_framing::semantic_section("system", sp),
        )),
        (Some(bp), None) => Some(format!(
            "{}\n\n{}",
            crate::queue::base_section(bp),
            workspace_section(cwd)
        )),
        (None, Some(sp)) => Some(crate::prompt_framing::semantic_section("system", sp)),
        (None, None) => None,
    }
}

fn workspace_section(cwd: &str) -> String {
    crate::prompt_framing::semantic_section(
        "workspace",
        &format!("Current working directory: {cwd}"),
    )
}

/// Append the team-owned instruction section after `<system>` and before core memory.
fn with_team(prompt: Option<String>, instructions: Option<&str>) -> Option<String> {
    let instructions = instructions
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match (prompt, instructions) {
        (Some(prompt), Some(instructions)) => Some(format!(
            "{prompt}\n\n{}",
            crate::prompt_framing::semantic_section("team-instructions", instructions)
        )),
        (None, Some(instructions)) => Some(crate::prompt_framing::semantic_section(
            "team-instructions",
            instructions,
        )),
        (Some(prompt), None) => Some(prompt),
        (None, None) => None,
    }
}

/// Append repository-owned Project instructions after team instructions and
/// before core memory.
fn with_project_instructions(prompt: Option<String>, instructions: Option<&str>) -> Option<String> {
    let instructions = instructions
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match (prompt, instructions) {
        (Some(prompt), Some(instructions)) => Some(format!(
            "{prompt}\n\n{}",
            crate::prompt_framing::semantic_section("project-instructions", instructions)
        )),
        (None, Some(instructions)) => Some(crate::prompt_framing::semantic_section(
            "project-instructions",
            instructions,
        )),
        (Some(prompt), None) => Some(prompt),
        (None, None) => None,
    }
}

/// Put the dedicated workspace contract before every generic, agent, memory,
/// huddle, and canvas section. Its first lines contain Nemo's golden rules,
/// which must be the first substantive instructions every managed agent sees.
fn with_project_instructions_first(
    prompt: Option<String>,
    instructions: Option<&str>,
) -> Option<String> {
    let instructions = instructions
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match (instructions, prompt) {
        (Some(instructions), Some(prompt)) => Some(format!(
            "{}\n\n{prompt}",
            crate::prompt_framing::semantic_section("project-instructions", instructions)
        )),
        (Some(instructions), None) => Some(crate::prompt_framing::semantic_section(
            "project-instructions",
            instructions,
        )),
        (None, Some(prompt)) => Some(prompt),
        (None, None) => None,
    }
}

/// Legacy agents receive standing context in the first user message. Preserve
/// Project provenance in an explicit heading inside the existing team slot.
fn legacy_team_with_project(
    team_instructions: Option<&str>,
    project_instructions: Option<&str>,
) -> Option<String> {
    let team = team_instructions
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let project = project_instructions
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match (team, project) {
        (Some(team), Some(project)) => {
            Some(format!("{team}\n\n# Project Instructions\n\n{project}"))
        }
        (Some(team), None) => Some(team.to_string()),
        (None, Some(project)) => Some(format!("# Project Instructions\n\n{project}")),
        (None, None) => None,
    }
}

/// Append the agent's core memory section onto the framed system prompt.
///
/// Core already carries its own `<core-memory>` boundary from
/// `engram_fetch::build_core_section`, so it is joined with a blank-line
/// separator and never re-labeled. Either side may be absent.
fn with_core(framed: Option<String>, core: Option<&str>) -> Option<String> {
    let core = core.map(|core| {
        crate::prompt_framing::normalize_semantic_section(
            "core-memory",
            "Agent Memory — core",
            core,
        )
    });
    match (framed, core) {
        (Some(framed), Some(core)) => Some(format!("{framed}\n\n{core}")),
        (Some(framed), None) => Some(framed),
        (None, Some(core)) => Some(core),
        (None, None) => None,
    }
}

/// Append owner-signed huddle instructions to this channel session's system prompt.
fn with_huddle_instructions(prompt: Option<String>, instructions: Option<&str>) -> Option<String> {
    let instructions = instructions
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match (prompt, instructions) {
        (Some(prompt), Some(instructions)) => Some(format!(
            "{prompt}\n\n{}",
            crate::prompt_framing::semantic_section("huddle-instructions", instructions)
        )),
        (None, Some(instructions)) => Some(crate::prompt_framing::semantic_section(
            "huddle-instructions",
            instructions,
        )),
        (Some(prompt), None) => Some(prompt),
        (None, None) => None,
    }
}

/// Append the `<channel-canvas>` metadata section onto the accumulated system prompt.
///
/// The canvas section already carries its `<channel-canvas>` boundary (from
/// `render_canvas_section`), so it is joined with a blank-line separator.
/// Either side may be absent.
fn with_canvas(prompt: Option<String>, canvas: Option<&str>) -> Option<String> {
    let canvas = canvas.map(|canvas| {
        crate::prompt_framing::normalize_semantic_section(
            "channel-canvas",
            "Channel Canvas",
            canvas,
        )
    });
    match (prompt, canvas) {
        (Some(prompt), Some(canvas)) => Some(format!("{prompt}\n\n{canvas}")),
        (Some(prompt), None) => Some(prompt),
        (None, Some(canvas)) => Some(canvas),
        (None, None) => None,
    }
}

/// Return `agent` to the pool via `result_tx`, clearing any steer receiver first.
///
/// Every path that returns an `OwnedAgent` to the pool via `PromptResult` goes
/// through this function. Panic/abort paths do not — and don't need to, since a
/// panicked task's agent is never sent back via `PromptResult`.
///
/// Clearing `steer_rx` here — rather than per-arm — makes the `install_steer_rx`
/// invariant (`steer_rx.is_none()` at dispatch) structurally unviolatable: a receiver
/// installed for a turn that ends before the read loop's `take()` (e.g. session-create
/// error) is always dropped before the agent re-enters the pool, so the next dispatch
/// can never trigger the assert.
///
/// On the happy path the read loop has already called `take()`, so this is a no-op.
fn send_prompt_result(
    result_tx: &mpsc::UnboundedSender<PromptResult>,
    turn_id: &str,
    mut agent: OwnedAgent,
    source: PromptSource,
    outcome: PromptOutcome,
    batch: Option<FlushBatch>,
) {
    agent.acp.clear_steer_rx();
    let _ = result_tx.send(PromptResult {
        agent,
        source,
        turn_id: turn_id.to_owned(),
        outcome,
        batch,
    });
}

/// Core async function spawned for each prompt.
///
/// Lifecycle:
/// 1. Resolve or create a session (channel or heartbeat).
/// 2. Send `initial_message` on new channel sessions (if configured).
/// 3. Fetch conversation context if needed (thread reply or DM).
/// 4. Build the prompt text from batch + context.
/// 5. Send the actual prompt with turn timeout.
/// 6. Handle all error paths, always returning the agent via `result_tx`.
///
/// The agent is ALWAYS returned — even on panic the `JoinSet` detects the
/// abort and the caller uses `task_map` to recover the agent index.
pub struct PromptExecution {
    pub job_working_directory: Option<String>,
    pub turn_id: String,
}

impl PromptExecution {
    pub fn new(job_working_directory: Option<String>, turn_id: String) -> Self {
        Self {
            job_working_directory,
            turn_id,
        }
    }
}

pub async fn run_prompt_task(
    mut agent: OwnedAgent,
    batch: Option<FlushBatch>,
    prompt_text: Option<String>,
    ctx: Arc<PromptContext>,
    result_tx: mpsc::UnboundedSender<PromptResult>,
    control_rx: Option<tokio::sync::oneshot::Receiver<ControlSignal>>,
    execution: PromptExecution,
) {
    let PromptExecution {
        job_working_directory,
        turn_id,
    } = execution;
    // Is this a channel prompt or a heartbeat?
    let source = match &batch {
        Some(b) => PromptSource::Channel(b.scope.clone()),
        None => PromptSource::Heartbeat,
    };
    if ctx.trusted_mcp_factory.is_some() && agent.acp.http_mcp_supported() {
        if let PromptSource::Channel(scope) = &source {
            let required = ctx
                .max_turn_duration
                .saturating_add(Duration::from_secs(30));
            if agent.state.sessions.contains_key(scope)
                && !agent.state.trusted_mcp_valid_for(scope, required)
            {
                tracing::info!(
                    target: "pool::session",
                    scope = %scope.telemetry_label(),
                    "rotating provider session before trusted MCP capability expiry"
                );
                agent.state.invalidate_scope(scope);
            }
        }
    }
    let observer_channel_id = source.channel_id();
    let turn_started_at = chrono::Utc::now().to_rfc3339();
    agent.acp.set_observer_context(observer::context_for_turn(
        observer_channel_id,
        None,
        turn_id.clone(),
        turn_started_at.clone(),
    ));
    let triggering_event_ids: Vec<String> = batch
        .as_ref()
        .map(|b| b.events.iter().map(|be| be.event.id.to_hex()).collect())
        .unwrap_or_default();
    agent.acp.observe(
        "turn_started",
        serde_json::json!({
            "source": match &source {
                PromptSource::Channel(_) => "channel",
                PromptSource::Heartbeat => "heartbeat",
            },
            "triggeringEventIds": triggering_event_ids,
        }),
    );

    // Emits `turn_completed` on any exit path. Captures observer handle and
    // metadata now, before the agent is moved into PromptResult. It must be
    // declared before `liveness_guard`: Rust drops locals in reverse order, so
    // liveness is aborted before completion makes the turn terminal.
    let _turn_guard = TurnCompletionGuard::new(
        agent.acp.observer_handle(),
        agent.acp.observer_agent_index(),
        observer_channel_id,
        turn_id.clone(),
    );

    // Start liveness with `turn_started`, not the final session/prompt call:
    // session creation, context fetches, and an initial message can themselves
    // take longer than the desktop's bounded prune pause. This future is pinned
    // for the whole task and dropped with the turn on every exit path.
    //
    // `liveness_state` is shared with `LivenessGuard`: see its docs for why a
    // bare `abort()` alone cannot prevent a `turn_liveness` frame emitted after
    // `turn_completed`. Once the session resolves below, `set_session_id`
    // updates the same shared state so later ticks stop carrying `None`.
    let liveness_state = Arc::new(Mutex::new(LivenessState {
        closed: false,
        session_id: None,
    }));
    let liveness = run_turn_liveness(
        agent.acp.observer_handle(),
        agent.acp.observer_agent_index(),
        observer::context_for_turn(
            observer_channel_id,
            None,
            turn_id.clone(),
            turn_started_at.clone(),
        ),
        ctx.turn_liveness_interval,
        Arc::clone(&liveness_state),
    );
    let liveness_handle = tokio::spawn(liveness);
    let liveness_guard = LivenessGuard::new(liveness_handle, liveness_state);

    // Collects event IDs up front. On drop (any exit path — normal, early
    // return, or panic), spawns best-effort cleanup of both 👀 and 💬.
    // See `ReactionGuard` docs for ordering guarantees and known edge cases.
    let reaction_ids: Vec<String> = batch
        .as_ref()
        .map(|b| b.events.iter().map(|be| be.event.id.to_hex()).collect())
        .unwrap_or_default();
    let _reaction_guard = ReactionGuard::new(ctx.rest_client.clone(), reaction_ids.clone());

    // Resolve project authority exactly once, before any ACP session creation or
    // initial-message delivery. An indeterminate result is a local relay-state
    // outcome: fail closed and preserve the batch without poisoning the healthy
    // ACP process.
    let resolved_channel_info = match &source {
        PromptSource::Channel(scope) => match ctx.channel_info.resolve(scope.channel_id()).await {
            Ok(info) => info,
            Err(error) => {
                tracing::warn!(
                    channel_id = %scope.channel_id(),
                    "project context is indeterminate; requeueing turn before ACP session creation: {}",
                    error.0
                );
                send_prompt_result(
                    &result_tx,
                    &turn_id,
                    agent,
                    source,
                    PromptOutcome::ProjectContextIndeterminate(error.0),
                    requeue_batch_if_queue(&ctx, batch),
                );
                return;
            }
        },
        PromptSource::Heartbeat => None,
    };

    // Resolve Project scope on every turn, including turns in an existing ACP
    // session. A channel can be rebound to a different Project after its
    // session was created; continuing that old Nemo-scoped session would mix
    // two repositories' policy. Blob loading remains new-session-only below.
    // This is policy context only: membership, job admission, and checkout
    // grants remain independently enforced.
    let workspace_prompt_project = match resolve_workspace_prompt_project(
        &ctx,
        &source,
        resolved_channel_info.as_ref(),
    )
    .await
    {
        Ok(project) => project,
        Err(error) => {
            tracing::warn!(
                source = %prompt_label(&source),
                "workspace project context is indeterminate; requeueing before ACP delivery: {}",
                error.0
            );
            send_prompt_result(
                &result_tx,
                &turn_id,
                agent,
                source,
                PromptOutcome::ProjectContextIndeterminate(error.0),
                requeue_batch_if_queue(&ctx, batch),
            );
            return;
        }
    };

    // Bind each provider session to the exact workspace Project metadata and
    // reviewed revision that supplied its developer instructions.  Project
    // events are replaceable; continuing a session after a home is rebound
    // would retain the old privileged policy in a newly authoritative context.
    let workspace_instruction_binding = match ctx.workspace_project_channel {
        Some(home_channel) => {
            let Some(project) = workspace_prompt_project.as_ref() else {
                send_prompt_result(
                    &result_tx,
                    &turn_id,
                    agent,
                    source,
                    PromptOutcome::ProjectContextIndeterminate(
                        "workspace Project resolved without authoritative metadata".into(),
                    ),
                    requeue_batch_if_queue(&ctx, batch),
                );
                return;
            };
            let revision = ctx
                .workspace_project_revision
                .as_deref()
                .map(str::trim)
                .filter(|revision| !revision.is_empty())
                .map(str::to_owned)
                .or_else(|| {
                    let repository_matches = project
                        .default_repo_clone_urls
                        .iter()
                        .filter_map(|value| {
                            crate::project_preload::canonical_github_repository(value)
                        })
                        .any(|value| value == buzz_core::nemo::REPOSITORY);
                    (project.coordinate == buzz_core::nemo::PROJECT_ADDRESS
                        && home_channel.to_string() == buzz_core::nemo::HOME_CHANNEL
                        && repository_matches)
                        .then(|| buzz_core::nemo::INSTRUCTION_SOURCE.to_owned())
                });
            let Some(revision) = revision else {
                send_prompt_result(
                    &result_tx,
                    &turn_id,
                    agent,
                    source,
                    PromptOutcome::ProjectContextIndeterminate(
                        "workspace Project has no reviewed instruction revision".into(),
                    ),
                    requeue_batch_if_queue(&ctx, batch),
                );
                return;
            };
            Some(WorkspaceInstructionBinding {
                home_channel,
                project: project.clone(),
                revision,
            })
        }
        None => None,
    };

    // Select repository instructions only when creating a provider session.
    // Without a configured workspace Project this preserves the original
    // Project-home-only behavior. With one configured, the same verified
    // checkout and complete instruction package apply to every session,
    // including DMs and heartbeats.
    let mut project_preload = None;
    let creating_session = match &source {
        PromptSource::Channel(scope) => !agent.state.sessions.contains_key(scope),
        PromptSource::Heartbeat => agent.state.heartbeat_session.is_none(),
    };
    if !creating_session {
        let session_binding = match &source {
            PromptSource::Channel(scope) => agent.state.workspace_instruction_bindings.get(scope),
            PromptSource::Heartbeat => agent.state.heartbeat_workspace_instruction_binding.as_ref(),
        };
        if session_binding != workspace_instruction_binding.as_ref() {
            tracing::warn!(
                source = %prompt_label(&source),
                "workspace Project binding changed; refusing to reuse provider session"
            );
            send_prompt_result(
                &result_tx,
                &turn_id,
                agent,
                source,
                PromptOutcome::ProjectContextIndeterminate(
                    "workspace Project binding changed; restart or recreate the provider session"
                        .into(),
                ),
                requeue_batch_if_queue(&ctx, batch),
            );
            return;
        }
    }
    if creating_session {
        if let Some(project) = workspace_prompt_project.as_ref() {
            let preferred_checkout = match &source {
                PromptSource::Channel(scope) if scope.is_job() => {
                    job_working_directory.as_deref().map(Path::new)
                }
                _ => None,
            };
            let instruction_revision = if ctx.workspace_project_channel.is_some() {
                match ctx.workspace_project_revision.as_deref() {
                    Some(revision) => Some(revision),
                    None if project.coordinate == buzz_core::nemo::PROJECT_ADDRESS => None,
                    None => {
                        send_prompt_result(
                            &result_tx,
                            &turn_id,
                            agent,
                            source,
                            PromptOutcome::ProjectContextIndeterminate(
                                "workspace Project has no reviewed instruction revision".into(),
                            ),
                            requeue_batch_if_queue(&ctx, batch),
                        );
                        return;
                    }
                }
            } else {
                None
            };
            match crate::project_preload::resolve_async(
                PathBuf::from(&ctx.cwd),
                project.clone(),
                preferred_checkout.map(Path::to_path_buf),
                instruction_revision.map(str::to_string),
            )
            .await
            {
                Ok(preload) => project_preload = preload,
                Err(reason) => {
                    tracing::warn!(
                        source = %prompt_label(&source),
                        "project instruction preload failed closed: {reason}"
                    );
                    send_prompt_result(
                        &result_tx,
                        &turn_id,
                        agent,
                        source,
                        PromptOutcome::ProjectContextIndeterminate(format!(
                            "project instruction preload failed: {reason}"
                        )),
                        requeue_batch_if_queue(&ctx, batch),
                    );
                    return;
                }
            }
        }
    }
    let default_session_working_directory = match (&source, job_working_directory.as_deref()) {
        (PromptSource::Channel(scope), Some(path)) if scope.is_job() => path,
        _ => &ctx.cwd,
    };
    let project_working_directory = project_preload
        .as_ref()
        .map(|preload| preload.working_directory.to_string_lossy().into_owned());
    let session_working_directory = project_working_directory
        .as_deref()
        .unwrap_or(default_session_working_directory);
    let project_instructions = project_preload
        .as_ref()
        .and_then(|preload| preload.instructions.as_deref());
    let managed_nemo_instructions = (ctx.workspace_project_address.as_deref()
        == Some(buzz_core::nemo::PROJECT_ADDRESS)
        && ctx.workspace_project_repository.as_deref() == Some(buzz_core::nemo::REPOSITORY))
    .then_some(project_instructions)
    .flatten();
    let legacy_team_instructions = if managed_nemo_instructions.is_some() {
        ctx.team_instructions.clone()
    } else {
        legacy_team_with_project(ctx.team_instructions.as_deref(), project_instructions)
    };

    //
    // Core memory is delivered inside the system prompt the harness already
    // builds (system role for protocol >= 2, the `<system>` user-message
    // section for legacy agents). To put it on the wire at `session/new` for
    // modern agents, the fetch must run *before* the session is created — so
    // we do it here and cache the rendered section in `state.core_sections`.
    //
    // Core is keyed by (agent_keys, owner) — both fixed for the process — so
    // it is identical across channels; the per-channel cache just avoids a
    // re-fetch on each new session and is cleared on session invalidation.
    //
    // Failure modes (all fail open — no crash, no block):
    //   * no owner configured → skip (no NIP-AE namespace exists)
    //   * confirmed absence → cache the onboarding nudge so the agent
    //     learns how to bootstrap itself.
    //   * transport / decrypt / parse error → inject nothing. We never
    //     mistake "relay slow or broken" for "no core" — that would invite
    //     the agent to overwrite real, just-unreachable memory.
    //   * fetch exceeds CORE_FETCH_TIMEOUT → inject nothing, same reason.
    //
    // Per Tyler's locked spec: NO mid-session refreshes. Re-fetch only
    // happens when a session is invalidated and recreated (see
    // `SessionState::invalidate_channel`).
    //
    // Operator opt-out: `--no-memory` / `BUZZ_ACP_NO_MEMORY` skips the fetch.
    if ctx.memory_enabled {
        if let (PromptSource::Channel(scope), Some(owner_pk)) =
            (&source, ctx.agent_owner_pubkey.as_ref())
        {
            // Session state is keyed by scope: repeated activity in a thread
            // reuses exactly that thread's session. `cid` is only for
            // channel-level fetches/logging.
            let cid = &scope.channel_id();
            let is_new_channel_session = !agent.state.sessions.contains_key(scope);
            if is_new_channel_session && !agent.state.core_sections.contains_key(scope) {
                // Bounded — we'd rather start the session with no core hint
                // than block session creation on a stalled relay.
                const CORE_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
                let fetch = crate::engram_fetch::build_core_section(
                    &ctx.rest_client,
                    &ctx.agent_keys,
                    owner_pk,
                );
                let section = match tokio::time::timeout(CORE_FETCH_TIMEOUT, fetch).await {
                    Ok(s) => s,
                    Err(_) => {
                        tracing::warn!(
                            target: "engram::core",
                            channel = %cid,
                            timeout_ms = CORE_FETCH_TIMEOUT.as_millis() as u64,
                            "core fetch timed out — emitting no section"
                        );
                        None
                    }
                };
                if let Some(rendered) = section {
                    tracing::info!(
                        target: "engram::core",
                        channel = %cid,
                        scope = %scope.telemetry_label(),
                        section_len = rendered.len(),
                        "injected NIP-AE core section into system prompt"
                    );
                    agent.state.core_sections.insert(scope.clone(), rendered);
                }
            }
        }
    }

    // Canvas metadata fetch — same lifecycle as core: once per new channel session,
    // never for heartbeats, cached until session invalidation.
    //
    // DM check: use startup channel_info first; lazy-fetch only when missing.
    // A confirmed DM never receives a canvas section. If the channel type cannot
    // be determined (metadata absent and lazy fetch fails/unknown), skip the canvas
    // rather than assuming non-DM — failing closed on DM ambiguity is safer.
    //
    // I3 lifecycle: hold the fetched section in a local `pending_canvas` and
    // commit it to `canvas_sections` only after session creation succeeds. This
    // prevents a stale revision A surviving a failed create and being re-used by
    // the next attempt after the canvas was cleared.
    let mut pending_canvas: Option<(SessionScope, String)> = None;
    let mut huddle_instructions: Option<String> = None;
    // Channel name for the session title, from the same single resolve the
    // canvas DM check uses — see `resolve_new_session_channel_context`.
    let mut title_channel: Option<String> = None;
    let mut origin_channel_type: Option<String> = None;
    if let PromptSource::Channel(scope) = &source {
        let cid = scope.channel_id();
        let is_new_channel_session = !agent.state.sessions.contains_key(scope);
        let needs_canvas =
            is_new_channel_session && !agent.state.canvas_sections.contains_key(scope);
        if is_new_channel_session {
            let (is_dm, resolved_channel, resolved_channel_type) =
                resolve_new_session_channel_context(resolved_channel_info.as_ref()).await;
            title_channel = resolved_channel;
            origin_channel_type = resolved_channel_type;
            if let Some(owner) = ctx.agent_owner_pubkey.as_ref() {
                huddle_instructions = fetch_huddle_instructions(cid, owner, &ctx.rest_client).await;
            }
            // A confirmed DM never receives a canvas section; an undeterminable
            // channel type fails closed as a DM for the same reason.
            if needs_canvas && !is_dm {
                if let Some(section) = fetch_canvas_section(cid, &ctx.rest_client).await {
                    pending_canvas = Some((scope.clone(), section));
                }
            }
        }
    }

    // The core section to fold into the system prompt for this turn's session.
    // Channel-scoped; heartbeats carry no owner core.
    let agent_core: Option<String> = match &source {
        PromptSource::Channel(scope) => agent.state.core_sections.get(scope).cloned(),
        PromptSource::Heartbeat => None,
    };

    // The canvas metadata section — channel-scoped, absent for heartbeats/DMs.
    // Prefer the committed cache; fall back to pending (for new sessions being created now).
    let agent_canvas: Option<String> = match &source {
        PromptSource::Channel(scope) => agent
            .state
            .canvas_sections
            .get(scope)
            .cloned()
            .or_else(|| pending_canvas.as_ref().map(|(_, s)| s.clone())),
        PromptSource::Heartbeat => None,
    };

    let (session_id, is_new_session) = match &source {
        PromptSource::Channel(scope) => {
            let cid = &scope.channel_id();
            if let Some(sid) = agent.state.sessions.get(scope) {
                (sid.clone(), false)
            } else {
                // The title includes channel and, for thread sessions, the
                // canonical root prefix so sibling sessions are distinguishable.
                // DMs, unresolved, and unnamed channels omit the channel name.
                match resolve_provider_session_at(
                    &mut agent,
                    &ctx,
                    session_working_directory,
                    project_instructions,
                    agent_core.as_deref(),
                    NewSessionChannelContext {
                        huddle_instructions: huddle_instructions.as_deref(),
                        canvas: agent_canvas.as_deref(),
                        name: title_channel.as_deref(),
                        scope: Some(scope),
                        channel_type: origin_channel_type.as_deref(),
                    },
                )
                .await
                {
                    Ok(resolution) => {
                        let sid = resolution.session_id;
                        tracing::info!(
                            target: "pool::session",
                            resumed = resolution.resumed,
                            "resolved session {sid} for channel {cid} (scope {})",
                            scope.telemetry_label()
                        );
                        agent.state.sessions.insert(scope.clone(), sid.clone());
                        if let Some(binding) = workspace_instruction_binding.clone() {
                            agent
                                .state
                                .workspace_instruction_bindings
                                .insert(scope.clone(), binding);
                        }
                        agent
                            .state
                            .deliveries
                            .insert(scope.clone(), ChannelDeliveryState::default());
                        if !resolution.resumed {
                            // New provider sessions start with a zero usage
                            // baseline. Resumed sessions retain provider history.
                            agent.acp.notify_session_spawned(&sid);
                        }
                        // Commit canvas only after session creation succeeds (I3).
                        if let Some((pending_scope, section)) = pending_canvas.take() {
                            agent.state.canvas_sections.insert(pending_scope, section);
                        }
                        (sid, !resolution.resumed)
                    }
                    Err(AcpError::AgentExited) => {
                        agent.state.invalidate_all();
                        send_prompt_result(
                            &result_tx,
                            &turn_id,
                            agent,
                            source,
                            PromptOutcome::AgentExited,
                            requeue_batch_if_queue(&ctx, batch),
                        );
                        return;
                    }
                    Err(e) => {
                        // Session creation failed; pending canvas was never committed,
                        // so the next retry will re-fetch a fresh revision.
                        send_prompt_result(
                            &result_tx,
                            &turn_id,
                            agent,
                            source,
                            PromptOutcome::Error(e),
                            requeue_batch_if_queue(&ctx, batch),
                        );
                        return;
                    }
                }
            }
        }
        PromptSource::Heartbeat => {
            if let Some(sid) = &agent.state.heartbeat_session {
                (sid.clone(), false)
            } else {
                match resolve_provider_session_at(
                    &mut agent,
                    &ctx,
                    session_working_directory,
                    project_instructions,
                    None,
                    NewSessionChannelContext {
                        huddle_instructions: None,
                        canvas: None,
                        name: None,
                        scope: None,
                        channel_type: None,
                    },
                )
                .await
                {
                    Ok(resolution) => {
                        let sid = resolution.session_id;
                        tracing::info!(
                            target: "pool::session",
                            "created heartbeat session {sid} for agent {}",
                            agent.index
                        );
                        agent.state.heartbeat_session = Some(sid.clone());
                        agent.state.heartbeat_workspace_instruction_binding =
                            workspace_instruction_binding.clone();
                        // Seed a zero usage baseline: buzz-acp spawned this session.
                        agent.acp.notify_session_spawned(&sid);
                        (sid, true)
                    }
                    Err(AcpError::AgentExited) => {
                        agent.state.invalidate_all();
                        send_prompt_result(
                            &result_tx,
                            &turn_id,
                            agent,
                            source,
                            PromptOutcome::AgentExited,
                            None,
                        );
                        return;
                    }
                    Err(e) => {
                        send_prompt_result(
                            &result_tx,
                            &turn_id,
                            agent,
                            source,
                            PromptOutcome::Error(e),
                            None,
                        );
                        return;
                    }
                }
            }
        }
    };
    agent.acp.set_observer_context(observer::context_for_turn(
        observer_channel_id,
        Some(session_id.clone()),
        turn_id.clone(),
        turn_started_at.clone(),
    ));
    // Backfill liveness's shared session ID so ticks after this point carry
    // it too, matching every other observer frame for this turn.
    liveness_guard.set_session_id(session_id.clone());
    agent.acp.observe(
        "session_resolved",
        serde_json::json!({
            "sessionId": session_id,
            "isNewSession": is_new_session,
        }),
    );

    // Standing context is fixed for the life of a session. Agents with
    // systemPrompt support already hold it from session/new; legacy agents
    // receive it in the session's first user message and never again.
    //
    // `is_new_session` comes from the session registry, which is cleared
    // whenever a session is invalidated — so the replacement session re-delivers
    // rather than leaving the agent unbriefed.
    let standing = crate::queue::StandingContext {
        leading_project_instructions: managed_nemo_instructions,
        base_prompt: ctx.base_prompt.as_deref(),
        system_prompt: ctx.system_prompt.as_deref(),
        team_instructions: legacy_team_instructions.as_deref(),
        agent_core: agent_core.as_deref(),
        huddle_instructions: huddle_instructions.as_deref(),
        agent_canvas: agent_canvas.as_deref(),
    };
    // Delivery state is committed only after ACP confirms success. Existing
    // sessions created before this field existed fail safe by behaving as
    // undelivered once, rather than silently omitting standing context.
    let mut standing_context_sent = match &source {
        PromptSource::Channel(scope) => agent
            .state
            .deliveries
            .get(scope)
            .is_some_and(|delivery| delivery.standing_context_sent),
        PromptSource::Heartbeat => agent.state.heartbeat_standing_context_sent,
    };

    if should_send_initial_message(&source, is_new_session) {
        if let (PromptSource::Channel(scope), Some(ref initial_msg)) =
            (&source, &ctx.initial_message)
        {
            let cid = &scope.channel_id();
            tracing::info!(
                target: "pool::session",
                "sending initial_message to session {session_id} for channel {cid}"
            );
            let init_msg = prepend_standing_for_legacy(
                if agent.has_system_prompt_support() {
                    2
                } else {
                    1
                },
                &standing,
                initial_msg,
            );
            let init_result = agent
                .acp
                .session_prompt_with_idle_timeout(
                    &session_id,
                    &init_msg,
                    ctx.idle_timeout,
                    ctx.max_turn_duration,
                )
                .await;

            match init_result {
                Ok(stop_reason) => {
                    tracing::info!(
                        target: "pool::session",
                        "initial_message complete for channel {cid}: {stop_reason:?}"
                    );
                    // The legacy agent has its standing context now; the turn
                    // prompt below must not repeat it. Every other arm returns.
                    standing_context_sent = true;
                    if !agent.has_system_prompt_support() {
                        agent
                            .state
                            .mark_scope_delivery_success(scope.clone(), true, []);
                    }
                    let usage = agent.acp.take_turn_usage();
                    publish_agent_turn_metric(
                        &ctx,
                        usage,
                        Some(*cid),
                        &session_id,
                        &format!("{turn_id}:initial"),
                        Some(acp_stop_to_core(&stop_reason)),
                    )
                    .await;
                }
                Err(AcpError::AgentExited) => {
                    agent.state.invalidate_all();
                    send_prompt_result(
                        &result_tx,
                        &turn_id,
                        agent,
                        source,
                        PromptOutcome::AgentExited,
                        requeue_pre_prompt_batch(&ctx, batch),
                    );
                    return;
                }
                Err(AcpError::IdleTimeout(_)) => {
                    tracing::warn!(
                        target: "pool::session",
                        "initial_message idle timeout ({}s) for channel {cid} — cancelling",
                        ctx.idle_timeout.as_secs()
                    );
                    match agent
                        .acp
                        .cancel_with_cleanup(&session_id, ctx.idle_timeout)
                        .await
                    {
                        Ok(stop_reason) => {
                            let usage = agent.acp.take_turn_usage();
                            publish_agent_turn_metric(
                                &ctx,
                                usage,
                                Some(*cid),
                                &session_id,
                                &format!("{turn_id}:initial"),
                                Some(acp_stop_to_core(&stop_reason)),
                            )
                            .await;
                            agent.state.invalidate(&source);
                        }
                        Err(AcpError::AgentExited) => {
                            agent.state.invalidate_all();
                            send_prompt_result(
                                &result_tx,
                                &turn_id,
                                agent,
                                source,
                                PromptOutcome::AgentExited,
                                requeue_batch_if_queue(&ctx, batch),
                            );
                            return;
                        }
                        Err(e) => {
                            tracing::error!(
                                target: "pool::session",
                                "cancel_with_cleanup failed during initial_message timeout: {e}"
                            );
                            agent.state.invalidate(&source);
                        }
                    }
                    send_prompt_result(
                        &result_tx,
                        &turn_id,
                        agent,
                        source,
                        PromptOutcome::Timeout(TimeoutKind::Idle),
                        requeue_pre_prompt_batch(&ctx, batch),
                    );
                    return;
                }
                Err(AcpError::HardTimeout { silence }) => {
                    let recently_active = silence < RECENT_ACTIVITY_WINDOW;
                    tracing::error!(
                        target: "pool::session",
                        "hard timeout ({}s cap, silence {silence:?}, recently_active={recently_active}) during initial_message for channel {cid} — agent process is unrecoverable",
                        ctx.max_turn_duration.as_secs()
                    );
                    agent.state.invalidate_all();
                    send_prompt_result(
                        &result_tx,
                        &turn_id,
                        agent,
                        source,
                        PromptOutcome::Timeout(TimeoutKind::Hard { recently_active }),
                        requeue_batch_if_queue(&ctx, batch),
                    );
                    return;
                }
                Err(e) => {
                    tracing::error!(
                        target: "pool::session",
                        "initial_message failed for channel {cid}: {e} — invalidating session"
                    );
                    agent.state.invalidate(&source);
                    send_prompt_result(
                        &result_tx,
                        &turn_id,
                        agent,
                        source,
                        PromptOutcome::Error(e),
                        requeue_batch_if_queue(&ctx, batch),
                    );
                    return;
                }
            }
        }
    }

    // When the batch is a single slash-command message (e.g. "@Eva /goal …"),
    // `slash_command` holds the bare command. It is sent as the FIRST prompt
    // content block so ACP connectors' slash-command detection
    // (`prompt[0].text.startsWith("/")`) fires; the wrapped Buzz context
    // follows as a second block.
    let mut slash_command: Option<String> = None;
    // Event IDs represented by this prompt. Commit only after ACP reports a
    // successful turn; failed/cancelled prompts must be retryable without loss.
    let mut pending_delivered_event_ids = HashSet::new();
    let prompt_sections: Vec<String> = if let Some(text) = prompt_text {
        // Heartbeats create their session before this point, so a Goose method-not-found
        // probe has already selected the correct framing for this process.
        //
        // Only the first heartbeat of a legacy session carries its standing
        // context; later ticks reuse the same session. Modern adapters already
        // received the same context as a system prompt at session/new.
        let text = if standing_context_sent {
            text
        } else {
            prepend_standing_for_legacy(
                if agent.has_system_prompt_support() {
                    2
                } else {
                    1
                },
                &standing,
                &text,
            )
        };
        vec![text]
    } else if let Some(ref b) = batch {
        // Project authority was resolved before any ACP session boundary above;
        // reuse that exact typed result for prompt formatting.
        let channel_info = resolved_channel_info.clone();

        let conversation_context = if ctx.context_message_limit > 0 {
            fetch_conversation_context(b, &channel_info, &ctx).await
        } else {
            None
        };
        let rendered_batch_ids: HashSet<String> = b
            .events
            .iter()
            .chain(b.cancelled_events.iter())
            .map(|event| event.event.id.to_hex())
            .collect();
        let delivered_ids = agent
            .state
            .deliveries
            .get(&b.scope)
            .map(|delivery| &delivery.delivered_event_ids)
            .cloned()
            .unwrap_or_default();
        let conversation_context_had_delivered_events =
            conversation_context.as_ref().is_some_and(|context| {
                conversation_context_event_ids(Some(context))
                    .iter()
                    .any(|event_id| delivered_ids.contains(event_id))
            });
        let conversation_context =
            conversation_context_delta(conversation_context, &delivered_ids, &rendered_batch_ids);
        pending_delivered_event_ids.extend(rendered_batch_ids);
        pending_delivered_event_ids.extend(conversation_context_event_ids(
            conversation_context.as_ref(),
        ));

        let profile_lookup =
            fetch_prompt_profile_lookup(b, conversation_context.as_ref(), &ctx.rest_client).await;

        let known_names: Vec<&str> = profile_lookup
            .iter()
            .flat_map(|lookup| lookup.values())
            .flat_map(|p| [p.display_name.as_deref(), p.nip05_handle.as_deref()])
            .flatten()
            .collect();
        slash_command = crate::queue::slash_command_for_batch(b, &known_names);
        if let Some(ref cmd) = slash_command {
            tracing::info!(
                target: "pool::prompt",
                channel = %b.channel_id,
                command = %cmd,
                "slash-command pass-through"
            );
        }

        let format_args = crate::queue::FormatPromptArgs {
            leading_project_instructions: standing.leading_project_instructions,
            agent_core: standing.agent_core,
            huddle_instructions: standing.huddle_instructions,
            channel_info: channel_info.as_ref(),
            conversation_context: conversation_context.as_ref(),
            conversation_context_had_delivered_events,
            profile_lookup: profile_lookup.as_ref(),
            reply_placement: ctx.reply_placement,
            has_system_prompt_support: agent.has_system_prompt_support(),
            base_prompt: standing.base_prompt,
            system_prompt: standing.system_prompt,
            team_instructions: standing.team_instructions,
            agent_canvas: standing.agent_canvas,
            standing_context_sent,
        };
        let chat_thread_root = crate::queue::trusted_chat_thread_root(b, &format_args);
        if let Some(session) = agent.state.trusted_mcp.get(&b.scope) {
            if let Err(error) = session.set_chat_thread_root_id(chat_thread_root.as_deref()) {
                tracing::error!(
                    target: "pool::session",
                    scope = %b.scope.telemetry_label(),
                    "failed to bind trusted chat destination: {error}"
                );
                send_prompt_result(
                    &result_tx,
                    &turn_id,
                    agent,
                    source,
                    PromptOutcome::Error(AcpError::Protocol(format!(
                        "failed to bind trusted chat destination: {error}"
                    ))),
                    requeue_batch_if_queue(&ctx, batch.clone()),
                );
                return;
            }
        }
        crate::queue::format_prompt(b, &format_args)
    } else {
        // Should not happen — batch is None only for heartbeats which have prompt_text.
        // Return the agent to the pool to prevent a permanent slot leak.
        tracing::error!("run_prompt_task: no batch and no prompt_text — returning agent");
        send_prompt_result(
            &result_tx,
            &turn_id,
            agent,
            source,
            PromptOutcome::Error(AcpError::Protocol("no batch and no prompt_text".into())),
            None,
        );
        return;
    };

    // 💬 — fire-and-forget so the prompt fires immediately.
    // The guard's cleanup (spawned on drop) removes 💬 after the turn completes.
    // A brief race where 💬 appears slightly after the agent starts is acceptable.
    if !reaction_ids.is_empty() {
        let rest = ctx.rest_client.clone();
        let ids = reaction_ids.clone();
        tokio::spawn(async move {
            react_working(&rest, &ids).await;
        });
    }

    // Slash-command pass-through sends the bare command as the first text
    // block (so connector detection fires), then each prompt section as its
    // own block. Per-section blocks let the observer size trimmer elide a
    // section body in place while every `[Header]` line survives at the head
    // of its own leaf — so the "Prompt context" panel counts every section.
    let prompt_blocks: Vec<&str> = match slash_command {
        Some(ref cmd) => std::iter::once(cmd.as_str())
            .chain(prompt_sections.iter().map(String::as_str))
            .collect(),
        None => prompt_sections.iter().map(String::as_str).collect(),
    };
    let prompt_bytes: usize = prompt_blocks.iter().map(|block| block.len()).sum();
    let has_standing_context = match &source {
        PromptSource::Channel(_) => !standing.sections().is_empty(),
        PromptSource::Heartbeat => !standing.sections().is_empty(),
    };
    let standing_context_included =
        !agent.has_system_prompt_support() && !standing_context_sent && has_standing_context;
    tracing::info!(
        target: "pool::prompt",
        prompt_bytes,
        standing_context_included,
        delivered_event_delta = pending_delivered_event_ids.len(),
        "prompt context delivery"
    );
    agent.acp.observe(
        "prompt_context_delivery",
        serde_json::json!({
            "promptBytes": prompt_bytes,
            "standingContextIncluded": standing_context_included,
            "eventDeltaCount": pending_delivered_event_ids.len(),
        }),
    );

    // Commit the ambiguous-effect boundary before the provider sees the
    // prompt. If the process exits before the matching idle write, the next
    // process may resume the provider context but must not replay this event.
    if let (PromptSource::Channel(scope), Some(store)) = (&source, &ctx.session_recovery) {
        if let Err(error) = store.mark_turn_started(
            scope,
            &session_id,
            &turn_id,
            &triggering_event_ids,
            &turn_started_at,
        ) {
            send_prompt_result(
                &result_tx,
                &turn_id,
                agent,
                source,
                PromptOutcome::Error(AcpError::Protocol(format!(
                    "failed to persist provider turn boundary: {error}"
                ))),
                requeue_batch_if_queue(&ctx, batch),
            );
            return;
        }
    }

    // Turn start, labelled exactly as `log_stop_reason` labels the end, so a
    // log reads as start/stop pairs. Purely observational: an unpaired start is
    // the only durable evidence that a turn was entered and never returned, and
    // without it a stalled agent and an agent nobody woke leave identical logs —
    // zero completions either way, so anything reading them afterwards has to
    // guess which happened.
    tracing::info!(
        target: "pool::prompt",
        "turn starting for {}",
        prompt_label(&source)
    );

    // When control_rx is Some (channel tasks), wrap the prompt in select! so
    // the main loop can cancel, interrupt, or rotate it. Heartbeats
    // (control_rx=None) take the simple await path — they are not controllable.
    //
    let prompt_result = match control_rx {
        None => {
            // Heartbeat / non-cancellable path.
            tokio::select! {
                biased;
                result = agent.acp.session_prompt_blocks_with_idle_timeout(
                    &session_id,
                    &prompt_blocks,
                    ctx.idle_timeout,
                    ctx.max_turn_duration,
                ) => result,
            }
        }
        Some(rx) => {
            tokio::select! {
                biased;
                result = agent.acp.session_prompt_blocks_with_idle_timeout(
                    &session_id,
                    &prompt_blocks,
                    ctx.idle_timeout,
                    ctx.max_turn_duration,
                ) => result,
                mode = rx => {
                    let control_signal = mode.unwrap_or(ControlSignal::Cancel);
                    // Land the model switch before any cancel/requeue work: setting
                    // `desired_model` here means the fresh session created by the
                    // requeued turn (busy) or the next turn (already-completed)
                    // applies the new model. Runtime-only — never persisted.
                    if let ControlSignal::SwitchModel { model_id, request_id } = &control_signal {
                        agent.desired_model = Some(model_id.clone());
                        agent.model_overridden = true;
                        agent.desired_model_request_id = request_id.clone();
                        // Busy path: the real apply is deferred to the requeued
                        // session. Arm the positive-terminal emit so that apply
                        // reports success explicitly rather than the Desktop
                        // inferring it from timeout silence.
                        agent.desired_model_pending_ack = true;
                    }
                    // Control signal received. Guard against Race 1: the turn may
                    // have completed naturally just as cancel fired.
                    if agent.acp.has_in_flight_prompt() {
                        // Prompt is genuinely in-flight — cancel it.
                        match agent
                            .acp
                            .cancel_with_cleanup_grace(&session_id, CONTROL_CANCEL_GRACE)
                            .await
                        {
                            Ok(stop_reason) => {
                                log_stop_reason(&source, &stop_reason);
                                agent.state.invalidate(&source);
                                if let (PromptSource::Channel(scope), Some(store)) =
                                    (&source, &ctx.session_recovery)
                                {
                                    if let Err(error) = store.remove(scope) {
                                        tracing::error!(target: "pool::session", "failed to retire cancelled session binding: {error}");
                                    }
                                }
                                let retry_batch =
                                    requeue_cancelled_batch(&ctx, control_signal, batch);

                                let usage = agent.acp.take_turn_usage();
                                publish_agent_turn_metric(
                                    &ctx,
                                    usage,
                                    observer_channel_id,
                                    &session_id,
                                    &turn_id,
                                    Some(buzz_core::agent_turn_metric::StopReason::Cancelled),
                                )
                                .await;
                                send_prompt_result(
                                    &result_tx,
                                    &turn_id,
                                    agent,
                                    source,
                                    PromptOutcome::Cancelled,
                                    retry_batch,
                                );
                                return;
                            }
                            Err(error) => {
                                // Single production arm: classify the error→outcome
                                // and outcome→batch-fate boundary once via the seam
                                // shared with tests, then invalidate/publish/send once.
                                let failure = classify_control_cancel_failure(
                                    &ctx,
                                    error,
                                    control_signal,
                                    batch,
                                );
                                if failure.invalidate_all {
                                    agent.state.invalidate_all();
                                } else {
                                    agent.state.invalidate(&source);
                                }

                                let usage = agent.acp.take_turn_usage();
                                publish_agent_turn_metric(
                                    &ctx,
                                    usage,
                                    observer_channel_id,
                                    &session_id,
                                    &turn_id,
                                    Some(buzz_core::agent_turn_metric::StopReason::Error),
                                )
                                .await;
                                send_prompt_result(
                                    &result_tx,
                                    &turn_id,
                                    agent,
                                    source,
                                    failure.outcome,
                                    failure.retry_batch,
                                );
                                return;
                            }
                        }
                    } else {
                        // Race 1 resolution: turn completed naturally before cancel
                        // could fire. last_prompt_id is None — cleared by
                        // session_prompt_with_idle_timeout() on success. The prompt
                        // future was dropped by select! — its Ok result is gone.
                        //
                        // Note: this `else` branch (last_prompt_id is None) cannot
                        // fire during the pre-prompt phase because `biased` select!
                        // polls the prompt arm first. That arm sets last_prompt_id
                        // synchronously before its first yield point, so by the time
                        // the cancel arm can win, last_prompt_id is already Some.
                        // This branch only fires when the turn genuinely completed
                        // and last_prompt_id was cleared by the success path.
                        //
                        // MUST send a PromptResult or the main loop deadlocks.
                        if matches!(
                            control_signal,
                            ControlSignal::Rotate | ControlSignal::SwitchModel { .. }
                        ) {
                            tracing::debug!(
                                target: "pool::prompt",
                                "rotate/switch signal arrived but turn already completed — invalidating session"
                            );
                        } else {
                            tracing::debug!(
                                target: "pool::prompt",
                                "control signal arrived but turn already completed — treating as success"
                            );
                        }
                        log_stop_reason(&source, &StopReason::EndTurn);
                        if let PromptSource::Channel(scope) = &source {
                            let standing_sent = !agent.has_system_prompt_support();
                            record_scope_delivery_success(
                                &mut agent,
                                scope.clone(),
                                standing_sent,
                                &pending_delivered_event_ids,
                            );
                            if let Some(store) = &ctx.session_recovery {
                                if let Err(error) = store.mark_idle(scope, &session_id) {
                                    tracing::error!(target: "pool::session", "failed to persist completed provider turn: {error}");
                                }
                            }
                        }
                        apply_completed_before_control_signal(
                            &mut agent.state,
                            &source,
                            &control_signal,
                        );
                        let usage = agent.acp.take_turn_usage();
                        publish_agent_turn_metric(
                            &ctx,
                            usage,
                            observer_channel_id,
                            &session_id,
                            &turn_id,
                            Some(buzz_core::agent_turn_metric::StopReason::EndTurn),
                        )
                        .await;
                        send_prompt_result(
                            &result_tx,
                            &turn_id,
                            agent,
                            source,
                            PromptOutcome::Ok(StopReason::EndTurn),
                            None, // turn succeeded — batch was processed, no requeue
                        );
                        return;
                    }
                }
            }
        }
    };

    match prompt_result {
        Ok(stop_reason) => {
            log_stop_reason(&source, &stop_reason);

            if let PromptSource::Channel(scope) = &source {
                let standing_sent = !agent.has_system_prompt_support();
                record_scope_delivery_success(
                    &mut agent,
                    scope.clone(),
                    standing_sent,
                    &pending_delivered_event_ids,
                );
                if let Some(store) = &ctx.session_recovery {
                    if let Err(error) = store.mark_idle(scope, &session_id) {
                        tracing::error!(target: "pool::session", "failed to persist completed provider turn: {error}");
                    }
                }
            } else if !agent.has_system_prompt_support() {
                agent.state.heartbeat_standing_context_sent = true;
            }

            let should_rotate = matches!(
                stop_reason,
                StopReason::MaxTokens | StopReason::MaxTurnRequests
            );

            let should_rotate = should_rotate || {
                let limit = ctx.max_turns_per_session;
                if limit > 0 {
                    match &source {
                        PromptSource::Channel(scope) => {
                            let count = agent.state.turn_counts.entry(scope.clone()).or_insert(0);
                            *count += 1;
                            *count >= limit
                        }
                        PromptSource::Heartbeat => {
                            agent.state.heartbeat_turn_count += 1;
                            agent.state.heartbeat_turn_count >= limit
                        }
                    }
                } else {
                    false
                }
            };

            if should_rotate {
                tracing::info!(
                    target: "pool::session",
                    "rotating session for {source:?} after {stop_reason:?}",
                );
                agent.state.invalidate(&source);
                if let (PromptSource::Channel(scope), Some(store)) =
                    (&source, &ctx.session_recovery)
                {
                    if let Err(error) = store.remove(scope) {
                        tracing::error!(target: "pool::session", "failed to retire rotated session binding: {error}");
                    }
                }
            }

            let core_stop = acp_stop_to_core(&stop_reason);
            let usage = agent.acp.take_turn_usage();
            publish_agent_turn_metric(
                &ctx,
                usage,
                observer_channel_id,
                &session_id,
                &turn_id,
                Some(core_stop),
            )
            .await;

            send_prompt_result(
                &result_tx,
                &turn_id,
                agent,
                source,
                PromptOutcome::Ok(stop_reason),
                None,
            );
        }
        Err(AcpError::AgentExited) => {
            tracing::error!(target: "pool::prompt", "agent {} exited during prompt", agent.index);
            agent.state.invalidate_all();
            let usage = agent.acp.take_turn_usage();
            publish_agent_turn_metric(
                &ctx,
                usage,
                observer_channel_id,
                &session_id,
                &turn_id,
                Some(buzz_core::agent_turn_metric::StopReason::Error),
            )
            .await;
            send_prompt_result(
                &result_tx,
                &turn_id,
                agent,
                source,
                PromptOutcome::AgentExited,
                // The durable turn boundary is still `turn_started`. The
                // provider may have committed tool effects or its answer
                // before the transport died, so replaying this batch would be
                // unsafe. A later, distinct Buzz event may explicitly continue
                // the resumed provider session.
                None,
            );
        }
        Err(AcpError::IdleTimeout(_)) => {
            tracing::warn!(
                target: "pool::prompt",
                "idle timeout ({}s) — cancelling session {session_id}",
                ctx.idle_timeout.as_secs()
            );
            match agent
                .acp
                .cancel_with_cleanup(&session_id, ctx.idle_timeout)
                .await
            {
                Ok(stop_reason) => {
                    log_stop_reason(&source, &stop_reason);
                    let usage = agent.acp.take_turn_usage();
                    publish_agent_turn_metric(
                        &ctx,
                        usage,
                        observer_channel_id,
                        &session_id,
                        &turn_id,
                        Some(buzz_core::agent_turn_metric::StopReason::Cancelled),
                    )
                    .await;
                    // Timeout triggers respawn in handle_prompt_result —
                    // session state will be discarded with the old agent.
                    send_prompt_result(
                        &result_tx,
                        &turn_id,
                        agent,
                        source,
                        PromptOutcome::Timeout(TimeoutKind::Idle),
                        requeue_ambiguous_provider_batch(&ctx, batch),
                    );
                }
                Err(AcpError::AgentExited) => {
                    tracing::error!(
                        target: "pool::prompt",
                        "agent {} exited during cancel_with_cleanup",
                        agent.index
                    );
                    agent.state.invalidate_all();
                    let usage = agent.acp.take_turn_usage();
                    publish_agent_turn_metric(
                        &ctx,
                        usage,
                        observer_channel_id,
                        &session_id,
                        &turn_id,
                        Some(buzz_core::agent_turn_metric::StopReason::Error),
                    )
                    .await;
                    send_prompt_result(
                        &result_tx,
                        &turn_id,
                        agent,
                        source,
                        PromptOutcome::AgentExited,
                        requeue_ambiguous_provider_batch(&ctx, batch),
                    );
                }
                Err(e) => {
                    tracing::error!(
                        target: "pool::prompt",
                        "cancel_with_cleanup error: {e} — invalidating session"
                    );
                    agent.state.invalidate(&source);
                    let usage = agent.acp.take_turn_usage();
                    publish_agent_turn_metric(
                        &ctx,
                        usage,
                        observer_channel_id,
                        &session_id,
                        &turn_id,
                        Some(buzz_core::agent_turn_metric::StopReason::Error),
                    )
                    .await;
                    send_prompt_result(
                        &result_tx,
                        &turn_id,
                        agent,
                        source,
                        PromptOutcome::Timeout(TimeoutKind::Idle),
                        requeue_ambiguous_provider_batch(&ctx, batch),
                    );
                }
            }
        }
        Err(AcpError::HardTimeout { silence }) => {
            let recently_active = silence < RECENT_ACTIVITY_WINDOW;
            tracing::error!(
                target: "pool::prompt",
                "hard timeout ({}s cap, silence {silence:?}, recently_active={recently_active}) — agent process is unrecoverable, invalidating all sessions",
                ctx.max_turn_duration.as_secs()
            );
            agent.state.invalidate_all();
            let usage = agent.acp.take_turn_usage();
            publish_agent_turn_metric(
                &ctx,
                usage,
                observer_channel_id,
                &session_id,
                &turn_id,
                Some(buzz_core::agent_turn_metric::StopReason::Error),
            )
            .await;
            send_prompt_result(
                &result_tx,
                &turn_id,
                agent,
                source,
                PromptOutcome::Timeout(TimeoutKind::Hard { recently_active }),
                requeue_ambiguous_provider_batch(&ctx, batch),
            );
        }
        Err(e) => {
            tracing::error!(target: "pool::prompt", "session_prompt error: {e}");
            // AgentError means the agent caught a problem before mutating
            // session state (e.g. bad LLM response). The session is healthy —
            // don't invalidate it. Other errors may have corrupted state.
            let rejected_before_mutation = matches!(e, AcpError::AgentError { .. });
            if !rejected_before_mutation {
                agent.state.invalidate(&source);
            }
            let usage = agent.acp.take_turn_usage();
            publish_agent_turn_metric(
                &ctx,
                usage,
                observer_channel_id,
                &session_id,
                &turn_id,
                Some(buzz_core::agent_turn_metric::StopReason::Error),
            )
            .await;
            send_prompt_result(
                &result_tx,
                &turn_id,
                agent,
                source,
                PromptOutcome::Error(e),
                if rejected_before_mutation {
                    requeue_batch_if_queue(&ctx, batch)
                } else {
                    requeue_ambiguous_provider_batch(&ctx, batch)
                },
            );
        }
    }
    // _reaction_guard drops here → spawns clear_reactions for all exit paths.
}

/// Retry wrapper for context fetches: one retry with `CONTEXT_FETCH_RETRY_DELAY`
/// on any `None` result. The closure is called twice at most.
///
/// Using a closure (not a `Future`) so the retry can construct a fresh `Future`
/// each attempt without requiring `Clone` or re-boxing.
async fn fetch_with_retry<F, Fut, T>(f: F) -> Option<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    if let Some(result) = f().await {
        return Some(result);
    }
    tokio::time::sleep(CONTEXT_FETCH_RETRY_DELAY).await;
    f().await
}

/// Lazy-fetch channel metadata for a channel not in the startup discovery cache.
///
/// Handles channels added dynamically via membership notifications after startup.
/// Uses `CONTEXT_FETCH_TIMEOUT` with one retry on failure. Returns `None` on
/// persistent failure (graceful degradation — prompt will lack channel name and
/// DM detection).
pub(crate) async fn fetch_channel_info(
    channel_id: Uuid,
    rest: &RestClient,
) -> Option<PromptChannelInfo> {
    fetch_with_retry(|| fetch_channel_info_once(channel_id, rest)).await
}

/// Fetch the current kind-39000 metadata with one bounded request.
///
/// Used by prompt-turn refreshes when cached metadata is already available as
/// a graceful fallback. First-time resolution uses [`fetch_channel_info`] so
/// unknown channels still receive the established retry behavior.
async fn fetch_channel_info_once(channel_id: Uuid, rest: &RestClient) -> Option<PromptChannelInfo> {
    use nostr::{Alphabet, SingleLetterTag};

    let d_tag = SingleLetterTag::lowercase(Alphabet::D);
    let filter = nostr::Filter::new()
        .kind(nostr::Kind::Custom(
            buzz_core::kind::KIND_NIP29_GROUP_METADATA as u16,
        ))
        .custom_tags(d_tag, [channel_id.to_string()]);

    match timeout(
        CONTEXT_FETCH_TIMEOUT,
        rest.query(std::slice::from_ref(&filter)),
    )
    .await
    {
        Ok(Ok(json)) => {
            let events = json.as_array()?;
            let ev = events.first()?;
            let tags = ev.get("tags")?.as_array()?;
            let mut name = None;
            let mut description = None;
            for tag in tags {
                if let Some(arr) = tag.as_array() {
                    match arr.first().and_then(|v| v.as_str()) {
                        Some("name") => name = arr.get(1).and_then(|v| v.as_str()),
                        Some("about") => description = arr.get(1).and_then(|v| v.as_str()),
                        _ => {}
                    }
                }
            }
            let channel_type = crate::relay::channel_type_from_tags(tags);
            let description = description
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            Some(PromptChannelInfo {
                name: name.unwrap_or(UNKNOWN_CHANNEL_NAME).to_string(),
                channel_type,
                description,
                project: None,
            })
        }
        Ok(Err(e)) => {
            tracing::debug!(channel_id = %channel_id, "channel info fetch failed: {e}");
            None
        }
        Err(_) => {
            tracing::debug!(channel_id = %channel_id, "channel info fetch timed out");
            None
        }
    }
}

/// Resolve the listed NIP-MP project whose home channel is `channel_id`.
pub(crate) async fn fetch_project_home_for_channel(
    channel_id: Uuid,
    rest: &RestClient,
) -> Result<Option<PromptProjectInfo>, ProjectLookupError> {
    let channel = channel_id.to_string();
    let filters = [
        serde_json::json!({
            "kinds": [buzz_core::kind::KIND_PROJECT],
            "#buzz-channel": [channel],
        }),
        serde_json::json!({
            "kinds": [buzz_core::kind::KIND_GIT_REPO_ANNOUNCEMENT],
            "#buzz-channel": [channel],
        }),
    ];

    let mut events = Vec::new();
    for filter in filters {
        let mut page_events = fetch_with_retry(|| async {
            match timeout(CONTEXT_FETCH_TIMEOUT, rest.query_raw_all(filter.clone())).await {
                Ok(Ok(events)) => Some(events),
                Ok(Err(e)) => {
                    tracing::debug!(
                        channel_id = %channel_id,
                        "project home fetch failed: {e} — will retry"
                    );
                    None
                }
                Err(_) => {
                    tracing::debug!(
                        channel_id = %channel_id,
                        "project home fetch timed out — will retry"
                    );
                    None
                }
            }
        })
        .await
        .ok_or_else(|| ProjectLookupError("relay query failed or timed out after retry".into()))?;
        events.append(&mut page_events);
    }
    let (projects, repos): (Vec<_>, Vec<_>) = events.into_iter().partition(|event| {
        event.get("kind").and_then(serde_json::Value::as_u64)
            == Some(buzz_core::kind::KIND_PROJECT as u64)
    });
    Ok(pick_authoritative_project_home(
        &projects,
        &repos,
        &channel_id.to_string(),
    ))
}

/// Resolve a workspace Project with one fresh, bounded query per required
/// event kind.  Unlike the general channel-context resolver, this path never
/// falls back to cached authority. The two independent requests run together,
/// keeping the fail-closed delay to one context-fetch timeout.
async fn fetch_project_home_for_channel_strict(
    channel_id: Uuid,
    rest: &RestClient,
) -> Result<Option<PromptProjectInfo>, ProjectLookupError> {
    let channel = channel_id.to_string();
    let project_filter = serde_json::json!({
        "kinds": [buzz_core::kind::KIND_PROJECT],
        "#buzz-channel": [channel],
    });
    let repo_filter = serde_json::json!({
        "kinds": [buzz_core::kind::KIND_GIT_REPO_ANNOUNCEMENT],
        "#buzz-channel": [channel_id.to_string()],
    });

    async fn query_once(
        channel_id: Uuid,
        rest: &RestClient,
        filter: serde_json::Value,
    ) -> Result<Vec<serde_json::Value>, ProjectLookupError> {
        match timeout(CONTEXT_FETCH_TIMEOUT, rest.query_raw_all(filter)).await {
            Ok(Ok(events)) => Ok(events),
            Ok(Err(error)) => Err(ProjectLookupError(format!(
                "strict workspace Project query failed for {channel_id}: {error}"
            ))),
            Err(_) => Err(ProjectLookupError(format!(
                "strict workspace Project query timed out for {channel_id}"
            ))),
        }
    }

    let (projects, repos) = tokio::join!(
        query_once(channel_id, rest, project_filter),
        query_once(channel_id, rest, repo_filter)
    );
    Ok(pick_authoritative_project_home(
        &projects?,
        &repos?,
        &channel_id.to_string(),
    ))
}

/// Fetch owner-signed huddle instructions for a new channel session.
///
/// The event is promoted into the system role, so accepting any channel member's
/// event would be a privilege escalation. Only the configured agent owner's
/// valid signature is accepted; absence or failure simply yields no section.
async fn fetch_huddle_instructions(
    channel_id: Uuid,
    owner: &nostr::PublicKey,
    rest: &RestClient,
) -> Option<String> {
    use nostr::{Alphabet, SingleLetterTag};

    let h_tag = SingleLetterTag::lowercase(Alphabet::H);
    let filter = nostr::Filter::new()
        .kind(nostr::Kind::Custom(
            buzz_core::kind::KIND_HUDDLE_GUIDELINES as u16,
        ))
        .author(*owner)
        .custom_tags(h_tag, [channel_id.to_string()])
        .limit(1);
    let json = match timeout(
        CONTEXT_FETCH_TIMEOUT,
        rest.query(std::slice::from_ref(&filter)),
    )
    .await
    {
        Ok(Ok(json)) => json,
        Ok(Err(error)) => {
            tracing::warn!(channel = %channel_id, "huddle instructions query failed: {error}");
            return None;
        }
        Err(_) => {
            tracing::warn!(channel = %channel_id, "huddle instructions query timed out");
            return None;
        }
    };
    huddle_instructions_from_query_response(json.as_array()?, channel_id, owner)
}

fn huddle_instructions_from_query_response(
    events: &[serde_json::Value],
    channel_id: Uuid,
    owner: &nostr::PublicKey,
) -> Option<String> {
    let raw = events.first()?;
    let event = serde_json::from_value::<nostr::Event>(raw.clone()).ok()?;
    event.verify().ok()?;
    let channel_id = channel_id.to_string();
    if event.pubkey != *owner
        || event.kind.as_u16() as u32 != buzz_core::kind::KIND_HUDDLE_GUIDELINES
        || !event
            .tags
            .iter()
            .any(|tag| tag.kind().to_string() == "h" && tag.content() == Some(channel_id.as_str()))
    {
        return None;
    }
    let content = event.content.trim();
    (!content.is_empty()).then(|| content.to_owned())
}

/// Fetch the latest canvas event for `channel_id` and return a rendered
/// `<channel-canvas>` metadata section, or `None` if absent/blank/error.
///
/// Failure modes (all fail open — no crash, no block):
/// * relay returns no event → `None`
/// * latest event's content is blank → `None` (cleared canvas; older revisions
///   are NOT resurrected)
/// * malformed JSON array, missing fields, bad event ID, bad timestamp →
///   logged at `warn`; returns `None`
/// * REST error or timeout → returns `None`
///
/// Called at most once per new channel session; the result is cached in
/// `SessionState::canvas_sections` and cleared on session invalidation.
async fn fetch_canvas_section(channel_id: Uuid, rest: &RestClient) -> Option<String> {
    use nostr::{Alphabet, SingleLetterTag};

    let h_tag = SingleLetterTag::lowercase(Alphabet::H);
    let filter = nostr::Filter::new()
        .kind(nostr::Kind::Custom(buzz_core::kind::KIND_CANVAS as u16))
        .custom_tags(h_tag, [channel_id.to_string()])
        .limit(1);

    const CANVAS_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
    let json = match tokio::time::timeout(
        CANVAS_FETCH_TIMEOUT,
        rest.query(std::slice::from_ref(&filter)),
    )
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            tracing::warn!(
                target: "canvas::fetch",
                channel = %channel_id,
                "canvas query failed: {e} — emitting no section"
            );
            return None;
        }
        Err(_) => {
            tracing::warn!(
                target: "canvas::fetch",
                channel = %channel_id,
                timeout_ms = CANVAS_FETCH_TIMEOUT.as_millis() as u64,
                "canvas fetch timed out — emitting no section"
            );
            return None;
        }
    };

    let events = match json.as_array() {
        Some(arr) => arr,
        None => {
            tracing::warn!(
                target: "canvas::fetch",
                channel = %channel_id,
                "canvas query response is not a JSON array — emitting no section"
            );
            return None;
        }
    };

    canvas_section_from_query_response(events, &channel_id.to_string())
}

/// Parse a canvas query response array and render a `<channel-canvas>` section.
///
/// Extracted as a pure function so tests can exercise the parsing/validation
/// logic without async machinery or relay connectivity.
///
/// Returns `None` on: empty array, blank content, malformed/partial event JSON
/// (requires a complete, structurally valid Nostr event), or an out-of-range
/// `created_at` timestamp. Never falls back to epoch or raw integers.
pub(crate) fn canvas_section_from_query_response(
    events: &[serde_json::Value],
    channel_uuid: &str,
) -> Option<String> {
    let raw = events.first()?;

    // Deserialise as a complete Nostr Event. Partial objects (missing pubkey,
    // sig, kind, or tags) are rejected here rather than trusted implicitly.
    let event = match serde_json::from_value::<nostr::Event>(raw.clone()) {
        Ok(ev) => ev,
        Err(err) => {
            tracing::warn!(
                target: "canvas::fetch",
                channel = %channel_uuid,
                %err,
                "canvas query returned a malformed event — emitting no section",
            );
            return None;
        }
    };

    // Verify the event's id and signature agree with its content.
    // A structurally complete but tampered event must not supply trusted metadata.
    if let Err(err) = event.verify() {
        tracing::warn!(
            target: "canvas::fetch",
            channel = %channel_uuid,
            %err,
            "canvas event failed signature verification — emitting no section",
        );
        return None;
    }

    // Validate kind: must be KIND_CANVAS (40100).
    if event.kind != nostr::Kind::Custom(buzz_core::kind::KIND_CANVAS as u16) {
        tracing::warn!(
            target: "canvas::fetch",
            channel = %channel_uuid,
            kind = %event.kind.as_u16(),
            "canvas event has unexpected kind — emitting no section",
        );
        return None;
    }

    // Validate h-tag: must carry the channel UUID we queried.
    // The REST boundary filters by #h, but we verify here to prevent a
    // misbehaving relay from injecting a different channel's canvas.
    let h_tag_matches = event.tags.iter().any(|tag| {
        let v = tag.as_slice();
        v.len() >= 2 && v[0] == "h" && v[1] == channel_uuid
    });
    if !h_tag_matches {
        tracing::warn!(
            target: "canvas::fetch",
            channel = %channel_uuid,
            "canvas event is missing expected h-tag — emitting no section",
        );
        return None;
    }

    // Blank content means the canvas was cleared; do not fall back to older events.
    if event.content.trim().is_empty() {
        tracing::debug!(
            target: "canvas::fetch",
            channel = %channel_uuid,
            "latest canvas event has blank content — emitting no section"
        );
        return None;
    }

    let id = event.id.to_hex();

    // Convert the Nostr timestamp to a UTC RFC3339 string with Z suffix.
    // Use checked conversion: a u64 that exceeds i64::MAX (e.g. Timestamp::max())
    // wraps silently with `as i64`, producing a negative value that chrono would
    // accept as a date in 1969. Reject out-of-range values explicitly instead.
    let ts_secs = match i64::try_from(event.created_at.as_secs()) {
        Ok(s) => s,
        Err(_) => {
            tracing::warn!(
                target: "canvas::fetch",
                channel = %channel_uuid,
                "canvas event created_at overflows i64 — emitting no section",
            );
            return None;
        }
    };
    let timestamp = match chrono::DateTime::from_timestamp(ts_secs, 0) {
        Some(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        None => {
            tracing::warn!(
                target: "canvas::fetch",
                channel = %channel_uuid,
                ts_secs,
                "canvas event has out-of-range created_at — emitting no section",
            );
            return None;
        }
    };

    tracing::info!(
        target: "canvas::fetch",
        channel = %channel_uuid,
        event_id = %id,
        "injected channel canvas metadata section into system prompt"
    );
    Some(render_canvas_section(&id, &timestamp, channel_uuid))
}

/// Render the `<channel-canvas>` metadata section string.
///
/// Pure function — kept separate so unit tests can exercise rendering
/// without async machinery or relay connectivity.
pub(crate) fn render_canvas_section(event_id: &str, timestamp: &str, channel_uuid: &str) -> String {
    crate::prompt_framing::semantic_section(
        "channel-canvas",
        &format!(
            "Canvas revision (event ID): {event_id}\n\
             Last modified: {timestamp}\n\
             Fetch current content with: buzz canvas get --channel {channel_uuid}"
        ),
    )
}

fn conversation_context_event_ids(context: Option<&ConversationContext>) -> HashSet<String> {
    match context {
        Some(ConversationContext::Thread { messages, .. })
        | Some(ConversationContext::Dm { messages, .. }) => messages
            .iter()
            .filter(|message| !message.event_id.is_empty())
            .map(|message| message.event_id.clone())
            .collect(),
        None => HashSet::new(),
    }
}

/// Remove events already delivered to this live ACP session. Triggering events
/// are also excluded because they are rendered separately in `[Event]`.
/// IDs are compared in Buzz's canonical 64-character lowercase hex form: relay
/// context JSON supplies the same form emitted by `EventId::to_hex()`. A
/// non-canonical or missing ID deliberately fails open and may be re-sent.
fn conversation_context_delta(
    context: Option<ConversationContext>,
    delivered: &HashSet<String>,
    triggering: &HashSet<String>,
) -> Option<ConversationContext> {
    let filter = |messages: Vec<ContextMessage>| {
        messages
            .into_iter()
            .filter(|message| {
                message.event_id.is_empty()
                    || (!delivered.contains(&message.event_id)
                        && !triggering.contains(&message.event_id))
            })
            .collect::<Vec<_>>()
    };

    match context? {
        ConversationContext::Thread {
            messages,
            total,
            root_present,
            truncated,
        } => {
            let messages = filter(messages);
            (!messages.is_empty()).then_some(ConversationContext::Thread {
                messages,
                total,
                root_present,
                truncated,
            })
        }
        ConversationContext::Dm {
            messages,
            total,
            truncated,
        } => {
            let messages = filter(messages);
            (!messages.is_empty()).then_some(ConversationContext::Dm {
                messages,
                total,
                truncated,
            })
        }
    }
}

/// Fetch conversation context (thread or DM) for a batch before prompting.
///
/// Returns `None` if:
/// - The event is a plain channel message (not a thread reply, not a DM)
/// - The REST fetch fails or times out (graceful degradation)
/// - `context_message_limit` is 0
///
/// Context is scoped by the batch's resolved [`SessionScope`], never inferred
/// from whichever event happens to be last:
///
/// - **Thread scope** → fetch only that canonical thread's history (all
///   messages under the root, including intervening non-mention human
///   messages). A brand-new thread (root == the triggering event, first turn)
///   has no prior history, so this returns `None`, which is correct: the
///   trigger itself is delivered as the `[Event]` block.
/// - **Conversation scope** (DMs always; channels under the `channel` policy)
///   → preserve legacy behavior: a threaded reply fetches its reply chain;
///   a DM non-reply fetches recent conversation history.
///
/// The delivery-delta filter (`conversation_context_delta`) then removes any
/// events this scope's live session already received, so subsequent turns
/// deliver only intervening same-thread messages plus the trigger.
async fn fetch_conversation_context(
    batch: &FlushBatch,
    channel_info: &Option<PromptChannelInfo>,
    ctx: &PromptContext,
) -> Option<ConversationContext> {
    let limit = ctx.context_message_limit;
    let is_dm = channel_info
        .as_ref()
        .map(|ci| ci.channel_type == "dm")
        .unwrap_or(false);

    match resolve_context_target(batch, is_dm) {
        ContextTarget::Thread(root_id) => {
            fetch_thread_context(
                batch.channel_id,
                &root_id,
                limit,
                ctx.agent_keys.public_key(),
                &ctx.rest_client,
            )
            .await
        }
        ContextTarget::Dm => fetch_dm_context(batch.channel_id, limit, &ctx.rest_client).await,
        ContextTarget::None => None,
    }
}

/// Which history to fetch for a batch's context section.
#[derive(Debug, PartialEq, Eq)]
enum ContextTarget {
    /// Fetch the canonical thread rooted at this event id.
    Thread(String),
    /// Fetch recent DM conversation history.
    Dm,
    /// No supplementary context (new thread's first turn, or plain channel).
    None,
}

/// Decide which history to gather, driven by the batch's resolved
/// [`SessionScope`] — never by inferring scope from the last event.
///
/// - Thread scope: the canonical root is authoritative.
/// - Conversation scope (DMs always; channels under `channel` policy): a
///   threaded reply fetches its reply chain; a DM non-reply fetches recent
///   conversation history; a plain top-level channel message has none.
fn resolve_context_target(batch: &FlushBatch, is_dm: bool) -> ContextTarget {
    if let Some(root_id) = batch.scope.root_event_id() {
        return ContextTarget::Thread(root_id.to_string());
    }
    let Some(last_event) = batch.events.last() else {
        return ContextTarget::None;
    };
    if let Some(root_id) = crate::queue::parse_thread_tags(&last_event.event).root_event_id {
        return ContextTarget::Thread(root_id);
    }
    if is_dm {
        return ContextTarget::Dm;
    }
    ContextTarget::None
}

/// Normalize AND validate a pubkey for the batch profile API request.
/// Returns `None` for malformed input — only valid 64-char hex passes.
/// See also: `normalize_lookup_key` in queue.rs (normalize-only, no validation).
fn normalize_prompt_pubkey(pubkey: &str) -> Option<String> {
    let normalized = pubkey.trim().to_ascii_lowercase();
    if normalized.len() == 64 && normalized.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(normalized)
    } else {
        None
    }
}

fn collect_prompt_pubkeys(
    batch: &FlushBatch,
    conversation_context: Option<&ConversationContext>,
) -> Vec<String> {
    let mut pubkeys = HashSet::new();

    for event in &batch.events {
        pubkeys.insert(event.event.pubkey.to_hex().to_ascii_lowercase());

        for mentioned in crate::queue::parse_thread_tags(&event.event).mentioned_pubkeys {
            if let Some(normalized) = normalize_prompt_pubkey(&mentioned) {
                pubkeys.insert(normalized);
            }
        }
    }

    let context_messages = match conversation_context {
        Some(ConversationContext::Thread { messages, .. })
        | Some(ConversationContext::Dm { messages, .. }) => Some(messages),
        None => None,
    };

    if let Some(messages) = context_messages {
        for message in messages {
            if let Some(normalized) = normalize_prompt_pubkey(&message.pubkey) {
                pubkeys.insert(normalized);
            }
        }
    }

    let mut pubkeys: Vec<String> = pubkeys.into_iter().collect();
    pubkeys.sort();
    pubkeys
}

/// Detect whether a kind:0 profile event belongs to an owned agent.
///
/// Agents carry a NIP-OA `["auth", owner_pk, conditions, sig]` tag in their
/// profile; humans do not. This checks for the tag's presence/shape only — a
/// cheap routing heuristic for reply anchoring, not a verified security gate
/// (the signing path in `lib.rs::check_sibling_via_profile` does full
/// verification where it matters).
fn profile_event_is_agent(ev: &serde_json::Value) -> bool {
    ev.get("tags")
        .and_then(|t| t.as_array())
        .is_some_and(|tags| {
            tags.iter().any(|tag| {
                tag.as_array()
                    .is_some_and(|parts| parts.len() == 4 && parts[0].as_str() == Some("auth"))
            })
        })
}

/// Parse kind:0 profile events into a `PromptProfileLookup`.
///
/// Each kind:0 event has `pubkey` and JSON `content` with optional fields:
/// `display_name` (or `name`), `nip05`.
fn parse_kind0_profile_lookup(json: serde_json::Value) -> Option<PromptProfileLookup> {
    let events = json.as_array()?;
    let mut lookup = PromptProfileLookup::new();

    for ev in events {
        let pubkey = ev.get("pubkey").and_then(|v| v.as_str());
        let content_str = ev.get("content").and_then(|v| v.as_str());
        if let (Some(pk), Some(content)) = (pubkey, content_str) {
            if let Ok(profile) = serde_json::from_str::<serde_json::Value>(content) {
                let display_name = profile
                    .get("display_name")
                    .or_else(|| profile.get("name"))
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let nip05_handle = profile
                    .get("nip05")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let is_agent = profile_event_is_agent(ev);
                lookup.insert(
                    pk.to_ascii_lowercase(),
                    PromptProfile {
                        display_name,
                        nip05_handle,
                        is_agent,
                    },
                );
            }
        }
    }

    if lookup.is_empty() {
        None
    } else {
        Some(lookup)
    }
}

async fn fetch_prompt_profile_lookup(
    batch: &FlushBatch,
    conversation_context: Option<&ConversationContext>,
    rest: &RestClient,
) -> Option<PromptProfileLookup> {
    let pubkeys = collect_prompt_pubkeys(batch, conversation_context);
    if pubkeys.is_empty() {
        return None;
    }

    // Query kind:0 (NIP-01 profile metadata) for all pubkeys.
    let authors: Vec<nostr::PublicKey> = pubkeys
        .iter()
        .filter_map(|s| nostr::PublicKey::from_hex(s).ok())
        .collect();
    if authors.is_empty() {
        return None;
    }
    let filter = nostr::Filter::new()
        .kind(nostr::Kind::Metadata)
        .authors(authors);

    fetch_with_retry(|| async {
        match timeout(
            CONTEXT_FETCH_TIMEOUT,
            rest.query(std::slice::from_ref(&filter)),
        )
        .await
        {
            Ok(Ok(json)) => parse_kind0_profile_lookup(json),
            Ok(Err(e)) => {
                tracing::debug!("prompt profile lookup failed: {e} — will retry");
                None
            }
            Err(_) => {
                tracing::debug!("prompt profile lookup timed out — will retry");
                None
            }
        }
    })
    .await
}

/// Fetch thread context via Nostr query: root event by ID + replies by `#e` tag.
///
/// The reply query intentionally requests one more reply than the configured
/// display window. That sentinel event lets the prompt say `N of M, truncated`
/// when the relay has more thread history, instead of reporting the capped page
/// as the total. When the window is full, a best-effort `/count` attempts to
/// improve that lower-bound total; because it is a separate racy request, the
/// result is clamped to the sentinel-proven minimum. The query also asks for the
/// agent's newest reply separately so the next prompt can include the agent's
/// own prior turn even in busy threads where the recent-message window would
/// otherwise push it out.
async fn fetch_thread_context(
    channel_id: Uuid,
    root_event_id: &str,
    limit: u32,
    agent_pubkey: nostr::PublicKey,
    rest: &RestClient,
) -> Option<ConversationContext> {
    fetch_thread_context_with(
        channel_id,
        root_event_id,
        limit,
        agent_pubkey,
        |filters| async move { rest.query(&filters).await },
        |filters| async move { rest.count(&filters).await },
    )
    .await
}

async fn fetch_thread_context_with<Query, QueryFut, Count, CountFut>(
    channel_id: Uuid,
    root_event_id: &str,
    limit: u32,
    agent_pubkey: nostr::PublicKey,
    query: Query,
    count: Count,
) -> Option<ConversationContext>
where
    Query: Fn(Vec<nostr::Filter>) -> QueryFut,
    QueryFut: std::future::Future<Output = Result<serde_json::Value, crate::relay::RelayError>>,
    Count: Fn(Vec<nostr::Filter>) -> CountFut,
    CountFut: std::future::Future<Output = Result<serde_json::Value, crate::relay::RelayError>>,
{
    use nostr::{Alphabet, SingleLetterTag};

    // Defense-in-depth: validate hex event ID.
    if root_event_id.is_empty()
        || root_event_id.len() != 64
        || !root_event_id.chars().all(|c| c.is_ascii_hexdigit())
    {
        tracing::warn!(
            channel_id = %channel_id,
            "invalid root_event_id (expected 64 hex chars) — skipping thread context fetch"
        );
        return None;
    }

    let e_tag = SingleLetterTag::lowercase(Alphabet::E);
    let h_tag = SingleLetterTag::lowercase(Alphabet::H);
    let ch_str = channel_id.to_string();

    // Three filters: (1) root event by ID, (2) recent replies with #e=root +
    // #h=channel plus a sentinel, and (3) the agent's newest reply for pinning.
    let root_filter = nostr::Filter::new().id(nostr::EventId::from_hex(root_event_id).ok()?);
    let replies_filter = nostr::Filter::new()
        .kinds([
            nostr::Kind::Custom(buzz_core::kind::KIND_STREAM_MESSAGE as u16),
            nostr::Kind::Custom(buzz_core::kind::KIND_STREAM_MESSAGE_V2 as u16),
            nostr::Kind::Custom(buzz_core::kind::KIND_JOB_ACCEPTED as u16),
            nostr::Kind::Custom(buzz_core::kind::KIND_JOB_PROGRESS as u16),
            nostr::Kind::Custom(buzz_core::kind::KIND_JOB_RESULT as u16),
            nostr::Kind::Custom(buzz_core::kind::KIND_JOB_CANCEL as u16),
            nostr::Kind::Custom(buzz_core::kind::KIND_JOB_ERROR as u16),
        ])
        .custom_tags(e_tag, [root_event_id])
        .custom_tags(h_tag, [ch_str.as_str()])
        .limit(limit.saturating_add(1) as usize);
    let agent_reply_filter = replies_filter.clone().author(agent_pubkey).limit(1);

    let context = fetch_with_retry(|| async {
        match timeout(
            CONTEXT_FETCH_TIMEOUT,
            query(vec![
                root_filter.clone(),
                replies_filter.clone(),
                agent_reply_filter.clone(),
            ]),
        )
        .await
        {
            Ok(Ok(json)) => {
                parse_nostr_thread_response_with_meta(json, root_event_id, limit, &agent_pubkey)
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    channel_id = %channel_id,
                    root = root_event_id,
                    "thread context fetch failed: {e} — will retry"
                );
                None
            }
            Err(_) => {
                tracing::warn!(
                    channel_id = %channel_id,
                    root = root_event_id,
                    "thread context fetch timed out — will retry"
                );
                None
            }
        }
    })
    .await;

    let mut parsed = context?;

    if matches!(
        parsed.context,
        ConversationContext::Thread {
            truncated: true,
            ..
        }
    ) {
        let replies_count_filter = replies_filter.clone().limit(0);
        if let Some(total) = fetch_thread_total(
            channel_id,
            &replies_count_filter,
            parsed.root_present,
            &count,
        )
        .await
        {
            if let ConversationContext::Thread {
                total: context_total,
                ..
            } = &mut parsed.context
            {
                let sentinel_minimum = *context_total;
                // `/count` is a separate best-effort request after the message
                // query. If replies are deleted between the two, the exact count
                // can fall below the already-proven sentinel minimum; never
                // render impossible labels like `13 of 12 messages, truncated`.
                *context_total = total.max(sentinel_minimum);
            }
        }
    }

    Some(parsed.context)
}

/// Best-effort exact thread size for truncated context labels.
async fn fetch_thread_total<Count, CountFut>(
    channel_id: Uuid,
    replies_filter: &nostr::Filter,
    root_present: bool,
    count: &Count,
) -> Option<usize>
where
    Count: Fn(Vec<nostr::Filter>) -> CountFut,
    CountFut: std::future::Future<Output = Result<serde_json::Value, crate::relay::RelayError>>,
{
    let replies_count =
        match timeout(CONTEXT_COUNT_TIMEOUT, count(vec![replies_filter.clone()])).await {
            Ok(Ok(json)) => json.get("count").and_then(|v| v.as_u64())?,
            Ok(Err(e)) => {
                tracing::debug!(
                    channel_id = %channel_id,
                    "thread context count failed; using sentinel minimum: {e}"
                );
                return None;
            }
            Err(_) => {
                tracing::debug!(
                    channel_id = %channel_id,
                    "thread context count timed out; using sentinel minimum"
                );
                return None;
            }
        };

    Some(replies_count as usize + usize::from(root_present))
}

/// Fetch DM context via Nostr query: recent messages in channel by `#h` tag.
async fn fetch_dm_context(
    channel_id: Uuid,
    limit: u32,
    rest: &RestClient,
) -> Option<ConversationContext> {
    use nostr::{Alphabet, SingleLetterTag};

    let h_tag = SingleLetterTag::lowercase(Alphabet::H);
    let ch_str = channel_id.to_string();
    let filter = nostr::Filter::new()
        .kinds([
            nostr::Kind::Custom(buzz_core::kind::KIND_STREAM_MESSAGE as u16),
            nostr::Kind::Custom(buzz_core::kind::KIND_STREAM_MESSAGE_V2 as u16),
        ])
        .custom_tags(h_tag, [ch_str.as_str()])
        .limit(limit as usize);

    fetch_with_retry(|| async {
        match timeout(
            CONTEXT_FETCH_TIMEOUT,
            rest.query(std::slice::from_ref(&filter)),
        )
        .await
        {
            Ok(Ok(json)) => parse_nostr_dm_response(json, limit),
            Ok(Err(e)) => {
                tracing::warn!(
                    channel_id = %channel_id,
                    "DM context fetch failed: {e} — will retry"
                );
                None
            }
            Err(_) => {
                tracing::warn!(
                    channel_id = %channel_id,
                    "DM context fetch timed out — will retry"
                );
                None
            }
        }
    })
    .await
}

/// Parse the legacy REST thread response (used in tests only).
#[cfg(test)]
fn parse_thread_response(json: serde_json::Value) -> Option<ConversationContext> {
    let mut messages = Vec::new();

    // Root message.
    if let Some(root) = json.get("root") {
        if let Some(msg) = json_to_context_message(root) {
            messages.push(msg);
        }
    }

    // Replies.
    if let Some(replies) = json.get("replies").and_then(|v| v.as_array()) {
        for reply in replies {
            if let Some(msg) = json_to_context_message(reply) {
                messages.push(msg);
            }
        }
    }

    let total_replies = json
        .get("total_replies")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let total = total_replies + 1; // +1 for root
    let truncated = total > messages.len();

    if messages.is_empty() {
        return None;
    }

    Some(ConversationContext::Thread {
        messages,
        total,
        root_present: json.get("root").and_then(json_to_context_message).is_some(),
        truncated,
    })
}

/// Parse the DM messages REST response into a `ConversationContext::Dm`.
///
/// Parse the legacy REST DM response (used in tests only).
#[cfg(test)]
fn parse_dm_response(json: serde_json::Value, limit: u32) -> Option<ConversationContext> {
    let arr = json.get("messages").and_then(|v| v.as_array())?;

    let mut messages: Vec<ContextMessage> =
        arr.iter().filter_map(json_to_context_message).collect();

    // API returns newest-first; reverse to chronological for the prompt.
    messages.reverse();

    // The relay's next_cursor is always set when the page is non-empty (not
    // just when more pages exist), so we can't use it for truncation detection.
    // Instead, compare returned count against the requested limit.
    let truncated = messages.len() >= limit as usize;
    let total = if truncated {
        messages.len() + 1 // indicate there are more
    } else {
        messages.len()
    };

    if messages.is_empty() {
        return None;
    }

    Some(ConversationContext::Dm {
        messages,
        total,
        truncated,
    })
}

/// Extract a `ContextMessage` from a JSON message object.
///
/// Works with both thread reply objects and channel message objects.
fn json_to_context_message(obj: &serde_json::Value) -> Option<ContextMessage> {
    let content = obj.get("content").and_then(|v| v.as_str())?;
    let pubkey = obj
        .get("pubkey")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let timestamp = obj
        .get("created_at")
        .and_then(|v| {
            // Handle both string timestamps and integer timestamps.
            v.as_str().map(|s| s.to_string()).or_else(|| {
                v.as_i64().map(|ts| {
                    chrono::DateTime::from_timestamp(ts, 0)
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_else(|| ts.to_string())
                })
            })
        })
        .unwrap_or_else(|| "unknown".to_string());

    let event_id = obj
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    Some(ContextMessage {
        event_id,
        pubkey: pubkey.to_string(),
        timestamp,
        content: content.to_string(),
    })
}

/// Parse a Nostr query response (array of events) into thread context.
///
/// Separates the root event (matching `root_event_id`) from replies, keeps the
/// newest `limit` replies returned by the sentinel query, then sorts the
/// displayed window chronologically for the prompt. If the agent's newest reply
/// is outside that window, keep it instead of the oldest displayed reply so the
/// next prompt always includes the agent's most recent prior turn.
#[cfg(test)]
fn parse_nostr_thread_response(
    json: serde_json::Value,
    root_event_id: &str,
    limit: u32,
    agent_pubkey: &nostr::PublicKey,
) -> Option<ConversationContext> {
    parse_nostr_thread_response_with_meta(json, root_event_id, limit, agent_pubkey)
        .map(|parsed| parsed.context)
}

struct ParsedThreadContext {
    context: ConversationContext,
    root_present: bool,
}

fn parse_nostr_thread_response_with_meta(
    json: serde_json::Value,
    root_event_id: &str,
    limit: u32,
    agent_pubkey: &nostr::PublicKey,
) -> Option<ParsedThreadContext> {
    let events = json.as_array()?;
    let agent_pubkey_hex = agent_pubkey.to_hex();
    let mut root_msg = None;
    let mut reply_msgs = Vec::new();
    let mut seen_reply_ids = HashSet::new();

    for ev in events {
        let ev_id = ev.get("id").and_then(|v| v.as_str()).unwrap_or("");
        if let Some(msg) = json_to_context_message(ev) {
            if ev_id == root_event_id {
                root_msg = Some(msg);
            } else if seen_reply_ids.insert(ev_id.to_string()) {
                let is_agent = msg.pubkey.eq_ignore_ascii_case(&agent_pubkey_hex);
                reply_msgs.push((
                    ev_id.to_string(),
                    ev.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0),
                    is_agent,
                    msg,
                ));
            }
        }
    }

    let root_present = root_msg.is_some();
    let fetched_total = reply_msgs.len() + usize::from(root_present);
    let newest_agent_reply = reply_msgs
        .iter()
        .filter(|(_, _, is_agent, _)| *is_agent)
        .max_by_key(|(_, ts, _, _)| *ts)
        .cloned();

    let truncated = reply_msgs.len() > limit as usize;
    if truncated {
        // The relay returns limited REQ results newest-first. Sort explicitly so
        // the sentinel we drop is the oldest reply in the fetched window, not an
        // arbitrary last element if the HTTP bridge ever changes iteration order.
        reply_msgs.sort_by_key(|(_, ts, _, _)| Reverse(*ts));
        reply_msgs.truncate(limit as usize);
    }

    if let Some(agent_reply) = newest_agent_reply {
        let agent_reply_already_displayed =
            reply_msgs.iter().any(|(id, _, _, _)| *id == agent_reply.0);
        if !agent_reply_already_displayed {
            reply_msgs.sort_by_key(|(_, ts, _, _)| *ts);
            if let Some(oldest) = reply_msgs.first_mut() {
                *oldest = agent_reply;
            }
        }
    }

    // Sort displayed replies chronologically.
    reply_msgs.sort_by_key(|(_, ts, _, _)| *ts);

    let mut messages = Vec::new();
    if let Some(root) = root_msg {
        messages.push(root);
    }
    messages.extend(reply_msgs.into_iter().map(|(_, _, _, msg)| msg));

    if messages.is_empty() {
        return None;
    }

    let total = if truncated {
        fetched_total // all distinct fetched replies plus the root are proven visible history
    } else {
        messages.len()
    };

    Some(ParsedThreadContext {
        context: ConversationContext::Thread {
            messages,
            total,
            root_present,
            truncated,
        },
        root_present,
    })
}

/// Parse a Nostr query response (array of events) into DM context.
///
/// Events arrive in relay order (newest first); reversed to chronological.
fn parse_nostr_dm_response(json: serde_json::Value, limit: u32) -> Option<ConversationContext> {
    let events = json.as_array()?;

    let mut messages: Vec<(u64, ContextMessage)> = events
        .iter()
        .filter_map(|ev| {
            let ts = ev.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0);
            json_to_context_message(ev).map(|msg| (ts, msg))
        })
        .collect();

    // Sort chronologically (oldest first).
    messages.sort_by_key(|(ts, _)| *ts);

    let messages: Vec<ContextMessage> = messages.into_iter().map(|(_, msg)| msg).collect();
    let truncated = messages.len() >= limit as usize;
    let total = if truncated {
        messages.len() + 1
    } else {
        messages.len()
    };

    if messages.is_empty() {
        return None;
    }

    Some(ConversationContext::Dm {
        messages,
        total,
        truncated,
    })
}

/// Return the batch for requeue only in Queue mode; drop it in Drop mode.
#[inline]
fn requeue_batch_if_queue(ctx: &PromptContext, batch: Option<FlushBatch>) -> Option<FlushBatch> {
    match ctx.dedup_mode {
        DedupMode::Queue => batch,
        DedupMode::Drop => None,
    }
}

/// Preserve legacy retry behavior for harnesses without durable session state.
/// Once recovery is enabled, an error after the provider boundary is
/// indeterminate and only a distinct follow-up may continue that session.
#[inline]
fn requeue_ambiguous_provider_batch(
    ctx: &PromptContext,
    batch: Option<FlushBatch>,
) -> Option<FlushBatch> {
    if ctx.session_recovery.is_some() {
        None
    } else {
        requeue_batch_if_queue(ctx, batch)
    }
}

/// Preserve a triggering batch that has not crossed its provider prompt
/// boundary. Session recovery suppresses ambiguous post-prompt replay, but an
/// initial-message failure happened before the actual event was delivered and
/// is therefore safe to retry through the ordinary bounded queue policy.
#[inline]
fn requeue_pre_prompt_batch(ctx: &PromptContext, batch: Option<FlushBatch>) -> Option<FlushBatch> {
    requeue_batch_if_queue(ctx, batch)
}

/// Map a cancelling [`ControlSignal`] to the [`CancelReason`] that should frame
/// the merged re-prompt, then requeue the batch (in `Queue` dedup mode) with
/// that reason stamped onto [`FlushBatch::cancel_reason`]. `Cancel`/`Rotate`
/// drop the batch entirely. The reason is consumed by the main loop at requeue
/// time (`requeue_as_cancelled`) and ultimately by `format_prompt`.
#[inline]
fn requeue_cancelled_batch(
    ctx: &PromptContext,
    signal: ControlSignal,
    batch: Option<FlushBatch>,
) -> Option<FlushBatch> {
    let reason = match signal {
        ControlSignal::Steer => CancelReason::Steer,
        ControlSignal::Interrupt | ControlSignal::SwitchModel { .. } => CancelReason::Interrupt,
        // Cancel/Rotate discard the batch — no merged re-prompt.
        ControlSignal::Cancel | ControlSignal::Rotate => return None,
    };
    requeue_batch_if_queue(ctx, batch).map(|mut b| {
        b.cancel_reason = Some(reason);
        b
    })
}

/// Result of classifying a failed [`AcpClient::cancel_with_cleanup_grace`]
/// call: the [`PromptOutcome`] to report and the triggering batch's fate,
/// decided together so tests cross the exact error→outcome→batch-fate
/// boundary the production `Err(error)` arm uses.
struct ControlCancelFailure {
    outcome: PromptOutcome,
    retry_batch: Option<FlushBatch>,
    /// `AgentExited` invalidates every session on the agent; every other
    /// failure invalidates only the source that triggered this turn.
    invalidate_all: bool,
}

/// Classify a failed control-signal cancellation (steer fallback, interrupt,
/// or explicit stop) into the [`PromptOutcome`] to report and the triggering
/// batch's fate. This is the single production seam used by the `Err(error)`
/// arm of the control-cancel branch in [`run_prompt_task`] — the boundary
/// this exists to keep singular, so regressions there are regression-tested.
///
/// [`AcpError::CancelDrainTimeout`] is the expected, common case: the agent
/// didn't stop within its bounded grace window. [`AcpError::HardTimeout`] is
/// not expected here — [`AcpClient::cancel_with_cleanup_grace`] translates its
/// own drain-deadline `HardTimeout` into `CancelDrainTimeout` before
/// returning — but for defense in depth an unexpected `HardTimeout` at this
/// bounded cancellation boundary must never regain real hard-cap/dead-letter
/// classification, so it maps to `CancelDrainTimeout(CONTROL_CANCEL_GRACE)`
/// rather than `Timeout(Hard)`.
fn classify_control_cancel_failure(
    ctx: &PromptContext,
    error: AcpError,
    signal: ControlSignal,
    batch: Option<FlushBatch>,
) -> ControlCancelFailure {
    let (outcome, invalidate_all) = match error {
        AcpError::AgentExited => (PromptOutcome::AgentExited, true),
        AcpError::IdleTimeout(_) => (PromptOutcome::Timeout(TimeoutKind::Idle), false),
        AcpError::CancelDrainTimeout(grace) => (PromptOutcome::CancelDrainTimeout(grace), false),
        // Defense in depth: this bounded cancellation API is documented to
        // translate its own HardTimeout into CancelDrainTimeout, so this arm
        // should be unreachable in practice. If it ever fires anyway, still
        // report the truthful non-hard outcome rather than the real hard-cap
        // (which would dead-letter the batch and claim the configured cap).
        AcpError::HardTimeout { .. } => (
            PromptOutcome::CancelDrainTimeout(CONTROL_CANCEL_GRACE),
            false,
        ),
        other => (PromptOutcome::Error(other), false),
    };
    let retry_batch = if invalidate_all || ctx.session_recovery.is_some() {
        // The adapter died while a provider turn was active. Provider/tool
        // effects are indeterminate, so even a pending steer must not replay
        // the original trigger into a fresh session.
        None
    } else {
        requeue_cancelled_batch(ctx, signal, batch)
    };
    ControlCancelFailure {
        outcome,
        retry_batch,
        invalidate_all,
    }
}

/// How a turn's source is named in the `pool::prompt` log lines.
///
/// Shared by the turn-start and turn-stop lines so a log can be read as pairs.
fn prompt_label(source: &PromptSource) -> String {
    match source {
        PromptSource::Channel(scope) => format!(
            "channel {} ({})",
            scope.channel_id(),
            scope.telemetry_label()
        ),
        PromptSource::Heartbeat => "heartbeat".to_string(),
    }
}

/// Log a stop reason at the appropriate tracing level.
fn log_stop_reason(source: &PromptSource, stop_reason: &StopReason) {
    let label = prompt_label(source);
    match stop_reason {
        StopReason::EndTurn => {
            tracing::info!(target: "pool::prompt", "turn complete for {label}: end_turn");
        }
        StopReason::Cancelled => {
            tracing::warn!(target: "pool::prompt", "turn cancelled for {label}");
        }
        StopReason::MaxTokens => {
            tracing::warn!(target: "pool::prompt", "turn hit max_tokens for {label} — session will be rotated");
        }
        StopReason::MaxTurnRequests => {
            tracing::warn!(target: "pool::prompt", "turn hit max_turn_requests for {label} — session will be rotated");
        }
        StopReason::Refusal => {
            tracing::warn!(target: "pool::prompt", "turn refused for {label}");
        }
    }
}

fn delivery_receipt_line(channel_id: Uuid, event_ids: &HashSet<String>) -> String {
    let mut event_ids: Vec<&str> = event_ids.iter().map(String::as_str).collect();
    event_ids.sort_unstable();
    format!(
        "turn delivered Buzz events for channel {channel_id}: {}",
        event_ids.join(",")
    )
}

fn record_scope_delivery_success(
    agent: &mut OwnedAgent,
    scope: SessionScope,
    standing_context_sent: bool,
    event_ids: &HashSet<String>,
) {
    tracing::info!(
        target: "pool::prompt",
        "{}",
        delivery_receipt_line(scope.channel_id(), event_ids)
    );
    agent.state.mark_scope_delivery_success(
        scope,
        standing_context_sent,
        event_ids.iter().cloned(),
    );
}

//
// Two-phase lifecycle visible to users:
//   👀  "seen"    — event was queued and an agent will handle it
//   💬  "working" — agent is actively prompting
//
// 💬 is awaited inline in `run_prompt_task` before the prompt fires, so
// add-before-remove ordering is structural. 👀 is fire-and-forget from
// `main.rs` at queue-push time for immediate responsiveness; on rare
// fast-failure paths the guard's cleanup may race with the 👀 add,
// leaving a cosmetic stale 👀 (see `ReactionGuard` docs).
//
// Cleanup is fire-and-forget via `ReactionGuard` (spawned on drop).
// Failures are debug-logged and ignored — reactions are cosmetic.

/// Drop guard that spawns reaction cleanup on any exit path.
///
/// Created at the top of `run_prompt_task`. On drop — normal return, early
/// return, or panic — spawns fire-and-forget removal of both 👀 and 💬.
///
/// ## Ordering
///
/// 💬 (`react_working`) is fire-and-forget (spawned before the prompt fires).
/// A brief race where 💬 appears slightly after the agent starts is acceptable.
///
/// 👀 (`react_seen`) is fire-and-forget from `main.rs` at queue-push time.
/// On rare fast-failure paths (e.g., `session_new` error on an idle agent),
/// the cleanup spawn may race with the 👀 add, leaving a stale 👀. This is
/// accepted as a cosmetic edge case — the message will be retried and the
/// stale 👀 is harmless.
struct ReactionGuard {
    rest: Option<crate::relay::RestClient>,
    ids: Vec<String>,
}

impl ReactionGuard {
    fn new(rest: crate::relay::RestClient, ids: Vec<String>) -> Self {
        Self {
            rest: if ids.is_empty() { None } else { Some(rest) },
            ids,
        }
    }
}

impl Drop for ReactionGuard {
    fn drop(&mut self) {
        // Guard against drop outside a tokio runtime (e.g., in unit tests or
        // during process teardown before the runtime is fully initialized).
        // `run_prompt_task` is always spawned via `JoinSet::spawn`, so a
        // runtime handle is normally available; `try_current` is the safe
        // fallback for the rare cases it isn't.
        if let Some(rest) = self.rest.take() {
            let ids = std::mem::take(&mut self.ids);
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(clear_reactions(rest, ids));
            }
            // If no runtime is available, reactions are left as-is — they are
            // cosmetic indicators and the stale state is harmless.
        }
    }
}

// Periodically emits a `turn_liveness` observer event while a turn is in-flight,
// so the desktop can prune turns whose host died without unwinding (kill -9 /
// crash) far sooner than the no-activity backstop. `run_prompt_task` runs it in
// a background task from `turn_started` until `LivenessGuard` drops, covering
// session setup as well as the final prompt call. When `interval` is zero,
// liveness is disabled and the future parks forever without emitting.
//
// `state` is the other half of `LivenessGuard`'s shutdown mutex (see its
// docs): held here across the check-then-emit, so a `LivenessGuard::drop`
// racing an in-flight tick either observes `state.closed == true` and skips
// the emit, or is blocked on the same lock until this tick's emit has
// already landed. Either way `turn_completed` cannot pass a live
// `turn_liveness` frame on the wire — the race is closed, not narrowed.
//
// `context`'s `session_id` starts `None` (liveness begins before session
// creation) and is filled in from `state.session_id` on each tick — set once
// by `run_prompt_task` after session resolution — so pings emitted for the
// remainder of the turn carry the real session, matching every other
// observer frame for this turn instead of a permanent `None`.
async fn run_turn_liveness(
    observer: Option<observer::ObserverHandle>,
    agent_index: Option<usize>,
    mut context: observer::ObserverContext,
    interval: Duration,
    state: Arc<Mutex<LivenessState>>,
) {
    let Some(observer) = observer else {
        return std::future::pending::<()>().await;
    };
    if interval.is_zero() {
        return std::future::pending::<()>().await;
    }
    let mut ticker = tokio::time::interval(interval);
    // The first tick completes immediately; skip it so the first liveness ping
    // fires one interval after the turn starts, not at t=0 (turn_started already
    // marks t=0).
    ticker.tick().await;
    loop {
        ticker.tick().await;
        // Nothing awaitable between the lock and the emit: `LivenessGuard::drop`
        // takes this same lock before its `abort()`, so the guard can only ever
        // observe this tick fully emitted or not yet started — never mid-emit.
        let guard = match state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if guard.closed {
            return;
        }
        context.session_id = guard.session_id.clone();
        observer.emit(
            "turn_liveness",
            agent_index,
            &context,
            serde_json::json!({}),
        );
        drop(guard);
    }
}

/// Shared shutdown/session state between `run_turn_liveness` and its
/// `LivenessGuard`. A single lock covers both fields so a tick's
/// check-session/emit and a guard's set-closed/abort can never interleave.
struct LivenessState {
    closed: bool,
    session_id: Option<String>,
}

/// Owns the background liveness task for one `run_prompt_task` invocation.
///
/// Dropping the guard aborts the non-resolving task, so liveness covers all
/// pre-prompt setup yet cannot survive a completed, cancelled, or panicked turn.
///
/// `abort()` alone leaves a race: tokio's cooperative cancellation only takes
/// effect at the next `.await` point inside the aborted task, so a tick that
/// has already passed its await and is mid-`observer.emit` when `drop` runs
/// can still complete that emit — a `turn_liveness` frame lands on the wire
/// after `turn_completed`, reviving a finished turn's badge for up to the
/// desktop's bounded prune-pause window. `state` shares a lock with
/// `run_turn_liveness`'s check-then-emit (see its docs): setting `closed`
/// and aborting under the same lock the emitter holds during its tick means
/// `drop` either sees the flag land before that tick's lock is taken (emit
/// skipped) or blocks until the in-flight emit under the lock has finished
/// (then aborts, so there is no next tick) — no interleaving emits a frame
/// after this guard has dropped.
struct LivenessGuard {
    handle: JoinHandle<()>,
    state: Arc<Mutex<LivenessState>>,
}

impl LivenessGuard {
    fn new(handle: JoinHandle<()>, state: Arc<Mutex<LivenessState>>) -> Self {
        Self { handle, state }
    }

    /// Record the turn's session ID once known, so subsequent liveness ticks
    /// stamp it on the emitted `turn_liveness` frame instead of `None`.
    fn set_session_id(&self, session_id: String) {
        let mut guard = match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.session_id = Some(session_id);
    }
}

impl Drop for LivenessGuard {
    fn drop(&mut self) {
        {
            let mut guard = match self.state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.closed = true;
        }
        self.handle.abort();
    }
}

// Emits a `turn_completed` observer event on drop, covering ALL exit paths
// (success, error, timeout, cancel, panic) from `run_prompt_task`. Captures
// observer handle and metadata at creation time so it remains valid even after
// the agent is moved into `PromptResult`.

struct TurnCompletionGuard {
    observer: Option<observer::ObserverHandle>,
    agent_index: Option<usize>,
    channel_id: Option<uuid::Uuid>,
    turn_id: String,
}

impl TurnCompletionGuard {
    fn new(
        observer: Option<observer::ObserverHandle>,
        agent_index: Option<usize>,
        channel_id: Option<uuid::Uuid>,
        turn_id: String,
    ) -> Self {
        Self {
            observer,
            agent_index,
            channel_id,
            turn_id,
        }
    }
}

impl Drop for TurnCompletionGuard {
    fn drop(&mut self) {
        if let Some(observer) = self.observer.take() {
            let context = observer::context_for(self.channel_id, None, Some(self.turn_id.clone()));
            observer.emit(
                "turn_completed",
                self.agent_index,
                &context,
                serde_json::json!({}),
            );
        }
    }
}

/// Map an ACP `StopReason` to the NIP-AM `StopReason` used in kind 44200 payloads.
fn acp_stop_to_core(r: &StopReason) -> buzz_core::agent_turn_metric::StopReason {
    use buzz_core::agent_turn_metric::StopReason as CoreStop;
    match r {
        StopReason::EndTurn => CoreStop::EndTurn,
        StopReason::Cancelled => CoreStop::Cancelled,
        StopReason::MaxTokens => CoreStop::MaxTokens,
        StopReason::MaxTurnRequests => CoreStop::Unknown,
        StopReason::Refusal => CoreStop::Unknown,
    }
}

/// Build the `(turn, cumulative)` `TokenCounts` pair for a NIP-AM kind-44200
/// payload from a completed `TurnUsage`.
///
/// Extracted as a pure function so the mapping logic can be tested independently
/// of relay/crypto infrastructure. `publish_agent_turn_metric` is the only
/// production caller.
///
/// - `turn` is `None` when `delta_reliable` is false; otherwise it carries the
///   per-turn i/o/total/cost deltas for this turn.
/// - `cumulative` always carries the session-aggregate i/o/cost totals.
///   `total_tokens` is `Some` only when the session accumulated a genuine
///   provider-reported total on every turn — never derived from i/o sums
///   (NIP-AM MUST NOT).
pub(crate) fn build_turn_metric_counts(
    usage: &crate::usage::TurnUsage,
) -> (
    Option<buzz_core::agent_turn_metric::TokenCounts>,
    Option<buzz_core::agent_turn_metric::TokenCounts>,
) {
    use buzz_core::agent_turn_metric::TokenCounts;

    let turn_counts = if usage.delta_reliable {
        Some(TokenCounts {
            input_tokens: usage.turn_input_tokens,
            output_tokens: usage.turn_output_tokens,
            // Field-local: present only when both the previous and current
            // cumulative totals were available and monotonic. Never derived
            // from input+output.
            total_tokens: usage.turn_total_tokens,
            cost_usd: usage.turn_cost_usd,
            // Field-local: present when the cumulative counter was monotonic
            // across this turn. Zero means no cache hits this turn (not absent).
            cache_read_tokens: usage.turn_cache_read_tokens,
            // Field-local: same contract as cache_read_tokens.
            cache_write_tokens: usage.turn_cache_write_tokens,
        })
    } else {
        // Defense-in-depth: UsageTracker already sets all turn_* fields to None
        // when delta_reliable is false, so the None arm here is technically
        // redundant. The explicit guard prevents a future refactor from
        // accidentally publishing unreliable per-turn counts.
        None
    };
    let cumulative_counts = Some(TokenCounts {
        input_tokens: usage.cumulative_input_tokens,
        output_tokens: usage.cumulative_output_tokens,
        // Present when every turn in the session reported a genuine provider
        // total. None when the session has never emitted one or any turn lacked
        // one. Never derived from input+output (NIP-AM MUST NOT).
        total_tokens: usage.cumulative_total_tokens,
        cost_usd: usage.cumulative_cost_usd,
        // Session-cumulative cache-read tokens; None when the harness never
        // reported this field (e.g. goose or older buzz-agent sessions).
        // Passes through directly — do not wrap in Some() as the field already
        // carries provenance (None vs Some(0) are distinct meanings).
        cache_read_tokens: usage.cumulative_cache_read_tokens,
        // Session-cumulative cache-write tokens; same provenance contract as
        // cache_read_tokens.
        cache_write_tokens: usage.cumulative_cache_write_tokens,
    });
    (turn_counts, cumulative_counts)
}

/// Best-effort: build and publish a `kind:44200` NIP-AM agent turn metric event.
///
/// Does nothing when `usage` is `None` (goose emitted no usage notification
/// for this turn) or when `owner_pubkey` is unconfigured (no NIP-AO identity).
/// Errors are logged at WARN and never surface to the caller — metric
/// publishing must never fail a turn.
async fn publish_agent_turn_metric(
    ctx: &PromptContext,
    usage: Option<crate::usage::TurnUsage>,
    channel_id: Option<uuid::Uuid>,
    session_id: &str,
    turn_id: &str,
    stop_reason: Option<buzz_core::agent_turn_metric::StopReason>,
) {
    use buzz_core::agent_turn_metric::AgentTurnMetricPayload;
    use nostr::{EventBuilder, Kind, Tag};

    let (usage, owner_pk) = match (usage, ctx.agent_owner_pubkey.as_ref()) {
        (Some(u), Some(pk)) => (u, pk),
        _ => return,
    };

    let (turn_counts, cumulative_counts) = build_turn_metric_counts(&usage);
    let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let payload = AgentTurnMetricPayload {
        harness: ctx.harness_name.clone(),
        model: usage.model.clone(),
        channel_id: channel_id.map(|id| id.to_string()),
        session_id: Some(usage.session_id.clone()),
        turn_id: Some(turn_id.to_string()),
        turn_seq: Some(usage.turn_seq),
        timestamp,
        turn: turn_counts,
        cumulative: cumulative_counts,
        delta_reliable: usage.delta_reliable,
        stop_reason,
        pricing_identity: usage.pricing_identity.clone(),
    };
    let ciphertext = match buzz_core::agent_turn_metric::encrypt_agent_turn_metric(
        &ctx.agent_keys,
        owner_pk,
        &payload,
    ) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                target: "pool::metrics",
                session_id,
                turn_id,
                "NIP-AM: encrypt failed: {e}"
            );
            return;
        }
    };
    let agent_hex = ctx.agent_keys.public_key().to_hex();
    let owner_hex = owner_pk.to_hex();
    let event = match EventBuilder::new(
        Kind::Custom(buzz_core::kind::KIND_AGENT_TURN_METRIC as u16),
        ciphertext,
    )
    .tags([
        Tag::parse(["p", &owner_hex]).expect("p tag"),
        Tag::parse(["agent", &agent_hex]).expect("agent tag"),
    ])
    .sign_with_keys(&ctx.agent_keys)
    {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(
                target: "pool::metrics",
                session_id,
                turn_id,
                "NIP-AM: sign failed: {e}"
            );
            return;
        }
    };
    const METRIC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
    match tokio::time::timeout(METRIC_TIMEOUT, ctx.rest_client.submit_event(&event)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::warn!(
            target: "pool::metrics",
            session_id,
            turn_id,
            "NIP-AM: publish failed: {e}"
        ),
        Err(_) => tracing::warn!(
            target: "pool::metrics",
            session_id,
            turn_id,
            "NIP-AM: publish timed out"
        ),
    }
}

const REACTION_SEEN: &str = "👀";
const REACTION_WORKING: &str = "💬";

/// Best-effort timeout for a single reaction REST call.
const REACTION_TIMEOUT: Duration = Duration::from_millis(500);

/// Percent-encode a string for use in a URL path segment (used in tests only).
#[cfg(test)]
fn pct_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                use std::fmt::Write;
                let _ = write!(out, "%{byte:02X}");
            }
        }
    }
    out
}

/// Best-effort: add a reaction via a signed Nostr kind-7 event (NIP-25).
///
/// Builds a reaction event with `buzz_sdk::build_reaction`, signs it with
/// the keys already stored in `RestClient`, and submits via `POST /events`.
/// Returns immediately on timeout or any error — reactions are cosmetic.
pub(crate) async fn reaction_add(rest: &crate::relay::RestClient, event_id: &str, emoji: &str) {
    let target_id = match nostr::EventId::from_hex(event_id) {
        Ok(id) => id,
        Err(e) => {
            tracing::debug!(event_id, emoji, "reaction add: invalid event ID: {e}");
            return;
        }
    };
    let builder = match buzz_sdk::build_reaction(target_id, emoji) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(event_id, emoji, "reaction add: build failed: {e}");
            return;
        }
    };
    let event = match builder.sign_with_keys(&rest.keys) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(event_id, emoji, "reaction add: sign failed: {e}");
            return;
        }
    };
    match tokio::time::timeout(REACTION_TIMEOUT, rest.submit_event(&event)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::debug!(event_id, emoji, "reaction add failed: {e}"),
        Err(_) => tracing::debug!(event_id, emoji, "reaction add timed out"),
    }
}

/// Best-effort: post a visible failure notice (kind:9) to a channel after a
/// batch is dead-lettered. Replies into the thread of `thread_tags` when the
/// triggering event was threaded. Errors are logged and swallowed — the
/// notice must never take down the main loop.
pub(crate) async fn post_failure_notice(
    rest: &crate::relay::RestClient,
    channel_id: Uuid,
    thread_tags: &ThreadTags,
    content: &str,
) {
    let thread_ref = thread_tags.root_event_id.as_deref().and_then(|root| {
        let root_id = nostr::EventId::from_hex(root).ok()?;
        let parent_id = thread_tags
            .parent_event_id
            .as_deref()
            .and_then(|p| nostr::EventId::from_hex(p).ok())
            .unwrap_or(root_id);
        Some(buzz_sdk::ThreadRef {
            root_event_id: root_id,
            parent_event_id: parent_id,
        })
    });
    let builder = match buzz_sdk::build_message(
        channel_id,
        content,
        thread_ref.as_ref(),
        &[],
        false,
        &[],
        &[],
    ) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(channel = %channel_id, "failure notice: build failed: {e}");
            return;
        }
    };
    let event = match builder.sign_with_keys(&rest.keys) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(channel = %channel_id, "failure notice: sign failed: {e}");
            return;
        }
    };
    match tokio::time::timeout(Duration::from_secs(5), rest.submit_event(&event)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::warn!(channel = %channel_id, "failure notice failed: {e}"),
        Err(_) => tracing::warn!(channel = %channel_id, "failure notice timed out"),
    }
}

/// Best-effort: remove a reaction via a signed kind:5 (NIP-09) deletion event.
///
/// Queries kind:7 reactions by our pubkey targeting the event, finds the matching
/// emoji, then submits a signed kind:5 deletion via `POST /events`.
/// Returns immediately on timeout or any error — reactions are cosmetic.
pub(crate) async fn reaction_remove(rest: &crate::relay::RestClient, event_id: &str, emoji: &str) {
    use nostr::{Alphabet, SingleLetterTag};

    // Step 1: query our kind:7 reactions targeting this event.
    let my_pubkey = rest.keys.public_key();
    let e_tag = SingleLetterTag::lowercase(Alphabet::E);
    let filter = nostr::Filter::new()
        .kind(nostr::Kind::Reaction)
        .author(my_pubkey)
        .custom_tags(e_tag, [event_id]);

    let resp = match tokio::time::timeout(Duration::from_millis(1_000), rest.query(&[filter])).await
    {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            tracing::debug!(event_id, emoji, "reaction remove: query failed: {e}");
            return;
        }
        Err(_) => {
            tracing::debug!(event_id, emoji, "reaction remove: query timed out");
            return;
        }
    };

    // Find our reaction event with matching emoji content.
    let reid = resp.as_array().and_then(|events| {
        events.iter().find_map(|ev| {
            let content = ev.get("content")?.as_str()?;
            if content != emoji {
                return None;
            }
            ev.get("id")?.as_str().map(|s| s.to_string())
        })
    });

    let reid = match reid {
        Some(id) => id,
        None => {
            tracing::debug!(event_id, emoji, "reaction remove: no reaction event found");
            return;
        }
    };

    // Step 2: build and submit a signed kind:5 deletion for the reaction event.
    let target_id = match nostr::EventId::from_hex(&reid) {
        Ok(id) => id,
        Err(e) => {
            tracing::debug!(
                event_id,
                emoji,
                "reaction remove: invalid reaction event ID: {e}"
            );
            return;
        }
    };
    let builder = match buzz_sdk::build_remove_reaction(target_id) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(event_id, emoji, "reaction remove: build failed: {e}");
            return;
        }
    };
    let event = match builder.sign_with_keys(&rest.keys) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(event_id, emoji, "reaction remove: sign failed: {e}");
            return;
        }
    };
    match tokio::time::timeout(Duration::from_millis(1_000), rest.submit_event(&event)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::debug!(event_id, emoji, "reaction remove failed: {e}"),
        Err(_) => tracing::debug!(event_id, emoji, "reaction remove timed out"),
    }
}

/// Maximum concurrent reaction HTTP requests per fan-out call.
/// Prevents unbounded parallelism when a large batch of events arrives.
const REACTION_CONCURRENCY: usize = 10;

/// Add 💬 to all events, capped at `REACTION_CONCURRENCY` concurrent requests.
/// Awaited inline before the prompt fires.
async fn react_working(rest: &crate::relay::RestClient, event_ids: &[String]) {
    for chunk in event_ids.chunks(REACTION_CONCURRENCY) {
        futures_util::future::join_all(
            chunk
                .iter()
                .map(|eid| reaction_add(rest, eid, REACTION_WORKING)),
        )
        .await;
    }
}

/// Fire-and-forget: remove both 👀 and 💬 from all events. Spawned on turn complete.
/// Capped at `REACTION_CONCURRENCY` concurrent requests per chunk to avoid
/// unbounded HTTP fan-out on large batches.
async fn clear_reactions(rest: crate::relay::RestClient, event_ids: Vec<String>) {
    // Each event needs two removals (👀 and 💬); pair them and chunk by
    // REACTION_CONCURRENCY pairs so the total concurrent requests stay bounded.
    for chunk in event_ids.chunks(REACTION_CONCURRENCY) {
        futures_util::future::join_all(chunk.iter().flat_map(|eid| {
            [
                reaction_remove(&rest, eid, REACTION_SEEN),
                reaction_remove(&rest, eid, REACTION_WORKING),
            ]
        }))
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, Tag, Timestamp};
    use serde_json::json;

    /// Conversation scope for a channel — the scope these pool tests exercise
    /// (equivalent to the pre-thread-scoping channel key).
    fn conv(channel_id: Uuid) -> SessionScope {
        SessionScope::Conversation { channel_id }
    }

    fn test_mcp_server() -> McpServer {
        McpServer::stdio("dev", "buzz-dev-mcp", vec![], vec![])
    }

    #[test]
    fn delivery_receipt_line_sorts_event_ids() {
        let channel_id = Uuid::nil();
        let event_ids = HashSet::from(["beta".to_string(), "alpha".to_string()]);

        assert_eq!(
            delivery_receipt_line(channel_id, &event_ids),
            format!("turn delivered Buzz events for channel {channel_id}: alpha,beta")
        );
    }

    // Permission-mode selection is provider-aware and fail-closed: Claude and
    // Codex use different native IDs for the requested unrestricted mode.
    #[test]
    fn agent_supports_mode_advertised_auto_is_true() {
        let session_new = json!({
            "modes": { "availableModes": [{ "id": "default" }, { "id": "auto" }] }
        });
        assert!(agent_supports_mode(
            &session_new,
            PermissionMode::Auto.as_wire_str()
        ));
    }

    #[test]
    fn agent_supports_mode_absent_auto_is_false() {
        let session_new = json!({
            "modes": { "availableModes": [{ "id": "default" }] }
        });
        assert!(!agent_supports_mode(
            &session_new,
            PermissionMode::Auto.as_wire_str()
        ));
    }

    #[test]
    fn agent_supports_mode_missing_modes_field_is_false() {
        let session_new = json!({ "sessionId": "sess-1" });
        assert!(!agent_supports_mode(
            &session_new,
            PermissionMode::Auto.as_wire_str()
        ));
    }

    #[test]
    fn unrestricted_mode_resolves_to_claude_native_id() {
        let session_new = json!({
            "modes": {"availableModes": [{"id": "default"}, {"id": "bypassPermissions"}]}
        });
        assert_eq!(
            resolve_permission_mode(&PermissionMode::BypassPermissions, &session_new).unwrap(),
            Some("bypassPermissions")
        );
    }

    #[test]
    fn unrestricted_mode_resolves_to_codex_full_access_and_updates_capture() {
        let session_new = json!({
            "modes": {
                "availableModes": [
                    {"id": "read-only"},
                    {"id": "agent"},
                    {"id": "agent-full-access"}
                ],
                "currentModeId": "agent"
            }
        });
        let resolved = resolve_permission_mode(&PermissionMode::BypassPermissions, &session_new)
            .unwrap()
            .unwrap();
        assert_eq!(resolved, "agent-full-access");
        let mut captured = session_new["modes"].clone();
        patch_session_mode_current_value(&mut captured, resolved);
        assert_eq!(captured["currentModeId"], "agent-full-access");
    }

    #[test]
    fn unsupported_permission_mode_fails_session_creation_visibly() {
        let session_new = json!({
            "modes": {"availableModes": [{"id": "default"}, {"id": "agent"}]}
        });
        let error = resolve_permission_mode(&PermissionMode::BypassPermissions, &session_new)
            .expect_err("unsupported mode must not silently fall back");
        assert!(error
            .to_string()
            .contains("advertised modes: default, agent"));
    }

    async fn spawn_permission_mode_acp(reply: &str) -> AcpClient {
        let script = format!(
            r#"IFS= read -r _request
printf '%s\n' '{{"jsonrpc":"2.0","id":0,{reply}}}'
while IFS= read -r _line; do :; done"#
        );
        AcpClient::spawn("bash", &["-c".to_string(), script], &[], false)
            .await
            .expect("spawn permission-mode ACP script")
    }

    #[tokio::test]
    async fn permission_mode_rpc_uses_codex_full_access_and_verifies_echo() {
        let reply = r#""result":{"configOptions":[{"id":"mode","category":"mode","currentValue":"agent-full-access"}]}"#;
        let mut acp = spawn_permission_mode_acp(reply).await;
        let observer = observer::ObserverHandle::in_process();
        acp.set_observer(Some(observer.clone()), 0);

        let session_new = json!({
            "modes": {"availableModes": [{"id": "agent-full-access"}]}
        });
        let (wire, applied) = apply_configured_permission_mode(
            &mut acp,
            "sess-1",
            &PermissionMode::BypassPermissions,
            &session_new,
            None,
        )
        .await
        .expect("matching effective mode must be accepted")
        .expect("ordinary session must apply its configured mode");
        assert_eq!(wire, "agent-full-access");
        assert!(applied.independently_verified);
        let request = observer
            .snapshot()
            .into_iter()
            .find(|event| {
                event.kind == "acp_write" && event.payload["method"] == "session/set_config_option"
            })
            .expect("mode request was sent");
        assert_eq!(request.payload["params"]["configId"], "mode");
        assert_eq!(request.payload["params"]["value"], "agent-full-access");
        acp.shutdown().await;
    }

    #[tokio::test]
    async fn immutable_job_policy_skips_configured_permission_mode_rpc() {
        let reply = r#""error":{"code":-32603,"message":"Internal error"}"#;
        let mut acp = spawn_permission_mode_acp(reply).await;
        let observer = observer::ObserverHandle::in_process();
        acp.set_observer(Some(observer.clone()), 0);
        let session_new = json!({
            "modes": {
                "availableModes": [{"id": "default"}, {"id": "bypassPermissions"}],
                "currentModeId": "default"
            }
        });
        let job_policy = JobSessionPolicy::new("a".repeat(64)).unwrap();

        let applied = apply_configured_permission_mode(
            &mut acp,
            "job-session",
            &PermissionMode::BypassPermissions,
            &session_new,
            Some(&job_policy),
        )
        .await
        .expect("the acknowledged immutable policy owns job permissions");

        assert!(applied.is_none());
        assert!(
            !observer
                .snapshot()
                .iter()
                .any(|event| event.kind == "acp_write"),
            "a JobPolicyV1 session must not receive a post-creation mode mutation"
        );
        acp.shutdown().await;
    }

    #[tokio::test]
    async fn permission_mode_rpc_rejects_explicit_effective_mode_mismatch() {
        let reply = r#""result":{"configOptions":[{"id":"mode","category":"mode","currentValue":"agent"}]}"#;
        let mut acp = spawn_permission_mode_acp(reply).await;
        let error = apply_permission_mode(&mut acp, "sess-1", "agent-full-access")
            .await
            .expect_err("an explicit adapter mismatch must fail session creation");
        assert!(error
            .to_string()
            .contains("reported effective mode \"agent\""));
        acp.shutdown().await;
    }

    #[tokio::test]
    async fn permission_mode_rpc_rejection_is_fatal() {
        let reply = r#""error":{"code":-32602,"message":"mode rejected"}"#;
        let mut acp = spawn_permission_mode_acp(reply).await;
        let error = apply_permission_mode(&mut acp, "sess-1", "agent-full-access")
            .await
            .expect_err("a rejected mode must not silently use adapter defaults");
        assert!(error.to_string().contains("mode rejected"));
        acp.shutdown().await;
    }

    #[test]
    fn empty_mode_response_is_accepted_but_not_independently_verified() {
        assert!(!verify_permission_mode_response(&json!({}), "bypassPermissions").unwrap());
    }

    #[test]
    fn job_session_never_receives_configured_initial_message() {
        let job = PromptSource::Channel(SessionScope::Job {
            channel_id: Uuid::new_v4(),
            operation_id: Uuid::new_v4().to_string(),
            request_event_id: "a".repeat(64),
        });
        let chat = PromptSource::Channel(conv(Uuid::new_v4()));
        assert!(!should_send_initial_message(&job, true));
        assert!(should_send_initial_message(&chat, true));
        assert!(!should_send_initial_message(&chat, false));
    }

    #[test]
    fn public_session_forwards_channel_origin_to_mcp() {
        let channel_id = Uuid::new_v4();
        let scope = SessionScope::Conversation { channel_id };
        let servers =
            mcp_servers_with_git_origin(&[test_mcp_server()], Some(&scope), Some("stream"), None);
        assert!(servers[0].stdio_env().iter().any(|entry| {
            entry.name == "BUZZ_GIT_ORIGIN_CHANNEL_ID" && entry.value == channel_id.to_string()
        }));
        assert!(!servers[0]
            .stdio_env()
            .iter()
            .any(|entry| entry.name == "BUZZ_GIT_ORIGIN_AGENT_NAME"));
    }

    #[test]
    fn private_session_forwards_agent_name_without_channel_id() {
        let scope = SessionScope::Conversation {
            channel_id: Uuid::new_v4(),
        };
        let servers = mcp_servers_with_git_origin(
            &[test_mcp_server()],
            Some(&scope),
            Some("dm"),
            Some("Builder"),
        );
        assert!(servers[0].stdio_env().iter().any(|entry| {
            entry.name == "BUZZ_GIT_ORIGIN_AGENT_NAME" && entry.value == "Builder"
        }));
        assert!(!servers[0]
            .stdio_env()
            .iter()
            .any(|entry| entry.name == "BUZZ_GIT_ORIGIN_CHANNEL_ID"));
    }

    #[test]
    fn job_scope_is_not_exposed_to_generic_stdio_mcp() {
        let scope = SessionScope::Job {
            channel_id: Uuid::new_v4(),
            operation_id: Uuid::new_v4().to_string(),
            request_event_id: "a".repeat(64),
        };
        let servers =
            mcp_servers_for_scope(&[test_mcp_server()], Some(&scope), Some("stream"), None);
        assert!(
            servers.is_empty(),
            "a Job must not inherit generic stdio or ambient MCP servers"
        );
    }

    // These pin the initial_message dispatch path (run_prompt_task, ~line 855):
    // a legacy agent WITH a base_prompt must get <base> prepended to the user
    // message. This is the exact regression that shipped in the round-2 bug.

    fn base_only(base_prompt: Option<&str>) -> crate::queue::StandingContext<'_> {
        crate::queue::StandingContext {
            base_prompt,
            ..Default::default()
        }
    }

    #[test]
    fn test_initial_message_legacy_agent_gets_base_prepended() {
        // protocol_version 1 + Some(base_prompt): <base> rides along in the
        // user message.
        let composed = prepend_standing_for_legacy(
            1,
            &base_only(Some("you are a helpful agent")),
            "hello channel",
        );
        assert_eq!(
            composed,
            "<base>\nyou are a helpful agent\n</base>\n\nhello channel"
        );
    }

    #[test]
    fn test_initial_message_modern_agent_omits_base() {
        // protocol_version 2 receives base_prompt via session/new, so the user
        // message is left untouched even when a base_prompt is present.
        let composed = prepend_standing_for_legacy(
            2,
            &base_only(Some("you are a helpful agent")),
            "hello channel",
        );
        assert_eq!(composed, "hello channel");
    }

    #[test]
    fn test_heartbeat_standing_block_includes_workspace_instructions() {
        let standing = crate::queue::StandingContext {
            base_prompt: Some("be helpful"),
            system_prompt: Some("agent persona"),
            team_instructions: Some("NEMO-A2A-1 workspace policy"),
            ..Default::default()
        };
        let composed = prepend_standing_for_legacy(1, &standing, "tick");
        assert!(composed.contains("<base>\nbe helpful\n</base>"));
        assert!(composed.contains("<system>\nagent persona\n</system>"));
        assert!(composed.contains("NEMO-A2A-1 workspace policy"));
        assert!(composed.ends_with("tick"));
    }

    #[test]
    fn goose_uses_system_prompt_only_after_custom_method_succeeds() {
        assert!(!has_system_prompt_support(2, "goose", None, false));
        assert!(!has_system_prompt_support(2, "goose", Some(false), false));
        assert!(has_system_prompt_support(2, "goose", Some(true), false));
        assert!(has_system_prompt_support(1, "goose", Some(true), false));
        assert!(has_system_prompt_support(2, "buzz-agent", None, false));
        // Goose never receives system prompt via session/new (uses post-hoc method).
        assert_eq!(
            session_new_system_prompt(true, 2, "goose", false, Some("instructions")),
            None
        );
        // Protocol-v2 non-goose gets Field transport.
        assert_eq!(
            session_new_system_prompt(false, 2, "buzz-agent", false, Some("instructions")),
            Some(SystemPromptTransport::Field("instructions"))
        );
        // Protocol-v1 non-goose, non-claude gets None (legacy user-message framing).
        assert_eq!(
            session_new_system_prompt(false, 1, "codex", false, Some("instructions")),
            None
        );
        assert_eq!(
            session_new_system_prompt(false, 1, "codex", true, Some("instructions")),
            Some(SystemPromptTransport::MetaAppend("instructions"))
        );
        // claude-agent-acp gets MetaAppend transport regardless of protocol version.
        assert_eq!(
            session_new_system_prompt(false, 1, CLAUDE_AGENT_ACP_NAME, false, Some("instructions")),
            Some(SystemPromptTransport::MetaAppend("instructions"))
        );
        assert_eq!(
            session_new_system_prompt(true, 1, CLAUDE_AGENT_ACP_NAME, false, Some("instructions")),
            None,
            "goose path must never produce a transport even when agent_name matches"
        );
    }

    #[test]
    fn claude_agent_acp_has_system_prompt_support_regardless_of_protocol_version() {
        // claude-agent-acp declares protocolVersion:1 but supports _meta.systemPrompt;
        // has_system_prompt_support must return true so user-message framing is suppressed.
        assert!(has_system_prompt_support(
            1,
            CLAUDE_AGENT_ACP_NAME,
            None,
            false
        ));
        assert!(has_system_prompt_support(
            2,
            CLAUDE_AGENT_ACP_NAME,
            None,
            false
        ));
    }

    #[test]
    fn old_zed_adapter_name_falls_through_to_protocol_version_gate() {
        // The renamed @zed-industries package predates the _meta.systemPrompt support,
        // so it must not be treated as capable and stays on legacy user-message framing.
        let old_name = "@zed-industries/claude-code-acp";
        assert!(!has_system_prompt_support(1, old_name, None, false));
        assert!(has_system_prompt_support(2, old_name, None, false));
    }

    #[test]
    fn test_initial_message_legacy_agent_without_base_is_unchanged() {
        // No base_prompt configured: nothing to prepend regardless of version.
        let composed = prepend_standing_for_legacy(1, &base_only(None), "hello channel");
        assert_eq!(composed, "hello channel");
    }

    // ── prepend_standing_for_legacy ───────────────────────────────────────────

    fn full_standing() -> crate::queue::StandingContext<'static> {
        crate::queue::StandingContext {
            leading_project_instructions: None,
            base_prompt: Some("be helpful"),
            system_prompt: Some("you are Eva"),
            team_instructions: Some("ship small"),
            agent_core: Some("[Agent Memory — core]\nremember this"),
            huddle_instructions: Some("reply immediately"),
            agent_canvas: Some("[Channel Canvas]\ncanvas content"),
        }
    }

    #[test]
    fn test_initial_message_legacy_agent_gets_whole_standing_block() {
        // The initial message is the legacy agent's first contact, so it must
        // carry every standing section — not just <base> and the canvas, which
        // left the agent acting on its first turn with no persona and no memory.
        let composed = prepend_standing_for_legacy(1, &full_standing(), "do the thing");
        let positions: Vec<usize> = [
            "<base>",
            "<system>",
            "<team-instructions>",
            "<core-memory>",
            "<huddle-instructions>",
            "<channel-canvas>",
            "do the thing",
        ]
        .iter()
        .map(|needle| {
            composed
                .find(needle)
                .unwrap_or_else(|| panic!("missing {needle} in: {composed}"))
        })
        .collect();
        assert!(
            positions.windows(2).all(|w| w[0] < w[1]),
            "sections must match the per-turn order, body last; got: {composed}"
        );
    }

    #[test]
    fn test_initial_message_standing_order_matches_per_turn_order() {
        // Both legacy paths render through StandingContext, so the initial
        // message and a first-turn prompt agree section-for-section.
        let standing = full_standing();
        let composed = prepend_standing_for_legacy(1, &standing, "do the thing");
        assert_eq!(
            composed,
            format!("{}\n\ndo the thing", standing.sections().join("\n\n"))
        );
    }

    #[test]
    fn test_initial_message_modern_agent_omits_standing_block() {
        // Protocol-v2 agents hold all of this from session/new; repeating it in
        // the initial-message user turn would double-render every section.
        let composed = prepend_standing_for_legacy(2, &full_standing(), "do the thing");
        assert_eq!(composed, "do the thing");
    }

    #[test]
    fn test_initial_message_legacy_agent_without_standing_is_unchanged() {
        // Nothing configured: body passes through with no stray blank lines.
        let composed =
            prepend_standing_for_legacy(1, &crate::queue::StandingContext::default(), "do it");
        assert_eq!(composed, "do it");
    }

    // Pin the session/new systemPrompt framing: each present prompt carries its
    // own paired tag so the desktop observer can split labeled sub-sections.

    #[test]
    fn test_framed_system_prompt_both_present_carries_both_headers() {
        // Also the regression guard against #2372: the session title travels
        // out of band in `_meta.sessionTitle`, so this exact-bytes assertion is
        // what pins the framing against a `[Session]` section reappearing here.
        let framed = framed_system_prompt("/workspace", Some("base text"), Some("persona text"))
            .expect("both present yields Some");
        assert_eq!(
            framed,
            "<base>\nbase text\n</base>\n\n<workspace>\nCurrent working directory: /workspace\n</workspace>\n\n<system>\npersona text\n</system>"
        );
    }

    #[test]
    fn test_framed_system_prompt_base_only_labels_base() {
        let framed =
            framed_system_prompt("/workspace", Some("base text"), None).expect("base yields Some");
        assert_eq!(
            framed,
            "<base>\nbase text\n</base>\n\n<workspace>\nCurrent working directory: /workspace\n</workspace>"
        );
    }

    #[test]
    fn test_framed_system_prompt_persona_only_labels_agent_instructions() {
        // A bare persona would be mislabeled "Base" downstream — it must carry
        // its own <system> boundary even when no base prompt exists.
        let framed = framed_system_prompt("/workspace", None, Some("persona text"))
            .expect("persona yields Some");
        assert_eq!(framed, "<system>\npersona text\n</system>");
    }

    #[test]
    fn test_framed_system_prompt_preserves_persona_bytes_verbatim() {
        let persona = "literal </system>, <T>, &quot;, & <policy>";
        let framed =
            framed_system_prompt("/workspace", None, Some(persona)).expect("persona yields Some");
        assert_eq!(framed, format!("<system>\n{persona}\n</system>"));
    }

    #[test]
    fn test_framed_system_prompt_neither_is_none() {
        assert!(framed_system_prompt("/workspace", None, None).is_none());
    }

    #[test]
    fn test_workspace_section_preserves_windows_cwd() {
        assert_eq!(
            workspace_section(r"C:\Users\me\buzz"),
            "<workspace>\nCurrent working directory: C:\\Users\\me\\buzz\n</workspace>"
        );
    }

    #[test]
    fn test_with_core_appends_below_framed() {
        let framed = with_core(
            Some("[Agent Instructions]\npersona".to_string()),
            Some("[Agent Memory — core]\nbe helpful"),
        )
        .expect("both present yields Some");
        assert_eq!(
            framed,
            "[Agent Instructions]\npersona\n\n<core-memory>\nbe helpful\n</core-memory>"
        );
    }

    #[test]
    fn test_with_core_framed_only_passes_through() {
        let framed = with_core(Some("[Agent Instructions]\npersona".to_string()), None)
            .expect("framed-only yields Some");
        assert_eq!(framed, "[Agent Instructions]\npersona");
    }

    #[test]
    fn test_with_core_core_only_is_just_core() {
        let framed = with_core(None, Some("[Agent Memory — core]\nbe helpful"))
            .expect("core-only yields Some");
        assert_eq!(framed, "<core-memory>\nbe helpful\n</core-memory>");
    }

    #[test]
    fn test_with_core_neither_is_none() {
        assert!(with_core(None, None).is_none());
    }

    #[test]
    fn test_parse_thread_response_basic() {
        let json = json!({
            "root": {
                "event_id": "abc123",
                "pubkey": "pub1",
                "content": "root message",
                "created_at": 1710518400
            },
            "replies": [
                {
                    "event_id": "def456",
                    "pubkey": "pub2",
                    "content": "first reply",
                    "created_at": 1710518460
                }
            ],
            "total_replies": 1
        });

        let ctx = parse_thread_response(json).expect("should parse");
        match ctx {
            ConversationContext::Thread {
                messages,
                total,
                root_present,
                truncated,
            } => {
                assert_eq!(messages.len(), 2); // root + 1 reply
                assert_eq!(total, 2); // 1 reply + 1 root
                assert!(!truncated);
                assert!(root_present);
                assert_eq!(messages[0].content, "root message");
                assert_eq!(messages[1].content, "first reply");
            }
            _ => panic!("expected Thread context"),
        }
    }

    #[test]
    fn test_parse_thread_response_truncated() {
        let json = json!({
            "root": {
                "event_id": "abc",
                "pubkey": "pub1",
                "content": "root",
                "created_at": 1710518400
            },
            "replies": [
                {
                    "event_id": "def",
                    "pubkey": "pub2",
                    "content": "reply1",
                    "created_at": 1710518460
                }
            ],
            "total_replies": 10
        });

        let ctx = parse_thread_response(json).expect("should parse");
        match ctx {
            ConversationContext::Thread {
                messages,
                total,
                root_present,
                truncated,
            } => {
                assert_eq!(messages.len(), 2);
                assert_eq!(total, 11); // 10 replies + 1 root
                assert!(truncated);
                assert!(root_present);
            }
            _ => panic!("expected Thread context"),
        }
    }

    #[test]
    fn test_parse_thread_response_empty() {
        let json = json!({
            "root": null,
            "replies": [],
            "total_replies": 0
        });
        assert!(parse_thread_response(json).is_none());
    }

    #[test]
    fn test_parse_thread_response_missing_fields() {
        // Malformed JSON — no root, no replies key.
        let json = json!({ "something": "else" });
        assert!(parse_thread_response(json).is_none());
    }

    #[test]
    fn test_parse_dm_response_basic() {
        let json = json!({
            "messages": [
                {
                    "event_id": "msg2",
                    "pubkey": "pub2",
                    "content": "newer message",
                    "created_at": 1710518500
                },
                {
                    "event_id": "msg1",
                    "pubkey": "pub1",
                    "content": "older message",
                    "created_at": 1710518400
                }
            ],
            "next_cursor": null
        });

        // limit=12 > 2 messages → not truncated.
        let ctx = parse_dm_response(json, 12).expect("should parse");
        match ctx {
            ConversationContext::Dm {
                messages,
                total,
                truncated,
            } => {
                // Should be reversed to chronological order.
                assert_eq!(messages.len(), 2);
                assert_eq!(messages[0].content, "older message");
                assert_eq!(messages[1].content, "newer message");
                assert!(!truncated);
                assert_eq!(total, 2);
            }
            _ => panic!("expected Dm context"),
        }
    }

    #[test]
    fn test_parse_dm_response_truncated() {
        let json = json!({
            "messages": [
                {
                    "event_id": "msg1",
                    "pubkey": "pub1",
                    "content": "message",
                    "created_at": 1710518400
                }
            ],
            "next_cursor": "00000000660f5a80"
        });

        // limit=1 == 1 message → truncated.
        let ctx = parse_dm_response(json, 1).expect("should parse");
        match ctx {
            ConversationContext::Dm {
                truncated, total, ..
            } => {
                assert!(truncated);
                assert_eq!(total, 2); // 1 message + indicator
            }
            _ => panic!("expected Dm context"),
        }
    }

    #[test]
    fn test_parse_dm_response_not_truncated_despite_cursor() {
        // Relay always sets next_cursor when page is non-empty, but if
        // returned count < limit, the page is complete.
        let json = json!({
            "messages": [
                {
                    "event_id": "msg1",
                    "pubkey": "pub1",
                    "content": "only message",
                    "created_at": 1710518400
                }
            ],
            "next_cursor": "00000000660f5a80"
        });

        // limit=12 > 1 message → NOT truncated despite next_cursor being set.
        let ctx = parse_dm_response(json, 12).expect("should parse");
        match ctx {
            ConversationContext::Dm {
                truncated, total, ..
            } => {
                assert!(!truncated, "should not be truncated when count < limit");
                assert_eq!(total, 1);
            }
            _ => panic!("expected Dm context"),
        }
    }

    #[test]
    fn test_parse_dm_response_empty() {
        let json = json!({
            "messages": [],
            "next_cursor": null
        });
        assert!(parse_dm_response(json, 12).is_none());
    }

    #[test]
    fn test_parse_dm_response_missing_messages_key() {
        let json = json!({ "data": [] });
        assert!(parse_dm_response(json, 12).is_none());
    }

    #[test]
    fn test_parse_nostr_thread_response_marks_query_window_truncated() {
        let agent = Keys::generate();
        let root_id = "1111111111111111111111111111111111111111111111111111111111111111";
        let agent_hex = agent.public_key().to_hex();
        let json = json!([
            {
                "id": root_id,
                "pubkey": "rootpub",
                "content": "root",
                "created_at": 1000
            },
            {
                "id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "pubkey": agent_hex,
                "content": "newest agent reply",
                "created_at": 4000
            },
            {
                "id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "pubkey": "humanpub",
                "content": "middle reply",
                "created_at": 3000
            },
            {
                "id": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "pubkey": "oldpub",
                "content": "sentinel omitted reply",
                "created_at": 2000
            }
        ]);

        let ctx = parse_nostr_thread_response(json, root_id, 2, &agent.public_key())
            .expect("should parse");
        match ctx {
            ConversationContext::Thread {
                messages,
                total,
                root_present,
                truncated,
            } => {
                assert_eq!(messages.len(), 3); // root + 2 displayed replies
                assert_eq!(total, 4); // root + displayed replies + sentinel
                assert!(truncated);
                assert!(root_present);
                assert_eq!(messages[0].content, "root");
                assert_eq!(messages[1].content, "middle reply");
                assert_eq!(messages[2].content, "newest agent reply");
                assert!(messages
                    .iter()
                    .all(|msg| msg.content != "sentinel omitted reply"));
            }
            _ => panic!("expected Thread context"),
        }
    }

    #[test]
    fn test_parse_nostr_thread_response_not_truncated_below_limit() {
        let agent = Keys::generate();
        let root_id = "1111111111111111111111111111111111111111111111111111111111111111";
        let json = json!([
            {
                "id": root_id,
                "pubkey": "rootpub",
                "content": "root",
                "created_at": 1000
            },
            {
                "id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "pubkey": "replypub",
                "content": "reply",
                "created_at": 2000
            }
        ]);

        let ctx = parse_nostr_thread_response(json, root_id, 2, &agent.public_key())
            .expect("should parse");
        match ctx {
            ConversationContext::Thread {
                messages,
                total,
                root_present,
                truncated,
            } => {
                assert_eq!(messages.len(), 2);
                assert_eq!(total, 2);
                assert!(!truncated);
                assert!(root_present);
            }
            _ => panic!("expected Thread context"),
        }
    }

    #[test]
    fn test_parse_nostr_thread_response_marks_missing_root_incomplete() {
        let agent = Keys::generate();
        let root_id = "1111111111111111111111111111111111111111111111111111111111111111";
        let json = json!([
            {
                "id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "pubkey": "replypub1",
                "content": "first reply",
                "created_at": 2000
            },
            {
                "id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "pubkey": "replypub2",
                "content": "second reply",
                "created_at": 3000
            }
        ]);

        let ctx = parse_nostr_thread_response(json, root_id, 12, &agent.public_key())
            .expect("reply context should still be available");
        match ctx {
            ConversationContext::Thread {
                messages,
                total,
                root_present,
                truncated,
            } => {
                assert_eq!(messages.len(), 2);
                assert_eq!(total, 2);
                assert!(!truncated);
                assert!(!root_present);
            }
            _ => panic!("expected Thread context"),
        }
    }

    #[test]
    fn test_parse_nostr_thread_response_keeps_agent_reply_outside_recent_window() {
        let agent = Keys::generate();
        let root_id = "1111111111111111111111111111111111111111111111111111111111111111";
        let agent_hex = agent.public_key().to_hex();
        let json = json!([
            {
                "id": root_id,
                "pubkey": "rootpub",
                "content": "root",
                "created_at": 1000
            },
            {
                "id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "pubkey": "humanpub",
                "content": "newer human reply",
                "created_at": 5000
            },
            {
                "id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "pubkey": "humanpub",
                "content": "middle human reply",
                "created_at": 4000
            },
            {
                "id": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "pubkey": "humanpub",
                "content": "oldest displayed reply without agent pin",
                "created_at": 3000
            },
            {
                "id": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                "pubkey": agent_hex,
                "content": "agent reply outside recent window",
                "created_at": 2000
            }
        ]);

        let ctx = parse_nostr_thread_response(json, root_id, 2, &agent.public_key())
            .expect("should parse");
        match ctx {
            ConversationContext::Thread { messages, .. } => {
                assert_eq!(messages.len(), 3); // root + 2 displayed replies
                assert_eq!(messages[0].content, "root");
                assert!(messages
                    .iter()
                    .any(|msg| msg.content == "agent reply outside recent window"));
                assert!(messages
                    .iter()
                    .any(|msg| msg.content == "newer human reply"));
                assert!(messages
                    .iter()
                    .all(|msg| msg.content != "middle human reply"));
                assert!(messages
                    .iter()
                    .all(|msg| msg.content != "oldest displayed reply without agent pin"));
            }
            _ => panic!("expected Thread context"),
        }
    }

    #[tokio::test]
    async fn test_fetch_thread_context_uses_exact_count_when_above_sentinel_minimum() {
        let agent = Keys::generate();
        let root_id = "1111111111111111111111111111111111111111111111111111111111111111";
        let channel_id = Uuid::new_v4();
        let agent_pubkey = agent.public_key();
        let json = json!([
            thread_event(root_id, "rootpub", "root", 1000),
            thread_event(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "humanpub",
                "newest reply",
                4000
            ),
            thread_event(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "humanpub",
                "middle reply",
                3000
            ),
            thread_event(
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "humanpub",
                "sentinel reply",
                2000
            )
        ]);

        let ctx = fetch_thread_context_with(
            channel_id,
            root_id,
            2,
            agent_pubkey,
            move |filters| {
                assert_thread_query_filters(&filters, channel_id, root_id, agent_pubkey, 3);
                std::future::ready(Ok(json.clone()))
            },
            move |filters| {
                assert_thread_count_filter(&filters, channel_id, root_id);
                std::future::ready(Ok(json!({ "count": 6 })))
            },
        )
        .await
        .expect("thread context");

        match ctx {
            ConversationContext::Thread {
                messages,
                total,
                truncated,
                ..
            } => {
                assert!(truncated);
                assert_eq!(messages.len(), 3);
                assert_eq!(total, 7); // 6 replies + root
            }
            _ => panic!("expected Thread context"),
        }
    }

    #[tokio::test]
    async fn test_fetch_thread_context_does_not_add_missing_root_to_exact_count() {
        let agent = Keys::generate();
        let root_id = "1111111111111111111111111111111111111111111111111111111111111111";
        let channel_id = Uuid::new_v4();
        let json = json!([
            thread_event(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "humanpub",
                "newest reply",
                4000
            ),
            thread_event(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "humanpub",
                "middle reply",
                3000
            ),
            thread_event(
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "humanpub",
                "sentinel reply",
                2000
            )
        ]);

        let ctx = fetch_thread_context_with(
            channel_id,
            root_id,
            2,
            agent.public_key(),
            move |_filters| std::future::ready(Ok(json.clone())),
            |_filters| std::future::ready(Ok(json!({ "count": 6 }))),
        )
        .await
        .expect("thread context");

        match ctx {
            ConversationContext::Thread {
                messages,
                total,
                root_present,
                truncated,
            } => {
                assert!(truncated);
                assert_eq!(messages.len(), 2);
                assert_eq!(total, 6);
                assert!(!root_present);
            }
            _ => panic!("expected Thread context"),
        }
    }

    #[tokio::test]
    async fn test_fetch_thread_context_clamps_count_below_sentinel_minimum() {
        let agent = Keys::generate();
        let root_id = "1111111111111111111111111111111111111111111111111111111111111111";
        let channel_id = Uuid::new_v4();
        let json = json!([
            thread_event(root_id, "rootpub", "root", 1000),
            thread_event(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "humanpub",
                "newest reply",
                4000
            ),
            thread_event(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "humanpub",
                "middle reply",
                3000
            ),
            thread_event(
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "humanpub",
                "sentinel reply",
                2000
            )
        ]);

        let ctx = fetch_thread_context_with(
            channel_id,
            root_id,
            2,
            agent.public_key(),
            move |_filters| std::future::ready(Ok(json.clone())),
            |_filters| std::future::ready(Ok(json!({ "count": 1 }))),
        )
        .await
        .expect("thread context");

        match ctx {
            ConversationContext::Thread {
                messages,
                total,
                truncated,
                ..
            } => {
                assert!(truncated);
                assert_eq!(messages.len(), 3);
                assert_eq!(total, 4); // root + displayed replies + sentinel minimum
            }
            _ => panic!("expected Thread context"),
        }
    }

    #[tokio::test]
    async fn test_fetch_thread_context_preserves_sentinel_minimum_when_count_fails() {
        let agent = Keys::generate();
        let root_id = "1111111111111111111111111111111111111111111111111111111111111111";
        let channel_id = Uuid::new_v4();
        let json = json!([
            thread_event(root_id, "rootpub", "root", 1000),
            thread_event(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "humanpub",
                "newest reply",
                4000
            ),
            thread_event(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "humanpub",
                "middle reply",
                3000
            ),
            thread_event(
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "humanpub",
                "sentinel reply",
                2000
            )
        ]);

        let ctx = fetch_thread_context_with(
            channel_id,
            root_id,
            2,
            agent.public_key(),
            move |_filters| std::future::ready(Ok(json.clone())),
            |_filters| std::future::ready(Err(crate::relay::RelayError::Http("boom".into()))),
        )
        .await
        .expect("thread context");

        match ctx {
            ConversationContext::Thread {
                messages,
                total,
                truncated,
                ..
            } => {
                assert!(truncated);
                assert_eq!(messages.len(), 3);
                assert_eq!(total, 4); // count failure leaves parser's sentinel minimum intact
            }
            _ => panic!("expected Thread context"),
        }
    }

    #[tokio::test]
    async fn test_fetch_thread_context_deduplicates_and_pins_agent_reply() {
        let agent = Keys::generate();
        let agent_hex = agent.public_key().to_hex();
        let root_id = "1111111111111111111111111111111111111111111111111111111111111111";
        let channel_id = Uuid::new_v4();
        let json = json!([
            thread_event(root_id, "rootpub", "root", 1000),
            thread_event(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "humanpub",
                "newer human reply",
                5000
            ),
            thread_event(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "humanpub",
                "middle human reply",
                4000
            ),
            thread_event(
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                &agent_hex,
                "agent reply outside recent window",
                2000
            ),
            // Same event as the separately fetched author-filtered result; the
            // parser should deduplicate it before pinning.
            thread_event(
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                &agent_hex,
                "agent reply outside recent window",
                2000
            )
        ]);

        let ctx = fetch_thread_context_with(
            channel_id,
            root_id,
            2,
            agent.public_key(),
            move |_filters| std::future::ready(Ok(json.clone())),
            |_filters| std::future::ready(Ok(json!({ "count": 3 }))),
        )
        .await
        .expect("thread context");

        match ctx {
            ConversationContext::Thread {
                messages,
                total,
                truncated,
                ..
            } => {
                assert!(truncated);
                assert_eq!(total, 4);
                assert_eq!(messages.len(), 3);
                assert_eq!(
                    messages
                        .iter()
                        .filter(|msg| msg.content == "agent reply outside recent window")
                        .count(),
                    1,
                    "separate agent-reply query must not duplicate the same event"
                );
                assert!(messages
                    .iter()
                    .any(|msg| msg.content == "newer human reply"));
                assert!(messages
                    .iter()
                    .all(|msg| msg.content != "middle human reply"));
            }
            _ => panic!("expected Thread context"),
        }
    }

    #[tokio::test]
    async fn test_fetch_thread_context_uses_distinct_fetched_replies_as_minimum() {
        let agent = Keys::generate();
        let agent_hex = agent.public_key().to_hex();
        let root_id = "1111111111111111111111111111111111111111111111111111111111111111";
        let channel_id = Uuid::new_v4();
        let json = json!([
            thread_event(root_id, "rootpub", "root", 1000),
            thread_event(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "humanpub",
                "newest human reply",
                5000
            ),
            thread_event(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "humanpub",
                "middle human reply",
                4000
            ),
            thread_event(
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "humanpub",
                "sentinel human reply",
                3000
            ),
            thread_event(
                "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                &agent_hex,
                "older distinct agent reply",
                2000
            )
        ]);

        let ctx = fetch_thread_context_with(
            channel_id,
            root_id,
            2,
            agent.public_key(),
            move |_filters| std::future::ready(Ok(json.clone())),
            |_filters| std::future::ready(Err(crate::relay::RelayError::Http("boom".into()))),
        )
        .await
        .expect("thread context");

        match ctx {
            ConversationContext::Thread {
                messages,
                total,
                truncated,
                ..
            } => {
                assert!(truncated);
                assert_eq!(messages.len(), 3);
                assert_eq!(
                    total, 5,
                    "root plus all four distinct fetched replies prove the lower bound"
                );
                assert!(messages
                    .iter()
                    .any(|msg| msg.content == "older distinct agent reply"));
                assert!(messages
                    .iter()
                    .any(|msg| msg.content == "newest human reply"));
                assert!(messages
                    .iter()
                    .all(|msg| msg.content != "middle human reply"));
                assert!(messages
                    .iter()
                    .all(|msg| msg.content != "sentinel human reply"));
            }
            _ => panic!("expected Thread context"),
        }
    }

    fn assert_thread_query_filters(
        filters: &[nostr::Filter],
        channel_id: Uuid,
        root_id: &str,
        agent_pubkey: nostr::PublicKey,
        reply_limit: u64,
    ) {
        assert_eq!(
            filters.len(),
            3,
            "root, recent replies, and agent reply filters"
        );

        let root = serde_json::to_value(&filters[0]).expect("serialize root filter");
        assert_eq!(root.get("ids"), Some(&json!([root_id])));
        assert!(root.get("limit").is_none());

        let replies = serde_json::to_value(&filters[1]).expect("serialize replies filter");
        assert_eq!(
            replies.get("kinds"),
            Some(&json!([9, 40002, 43002, 43003, 43004, 43005, 43006]))
        );
        assert_eq!(replies.get("#e"), Some(&json!([root_id])));
        assert_eq!(replies.get("#h"), Some(&json!([channel_id.to_string()])));
        assert_eq!(replies.get("limit"), Some(&json!(reply_limit)));
        assert!(replies.get("authors").is_none());

        let agent = serde_json::to_value(&filters[2]).expect("serialize agent filter");
        assert_eq!(
            agent.get("kinds"),
            Some(&json!([9, 40002, 43002, 43003, 43004, 43005, 43006]))
        );
        assert_eq!(agent.get("#e"), Some(&json!([root_id])));
        assert_eq!(agent.get("#h"), Some(&json!([channel_id.to_string()])));
        assert_eq!(agent.get("authors"), Some(&json!([agent_pubkey.to_hex()])));
        assert_eq!(agent.get("limit"), Some(&json!(1)));
    }

    fn assert_thread_count_filter(filters: &[nostr::Filter], channel_id: Uuid, root_id: &str) {
        assert_eq!(filters.len(), 1, "count should query only matching replies");

        let count = serde_json::to_value(&filters[0]).expect("serialize count filter");
        assert_eq!(
            count.get("kinds"),
            Some(&json!([9, 40002, 43002, 43003, 43004, 43005, 43006]))
        );
        assert_eq!(count.get("#e"), Some(&json!([root_id])));
        assert_eq!(count.get("#h"), Some(&json!([channel_id.to_string()])));
        assert_eq!(count.get("limit"), Some(&json!(0)));
        assert!(count.get("ids").is_none());
        assert!(count.get("authors").is_none());
    }

    fn thread_event(id: &str, pubkey: &str, content: &str, created_at: u64) -> serde_json::Value {
        json!({
            "id": id,
            "pubkey": pubkey,
            "content": content,
            "created_at": created_at
        })
    }

    #[test]
    fn test_json_to_context_message_integer_timestamp() {
        let obj = json!({
            "pubkey": "abc",
            "content": "hello",
            "created_at": 1710518400
        });
        let msg = json_to_context_message(&obj).expect("should parse");
        assert_eq!(msg.pubkey, "abc");
        assert_eq!(msg.content, "hello");
        assert!(msg.timestamp.contains("2024")); // 1710518400 = 2024-03-15
    }

    #[test]
    fn test_json_to_context_message_string_timestamp() {
        let obj = json!({
            "pubkey": "abc",
            "content": "hello",
            "created_at": "2026-03-15T16:30:00+00:00"
        });
        let msg = json_to_context_message(&obj).expect("should parse");
        assert_eq!(msg.timestamp, "2026-03-15T16:30:00+00:00");
    }

    #[test]
    fn test_json_to_context_message_missing_content() {
        let obj = json!({ "pubkey": "abc" });
        assert!(json_to_context_message(&obj).is_none());
    }

    #[test]
    fn test_collect_prompt_pubkeys_includes_authors_mentions_and_context() {
        let keys = Keys::generate();
        let p_tag = Tag::parse([
            "p",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ])
        .unwrap();
        let event = EventBuilder::new(Kind::Custom(9), "hello")
            .tags([p_tag])
            .sign_with_keys(&keys)
            .unwrap();
        let author_hex = event.pubkey.to_hex();
        let channel_id = Uuid::new_v4();
        let batch = FlushBatch {
            channel_id,
            scope: SessionScope::Conversation { channel_id },
            events: vec![crate::queue::BatchEvent {
                event,
                prompt_tag: "@mention".into(),
                received_at: std::time::Instant::now(),
            }],
            cancelled_events: vec![],
            cancel_reason: None,
        };
        let context = ConversationContext::Thread {
            messages: vec![ContextMessage {
                event_id: String::new(),
                pubkey: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                timestamp: "2026-03-25T05:51:25Z".into(),
                content: "follow up".into(),
            }],
            total: 1,
            root_present: true,
            truncated: false,
        };

        let pubkeys = collect_prompt_pubkeys(&batch, Some(&context));

        let mut expected = vec![
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            author_hex,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        ];
        expected.sort();

        assert_eq!(pubkeys, expected);
    }

    #[test]
    fn test_parse_kind0_profile_lookup_extracts_display_name_and_nip05() {
        let lookup = parse_kind0_profile_lookup(json!([
            {
                "id": "0000000000000000000000000000000000000000000000000000000000000001",
                "pubkey": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "kind": 0,
                "content": "{\"display_name\":\"Wes\",\"nip05\":\"wes@example.com\"}",
                "created_at": 1000,
                "tags": [],
                "sig": "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            }
        ]))
        .expect("lookup should parse");

        assert_eq!(
            lookup.get("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some(&PromptProfile {
                display_name: Some("Wes".into()),
                nip05_handle: Some("wes@example.com".into()),
                is_agent: false,
            })
        );
    }

    #[test]
    fn test_profile_event_is_agent_detects_nip_oa_auth_tag() {
        // Agent profile carries a 4-element NIP-OA ["auth", owner, cond, sig] tag.
        let agent_ev = json!({
            "pubkey": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "tags": [["auth", "owner_pk", "conditions", "sig"]],
        });
        assert!(profile_event_is_agent(&agent_ev));

        // Human profile: no auth tag.
        let human_ev = json!({ "pubkey": "bbbb", "tags": [["t", "topic"]] });
        assert!(!profile_event_is_agent(&human_ev));

        // Empty / missing tags → not an agent.
        assert!(!profile_event_is_agent(&json!({ "tags": [] })));
        assert!(!profile_event_is_agent(&json!({})));

        // Malformed auth tag (wrong arity) → not treated as an agent.
        let malformed = json!({ "tags": [["auth", "owner_pk"]] });
        assert!(!profile_event_is_agent(&malformed));
    }

    #[test]
    fn test_parse_kind0_profile_lookup_returns_none_for_empty() {
        assert!(parse_kind0_profile_lookup(json!([])).is_none());
        assert!(parse_kind0_profile_lookup(json!({})).is_none());
    }

    fn context_message(event_id: &str, content: &str) -> ContextMessage {
        ContextMessage {
            event_id: event_id.to_string(),
            pubkey: "author".into(),
            timestamp: "2026-08-09T00:00:00Z".into(),
            content: content.into(),
        }
    }

    #[tokio::test]
    async fn run_prompt_task_commits_standing_context_only_after_acp_success() {
        let capture = std::env::temp_dir().join(format!(
            "buzz-acp-standing-lifecycle-{}.ndjson",
            Uuid::new_v4()
        ));
        let quoted_capture = capture.to_string_lossy().replace('\'', "'\\''");
        let script = format!(
            r#"count=0
while IFS= read -r line; do
  printf '%s\n' "$line" >> '{quoted_capture}'
  count=$((count + 1))
  if [ "$count" -eq 1 ]; then
    printf '%s\n' '{{"jsonrpc":"2.0","id":0,"error":{{"code":-32000,"message":"retry me"}}}}'
  else
    printf '%s\n' "{{\"jsonrpc\":\"2.0\",\"id\":$((count - 1)),\"result\":{{\"stopReason\":\"end_turn\"}}}}"
  fi
done"#
        );
        let acp = AcpClient::spawn("bash", &["-c".to_string(), script], &[], false)
            .await
            .expect("spawn lifecycle ACP script");
        let mut agent = OwnedAgent {
            index: 0,
            acp,
            state: SessionState::default(),
            model_capabilities: None,
            desired_model: None,
            model_overridden: false,
            desired_model_request_id: None,
            desired_model_pending_ack: false,
            startup_effort: None,
            agent_name: "legacy-test-agent".into(),
            goose_system_prompt_supported: None,
            protocol_version: 1,
        };
        agent.state.heartbeat_session = Some("live-session".into());

        let mut ctx = make_prompt_context_no_owner();
        ctx.base_prompt = Some("standing-once".into());
        let ctx = Arc::new(ctx);
        let (result_tx, mut result_rx) = mpsc::unbounded_channel();

        for turn in 1..=3 {
            run_prompt_task(
                agent,
                None,
                Some(format!("heartbeat-{turn}")),
                Arc::clone(&ctx),
                result_tx.clone(),
                None,
                PromptExecution::new(None, format!("turn-{turn}")),
            )
            .await;
            let result = result_rx.recv().await.expect("prompt result");
            match turn {
                1 => assert!(matches!(result.outcome, PromptOutcome::Error(_))),
                _ => assert!(matches!(
                    result.outcome,
                    PromptOutcome::Ok(StopReason::EndTurn)
                )),
            }
            assert_eq!(
                result.agent.state.heartbeat_standing_context_sent,
                turn >= 2,
                "failed first delivery must not commit; first success must commit"
            );
            agent = result.agent;
        }
        agent.acp.shutdown().await;

        let requests: Vec<serde_json::Value> = std::fs::read_to_string(&capture)
            .expect("read captured ACP requests")
            .lines()
            .map(|line| serde_json::from_str(line).expect("captured request is JSON"))
            .collect();
        std::fs::remove_file(&capture).expect("remove ACP capture");
        assert_eq!(requests.len(), 3);
        let prompt_text = |index: usize| {
            requests[index]["params"]["prompt"][0]["text"]
                .as_str()
                .expect("text prompt")
        };
        assert_eq!(
            prompt_text(0),
            "<base>\nstanding-once\n</base>\n\nheartbeat-1"
        );
        assert_eq!(
            prompt_text(1),
            "<base>\nstanding-once\n</base>\n\nheartbeat-2",
            "retry after ACP failure must resend standing context"
        );
        assert_eq!(
            prompt_text(2),
            "heartbeat-3",
            "turn after ACP success must omit standing context"
        );
    }

    #[tokio::test]
    async fn channel_prompt_commits_delivery_state_only_after_acp_success() {
        let capture = std::env::temp_dir().join(format!(
            "buzz-acp-channel-delivery-lifecycle-{}.ndjson",
            Uuid::new_v4()
        ));
        let quoted_capture = capture.to_string_lossy().replace('\'', "'\\''");
        let script = format!(
            r#"count=0
while IFS= read -r line; do
  printf '%s\n' "$line" >> '{quoted_capture}'
  count=$((count + 1))
  if [ "$count" -eq 1 ]; then
    printf '%s\n' '{{"jsonrpc":"2.0","id":0,"error":{{"code":-32000,"message":"retry me"}}}}'
  else
    printf '%s\n' "{{\"jsonrpc\":\"2.0\",\"id\":$((count - 1)),\"result\":{{\"stopReason\":\"end_turn\"}}}}"
  fi
done"#
        );
        let acp = AcpClient::spawn("bash", &["-c".to_string(), script], &[], false)
            .await
            .expect("spawn channel lifecycle ACP script");
        let channel_id = Uuid::new_v4();
        let mut agent = OwnedAgent {
            index: 0,
            acp,
            state: SessionState::default(),
            model_capabilities: None,
            desired_model: None,
            model_overridden: false,
            desired_model_request_id: None,
            desired_model_pending_ack: false,
            startup_effort: None,
            agent_name: "legacy-test-agent".into(),
            goose_system_prompt_supported: None,
            protocol_version: 1,
        };
        agent
            .state
            .sessions
            .insert(conv(channel_id), "live-session".into());
        agent
            .state
            .deliveries
            .insert(conv(channel_id), ChannelDeliveryState::default());

        let mut ctx = make_prompt_context_no_owner();
        ctx.base_prompt = Some("standing-once".into());
        let ctx = Arc::new(ctx);
        let (result_tx, mut result_rx) = mpsc::unbounded_channel();

        for turn in 1..=3 {
            let event = EventBuilder::new(Kind::Custom(9), format!("channel-{turn}"))
                .sign_with_keys(&Keys::generate())
                .unwrap();
            let event_id = event.id.to_hex();
            let batch = FlushBatch {
                channel_id,
                scope: SessionScope::Conversation { channel_id },
                events: vec![crate::queue::BatchEvent {
                    event,
                    prompt_tag: "test".into(),
                    received_at: std::time::Instant::now(),
                }],
                cancelled_events: vec![],
                cancel_reason: None,
            };
            run_prompt_task(
                agent,
                Some(batch),
                None,
                Arc::clone(&ctx),
                result_tx.clone(),
                None,
                PromptExecution::new(None, format!("turn-{turn}")),
            )
            .await;
            let result = result_rx.recv().await.expect("prompt result");
            match turn {
                1 => assert!(matches!(result.outcome, PromptOutcome::Error(_))),
                _ => assert!(matches!(
                    result.outcome,
                    PromptOutcome::Ok(StopReason::EndTurn)
                )),
            }
            let delivery = &result.agent.state.deliveries[&conv(channel_id)];
            assert_eq!(
                delivery.standing_context_sent,
                turn >= 2,
                "failed channel delivery must not commit; first success must commit"
            );
            assert_eq!(
                delivery.delivered_event_ids.contains(&event_id),
                turn >= 2,
                "channel event IDs must commit only after ACP success"
            );
            agent = result.agent;
        }
        agent.acp.shutdown().await;

        let requests: Vec<serde_json::Value> = std::fs::read_to_string(&capture)
            .expect("read captured ACP requests")
            .lines()
            .map(|line| serde_json::from_str(line).expect("captured request is JSON"))
            .collect();
        std::fs::remove_file(&capture).expect("remove ACP capture");
        let prompt_text = |index: usize| {
            requests[index]["params"]["prompt"][0]["text"]
                .as_str()
                .expect("text prompt")
        };
        assert!(prompt_text(0).contains("<base>\nstanding-once\n</base>"));
        assert!(
            prompt_text(1).contains("<base>\nstanding-once\n</base>"),
            "retry after channel ACP failure must resend standing context"
        );
        assert!(
            !prompt_text(2).contains("<base>\nstanding-once\n</base>"),
            "turn after channel ACP success must omit standing context"
        );
    }

    #[tokio::test]
    async fn merged_cancel_prompt_commits_and_deduplicates_all_rendered_event_ids() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let channel_id = Uuid::new_v4();
        let keys = Keys::generate();
        let carry_over = EventBuilder::new(Kind::Custom(9), "merged carry-over sentinel")
            .sign_with_keys(&keys)
            .unwrap();
        let carry_over_id = carry_over.id.to_hex();
        let new_event = EventBuilder::new(Kind::Custom(9), "merged new-event sentinel")
            .sign_with_keys(&keys)
            .unwrap();
        let new_event_id = new_event.id.to_hex();
        let next_event = EventBuilder::new(Kind::Custom(9), "ordinary next-turn sentinel")
            .sign_with_keys(&keys)
            .unwrap();
        let merged_batch = FlushBatch {
            channel_id,
            scope: SessionScope::Conversation { channel_id },
            events: vec![crate::queue::BatchEvent {
                event: new_event.clone(),
                prompt_tag: "test".into(),
                received_at: std::time::Instant::now(),
            }],
            cancelled_events: vec![crate::queue::BatchEvent {
                event: carry_over.clone(),
                prompt_tag: "test".into(),
                received_at: std::time::Instant::now(),
            }],
            cancel_reason: Some(crate::queue::CancelReason::Steer),
        };
        let next_batch = FlushBatch {
            channel_id,
            scope: SessionScope::Conversation { channel_id },
            events: vec![crate::queue::BatchEvent {
                event: next_event,
                prompt_tag: "test".into(),
                received_at: std::time::Instant::now(),
            }],
            cancelled_events: vec![],
            cancel_reason: None,
        };

        // Return both merged events as DM history. They must be excluded from
        // the merged prompt's context and, after success, from the next turn.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind context server");
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let response_body = serde_json::to_string(&vec![carry_over, new_event]).unwrap();
        let server = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = vec![0; 16 * 1024];
                let _ = socket.read(&mut request).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response_body.len(), response_body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });

        let capture = std::env::temp_dir().join(format!(
            "buzz-acp-merged-delivery-wire-{}.ndjson",
            Uuid::new_v4()
        ));
        let quoted_capture = capture.to_string_lossy().replace('\'', "'\\''");
        let script = format!(
            r#"count=0
while IFS= read -r line; do
  printf '%s\n' "$line" >> '{quoted_capture}'
  printf '%s\n' "{{\"jsonrpc\":\"2.0\",\"id\":$count,\"result\":{{\"stopReason\":\"end_turn\"}}}}"
  count=$((count + 1))
done"#
        );
        let acp = AcpClient::spawn("bash", &["-c".into(), script], &[], false)
            .await
            .expect("spawn wire-capture ACP");
        let mut agent = OwnedAgent {
            index: 0,
            acp,
            state: SessionState::default(),
            model_capabilities: None,
            desired_model: None,
            model_overridden: false,
            desired_model_request_id: None,
            desired_model_pending_ack: false,
            startup_effort: None,
            agent_name: "legacy-test-agent".into(),
            goose_system_prompt_supported: None,
            protocol_version: 1,
        };
        agent
            .state
            .sessions
            .insert(conv(channel_id), "live-session".into());
        agent
            .state
            .deliveries
            .insert(conv(channel_id), ChannelDeliveryState::default());

        let mut ctx = make_prompt_context_no_owner();
        ctx.context_message_limit = 10;
        ctx.rest_client.base_url = base_url.clone();
        ctx.channel_info = ChannelInfoResolver::new(
            HashMap::from([(
                channel_id,
                crate::relay::ChannelInfo {
                    name: "test-dm".into(),
                    channel_type: "dm".into(),
                    description: None,
                },
            )]),
            RestClient {
                http: reqwest::Client::new(),
                base_url,
                keys: ctx.agent_keys.clone(),
                auth_tag_json: None,
            },
        );
        let ctx = Arc::new(ctx);
        let (result_tx, mut result_rx) = mpsc::unbounded_channel();

        for (turn_id, batch) in [("merged-turn", merged_batch), ("next-turn", next_batch)] {
            run_prompt_task(
                agent,
                Some(batch),
                None,
                Arc::clone(&ctx),
                result_tx.clone(),
                None,
                PromptExecution::new(None, turn_id.into()),
            )
            .await;
            let result = result_rx.recv().await.expect("prompt result");
            assert!(matches!(
                result.outcome,
                PromptOutcome::Ok(StopReason::EndTurn)
            ));
            agent = result.agent;
        }
        let delivery = &agent.state.deliveries[&conv(channel_id)];
        assert!(delivery.delivered_event_ids.contains(&carry_over_id));
        assert!(delivery.delivered_event_ids.contains(&new_event_id));
        agent.acp.shutdown().await;
        server.abort();

        let requests: Vec<serde_json::Value> = std::fs::read_to_string(&capture)
            .expect("read captured prompts")
            .lines()
            .map(|line| serde_json::from_str(line).expect("captured prompt JSON"))
            .collect();
        std::fs::remove_file(&capture).expect("remove prompt capture");
        assert_eq!(requests.len(), 2);
        let wire = |index: usize| {
            requests[index]["params"]["prompt"]
                .as_array()
                .expect("prompt blocks")
                .iter()
                .filter_map(|block| block["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n")
        };
        let merged_wire = wire(0);
        assert_eq!(merged_wire.matches("merged carry-over sentinel").count(), 1);
        assert_eq!(merged_wire.matches("merged new-event sentinel").count(), 1);
        let next_wire = wire(1);
        assert!(next_wire.contains("ordinary next-turn sentinel"));
        assert!(!next_wire.contains("merged carry-over sentinel"));
        assert!(!next_wire.contains("merged new-event sentinel"));
        assert!(!next_wire.contains(&carry_over_id));
        assert!(!next_wire.contains(&new_event_id));
    }

    #[tokio::test]
    async fn late_successful_steer_ack_excludes_event_from_next_channel_wire_prompt() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let channel_id = Uuid::new_v4();
        let keys = Keys::generate();
        let steered_event = EventBuilder::new(Kind::Custom(9), "steered context must not replay")
            .sign_with_keys(&keys)
            .unwrap();
        let steered_event_id = steered_event.id.to_hex();
        let trigger = EventBuilder::new(Kind::Custom(9), "ordinary next turn")
            .sign_with_keys(&keys)
            .unwrap();
        let batch = FlushBatch {
            channel_id,
            scope: SessionScope::Conversation { channel_id },
            events: vec![crate::queue::BatchEvent {
                event: trigger,
                prompt_tag: "test".into(),
                received_at: std::time::Instant::now(),
            }],
            cancelled_events: vec![],
            cancel_reason: None,
        };

        // The local REST bridge returns the already-delivered steer as DM
        // history. Profile/reaction requests may also arrive; the same valid
        // event array is harmless for those best-effort paths.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind context server");
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let response_body = serde_json::to_string(&vec![steered_event]).unwrap();
        let server = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = vec![0; 16 * 1024];
                let _ = socket.read(&mut request).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response_body.len(), response_body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });

        let capture = std::env::temp_dir().join(format!(
            "buzz-acp-late-steer-wire-{}.ndjson",
            Uuid::new_v4()
        ));
        let quoted_capture = capture.to_string_lossy().replace('\'', "'\\''");
        let script = format!(
            r#"IFS= read -r line
printf '%s\n' "$line" > '{quoted_capture}'
printf '%s\n' '{{"jsonrpc":"2.0","id":0,"result":{{"stopReason":"end_turn"}}}}'"#
        );
        let acp = AcpClient::spawn("bash", &["-c".into(), script], &[], false)
            .await
            .expect("spawn wire-capture ACP");
        let mut agent = OwnedAgent {
            index: 0,
            acp,
            state: SessionState::default(),
            model_capabilities: None,
            desired_model: None,
            model_overridden: false,
            desired_model_request_id: None,
            desired_model_pending_ack: false,
            startup_effort: None,
            agent_name: "legacy-test-agent".into(),
            goose_system_prompt_supported: None,
            protocol_version: 1,
        };
        agent
            .state
            .sessions
            .insert(conv(channel_id), "live-session".into());
        agent
            .state
            .deliveries
            .insert(conv(channel_id), ChannelDeliveryState::default());

        // Model the adversarial ordering: the task result has already retired
        // its TaskMeta and returned the agent before the successful ack arrives.
        let mut pool = AgentPool::from_slots(vec![Some(agent)]);
        assert!(pool.record_successful_steer(
            &conv(channel_id),
            steered_event_id.clone(),
            "live-session".into(),
        ));
        let agent = pool
            .try_claim(Some(&conv(channel_id)))
            .expect("claim returned agent");

        let mut ctx = make_prompt_context_no_owner();
        ctx.context_message_limit = 10;
        ctx.rest_client.base_url = base_url.clone();
        ctx.channel_info = ChannelInfoResolver::new(
            HashMap::from([(
                channel_id,
                crate::relay::ChannelInfo {
                    name: "test-dm".into(),
                    channel_type: "dm".into(),
                    description: None,
                },
            )]),
            RestClient {
                http: reqwest::Client::new(),
                base_url,
                keys: ctx.agent_keys.clone(),
                auth_tag_json: None,
            },
        );
        let (result_tx, mut result_rx) = mpsc::unbounded_channel();
        run_prompt_task(
            agent,
            Some(batch),
            None,
            Arc::new(ctx),
            result_tx,
            None,
            PromptExecution::new(None, "next-turn".into()),
        )
        .await;
        let mut result = result_rx.recv().await.expect("next prompt result");
        assert!(matches!(
            result.outcome,
            PromptOutcome::Ok(StopReason::EndTurn)
        ));
        result.agent.acp.shutdown().await;
        server.abort();

        let request: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&capture).expect("read captured prompt"))
                .expect("captured prompt JSON");
        std::fs::remove_file(&capture).expect("remove prompt capture");
        let wire = request["params"]["prompt"]
            .as_array()
            .expect("prompt blocks")
            .iter()
            .filter_map(|block| block["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(wire.contains("ordinary next turn"));
        assert!(!wire.contains("steered context must not replay"));
        assert!(!wire.contains(&steered_event_id));
    }

    #[test]
    fn delivery_state_commits_only_when_explicitly_marked_successful() {
        let channel = Uuid::new_v4();
        let mut state = SessionState::default();
        state
            .deliveries
            .insert(conv(channel), ChannelDeliveryState::default());

        // Building or attempting a prompt does not mutate delivery state.
        let delivery = state.deliveries.get(&conv(channel)).unwrap();
        assert!(!delivery.standing_context_sent);
        assert!(delivery.delivered_event_ids.is_empty());

        state.mark_scope_delivery_success(
            conv(channel),
            true,
            ["trigger".to_string(), "context".to_string()],
        );
        let delivery = state.deliveries.get(&conv(channel)).unwrap();
        assert!(delivery.standing_context_sent);
        assert_eq!(delivery.delivered_event_ids.len(), 2);
    }

    #[test]
    fn delivery_state_is_cleared_on_rotation_and_restarts_empty() {
        let channel = Uuid::new_v4();
        let mut state = SessionState::default();
        state.sessions.insert(conv(channel), "old-session".into());
        state.mark_scope_delivery_success(conv(channel), true, ["old-event".to_string()]);

        assert!(state.invalidate_channel(&channel) > 0);
        assert!(!state.deliveries.contains_key(&conv(channel)));

        state.sessions.insert(conv(channel), "new-session".into());
        state
            .deliveries
            .insert(conv(channel), ChannelDeliveryState::default());
        let delivery = state.deliveries.get(&conv(channel)).unwrap();
        assert!(!delivery.standing_context_sent);
        assert!(delivery.delivered_event_ids.is_empty());
    }

    #[test]
    fn conversation_context_delta_omits_delivered_and_triggering_events() {
        let delivered = HashSet::from(["old".to_string()]);
        let triggering = HashSet::from(["trigger".to_string()]);
        let context = ConversationContext::Thread {
            messages: vec![
                context_message("old", "already sent"),
                context_message("trigger", "rendered as trigger"),
                context_message("new", "new context"),
            ],
            total: 3,
            root_present: true,
            truncated: false,
        };

        let delta = conversation_context_delta(Some(context), &delivered, &triggering)
            .expect("new context remains");
        match delta {
            ConversationContext::Thread {
                messages,
                total,
                root_present,
                truncated,
            } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].event_id, "new");
                assert_eq!(total, 3);
                assert!(!truncated);
                assert!(root_present);
            }
            _ => panic!("expected thread context"),
        }
    }

    #[test]
    fn conversation_context_delta_returns_none_when_no_new_events_remain() {
        let delivered = HashSet::from(["old".to_string()]);
        let context = ConversationContext::Dm {
            messages: vec![context_message("old", "already sent")],
            total: 1,
            truncated: false,
        };

        assert!(conversation_context_delta(Some(context), &delivered, &HashSet::new()).is_none());
    }

    #[test]
    fn conversation_context_delta_preserves_unidentified_legacy_messages() {
        let context = ConversationContext::Dm {
            messages: vec![context_message("", "cannot safely deduplicate")],
            total: 1,
            truncated: false,
        };

        assert!(
            conversation_context_delta(Some(context), &HashSet::new(), &HashSet::new()).is_some()
        );
    }

    #[test]
    fn test_json_to_context_message_missing_pubkey_uses_default() {
        let obj = json!({ "content": "hello" });
        let msg = json_to_context_message(&obj).expect("should parse");
        assert_eq!(msg.pubkey, "unknown");
    }

    #[test]
    fn test_pct_encode_hex_passthrough() {
        let hex = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        assert_eq!(pct_encode(hex), hex);
    }

    #[test]
    fn test_pct_encode_emoji() {
        // 👀 = U+1F440 = F0 9F 91 80 in UTF-8
        assert_eq!(pct_encode("👀"), "%F0%9F%91%80");
    }

    #[test]
    fn test_pct_encode_emoji_speech_balloon() {
        // 💬 = U+1F4AC = F0 9F 92 AC in UTF-8
        assert_eq!(pct_encode("💬"), "%F0%9F%92%AC");
    }

    #[test]
    fn test_pct_encode_empty() {
        assert_eq!(pct_encode(""), "");
    }

    #[test]
    fn test_pct_encode_unreserved_passthrough() {
        assert_eq!(pct_encode("AZaz09-_.~"), "AZaz09-_.~");
    }

    #[test]
    fn test_pct_encode_reserved_chars() {
        assert_eq!(pct_encode("/"), "%2F");
        assert_eq!(pct_encode("+"), "%2B");
        assert_eq!(pct_encode(" "), "%20");
    }

    fn make_state() -> (SessionState, Uuid, Uuid) {
        let ch_a = Uuid::new_v4();
        let ch_b = Uuid::new_v4();
        let mut s = SessionState::default();
        s.sessions.insert(conv(ch_a), "sess-a".into());
        s.sessions.insert(conv(ch_b), "sess-b".into());
        s.turn_counts.insert(conv(ch_a), 5);
        s.turn_counts.insert(conv(ch_b), 3);
        s.core_sections.insert(conv(ch_a), "core-a".into());
        s.core_sections.insert(conv(ch_b), "core-b".into());
        s.deliveries.insert(
            conv(ch_a),
            ChannelDeliveryState {
                standing_context_sent: true,
                delivered_event_ids: HashSet::from(["event-a".into()]),
            },
        );
        s.deliveries.insert(
            conv(ch_b),
            ChannelDeliveryState {
                standing_context_sent: true,
                delivered_event_ids: HashSet::from(["event-b".into()]),
            },
        );
        s.heartbeat_session = Some("sess-hb".into());
        s.heartbeat_turn_count = 7;
        s.heartbeat_standing_context_sent = true;
        (s, ch_a, ch_b)
    }

    fn thread_scope(channel_id: Uuid, root: &str) -> SessionScope {
        SessionScope::Thread {
            channel_id,
            root_event_id: root.to_string(),
        }
    }

    #[test]
    fn two_threads_in_one_channel_get_distinct_sessions() {
        let ch = Uuid::new_v4();
        let ta = thread_scope(ch, &"a".repeat(64));
        let tb = thread_scope(ch, &"b".repeat(64));
        let mut s = SessionState::default();
        s.sessions.insert(ta.clone(), "sess-thread-a".into());
        s.sessions.insert(tb.clone(), "sess-thread-b".into());
        // Distinct roots key distinct provider sessions.
        assert_eq!(
            s.sessions.get(&ta).map(String::as_str),
            Some("sess-thread-a")
        );
        assert_eq!(
            s.sessions.get(&tb).map(String::as_str),
            Some("sess-thread-b")
        );
        // Repeated activity under one root reuses that exact session.
        assert_eq!(
            s.sessions.get(&ta).map(String::as_str),
            Some("sess-thread-a")
        );
        // The conversation scope is a different key again (no accidental reuse).
        assert!(!s.sessions.contains_key(&conv(ch)));
    }

    #[test]
    fn invalidate_scope_leaves_sibling_thread_untouched() {
        let ch = Uuid::new_v4();
        let ta = thread_scope(ch, &"a".repeat(64));
        let tb = thread_scope(ch, &"b".repeat(64));
        let mut s = SessionState::default();
        s.sessions.insert(ta.clone(), "a".into());
        s.sessions.insert(tb.clone(), "b".into());
        s.turn_counts.insert(ta.clone(), 2);
        assert!(s.invalidate_scope(&ta));
        assert!(!s.sessions.contains_key(&ta));
        assert!(!s.turn_counts.contains_key(&ta));
        // Sibling thread's session survives.
        assert_eq!(s.sessions.get(&tb).map(String::as_str), Some("b"));
    }

    fn batch_with_scope(scope: SessionScope, event: nostr::Event) -> FlushBatch {
        FlushBatch {
            channel_id: scope.channel_id(),
            scope,
            events: vec![crate::queue::BatchEvent {
                event,
                prompt_tag: "test".into(),
                received_at: std::time::Instant::now(),
            }],
            cancelled_events: vec![],
            cancel_reason: None,
        }
    }

    fn signed_event_with_tags(tags: Vec<Vec<String>>) -> nostr::Event {
        let keys = Keys::generate();
        let tags: Vec<Tag> = tags.into_iter().map(|t| Tag::parse(t).unwrap()).collect();
        EventBuilder::new(Kind::Custom(9), "hi")
            .tags(tags)
            .sign_with_keys(&keys)
            .unwrap()
    }

    fn thread_reply(root: &str) -> nostr::Event {
        signed_event_with_tags(vec![
            vec!["e".into(), root.into(), String::new(), "root".into()],
            vec!["e".into(), root.into(), String::new(), "reply".into()],
        ])
    }

    #[test]
    fn native_steer_only_reuses_a_provably_stable_trusted_chat_destination() {
        let channel_id = Uuid::new_v4();
        let scope = conv(channel_id);
        let root_a = "a".repeat(64);
        let root_b = "b".repeat(64);
        let top_level = signed_event_with_tags(vec![]);
        let active_top_level = batch_with_scope(scope.clone(), top_level.clone());

        assert!(native_steer_preserves_chat_destination(
            &active_top_level,
            &signed_event_with_tags(vec![]),
            crate::reply_placement::ReplyPlacement::Timeline,
        ));
        assert!(!native_steer_preserves_chat_destination(
            &active_top_level,
            &thread_reply(&root_a),
            crate::reply_placement::ReplyPlacement::Timeline,
        ));
        assert!(!native_steer_preserves_chat_destination(
            &active_top_level,
            &signed_event_with_tags(vec![]),
            crate::reply_placement::ReplyPlacement::Thread,
        ));

        let active_thread = batch_with_scope(scope, thread_reply(&root_a));
        assert!(!native_steer_preserves_chat_destination(
            &active_thread,
            &thread_reply(&root_a),
            crate::reply_placement::ReplyPlacement::Timeline,
        ));
        assert!(!native_steer_preserves_chat_destination(
            &active_thread,
            &thread_reply(&root_b),
            crate::reply_placement::ReplyPlacement::Timeline,
        ));
        assert!(!native_steer_preserves_chat_destination(
            &active_thread,
            &top_level,
            crate::reply_placement::ReplyPlacement::Timeline,
        ));
    }

    #[test]
    fn context_target_uses_thread_scope_root_not_last_event_tags() {
        let ch = Uuid::new_v4();
        let scope_root = "a".repeat(64);
        // Last event carries a DIFFERENT root tag than the scope; the scope
        // must win so context is gathered for the canonical thread.
        let ev = signed_event_with_tags(vec![vec![
            "e".into(),
            "b".repeat(64),
            String::new(),
            "root".into(),
        ]]);
        let batch = batch_with_scope(thread_scope(ch, &scope_root), ev);
        assert_eq!(
            resolve_context_target(&batch, false),
            ContextTarget::Thread(scope_root)
        );
    }

    #[test]
    fn context_target_uses_job_request_as_task_thread_root() {
        let channel_id = Uuid::new_v4();
        let request_event_id = "a".repeat(64);
        let batch = batch_with_scope(
            SessionScope::Job {
                channel_id,
                operation_id: Uuid::new_v4().to_string(),
                request_event_id: request_event_id.clone(),
            },
            signed_event_with_tags(vec![]),
        );

        assert_eq!(
            resolve_context_target(&batch, false),
            ContextTarget::Thread(request_event_id)
        );
    }

    #[test]
    fn context_target_new_top_level_thread_has_no_history() {
        // A top-level mention opens a thread rooted at its own id; on the first
        // turn there is no prior thread history to fetch, but the scope still
        // resolves to that root (subsequent turns fetch it).
        let ch = Uuid::new_v4();
        let ev = signed_event_with_tags(vec![]);
        let root = ev.id.to_hex();
        let batch = batch_with_scope(thread_scope(ch, &root), ev);
        assert_eq!(
            resolve_context_target(&batch, false),
            ContextTarget::Thread(root)
        );
    }

    #[test]
    fn context_target_conversation_channel_plain_has_none() {
        // Channel-policy conversation scope + a plain (no-thread-tag) event =>
        // no unrelated channel transcript is injected.
        let ch = Uuid::new_v4();
        let ev = signed_event_with_tags(vec![]);
        let batch = batch_with_scope(conv(ch), ev);
        assert_eq!(resolve_context_target(&batch, false), ContextTarget::None);
    }

    #[test]
    fn context_target_dm_nonreply_is_dm_history() {
        let ch = Uuid::new_v4();
        let ev = signed_event_with_tags(vec![]);
        let batch = batch_with_scope(conv(ch), ev);
        assert_eq!(resolve_context_target(&batch, true), ContextTarget::Dm);
    }

    #[test]
    fn context_target_conversation_reply_uses_reply_chain() {
        // DM (or legacy channel-policy) reply: conversation scope but the last
        // event has thread tags => fetch that reply chain.
        let ch = Uuid::new_v4();
        let root = "c".repeat(64);
        let ev = signed_event_with_tags(vec![
            vec!["e".into(), root.clone(), String::new(), "root".into()],
            vec!["e".into(), "d".repeat(64), String::new(), "reply".into()],
        ]);
        let batch = batch_with_scope(conv(ch), ev);
        assert_eq!(
            resolve_context_target(&batch, true),
            ContextTarget::Thread(root)
        );
    }

    #[test]
    fn invalidate_channel_clears_every_thread_scope() {
        let ch = Uuid::new_v4();
        let other = Uuid::new_v4();
        let mut s = SessionState::default();
        s.sessions
            .insert(thread_scope(ch, &"a".repeat(64)), "a".into());
        s.sessions
            .insert(thread_scope(ch, &"b".repeat(64)), "b".into());
        s.sessions.insert(conv(ch), "c".into());
        s.sessions
            .insert(thread_scope(other, &"d".repeat(64)), "d".into());
        let cleared = s.invalidate_channel(&ch);
        assert_eq!(cleared, 3, "all three ch scopes had sessions");
        assert!(s.sessions.keys().all(|k| k.channel_id() == other));
    }

    #[test]
    fn prompt_source_scope_exposes_thread_scope_and_none_for_heartbeat() {
        let ch = Uuid::new_v4();
        let scope = thread_scope(ch, &"a".repeat(64));
        let channel = PromptSource::Channel(scope.clone());
        // The scope-precise accessor returns the exact thread so a completing
        // turn clears only its own typing indicator.
        assert_eq!(channel.scope(), Some(&scope));
        assert_eq!(channel.channel_id(), Some(ch));
        assert_eq!(PromptSource::Heartbeat.scope(), None);
    }

    #[tokio::test]
    async fn invalidate_scope_session_targets_one_thread_and_drops_its_owner() {
        // The idle `!rotate` path: rotating thread A must invalidate only thread
        // A's session and drop its scope-owner entry, leaving a sibling thread
        // in the same channel fully intact.
        let ch = Uuid::new_v4();
        let ta = thread_scope(ch, &"a".repeat(64));
        let tb = thread_scope(ch, &"b".repeat(64));
        let acp = AcpClient::spawn("bash", &["-c".into(), "sleep 10".into()], &[], false)
            .await
            .expect("spawn dummy ACP");
        let mut agent = OwnedAgent {
            index: 0,
            acp,
            state: SessionState::default(),
            model_capabilities: None,
            desired_model: None,
            model_overridden: false,
            desired_model_request_id: None,
            desired_model_pending_ack: false,
            startup_effort: None,
            agent_name: "test".into(),
            goose_system_prompt_supported: None,
            protocol_version: 2,
        };
        agent.state.sessions.insert(ta.clone(), "sess-a".into());
        agent.state.sessions.insert(tb.clone(), "sess-b".into());
        let mut pool = AgentPool::from_slots(vec![Some(agent)]);
        pool.record_scope_owner(ta.clone(), 0);
        pool.record_scope_owner(tb.clone(), 0);

        let cleared = pool.invalidate_scope_session(&ta);

        assert_eq!(cleared, 1, "exactly one worker held thread A's session");
        assert!(!pool.has_session_for(&ta), "thread A session invalidated");
        assert!(
            pool.has_session_for(&tb),
            "sibling thread B session survives"
        );
        assert!(
            !pool.session_owners.contains_key(&ta),
            "thread A owner dropped"
        );
        assert!(
            pool.session_owners.contains_key(&tb),
            "thread B owner retained"
        );
    }

    #[test]
    fn test_rotate_after_natural_completion_invalidates_channel_state() {
        let (mut s, ch_a, ch_b) = make_state();

        apply_completed_before_control_signal(
            &mut s,
            &PromptSource::Channel(SessionScope::Conversation { channel_id: ch_a }),
            &ControlSignal::Rotate,
        );

        assert!(!s.sessions.contains_key(&conv(ch_a)));
        assert!(!s.turn_counts.contains_key(&conv(ch_a)));
        assert!(!s.core_sections.contains_key(&conv(ch_a)));
        assert!(!s.has_channel_state(&ch_a));
        assert_eq!(s.sessions.get(&conv(ch_b)).unwrap(), "sess-b");
        assert_eq!(*s.turn_counts.get(&conv(ch_b)).unwrap(), 3);
        assert_eq!(s.core_sections.get(&conv(ch_b)).unwrap(), "core-b");
        assert_eq!(s.heartbeat_session.as_deref(), Some("sess-hb"));
        assert_eq!(s.heartbeat_turn_count, 7);
    }

    #[test]
    fn test_cancel_after_natural_completion_preserves_channel_state() {
        let (mut s, ch_a, ch_b) = make_state();

        apply_completed_before_control_signal(
            &mut s,
            &PromptSource::Channel(SessionScope::Conversation { channel_id: ch_a }),
            &ControlSignal::Cancel,
        );

        assert_eq!(s.sessions.get(&conv(ch_a)).unwrap(), "sess-a");
        assert_eq!(*s.turn_counts.get(&conv(ch_a)).unwrap(), 5);
        assert_eq!(s.core_sections.get(&conv(ch_a)).unwrap(), "core-a");
        assert_eq!(s.sessions.get(&conv(ch_b)).unwrap(), "sess-b");
    }

    #[test]
    fn test_invalidate_channel_clears_session_and_turn_count() {
        let (mut s, ch_a, ch_b) = make_state();
        s.invalidate(&PromptSource::Channel(SessionScope::Conversation {
            channel_id: ch_a,
        }));

        assert!(!s.sessions.contains_key(&conv(ch_a)));
        assert!(!s.turn_counts.contains_key(&conv(ch_a)));
        assert!(!s.core_sections.contains_key(&conv(ch_a)));
        assert!(!s.has_channel_state(&ch_a));
        // ch_b untouched
        assert_eq!(s.sessions.get(&conv(ch_b)).unwrap(), "sess-b");
        assert_eq!(*s.turn_counts.get(&conv(ch_b)).unwrap(), 3);
        assert_eq!(s.core_sections.get(&conv(ch_b)).unwrap(), "core-b");
        // heartbeat untouched
        assert_eq!(s.heartbeat_session.as_deref(), Some("sess-hb"));
        assert_eq!(s.heartbeat_turn_count, 7);
    }

    #[test]
    fn test_invalidate_heartbeat_clears_session_and_turn_count() {
        let (mut s, ch_a, ch_b) = make_state();
        s.invalidate(&PromptSource::Heartbeat);

        assert!(s.heartbeat_session.is_none());
        assert_eq!(s.heartbeat_turn_count, 0);
        assert!(!s.heartbeat_standing_context_sent);
        // channels untouched
        assert_eq!(s.sessions.len(), 2);
        assert_eq!(*s.turn_counts.get(&conv(ch_a)).unwrap(), 5);
        assert_eq!(*s.turn_counts.get(&conv(ch_b)).unwrap(), 3);
        assert_eq!(s.core_sections.get(&conv(ch_a)).unwrap(), "core-a");
        assert_eq!(s.core_sections.get(&conv(ch_b)).unwrap(), "core-b");
    }

    #[test]
    fn test_invalidate_all_clears_everything() {
        let (mut s, _ch_a, _ch_b) = make_state();
        s.invalidate_all();

        assert!(s.sessions.is_empty());
        assert!(s.turn_counts.is_empty());
        assert!(s.core_sections.is_empty());
        assert!(s.heartbeat_session.is_none());
        assert_eq!(s.heartbeat_turn_count, 0);
        assert!(!s.heartbeat_standing_context_sent);
    }

    #[test]
    fn test_invalidate_nonexistent_channel_is_noop() {
        let (mut s, ch_a, ch_b) = make_state();
        let ghost = Uuid::new_v4();
        s.invalidate(&PromptSource::Channel(SessionScope::Conversation {
            channel_id: ghost,
        }));

        // Everything still intact.
        assert_eq!(s.sessions.len(), 2);
        assert_eq!(s.turn_counts.len(), 2);
        assert_eq!(*s.turn_counts.get(&conv(ch_a)).unwrap(), 5);
        assert_eq!(*s.turn_counts.get(&conv(ch_b)).unwrap(), 3);
        assert_eq!(s.core_sections.get(&conv(ch_a)).unwrap(), "core-a");
        assert_eq!(s.core_sections.get(&conv(ch_b)).unwrap(), "core-b");
    }

    #[test]
    fn test_invalidate_all_on_empty_state_is_noop() {
        let mut s = SessionState::default();
        s.invalidate_all(); // should not panic
        assert!(s.sessions.is_empty());
        assert!(s.turn_counts.is_empty());
        assert!(s.core_sections.is_empty());
    }

    #[test]
    fn test_invalidate_channel_returns_true_when_session_existed() {
        let (mut s, ch_a, ch_b) = make_state();
        assert!(s.invalidate_channel(&ch_a) > 0);
        assert!(!s.sessions.contains_key(&conv(ch_a)));
        assert!(!s.turn_counts.contains_key(&conv(ch_a)));
        assert!(!s.core_sections.contains_key(&conv(ch_a)));
        assert!(!s.has_channel_state(&ch_a));
        // ch_b untouched
        assert_eq!(s.sessions.get(&conv(ch_b)).unwrap(), "sess-b");
        assert_eq!(*s.turn_counts.get(&conv(ch_b)).unwrap(), 3);
        assert_eq!(s.core_sections.get(&conv(ch_b)).unwrap(), "core-b");
        // heartbeat untouched
        assert_eq!(s.heartbeat_session.as_deref(), Some("sess-hb"));
        assert_eq!(s.heartbeat_turn_count, 7);
    }

    #[test]
    fn test_invalidate_channel_returns_false_when_no_session() {
        let (mut s, _ch_a, _ch_b) = make_state();
        let ghost = Uuid::new_v4();
        assert_eq!(s.invalidate_channel(&ghost), 0);
        // Nothing changed.
        assert_eq!(s.sessions.len(), 2);
        assert_eq!(s.turn_counts.len(), 2);
    }

    #[test]
    fn test_removed_channels_cleaned_via_invalidate_channel() {
        // Simulates handle_prompt_result: channels removed while agent
        // was checked out should have both sessions and turn_counts stripped.
        let (mut s, ch_a, ch_b) = make_state();
        let removed = vec![ch_a];
        for ch in &removed {
            s.invalidate_channel(ch);
        }
        assert!(!s.sessions.contains_key(&conv(ch_a)));
        assert!(!s.turn_counts.contains_key(&conv(ch_a)));
        assert!(!s.core_sections.contains_key(&conv(ch_a)));
        assert!(!s.has_channel_state(&ch_a));
        assert_eq!(s.sessions.get(&conv(ch_b)).unwrap(), "sess-b");
        assert_eq!(*s.turn_counts.get(&conv(ch_b)).unwrap(), 3);
        assert_eq!(s.core_sections.get(&conv(ch_b)).unwrap(), "core-b");
    }

    // ── ControlSignal::SwitchModel (Phase 3a, Option ii) ─────────────────────

    #[test]
    fn test_switch_model_after_natural_completion_invalidates_channel_state() {
        let (mut s, ch_a, ch_b) = make_state();

        // SwitchModel must invalidate just like Rotate so the requeued turn
        // re-creates a fresh session that re-applies the new desired_model.
        apply_completed_before_control_signal(
            &mut s,
            &PromptSource::Channel(SessionScope::Conversation { channel_id: ch_a }),
            &ControlSignal::SwitchModel {
                model_id: "gpt-5".into(),
                request_id: None,
            },
        );

        assert!(!s.has_channel_state(&ch_a));
        // ch_b untouched — the switch is channel-scoped.
        assert_eq!(s.sessions.get(&conv(ch_b)).unwrap(), "sess-b");
        assert_eq!(*s.turn_counts.get(&conv(ch_b)).unwrap(), 3);
    }

    // ── requeue_cancelled_batch ────────────────────────────────────────────
    // Table-driven pin of the `ControlSignal` → `CancelReason` ownership that
    // decides whether a cancel-drain-expiry batch is merged into the next
    // flush or dropped outright. `Cancel`/`Rotate` must return `None` — a
    // regression here would silently fall through to
    // `unwrap_or(CancelReason::Steer)` at the requeue site and preserve a
    // batch that should have been discarded.

    fn one_event_batch(channel_id: Uuid) -> FlushBatch {
        let keys = Keys::generate();
        let event = EventBuilder::new(Kind::Custom(9), "test")
            .sign_with_keys(&keys)
            .unwrap();
        FlushBatch {
            channel_id,
            scope: SessionScope::Conversation { channel_id },
            events: vec![crate::queue::BatchEvent {
                event,
                prompt_tag: "test".into(),
                received_at: std::time::Instant::now(),
            }],
            cancelled_events: vec![],
            cancel_reason: None,
        }
    }

    #[test]
    fn test_requeue_cancelled_batch_maps_control_signal_to_cancel_reason() {
        let cases = [
            (ControlSignal::Steer, Some(CancelReason::Steer)),
            (ControlSignal::Interrupt, Some(CancelReason::Interrupt)),
            (
                ControlSignal::SwitchModel {
                    model_id: "gpt-5".into(),
                    request_id: None,
                },
                Some(CancelReason::Interrupt),
            ),
            (ControlSignal::Cancel, None),
            (ControlSignal::Rotate, None),
        ];
        let mut ctx = make_prompt_context_no_owner();
        ctx.dedup_mode = DedupMode::Queue;

        for (signal, expected_reason) in cases {
            let channel_id = Uuid::new_v4();
            let batch = one_event_batch(channel_id);
            let result = requeue_cancelled_batch(&ctx, signal.clone(), Some(batch));
            match expected_reason {
                Some(reason) => {
                    let batch = result
                        .unwrap_or_else(|| panic!("{signal:?} must preserve the batch, got None"));
                    assert_eq!(
                        batch.cancel_reason,
                        Some(reason),
                        "{signal:?} must stamp {reason:?}"
                    );
                }
                None => assert!(
                    result.is_none(),
                    "{signal:?} must drop the batch, got {result:?}"
                ),
            }
        }
    }

    #[test]
    fn durable_recovery_preserves_pre_prompt_and_drops_ambiguous_post_prompt_retry() {
        let mut ctx = make_prompt_context_no_owner();
        ctx.dedup_mode = DedupMode::Queue;
        let recovery_dir = tempfile::tempdir().unwrap();
        ctx.session_recovery = Some(
            crate::session_recovery::SessionRecoveryStore::open(
                recovery_dir.path().join("sessions.json"),
            )
            .unwrap(),
        );
        let channel_id = Uuid::new_v4();
        let batch = one_event_batch(channel_id);

        assert!(
            requeue_pre_prompt_batch(&ctx, Some(batch.clone())).is_some(),
            "the triggering event was not sent during initial-message setup"
        );
        assert!(
            requeue_ambiguous_provider_batch(&ctx, Some(batch.clone())).is_none(),
            "a batch that crossed the real provider prompt boundary is indeterminate"
        );
        let failure = classify_control_cancel_failure(
            &ctx,
            AcpError::CancelDrainTimeout(CONTROL_CANCEL_GRACE),
            ControlSignal::Steer,
            Some(batch),
        );
        assert!(failure.retry_batch.is_none());
    }

    // ── classify_control_cancel_failure ─────────────────────────────────────
    // Table-driven pin of the single production seam used by the
    // `Err(error)` arm in `run_prompt_task`'s control-cancel branch. Crosses
    // the exact error→outcome AND outcome→batch-fate boundary in one call,
    // so a regression to the old per-arm duplication (or to routing an
    // unexpected HardTimeout back through the real hard-cap path) fails
    // here rather than only in independently-manufactured unit tests.

    /// Assert `outcome` is the expected `PromptOutcome` variant. `PromptOutcome`
    /// has no `PartialEq` (it wraps `AcpError`, which isn't `PartialEq`), so
    /// this matches by shape instead of deriving equality onto the whole enum.
    fn assert_outcome_matches(outcome: &PromptOutcome, expected: &str) {
        let label = match outcome {
            PromptOutcome::AgentExited => "AgentExited",
            PromptOutcome::Timeout(TimeoutKind::Idle) => "Timeout(Idle)",
            PromptOutcome::Timeout(TimeoutKind::Hard { .. }) => "Timeout(Hard)",
            PromptOutcome::CancelDrainTimeout(_) => "CancelDrainTimeout",
            PromptOutcome::Error(_) => "Error",
            PromptOutcome::ProjectContextIndeterminate(_) => "ProjectContextIndeterminate",
            PromptOutcome::Cancelled => "Cancelled",
            PromptOutcome::Ok(_) => "Ok",
        };
        assert_eq!(
            label, expected,
            "got outcome shape {label}, want {expected}"
        );
    }

    #[test]
    fn test_classify_control_cancel_failure_crosses_error_outcome_and_batch_fate() {
        let ctx = {
            let mut ctx = make_prompt_context_no_owner();
            ctx.dedup_mode = DedupMode::Queue;
            ctx
        };

        struct Case {
            name: &'static str,
            error: fn() -> AcpError,
            signal: ControlSignal,
            expected_outcome: &'static str,
            batch_preserved: bool,
            expected_reason: Option<CancelReason>,
            invalidate_all: bool,
        }

        let cases = [
            Case {
                name: "CancelDrainTimeout + Steer preserves batch with Steer reason",
                error: || AcpError::CancelDrainTimeout(CONTROL_CANCEL_GRACE),
                signal: ControlSignal::Steer,
                expected_outcome: "CancelDrainTimeout",
                batch_preserved: true,
                expected_reason: Some(CancelReason::Steer),
                invalidate_all: false,
            },
            Case {
                name: "CancelDrainTimeout + Cancel drops the batch",
                error: || AcpError::CancelDrainTimeout(CONTROL_CANCEL_GRACE),
                signal: ControlSignal::Cancel,
                expected_outcome: "CancelDrainTimeout",
                batch_preserved: false,
                expected_reason: None,
                invalidate_all: false,
            },
            Case {
                name: "CancelDrainTimeout + Interrupt preserves batch with Interrupt reason",
                error: || AcpError::CancelDrainTimeout(CONTROL_CANCEL_GRACE),
                signal: ControlSignal::Interrupt,
                expected_outcome: "CancelDrainTimeout",
                batch_preserved: true,
                expected_reason: Some(CancelReason::Interrupt),
                invalidate_all: false,
            },
            Case {
                name: "CancelDrainTimeout + Rotate drops the batch",
                error: || AcpError::CancelDrainTimeout(CONTROL_CANCEL_GRACE),
                signal: ControlSignal::Rotate,
                expected_outcome: "CancelDrainTimeout",
                batch_preserved: false,
                expected_reason: None,
                invalidate_all: false,
            },
            Case {
                name: "CancelDrainTimeout + SwitchModel preserves batch with Interrupt reason",
                error: || AcpError::CancelDrainTimeout(CONTROL_CANCEL_GRACE),
                signal: ControlSignal::SwitchModel {
                    model_id: "gpt-5".to_string(),
                    request_id: None,
                },
                expected_outcome: "CancelDrainTimeout",
                batch_preserved: true,
                expected_reason: Some(CancelReason::Interrupt),
                invalidate_all: false,
            },
            Case {
                name: "unexpected HardTimeout cannot become Timeout(Hard)",
                error: || AcpError::HardTimeout {
                    silence: Duration::from_secs(300),
                },
                signal: ControlSignal::Steer,
                expected_outcome: "CancelDrainTimeout",
                batch_preserved: true,
                expected_reason: Some(CancelReason::Steer),
                invalidate_all: false,
            },
            Case {
                name: "AgentExited requests all-session invalidation without replay",
                error: || AcpError::AgentExited,
                signal: ControlSignal::Steer,
                expected_outcome: "AgentExited",
                batch_preserved: false,
                expected_reason: None,
                invalidate_all: true,
            },
            Case {
                name: "AgentExited + Cancel still drops the batch",
                error: || AcpError::AgentExited,
                signal: ControlSignal::Cancel,
                expected_outcome: "AgentExited",
                batch_preserved: false,
                expected_reason: None,
                invalidate_all: true,
            },
            Case {
                name: "IdleTimeout maps to Timeout(Idle)",
                error: || AcpError::IdleTimeout(Duration::from_secs(30)),
                signal: ControlSignal::Steer,
                expected_outcome: "Timeout(Idle)",
                batch_preserved: true,
                expected_reason: Some(CancelReason::Steer),
                invalidate_all: false,
            },
        ];

        for case in cases {
            let channel_id = Uuid::new_v4();
            let batch = one_event_batch(channel_id);
            let failure = classify_control_cancel_failure(
                &ctx,
                (case.error)(),
                case.signal.clone(),
                Some(batch),
            );
            assert_outcome_matches(&failure.outcome, case.expected_outcome);
            assert_eq!(
                failure.invalidate_all, case.invalidate_all,
                "{}: invalidate_all mismatch",
                case.name
            );
            match case.expected_reason {
                Some(reason) => {
                    let batch = failure
                        .retry_batch
                        .unwrap_or_else(|| panic!("{}: batch must be preserved", case.name));
                    assert_eq!(
                        batch.cancel_reason,
                        Some(reason),
                        "{}: cancel_reason mismatch",
                        case.name
                    );
                }
                None => assert!(
                    failure.retry_batch.is_none(),
                    "{}: batch must be dropped, got {:?}",
                    case.name,
                    failure.retry_batch
                ),
            }
            assert_eq!(
                case.batch_preserved,
                case.expected_reason.is_some(),
                "{}: test table internally inconsistent",
                case.name
            );
        }
    }

    // ── turn liveness emission ───────────────────────────────────────────────

    fn liveness_count(handle: &observer::ObserverHandle) -> usize {
        handle
            .snapshot()
            .iter()
            .filter(|e| e.kind == "turn_liveness")
            .count()
    }

    fn open_liveness_state() -> Arc<Mutex<LivenessState>> {
        Arc::new(Mutex::new(LivenessState {
            closed: false,
            session_id: None,
        }))
    }

    #[tokio::test(start_paused = true)]
    async fn test_liveness_stops_before_completion_frame() {
        let observer = observer::ObserverHandle::in_process();
        let context =
            observer::context_for_turn(None, None, "t-1".into(), "2026-07-14T21:00:00Z".into());
        let completion_context = observer::context_for(None, None, Some("t-1".into()));
        let completion_observer = observer.clone();
        let completion_handle = tokio::spawn(async move {
            let state = open_liveness_state();
            let _liveness_guard = LivenessGuard::new(
                tokio::spawn(run_turn_liveness(
                    Some(observer.clone()),
                    Some(0),
                    context,
                    Duration::from_secs(10),
                    Arc::clone(&state),
                )),
                state,
            );
            tokio::time::sleep(Duration::from_secs(25)).await;
            observer.emit(
                "turn_completed",
                Some(0),
                &completion_context,
                serde_json::json!({}),
            );
        });
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(25)).await;
        completion_handle.await.unwrap();
        tokio::task::yield_now().await;

        let events = completion_observer.snapshot();
        let completion_index = events
            .iter()
            .position(|event| event.kind == "turn_completed")
            .expect("turn must complete");
        assert!(
            events[..completion_index]
                .iter()
                .all(|event| event.kind != "turn_liveness"
                    || event.turn_id.as_deref() == Some("t-1")),
            "pre-completion liveness must belong to the active turn"
        );
        assert!(
            events[completion_index + 1..]
                .iter()
                .all(|event| event.kind != "turn_liveness"),
            "liveness must be aborted before a completion frame is emitted"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_liveness_fires_until_guard_drops() {
        let observer = observer::ObserverHandle::in_process();
        let started_at = "2026-07-14T21:00:00Z".to_string();
        let context = observer::context_for_turn(None, None, "t-1".into(), started_at.clone());
        let state = open_liveness_state();
        let guard = LivenessGuard::new(
            tokio::spawn(run_turn_liveness(
                Some(observer.clone()),
                Some(0),
                context,
                Duration::from_secs(10),
                Arc::clone(&state),
            )),
            state,
        );
        tokio::task::yield_now().await;

        // First liveness tick at 10s and the second at 20s.
        tokio::time::advance(Duration::from_secs(25)).await;
        tokio::task::yield_now().await;
        assert_eq!(liveness_count(&observer), 2);

        let pings: Vec<_> = observer
            .snapshot()
            .into_iter()
            .filter(|e| e.kind == "turn_liveness")
            .collect();
        assert!(pings
            .iter()
            .all(|event| event.turn_id.as_deref() == Some("t-1")));
        assert!(pings
            .iter()
            .all(|event| event.started_at.as_deref() == Some(&started_at)));
        assert!(pings
            .iter()
            .all(|event| event.payload == serde_json::json!({})));
        assert_eq!(
            serde_json::to_value(&pings[0]).unwrap()["startedAt"],
            started_at,
            "turn start must serialize in the observer envelope"
        );

        // The guard is owned by `run_prompt_task`; dropping it aborts liveness
        // so completed, cancelled, and errored turns cannot emit late pings.
        drop(guard);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert_eq!(liveness_count(&observer), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn test_liveness_backfills_session_id_after_resolution() {
        let observer = observer::ObserverHandle::in_process();
        let context =
            observer::context_for_turn(None, None, "t-1".into(), "2026-07-14T21:00:00Z".into());
        let state = open_liveness_state();
        let guard = LivenessGuard::new(
            tokio::spawn(run_turn_liveness(
                Some(observer.clone()),
                Some(0),
                context,
                Duration::from_secs(10),
                Arc::clone(&state),
            )),
            state,
        );
        tokio::task::yield_now().await;

        // First tick at 10s fires before the session resolves — must carry
        // no session ID, matching every other pre-resolution observer frame.
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        guard.set_session_id("sess-1".to_string());

        // Second tick at 20s fires after resolution — must carry it.
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;

        let pings: Vec<_> = observer
            .snapshot()
            .into_iter()
            .filter(|e| e.kind == "turn_liveness")
            .collect();
        assert_eq!(pings.len(), 2);
        assert_eq!(
            pings[0].session_id, None,
            "pre-resolution ping must not carry a session ID"
        );
        assert_eq!(
            pings[1].session_id.as_deref(),
            Some("sess-1"),
            "post-resolution ping must carry the resolved session ID"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_liveness_disabled_when_interval_zero_emits_nothing() {
        let observer = observer::ObserverHandle::in_process();
        let context = observer::context_for(None, None, Some("t-1".into()));
        let liveness = run_turn_liveness(
            Some(observer.clone()),
            Some(0),
            context,
            Duration::ZERO,
            open_liveness_state(),
        );
        tokio::pin!(liveness);

        tokio::select! {
            biased;
            () = tokio::time::sleep(Duration::from_secs(120)) => {}
            _ = &mut liveness => unreachable!("disabled liveness future never resolves"),
        }

        assert_eq!(liveness_count(&observer), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_liveness_without_observer_emits_nothing() {
        // A turn that never started has no observer handle — the future must
        // park without emitting or panicking.
        let context = observer::context_for(None, None, Some("t-1".into()));
        let liveness = run_turn_liveness(
            None,
            None,
            context,
            Duration::from_secs(10),
            open_liveness_state(),
        );
        tokio::pin!(liveness);

        tokio::select! {
            biased;
            () = tokio::time::sleep(Duration::from_secs(120)) => {}
            _ = &mut liveness => unreachable!("handle-less liveness future never resolves"),
        }
        // No observer to assert against — reaching here without panic is the test.
    }

    // These two tests pin the shutdown mechanism itself (F1), not timing.
    // The existing paused-clock tests above only prove liveness stops
    // *eventually* after a guard drop — under `tokio::time::pause`, the
    // scheduler never actually interleaves a drop with an in-flight emit, so
    // they cannot catch a real cross-thread race between `LivenessGuard::drop`
    // and `run_turn_liveness`'s tick. These assert the two halves of the
    // contract directly: the check gates the emit with the flag pre-set (no
    // `LivenessGuard` involved), and `drop` cannot return while the shared
    // lock is held by an in-flight tick (real OS threads, no cooperative
    // scheduling to serialize the race away).

    #[tokio::test(start_paused = true)]
    async fn test_liveness_emits_nothing_once_closed_flag_is_set() {
        let observer = observer::ObserverHandle::in_process();
        let context =
            observer::context_for_turn(None, None, "t-1".into(), "2026-07-14T21:00:00Z".into());
        // Set directly, bypassing `LivenessGuard` — isolates the read side of
        // the contract: the check under the lock must gate the emit on its own.
        let state = Arc::new(Mutex::new(LivenessState {
            closed: true,
            session_id: None,
        }));
        let liveness = run_turn_liveness(
            Some(observer.clone()),
            Some(0),
            context,
            Duration::from_secs(10),
            state,
        );
        tokio::time::timeout(Duration::from_secs(60), liveness)
            .await
            .expect("run_turn_liveness must return once closed, not park forever");

        assert_eq!(
            liveness_count(&observer),
            0,
            "the pre-set closed flag must suppress every tick's emit"
        );
    }

    #[test]
    fn test_liveness_guard_drop_blocks_while_emit_lock_is_held() {
        // Standing in for a tick that has already entered its critical
        // section: hold the shared lock before the guard drops.
        let state = Arc::new(Mutex::new(LivenessState {
            closed: false,
            session_id: None,
        }));
        let held = state.lock().unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.spawn(std::future::pending::<()>());
        let guard = LivenessGuard::new(handle, Arc::clone(&state));

        let (tx, rx) = std::sync::mpsc::channel();
        let drop_thread = std::thread::spawn(move || {
            drop(guard);
            tx.send(()).unwrap();
        });

        // While the emit lock is held, `drop` cannot have completed: it takes
        // the same lock before it sets the flag and aborts. A bounded timeout
        // proves non-completion by construction of the lock, not the clock —
        // `recv_timeout` returning `Timeout` here only holds because the
        // mutex is genuinely contended; it cannot pass by scheduling luck.
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(200)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout),
            "drop must block while the tick's emit lock is held"
        );

        // Release the lock — drop can now acquire it, set the flag, and abort.
        drop(held);
        rx.recv_timeout(Duration::from_secs(5))
            .expect("drop must complete once the emit lock is released");
        drop_thread.join().unwrap();
        assert!(
            state.lock().unwrap().closed,
            "closed flag must be set by the time drop has returned"
        );
    }

    // ── steer_rx invariant tests ──────────────────────────────────────────
    //
    // These pin the `send_prompt_result` invariant: `steer_rx` is always
    // `None` on any agent returned to the pool, regardless of which exit
    // path fired.
    //
    // Test 1 (session-create-error path): installs a receiver, then calls
    // `send_prompt_result` without the read loop running `take()` — simulating
    // any early-return arm (e.g. session-create failure). The receiver must be
    // cleared and the next `install_steer_rx` must not panic.
    //
    // Test 2 (post-read-loop path): receiver is already `None` (the read loop
    // already consumed it via `take()`). `send_prompt_result` is idempotent —
    // `steer_rx` stays `None` and the next `install_steer_rx` still does not
    // panic.

    /// After an early-return path (receiver installed but read loop never ran),
    /// the returned agent's `steer_rx` is `None` and a subsequent
    /// `install_steer_rx` does not panic.
    #[tokio::test]
    async fn test_send_prompt_result_clears_steer_rx_on_early_return() {
        let acp = AcpClient::spawn(
            "bash",
            &["-c".to_string(), "sleep 10".to_string()],
            &[],
            false,
        )
        .await
        .expect("failed to spawn test agent");
        let mut agent = OwnedAgent {
            index: 0,
            acp,
            state: SessionState::default(),
            model_capabilities: None,
            desired_model: None,
            model_overridden: false,
            desired_model_request_id: None,
            desired_model_pending_ack: false,
            startup_effort: None,
            agent_name: "unknown".into(),
            goose_system_prompt_supported: None,
            protocol_version: 2,
        };

        // Simulate dispatch: install a steer receiver (normally done by
        // `dispatch_pending` before `run_prompt_task` is spawned).
        let (_steer_tx, steer_rx) = tokio::sync::mpsc::channel::<SteerRequest>(1);
        agent.acp.install_steer_rx(steer_rx);

        // Simulate session-create error: early-return path calls
        // `send_prompt_result` without the read loop ever running `take()`.
        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel::<PromptResult>();
        let source = PromptSource::Heartbeat;
        send_prompt_result(
            &result_tx,
            "test-turn-id",
            agent,
            source,
            PromptOutcome::Error(AcpError::Protocol("simulated session-create error".into())),
            None,
        );

        // Receive the PromptResult back from the channel.
        let mut result = result_rx.recv().await.expect("PromptResult must be sent");

        // steer_rx must be cleared even though the read loop never ran take().
        assert!(
            result.agent.acp.steer_rx_is_none(),
            "steer_rx must be None after send_prompt_result on error path"
        );

        // The next dispatch can now install a fresh receiver without panicking.
        let (_steer_tx2, steer_rx2) = tokio::sync::mpsc::channel::<SteerRequest>(1);
        result.agent.acp.install_steer_rx(steer_rx2);
        // Reaching here without a panic is the test.
    }

    /// After a successful prompt (read loop already consumed `steer_rx` via
    /// `take()`), `send_prompt_result` is a no-op — `steer_rx` stays `None`
    /// and the next `install_steer_rx` does not panic.
    #[tokio::test]
    async fn test_send_prompt_result_is_noop_when_steer_rx_already_consumed() {
        let acp = AcpClient::spawn(
            "bash",
            &["-c".to_string(), "sleep 10".to_string()],
            &[],
            false,
        )
        .await
        .expect("failed to spawn test agent");
        let agent = OwnedAgent {
            index: 0,
            acp,
            state: SessionState::default(),
            model_capabilities: None,
            desired_model: None,
            model_overridden: false,
            desired_model_request_id: None,
            desired_model_pending_ack: false,
            startup_effort: None,
            agent_name: "unknown".into(),
            goose_system_prompt_supported: None,
            protocol_version: 2,
        };

        // Simulate a completed turn: `steer_rx` was consumed by the read loop
        // (`take()` was called), so it is already `None` when the turn ends.
        assert!(
            agent.acp.steer_rx_is_none(),
            "precondition: steer_rx starts as None"
        );

        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel::<PromptResult>();
        let source = PromptSource::Heartbeat;
        send_prompt_result(
            &result_tx,
            "test-turn-id",
            agent,
            source,
            PromptOutcome::Ok(StopReason::EndTurn),
            None,
        );

        let mut result = result_rx.recv().await.expect("PromptResult must be sent");

        // Still None — clear_steer_rx on an already-None field is idempotent.
        assert!(
            result.agent.acp.steer_rx_is_none(),
            "steer_rx must remain None after send_prompt_result on happy path"
        );

        // The next dispatch can install a fresh receiver without panicking.
        let (_steer_tx, steer_rx) = tokio::sync::mpsc::channel::<SteerRequest>(1);
        result.agent.acp.install_steer_rx(steer_rx);
        // Reaching here without a panic is the test.
    }

    // ── NIP-AM emit-hook unit tests ────────────────────────────────────────

    /// `acp_stop_to_core` maps all ACP stop reasons to the correct NIP-AM
    /// variants without panicking on any input.
    #[test]
    fn test_acp_stop_to_core_maps_all_variants() {
        use buzz_core::agent_turn_metric::StopReason as CoreStop;
        assert_eq!(acp_stop_to_core(&StopReason::EndTurn), CoreStop::EndTurn);
        assert_eq!(
            acp_stop_to_core(&StopReason::Cancelled),
            CoreStop::Cancelled
        );
        assert_eq!(
            acp_stop_to_core(&StopReason::MaxTokens),
            CoreStop::MaxTokens
        );
        assert_eq!(
            acp_stop_to_core(&StopReason::MaxTurnRequests),
            CoreStop::Unknown
        );
        assert_eq!(acp_stop_to_core(&StopReason::Refusal), CoreStop::Unknown);
    }

    /// `publish_agent_turn_metric` is a no-op when `usage` is `None`.
    #[tokio::test]
    async fn test_publish_agent_turn_metric_noop_on_no_usage() {
        let ctx = make_prompt_context_no_owner();
        // usage = None → early return, no panic.
        publish_agent_turn_metric(
            &ctx,
            None,
            None,
            "sess-1",
            "turn-1",
            Some(buzz_core::agent_turn_metric::StopReason::EndTurn),
        )
        .await;
    }

    /// `publish_agent_turn_metric` is a no-op when `owner_pubkey` is absent.
    #[tokio::test]
    async fn test_publish_agent_turn_metric_noop_on_no_owner() {
        let ctx = make_prompt_context_no_owner();
        let usage = crate::usage::TurnUsage {
            session_id: "sess-1".to_string(),
            turn_seq: 1,
            delta_reliable: true,
            turn_input_tokens: Some(100),
            turn_output_tokens: Some(50),
            turn_total_tokens: None,
            turn_cost_usd: None,
            turn_cache_read_tokens: None,
            turn_cache_write_tokens: None,
            cumulative_input_tokens: Some(100),
            cumulative_output_tokens: Some(50),
            cumulative_total_tokens: None,
            cumulative_cost_usd: None,
            cumulative_cache_read_tokens: None,
            cumulative_cache_write_tokens: None,
            model: None,
            pricing_identity: None,
        };
        // owner_pubkey = None → early return, no panic.
        publish_agent_turn_metric(
            &ctx,
            Some(usage),
            None,
            "sess-1",
            "turn-1",
            Some(buzz_core::agent_turn_metric::StopReason::EndTurn),
        )
        .await;
    }

    /// `publish_agent_turn_metric` encrypts the payload when owner is present
    /// (the HTTP submit will fail in tests, but we verify no panic and the
    /// encrypt/sign path executes).
    #[tokio::test]
    async fn test_publish_agent_turn_metric_encrypts_with_owner() {
        let agent_keys = nostr::Keys::generate();
        let owner_keys = nostr::Keys::generate();
        let ctx = make_prompt_context_with_owner(&agent_keys, owner_keys.public_key());
        let usage = crate::usage::TurnUsage {
            session_id: "sess-1".to_string(),
            turn_seq: 1,
            delta_reliable: true,
            turn_input_tokens: Some(200),
            turn_output_tokens: Some(80),
            turn_total_tokens: None,
            turn_cost_usd: Some(0.001),
            turn_cache_read_tokens: None,
            turn_cache_write_tokens: None,
            cumulative_input_tokens: Some(200),
            cumulative_output_tokens: Some(80),
            cumulative_total_tokens: None,
            cumulative_cost_usd: Some(0.001),
            cumulative_cache_read_tokens: None,
            cumulative_cache_write_tokens: None,
            model: None,
            pricing_identity: None,
        };
        // Will try to publish and fail (no real relay) but must not panic.
        publish_agent_turn_metric(
            &ctx,
            Some(usage),
            Some(uuid::Uuid::new_v4()),
            "sess-1",
            "turn-1",
            Some(buzz_core::agent_turn_metric::StopReason::EndTurn),
        )
        .await;
    }

    /// Regression for the control-cancel drain: `publish_agent_turn_metric`
    /// with a `Cancelled` stop reason and pending usage executes without panic
    /// (encrypt+sign path). This mirrors the control-signal arm that previously
    /// returned early without draining usage.
    #[tokio::test]
    async fn test_publish_agent_turn_metric_cancelled_stop_reason() {
        let agent_keys = nostr::Keys::generate();
        let owner_keys = nostr::Keys::generate();
        let ctx = make_prompt_context_with_owner(&agent_keys, owner_keys.public_key());
        let usage = crate::usage::TurnUsage {
            session_id: "sess-cancel".to_string(),
            turn_seq: 2,
            delta_reliable: true,
            turn_input_tokens: Some(50),
            turn_output_tokens: Some(20),
            turn_total_tokens: None,
            turn_cost_usd: None,
            turn_cache_read_tokens: None,
            turn_cache_write_tokens: None,
            cumulative_input_tokens: Some(150),
            cumulative_output_tokens: Some(70),
            cumulative_total_tokens: None,
            cumulative_cost_usd: None,
            cumulative_cache_read_tokens: None,
            cumulative_cache_write_tokens: None,
            model: None,
            pricing_identity: None,
        };
        // Must not panic; HTTP submit will fail (no real relay) — that's fine.
        publish_agent_turn_metric(
            &ctx,
            Some(usage),
            Some(uuid::Uuid::new_v4()),
            "sess-cancel",
            "turn-cancel",
            Some(buzz_core::agent_turn_metric::StopReason::Cancelled),
        )
        .await;
    }

    /// `publish_agent_turn_metric` uses `ctx.harness_name` in the payload.
    /// A buzz-agent-commanded context must not panic — verifies the harness
    /// field flows through encrypt/sign without error.
    #[tokio::test]
    async fn test_publish_agent_turn_metric_buzz_agent_harness_name() {
        let agent_keys = nostr::Keys::generate();
        let owner_keys = nostr::Keys::generate();
        let mut ctx = make_prompt_context_with_owner(&agent_keys, owner_keys.public_key());
        ctx.harness_name = "buzz-agent".to_string();
        let usage = crate::usage::TurnUsage {
            session_id: "sess-ba".to_string(),
            turn_seq: 1,
            delta_reliable: false, // first turn from buzz-agent
            turn_input_tokens: None,
            turn_output_tokens: None,
            turn_total_tokens: None,
            turn_cost_usd: None,
            turn_cache_read_tokens: None,
            turn_cache_write_tokens: None,
            cumulative_input_tokens: Some(400),
            cumulative_output_tokens: Some(100),
            cumulative_total_tokens: None,
            cumulative_cost_usd: None,
            cumulative_cache_read_tokens: None,
            cumulative_cache_write_tokens: None,
            model: None,
            pricing_identity: None,
        };
        // Will try to publish (encrypt succeeds) and fail HTTP (no relay) — must not panic.
        publish_agent_turn_metric(
            &ctx,
            Some(usage),
            Some(uuid::Uuid::new_v4()),
            "sess-ba",
            "turn-ba",
            Some(buzz_core::agent_turn_metric::StopReason::EndTurn),
        )
        .await;
    }

    /// `build_turn_metric_counts` maps exact turn and cumulative totals from
    /// `TurnUsage` to the corresponding `TokenCounts.total_tokens` fields.
    /// Reverting the production fields at the call site to `None` would break
    /// this test; the test constrains the real code path.
    #[test]
    fn test_build_turn_metric_counts_exact_totals_map_through() {
        let usage = crate::usage::TurnUsage {
            session_id: "sess-total".to_string(),
            turn_seq: 2,
            delta_reliable: true,
            turn_input_tokens: Some(100),
            turn_output_tokens: Some(30),
            turn_total_tokens: Some(130), // genuine per-turn total
            turn_cost_usd: None,
            turn_cache_read_tokens: None,
            turn_cache_write_tokens: None,
            cumulative_input_tokens: Some(500),
            cumulative_output_tokens: Some(120),
            cumulative_total_tokens: Some(620), // genuine cumulative total
            cumulative_cost_usd: None,
            cumulative_cache_read_tokens: None,
            cumulative_cache_write_tokens: None,
            model: None,
            pricing_identity: None,
        };

        let (turn, cumulative) = crate::pool::build_turn_metric_counts(&usage);

        // Serialise to JSON — this is what ultimately goes on the wire.
        let turn_json = serde_json::to_value(turn.as_ref().expect("turn counts present")).unwrap();
        let cum_json =
            serde_json::to_value(cumulative.as_ref().expect("cumulative counts present")).unwrap();

        // Per-turn total must be the genuine provider-reported value.
        assert_eq!(
            turn_json["totalTokens"],
            serde_json::json!(130),
            "per-turn total must map to TokenCounts.totalTokens in wire JSON"
        );
        assert_eq!(turn_json["inputTokens"], serde_json::json!(100));
        assert_eq!(turn_json["outputTokens"], serde_json::json!(30));

        // Cumulative total must be the genuine session total.
        assert_eq!(
            cum_json["totalTokens"],
            serde_json::json!(620),
            "cumulative total must map to TokenCounts.totalTokens in wire JSON"
        );
        assert_eq!(cum_json["inputTokens"], serde_json::json!(500));
        assert_eq!(cum_json["outputTokens"], serde_json::json!(120));
    }

    /// When totals are absent, `build_turn_metric_counts` must produce null
    /// `total_tokens` — never a derived input+output sum (NIP-AM MUST NOT).
    /// Reverting the production fields to hardcoded `None` would leave this test
    /// passing but input/output would disagree, making the null-path detectable.
    #[test]
    fn test_build_turn_metric_counts_null_totals_never_derived() {
        let usage = crate::usage::TurnUsage {
            session_id: "sess-nototal".to_string(),
            turn_seq: 1,
            delta_reliable: true,
            turn_input_tokens: Some(200),
            turn_output_tokens: Some(60),
            turn_total_tokens: None, // provider did not supply a total
            turn_cost_usd: None,
            turn_cache_read_tokens: None,
            turn_cache_write_tokens: None,
            cumulative_input_tokens: Some(200),
            cumulative_output_tokens: Some(60),
            cumulative_total_tokens: None, // session has no total
            cumulative_cost_usd: None,
            cumulative_cache_read_tokens: None,
            cumulative_cache_write_tokens: None,
            model: None,
            pricing_identity: None,
        };

        let (turn, cumulative) = crate::pool::build_turn_metric_counts(&usage);

        let turn_json = serde_json::to_value(turn.as_ref().expect("turn counts present")).unwrap();
        let cum_json =
            serde_json::to_value(cumulative.as_ref().expect("cumulative counts present")).unwrap();

        // total_tokens must be null in the wire JSON.
        assert!(
            turn_json["totalTokens"].is_null(),
            "absent turn total must serialize as null — not derived from in+out"
        );
        assert!(
            cum_json["totalTokens"].is_null(),
            "absent cumulative total must serialize as null — not derived from in+out"
        );

        // Input/output must still carry their real values.
        assert_eq!(
            turn_json["inputTokens"],
            serde_json::json!(200),
            "inputTokens must be present even when total is absent"
        );
        assert_eq!(
            turn_json["outputTokens"],
            serde_json::json!(60),
            "outputTokens must be present even when total is absent"
        );

        // The null total must not equal the input+output sum — it must be genuinely null.
        let derived_sum = serde_json::json!(200u64 + 60u64);
        assert_ne!(
            turn_json["totalTokens"], derived_sum,
            "total_tokens must never equal input+output when provider omitted it"
        );
    }

    /// A payload with nonzero `accumulatedCachedInputTokens` on the second turn
    /// must produce a kind:44200 payload where `cumulative.cacheReadTokens` is
    /// nonzero and `turn.cacheReadTokens` reflects the per-turn delta.
    /// This is the acceptance-criterion test: it proves the threading is live,
    /// not hardcoded to None.
    #[test]
    fn test_build_turn_metric_counts_cache_read_tokens_thread_through() {
        // Wire-parse a buzz-agent payload with cache, run it through the tracker,
        // and verify the published TokenCounts carry the cache field.
        let raw1 = serde_json::json!({
            "sessionId": "cache-sess",
            "update": {
                "sessionUpdate": "usage_update",
                "accumulatedInputTokens": 15_091,
                "accumulatedOutputTokens": 156,
                "accumulatedCachedInputTokens": 5_033,
            }
        });
        let raw2 = serde_json::json!({
            "sessionId": "cache-sess",
            "update": {
                "sessionUpdate": "usage_update",
                "accumulatedInputTokens": 28_500,
                "accumulatedOutputTokens": 310,
                "accumulatedCachedInputTokens": 11_000,
            }
        });

        let mut tracker = crate::usage::UsageTracker::default();

        // Turn 1 — establish baseline (delta unreliable, but cumulative still present).
        tracker.begin_turn("cache-sess");
        if let crate::usage::GooseSessionUpdateVariant::UsageUpdate(p) =
            serde_json::from_value::<crate::usage::GooseSessionUpdateNotification>(raw1)
                .unwrap()
                .update
        {
            tracker.record("cache-sess", &p);
        }
        let t1 = tracker.take().expect("turn 1");

        // Turn 1: cumulative must carry the cache count; turn delta is None (no baseline).
        let (turn1, cum1) = crate::pool::build_turn_metric_counts(&t1);
        // delta_reliable = false on first turn → no turn counts.
        assert!(turn1.is_none(), "first turn: no reliable turn counts");
        let cum1 = cum1.expect("cumulative always present");
        assert_eq!(
            cum1.cache_read_tokens,
            Some(5_033),
            "cumulative.cacheReadTokens must be 5033 after turn 1"
        );

        // Turn 2 — delta reliable.
        tracker.begin_turn("cache-sess");
        if let crate::usage::GooseSessionUpdateVariant::UsageUpdate(p) =
            serde_json::from_value::<crate::usage::GooseSessionUpdateNotification>(raw2)
                .unwrap()
                .update
        {
            tracker.record("cache-sess", &p);
        }
        let t2 = tracker.take().expect("turn 2");

        let (turn2, cum2) = crate::pool::build_turn_metric_counts(&t2);

        let turn2 = turn2.expect("reliable turn counts on turn 2");
        // Per-turn cache delta: 11_000 - 5_033 = 5_967.
        assert_eq!(
            turn2.cache_read_tokens,
            Some(5_967),
            "turn.cacheReadTokens must be the per-turn delta"
        );
        // cache_write_tokens: None in this test because the payloads don't
        // include accumulatedCacheWriteTokens (Anthropic cache-read only test).
        assert!(
            turn2.cache_write_tokens.is_none(),
            "cache_write_tokens must be None when harness omits the field"
        );

        let cum2 = cum2.expect("cumulative always present");
        assert_eq!(
            cum2.cache_read_tokens,
            Some(11_000),
            "cumulative.cacheReadTokens must be 11_000 after turn 2"
        );
        assert!(
            cum2.cache_write_tokens.is_none(),
            "cache_write_tokens must be None on cumulative too"
        );
    }

    pub(super) fn make_prompt_context_no_owner() -> PromptContext {
        let agent_keys = nostr::Keys::generate();
        make_prompt_context_impl(&agent_keys, None)
    }

    fn make_prompt_context_with_owner(
        agent_keys: &nostr::Keys,
        owner_pubkey: nostr::PublicKey,
    ) -> PromptContext {
        make_prompt_context_impl(agent_keys, Some(owner_pubkey))
    }

    fn make_prompt_context_impl(
        agent_keys: &nostr::Keys,
        owner_pubkey: Option<nostr::PublicKey>,
    ) -> PromptContext {
        use crate::relay::RestClient;
        PromptContext {
            mcp_servers: vec![],
            trusted_mcp_factory: None,
            session_recovery: None,
            initial_message: None,
            idle_timeout: Duration::from_secs(60),
            max_turn_duration: Duration::from_secs(120),
            turn_liveness_interval: Duration::ZERO,
            dedup_mode: DedupMode::Drop,
            reply_placement: crate::reply_placement::ReplyPlacement::Thread,
            system_prompt: None,
            session_title: None,
            team_instructions: None,
            workspace_project_channel: None,
            workspace_project_address: None,
            workspace_project_repository: None,
            workspace_project_revision: None,
            heartbeat_prompt: None,
            base_prompt: None,
            cwd: ".".to_string(),
            rest_client: RestClient {
                http: reqwest::Client::new(),
                base_url: "http://127.0.0.1:0".to_string(),
                keys: agent_keys.clone(),
                auth_tag_json: None,
            },
            channel_info: ChannelInfoResolver::new(
                std::collections::HashMap::new(),
                RestClient {
                    http: reqwest::Client::new(),
                    base_url: "http://127.0.0.1:0".to_string(),
                    keys: agent_keys.clone(),
                    auth_tag_json: None,
                },
            ),
            context_message_limit: 0,
            max_turns_per_session: 0,
            permission_mode: PermissionMode::Default,
            agent_keys: agent_keys.clone(),
            agent_owner_pubkey: owner_pubkey,
            memory_enabled: false,
            harness_name: "goose".to_string(),
            relay_url: "ws://127.0.0.1:3000".to_string(),
        }
    }

    // ── huddle instructions ─────────────────────────────────────────────────

    #[test]
    fn huddle_instructions_append_as_system_section() {
        assert_eq!(
            with_huddle_instructions(Some("base".into()), Some("  reply now  ")).as_deref(),
            Some("base\n\n<huddle-instructions>\nreply now\n</huddle-instructions>")
        );
    }

    #[test]
    fn huddle_instructions_require_owner_signature_and_channel() {
        let owner = Keys::generate();
        let stranger = Keys::generate();
        let channel = Uuid::parse_str("00f1ccaf-1506-4dd7-9a0e-fa67e9e486ae").unwrap();
        let event = |keys: &Keys, channel_id: Uuid| {
            let channel_id = channel_id.to_string();
            let h_tag = Tag::parse(["h", channel_id.as_str()]).unwrap();
            serde_json::to_value(
                EventBuilder::new(
                    Kind::Custom(buzz_core::kind::KIND_HUDDLE_GUIDELINES as u16),
                    "reply immediately",
                )
                .tags([h_tag])
                .sign_with_keys(keys)
                .unwrap(),
            )
            .unwrap()
        };

        assert_eq!(
            huddle_instructions_from_query_response(
                &[event(&owner, channel)],
                channel,
                &owner.public_key(),
            )
            .as_deref(),
            Some("reply immediately")
        );
        assert!(huddle_instructions_from_query_response(
            &[event(&stranger, channel)],
            channel,
            &owner.public_key(),
        )
        .is_none());
        assert!(huddle_instructions_from_query_response(
            &[event(&owner, Uuid::new_v4())],
            channel,
            &owner.public_key(),
        )
        .is_none());
    }

    // ── render_canvas_section ────────────────────────────────────────────────

    #[test]
    fn test_render_canvas_section_produces_exact_shape() {
        let id = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        let ts = "2024-01-15T10:30:00+00:00";
        let uuid = "00f1ccaf-1506-4dd7-9a0e-fa67e9e486ae";
        let section = render_canvas_section(id, ts, uuid);
        assert_eq!(
            section,
            "<channel-canvas>\n\
             Canvas revision (event ID): a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2\n\
             Last modified: 2024-01-15T10:30:00+00:00\n\
             Fetch current content with: buzz canvas get --channel 00f1ccaf-1506-4dd7-9a0e-fa67e9e486ae\n\
             </channel-canvas>"
        );
    }

    // ── with_canvas ──────────────────────────────────────────────────────────

    #[test]
    fn test_with_canvas_appends_to_existing_prompt() {
        let result = with_canvas(Some("base content".into()), Some("[Channel Canvas]\nstuff"));
        assert_eq!(
            result.unwrap(),
            "base content\n\n<channel-canvas>\nstuff\n</channel-canvas>"
        );
    }

    #[test]
    fn test_with_canvas_returns_canvas_alone_when_no_prompt() {
        let result = with_canvas(None, Some("[Channel Canvas]\nstuff"));
        assert_eq!(
            result.unwrap(),
            "<channel-canvas>\nstuff\n</channel-canvas>"
        );
    }

    #[test]
    fn test_with_canvas_returns_prompt_alone_when_no_canvas() {
        let result = with_canvas(Some("base content".into()), None);
        assert_eq!(result.unwrap(), "base content");
    }

    #[test]
    fn test_with_canvas_returns_none_when_both_absent() {
        let result = with_canvas(None, None);
        assert!(result.is_none());
    }

    // ── canvas_sections cache invalidation ───────────────────────────────────

    #[test]
    fn test_invalidate_channel_clears_canvas_section() {
        let ch = Uuid::new_v4();
        let mut s = SessionState::default();
        s.sessions.insert(conv(ch), "sess".into());
        s.canvas_sections
            .insert(conv(ch), "[Channel Canvas]\nrev abc".into());

        s.invalidate_channel(&ch);

        assert!(!s.canvas_sections.contains_key(&conv(ch)));
        assert!(!s.sessions.contains_key(&conv(ch)));
    }

    #[test]
    fn test_invalidate_all_clears_canvas_sections() {
        let ch_a = Uuid::new_v4();
        let ch_b = Uuid::new_v4();
        let mut s = SessionState::default();
        s.canvas_sections.insert(conv(ch_a), "canvas-a".into());
        s.canvas_sections.insert(conv(ch_b), "canvas-b".into());
        s.sessions.insert(conv(ch_a), "sess-a".into());

        s.invalidate_all();

        assert!(s.canvas_sections.is_empty());
        assert!(s.sessions.is_empty());
    }

    #[test]
    fn test_invalidate_channel_leaves_other_channels_canvas_intact() {
        let ch_a = Uuid::new_v4();
        let ch_b = Uuid::new_v4();
        let mut s = SessionState::default();
        s.sessions.insert(conv(ch_a), "sess-a".into());
        s.sessions.insert(conv(ch_b), "sess-b".into());
        s.canvas_sections.insert(conv(ch_a), "canvas-a".into());
        s.canvas_sections.insert(conv(ch_b), "canvas-b".into());

        s.invalidate_channel(&ch_a);

        assert!(!s.canvas_sections.contains_key(&conv(ch_a)));
        assert_eq!(s.canvas_sections.get(&conv(ch_b)).unwrap(), "canvas-b");
    }

    #[test]
    fn test_has_channel_state_true_when_only_canvas_section_present() {
        let ch = Uuid::new_v4();
        let mut s = SessionState::default();
        s.canvas_sections.insert(conv(ch), "canvas".into());
        assert!(s.has_channel_state(&ch));
    }

    // ── canvas_section_from_query_response ───────────────────────────────────

    const CHANNEL_UUID: &str = "00f1ccaf-1506-4dd7-9a0e-fa67e9e486ae";

    /// Build a real, cryptographically signed Nostr canvas event for tests.
    ///
    /// Includes the correct kind (40100) and an `h` tag carrying `CHANNEL_UUID`
    /// so all structural and content validations pass.
    fn make_canvas_event_value(content: &str) -> serde_json::Value {
        let keys = Keys::generate();
        let h_tag = Tag::parse(["h", CHANNEL_UUID]).expect("h tag");
        let event = EventBuilder::new(Kind::Custom(buzz_core::kind::KIND_CANVAS as u16), content)
            .tags([h_tag])
            .sign_with_keys(&keys)
            .expect("sign");
        serde_json::to_value(&event).expect("serialise")
    }

    #[test]
    fn test_canvas_section_from_query_response_happy_path() {
        let ev = make_canvas_event_value("# Team instructions\nBe helpful.");
        let id = ev["id"].as_str().unwrap().to_string();
        let result = canvas_section_from_query_response(&[ev], CHANNEL_UUID);
        let section = result.expect("expected Some");
        assert!(section.contains(&id), "section must contain the event id");
        assert!(section.contains("buzz canvas get --channel"));
        assert!(section.contains(CHANNEL_UUID));
        assert!(section.starts_with("<channel-canvas>"));
        // Timestamp must use Z suffix, not +00:00
        assert!(section.contains('Z'), "timestamp must use Z suffix");
    }

    #[test]
    fn test_canvas_section_from_query_response_empty_array_returns_none() {
        let result = canvas_section_from_query_response(&[], CHANNEL_UUID);
        assert!(result.is_none());
    }

    #[test]
    fn test_canvas_section_from_query_response_blank_content_returns_none() {
        let ev = make_canvas_event_value("   ");
        let result = canvas_section_from_query_response(&[ev], CHANNEL_UUID);
        assert!(
            result.is_none(),
            "blank content must return None (cleared canvas)"
        );
    }

    #[test]
    fn test_canvas_section_from_query_response_empty_content_returns_none() {
        let ev = make_canvas_event_value("");
        let result = canvas_section_from_query_response(&[ev], CHANNEL_UUID);
        assert!(result.is_none());
    }

    /// A bare JSON object with a plausible-looking id but missing pubkey/sig/kind/tags
    /// must be rejected — not silently accepted with partial metadata.
    #[test]
    fn test_canvas_section_from_query_response_partial_object_returns_none() {
        let partial = serde_json::json!({
            "id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
            "created_at": 1705312200_i64,
            "content": "some instructions"
        });
        let result = canvas_section_from_query_response(&[partial], CHANNEL_UUID);
        assert!(
            result.is_none(),
            "partial event object (missing pubkey/sig/kind/tags) must return None"
        );
    }

    /// A JSON object that looks like an event but has `created_at` as a string
    /// must be rejected — the nostr::Event parser enforces integer type.
    #[test]
    fn test_canvas_section_from_query_response_string_timestamp_returns_none() {
        let keys = Keys::generate();
        let h_tag = Tag::parse(["h", CHANNEL_UUID]).expect("h tag");
        let mut ev = serde_json::to_value(
            EventBuilder::new(Kind::Custom(buzz_core::kind::KIND_CANVAS as u16), "content")
                .tags([h_tag])
                .sign_with_keys(&keys)
                .expect("sign"),
        )
        .expect("serialise");
        // Corrupt created_at to a string value.
        ev["created_at"] = serde_json::Value::String("2026-03-15T16:30:00+00:00".into());
        let result = canvas_section_from_query_response(&[ev], CHANNEL_UUID);
        assert!(
            result.is_none(),
            "string created_at must be rejected by nostr::Event deserialiser"
        );
    }

    /// A JSON object that looks like an event but is missing `created_at`
    /// must be rejected — nostr::Event requires the field.
    #[test]
    fn test_canvas_section_from_query_response_missing_timestamp_returns_none() {
        let keys = Keys::generate();
        let h_tag = Tag::parse(["h", CHANNEL_UUID]).expect("h tag");
        let mut ev = serde_json::to_value(
            EventBuilder::new(Kind::Custom(buzz_core::kind::KIND_CANVAS as u16), "content")
                .tags([h_tag])
                .sign_with_keys(&keys)
                .expect("sign"),
        )
        .expect("serialise");
        ev.as_object_mut().unwrap().remove("created_at");
        let result = canvas_section_from_query_response(&[ev], CHANNEL_UUID);
        assert!(
            result.is_none(),
            "missing created_at must be rejected by nostr::Event deserialiser"
        );
    }

    /// An event with a timestamp at Timestamp::max() (u64::MAX) must return None.
    ///
    /// `u64::MAX as i64` wraps to -1, which chrono silently accepts as
    /// 1969-12-31T23:59:59Z. The checked i64::try_from must reject it first.
    #[test]
    fn test_canvas_section_from_query_response_timestamp_max_returns_none() {
        let keys = Keys::generate();
        let h_tag = Tag::parse(["h", CHANNEL_UUID]).expect("h tag");
        let ev = serde_json::to_value(
            EventBuilder::new(Kind::Custom(buzz_core::kind::KIND_CANVAS as u16), "content")
                .tags([h_tag])
                .custom_created_at(Timestamp::max())
                .sign_with_keys(&keys)
                .expect("sign"),
        )
        .expect("serialise");
        let result = canvas_section_from_query_response(&[ev], CHANNEL_UUID);
        assert!(
            result.is_none(),
            "Timestamp::max() (u64::MAX) must return None — not wrap to 1969"
        );
    }

    /// A structurally complete but tampered event (content altered after signing)
    /// must be rejected by event.verify().
    #[test]
    fn test_canvas_section_from_query_response_tampered_event_returns_none() {
        let keys = Keys::generate();
        let h_tag = Tag::parse(["h", CHANNEL_UUID]).expect("h tag");
        let mut ev = serde_json::to_value(
            EventBuilder::new(
                Kind::Custom(buzz_core::kind::KIND_CANVAS as u16),
                "original",
            )
            .tags([h_tag])
            .sign_with_keys(&keys)
            .expect("sign"),
        )
        .expect("serialise");
        // Tamper the content after signing — id and sig no longer agree.
        ev["content"] = serde_json::Value::String("injected instructions".into());
        let result = canvas_section_from_query_response(&[ev], CHANNEL_UUID);
        assert!(
            result.is_none(),
            "tampered event must fail verify() and return None"
        );
    }

    /// An event with the wrong kind (not 40100) must be rejected.
    #[test]
    fn test_canvas_section_from_query_response_wrong_kind_returns_none() {
        let keys = Keys::generate();
        let h_tag = Tag::parse(["h", CHANNEL_UUID]).expect("h tag");
        let ev = serde_json::to_value(
            EventBuilder::new(Kind::Custom(9), "content")
                .tags([h_tag])
                .sign_with_keys(&keys)
                .expect("sign"),
        )
        .expect("serialise");
        let result = canvas_section_from_query_response(&[ev], CHANNEL_UUID);
        assert!(result.is_none(), "wrong kind must return None");
    }

    /// An event missing the expected h-tag (or carrying a different channel UUID)
    /// must be rejected.
    #[test]
    fn test_canvas_section_from_query_response_wrong_h_tag_returns_none() {
        let keys = Keys::generate();
        let wrong_h = Tag::parse(["h", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"]).expect("h tag");
        let ev = serde_json::to_value(
            EventBuilder::new(Kind::Custom(buzz_core::kind::KIND_CANVAS as u16), "content")
                .tags([wrong_h])
                .sign_with_keys(&keys)
                .expect("sign"),
        )
        .expect("serialise");
        let result = canvas_section_from_query_response(&[ev], CHANNEL_UUID);
        assert!(result.is_none(), "mismatched h-tag must return None");
    }

    #[test]
    fn test_canvas_section_from_query_response_timestamp_uses_z_suffix() {
        let ev = make_canvas_event_value("instructions");
        let result = canvas_section_from_query_response(&[ev], CHANNEL_UUID);
        let section = result.expect("valid event must produce a section");
        assert!(
            section.contains('Z'),
            "RFC3339 timestamp must use Z suffix, not +00:00"
        );
        assert!(
            !section.contains("+00:00"),
            "timestamp must not use +00:00 offset"
        );
    }

    // ── new-session channel context (one resolve, two consumers) ─────────────

    /// A [`ChannelInfoResolver`] whose lazy REST fallback is served by a local
    /// HTTP server, plus a counter of the requests that actually reached it.
    /// Counting real requests is the point: the composition tests are pure and
    /// cannot see duplicated I/O.
    async fn counting_resolver(
        response: serde_json::Value,
    ) -> (
        ChannelInfoResolver,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test HTTP server");
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let requests = std::sync::Arc::new(AtomicUsize::new(0));
        let server_requests = requests.clone();
        let body = response.to_string();
        let server = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = vec![0; 8192];
                let _ = socket.read(&mut buf).await;
                server_requests.fetch_add(1, Ordering::SeqCst);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        let rest = crate::relay::RestClient {
            http: reqwest::Client::new(),
            base_url,
            keys: nostr::Keys::generate(),
            auth_tag_json: None,
        };
        (
            ChannelInfoResolver::new(std::collections::HashMap::new(), rest),
            requests,
            server,
        )
    }

    fn channel_metadata_response(id: Uuid, tags: &[[&str; 2]]) -> serde_json::Value {
        let mut event_tags = vec![json!(["d", id.to_string()])];
        event_tags.extend(tags.iter().map(|[k, v]| json!([k, v])));
        json!([{ "tags": event_tags }])
    }

    #[tokio::test]
    async fn expired_absence_refreshes_to_project_without_restart() {
        use std::sync::atomic::Ordering;

        let id = Uuid::new_v4();
        let channel = id.to_string();
        let owner = "a".repeat(64);
        let coordinate = format!("30617:{owner}:app");
        let responses = [
            json!([{
                "kind": 30621,
                "pubkey": owner,
                "tags": [["d", "app"], ["buzz-channel", channel], ["a", coordinate]]
            }]),
            json!([{
                "kind": 30617,
                "pubkey": "a".repeat(64),
                "tags": [["d", "app"], ["buzz-channel", id.to_string()]]
            }]),
        ];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server_requests = requests.clone();
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = vec![0; 8192];
                let _ = socket.read(&mut buf).await;
                let index = server_requests.fetch_add(1, Ordering::SeqCst).min(1);
                let body = responses[index].to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(), body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        let resolver = ChannelInfoResolver::new(
            std::collections::HashMap::new(),
            crate::relay::RestClient {
                http: reqwest::Client::new(),
                base_url,
                keys: nostr::Keys::generate(),
                auth_tag_json: None,
            },
        );
        resolver.projects.write().unwrap().insert(
            id,
            CachedProjectInfo {
                fetched_at: std::time::Instant::now() - PROJECT_INFO_CACHE_TTL,
                value: None,
            },
        );

        let project = resolver
            .lookup_project(id)
            .await
            .expect("project lookup succeeds")
            .expect("project refreshes");
        assert_eq!(project.slug, "app");
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        server.abort();
    }

    #[tokio::test]
    async fn failed_refresh_rejects_expired_absence_but_retains_expired_project() {
        use std::sync::atomic::Ordering;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let id = Uuid::new_v4();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server_requests = requests.clone();
        let server = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = vec![0; 8192];
                let _ = socket.read(&mut buf).await;
                server_requests.fetch_add(1, Ordering::SeqCst);
                let body = "not-json";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(), body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        let resolver = ChannelInfoResolver::new(
            std::collections::HashMap::from([(
                id,
                crate::relay::ChannelInfo {
                    name: "ordinary-looking".into(),
                    channel_type: "stream".into(),
                    description: None,
                },
            )]),
            crate::relay::RestClient {
                http: reqwest::Client::new(),
                base_url,
                keys: nostr::Keys::generate(),
                auth_tag_json: None,
            },
        );

        resolver.projects.write().unwrap().insert(
            id,
            CachedProjectInfo {
                fetched_at: std::time::Instant::now() - PROJECT_INFO_CACHE_TTL,
                value: None,
            },
        );
        assert!(
            resolver.resolve(id).await.is_err(),
            "an expired absence plus failed refresh must remain indeterminate"
        );
        assert!(
            resolver
                .projects
                .read()
                .unwrap()
                .get(&id)
                .unwrap()
                .value
                .is_none(),
            "failed refresh must not renew the expired absence"
        );

        let stale_project = PromptProjectInfo {
            name: "Last known project".into(),
            slug: "last-known".into(),
            owner: "a".repeat(64),
            coordinate: format!("30621:{}:last-known", "a".repeat(64)),
            default_repo_owner: None,
            default_repo_id: None,
            default_repo_clone_urls: Vec::new(),
        };
        resolver.projects.write().unwrap().insert(
            id,
            CachedProjectInfo {
                fetched_at: std::time::Instant::now() - PROJECT_INFO_CACHE_TTL,
                value: Some(stale_project.clone()),
            },
        );
        let resolved = resolver
            .resolve(id)
            .await
            .expect("project lookup succeeds")
            .expect("stale project is retained");
        assert_eq!(resolved.project, Some(stale_project));
        assert_eq!(
            requests.load(Ordering::SeqCst),
            6,
            "each resolve makes one metadata refresh and retries project refresh once"
        );
        server.abort();
    }

    #[tokio::test]
    async fn indeterminate_project_context_never_reaches_acp_prompt_boundary() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let channel_id = Uuid::new_v4();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = vec![0; 8192];
                let _ = socket.read(&mut buf).await;
                let body = "not-json";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(), body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });

        let capture = std::env::temp_dir().join(format!(
            "buzz-acp-indeterminate-project-wire-{}.ndjson",
            Uuid::new_v4()
        ));
        let quoted_capture = capture.to_string_lossy().replace('\'', "'\\''");
        let script = format!(
            r#"while IFS= read -r line; do
  printf '%s\n' "$line" >> '{quoted_capture}'
  printf '%s\n' '{{"jsonrpc":"2.0","id":0,"result":{{"stopReason":"end_turn"}}}}'
done"#
        );
        let acp = AcpClient::spawn("bash", &["-c".into(), script], &[], false)
            .await
            .expect("spawn wire-capture ACP");
        let agent = OwnedAgent {
            index: 0,
            acp,
            state: SessionState::default(),
            model_capabilities: None,
            desired_model: None,
            model_overridden: false,
            desired_model_request_id: None,
            desired_model_pending_ack: false,
            startup_effort: None,
            agent_name: "boundary-test-agent".into(),
            goose_system_prompt_supported: None,
            protocol_version: 1,
        };

        let event = EventBuilder::new(Kind::Custom(9), "do project work")
            .sign_with_keys(&Keys::generate())
            .unwrap();
        let event_id = event.id.to_hex();
        let batch = FlushBatch {
            channel_id,
            scope: conv(channel_id),
            events: vec![crate::queue::BatchEvent {
                event,
                prompt_tag: "test".into(),
                received_at: std::time::Instant::now(),
            }],
            cancelled_events: vec![],
            cancel_reason: None,
        };

        let mut ctx = make_prompt_context_no_owner();
        ctx.dedup_mode = DedupMode::Queue;
        ctx.initial_message = Some("inspect this project before the triggering turn".into());
        ctx.rest_client.base_url = base_url.clone();
        ctx.channel_info = ChannelInfoResolver::new(
            HashMap::from([(
                channel_id,
                crate::relay::ChannelInfo {
                    name: "ordinary-looking".into(),
                    channel_type: "stream".into(),
                    description: None,
                },
            )]),
            RestClient {
                http: reqwest::Client::new(),
                base_url,
                keys: ctx.agent_keys.clone(),
                auth_tag_json: None,
            },
        );
        ctx.channel_info.projects.write().unwrap().insert(
            channel_id,
            CachedProjectInfo {
                fetched_at: std::time::Instant::now() - PROJECT_INFO_CACHE_TTL,
                value: None,
            },
        );
        let (result_tx, mut result_rx) = mpsc::unbounded_channel();
        run_prompt_task(
            agent,
            Some(batch),
            None,
            Arc::new(ctx),
            result_tx,
            None,
            PromptExecution::new(None, "indeterminate-project-turn".into()),
        )
        .await;

        let mut result = result_rx.recv().await.expect("prompt result");
        assert!(matches!(
            result.outcome,
            PromptOutcome::ProjectContextIndeterminate(_)
        ));
        let retry = result
            .batch
            .take()
            .expect("indeterminate turn must be requeued");
        assert_eq!(retry.events[0].event.id.to_hex(), event_id);
        result.agent.acp.shutdown().await;
        server.abort();
        assert!(
            !capture.exists(),
            "indeterminate project context must not send any ACP prompt, especially Scope: channel"
        );
    }

    #[tokio::test]
    async fn resolve_finds_authoritative_project_beyond_first_bridge_page() {
        use std::sync::atomic::Ordering;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let id = Uuid::new_v4();
        let channel = id.to_string();
        let owner = "a".repeat(64);
        let coordinate = format!("30617:{owner}:app");
        let first_page: Vec<_> = (0..500)
            .map(|index| {
                json!({
                    "id": format!("{index:064x}"),
                    "created_at": 1_000 - index,
                    "kind": 30621,
                    "pubkey": "b".repeat(64),
                    "tags": [["d", format!("decoy-{index}")], ["buzz-channel", channel]]
                })
            })
            .collect();
        let responses = [
            channel_metadata_response(id, &[["name", "project-home"], ["t", "stream"]]),
            serde_json::Value::Array(first_page),
            json!([{
                "id": "f".repeat(64), "created_at": 1, "kind": 30621, "pubkey": owner,
                "tags": [["d", "app"], ["buzz-channel", channel], ["a", coordinate]]
            }]),
            json!([{
                "id": "e".repeat(64), "created_at": 1, "kind": 30617, "pubkey": "a".repeat(64),
                "tags": [["d", "app"], ["buzz-channel", id.to_string()]]
            }]),
        ];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server_requests = requests.clone();
        let server = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = vec![0; 65_536];
                let read = socket.read(&mut buf).await.unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..read]);
                let index = server_requests.fetch_add(1, Ordering::SeqCst);
                if index > 0 {
                    assert!(request.contains("#buzz-channel"));
                }
                let body = responses[index].to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(), body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let resolver = ChannelInfoResolver::new(
            std::collections::HashMap::from([(
                id,
                crate::relay::ChannelInfo {
                    name: "project-home".into(),
                    channel_type: "stream".into(),
                    description: None,
                },
            )]),
            crate::relay::RestClient {
                http: reqwest::Client::new(),
                base_url,
                keys: nostr::Keys::generate(),
                auth_tag_json: None,
            },
        );

        let info = resolver
            .resolve(id)
            .await
            .expect("project lookup succeeds")
            .expect("context resolves");
        assert_eq!(info.project.expect("project context").slug, "app");
        assert_eq!(requests.load(Ordering::SeqCst), 4);
        server.abort();
    }

    /// A normal channel yields a non-DM (canvas allowed) and its name for the
    /// title suffix. Prompt-visible channel metadata refreshes for each resolve;
    /// project context remains cached independently.
    #[tokio::test]
    async fn test_new_session_channel_context_qualifies_a_normal_channel() {
        use std::sync::atomic::Ordering;

        let id = Uuid::new_v4();
        let response = channel_metadata_response(id, &[["name", "buzz-dev"], ["t", "stream"]]);
        let (resolver, requests, server) = counting_resolver(response).await;

        let info = resolver.resolve(id).await.expect("project lookup succeeds");
        let (is_dm, title_channel, channel_type) =
            resolve_new_session_channel_context(info.as_ref()).await;
        assert!(!is_dm, "a stream channel is not a DM");
        assert_eq!(title_channel.as_deref(), Some("buzz-dev"));
        assert_eq!(channel_type.as_deref(), Some("stream"));
        assert_eq!(requests.load(Ordering::SeqCst), 3);

        let again_info = resolver
            .resolve(id)
            .await
            .expect("refreshed lookup succeeds");
        let (_, again, _) = resolve_new_session_channel_context(again_info.as_ref()).await;
        assert_eq!(again.as_deref(), Some("buzz-dev"));
        assert_eq!(
            requests.load(Ordering::SeqCst),
            4,
            "channel metadata refreshes while project event classes remain cached"
        );
        server.abort();
    }

    /// Prompt turns refresh kind-39000 metadata so an edit made while the
    /// harness is running reaches the next agent prompt without a restart.
    #[tokio::test]
    async fn test_channel_resolver_refreshes_edited_description() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let id = Uuid::new_v4();
        let responses = [
            channel_metadata_response(
                id,
                &[
                    ["name", "team-chat"],
                    ["t", "stream"],
                    ["about", "First version"],
                ],
            ),
            json!([]),
            json!([]),
            channel_metadata_response(
                id,
                &[
                    ["name", "team-chat"],
                    ["t", "stream"],
                    ["about", "First paragraph.\n\nUpdated second paragraph."],
                ],
            ),
        ];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let requests = std::sync::Arc::new(AtomicUsize::new(0));
        let server_requests = requests.clone();
        let server = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = vec![0; 8192];
                let _ = socket.read(&mut buf).await;
                let index = server_requests.fetch_add(1, Ordering::SeqCst).min(3);
                let body = responses[index].to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(), body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        let resolver = ChannelInfoResolver::new(
            std::collections::HashMap::new(),
            crate::relay::RestClient {
                http: reqwest::Client::new(),
                base_url,
                keys: nostr::Keys::generate(),
                auth_tag_json: None,
            },
        );

        let first = resolver
            .resolve(id)
            .await
            .expect("initial project lookup succeeds")
            .expect("initial metadata resolves");
        assert_eq!(first.description.as_deref(), Some("First version"));

        let updated = resolver
            .resolve(id)
            .await
            .expect("updated project lookup succeeds")
            .expect("updated metadata resolves");
        assert_eq!(
            updated.description.as_deref(),
            Some("First paragraph.\n\nUpdated second paragraph.")
        );
        assert_eq!(requests.load(Ordering::SeqCst), 4);
        server.abort();
    }

    /// A channel's `about` tag is parsed through the lazy-fetch path and
    /// delivered as the resolved description.
    #[tokio::test]
    async fn test_channel_resolver_delivers_description() {
        let id = Uuid::new_v4();
        let response = channel_metadata_response(
            id,
            &[
                ["name", "team-chat"],
                ["t", "stream"],
                ["about", "Engineering discussions"],
            ],
        );
        let (resolver, _requests, server) = counting_resolver(response).await;

        let info = resolver
            .resolve(id)
            .await
            .expect("project lookup succeeds")
            .expect("should resolve");
        assert_eq!(info.description.as_deref(), Some("Engineering discussions"));
        server.abort();
    }

    /// A metadata event with no `about` tag yields no description.
    #[tokio::test]
    async fn test_channel_resolver_absent_description_when_no_about_tag() {
        let id = Uuid::new_v4();
        let response = channel_metadata_response(id, &[["name", "buzz-dev"], ["t", "stream"]]);
        let (resolver, _requests, server) = counting_resolver(response).await;

        let info = resolver
            .resolve(id)
            .await
            .expect("project lookup succeeds")
            .expect("should resolve");
        assert_eq!(info.description, None);
        server.abort();
    }

    /// A DM carries no useful name, so it gets the bare agent title (and no
    /// canvas section).
    #[tokio::test]
    async fn test_new_session_channel_context_leaves_a_dm_unqualified() {
        let id = Uuid::new_v4();
        let response = channel_metadata_response(id, &[["name", "DM"], ["t", "dm"]]);
        let (resolver, _requests, server) = counting_resolver(response).await;

        let info = resolver.resolve(id).await.expect("project lookup succeeds");
        let (is_dm, title_channel, channel_type) =
            resolve_new_session_channel_context(info.as_ref()).await;
        assert!(is_dm);
        assert_eq!(channel_type.as_deref(), Some("dm"));
        assert_eq!(
            title_channel, None,
            "a DM name must never reach the session title"
        );
        server.abort();
    }

    /// The `"unknown"` placeholder `fetch_channel_info` substitutes for a
    /// metadata event with no `name` tag is not a channel name: qualifying with
    /// it would title every unnamed channel `Agent · #unknown`.
    #[tokio::test]
    async fn test_new_session_channel_context_treats_the_unknown_name_as_absent() {
        let id = Uuid::new_v4();
        let response = channel_metadata_response(id, &[["t", "stream"]]);
        let (resolver, _requests, server) = counting_resolver(response).await;

        let info = resolver.resolve(id).await.expect("project lookup succeeds");
        let (is_dm, title_channel, _) = resolve_new_session_channel_context(info.as_ref()).await;
        assert!(!is_dm, "a nameless stream channel is still not a DM");
        assert_eq!(
            title_channel, None,
            "the `unknown` placeholder must yield a bare title"
        );
        server.abort();
    }

    /// An unresolvable channel yields the bare title, fails closed as a DM, and
    /// costs exactly ONE `fetch_channel_info` sequence — two attempts, because
    /// `fetch_with_retry` retries once. `resolve()` caches only `Some`, so a
    /// second resolve for the title would double this in front of `session/new`,
    /// exactly when the relay is already degraded.
    #[tokio::test]
    async fn test_new_session_channel_context_attempts_an_unresolved_channel_once() {
        use std::sync::atomic::Ordering;

        let (resolver, requests, server) = counting_resolver(json!([])).await;

        let info = resolver
            .resolve(Uuid::new_v4())
            .await
            .expect("missing metadata is not a project lookup error");
        let (is_dm, title_channel, channel_type) =
            resolve_new_session_channel_context(info.as_ref()).await;
        assert!(is_dm, "an undeterminable channel type must fail closed");
        assert_eq!(title_channel, None, "unresolved channels get a bare title");
        assert_eq!(channel_type, None);
        assert_eq!(
            requests.load(Ordering::SeqCst),
            2,
            "one fetch_channel_info sequence (initial attempt + single retry)"
        );
        server.abort();
    }
}

#[cfg(test)]
mod trusted_mcp_session_tests {
    use super::*;
    use crate::acp::AcpClient;
    use crate::trusted_mcp::TrustedMcpFactory;
    use buzz_dev_mcp::HarnessTrustedIdentity;
    use nostr::{EventBuilder, Keys, Kind};
    use serde_json::json;

    async fn scripted_agent(http_capability: Option<bool>) -> (AcpClient, std::path::PathBuf) {
        let capture =
            std::env::temp_dir().join(format!("buzz-acp-session-new-{}.json", Uuid::new_v4()));
        let capability = match http_capability {
            Some(value) => format!(r#""mcpCapabilities":{{"http":{value}}}"#),
            None => String::new(),
        };
        let agent_capabilities = if capability.is_empty() {
            r#""agentCapabilities":{}"#.to_owned()
        } else {
            format!(r#""agentCapabilities":{{{capability}}}"#)
        };
        let script = format!(
            r#"count=0
while IFS= read -r line; do
  count=$((count + 1))
  if [ "$count" -eq 1 ]; then
    printf '%s\n' '{{"jsonrpc":"2.0","id":0,"result":{{{agent_capabilities},"agentInfo":{{"name":"adapter-test","version":"1"}}}}}}'
  else
    printf '%s' "$line" > "$CAPTURE_FILE"
    printf '%s\n' '{{"jsonrpc":"2.0","id":1,"result":{{"sessionId":"sess-1"}}}}'
  fi
done"#
        );
        let mut acp = AcpClient::spawn(
            "bash",
            &["-c".into(), script],
            &[(
                "CAPTURE_FILE".into(),
                capture.to_string_lossy().into_owned(),
            )],
            false,
        )
        .await
        .expect("spawn scripted ACP");
        acp.initialize().await.expect("initialize scripted ACP");
        (acp, capture)
    }

    async fn scripted_turn_agent() -> (AcpClient, std::path::PathBuf) {
        let capture =
            std::env::temp_dir().join(format!("buzz-acp-heartbeat-{}.ndjson", Uuid::new_v4()));
        let script = r#"count=0
while IFS= read -r line; do
  count=$((count + 1))
  printf '%s\n' "$line" >> "$CAPTURE_FILE"
  if [ "$count" -eq 1 ]; then
    printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{"agentCapabilities":{},"agentInfo":{"name":"adapter-test","version":"1"}}}'
  elif [ "$count" -eq 2 ]; then
    printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"sessionId":"heartbeat-session"}}'
  else
    printf '%s\n' "{\"jsonrpc\":\"2.0\",\"id\":$((count - 1)),\"result\":{\"stopReason\":\"end_turn\"}}"
  fi
done"#;
        let mut acp = AcpClient::spawn(
            "bash",
            &["-c".into(), script.into()],
            &[(
                "CAPTURE_FILE".into(),
                capture.to_string_lossy().into_owned(),
            )],
            false,
        )
        .await
        .expect("spawn scripted heartbeat ACP");
        acp.initialize().await.expect("initialize heartbeat ACP");
        (acp, capture)
    }

    async fn scripted_resume_agent() -> (AcpClient, std::path::PathBuf) {
        let capture =
            std::env::temp_dir().join(format!("buzz-acp-session-resume-{}.ndjson", Uuid::new_v4()));
        let script = r#"count=0
while IFS= read -r line; do
  count=$((count + 1))
  printf '%s\n' "$line" >> "$CAPTURE_FILE"
  if [ "$count" -eq 1 ]; then
    printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"resume":{}},"mcpCapabilities":{"http":true}},"agentInfo":{"name":"adapter-test","version":"1"}}}'
  else
    printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"sessionId":"persisted-session","configOptions":[],"modes":{}}}'
  fi
done"#;
        let mut acp = AcpClient::spawn(
            "bash",
            &["-c".into(), script.into()],
            &[(
                "CAPTURE_FILE".into(),
                capture.to_string_lossy().into_owned(),
            )],
            false,
        )
        .await
        .expect("spawn scripted resume ACP");
        acp.initialize().await.expect("initialize resume ACP");
        (acp, capture)
    }

    fn owned(acp: AcpClient) -> OwnedAgent {
        OwnedAgent {
            index: 0,
            acp,
            state: SessionState::default(),
            model_capabilities: None,
            desired_model: None,
            model_overridden: false,
            desired_model_request_id: None,
            desired_model_pending_ack: false,
            startup_effort: None,
            agent_name: "adapter-test".into(),
            goose_system_prompt_supported: None,
            protocol_version: 2,
        }
    }

    fn heartbeat_workspace_fixture() -> (tempfile::TempDir, PromptProjectInfo, String) {
        let harness = tempfile::TempDir::new().expect("heartbeat workspace fixture");
        let checkout = harness.path().join("REPOS/nemo");
        let skill = checkout.join(".agents/skills/nemo-a2a/SKILL.md");
        std::fs::create_dir_all(skill.parent().unwrap()).unwrap();
        std::fs::write(
            &skill,
            "---\nname: nemo-a2a\ndescription: Coordinate Nemo agents.\n---\n\nNEMO-A2A-1\nbuzz_chat_send buzz_a2a_dispatch buzz_a2a_inbox buzz_a2a_status buzz_a2a_cancel buzz_a2a_handoff\nEND-NEMO-SKILL\n",
        )
        .unwrap();
        std::fs::write(
            checkout.join(".agents/buzz-preload.json"),
            r#"{"schema_version":"buzz.project-preload.v1","repository":"https://github.com/mysteropodes/nemo","skills":["nemo-a2a"]}"#,
        )
        .unwrap();
        for args in [
            vec!["init", "--quiet"],
            vec![
                "remote",
                "add",
                "origin",
                "https://github.com/mysteropodes/nemo.git",
            ],
            vec!["add", ".agents"],
            vec![
                "-c",
                "user.name=Buzz Test",
                "-c",
                "user.email=buzz-test@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "fixture",
            ],
        ] {
            assert!(std::process::Command::new("git")
                .arg("-C")
                .arg(&checkout)
                .args(args)
                .status()
                .unwrap()
                .success());
        }
        let revision = std::process::Command::new("git")
            .arg("-C")
            .arg(&checkout)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        let revision = String::from_utf8(revision.stdout)
            .unwrap()
            .trim()
            .to_string();
        let project = PromptProjectInfo {
            name: "Nemo".into(),
            slug: "nemo".into(),
            owner: "a".repeat(64),
            coordinate: format!("30621:{}:nemo", "a".repeat(64)),
            default_repo_owner: Some("a".repeat(64)),
            default_repo_id: Some("nemo".into()),
            default_repo_clone_urls: vec!["https://github.com/mysteropodes/nemo.git".into()],
        };
        (harness, project, revision)
    }

    async fn workspace_project_relay(
        channel_id: Uuid,
        project: &PromptProjectInfo,
    ) -> (String, tokio::task::JoinHandle<()>) {
        workspace_projects_relay(vec![(channel_id, project.clone())]).await
    }

    async fn workspace_projects_relay(
        projects: Vec<(Uuid, PromptProjectInfo)>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind workspace Project fixture relay");
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let bodies = projects
            .into_iter()
            .map(|(channel_id, project)| {
                let repo_owner = project
                    .default_repo_owner
                    .as_deref()
                    .expect("fixture repository owner");
                let repo_id = project
                    .default_repo_id
                    .as_deref()
                    .expect("fixture repository id");
                let repo_coordinate = format!("30617:{repo_owner}:{repo_id}");
                let project_event = json!({
                    "id": "1".repeat(64),
                    "created_at": 1,
                    "kind": buzz_core::kind::KIND_PROJECT,
                    "pubkey": project.owner,
                    "tags": [
                        ["d", project.slug],
                        ["name", project.name],
                        ["buzz-channel", channel_id.to_string()],
                        ["a", repo_coordinate]
                    ]
                });
                let mut repo_tags = vec![
                    json!(["d", repo_id]),
                    json!(["buzz-channel", channel_id.to_string()]),
                ];
                if !project.default_repo_clone_urls.is_empty() {
                    let mut clone_tag = vec![json!("clone")];
                    clone_tag.extend(project.default_repo_clone_urls.iter().map(|url| json!(url)));
                    repo_tags.push(serde_json::Value::Array(clone_tag));
                }
                let repo_event = json!({
                    "id": "2".repeat(64),
                    "created_at": 1,
                    "kind": buzz_core::kind::KIND_GIT_REPO_ANNOUNCEMENT,
                    "pubkey": repo_owner,
                    "tags": repo_tags
                });
                (
                    channel_id.to_string(),
                    serde_json::to_string(&vec![project_event]).unwrap(),
                    serde_json::to_string(&vec![repo_event]).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let server = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let bodies = bodies.clone();
                tokio::spawn(async move {
                    let mut request = vec![0_u8; 16 * 1024];
                    let Ok(read) = socket.read(&mut request).await else {
                        return;
                    };
                    let request = String::from_utf8_lossy(&request[..read]);
                    let body = bodies
                        .iter()
                        .find(|(channel, _, _)| request.contains(channel))
                        .map(|(_, project, repo)| {
                            if request.contains(&buzz_core::kind::KIND_PROJECT.to_string()) {
                                project.clone()
                            } else {
                                repo.clone()
                            }
                        })
                        .unwrap_or_else(|| "[]".into());
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        (base_url, server)
    }

    fn factory(keys: &nostr::Keys) -> TrustedMcpFactory {
        let identity = HarnessTrustedIdentity::new(
            std::path::Path::new("."),
            "http://127.0.0.1:9".into(),
            keys.clone(),
            None,
            None,
            None,
            None,
            true,
        )
        .expect("trusted identity");
        TrustedMcpFactory::new(identity, Duration::from_secs(60)).expect("factory")
    }

    async fn create_captured_session(
        http_capability: Option<bool>,
    ) -> (
        serde_json::Value,
        OwnedAgent,
        observer::ObserverHandle,
        String,
    ) {
        let keys = nostr::Keys::generate();
        let raw_key = keys.secret_key().to_secret_hex();
        let (acp, capture) = scripted_agent(http_capability).await;
        let mut agent = owned(acp);
        let observer = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(observer.clone()), 0);
        let mut ctx = tests::make_prompt_context_no_owner();
        ctx.trusted_mcp_factory = Some(factory(&keys));
        ctx.mcp_servers = vec![McpServer::stdio(
            "generic-dev",
            "buzz-dev-mcp",
            vec![],
            vec![EnvVar {
                name: "BUZZ_ACP_DISPLAY_NAME".into(),
                value: "Test Agent".into(),
            }],
        )];
        let scope = SessionScope::Conversation {
            channel_id: Uuid::new_v4(),
        };
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: Some(&scope),
                channel_type: Some("channel"),
            },
        )
        .await
        .expect("session creation");
        let wire: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&capture).expect("captured session/new"))
                .expect("session/new JSON");
        let _ = std::fs::remove_file(capture);
        (wire, agent, observer, raw_key)
    }

    #[tokio::test]
    async fn production_session_new_gates_scoped_http_mcp_on_adapter_capability() {
        let (supported, mut agent, observer, raw_key) = create_captured_session(Some(true)).await;
        let servers = supported["params"]["mcpServers"].as_array().unwrap();
        assert_eq!(servers.len(), 2);
        let trusted = servers
            .iter()
            .find(|server| server["name"] == "buzz-trusted-session")
            .expect("trusted HTTP server");
        assert_eq!(trusted["type"], "http");
        assert!(trusted["url"]
            .as_str()
            .is_some_and(|url| url.starts_with("http://127.0.0.1:")));
        let bearer = trusted["headers"][0]["value"].as_str().unwrap();
        assert!(bearer.starts_with("Bearer "));
        let wire_text = supported.to_string();
        assert!(!wire_text.contains(&raw_key));
        let observed = serde_json::to_string(&observer.snapshot()).unwrap();
        assert!(!observed.contains(bearer));
        assert!(!observed.contains(&raw_key));
        assert!(observed.contains("[REDACTED]"));
        agent.state.invalidate_all();
        agent.acp.shutdown().await;

        for unsupported in [Some(false), None] {
            let keys = nostr::Keys::generate();
            let (acp, capture) = scripted_agent(unsupported).await;
            let mut agent = owned(acp);
            let mut ctx = tests::make_prompt_context_no_owner();
            ctx.trusted_mcp_factory = Some(factory(&keys));
            ctx.mcp_servers = vec![McpServer::stdio(
                "generic-dev",
                "buzz-dev-mcp",
                vec![],
                vec![],
            )];
            let scope = SessionScope::Conversation {
                channel_id: Uuid::new_v4(),
            };
            let err = create_session_and_apply_model(
                &mut agent,
                &ctx,
                None,
                NewSessionChannelContext {
                    huddle_instructions: None,
                    canvas: None,
                    name: None,
                    scope: Some(&scope),
                    channel_type: Some("channel"),
                },
            )
            .await
            .expect_err("HTTP-unsupported adapter must fail before session/new");
            let message = err.to_string();
            assert!(message.contains("does not support the required HTTP MCP"));
            assert!(message.contains("Codex or Claude"));
            assert!(!capture.exists(), "session/new must not be sent");
            assert!(agent.state.trusted_mcp.is_empty());
            agent.acp.shutdown().await;
        }
    }

    #[tokio::test]
    async fn production_session_resolution_resumes_exact_binding_with_fresh_trusted_mcp() {
        let keys = nostr::Keys::generate();
        let (acp, capture) = scripted_resume_agent().await;
        let mut agent = owned(acp);
        let mut ctx = tests::make_prompt_context_no_owner();
        ctx.cwd = std::env::current_dir()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        ctx.trusted_mcp_factory = Some(factory(&keys));
        let recovery_dir = tempfile::tempdir().unwrap();
        let store = crate::session_recovery::SessionRecoveryStore::open(
            recovery_dir.path().join("sessions.json"),
        )
        .unwrap();
        let scope = SessionScope::Conversation {
            channel_id: Uuid::new_v4(),
        };
        store
            .record_binding(crate::session_recovery::PersistedSessionBinding {
                scope: scope.clone(),
                provider: "adapter-test".into(),
                provider_session_id: "persisted-session".into(),
                cwd: ctx.cwd.clone(),
                phase: crate::session_recovery::RecoveryPhase::TurnStarted {
                    turn_id: "interrupted-turn".into(),
                    trigger_event_ids: vec!["event-1".into()],
                    started_at: "2026-09-04T00:00:00Z".into(),
                },
            })
            .unwrap();
        ctx.session_recovery = Some(store);

        let resolution = resolve_provider_session_at(
            &mut agent,
            &ctx,
            &ctx.cwd,
            None,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: Some(&scope),
                channel_type: Some("dm"),
            },
        )
        .await
        .expect("resume persisted session");
        assert!(resolution.resumed);
        assert_eq!(resolution.session_id, "persisted-session");

        let requests = std::fs::read_to_string(&capture).unwrap();
        let requests = requests
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1]["method"], "session/resume");
        assert_eq!(requests[1]["params"]["sessionId"], "persisted-session");
        let trusted = requests[1]["params"]["mcpServers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|server| server["name"] == "buzz-trusted-session")
            .expect("fresh trusted MCP on resume");
        assert!(trusted["url"]
            .as_str()
            .is_some_and(|url| url.starts_with("http://127.0.0.1:")));
        assert!(!requests
            .iter()
            .any(|request| request["method"] == "session/new"));
        let persisted = ctx
            .session_recovery
            .as_ref()
            .unwrap()
            .binding(&scope, "adapter-test", &ctx.cwd)
            .unwrap()
            .unwrap();
        assert!(matches!(
            persisted.phase,
            crate::session_recovery::RecoveryPhase::TurnStarted { .. }
        ));
        agent.state.invalidate_all();
        agent.acp.shutdown().await;
        let _ = std::fs::remove_file(capture);
    }

    #[tokio::test]
    async fn fresh_codex_heartbeat_delivers_complete_pinned_workspace_skill_once() {
        let (harness, project, revision) = heartbeat_workspace_fixture();
        let workspace_home = Uuid::new_v4();
        let (relay_base_url, relay_server) =
            workspace_project_relay(workspace_home, &project).await;
        let (mut acp, capture) = scripted_turn_agent().await;
        acp.set_developer_instructions_append_supported_for_test();
        let mut agent = owned(acp);
        agent.agent_name = "@agentclientprotocol/codex-acp".into();
        agent.protocol_version = 1;

        let mut ctx = tests::make_prompt_context_no_owner();
        ctx.cwd = harness.path().to_string_lossy().into_owned();
        ctx.base_prompt = Some("base policy".into());
        ctx.system_prompt = Some("agent persona".into());
        ctx.team_instructions = Some("team policy".into());
        ctx.workspace_project_channel = Some(workspace_home);
        ctx.workspace_project_address = Some(project.coordinate.clone());
        ctx.workspace_project_repository = project
            .default_repo_clone_urls
            .first()
            .and_then(|repository| crate::project_preload::canonical_github_repository(repository));
        ctx.workspace_project_revision = Some(revision);
        ctx.channel_info = ChannelInfoResolver::new(
            HashMap::new(),
            RestClient {
                http: reqwest::Client::new(),
                base_url: relay_base_url,
                keys: ctx.agent_keys.clone(),
                auth_tag_json: None,
            },
        );
        ctx.channel_info.projects.write().unwrap().insert(
            workspace_home,
            CachedProjectInfo {
                fetched_at: std::time::Instant::now(),
                value: Some(project),
            },
        );
        let ctx = Arc::new(ctx);
        let (result_tx, mut result_rx) = mpsc::unbounded_channel();

        for turn in 1..=2 {
            run_prompt_task(
                agent,
                None,
                Some(format!("heartbeat-{turn}")),
                Arc::clone(&ctx),
                result_tx.clone(),
                None,
                PromptExecution::new(None, format!("heartbeat-turn-{turn}")),
            )
            .await;
            let result = result_rx.recv().await.expect("heartbeat result");
            assert!(matches!(
                result.outcome,
                PromptOutcome::Ok(StopReason::EndTurn)
            ));
            agent = result.agent;
        }
        agent.acp.shutdown().await;

        let requests = std::fs::read_to_string(&capture).unwrap();
        std::fs::remove_file(&capture).unwrap();
        let requests = requests
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let prompts = requests
            .iter()
            .filter(|request| request["method"] == "session/prompt")
            .map(|request| request["params"]["prompt"][0]["text"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(prompts, vec!["heartbeat-1", "heartbeat-2"]);
        let startup = requests
            .iter()
            .find(|request| request["method"] == "session/new")
            .and_then(|request| request["params"]["_meta"]["systemPrompt"]["append"].as_str())
            .expect("Codex developer instruction append");
        assert!(startup.contains("<base>\nbase policy\n</base>"));
        assert!(startup.contains("<system>\nagent persona\n</system>"));
        assert!(startup.contains("NEMO-A2A-1"));
        assert!(startup.contains("buzz_a2a_handoff"));
        assert!(startup.contains("END-NEMO-SKILL"));
        relay_server.abort();
    }

    #[tokio::test]
    async fn job_session_never_reaches_unqualified_provider() {
        let (acp, capture) = scripted_agent(Some(true)).await;
        let mut agent = owned(acp);
        let ctx = tests::make_prompt_context_no_owner();
        let scope = SessionScope::Job {
            channel_id: Uuid::new_v4(),
            operation_id: Uuid::new_v4().to_string(),
            request_event_id: "a".repeat(64),
        };
        let verified = "/tmp/buzz-receiver-verified-checkout";
        let error = create_session_and_apply_model_at(
            &mut agent,
            &ctx,
            verified,
            None,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: Some(&scope),
                channel_type: Some("stream"),
            },
        )
        .await
        .expect_err("unqualified provider must remain chat-only");
        assert!(error.to_string().contains("chat-only"));
        assert!(
            !capture.exists(),
            "an unqualified provider must receive neither session/new nor session/prompt"
        );
        agent.acp.shutdown().await;
    }

    #[tokio::test]
    async fn nemo_workspace_skill_enters_codex_and_claude_dm_startup_prompts() {
        const NEMO_SKILL_FIXTURE: &str = r#"---
name: nemo-a2a
description: Coordinate authenticated Nemo development work between Codex and Claude agents over Buzz.
---

# Nemo A2A

Protocol version: `NEMO-A2A-1`

Use `buzz_chat_send` for normal replies and `buzz_a2a_dispatch`,
`buzz_a2a_inbox`, `buzz_a2a_status`, `buzz_a2a_cancel`, and
`buzz_a2a_handoff` for typed coordination. A relay acknowledgement is storage,
not acceptance. Never put credentials or host-local paths in a prompt."#;
        let nemo_skill = std::env::var_os("BUZZ_ACP_TEST_NEMO_SKILL_PATH")
            .map(std::path::PathBuf::from)
            .map(std::fs::read_to_string)
            .transpose()
            .expect("read optional canonical Nemo skill fixture")
            .unwrap_or_else(|| NEMO_SKILL_FIXTURE.to_string());
        let harness = tempfile::TempDir::new().expect("harness fixture");
        let checkout = harness.path().join("REPOS/nemo");
        let skill_path = checkout.join(".agents/skills/nemo-a2a/SKILL.md");
        std::fs::create_dir_all(skill_path.parent().expect("skill parent"))
            .expect("checkout fixture");
        std::fs::write(&skill_path, &nemo_skill).expect("skill fixture");
        std::fs::write(
            checkout.join(".agents/buzz-preload.json"),
            r#"{"schema_version":"buzz.project-preload.v1","repository":"https://github.com/mysteropodes/nemo","skills":["nemo-a2a"]}"#,
        )
        .expect("manifest fixture");
        for args in [
            vec!["init", "--quiet"],
            vec![
                "remote",
                "add",
                "origin",
                "https://github.com/mysteropodes/nemo.git",
            ],
        ] {
            assert!(std::process::Command::new("git")
                .arg("-C")
                .arg(&checkout)
                .args(args)
                .status()
                .expect("git fixture command")
                .success());
        }
        let project = PromptProjectInfo {
            name: "Nemo".into(),
            slug: "nemo".into(),
            owner: "a".repeat(64),
            coordinate: format!("30621:{}:nemo", "a".repeat(64)),
            default_repo_owner: Some("a".repeat(64)),
            default_repo_id: Some("nemo".into()),
            default_repo_clone_urls: vec!["https://github.com/mysteropodes/nemo.git".into()],
        };
        let workspace_home = Uuid::new_v4();
        let dm_channel = Uuid::new_v4();
        let (relay_base_url, relay_server) =
            workspace_project_relay(workspace_home, &project).await;
        let mut prompt_context = tests::make_prompt_context_no_owner();
        prompt_context.workspace_project_channel = Some(workspace_home);
        prompt_context.workspace_project_address = Some(project.coordinate.clone());
        prompt_context.workspace_project_repository = project
            .default_repo_clone_urls
            .first()
            .and_then(|repository| crate::project_preload::canonical_github_repository(repository));
        prompt_context.channel_info = ChannelInfoResolver::new(
            HashMap::new(),
            RestClient {
                http: reqwest::Client::new(),
                base_url: relay_base_url,
                keys: prompt_context.agent_keys.clone(),
                auth_tag_json: None,
            },
        );
        let instruction_revision = {
            assert!(std::process::Command::new("git")
                .arg("-C")
                .arg(&checkout)
                .args([
                    "-c",
                    "user.name=Buzz Test",
                    "-c",
                    "user.email=buzz-test@example.invalid",
                    "add",
                    ".agents",
                ])
                .status()
                .expect("git add fixture")
                .success());
            assert!(std::process::Command::new("git")
                .arg("-C")
                .arg(&checkout)
                .args([
                    "-c",
                    "user.name=Buzz Test",
                    "-c",
                    "user.email=buzz-test@example.invalid",
                    "commit",
                    "--quiet",
                    "-m",
                    "fixture",
                ])
                .status()
                .expect("git commit fixture")
                .success());
            let output = std::process::Command::new("git")
                .arg("-C")
                .arg(&checkout)
                .args(["rev-parse", "HEAD"])
                .output()
                .expect("git revision fixture");
            String::from_utf8(output.stdout)
                .expect("UTF-8 revision")
                .trim()
                .to_string()
        };
        prompt_context.workspace_project_revision = Some(instruction_revision.clone());
        prompt_context
            .channel_info
            .projects
            .write()
            .unwrap()
            .insert(
                workspace_home,
                CachedProjectInfo {
                    fetched_at: std::time::Instant::now(),
                    value: Some(project.clone()),
                },
            );
        let source = PromptSource::Channel(SessionScope::Conversation {
            channel_id: dm_channel,
        });
        let dm_info = PromptChannelInfo {
            name: "Direct message".into(),
            channel_type: "dm".into(),
            description: None,
            project: None,
        };
        let selected_project =
            resolve_workspace_prompt_project(&prompt_context, &source, Some(&dm_info))
                .await
                .expect("workspace project selection")
                .expect("workspace Project applies outside its home channel");
        assert_eq!(selected_project, project);
        let ordinary_project = resolve_workspace_prompt_project(
            &prompt_context,
            &PromptSource::Channel(SessionScope::Conversation {
                channel_id: Uuid::new_v4(),
            }),
            Some(&PromptChannelInfo {
                name: "General".into(),
                channel_type: "stream".into(),
                description: None,
                project: None,
            }),
        )
        .await
        .expect("ordinary channel selection")
        .expect("workspace Project applies to ordinary channels");
        assert_eq!(ordinary_project, project);
        let heartbeat_project =
            resolve_workspace_prompt_project(&prompt_context, &PromptSource::Heartbeat, None)
                .await
                .expect("heartbeat selection")
                .expect("workspace Project applies to heartbeats");
        assert_eq!(heartbeat_project, project);

        let preload = crate::project_preload::resolve(
            harness.path(),
            &selected_project,
            None,
            Some(&instruction_revision),
        )
        .expect("Nemo preload resolution")
        .expect("Nemo checkout");
        let project_instructions = preload.instructions.expect("project instructions");
        let checkout = preload.working_directory.to_string_lossy().into_owned();

        for (agent_name, protocol_version, transport) in [
            ("@agentclientprotocol/codex-acp", 1, "meta-append"),
            (CLAUDE_AGENT_ACP_NAME, 1, "meta-append"),
        ] {
            let (acp, capture) = scripted_agent(Some(true)).await;
            let mut agent = owned(acp);
            agent.agent_name = agent_name.into();
            agent.protocol_version = protocol_version;
            if agent_name == "@agentclientprotocol/codex-acp" {
                agent
                    .acp
                    .set_developer_instructions_append_supported_for_test();
            }
            create_session_and_apply_model_at(
                &mut agent,
                &prompt_context,
                &checkout,
                Some(&project_instructions),
                None,
                NewSessionChannelContext {
                    huddle_instructions: None,
                    canvas: None,
                    name: None,
                    scope: None,
                    channel_type: Some("dm"),
                },
            )
            .await
            .expect("Nemo project session creation");

            let wire: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&capture).expect("captured session/new"))
                    .expect("session/new JSON");
            assert_eq!(wire["params"]["cwd"], checkout);
            let startup_prompt = match transport {
                "meta-append" => wire["params"]["_meta"]["systemPrompt"]["append"]
                    .as_str()
                    .expect("systemPrompt append"),
                _ => unreachable!(),
            };
            assert!(startup_prompt.contains(nemo_skill.trim()));
            assert!(startup_prompt.contains("<project-instructions>"));
            assert!(startup_prompt.contains("NEMO-A2A-1"));
            assert!(startup_prompt.contains("buzz_a2a_dispatch"));
            assert!(startup_prompt.contains("relay acknowledgement"));

            let _ = std::fs::remove_file(capture);
            agent.acp.shutdown().await;
        }
        relay_server.abort();
    }

    #[tokio::test]
    async fn workspace_project_rejects_a_different_project_home() {
        let workspace_home = Uuid::new_v4();
        let other_home = Uuid::new_v4();
        let mut ctx = tests::make_prompt_context_no_owner();
        ctx.workspace_project_channel = Some(workspace_home);
        ctx.workspace_project_revision = Some("a".repeat(40));
        let other = PromptProjectInfo {
            name: "Other".into(),
            slug: "other".into(),
            owner: "b".repeat(64),
            coordinate: format!("30621:{}:other", "b".repeat(64)),
            default_repo_owner: None,
            default_repo_id: None,
            default_repo_clone_urls: Vec::new(),
        };
        let current = PromptChannelInfo {
            name: "Other".into(),
            channel_type: "stream".into(),
            description: None,
            project: Some(other),
        };
        let error = resolve_workspace_prompt_project(
            &ctx,
            &PromptSource::Channel(SessionScope::Conversation {
                channel_id: other_home,
            }),
            Some(&current),
        )
        .await
        .expect_err("a different Project must not inherit workspace policy");
        assert!(error.0.contains("different Project"));
    }

    #[tokio::test]
    async fn strict_workspace_scope_rejects_a_new_project_despite_cached_absence() {
        let workspace_home = Uuid::new_v4();
        let current_channel = Uuid::new_v4();
        let owner = "a".repeat(64);
        let workspace = PromptProjectInfo {
            name: "Nemo".into(),
            slug: "nemo".into(),
            owner: owner.clone(),
            coordinate: format!("30621:{owner}:nemo"),
            default_repo_owner: Some(owner.clone()),
            default_repo_id: Some("nemo".into()),
            default_repo_clone_urls: vec!["https://github.com/mysteropodes/nemo.git".into()],
        };
        let other = PromptProjectInfo {
            name: "Other".into(),
            slug: "other".into(),
            owner: owner.clone(),
            coordinate: format!("30621:{owner}:other"),
            default_repo_owner: Some(owner),
            default_repo_id: Some("other".into()),
            default_repo_clone_urls: vec!["https://github.com/mysteropodes/other.git".into()],
        };
        let (base_url, relay_server) = workspace_projects_relay(vec![
            (workspace_home, workspace),
            (current_channel, other.clone()),
        ])
        .await;
        let mut ctx = tests::make_prompt_context_no_owner();
        ctx.workspace_project_channel = Some(workspace_home);
        ctx.workspace_project_revision = Some("a".repeat(40));
        ctx.channel_info = ChannelInfoResolver::new(
            HashMap::new(),
            RestClient {
                http: reqwest::Client::new(),
                base_url,
                keys: ctx.agent_keys.clone(),
                auth_tag_json: None,
            },
        );
        ctx.channel_info.projects.write().unwrap().insert(
            current_channel,
            CachedProjectInfo {
                fetched_at: std::time::Instant::now(),
                value: None,
            },
        );
        let current = PromptChannelInfo {
            name: "General".into(),
            channel_type: "stream".into(),
            description: None,
            project: None,
        };
        let error = resolve_workspace_prompt_project(
            &ctx,
            &PromptSource::Channel(SessionScope::Conversation {
                channel_id: current_channel,
            }),
            Some(&current),
        )
        .await
        .expect_err("fresh Project authority must override cached absence");
        assert!(error.0.contains("different Project"));
        assert_eq!(
            ctx.channel_info
                .projects
                .read()
                .unwrap()
                .get(&current_channel)
                .and_then(|cached| cached.value.clone()),
            Some(other)
        );
        relay_server.abort();
    }

    #[tokio::test]
    async fn rebound_workspace_project_never_reaches_existing_acp_session() {
        let workspace_home = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        let scope = SessionScope::Conversation { channel_id };
        let owner = "a".repeat(64);
        let replacement = PromptProjectInfo {
            name: "Replacement".into(),
            slug: "replacement".into(),
            owner: owner.clone(),
            coordinate: format!("30621:{owner}:replacement"),
            default_repo_owner: Some(owner.clone()),
            default_repo_id: Some("replacement".into()),
            default_repo_clone_urls: vec!["https://github.com/mysteropodes/replacement.git".into()],
        };
        let original = PromptProjectInfo {
            name: "Nemo".into(),
            slug: "nemo".into(),
            owner: owner.clone(),
            coordinate: format!("30621:{owner}:nemo"),
            default_repo_owner: Some(owner),
            default_repo_id: Some("nemo".into()),
            default_repo_clone_urls: vec!["https://github.com/mysteropodes/nemo.git".into()],
        };
        let revision = "a".repeat(40);
        let (relay_base_url, relay_server) =
            workspace_project_relay(workspace_home, &replacement).await;

        let capture = std::env::temp_dir().join(format!(
            "buzz-acp-workspace-rebind-wire-{}.ndjson",
            Uuid::new_v4()
        ));
        let script =
            "while IFS= read -r line; do printf '%s\\n' \"$line\" >> \"$CAPTURE_FILE\"; done";
        let acp = AcpClient::spawn(
            "bash",
            &["-c".into(), script.into()],
            &[(
                "CAPTURE_FILE".into(),
                capture.to_string_lossy().into_owned(),
            )],
            false,
        )
        .await
        .expect("spawn workspace rebind wire capture");
        let mut agent = owned(acp);
        agent
            .state
            .sessions
            .insert(scope.clone(), "existing-session".into());
        agent
            .state
            .deliveries
            .insert(scope.clone(), ChannelDeliveryState::default());
        agent.state.workspace_instruction_bindings.insert(
            scope.clone(),
            WorkspaceInstructionBinding {
                home_channel: workspace_home,
                project: original,
                revision: revision.clone(),
            },
        );

        let event = EventBuilder::new(Kind::Custom(9), "continue existing session")
            .sign_with_keys(&Keys::generate())
            .unwrap();
        let event_id = event.id.to_hex();
        let batch = FlushBatch {
            channel_id,
            scope,
            events: vec![crate::queue::BatchEvent {
                event,
                prompt_tag: "test".into(),
                received_at: std::time::Instant::now(),
            }],
            cancelled_events: vec![],
            cancel_reason: None,
        };

        let mut ctx = tests::make_prompt_context_no_owner();
        ctx.dedup_mode = DedupMode::Queue;
        ctx.workspace_project_channel = Some(workspace_home);
        ctx.workspace_project_revision = Some(revision);
        ctx.channel_info = ChannelInfoResolver::new(
            HashMap::from([(
                channel_id,
                crate::relay::ChannelInfo {
                    name: "general".into(),
                    channel_type: "stream".into(),
                    description: None,
                },
            )]),
            RestClient {
                http: reqwest::Client::new(),
                base_url: relay_base_url,
                keys: ctx.agent_keys.clone(),
                auth_tag_json: None,
            },
        );
        let (result_tx, mut result_rx) = mpsc::unbounded_channel();
        run_prompt_task(
            agent,
            Some(batch),
            None,
            Arc::new(ctx),
            result_tx,
            None,
            PromptExecution::new(None, "workspace-rebind-turn".into()),
        )
        .await;

        let mut result = result_rx.recv().await.expect("workspace rebind result");
        assert!(matches!(
            result.outcome,
            PromptOutcome::ProjectContextIndeterminate(_)
        ));
        let retry = result.batch.take().expect("rebound turn must be requeued");
        assert_eq!(retry.events[0].event.id.to_hex(), event_id);
        result.agent.acp.shutdown().await;
        relay_server.abort();
        assert!(
            !capture.exists(),
            "a rebound workspace Project must not reach session/prompt on the old session"
        );
    }

    #[tokio::test]
    async fn invalidating_many_job_scopes_returns_session_state_to_baseline() {
        let keys = nostr::Keys::generate();
        let factory = factory(&keys);
        let (acp, _) = scripted_agent(Some(true)).await;
        let mut agent = owned(acp);
        let mut scopes = Vec::new();
        let mut urls = Vec::new();
        for index in 0..12 {
            let scope = SessionScope::Job {
                channel_id: Uuid::new_v4(),
                operation_id: Uuid::new_v4().to_string(),
                request_event_id: format!("{index:064x}"),
            };
            // This test exercises scope-keyed cleanup, not job authority. Use
            // an ordinary session capability and store it under the synthetic
            // job key; production job sessions must have a registered gate.
            let session_scope = SessionScope::Conversation {
                channel_id: scope.channel_id(),
            };
            let session = factory
                .start(&session_scope, std::path::Path::new("."))
                .await
                .expect("trusted session");
            urls.push(session.url());
            agent
                .state
                .sessions
                .insert(scope.clone(), format!("sess-{index}"));
            agent.state.trusted_mcp.insert(scope.clone(), session);
            scopes.push(scope);
        }
        let mut pool = AgentPool::from_slots(vec![Some(agent)]);
        for scope in &scopes {
            pool.record_scope_owner(scope.clone(), 0);
            assert_eq!(pool.invalidate_scope_session(scope), 1);
        }
        let agent = pool.agents_mut()[0].as_ref().unwrap();
        assert!(agent.state.sessions.is_empty());
        assert!(agent.state.trusted_mcp.is_empty());
        assert!(pool.session_owners.is_empty());
        tokio::time::sleep(Duration::from_millis(30)).await;
        for url in urls {
            assert!(reqwest::Client::new().post(url).send().await.is_err());
        }
    }
}

#[cfg(test)]
mod startup_effort_tests {
    use super::*;
    use crate::acp::AcpClient;
    use tests::make_prompt_context_no_owner;

    /// Build a protocol-v2, non-goose agent whose only ACP requests will be
    /// `session/new` (id 0) then the startup-effort `session/set_config_option`
    /// (id 1). `startup_effort` is the held spawn-scoped value under test.
    fn effort_agent(acp: AcpClient, startup_effort: Option<&str>) -> OwnedAgent {
        OwnedAgent {
            index: 0,
            acp,
            state: SessionState::default(),
            model_capabilities: None,
            desired_model: None,
            model_overridden: false,
            desired_model_request_id: None,
            desired_model_pending_ack: false,
            startup_effort: startup_effort.map(str::to_string),
            agent_name: "effort-test-agent".into(),
            goose_system_prompt_supported: None,
            protocol_version: 2,
        }
    }

    /// Spawn a scripted ACP that answers `session/new` (request #1) with the
    /// given configOptions, then replies to the effort `set_config_option`
    /// (request #2) with `effort_reply` (a JSON-RPC `result`/`error` body, minus
    /// the id which is filled in). Any later request gets `{"ok":true}`.
    async fn spawn_effort_acp(session_new_config_options: &str, effort_reply: &str) -> AcpClient {
        let script = format!(
            r#"count=0
while IFS= read -r line; do
  count=$((count + 1))
  id=$((count - 1))
  if [ "$count" -eq 1 ]; then
    printf '%s\n' '{{"jsonrpc":"2.0","id":0,"result":{{"sessionId":"sess-1","configOptions":{session_new_config_options}}}}}'
  elif [ "$count" -eq 2 ]; then
    printf '%s\n' '{{"jsonrpc":"2.0","id":'"$id"',{effort_reply}}}'
  else
    printf '%s\n' '{{"jsonrpc":"2.0","id":'"$id"',"result":{{"ok":true}}}}'
  fi
done"#
        );
        AcpClient::spawn("bash", &["-c".to_string(), script], &[], false)
            .await
            .expect("spawn effort ACP script")
    }

    fn captured_config_options(obs: &observer::ObserverHandle) -> serde_json::Value {
        obs.snapshot()
            .into_iter()
            .find(|e| e.kind == "session_config_captured")
            .expect("session_config_captured emitted")
            .payload["configOptions"]
            .clone()
    }

    fn effort_current_value(options: &serde_json::Value) -> Option<String> {
        options
            .as_array()?
            .iter()
            .find(|o| o["category"] == "thought_level")
            .and_then(|o| o["currentValue"].as_str())
            .map(str::to_string)
    }

    const OPTS_WITH_EFFORT_DEFAULT_LOW: &str = r#"[{"configId":"effort","category":"thought_level","currentValue":"low","options":[{"value":"low"},{"value":"high"}]}]"#;

    #[tokio::test]
    async fn test_applied_effort_patches_captured_current_value_to_high() {
        let acp = spawn_effort_acp(OPTS_WITH_EFFORT_DEFAULT_LOW, r#""result":{"ok":true}"#).await;
        let mut agent = effort_agent(acp, Some("high"));
        let obs = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(obs.clone()), 0);

        let ctx = make_prompt_context_no_owner();
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect("session creation must succeed");

        let opts = captured_config_options(&obs);
        assert_eq!(
            effort_current_value(&opts).as_deref(),
            Some("high"),
            "applied effort must overwrite the pre-set currentValue in the capture"
        );
    }

    #[tokio::test]
    async fn test_rejected_effort_retains_captured_current_value() {
        // Adapter answers the effort set with a JSON-RPC error → AgentError →
        // application-level rejection: non-fatal, capture keeps the default.
        let acp = spawn_effort_acp(
            OPTS_WITH_EFFORT_DEFAULT_LOW,
            r#""error":{"code":-32602,"message":"unsupported effort value"}"#,
        )
        .await;
        let mut agent = effort_agent(acp, Some("high"));
        let obs = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(obs.clone()), 0);

        let ctx = make_prompt_context_no_owner();
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect("rejection is non-fatal; session creation still succeeds");

        let opts = captured_config_options(&obs);
        assert_eq!(
            effort_current_value(&opts).as_deref(),
            Some("low"),
            "a rejected effort must not falsify the capture — keep the running value"
        );
    }

    #[tokio::test]
    async fn test_no_thought_level_model_leaves_capture_unpatched() {
        // Model advertises only a `model` option — no thought_level. The held
        // effort is silently ignored and no set_config_option is sent.
        let opts_no_effort = r#"[{"configId":"model","category":"model","currentValue":"m-a","options":[{"value":"m-a"}]}]"#;
        let acp = spawn_effort_acp(opts_no_effort, r#""result":{"ok":true}"#).await;
        let mut agent = effort_agent(acp, Some("high"));
        let obs = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(obs.clone()), 0);

        let ctx = make_prompt_context_no_owner();
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect("session creation must succeed");

        let opts = captured_config_options(&obs);
        assert_eq!(
            opts,
            serde_json::from_str::<serde_json::Value>(opts_no_effort).unwrap(),
            "no thought_level option → capture is the untouched session/new snapshot"
        );
    }

    #[tokio::test]
    async fn test_no_startup_effort_leaves_capture_unpatched() {
        // No held effort at all: the set_config_option is never sent and the
        // default currentValue survives into the capture.
        let acp = spawn_effort_acp(OPTS_WITH_EFFORT_DEFAULT_LOW, r#""result":{"ok":true}"#).await;
        let mut agent = effort_agent(acp, None);
        let obs = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(obs.clone()), 0);

        let ctx = make_prompt_context_no_owner();
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect("session creation must succeed");

        let opts = captured_config_options(&obs);
        assert_eq!(
            effort_current_value(&opts).as_deref(),
            Some("low"),
            "with no configured effort the capture reflects the model default"
        );
    }

    #[tokio::test]
    async fn test_transport_error_on_effort_propagates_for_respawn() {
        // Adapter exits after answering session/new but before the effort set →
        // AgentExited (transport class) → Err so the caller respawns the worker
        // instead of reusing a possibly-poisoned stream.
        let script = format!(
            r#"IFS= read -r _new
printf '%s\n' '{{"jsonrpc":"2.0","id":0,"result":{{"sessionId":"sess-1","configOptions":{OPTS_WITH_EFFORT_DEFAULT_LOW}}}}}'
IFS= read -r _effort
exit 0"#
        );
        let acp = AcpClient::spawn("bash", &["-c".to_string(), script], &[], false)
            .await
            .expect("spawn transport-exit ACP script");
        let mut agent = effort_agent(acp, Some("high"));

        let ctx = make_prompt_context_no_owner();
        let err = create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect_err("transport-class effort failure must propagate as Err");
        assert!(
            matches!(err, AcpError::AgentExited | AcpError::Io(_)),
            "process exit mid-effort is a transport error, got {err:?}"
        );
    }

    #[test]
    fn test_patch_config_option_current_value_matches_by_id_key() {
        // The `id` key (claude-agent-acp) must also match, not just `configId`.
        let mut opts = serde_json::json!([
            { "id": "effort", "category": "thought_level", "currentValue": "low" }
        ]);
        patch_config_option_current_value(&mut opts, "effort", "high");
        assert_eq!(opts[0]["currentValue"], "high");
    }

    #[test]
    fn test_patch_config_option_current_value_noop_on_non_array() {
        let mut opts = serde_json::Value::Null;
        patch_config_option_current_value(&mut opts, "effort", "high");
        assert!(opts.is_null(), "a null snapshot must stay null");
    }
}

#[cfg(test)]
mod model_switch_tests {
    use super::*;
    use crate::acp::AcpClient;
    use tests::make_prompt_context_no_owner;

    /// A protocol-v2 agent with a live `desired_model` override and no startup
    /// effort. `model_overridden` is set so the capture's `modelOverridden`
    /// reflects only whether the switch actually landed.
    fn switching_agent(acp: AcpClient, desired_model: &str) -> OwnedAgent {
        OwnedAgent {
            index: 0,
            acp,
            state: SessionState::default(),
            model_capabilities: None,
            desired_model: Some(desired_model.to_string()),
            model_overridden: true,
            desired_model_request_id: None,
            desired_model_pending_ack: false,
            startup_effort: None,
            agent_name: "switch-test-agent".into(),
            goose_system_prompt_supported: None,
            protocol_version: 2,
        }
    }

    /// Scripted ACP: `session/new` (request #1) returns `session_new_options`,
    /// then the model-switch `set_config_option` (request #2) replies with
    /// `switch_reply` (a JSON-RPC `result`/`error` body minus the id). Any later
    /// request gets `{"ok":true}`.
    async fn spawn_switch_acp(session_new_options: &str, switch_reply: &str) -> AcpClient {
        let script = format!(
            r#"count=0
while IFS= read -r line; do
  count=$((count + 1))
  id=$((count - 1))
  if [ "$count" -eq 1 ]; then
    printf '%s\n' '{{"jsonrpc":"2.0","id":0,"result":{{"sessionId":"sess-1","configOptions":{session_new_options}}}}}'
  elif [ "$count" -eq 2 ]; then
    printf '%s\n' '{{"jsonrpc":"2.0","id":'"$id"',{switch_reply}}}'
  else
    printf '%s\n' '{{"jsonrpc":"2.0","id":'"$id"',"result":{{"ok":true}}}}'
  fi
done"#
        );
        AcpClient::spawn("bash", &["-c".to_string(), script], &[], false)
            .await
            .expect("spawn switch ACP script")
    }

    fn capture(obs: &observer::ObserverHandle) -> serde_json::Value {
        obs.snapshot()
            .into_iter()
            .find(|e| e.kind == "session_config_captured")
            .expect("session_config_captured emitted")
            .payload
    }

    fn control_results(obs: &observer::ObserverHandle) -> Vec<serde_json::Value> {
        obs.snapshot()
            .into_iter()
            .filter(|e| e.kind == "control_result")
            .map(|e| e.payload)
            .collect()
    }

    // A `model`-category option offering the default model plus the target the
    // agent wants to switch to.
    const OPTS_MODEL_A_AND_B: &str = r#"[{"configId":"model","category":"model","currentValue":"model-a","options":[{"value":"model-a"},{"value":"model-b"}]}]"#;

    #[tokio::test]
    async fn session_new_sends_policy_specific_base_and_scope_specific_title() {
        use crate::scope::SessionPolicy;

        let channel_id = Uuid::new_v4();
        let thread_a = SessionScope::Thread {
            channel_id,
            root_event_id: "abcdef01".repeat(8),
        };
        let thread_b = SessionScope::Thread {
            channel_id,
            root_event_id: "12345678".repeat(8),
        };
        let conversation = SessionScope::Conversation { channel_id };
        for (policy, scope, name, channel_type, title) in [
            (
                SessionPolicy::Channel,
                Some(&conversation),
                Some("engineering"),
                Some("stream"),
                "Fizz · #engineering",
            ),
            (
                SessionPolicy::Thread,
                Some(&thread_a),
                Some("engineering"),
                Some("stream"),
                "Fizz · #engineering · abcdef01",
            ),
            (
                SessionPolicy::Thread,
                Some(&thread_b),
                Some("engineering"),
                Some("stream"),
                "Fizz · #engineering · 12345678",
            ),
            (
                SessionPolicy::Thread,
                Some(&conversation),
                None,
                Some("dm"),
                "Fizz",
            ),
            (SessionPolicy::Thread, None, None, None, "Fizz"),
        ] {
            for (version, include_base) in [(1, true), (2, true), (1, false), (2, false)] {
                let acp = spawn_switch_acp("[]", r#""result":{}"#).await;
                let mut agent = switching_agent(acp, "unused");
                agent.desired_model = None;
                agent.protocol_version = version;
                let observer = observer::ObserverHandle::in_process();
                agent.acp.set_observer(Some(observer.clone()), 0);
                let mut ctx = make_prompt_context_no_owner();
                ctx.session_title = Some("Fizz".into());
                ctx.base_prompt =
                    include_base.then(|| policy.append_session_model("Custom base instructions."));
                create_session_and_apply_model(
                    &mut agent,
                    &ctx,
                    None,
                    NewSessionChannelContext {
                        huddle_instructions: None,
                        canvas: None,
                        name,
                        scope,
                        channel_type,
                    },
                )
                .await
                .unwrap();
                let request = observer
                    .snapshot()
                    .into_iter()
                    .find(|event| {
                        event.kind == "acp_write" && event.payload["method"] == "session/new"
                    })
                    .unwrap()
                    .payload;
                assert_eq!(request["params"]["_meta"]["sessionTitle"], title);
                let base = ctx
                    .base_prompt
                    .as_deref()
                    .map(crate::queue::base_section)
                    .unwrap_or_default();
                if !include_base {
                    assert!(request["params"].get("systemPrompt").is_none());
                } else if version == 2 {
                    let system = request["params"]["systemPrompt"].as_str().unwrap();
                    assert!(system.starts_with(&base));
                    assert_eq!(system.matches("## Session Model").count(), 1);
                } else {
                    assert!(request["params"].get("systemPrompt").is_none());
                    let legacy = prepend_standing_for_legacy(
                        version,
                        &crate::queue::StandingContext {
                            base_prompt: ctx.base_prompt.as_deref(),
                            ..Default::default()
                        },
                        "hello",
                    );
                    assert!(legacy.starts_with(&base));
                    assert_eq!(legacy.matches("## Session Model").count(), 1);
                }
                agent.acp.shutdown().await;
            }
        }
    }

    #[tokio::test]
    async fn idle_channel_switch_preserves_all_sibling_sessions_and_model() {
        let channel_id = Uuid::new_v4();
        let scopes = ["a", "b"].map(|root| SessionScope::Thread {
            channel_id,
            root_event_id: root.repeat(64),
        });
        let acp = spawn_switch_acp(OPTS_MODEL_A_AND_B, r#""result":{}"#).await;
        let mut agent = switching_agent(acp, "model-a");
        for scope in &scopes {
            agent
                .state
                .sessions
                .insert(scope.clone(), scope.telemetry_label());
        }
        let original_sessions = agent.state.sessions.clone();
        let mut pool = AgentPool::from_slots(vec![Some(agent)]);
        assert_eq!(
            pool.switch_idle_agent_model(channel_id, "model-b", Some("pick".into())),
            IdleSwitchResult::AmbiguousTarget,
        );
        let agent = pool.agents[0].as_ref().unwrap();
        assert_eq!(agent.desired_model.as_deref(), Some("model-a"));
        assert_eq!(agent.desired_model_request_id, None);
        assert_eq!(agent.state.sessions, original_sessions);

        // One remaining session is an unambiguous channel control again. The
        // selected scope and its owner are cleared without broad channel cleanup.
        pool.invalidate_scope_session(&scopes[1]);
        pool.record_scope_owner(scopes[0].clone(), 0);
        assert_eq!(
            pool.switch_idle_agent_model(channel_id, "model-b", Some("pick".into())),
            IdleSwitchResult::Switched,
        );
        let agent = pool.agents[0].as_ref().unwrap();
        assert_eq!(agent.desired_model.as_deref(), Some("model-b"));
        assert!(!agent.state.sessions.contains_key(&scopes[0]));
        assert!(!pool.session_owners.contains_key(&scopes[0]));
    }

    #[tokio::test]
    async fn test_applied_switch_refreshes_capabilities_from_post_switch_snapshot() {
        // The adapter accepts the switch and echoes the target model's rebuilt
        // configOptions — including a thought_level option the default model
        // never advertised. Capabilities and the capture must reflect the target
        // model, not the pre-switch default.
        let switch_reply = r#""result":{"configOptions":[{"configId":"model","category":"model","currentValue":"model-b","options":[{"value":"model-a"},{"value":"model-b"}]},{"configId":"effort","category":"thought_level","currentValue":"medium","options":[{"value":"low"},{"value":"medium"}]}]}"#;
        let acp = spawn_switch_acp(OPTS_MODEL_A_AND_B, switch_reply).await;
        let mut agent = switching_agent(acp, "model-b");
        // Busy path: this switch was delivered to an in-flight turn and its apply
        // is deferred to this requeued session. Arm the pending-ack and carry the
        // pick's correlator so the Applied arm emits a correlated positive
        // terminal instead of leaving the Desktop to infer success from silence.
        agent.desired_model_pending_ack = true;
        agent.desired_model_request_id = Some("req-busy-1".into());
        let obs = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(obs.clone()), 0);

        let ctx = make_prompt_context_no_owner();
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect("session creation must succeed");

        let caps = agent
            .model_capabilities
            .as_ref()
            .expect("capabilities refreshed from the post-switch snapshot");
        assert_eq!(
            caps.thought_level_config_id.as_deref(),
            Some("effort"),
            "the target model's thought_level option must be discovered post-switch"
        );
        let cap = capture(&obs);
        assert_eq!(
            cap["modelOverridden"], true,
            "an applied switch must report modelOverridden true"
        );
        assert!(
            cap["configOptions"]
                .as_array()
                .is_some_and(|a| a.iter().any(|o| o["category"] == "thought_level")),
            "the cached configOptions must be the target model's post-switch set"
        );
        // The deferred apply must emit exactly one correlated positive terminal
        // so the Desktop learns success from a real frame, not timeout silence.
        let results = control_results(&obs);
        assert_eq!(
            results.len(),
            1,
            "a busy-path applied switch emits exactly one positive terminal"
        );
        assert_eq!(results[0]["status"], "switched");
        assert_eq!(results[0]["modelId"], "model-b");
        assert_eq!(
            results[0]["requestId"], "req-busy-1",
            "the positive terminal must carry the pick's correlator"
        );
        assert!(
            !agent.desired_model_pending_ack,
            "the pending-ack is consumed once so it cannot re-fire on a later session"
        );
    }

    #[tokio::test]
    async fn test_rejected_switch_preserves_capabilities_and_emits_failure() {
        // The adapter refuses the switch with a JSON-RPC error. The session is
        // still on its default model: pre-switch capabilities survive, the
        // capture reports modelOverridden false, and a terminal `failure`
        // control_result tells Desktop the pick did not land.
        let acp = spawn_switch_acp(
            OPTS_MODEL_A_AND_B,
            r#""error":{"code":-32602,"message":"model not accepted"}"#,
        )
        .await;
        let mut agent = switching_agent(acp, "model-b");
        let obs = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(obs.clone()), 0);

        let ctx = make_prompt_context_no_owner();
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect("an application-level rejection is non-fatal");

        let caps = agent
            .model_capabilities
            .as_ref()
            .expect("pre-switch capabilities must be preserved on rejection");
        assert!(
            caps.config_options_raw
                .iter()
                .any(|o| o["currentValue"] == "model-a"),
            "capabilities must still describe the default model the session runs"
        );
        let cap = capture(&obs);
        assert_eq!(
            cap["modelOverridden"], false,
            "a rejected switch must not claim an override"
        );
        let results = control_results(&obs);
        assert_eq!(results.len(), 1, "exactly one control_result on rejection");
        assert_eq!(results[0]["status"], "failure");
        assert_eq!(results[0]["modelId"], "model-b");
    }

    #[tokio::test]
    async fn test_busy_path_rejection_emits_only_failure_and_consumes_pending_ack() {
        // K1 delayed-rejection at the Rust seam: a busy-path switch is armed
        // (pending_ack), its apply is deferred to this requeued session, and the
        // adapter then refuses it. The rejection arm must emit exactly one
        // `failure` (no spurious positive `switched`) and consume the pending-ack
        // so no later session can fire a phantom success.
        let acp = spawn_switch_acp(
            OPTS_MODEL_A_AND_B,
            r#""error":{"code":-32602,"message":"model not accepted"}"#,
        )
        .await;
        let mut agent = switching_agent(acp, "model-b");
        agent.desired_model_pending_ack = true;
        agent.desired_model_request_id = Some("req-busy-reject".into());
        let obs = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(obs.clone()), 0);

        let ctx = make_prompt_context_no_owner();
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect("an application-level rejection is non-fatal");

        let results = control_results(&obs);
        assert_eq!(
            results.len(),
            1,
            "a busy-path rejection emits exactly one terminal — no phantom success"
        );
        assert_eq!(results[0]["status"], "failure");
        assert_eq!(results[0]["requestId"], "req-busy-reject");
        assert!(
            !agent.desired_model_pending_ack,
            "the pending-ack is consumed even on rejection so it cannot re-fire"
        );
    }

    #[tokio::test]
    async fn test_applied_switch_without_options_drops_capabilities() {
        // A successful switch whose response carries no configOptions (older
        // adapter, or a model with no options): the pre-switch snapshot cannot
        // be trusted for the target model, so capabilities drop to None to be
        // re-derived on the next session — but the switch still counts as an
        // override with no failure surfaced.
        let acp = spawn_switch_acp(OPTS_MODEL_A_AND_B, r#""result":{"ok":true}"#).await;
        let mut agent = switching_agent(acp, "model-b");
        let obs = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(obs.clone()), 0);

        let ctx = make_prompt_context_no_owner();
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect("session creation must succeed");

        assert!(
            agent.model_capabilities.is_none(),
            "an optionless successful switch must drop stale capabilities"
        );
        let cap = capture(&obs);
        assert_eq!(
            cap["modelOverridden"], true,
            "the switch still applied even with no echoed options"
        );
        assert!(
            control_results(&obs).is_empty(),
            "a successful switch emits no failure control_result"
        );
    }

    #[tokio::test]
    async fn test_unsupported_model_emits_unsupported_without_switch_rpc() {
        // The desired model is absent from the session/new catalog: no switch
        // RPC is sent, the capture reports no override, and an
        // `unsupported_model` control_result rejects the live pick.
        let acp = spawn_switch_acp(OPTS_MODEL_A_AND_B, r#""result":{"ok":true}"#).await;
        let mut agent = switching_agent(acp, "model-z");
        let obs = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(obs.clone()), 0);

        let ctx = make_prompt_context_no_owner();
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect("an unresolvable model is non-fatal");

        let cap = capture(&obs);
        assert_eq!(cap["modelOverridden"], false);
        let results = control_results(&obs);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["status"], "unsupported_model");
        assert_eq!(results[0]["modelId"], "model-z");
    }

    /// Scripted ACP whose `session/new` (request #1) returns a full result body
    /// `session_new_result` (a JSON object minus the outer envelope), and whose
    /// model-switch `set_config_option` (request #2) replies with `switch_reply`
    /// (a JSON-RPC `result`/`error` body minus the id). Lets a test control the
    /// `models` block in both the pre-switch and post-switch snapshots.
    async fn spawn_switch_acp_full(session_new_result: &str, switch_reply: &str) -> AcpClient {
        let script = format!(
            r#"count=0
while IFS= read -r line; do
  count=$((count + 1))
  id=$((count - 1))
  if [ "$count" -eq 1 ]; then
    printf '%s\n' '{{"jsonrpc":"2.0","id":0,"result":{session_new_result}}}'
  elif [ "$count" -eq 2 ]; then
    printf '%s\n' '{{"jsonrpc":"2.0","id":'"$id"',{switch_reply}}}'
  else
    printf '%s\n' '{{"jsonrpc":"2.0","id":'"$id"',"result":{{"ok":true}}}}'
  fi
done"#
        );
        AcpClient::spawn("bash", &["-c".to_string(), script], &[], false)
            .await
            .expect("spawn switch ACP script")
    }

    /// F3: an applied switch must cache `models` from the POST-switch snapshot,
    /// not the pre-switch `session/new` response. The pre-switch snapshot reports
    /// the default model as current; the target response reports the target as
    /// current. The emitted capture must carry the target's models block. The
    /// Desktop-parsing half of this contract lives in `agent_config_tests.rs`
    /// (`live_switch_models_from_post_switch_snapshot_parses_target_current`).
    #[tokio::test]
    async fn test_applied_switch_caches_target_model_not_pre_switch() {
        // session/new: model-a is current. switch reply: model-b is current,
        // and it echoes rebuilt configOptions so capabilities refresh cleanly.
        let session_new = r#"{"sessionId":"sess-1","configOptions":[{"configId":"model","category":"model","currentValue":"model-a","options":[{"value":"model-a"},{"value":"model-b"}]}],"models":{"currentModelId":"model-a","availableModels":[{"modelId":"model-a"},{"modelId":"model-b"}]}}"#;
        let switch_reply = r#""result":{"configOptions":[{"configId":"model","category":"model","currentValue":"model-b","options":[{"value":"model-a"},{"value":"model-b"}]}],"models":{"currentModelId":"model-b","availableModels":[{"modelId":"model-a"},{"modelId":"model-b"}]}}"#;
        let acp = spawn_switch_acp_full(session_new, switch_reply).await;
        let mut agent = switching_agent(acp, "model-b");
        let obs = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(obs.clone()), 0);

        let ctx = make_prompt_context_no_owner();
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect("session creation must succeed");

        let cap = capture(&obs);
        assert_eq!(
            cap["models"]["currentModelId"], "model-b",
            "an applied switch must cache the target model, not the pre-switch model-a"
        );
    }

    /// F3: an applied switch whose target response omits `models` must cache
    /// Null — never fall back to the pre-switch `resp.raw.models`. Otherwise the
    /// panel would report the pre-switch model as live after a successful switch.
    #[tokio::test]
    async fn test_applied_switch_without_models_does_not_leak_pre_switch_model() {
        // session/new advertises model-a as current; the successful switch reply
        // echoes configOptions (so the switch is Applied) but NO models block.
        let session_new = r#"{"sessionId":"sess-1","configOptions":[{"configId":"model","category":"model","currentValue":"model-a","options":[{"value":"model-a"},{"value":"model-b"}]}],"models":{"currentModelId":"model-a","availableModels":[{"modelId":"model-a"},{"modelId":"model-b"}]}}"#;
        let switch_reply = r#""result":{"configOptions":[{"configId":"model","category":"model","currentValue":"model-b","options":[{"value":"model-a"},{"value":"model-b"}]}]}"#;
        let acp = spawn_switch_acp_full(session_new, switch_reply).await;
        let mut agent = switching_agent(acp, "model-b");
        let obs = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(obs.clone()), 0);

        let ctx = make_prompt_context_no_owner();
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect("session creation must succeed");

        let cap = capture(&obs);
        assert!(
            cap["models"].is_null(),
            "an optionless-models successful switch must emit Null, not the pre-switch models"
        );
    }

    /// Like `switching_agent` but also holds a spawn-scoped startup effort, so a
    /// single session creation both switches the model AND applies startup
    /// effort — the interaction F5.6 pins.
    fn switching_agent_with_effort(
        acp: AcpClient,
        desired_model: &str,
        startup_effort: &str,
    ) -> OwnedAgent {
        OwnedAgent {
            index: 0,
            acp,
            state: SessionState::default(),
            model_capabilities: None,
            desired_model: Some(desired_model.to_string()),
            model_overridden: true,
            desired_model_request_id: None,
            desired_model_pending_ack: false,
            startup_effort: Some(startup_effort.to_string()),
            agent_name: "switch-effort-test-agent".into(),
            goose_system_prompt_supported: None,
            protocol_version: 2,
        }
    }

    fn effort_option_current_value(cap: &serde_json::Value) -> Option<String> {
        cap["configOptions"]
            .as_array()?
            .iter()
            .find(|o| o["category"] == "thought_level")
            .and_then(|o| o["currentValue"].as_str())
            .map(str::to_string)
    }

    /// F5.6: startup effort resolves against the TARGET model's option set. The
    /// pre-switch model-a advertises no `thought_level`; only the post-switch
    /// model-b does. `apply_startup_effort` reads the post-switch snapshot, so
    /// the held `high` applies against model-b's option and the cached
    /// configOptions show it at `high`. Had it read the pre-switch snapshot the
    /// effort would find no option and silently no-op.
    #[tokio::test]
    async fn test_startup_effort_resolves_against_post_switch_target_options() {
        // session/new: model-a, model option only — NO thought_level.
        let session_new = r#"[{"configId":"model","category":"model","currentValue":"model-a","options":[{"value":"model-a"},{"value":"model-b"}]}]"#;
        // switch reply: model-b current AND a target-only thought_level option.
        let switch_reply = r#""result":{"configOptions":[{"configId":"model","category":"model","currentValue":"model-b","options":[{"value":"model-a"},{"value":"model-b"}]},{"configId":"effort","category":"thought_level","currentValue":"low","options":[{"value":"low"},{"value":"high"}]}]}"#;
        let acp = spawn_switch_acp(session_new, switch_reply).await;
        let mut agent = switching_agent_with_effort(acp, "model-b", "high");
        let obs = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(obs.clone()), 0);

        let ctx = make_prompt_context_no_owner();
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect("session creation must succeed");

        let cap = capture(&obs);
        assert_eq!(
            effort_option_current_value(&cap).as_deref(),
            Some("high"),
            "startup effort must apply against the target model's thought_level option"
        );
    }

    /// F5.6: an applied switch whose target response echoes NO options must not
    /// apply the held startup effort against the STALE pre-switch options. The
    /// pre-switch model-a advertised a `thought_level` option; the optionless
    /// target response means the effort has no target option and must be
    /// skipped — so the cached configOptions are Null, never the pre-switch
    /// model-a options with a falsely patched `high`.
    #[tokio::test]
    async fn test_startup_effort_skips_stale_options_on_optionless_switch() {
        // session/new: model-a WITH a thought_level option.
        let session_new = r#"[{"configId":"model","category":"model","currentValue":"model-a","options":[{"value":"model-a"},{"value":"model-b"}]},{"configId":"effort","category":"thought_level","currentValue":"low","options":[{"value":"low"},{"value":"high"}]}]"#;
        // switch reply: applied, but NO echoed options.
        let switch_reply = r#""result":{"ok":true}"#;
        let acp = spawn_switch_acp(session_new, switch_reply).await;
        let mut agent = switching_agent_with_effort(acp, "model-b", "high");
        let obs = observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(obs.clone()), 0);

        let ctx = make_prompt_context_no_owner();
        create_session_and_apply_model(
            &mut agent,
            &ctx,
            None,
            NewSessionChannelContext {
                huddle_instructions: None,
                canvas: None,
                name: None,
                scope: None,
                channel_type: None,
            },
        )
        .await
        .expect("session creation must succeed");

        let cap = capture(&obs);
        assert_eq!(
            cap["modelOverridden"], true,
            "the switch still applied even with no echoed options"
        );
        assert!(
            cap["configOptions"].is_null(),
            "an optionless switch caches the target's (empty) options, never the pre-switch model-a options with a patched effort"
        );
    }
}
