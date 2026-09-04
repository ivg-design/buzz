//! Opaque lifecycle fence for job-scoped privileged operations.

use std::future::Future;
use std::pin::Pin;

use nostr::Event;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Boxed future used by the object-safe privilege-gate boundary.
pub type PrivilegeFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Privileged operation whose authority must still be current at execution.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectGitOperation {
    /// Create and advance one exact job-bound commit.
    Commit,
    /// Fetch the exact granted branch.
    Fetch,
    /// Push the exact verified immutable head.
    Push,
    /// Transfer the active job to another granted agent.
    Handoff,
}

impl ProjectGitOperation {
    /// Canonical local-grant token for this operation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Fetch => "fetch",
            Self::Push => "push",
            Self::Handoff => "handoff",
        }
    }
}

/// Whether the exact ref/object effect of a privileged Git invocation is known.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivilegedGitDisposition {
    /// The exact intended object was observed at the exact target ref.
    Applied,
    /// The target ref was proven to remain at its pre-invocation value.
    NotApplied,
    /// The target could not be read or held a third value after an uncertain command result.
    Ambiguous,
}

/// Durable, credential-free receipt for one privileged Git invocation.
///
/// The ACP lease binds this producer-supplied record to its independently held
/// job, marker, operation, and invocation before persisting it. Machine-local
/// checkout paths and authentication material are deliberately excluded.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrivilegedGitOperationReceipt {
    /// Receipt schema identifier.
    pub schema_version: String,
    /// Exact tool invocation UUID also supplied when the ACP lease was begun.
    pub invocation_id: Uuid,
    /// Exact typed operation also supplied when the ACP lease was begun.
    pub operation: ProjectGitOperation,
    /// Session-bound Project home channel UUID.
    pub session_channel_id: String,
    /// Signed request operation UUID.
    pub operation_id: String,
    /// Signed request event ID.
    pub request_event_id: String,
    /// Managed worker pubkey that owns the scoped session.
    pub worker_pubkey: String,
    /// Semantic digest of the exact signed request, once it has been resolved.
    pub scope_digest: Option<String>,
    /// Canonical GitHub repository, once the exact checkout has been resolved.
    pub repository: Option<String>,
    /// Full `refs/...` mutation target, once resolved.
    pub branch_ref: Option<String>,
    /// Object observed before the mutator; `None` also represents an absent ref.
    pub previous_object: Option<String>,
    /// Exact object the invocation intended to install.
    pub intended_object: Option<String>,
    /// Object observed during mandatory post-command reconciliation.
    pub observed_object: Option<String>,
    /// Proven effect disposition. A missing receipt must be treated as ambiguous.
    pub disposition: PrivilegedGitDisposition,
}

/// Result supplied when the privileged-operation lease is released.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrivilegedOperationOutcome {
    /// Operation completed and its result is known.
    Completed,
    /// Operation failed before reporting success.
    Failed,
    /// Operation was interrupted after its lease was acquired.
    Cancelled,
    /// A possibly side-effecting operation could not be reconciled exactly.
    Indeterminate,
}

/// Active lifecycle lease returned only after current authority is fenced.
///
/// Implementations own any durable lock and relay transition needed to keep a
/// Cancel, terminal result, or superseding request from racing the operation.
pub trait TrustedGitOperationLease: Send {
    /// Cancellation triggered when the job loses authority while this lease
    /// remains active. The tool combines it with session/request cancellation.
    fn cancellation_token(&self) -> CancellationToken;

    /// Validate and durably freeze the exact signed Handoff before the typed
    /// relay client is allowed to publish it. Git leases reject this method.
    fn stage_handoff<'a>(
        &'a mut self,
        event: Event,
        cancellation: CancellationToken,
    ) -> PrivilegeFuture<'a, Result<(), String>>;

    /// Reconcile and release the active operation after its child is reaped.
    /// `terminal_event_id` is the exact accepted Handoff event; Git operations
    /// pass `None` because they do not terminalize the job lifecycle.
    fn finish(
        self: Box<Self>,
        outcome: PrivilegedOperationOutcome,
        git_receipt: Option<PrivilegedGitOperationReceipt>,
        terminal_event_id: Option<String>,
    ) -> PrivilegeFuture<'static, Result<(), String>>;
}

/// Harness-owned capability for entering one privileged job operation.
///
/// The implementation lives in `buzz-acp`, where it can validate the durable
/// claim and lifecycle, perform a fresh relay authorization, and persist the
/// receiver-signed start marker. The typed MCP sees only this opaque object.
pub trait JobPrivilegeGate: Send + Sync {
    /// Enter one invocation only after current durable and relay authority has
    /// been revalidated and its signed start marker acknowledged.
    fn begin<'a>(
        &'a self,
        operation: ProjectGitOperation,
        invocation_id: Uuid,
        cancellation: CancellationToken,
    ) -> PrivilegeFuture<'a, Result<Box<dyn TrustedGitOperationLease>, String>>;
}
