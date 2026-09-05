use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A project coordinate and its authoritative collaboration channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobProject {
    /// NIP-33 project address (`30621:<pubkey>:<d>`).
    pub address: String,
    /// Canonical UUID of the project's Buzz home channel.
    pub home_channel: String,
}

/// Canonical repository and checkout scope for a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobRepository {
    /// Canonical repository identifier or URL.
    pub canonical: String,
    /// Canonical GitHub issue reference, when one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub github_issue: Option<String>,
    /// Canonical GitHub pull-request reference, when one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub github_pr: Option<String>,
    /// Canonical GitHub run reference, when one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub github_run: Option<String>,
    /// Immutable base commit SHA.
    pub base_sha: String,
    /// Intended branch name.
    pub branch: String,
    /// Opaque portable worktree identifier (never a host path).
    pub worktree_id: String,
    /// Repository-relative ownership coordinates; empty for an information-only request.
    pub paths: Vec<String>,
    /// Inert `contract:<portable-id>` coordinates resolved by trusted local policy.
    pub contracts: Vec<String>,
}

/// Human sponsor asserted in the body and verified from Buzz ownership state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobSponsor {
    /// Sponsor's canonical hex Nostr public key.
    pub pubkey: String,
    /// Sponsor's GitHub login (metadata only, never an authority source).
    pub github_login: String,
}

/// Optional initiating Buzz conversation retained for presentation routing.
///
/// This is descriptive context only. The signed job `h` and `p` tags remain
/// the transport and authorization coordinates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobOrigin {
    /// Channel in which the requester initiated the task.
    pub channel_id: String,
    /// Existing conversation root when the task began inside a thread.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_root_id: Option<String>,
    /// Immutable requester provider-session channel used for continuation routing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_channel_id: Option<String>,
    /// Immutable requester provider-session root used for continuation routing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_thread_root_id: Option<String>,
}

/// Ordinary Buzz task conversation chosen by the requester before dispatch.
///
/// This is a presentation destination only. The project home channel and the
/// signed job route tags remain the protocol and authorization coordinates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobConversation {
    /// Channel containing the ordinary task thread.
    pub channel_id: String,
    /// Existing kind-9 task root created before the job request is dispatched.
    pub thread_root_id: String,
}

/// Fields repeated by every event in one operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCommon {
    /// Wire schema discriminator; always [`JOB_SCHEMA_VERSION`].
    pub schema_version: String,
    /// Stable UUID for the logical operation.
    pub operation_id: String,
    /// Caller-chosen stable idempotency key.
    pub idempotency_key: String,
    /// Positive coordinator generation for replay and handoff fencing.
    pub coordinator_epoch: u32,
    /// Project address and home channel.
    pub project: JobProject,
    /// Ordinary authenticated conversation used for human-facing task updates.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation: Option<JobConversation>,
    /// GitHub/repository/checkout scope.
    pub repository: JobRepository,
    /// Signed event actor, repeated for body/tag consistency checks.
    pub sender_pubkey: String,
    /// Single addressed peer, repeated from the event's `p` tag.
    pub recipient_pubkey: String,
    /// Human sponsor assertion. Relays must verify this against ownership state.
    pub sponsor: JobSponsor,
    /// Canonical UTC RFC3339 expiry.
    pub expires_at: String,
}

/// Kind 43001 request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRequest {
    /// Common routing and scope fields.
    #[serde(flatten)]
    pub common: JobCommon,
    /// Stable capability name required from the recipient.
    pub capability: String,
    /// Optional compact presentation title; execution authority never depends on it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional initiating conversation for human-facing result routing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<JobOrigin>,
    /// Short human-readable task summary.
    pub summary: String,
    /// Falsifiable acceptance criteria.
    pub acceptance: Vec<String>,
    /// Terminal handoff event superseded by this higher-epoch request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supersedes_event_id: Option<String>,
}

/// Processing/claim lifecycle state carried by kind 43002.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobClaimStatus {
    /// Recipient durably processed the delivery, but has not claimed execution.
    Processed,
    /// Recipient durably accepted and claimed execution.
    Accepted,
    /// Recipient refuses the request before accepting execution.
    Declined,
}

/// Claim details for kind 43002.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobClaim {
    /// Processing milestone.
    pub status: JobClaimStatus,
    /// Recipient-computed SHA-256 semantic scope digest.
    pub scope_digest: String,
    /// Stable machine reason, required only for `declined`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Fields shared by kinds 43002-43006.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobFollowup {
    /// Common routing and scope fields.
    #[serde(flatten)]
    pub common: JobCommon,
    /// Exact kind 43001 event ID at the operation root.
    pub request_event_id: String,
    /// Immediate predecessor event ID, if one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prior_event_id: Option<String>,
}

/// Kind 43002 processed/accepted body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobAccepted {
    /// Follow-up routing fields.
    #[serde(flatten)]
    pub followup: JobFollowup,
    /// Processing or accepted claim.
    pub claim: JobClaim,
}

/// Progress state carried by kind 43003.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobProgressStatus {
    /// Work is advancing.
    Progress,
    /// Work cannot advance without a named condition changing.
    Blocked,
}

/// Kind 43003 progress/block body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobProgress {
    /// Follow-up routing fields.
    #[serde(flatten)]
    pub followup: JobFollowup,
    /// Progress or blocked milestone.
    pub status: JobProgressStatus,
    /// Bounded human-readable update.
    pub message: String,
    /// Durable evidence references.
    pub evidence: Vec<String>,
}

/// Successful outcome discriminator for kind 43004.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobSuccessOutcome {
    /// Execution completed successfully.
    Success,
}

/// Kind 43004 successful result body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobResult {
    /// Follow-up routing fields.
    #[serde(flatten)]
    pub followup: JobFollowup,
    /// Must be `success`; relay delivery alone never implies this outcome.
    pub outcome: JobSuccessOutcome,
    /// Bounded human-readable result summary. Absent on earlier v1 events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// Candidate commit SHA, if the work produced one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub candidate_sha: Option<String>,
    /// Result artifact references.
    pub artifacts: Vec<String>,
    /// Durable validation evidence.
    pub evidence: Vec<String>,
    /// Capabilities advertised by a capability-discovery result.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
}

/// Control action carried by kind 43005.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobControlAction {
    /// Request cancellation.
    Cancel,
    /// Worker acknowledgement that cancellation is complete.
    Cancelled,
    /// Current recipient releases its claim.
    Release,
    /// Current recipient requests handoff to another direct channel member.
    Handoff,
}

/// Kind 43005 cancel/release/handoff body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobControl {
    /// Follow-up routing fields.
    #[serde(flatten)]
    pub followup: JobFollowup,
    /// Requested control action.
    pub action: JobControlAction,
    /// Bounded explanation.
    pub reason: String,
    /// New recipient for a handoff; forbidden for cancel/release.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handoff_to: Option<String>,
}

/// Error outcome discriminator for kind 43006.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobErrorOutcome {
    /// Execution failed with a known terminal error.
    Failed,
    /// Side-effect status cannot be proven; reconciliation is required.
    Indeterminate,
}

/// Kind 43006 terminal error body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobError {
    /// Follow-up routing fields.
    #[serde(flatten)]
    pub followup: JobFollowup,
    /// Must be `failed` or `indeterminate`.
    pub outcome: JobErrorOutcome,
    /// Stable machine-readable error code.
    pub code: String,
    /// Bounded human-readable error.
    pub message: String,
    /// Whether a new request may succeed. Must be false for `indeterminate`;
    /// reconciliation is required before any retry.
    pub retryable: bool,
}

/// Strict typed view of one signed job event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobEvent {
    /// Kind 43001.
    Request(JobRequest),
    /// Kind 43002.
    Accepted(JobAccepted),
    /// Kind 43003.
    Progress(JobProgress),
    /// Kind 43004.
    Result(JobResult),
    /// Kind 43005.
    Control(JobControl),
    /// Kind 43006.
    Error(JobError),
}

/// Strict schema, canonicalization, or tag-binding failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{0}")]
pub struct JobValidationError(String);

impl JobValidationError {
    pub(super) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}
