use nostr::{Event, Tag};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::kind::{
    KIND_JOB_ACCEPTED, KIND_JOB_CANCEL, KIND_JOB_ERROR, KIND_JOB_PROGRESS, KIND_JOB_REQUEST,
    KIND_JOB_RESULT,
};

use super::model::{
    JobClaimStatus, JobCommon, JobControlAction, JobErrorOutcome, JobEvent, JobRequest,
    JobValidationError,
};
use super::validation::{
    github_repository_tag, make_tag, require_prior, validate_allowed_tags, validate_common,
    validate_event_id, validate_exact_tag, validate_followup, validate_hex,
    validate_inert_references, validate_list, validate_machine_token, validate_no_secret_material,
    validate_optional_exact_tag, validate_pubkey, validate_text, validate_wire_keys, UniqueValue,
};
use super::{MAX_MESSAGE_BYTES, MAX_SHORT_TEXT_BYTES};

impl JobEvent {
    /// Strictly decode job JSON before any event is signed.
    ///
    /// This rejects duplicate/unknown keys, explicit `null`, credential
    /// material, and kind/body mismatches. Envelope/tag binding is completed
    /// by [`Self::parse`] once a signed event exists.
    pub fn parse_content(kind: u32, content: &str) -> Result<Self, JobValidationError> {
        let raw = serde_json::from_str::<UniqueValue>(content).map_err(|error| {
            JobValidationError::new(format!("job content must be JSON: {error}"))
        })?;
        let raw = raw.0;
        if contains_null(&raw) {
            return Err(JobValidationError::new(
                "job content uses null; omit absent optional fields",
            ));
        }
        validate_no_secret_material(&raw)?;
        validate_wire_keys(kind, &raw)?;
        match kind {
            KIND_JOB_REQUEST => Ok(Self::Request(parse_body(raw)?)),
            KIND_JOB_ACCEPTED => Ok(Self::Accepted(parse_body(raw)?)),
            KIND_JOB_PROGRESS => Ok(Self::Progress(parse_body(raw)?)),
            KIND_JOB_RESULT => Ok(Self::Result(parse_body(raw)?)),
            KIND_JOB_CANCEL => Ok(Self::Control(parse_body(raw)?)),
            KIND_JOB_ERROR => Ok(Self::Error(parse_body(raw)?)),
            kind => Err(JobValidationError::new(format!(
                "kind {kind} is not a job protocol event"
            ))),
        }
    }

    /// Parse a kind 43001-43006 event and enforce the strict JSON/tag contract.
    ///
    /// This does not perform I/O-backed channel, project, sponsor, or transition
    /// authorization; the relay performs those after this pure validation door.
    pub fn parse(event: &Event) -> Result<Self, JobValidationError> {
        let parsed = Self::parse_content(u32::from(event.kind.as_u16()), &event.content)?;
        parsed.validate(event)?;
        Ok(parsed)
    }

    /// Common operation fields.
    #[must_use]
    pub fn common(&self) -> &JobCommon {
        match self {
            Self::Request(body) => &body.common,
            Self::Accepted(body) => &body.followup.common,
            Self::Progress(body) => &body.followup.common,
            Self::Result(body) => &body.followup.common,
            Self::Control(body) => &body.followup.common,
            Self::Error(body) => &body.followup.common,
        }
    }

    /// Root request event ID for follow-ups; requests return `None`.
    #[must_use]
    pub fn request_event_id(&self) -> Option<&str> {
        match self {
            Self::Request(_) => None,
            Self::Accepted(body) => Some(&body.followup.request_event_id),
            Self::Progress(body) => Some(&body.followup.request_event_id),
            Self::Result(body) => Some(&body.followup.request_event_id),
            Self::Control(body) => Some(&body.followup.request_event_id),
            Self::Error(body) => Some(&body.followup.request_event_id),
        }
    }

    /// Immediate predecessor event ID for follow-ups.
    #[must_use]
    pub fn prior_event_id(&self) -> Option<&str> {
        match self {
            Self::Request(_) => None,
            Self::Accepted(body) => body.followup.prior_event_id.as_deref(),
            Self::Progress(body) => body.followup.prior_event_id.as_deref(),
            Self::Result(body) => body.followup.prior_event_id.as_deref(),
            Self::Control(body) => body.followup.prior_event_id.as_deref(),
            Self::Error(body) => body.followup.prior_event_id.as_deref(),
        }
    }

    /// Handoff event superseded by a higher-epoch request.
    #[must_use]
    pub fn supersedes_event_id(&self) -> Option<&str> {
        match self {
            Self::Request(body) => body.supersedes_event_id.as_deref(),
            _ => None,
        }
    }

    /// Canonical JSON serialization used for receiver-side semantic digests.
    pub fn canonical_json(&self) -> Result<String, JobValidationError> {
        match self {
            Self::Request(body) => serialize_body(body),
            Self::Accepted(body) => serialize_body(body),
            Self::Progress(body) => serialize_body(body),
            Self::Result(body) => serialize_body(body),
            Self::Control(body) => serialize_body(body),
            Self::Error(body) => serialize_body(body),
        }
    }

    fn validate(&self, event: &Event) -> Result<(), JobValidationError> {
        let common = self.common();
        validate_allowed_tags(event)?;
        validate_common(common, event)?;
        validate_exact_tag(event, "h", &common.project.home_channel)?;
        validate_exact_tag(event, "p", &common.recipient_pubkey)?;
        validate_exact_tag(event, "i", &common.operation_id)?;
        validate_exact_tag(event, "k", &common.idempotency_key)?;
        validate_exact_tag(event, "a", &common.project.address)?;
        let repository_tag = github_repository_tag(&common.repository.canonical)?;
        validate_exact_tag(event, "github-repository", &repository_tag)?;
        validate_optional_exact_tag(
            event,
            "github-issue",
            common.repository.github_issue.as_deref(),
        )?;
        validate_optional_exact_tag(event, "github-pr", common.repository.github_pr.as_deref())?;
        validate_optional_exact_tag(event, "github-run", common.repository.github_run.as_deref())?;

        match self {
            Self::Request(body) => {
                let e_tags: Vec<&[String]> = event
                    .tags
                    .iter()
                    .filter_map(|tag| {
                        let parts = tag.as_slice();
                        (parts.first().map(String::as_str) == Some("e")).then_some(parts)
                    })
                    .collect();
                match body.supersedes_event_id.as_deref() {
                    None if e_tags.is_empty() => {}
                    Some(id) => {
                        validate_event_id("supersedes_event_id", id)?;
                        if e_tags.len() != 1 || e_tags[0] != ["e", id, "", "supersedes"] {
                            return Err(JobValidationError::new(
                                "superseding request must carry one canonical supersedes e tag",
                            ));
                        }
                    }
                    None => {
                        return Err(JobValidationError::new(
                            "initial job request must not carry an e tag",
                        ));
                    }
                }
                validate_text("capability", &body.capability, MAX_SHORT_TEXT_BYTES)?;
                validate_text("summary", &body.summary, MAX_MESSAGE_BYTES)?;
                validate_list("acceptance", &body.acceptance, true)?;
            }
            Self::Accepted(body) => {
                validate_followup(event, &body.followup)?;
                match body.claim.status {
                    JobClaimStatus::Processed if body.followup.prior_event_id.is_some() => {
                        return Err(JobValidationError::new(
                            "processed receipt must omit prior_event_id",
                        ));
                    }
                    JobClaimStatus::Accepted if body.followup.prior_event_id.is_none() => {
                        return Err(JobValidationError::new(
                            "accepted receipt requires prior_event_id",
                        ));
                    }
                    JobClaimStatus::Declined if body.followup.prior_event_id.is_some() => {
                        return Err(JobValidationError::new(
                            "declined receipt must omit prior_event_id",
                        ));
                    }
                    _ => {}
                }
                match (body.claim.status, body.claim.reason.as_deref()) {
                    (JobClaimStatus::Declined, Some(reason)) => {
                        validate_machine_token("claim.reason", reason)?;
                    }
                    (JobClaimStatus::Declined, None) => {
                        return Err(JobValidationError::new(
                            "declined receipt requires claim.reason",
                        ));
                    }
                    (_, Some(_)) => {
                        return Err(JobValidationError::new(
                            "claim.reason is only valid for declined receipts",
                        ));
                    }
                    (_, None) => {}
                }
                validate_hex("scope_digest", &body.claim.scope_digest, &[64])?;
            }
            Self::Progress(body) => {
                validate_followup(event, &body.followup)?;
                require_prior(&body.followup, "progress")?;
                validate_text("message", &body.message, MAX_MESSAGE_BYTES)?;
                validate_list("evidence", &body.evidence, false)?;
                validate_inert_references("evidence", &body.evidence)?;
            }
            Self::Result(body) => {
                validate_followup(event, &body.followup)?;
                require_prior(&body.followup, "result")?;
                if let Some(sha) = &body.candidate_sha {
                    validate_hex("candidate_sha", sha, &[40, 64])?;
                }
                validate_list("artifacts", &body.artifacts, false)?;
                validate_list("evidence", &body.evidence, false)?;
                validate_inert_references("artifacts", &body.artifacts)?;
                validate_inert_references("evidence", &body.evidence)?;
                validate_list("capabilities", &body.capabilities, false)?;
            }
            Self::Control(body) => {
                validate_followup(event, &body.followup)?;
                if matches!(
                    body.action,
                    JobControlAction::Cancelled
                        | JobControlAction::Release
                        | JobControlAction::Handoff
                ) {
                    require_prior(&body.followup, "cancelled/release/handoff")?;
                }
                validate_text("reason", &body.reason, MAX_MESSAGE_BYTES)?;
                match (body.action, body.handoff_to.as_deref()) {
                    (JobControlAction::Handoff, Some(target)) => {
                        validate_pubkey("handoff_to", target)?;
                    }
                    (JobControlAction::Handoff, None) => {
                        return Err(JobValidationError::new(
                            "handoff action requires handoff_to",
                        ));
                    }
                    (_, Some(_)) => {
                        return Err(JobValidationError::new(
                            "handoff_to is only valid for handoff action",
                        ));
                    }
                    (_, None) => {}
                }
            }
            Self::Error(body) => {
                validate_followup(event, &body.followup)?;
                require_prior(&body.followup, "error")?;
                validate_text("code", &body.code, MAX_SHORT_TEXT_BYTES)?;
                validate_text("message", &body.message, MAX_MESSAGE_BYTES)?;
                if body.outcome == JobErrorOutcome::Indeterminate && body.retryable {
                    return Err(JobValidationError::new(
                        "indeterminate errors require retryable=false pending reconciliation",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Build the canonical routing tags for a job body.
pub fn build_job_tags(job: &JobEvent) -> Result<Vec<Tag>, JobValidationError> {
    let common = job.common();
    let mut tags = vec![
        make_tag(["h", common.project.home_channel.as_str()])?,
        make_tag(["p", common.recipient_pubkey.as_str()])?,
        make_tag(["i", common.operation_id.as_str()])?,
        make_tag(["k", common.idempotency_key.as_str()])?,
        make_tag(["a", common.project.address.as_str()])?,
        make_tag([
            "github-repository",
            github_repository_tag(&common.repository.canonical)?.as_str(),
        ])?,
    ];
    for (name, value) in [
        ("github-issue", common.repository.github_issue.as_deref()),
        ("github-pr", common.repository.github_pr.as_deref()),
        ("github-run", common.repository.github_run.as_deref()),
    ] {
        if let Some(value) = value {
            tags.push(make_tag([name, value])?);
        }
    }
    if let Some(root) = job.request_event_id() {
        tags.push(make_tag(["e", root, "", "root"])?);
    }
    if let Some(prior) = job.prior_event_id() {
        tags.push(make_tag(["e", prior, "", "reply"])?);
    }
    if let Some(supersedes) = job.supersedes_event_id() {
        tags.push(make_tag(["e", supersedes, "", "supersedes"])?);
    }
    Ok(tags)
}

fn parse_body<T: for<'de> Deserialize<'de>>(raw: Value) -> Result<T, JobValidationError> {
    serde_json::from_value(raw)
        .map_err(|error| JobValidationError::new(format!("invalid job content: {error}")))
}

fn serialize_body<T: Serialize>(body: &T) -> Result<String, JobValidationError> {
    let value = serde_json::to_value(body)
        .map_err(|error| JobValidationError::new(format!("serializing job content: {error}")))?;
    serde_json::to_string(&sort_json(value))
        .map_err(|error| JobValidationError::new(format!("serializing job content: {error}")))
}

fn sort_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sort_json).collect()),
        Value::Object(values) => {
            let mut entries: Vec<(String, Value)> = values.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut sorted = serde_json::Map::new();
            for (key, value) in entries {
                sorted.insert(key, sort_json(value));
            }
            Value::Object(sorted)
        }
        scalar => scalar,
    }
}

/// Receiver-computed SHA-256 digest of canonical kind-43001 request semantics.
pub fn semantic_request_digest(request: &JobRequest) -> Result<String, JobValidationError> {
    use sha2::{Digest, Sha256};

    let canonical = serialize_body(request)?;
    Ok(hex::encode(Sha256::digest(canonical.as_bytes())))
}

fn contains_null(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Array(values) => values.iter().any(contains_null),
        Value::Object(values) => values.values().any(contains_null),
        _ => false,
    }
}
