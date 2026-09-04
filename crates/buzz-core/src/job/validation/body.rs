use std::path::{Component, Path};

use nostr::{Event, Tag};
use serde_json::Value;

use super::primitives::{
    validate_branch, validate_event_id, validate_expiry, validate_hex, validate_idempotency_key,
    validate_list, validate_pubkey, validate_text, validate_uuid, validate_worktree_id,
};
use super::references::validate_inert_references;
use crate::job::model::{JobCommon, JobFollowup, JobRepository, JobValidationError};
use crate::job::{JOB_SCHEMA_VERSION, MAX_SHORT_TEXT_BYTES};

pub(in crate::job) fn validate_no_secret_material(value: &Value) -> Result<(), JobValidationError> {
    match value {
        Value::String(text) => {
            let lower = text.to_ascii_lowercase();
            if contains_credential_marker(&lower) {
                return Err(JobValidationError::new(
                    "job content must not contain credential material",
                ));
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_no_secret_material(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_no_secret_material(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn contains_credential_marker(lower: &str) -> bool {
    [
        "token=",
        "password=",
        "secret=",
        "authorization:",
        "bearer ",
        "begin private key",
        "github_token",
        "api_key",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
        || (lower.contains("-----begin ") && lower.contains(" private key-----"))
        || ["github_pat_", "ghp_", "sk-"]
            .iter()
            .any(|prefix| contains_prefixed_credential(lower, prefix))
}

fn contains_prefixed_credential(value: &str, prefix: &str) -> bool {
    value.match_indices(prefix).any(|(offset, _)| {
        value[offset + prefix.len()..]
            .bytes()
            .take_while(u8::is_ascii_alphanumeric)
            .take(12)
            .count()
            == 12
    })
}

pub(in crate::job) fn validate_common(
    common: &JobCommon,
    event: &Event,
) -> Result<(), JobValidationError> {
    if common.schema_version != JOB_SCHEMA_VERSION {
        return Err(JobValidationError::new(format!(
            "schema_version must be {JOB_SCHEMA_VERSION}"
        )));
    }
    validate_uuid("operation_id", &common.operation_id)?;
    validate_idempotency_key(&common.idempotency_key)?;
    if common.coordinator_epoch == 0 {
        return Err(JobValidationError::new(
            "coordinator_epoch must be greater than zero",
        ));
    }
    validate_project_address(&common.project.address)?;
    validate_uuid("project.home_channel", &common.project.home_channel)?;
    validate_repository(&common.repository)?;
    validate_pubkey("sender_pubkey", &common.sender_pubkey)?;
    validate_pubkey("recipient_pubkey", &common.recipient_pubkey)?;
    validate_pubkey("sponsor.pubkey", &common.sponsor.pubkey)?;
    validate_text(
        "sponsor.github_login",
        &common.sponsor.github_login,
        MAX_SHORT_TEXT_BYTES,
    )?;
    validate_expiry(&common.expires_at)?;
    if common.sender_pubkey != event.pubkey.to_hex() {
        return Err(JobValidationError::new(
            "sender_pubkey does not match signed event author",
        ));
    }
    if common.sender_pubkey == common.recipient_pubkey {
        return Err(JobValidationError::new(
            "job sender and recipient must differ",
        ));
    }
    Ok(())
}

pub(crate) fn validate_project_address(address: &str) -> Result<(), JobValidationError> {
    validate_text("project.address", address, MAX_SHORT_TEXT_BYTES)?;
    let mut parts = address.split(':');
    if parts.next() != Some("30621") {
        return Err(JobValidationError::new(
            "project.address must use kind 30621",
        ));
    }
    let owner = parts.next().ok_or_else(|| {
        JobValidationError::new("project.address must contain a canonical owner pubkey")
    })?;
    let identifier = parts.next().ok_or_else(|| {
        JobValidationError::new("project.address must contain a non-empty project identifier")
    })?;
    if parts.next().is_some() || identifier.is_empty() {
        return Err(JobValidationError::new(
            "project.address must be 30621:<lowercase-hex-owner>:<nonempty-id>",
        ));
    }
    validate_pubkey("project.address owner", owner)?;
    if owner != owner.to_ascii_lowercase() {
        return Err(JobValidationError::new(
            "project.address owner must use lowercase hex",
        ));
    }
    if identifier.len() > MAX_SHORT_TEXT_BYTES
        || !identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(JobValidationError::new(
            "project.address identifier must be a portable non-empty identifier",
        ));
    }
    Ok(())
}

pub(crate) fn validate_repository(repo: &JobRepository) -> Result<(), JobValidationError> {
    github_repository_tag(&repo.canonical)?;
    if repo.github_issue.is_some() && repo.github_pr.is_some() {
        return Err(JobValidationError::new(
            "repository.github_issue and repository.github_pr are mutually exclusive",
        ));
    }
    for (name, value) in [
        ("repository.github_issue", repo.github_issue.as_deref()),
        ("repository.github_pr", repo.github_pr.as_deref()),
        ("repository.github_run", repo.github_run.as_deref()),
    ] {
        if let Some(value) = value {
            validate_positive_decimal(name, value)?;
        }
    }
    validate_hex("repository.base_sha", &repo.base_sha, &[40, 64])?;
    validate_branch(&repo.branch)?;
    validate_worktree_id(&repo.worktree_id)?;
    validate_list("repository.paths", &repo.paths, true)?;
    validate_list("repository.contracts", &repo.contracts, false)?;
    validate_inert_references("repository.contracts", &repo.contracts)?;
    if repo
        .contracts
        .iter()
        .any(|contract| !contract.starts_with("contract:"))
    {
        return Err(JobValidationError::new(
            "repository.contracts must contain only inert contract: coordinates",
        ));
    }
    for path in &repo.paths {
        let candidate = Path::new(path);
        if candidate.is_absolute()
            || path.contains('\\')
            || path
                .split('/')
                .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
            || candidate
                .components()
                .any(|part| !matches!(part, Component::Normal(_)))
        {
            return Err(JobValidationError::new(format!(
                "repository path must be normalized and repo-relative: {path}"
            )));
        }
    }
    Ok(())
}

fn validate_positive_decimal(name: &str, value: &str) -> Result<(), JobValidationError> {
    if value.is_empty()
        || value.len() > 20
        || value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(JobValidationError::new(format!(
            "{name} must be a canonical positive decimal identifier"
        )));
    }
    Ok(())
}

pub(in crate::job) fn github_repository_tag(canonical: &str) -> Result<String, JobValidationError> {
    let parsed = url::Url::parse(canonical).map_err(|_| {
        JobValidationError::new("repository.canonical must be a canonical GitHub URL")
    })?;
    if parsed.scheme() != "https"
        || parsed.host_str() != Some("github.com")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(JobValidationError::new(
            "repository.canonical must be https://github.com/owner/repo without credentials, port, query, or fragment",
        ));
    }
    let segments: Vec<&str> = parsed
        .path_segments()
        .map(Iterator::collect)
        .unwrap_or_default();
    if segments.len() != 2
        || segments.iter().any(|segment| {
            segment.is_empty()
                || !segment.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'_' | b'-')
                })
        })
        || segments[1].ends_with(".git")
    {
        return Err(JobValidationError::new(
            "repository.canonical owner/repo must use lowercase GitHub path spelling without .git",
        ));
    }
    let expected = format!("https://github.com/{}/{}", segments[0], segments[1]);
    if canonical != expected {
        return Err(JobValidationError::new(
            "repository.canonical must not contain a trailing slash or alternate spelling",
        ));
    }
    Ok(format!("{}/{}", segments[0], segments[1]))
}

pub(in crate::job) fn validate_followup(
    event: &Event,
    followup: &JobFollowup,
) -> Result<(), JobValidationError> {
    validate_event_id("request_event_id", &followup.request_event_id)?;
    if let Some(prior) = &followup.prior_event_id {
        validate_event_id("prior_event_id", prior)?;
        if prior == &followup.request_event_id {
            return Err(JobValidationError::new(
                "prior_event_id must differ from request_event_id",
            ));
        }
    }
    let e_tags: Vec<&[String]> = event
        .tags
        .iter()
        .filter_map(|tag| {
            let parts = tag.as_slice();
            (parts.first().map(String::as_str) == Some("e")).then_some(parts)
        })
        .collect();
    let expected = usize::from(followup.prior_event_id.is_some()) + 1;
    if e_tags.len() != expected || e_tags[0] != ["e", &followup.request_event_id, "", "root"] {
        return Err(JobValidationError::new(
            "follow-up must carry one canonical root e tag",
        ));
    }
    if let Some(prior) = &followup.prior_event_id {
        if e_tags[1] != ["e", prior, "", "reply"] {
            return Err(JobValidationError::new(
                "prior_event_id must match one canonical reply e tag",
            ));
        }
    }
    Ok(())
}

pub(in crate::job) fn require_prior(
    followup: &JobFollowup,
    label: &str,
) -> Result<(), JobValidationError> {
    if followup.prior_event_id.is_none() {
        return Err(JobValidationError::new(format!(
            "{label} requires prior_event_id"
        )));
    }
    Ok(())
}

pub(in crate::job) fn validate_exact_tag(
    event: &Event,
    name: &str,
    expected: &str,
) -> Result<(), JobValidationError> {
    let matches: Vec<&[String]> = event
        .tags
        .iter()
        .filter_map(|tag| {
            let parts = tag.as_slice();
            (parts.first().map(String::as_str) == Some(name)).then_some(parts)
        })
        .collect();
    if matches.len() != 1 || matches[0] != [name, expected] {
        return Err(JobValidationError::new(format!(
            "job event must carry exactly one [{name}, value] tag matching its body"
        )));
    }
    Ok(())
}

pub(in crate::job) fn validate_optional_exact_tag(
    event: &Event,
    name: &str,
    expected: Option<&str>,
) -> Result<(), JobValidationError> {
    let matches: Vec<&[String]> = event
        .tags
        .iter()
        .filter_map(|tag| {
            let parts = tag.as_slice();
            (parts.first().map(String::as_str) == Some(name)).then_some(parts)
        })
        .collect();
    match expected {
        Some(value) if matches.len() == 1 && matches[0] == [name, value] => Ok(()),
        None if matches.is_empty() => Ok(()),
        Some(_) => Err(JobValidationError::new(format!(
            "job event must carry exactly one {name} tag matching its body"
        ))),
        None => Err(JobValidationError::new(format!(
            "job event must omit {name} when its body field is absent"
        ))),
    }
}

pub(in crate::job) fn validate_allowed_tags(event: &Event) -> Result<(), JobValidationError> {
    const ALLOWED: &[&str] = &[
        "h",
        "p",
        "i",
        "k",
        "a",
        "e",
        "github-repository",
        "github-issue",
        "github-pr",
        "github-run",
    ];
    for tag in event.tags.iter() {
        let Some(name) = tag.as_slice().first().map(String::as_str) else {
            return Err(JobValidationError::new("job event contains an empty tag"));
        };
        if !ALLOWED.contains(&name) {
            return Err(JobValidationError::new(format!(
                "job event contains unsupported tag {name}"
            )));
        }
    }
    Ok(())
}

pub(in crate::job) fn make_tag<const N: usize>(
    parts: [&str; N],
) -> Result<Tag, JobValidationError> {
    Tag::parse(parts).map_err(|error| JobValidationError::new(format!("building job tag: {error}")))
}
