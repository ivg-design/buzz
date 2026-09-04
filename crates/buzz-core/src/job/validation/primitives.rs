use chrono::{DateTime, SecondsFormat, Utc};
use nostr::{EventId, PublicKey};
use uuid::Uuid;

use crate::job::model::JobValidationError;
use crate::job::{
    MAX_IDEMPOTENCY_KEY_BYTES, MAX_LIST_ITEMS, MAX_MESSAGE_BYTES, MAX_SHORT_TEXT_BYTES,
};

pub(super) fn validate_idempotency_key(value: &str) -> Result<(), JobValidationError> {
    if value.is_empty()
        || value.len() > MAX_IDEMPOTENCY_KEY_BYTES
        || value.bytes().any(|byte| !(0x21..=0x7e).contains(&byte))
    {
        return Err(JobValidationError::new(format!(
            "idempotency_key must be 1-{MAX_IDEMPOTENCY_KEY_BYTES} printable ASCII bytes without spaces"
        )));
    }
    Ok(())
}

pub(super) fn validate_worktree_id(value: &str) -> Result<(), JobValidationError> {
    if value.is_empty()
        || value.len() > MAX_IDEMPOTENCY_KEY_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
    {
        return Err(JobValidationError::new(
            "repository.worktree_id must be an opaque 1-128 byte portable identifier",
        ));
    }
    Ok(())
}

pub(super) fn validate_branch(value: &str) -> Result<(), JobValidationError> {
    let invalid_byte = |byte: u8| {
        byte <= 0x20
            || byte == 0x7f
            || matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
    };
    if value.is_empty()
        || value.len() > MAX_SHORT_TEXT_BYTES
        || value == "@"
        || value.starts_with('/')
        || value.ends_with('/')
        || value.ends_with('.')
        || value.contains("//")
        || value.contains("..")
        || value.contains("@{")
        || value.bytes().any(invalid_byte)
        || value
            .split('/')
            .any(|segment| segment.starts_with('.') || segment.ends_with(".lock"))
    {
        return Err(JobValidationError::new(
            "repository.branch must be a conservative canonical git ref name",
        ));
    }
    Ok(())
}

pub(super) fn validate_uuid(name: &str, value: &str) -> Result<(), JobValidationError> {
    let parsed = Uuid::parse_str(value)
        .map_err(|_| JobValidationError::new(format!("{name} must be a UUID")))?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(JobValidationError::new(format!(
            "{name} must use canonical non-nil lowercase hyphenated UUID spelling"
        )));
    }
    Ok(())
}

pub(in crate::job) fn validate_pubkey(name: &str, value: &str) -> Result<(), JobValidationError> {
    let parsed = PublicKey::parse(value)
        .map_err(|_| JobValidationError::new(format!("{name} must be a public key")))?;
    if parsed.to_hex() != value {
        return Err(JobValidationError::new(format!(
            "{name} must use canonical lowercase hex spelling"
        )));
    }
    Ok(())
}

pub(in crate::job) fn validate_event_id(name: &str, value: &str) -> Result<(), JobValidationError> {
    let parsed = EventId::from_hex(value)
        .map_err(|_| JobValidationError::new(format!("{name} must be an event ID")))?;
    if parsed.to_hex() != value {
        return Err(JobValidationError::new(format!(
            "{name} must use canonical lowercase hex spelling"
        )));
    }
    Ok(())
}

pub(in crate::job) fn validate_hex(
    name: &str,
    value: &str,
    lengths: &[usize],
) -> Result<(), JobValidationError> {
    if !lengths.contains(&value.len())
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(JobValidationError::new(format!(
            "{name} must be canonical lowercase hex with length {lengths:?}"
        )));
    }
    Ok(())
}

pub(super) fn validate_expiry(value: &str) -> Result<(), JobValidationError> {
    let parsed = DateTime::parse_from_rfc3339(value)
        .map_err(|_| JobValidationError::new("expires_at must be RFC3339"))?
        .with_timezone(&Utc);
    if parsed.to_rfc3339_opts(SecondsFormat::Secs, true) != value {
        return Err(JobValidationError::new(
            "expires_at must use canonical UTC RFC3339 seconds",
        ));
    }
    Ok(())
}

pub(in crate::job) fn validate_text(
    name: &str,
    value: &str,
    max: usize,
) -> Result<(), JobValidationError> {
    if value.trim() != value
        || value.is_empty()
        || value.len() > max
        || value
            .chars()
            .any(|ch| ch.is_control() && ch != '\n' && ch != '\t')
    {
        return Err(JobValidationError::new(format!(
            "{name} must be non-empty, trimmed, and at most {max} bytes"
        )));
    }
    Ok(())
}

pub(in crate::job) fn validate_machine_token(
    name: &str,
    value: &str,
) -> Result<(), JobValidationError> {
    if value.is_empty()
        || value.len() > 64
        || !value.as_bytes()[0].is_ascii_lowercase() && !value.as_bytes()[0].is_ascii_digit()
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(JobValidationError::new(format!(
            "{name} must be a 1-64 byte lowercase machine token"
        )));
    }
    Ok(())
}

pub(in crate::job) fn validate_list(
    name: &str,
    values: &[String],
    required: bool,
) -> Result<(), JobValidationError> {
    if values.len() > MAX_LIST_ITEMS || (required && values.is_empty()) {
        return Err(JobValidationError::new(format!(
            "{name} must contain {}-{MAX_LIST_ITEMS} items",
            usize::from(required)
        )));
    }
    for value in values {
        validate_text(name, value, MAX_MESSAGE_BYTES)?;
    }
    Ok(())
}
