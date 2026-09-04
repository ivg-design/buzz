use crate::job::model::JobValidationError;
use crate::job::MAX_IDEMPOTENCY_KEY_BYTES;

use super::body::contains_credential_marker;
use super::primitives::validate_hex;

pub(in crate::job) fn validate_portable_references(
    name: &str,
    values: &[String],
) -> Result<(), JobValidationError> {
    for value in values {
        let lower = value.to_ascii_lowercase();
        let bytes = value.as_bytes();
        let windows_absolute =
            bytes.get(1) == Some(&b':') && bytes.first().is_some_and(u8::is_ascii_alphabetic);
        if value.starts_with('/')
            || value.starts_with('~')
            || value.contains('\\')
            || value.split('/').any(|segment| segment == "..")
            || lower.starts_with("file:")
            || lower.contains("/users/")
            || lower.contains("/home/")
            || contains_credential_marker(&lower)
            || windows_absolute
        {
            return Err(JobValidationError::new(format!(
                "{name} must not expose an absolute or host-local path"
            )));
        }
        if let Some(digest) = value.strip_prefix("git:") {
            validate_hex(name, digest, &[40, 64])?;
            continue;
        }
        if let Some(contract) = value.strip_prefix("contract:") {
            if contract.is_empty()
                || contract.len() > MAX_IDEMPOTENCY_KEY_BYTES
                || !contract.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/')
                })
            {
                return Err(JobValidationError::new(format!(
                    "{name} contract reference is not a portable identifier"
                )));
            }
        }
    }
    Ok(())
}

pub(in crate::job) fn validate_inert_references(
    name: &str,
    values: &[String],
) -> Result<(), JobValidationError> {
    validate_portable_references(name, values)?;
    for value in values {
        if let Some(digest) = value.strip_prefix("git:") {
            validate_hex(name, digest, &[40, 64])?;
            continue;
        }
        if let Some(contract) = value.strip_prefix("contract:") {
            if contract.is_empty()
                || contract.len() > MAX_IDEMPOTENCY_KEY_BYTES
                || contract
                    .split('/')
                    .any(|segment| segment.is_empty() || segment == "..")
                || !contract.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/')
                })
            {
                return Err(JobValidationError::new(format!(
                    "{name} contract reference is not a portable identifier"
                )));
            }
            continue;
        }
        if let Some(event_id) = value.strip_prefix("buzz:event:") {
            validate_hex(name, event_id, &[64])?;
            continue;
        }
        let url = url::Url::parse(value).map_err(|_| {
            JobValidationError::new(format!("{name} must contain inert portable references"))
        })?;
        if url.scheme() != "https"
            || url.host_str() != Some("github.com")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(JobValidationError::new(format!(
                "{name} URL must use canonical credential-free https://github.com without port, query, or fragment"
            )));
        }
    }
    Ok(())
}
