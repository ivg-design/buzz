use std::collections::HashSet;

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

const OUTCOME_SCHEMA: &str = "buzz.job-outcome.v1";
const MAX_SUMMARY_BYTES: usize = 8 * 1024;
const MAX_REASON_BYTES: usize = 8 * 1024;
const MAX_REFERENCES: usize = 256;
const MAX_REFERENCE_BYTES: usize = 8 * 1024;
const MAX_TOTAL_REFERENCE_BYTES: usize = 64 * 1024;
const MAX_REPORT_DEPTH: usize = 8;
const MAX_REPORT_NODES: usize = 4_096;
const MAX_REPORT_OBJECT_FIELDS: usize = 64;
const MAX_REPORT_ARRAY_ITEMS: usize = 256;
const MAX_REPORT_KEY_BYTES: usize = 128;
const MAX_HUMAN_REPORT_BYTES: usize = 64 * 1024;

#[derive(Debug, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum OutcomeEnvelope {
    Success {
        schema_version: String,
        operation_id: String,
        request_event_id: String,
        scope_digest: String,
        summary: String,
        #[serde(default)]
        candidate_sha: Option<String>,
        artifacts: Vec<serde_json::Value>,
        evidence: Vec<serde_json::Value>,
        #[serde(default)]
        limits: Option<serde_json::Value>,
    },
    Failed {
        schema_version: String,
        operation_id: String,
        request_event_id: String,
        scope_digest: String,
        code: String,
        reason: String,
        retryable: bool,
        #[serde(default)]
        limits: Option<serde_json::Value>,
    },
    Indeterminate {
        schema_version: String,
        operation_id: String,
        request_event_id: String,
        scope_digest: String,
        code: String,
        reason: String,
        retryable: bool,
        #[serde(default)]
        limits: Option<serde_json::Value>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalDisposition {
    Success {
        summary: String,
        candidate_sha: Option<String>,
        artifacts: Vec<String>,
        evidence: Vec<String>,
    },
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
    Indeterminate {
        code: String,
        message: String,
    },
}

/// Accept an optional single Markdown JSON fence without weakening scope or
/// duplicate-key validation. Multiple objects/fences remain invalid JSON.
pub(super) fn terminal_json_text(text: &str) -> &str {
    let text = text.trim();
    // Some adapters omit logical message IDs and concatenate the final
    // explanation with one fenced envelope. Accept that final block only;
    // do not scan arbitrary prose for objects or choose among multiple fences.
    let fences: Vec<_> = text.match_indices("```").collect();
    if fences.len() != 2 || fences[1].0 + 3 != text.len() {
        return text;
    }
    let fenced = &text[fences[0].0..];
    let unfenced = fenced
        .strip_prefix("```json\n")
        .or_else(|| fenced.strip_prefix("```JSON\n"))
        .or_else(|| fenced.strip_prefix("```\n"));
    unfenced
        .and_then(|body| body.trim_end().strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(text)
}

/// Parse one exact, scope-bound terminal envelope from the assistant's final text.
pub fn parse_terminal_outcome(
    text: Option<String>,
    operation_id: &str,
    request_event_id: &str,
    scope_digest: &str,
) -> TerminalDisposition {
    parse_terminal_outcome_with_report(text, operation_id, request_event_id, scope_digest, None)
}

/// Parse a terminal envelope and bind descriptive output to a confirmed report event.
///
/// Portable reference strings remain lifecycle fields. Structured or free-form
/// report descriptors never enter the lifecycle schema; a confirmed report event
/// is required and represented by one `buzz:event:<id>` evidence reference.
pub fn parse_terminal_outcome_with_report(
    text: Option<String>,
    operation_id: &str,
    request_event_id: &str,
    scope_digest: &str,
    confirmed_report_event_id: Option<&str>,
) -> TerminalDisposition {
    let fallback = || TerminalDisposition::Indeterminate {
        code: "invalid_terminal_envelope".into(),
        message: "Worker did not return one valid scope-bound buzz.job-outcome.v1 envelope".into(),
    };
    let Some(text) = text else { return fallback() };
    let Ok(unique) = serde_json::from_str::<NoDuplicateValue>(terminal_json_text(&text)) else {
        return fallback();
    };
    let Ok(envelope) = serde_json::from_value::<OutcomeEnvelope>(unique.0) else {
        return fallback();
    };
    match envelope {
        OutcomeEnvelope::Success {
            schema_version,
            operation_id: actual_operation,
            request_event_id: actual_request,
            scope_digest: actual_digest,
            summary,
            candidate_sha,
            artifacts,
            evidence,
            limits,
        } if exact_scope(
            &schema_version,
            &actual_operation,
            &actual_request,
            &actual_digest,
            operation_id,
            request_event_id,
            scope_digest,
        ) && valid_text(&summary, MAX_SUMMARY_BYTES)
            && candidate_sha
                .as_deref()
                .is_none_or(|sha| valid_lower_hex(sha, &[40, 64]))
            && artifacts.len() <= MAX_REFERENCES
            && evidence.len() <= MAX_REFERENCES
            && (!artifacts.is_empty() || !evidence.is_empty())
            && limits.as_ref().is_none_or(valid_limits) =>
        {
            let Some((artifacts, evidence)) = normalize_success_references(
                &artifacts,
                &evidence,
                limits.as_ref(),
                confirmed_report_event_id,
            ) else {
                return fallback();
            };
            TerminalDisposition::Success {
                summary,
                candidate_sha,
                artifacts,
                evidence,
            }
        }
        OutcomeEnvelope::Failed {
            schema_version,
            operation_id: actual_operation,
            request_event_id: actual_request,
            scope_digest: actual_digest,
            code,
            reason,
            retryable,
            limits,
        } if exact_scope(
            &schema_version,
            &actual_operation,
            &actual_request,
            &actual_digest,
            operation_id,
            request_event_id,
            scope_digest,
        ) && valid_code(&code)
            && valid_text(&reason, MAX_REASON_BYTES)
            && valid_optional_limits(&limits, confirmed_report_event_id) =>
        {
            TerminalDisposition::Failed {
                code,
                message: reason,
                retryable,
            }
        }
        OutcomeEnvelope::Indeterminate {
            schema_version,
            operation_id: actual_operation,
            request_event_id: actual_request,
            scope_digest: actual_digest,
            code,
            reason,
            retryable: false,
            limits,
        } if exact_scope(
            &schema_version,
            &actual_operation,
            &actual_request,
            &actual_digest,
            operation_id,
            request_event_id,
            scope_digest,
        ) && valid_code(&code)
            && valid_text(&reason, MAX_REASON_BYTES)
            && valid_optional_limits(&limits, confirmed_report_event_id) =>
        {
            TerminalDisposition::Indeterminate {
                code,
                message: reason,
            }
        }
        _ => fallback(),
    }
}

/// Prepare bounded worker output for durable human-facing task-thread publication.
///
/// Lines containing credential-like markers are replaced rather than published.
/// Other control characters are made visible, while ordinary newlines and tabs
/// are retained. Terminal parsing never consumes or trusts this rendered report.
pub fn prepare_human_report_text(substantive_text: Option<&str>) -> Option<String> {
    let text = substantive_text.filter(|value| !value.trim().is_empty())?;
    let mut sanitized = String::with_capacity(text.len().min(MAX_HUMAN_REPORT_BYTES));
    for (index, line) in text.split('\n').enumerate() {
        if index > 0 {
            sanitized.push('\n');
        }
        if contains_secret(line) {
            sanitized.push_str("[credential-like worker output redacted]");
            continue;
        }
        for character in line.chars() {
            match character {
                '\t' => sanitized.push('\t'),
                character if character.is_control() => sanitized.push('�'),
                character => sanitized.push(character),
            }
        }
    }
    truncate_report(sanitized)
}

fn normalize_success_references(
    artifact_values: &[serde_json::Value],
    evidence_values: &[serde_json::Value],
    limits: Option<&serde_json::Value>,
    confirmed_report_event_id: Option<&str>,
) -> Option<(Vec<String>, Vec<String>)> {
    let (artifacts, artifact_report_only, artifact_bytes) =
        normalize_reference_values(artifact_values)?;
    let (mut evidence, evidence_report_only, evidence_bytes) =
        normalize_reference_values(evidence_values)?;
    let limits_bytes = limits
        .map(serde_json::to_string)
        .transpose()
        .ok()?
        .map_or(0, |value| value.len());
    if artifact_bytes
        .saturating_add(evidence_bytes)
        .saturating_add(limits_bytes)
        > MAX_TOTAL_REFERENCE_BYTES
    {
        return None;
    }
    let requires_report = artifact_report_only || evidence_report_only || limits.is_some();
    let report_reference = match confirmed_report_event_id {
        Some(event_id) => Some(report_reference(event_id)?),
        None => None,
    };
    if requires_report && report_reference.is_none() {
        return None;
    }
    if let Some(report_reference) = report_reference {
        if !evidence.contains(&report_reference) {
            if evidence.len() >= MAX_REFERENCES {
                if requires_report {
                    return None;
                }
            } else {
                evidence.push(report_reference);
            }
        }
    }
    if artifacts
        .iter()
        .chain(&evidence)
        .map(String::len)
        .sum::<usize>()
        > MAX_TOTAL_REFERENCE_BYTES
    {
        return None;
    }
    Some((artifacts, evidence))
}

fn normalize_reference_values(values: &[serde_json::Value]) -> Option<(Vec<String>, bool, usize)> {
    let mut portable = Vec::new();
    let mut report_only = false;
    let mut total_bytes = 0usize;
    for value in values {
        let serialized = serde_json::to_string(value).ok()?;
        total_bytes = total_bytes.saturating_add(serialized.len());
        if total_bytes > MAX_TOTAL_REFERENCE_BYTES || !valid_report_descriptor(value) {
            return None;
        }
        match value {
            serde_json::Value::String(reference)
                if valid_references(std::slice::from_ref(reference)) =>
            {
                portable.push(reference.clone());
            }
            serde_json::Value::String(_) | serde_json::Value::Object(_) => {
                report_only = true;
            }
            _ => return None,
        }
    }
    Some((portable, report_only, total_bytes))
}

fn valid_optional_limits(
    limits: &Option<serde_json::Value>,
    confirmed_report_event_id: Option<&str>,
) -> bool {
    match limits {
        None => confirmed_report_event_id.is_none_or(|id| report_reference(id).is_some()),
        Some(limits) => {
            valid_limits(limits)
                && confirmed_report_event_id.is_some_and(|id| report_reference(id).is_some())
        }
    }
}

fn valid_limits(value: &serde_json::Value) -> bool {
    let mut nodes = 0;
    value.as_object().is_some_and(|limits| !limits.is_empty())
        && serde_json::to_string(value)
            .is_ok_and(|serialized| serialized.len() <= MAX_TOTAL_REFERENCE_BYTES)
        && valid_report_value(value, 0, &mut nodes)
}

fn valid_report_descriptor(value: &serde_json::Value) -> bool {
    let mut nodes = 0;
    match value {
        serde_json::Value::String(text) => {
            !text.trim().is_empty() && valid_report_value(value, 0, &mut nodes)
        }
        serde_json::Value::Object(fields) => {
            !fields.is_empty() && valid_report_value(value, 0, &mut nodes)
        }
        _ => false,
    }
}

fn valid_report_value(value: &serde_json::Value, depth: usize, nodes: &mut usize) -> bool {
    if depth > MAX_REPORT_DEPTH || *nodes >= MAX_REPORT_NODES {
        return false;
    }
    *nodes += 1;
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => true,
        serde_json::Value::String(text) => text.len() <= MAX_REFERENCE_BYTES,
        serde_json::Value::Array(values) => {
            values.len() <= MAX_REPORT_ARRAY_ITEMS
                && values
                    .iter()
                    .all(|value| valid_report_value(value, depth + 1, nodes))
        }
        serde_json::Value::Object(fields) => {
            fields.len() <= MAX_REPORT_OBJECT_FIELDS
                && fields.keys().all(|key| {
                    !key.is_empty()
                        && key.len() <= MAX_REPORT_KEY_BYTES
                        && !key.chars().any(char::is_control)
                })
                && fields
                    .values()
                    .all(|value| valid_report_value(value, depth + 1, nodes))
        }
    }
}

fn report_reference(event_id: &str) -> Option<String> {
    valid_lower_hex(event_id, &[64]).then(|| format!("buzz:event:{event_id}"))
}

fn truncate_report(mut report: String) -> Option<String> {
    const TRUNCATED: &str = "\n[worker output truncated]";
    if report.len() > MAX_HUMAN_REPORT_BYTES {
        let target = MAX_HUMAN_REPORT_BYTES.saturating_sub(TRUNCATED.len());
        let mut boundary = target;
        while boundary > 0 && !report.is_char_boundary(boundary) {
            boundary -= 1;
        }
        report.truncate(boundary);
        report.push_str(TRUNCATED);
    }
    (!report.trim().is_empty()).then_some(report)
}

fn exact_scope(
    schema_version: &str,
    actual_operation: &str,
    actual_request: &str,
    actual_digest: &str,
    operation_id: &str,
    request_event_id: &str,
    scope_digest: &str,
) -> bool {
    schema_version == OUTCOME_SCHEMA
        && actual_operation == operation_id
        && actual_request == request_event_id
        && actual_digest == scope_digest
}

fn valid_text(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= max
        && !value.chars().any(char::is_control)
        && !contains_secret(value)
}

fn valid_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn valid_references(values: &[String]) -> bool {
    values.iter().all(|value| {
        if value.is_empty()
            || value.len() > MAX_REFERENCE_BYTES
            || value.chars().any(char::is_control)
            || value.starts_with(['/', '~'])
            || value.contains('\\')
            || value.split('/').any(|segment| segment == "..")
            || value
                .split('/')
                .any(|segment| segment.eq_ignore_ascii_case(".git"))
            || value.to_ascii_lowercase().starts_with("file:")
            || contains_secret(value)
        {
            return false;
        }
        if let Some(digest) = value.strip_prefix("git:") {
            return valid_lower_hex(digest, &[40, 64]);
        }
        if let Some(contract) = value.strip_prefix("contract:") {
            return !contract.is_empty()
                && contract.len() <= 128
                && !contract.split('/').any(str::is_empty)
                && contract.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/')
                });
        }
        if let Some(event_id) = value.strip_prefix("buzz:event:") {
            return valid_lower_hex(event_id, &[64]);
        }
        let Ok(url) = url::Url::parse(value) else {
            return false;
        };
        url.scheme() == "https"
            && url.host_str() == Some("github.com")
            && url.username().is_empty()
            && url.password().is_none()
            && url.port().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
    })
}

fn contains_secret(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    [
        "token=",
        "password=",
        "secret=",
        "authorization:",
        "bearer ",
        "begin private key",
        "github_token",
        "api_key",
        "github_pat_",
        "ghp_",
        "sk-",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

fn valid_lower_hex(value: &str, lengths: &[usize]) -> bool {
    lengths.contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

struct NoDuplicateValue(serde_json::Value);

pub(super) fn parse_unique_json(text: &str) -> Result<serde_json::Value, serde_json::Error> {
    serde_json::from_str::<NoDuplicateValue>(text).map(|value| value.0)
}

impl<'de> Deserialize<'de> for NoDuplicateValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct UniqueVisitor;

        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = serde_json::Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("JSON without duplicate object keys")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(value.into())
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(value.into())
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(value.into())
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
                Ok(value.into())
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(value.into())
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(serde_json::Value::Null)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(serde_json::Value::Null)
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<NoDuplicateValue>()? {
                    values.push(value.0);
                }
                Ok(serde_json::Value::Array(values))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut keys = HashSet::new();
                let mut values = serde_json::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !keys.insert(key.clone()) {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate JSON key: {key}"
                        )));
                    }
                    values.insert(key, map.next_value::<NoDuplicateValue>()?.0);
                }
                Ok(serde_json::Value::Object(values))
            }
        }

        deserializer.deserialize_any(UniqueVisitor).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn success(operation: &str, request: &str) -> String {
        serde_json::json!({
            "schema_version": OUTCOME_SCHEMA,
            "operation_id": operation,
            "request_event_id": request,
            "scope_digest": "c".repeat(64),
            "outcome": "success",
            "summary": "Implemented and verified",
            "candidate_sha": "a".repeat(40),
            "artifacts": [],
            "evidence": ["git:".to_string() + &"a".repeat(40)]
        })
        .to_string()
    }

    #[test]
    fn only_one_exact_scope_bound_envelope_can_succeed() {
        let operation = "31dbb246-bc79-4ddc-aab0-2773f05b5cb2";
        let request = "b".repeat(64);
        assert!(matches!(
            parse_terminal_outcome(
                Some(success(operation, &request)),
                operation,
                &request,
                &"c".repeat(64)
            ),
            TerminalDisposition::Success { summary, .. }
                if summary == "Implemented and verified"
        ));
        for invalid in [
            "ordinary agent response".into(),
            format!(
                "{}\n{}",
                success(operation, &request),
                success(operation, &request)
            ),
            success("31dbb246-bc79-4ddc-aab0-2773f05b5cb3", &request),
            format!("buzz jobs complete\n{}", success(operation, &request)),
        ] {
            assert!(matches!(
                parse_terminal_outcome(Some(invalid), operation, &request, &"c".repeat(64)),
                TerminalDisposition::Indeterminate { .. }
            ));
        }
    }

    #[test]
    fn structured_descriptors_require_and_normalize_to_confirmed_report() {
        let operation = "31dbb246-bc79-4ddc-aab0-2773f05b5cb2";
        let request = "b".repeat(64);
        let report_id = "d".repeat(64);
        let value = serde_json::json!({
            "schema_version": OUTCOME_SCHEMA,
            "operation_id": operation,
            "request_event_id": request,
            "scope_digest": "c".repeat(64),
            "outcome": "success",
            "summary": "Implemented and verified",
            "artifacts": [{
                "type": "file",
                "path": "/tmp/result.txt",
                "bytes": 28,
                "sha256": "a".repeat(64),
                "content": "line one\nline two\n"
            }],
            "evidence": [
                {"check": "shell", "commands": ["printf test", "wc -c"]},
                {"check": "tools", "calls": [{"tool": "search", "status": "ok", "result": "one match"}]}
            ]
        });

        assert!(matches!(
            parse_terminal_outcome(
                Some(value.to_string()),
                operation,
                &request,
                &"c".repeat(64)
            ),
            TerminalDisposition::Indeterminate { ref code, .. }
                if code == "invalid_terminal_envelope"
        ));
        assert_eq!(
            parse_terminal_outcome_with_report(
                Some(value.to_string()),
                operation,
                &request,
                &"c".repeat(64),
                Some(&report_id),
            ),
            TerminalDisposition::Success {
                summary: "Implemented and verified".into(),
                candidate_sha: None,
                artifacts: Vec::new(),
                evidence: vec![format!("buzz:event:{report_id}")],
            }
        );
    }

    #[test]
    fn report_normalization_preserves_only_portable_lifecycle_references() {
        let operation = "31dbb246-bc79-4ddc-aab0-2773f05b5cb2";
        let request = "b".repeat(64);
        let report_id = "d".repeat(64);
        let git_ref = format!("git:{}", "a".repeat(40));
        let github_ref = "https://github.com/mysteropodes/nemo/issues/1";
        let value = serde_json::json!({
            "schema_version": OUTCOME_SCHEMA,
            "operation_id": operation,
            "request_event_id": request,
            "scope_digest": "c".repeat(64),
            "outcome": "success",
            "summary": "Implemented and verified",
            "artifacts": [git_ref, "relative/output.txt"],
            "evidence": [github_ref, {"check": "cargo test", "result": "passed"}]
        });

        assert_eq!(
            parse_terminal_outcome_with_report(
                Some(value.to_string()),
                operation,
                &request,
                &"c".repeat(64),
                Some(&report_id),
            ),
            TerminalDisposition::Success {
                summary: "Implemented and verified".into(),
                candidate_sha: None,
                artifacts: vec![format!("git:{}", "a".repeat(40))],
                evidence: vec![github_ref.into(), format!("buzz:event:{report_id}")],
            }
        );
    }

    #[test]
    fn optional_limits_are_bounded_report_data() {
        let operation = "31dbb246-bc79-4ddc-aab0-2773f05b5cb2";
        let request = "b".repeat(64);
        let report_id = "d".repeat(64);
        let mut value: serde_json::Value =
            serde_json::from_str(&success(operation, &request)).expect("base outcome");
        value["limits"] = serde_json::json!({
            "not_run": ["live mutation"],
            "reason": "outside this bounded validation"
        });

        assert!(matches!(
            parse_terminal_outcome(
                Some(value.to_string()),
                operation,
                &request,
                &"c".repeat(64)
            ),
            TerminalDisposition::Indeterminate { ref code, .. }
                if code == "invalid_terminal_envelope"
        ));
        assert!(matches!(
            parse_terminal_outcome_with_report(
                Some(value.to_string()),
                operation,
                &request,
                &"c".repeat(64),
                Some(&report_id),
            ),
            TerminalDisposition::Success { ref evidence, .. }
                if evidence.contains(&format!("buzz:event:{report_id}"))
        ));
    }

    #[test]
    fn malformed_report_descriptors_and_report_ids_fail_closed() {
        let operation = "31dbb246-bc79-4ddc-aab0-2773f05b5cb2";
        let request = "b".repeat(64);
        let mut scalar: serde_json::Value =
            serde_json::from_str(&success(operation, &request)).expect("base outcome");
        scalar["evidence"] = serde_json::json!([42]);
        let mut oversized: serde_json::Value =
            serde_json::from_str(&success(operation, &request)).expect("base outcome");
        oversized["evidence"] =
            serde_json::json!([{"result": "x".repeat(MAX_REFERENCE_BYTES + 1)}]);
        let mut too_deep: serde_json::Value =
            serde_json::from_str(&success(operation, &request)).expect("base outcome");
        let mut nested = serde_json::json!("leaf");
        for _ in 0..=MAX_REPORT_DEPTH {
            nested = serde_json::json!({"nested": nested});
        }
        too_deep["evidence"] = serde_json::json!([{"check": nested}]);

        for (value, report_id) in [
            (scalar, "d".repeat(64)),
            (oversized, "d".repeat(64)),
            (too_deep, "d".repeat(64)),
            (
                serde_json::from_str(&success(operation, &request)).expect("base outcome"),
                "not-an-event-id".into(),
            ),
        ] {
            assert!(matches!(
                parse_terminal_outcome_with_report(
                    Some(value.to_string()),
                    operation,
                    &request,
                    &"c".repeat(64),
                    Some(&report_id),
                ),
                TerminalDisposition::Indeterminate { ref code, .. }
                    if code == "invalid_terminal_envelope"
            ));
        }
    }

    #[test]
    fn duplicate_keys_inside_structured_descriptors_fail_closed() {
        let operation = "31dbb246-bc79-4ddc-aab0-2773f05b5cb2";
        let request = "b".repeat(64);
        let digest = "c".repeat(64);
        let report_id = "d".repeat(64);
        let terminal = format!(
            r#"{{"schema_version":"{OUTCOME_SCHEMA}","operation_id":"{operation}","request_event_id":"{request}","scope_digest":"{digest}","outcome":"success","summary":"done","artifacts":[],"evidence":[{{"check":"one","check":"two"}}]}}"#
        );

        assert!(matches!(
            parse_terminal_outcome_with_report(
                Some(terminal),
                operation,
                &request,
                &digest,
                Some(&report_id),
            ),
            TerminalDisposition::Indeterminate { ref code, .. }
                if code == "invalid_terminal_envelope"
        ));
    }

    #[test]
    fn human_report_is_bounded_and_redacts_credential_like_lines() {
        let text = format!(
            "useful\tresult\nAuthorization: Bearer abc\n{}",
            "é".repeat(MAX_HUMAN_REPORT_BYTES)
        );
        let report = prepare_human_report_text(Some(&text)).expect("report");
        assert!(report.starts_with("useful\tresult\n[credential-like worker output redacted]\n"));
        assert!(!report.contains("Bearer abc"));
        assert!(report.ends_with("\n[worker output truncated]"));
        assert!(report.len() <= MAX_HUMAN_REPORT_BYTES);
        assert!(std::str::from_utf8(report.as_bytes()).is_ok());
    }

    #[test]
    fn malformed_and_retryable_indeterminate_fail_closed() {
        let operation = "31dbb246-bc79-4ddc-aab0-2773f05b5cb2";
        let request = "b".repeat(64);
        let value = serde_json::json!({
            "schema_version": OUTCOME_SCHEMA,
            "operation_id": operation,
            "request_event_id": request,
            "scope_digest": "c".repeat(64),
            "outcome": "indeterminate",
            "code": "unknown_state",
            "reason": "process stopped",
            "retryable": true
        });
        assert!(matches!(
            parse_terminal_outcome(Some(value.to_string()), operation, &request, &"c".repeat(64)),
            TerminalDisposition::Indeterminate { code, .. } if code == "invalid_terminal_envelope"
        ));
    }

    #[test]
    fn duplicate_keys_and_wrong_scope_digest_fail_closed() {
        let operation = "31dbb246-bc79-4ddc-aab0-2773f05b5cb2";
        let request = "b".repeat(64);
        let duplicate = success(operation, &request).replacen(
            "\"outcome\":\"success\"",
            "\"outcome\":\"success\",\"outcome\":\"failed\"",
            1,
        );
        for invalid in [
            duplicate,
            success(operation, &request).replace(&"c".repeat(64), &"d".repeat(64)),
        ] {
            assert!(matches!(
                parse_terminal_outcome(Some(invalid), operation, &request, &"c".repeat(64)),
                TerminalDisposition::Indeterminate { code, .. }
                    if code == "invalid_terminal_envelope"
            ));
        }
    }

    #[test]
    fn unsafe_or_oversized_success_evidence_and_bad_shas_fail_closed() {
        let operation = "31dbb246-bc79-4ddc-aab0-2773f05b5cb2";
        let request = "b".repeat(64);
        let base: serde_json::Value =
            serde_json::from_str(&success(operation, &request)).expect("base outcome");
        let mut invalid = Vec::new();
        for reference in [
            "https://example.com/result",
            "https://github.com/mysteropodes/nemo?token=x",
            "https://github.com/mysteropodes/nemo#secret",
            "/Users/dev/secret.txt",
            "C:\\secret.txt",
            "src/.git/config",
            "token=secret",
            "bad\nref",
        ] {
            let mut value = base.clone();
            value["evidence"] = serde_json::json!([reference]);
            invalid.push(value.to_string());
        }
        for sha in ["A".repeat(40), "a".repeat(39), "z".repeat(40)] {
            let mut value = base.clone();
            value["candidate_sha"] = sha.into();
            invalid.push(value.to_string());
        }
        let mut oversized_item = base.clone();
        oversized_item["evidence"] =
            serde_json::json!([format!("contract:{}", "a".repeat(MAX_REFERENCE_BYTES))]);
        invalid.push(oversized_item.to_string());
        let mut oversized_total = base;
        oversized_total["evidence"] = serde_json::json!((0..9)
            .map(|index| format!(
                "https://github.com/mysteropodes/nemo/{}-{index}",
                "a".repeat(8_000)
            ))
            .collect::<Vec<_>>());
        invalid.push(oversized_total.to_string());

        let mut secret_summary: serde_json::Value =
            serde_json::from_str(&success(operation, &request)).expect("base outcome");
        secret_summary["summary"] = "authorization: Bearer abcdefghijkl".into();
        invalid.push(secret_summary.to_string());
        let mut oversized_summary: serde_json::Value =
            serde_json::from_str(&success(operation, &request)).expect("base outcome");
        oversized_summary["summary"] = "x".repeat(MAX_SUMMARY_BYTES + 1).into();
        invalid.push(oversized_summary.to_string());

        for text in invalid {
            assert!(matches!(
                parse_terminal_outcome(Some(text), operation, &request, &"c".repeat(64)),
                TerminalDisposition::Indeterminate { code, .. }
                    if code == "invalid_terminal_envelope"
            ));
        }
    }

    #[test]
    fn unknown_fields_and_secret_bearing_failure_reasons_fail_closed() {
        let operation = "31dbb246-bc79-4ddc-aab0-2773f05b5cb2";
        let request = "b".repeat(64);
        let mut unknown: serde_json::Value =
            serde_json::from_str(&success(operation, &request)).expect("base outcome");
        unknown["unexpected"] = true.into();
        let failed = serde_json::json!({
            "schema_version": OUTCOME_SCHEMA,
            "operation_id": operation,
            "request_event_id": request,
            "scope_digest": "c".repeat(64),
            "outcome": "failed",
            "code": "tool_error",
            "reason": "authorization: Bearer secret",
            "retryable": false
        });
        for invalid in [unknown.to_string(), failed.to_string()] {
            assert!(matches!(
                parse_terminal_outcome(Some(invalid), operation, &request, &"c".repeat(64)),
                TerminalDisposition::Indeterminate { code, .. }
                    if code == "invalid_terminal_envelope"
            ));
        }
    }
}

#[cfg(test)]
mod fenced_outcome_regression {
    use super::*;
    #[test]
    fn fenced_r06_report_finalizes_with_confirmed_evidence_and_exact_scope() {
        let operation = "603b97c8-d3dc-4663-91d9-bf6a4cb37b65";
        let request = "c09bbd869eaa7a46ead554021a23f2c9897f2745b585b242bbee2f931206b87c";
        let digest = "f082b8c67d0e9d3d943ab368579f4071609f1492a45d22748b0763c739e1a777";
        let report = "d".repeat(64);
        let value = serde_json::json!({"schema_version":OUTCOME_SCHEMA,"operation_id":operation,"request_event_id":request,"scope_digest":digest,"outcome":"success","summary":"R06 source review complete; local changes require reconciliation.","artifacts":[{"note":"Source inspection","file_refs":["src/native.rs"]},format!("git:{}","a".repeat(40))],"evidence":[format!("buzz:event:{}","b".repeat(64)),{"searches":["native isolation"]}]});
        let text = format!("I inspected the source. An accidental local reset needs reconciliation.\n\n```json\n{value}\n```");
        assert!(matches!(
            parse_terminal_outcome_with_report(
                Some(text.clone()),
                operation,
                request,
                digest,
                Some(&report)
            ),
            TerminalDisposition::Success { .. }
        ));
        assert!(matches!(
            parse_terminal_outcome_with_report(
                Some(text.clone()),
                operation,
                request,
                "wrong",
                Some(&report)
            ),
            TerminalDisposition::Indeterminate { .. }
        ));
        assert!(matches!(
            parse_terminal_outcome(Some(text.clone()), operation, request, digest),
            TerminalDisposition::Indeterminate { .. }
        ));
        let human =
            super::super::human_report::HumanJobReport::from_turn_output(Some(&text), Some(&text))
                .unwrap();
        assert!(human.content().contains("Source inspection"));
        assert!(human.content().contains("accidental local reset"));
        assert!(!human.content().contains("schema_version"));
        assert!(!human.content().contains("```"));
    }
}
