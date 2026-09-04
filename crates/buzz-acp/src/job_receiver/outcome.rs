use std::collections::HashSet;

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

const OUTCOME_SCHEMA: &str = "buzz.job-outcome.v1";
const MAX_SUMMARY_BYTES: usize = 8 * 1024;
const MAX_REASON_BYTES: usize = 8 * 1024;
const MAX_REFERENCES: usize = 256;
const MAX_REFERENCE_BYTES: usize = 8 * 1024;
const MAX_TOTAL_REFERENCE_BYTES: usize = 64 * 1024;

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
        artifacts: Vec<String>,
        evidence: Vec<String>,
    },
    Failed {
        schema_version: String,
        operation_id: String,
        request_event_id: String,
        scope_digest: String,
        code: String,
        reason: String,
        retryable: bool,
    },
    Indeterminate {
        schema_version: String,
        operation_id: String,
        request_event_id: String,
        scope_digest: String,
        code: String,
        reason: String,
        retryable: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalDisposition {
    Success {
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

/// Parse one exact, scope-bound terminal envelope from the assistant's final text.
pub fn parse_terminal_outcome(
    text: Option<String>,
    operation_id: &str,
    request_event_id: &str,
    scope_digest: &str,
) -> TerminalDisposition {
    let fallback = || TerminalDisposition::Indeterminate {
        code: "invalid_terminal_envelope".into(),
        message: "Worker did not return one valid scope-bound buzz.job-outcome.v1 envelope".into(),
    };
    let Some(text) = text else { return fallback() };
    let Ok(unique) = serde_json::from_str::<NoDuplicateValue>(text.trim()) else {
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
            && valid_references(&artifacts)
            && valid_references(&evidence)
            && artifacts
                .iter()
                .chain(&evidence)
                .map(String::len)
                .sum::<usize>()
                <= MAX_TOTAL_REFERENCE_BYTES
            && (!artifacts.is_empty() || !evidence.is_empty()) =>
        {
            // The v1 relay result schema has no summary field. Requiring and
            // validating it still proves the worker made an explicit terminal
            // assertion; only inert protocol evidence is published.
            TerminalDisposition::Success {
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
        } if exact_scope(
            &schema_version,
            &actual_operation,
            &actual_request,
            &actual_digest,
            operation_id,
            request_event_id,
            scope_digest,
        ) && valid_code(&code)
            && valid_text(&reason, MAX_REASON_BYTES) =>
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
        } if exact_scope(
            &schema_version,
            &actual_operation,
            &actual_request,
            &actual_digest,
            operation_id,
            request_event_id,
            scope_digest,
        ) && valid_code(&code)
            && valid_text(&reason, MAX_REASON_BYTES) =>
        {
            TerminalDisposition::Indeterminate {
                code,
                message: reason,
            }
        }
        _ => fallback(),
    }
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
            TerminalDisposition::Success { .. }
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
