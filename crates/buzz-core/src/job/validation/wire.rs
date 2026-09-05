use serde::de::DeserializeOwned;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::job::model::JobValidationError;
use crate::kind::{
    KIND_JOB_ACCEPTED, KIND_JOB_CANCEL, KIND_JOB_ERROR, KIND_JOB_PROGRESS, KIND_JOB_REQUEST,
    KIND_JOB_RESULT,
};

pub(in crate::job) struct UniqueValue(pub(in crate::job) Value);

pub(crate) fn parse_strict_json<T: DeserializeOwned>(raw: &str) -> Result<T, String> {
    let value = serde_json::from_str::<UniqueValue>(raw)
        .map_err(|error| format!("JSON must not contain duplicate keys: {error}"))?
        .0;
    if contains_null(&value) {
        return Err("JSON must omit absent optional fields instead of using null".into());
    }
    serde_json::from_value(value).map_err(|error| format!("invalid JSON shape: {error}"))
}

fn contains_null(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Array(values) => values.iter().any(contains_null),
        Value::Object(values) => values.values().any(contains_null),
        _ => false,
    }
}

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueValueVisitor)
    }
}

struct UniqueValueVisitor;

impl<'de> Visitor<'de> for UniqueValueVisitor {
    type Value = UniqueValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(value.into())))
    }

    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .map(UniqueValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<UniqueValue>()? {
            values.push(value.0);
        }
        Ok(UniqueValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = serde_json::Map::new();
        while let Some((key, value)) = map.next_entry::<String, UniqueValue>()? {
            if values.insert(key.clone(), value.0).is_some() {
                return Err(serde::de::Error::custom(format!(
                    "duplicate JSON key: {key}"
                )));
            }
        }
        Ok(UniqueValue(Value::Object(values)))
    }
}

pub(in crate::job) fn validate_wire_keys(kind: u32, raw: &Value) -> Result<(), JobValidationError> {
    const COMMON: &[&str] = &[
        "schema_version",
        "operation_id",
        "idempotency_key",
        "coordinator_epoch",
        "project",
        "repository",
        "sender_pubkey",
        "recipient_pubkey",
        "sponsor",
        "expires_at",
    ];
    let mut required = COMMON.to_vec();
    let mut optional = vec!["conversation"];
    match kind {
        KIND_JOB_REQUEST => {
            required.extend(["capability", "summary", "acceptance"]);
            optional.extend(["title", "origin", "supersedes_event_id"]);
        }
        KIND_JOB_ACCEPTED => {
            required.extend(["request_event_id", "claim"]);
            optional.push("prior_event_id");
        }
        KIND_JOB_PROGRESS => {
            required.extend(["request_event_id", "status", "message", "evidence"]);
            optional.push("prior_event_id");
        }
        KIND_JOB_RESULT => {
            required.extend(["request_event_id", "outcome", "artifacts", "evidence"]);
            optional.extend(["prior_event_id", "summary", "candidate_sha", "capabilities"]);
        }
        KIND_JOB_CANCEL => {
            required.extend(["request_event_id", "action", "reason"]);
            optional.extend(["prior_event_id", "handoff_to"]);
        }
        KIND_JOB_ERROR => {
            required.extend([
                "request_event_id",
                "outcome",
                "code",
                "message",
                "retryable",
            ]);
            optional.push("prior_event_id");
        }
        _ => return Ok(()),
    }
    validate_object_keys("job content", raw, &required, &optional)?;
    validate_object_keys(
        "project",
        &raw["project"],
        &["address", "home_channel"],
        &[],
    )?;
    if raw.get("conversation").is_some() {
        validate_object_keys(
            "conversation",
            &raw["conversation"],
            &["channel_id", "thread_root_id"],
            &[],
        )?;
    }
    validate_object_keys(
        "repository",
        &raw["repository"],
        &[
            "canonical",
            "base_sha",
            "branch",
            "worktree_id",
            "paths",
            "contracts",
        ],
        &["github_issue", "github_pr", "github_run"],
    )?;
    if raw.get("origin").is_some() {
        validate_object_keys(
            "origin",
            &raw["origin"],
            &["channel_id"],
            &[
                "thread_root_id",
                "session_channel_id",
                "session_thread_root_id",
            ],
        )?;
    }
    validate_object_keys("sponsor", &raw["sponsor"], &["pubkey", "github_login"], &[])?;
    if kind == KIND_JOB_ACCEPTED {
        validate_object_keys(
            "claim",
            &raw["claim"],
            &["status", "scope_digest"],
            &["reason"],
        )?;
    }
    Ok(())
}

fn validate_object_keys(
    name: &str,
    value: &Value,
    required: &[&str],
    optional: &[&str],
) -> Result<(), JobValidationError> {
    let object = value
        .as_object()
        .ok_or_else(|| JobValidationError::new(format!("{name} must be a JSON object")))?;
    for key in required {
        if !object.contains_key(*key) {
            return Err(JobValidationError::new(format!(
                "{name} is missing required field {key}"
            )));
        }
    }
    for key in object.keys() {
        if !required.contains(&key.as_str()) && !optional.contains(&key.as_str()) {
            return Err(JobValidationError::new(format!(
                "{name} contains unknown field {key}"
            )));
        }
    }
    Ok(())
}
