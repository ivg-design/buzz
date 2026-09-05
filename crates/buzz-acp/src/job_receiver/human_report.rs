use super::outcome::{prepare_human_report_text, terminal_json_text};

/// Bounded worker-authored text suitable for the durable task conversation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HumanJobReport {
    content: String,
}

impl HumanJobReport {
    /// Preserve prose and render terminal descriptors as readable sections.
    /// Protocol coordinates stay in Activity; the candidate is only a text
    /// fallback for older adapters without separate substantive capture.
    pub fn from_turn_output(
        terminal_candidate: Option<&str>,
        substantive_text: Option<&str>,
    ) -> Option<Self> {
        let candidate = terminal_candidate
            .map(str::trim)
            .filter(|text| !text.is_empty());
        let substantive = substantive_text
            .map(str::trim)
            .filter(|text| !text.is_empty());
        let report_text = match candidate {
            Some(candidate) => {
                let prior = substantive
                    .and_then(|text| text.strip_suffix(candidate))
                    .map(str::trim_end)
                    .filter(|text| !text.is_empty());
                match render_terminal_candidate(candidate) {
                    Some(rendered) => [
                        prior,
                        candidate.find("```").filter(|index| *index > 0)
                            .map(|index| candidate[..index].trim())
                            .filter(|prefix| !prefix.is_empty()),
                        Some(rendered.as_str()),
                    ]
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>()
                        .join("\n\n"),
                    None if looks_like_structured_payload(candidate) => [
                        prior,
                        Some(
                            "Worker returned structured output that could not be displayed. See Activity for technical details.",
                        ),
                    ]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join("\n\n"),
                    None => substantive.unwrap_or(candidate).to_owned(),
                }
            }
            None => substantive?.to_owned(),
        };
        prepare_human_report_text(Some(&report_text)).map(|content| Self { content })
    }

    pub fn content(&self) -> &str {
        &self.content
    }
}

fn looks_like_structured_payload(text: &str) -> bool {
    matches!(
        terminal_json_text(text).chars().next(),
        Some('{') | Some('[')
    ) || text.contains("buzz.job-outcome.v1")
}

fn render_terminal_candidate(candidate: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(terminal_json_text(candidate)).ok()?;
    let object = value.as_object()?;
    let mut sections = Vec::new();
    if let Some(summary) = object
        .get("summary")
        .or_else(|| object.get("reason"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
    {
        sections.push(summary.to_owned());
    }
    let mut nodes = 0usize;
    for (key, label) in [
        ("artifacts", "Artifacts"),
        ("evidence", "Evidence"),
        ("limits", "Limits"),
    ] {
        if let Some(value) = object.get(key).filter(|value| !value.is_null()) {
            let rendered = render_report_value(value, 0, &mut nodes);
            if !rendered.is_empty() {
                sections.push(format!("{label}:\n{rendered}"));
            }
        }
    }
    (!sections.is_empty()).then(|| sections.join("\n\n"))
}

fn render_report_value(value: &serde_json::Value, depth: usize, nodes: &mut usize) -> String {
    *nodes = nodes.saturating_add(1);
    if depth > 8 || *nodes > 4_096 {
        return "- …".into();
    }
    match value {
        serde_json::Value::Array(values) => values
            .iter()
            .map(|value| {
                let rendered = render_report_value(value, depth + 1, nodes);
                if rendered.starts_with("- ") && rendered.contains('\n') {
                    format!("-\n{}", indent_report_lines(&rendered))
                } else {
                    format!("- {}", rendered.strip_prefix("- ").unwrap_or(&rendered))
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                let label = key.replace('_', " ");
                let rendered = render_report_value(value, depth + 1, nodes);
                if matches!(
                    value,
                    serde_json::Value::Array(_) | serde_json::Value::Object(_)
                ) {
                    format!("- {label}:\n{}", indent_report_lines(&rendered))
                } else {
                    format!(
                        "- {label}: {}",
                        rendered.strip_prefix("- ").unwrap_or(&rendered)
                    )
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Null => "none".into(),
    }
}

fn indent_report_lines(text: &str) -> String {
    text.lines()
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_terminal_details_without_protocol_coordinates() {
        let envelope = format!(
            r#"{{"schema_version":"buzz.job-outcome.v1","operation_id":"{}","request_event_id":"{}","scope_digest":"{}","outcome":"success","summary":"Implemented the fix.","artifacts":[{{"path":"src/main.rs","status":"updated"}}],"evidence":["test:focused-pass"],"limits":{{"remaining":"none"}}}}"#,
            "31dbb246-bc79-4ddc-aab0-2773f05b5cb2",
            "f".repeat(64),
            "a".repeat(64),
        );
        let report = HumanJobReport::from_turn_output(
            Some(&envelope),
            Some(&format!("Checked the implementation.\n\n{envelope}")),
        )
        .expect("substantive report");
        assert!(report.content().starts_with("Checked the implementation."));
        assert!(report.content().contains("Implemented the fix."));
        assert!(report.content().contains("Artifacts:"));
        assert!(report.content().contains("path: src/main.rs"));
        assert!(report.content().contains("Evidence:\n- test:focused-pass"));
        assert!(report.content().contains("Limits:"));
        assert!(!report.content().contains("schema_version"));
        assert!(!report.content().contains("request_event_id"));

        let malformed = HumanJobReport::from_turn_output(
            Some("I finished, but missed the envelope."),
            Some("I finished, but missed the envelope."),
        )
        .expect("malformed final remains visible");
        assert_eq!(malformed.content(), "I finished, but missed the envelope.");

        let malformed_json = HumanJobReport::from_turn_output(
            Some(r#"{"outcome":"success","artifacts":[not-json]}"#),
            Some(
                r#"Useful work note.

{"outcome":"success","artifacts":[not-json]}"#,
            ),
        )
        .expect("structured failure note");
        assert!(malformed_json.content().starts_with("Useful work note."));
        assert!(malformed_json.content().contains("See Activity"));
        assert!(!malformed_json.content().contains("not-json"));
    }
}
