//! Bounded capture of logical top-level assistant messages for one ACP turn.

const MAX_CAPTURED_TURN_BYTES: usize = 128 * 1024;
const MAX_CAPTURED_MESSAGES: usize = 256;
const MAX_MESSAGE_ID_BYTES: usize = 256;
const MESSAGE_SEPARATOR: &str = "\n\n";

#[derive(Debug, Default)]
pub(super) struct TurnTextCapture {
    messages: Vec<CapturedMessage>,
    retained_bytes: usize,
    overflowed: bool,
}

#[derive(Debug)]
struct CapturedMessage {
    message_id: Option<String>,
    text: String,
}

/// Assistant output retained for job finalization and useful transcript publication.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CapturedTurnOutput {
    /// The last complete logical top-level assistant message.
    pub(crate) terminal_candidate: Option<String>,
    /// Every retained top-level assistant message in protocol order.
    pub(crate) substantive_text: Option<String>,
}

impl TurnTextCapture {
    pub(super) fn clear(&mut self) {
        self.messages.clear();
        self.retained_bytes = 0;
        self.overflowed = false;
    }

    pub(super) fn push(&mut self, message_id: Option<&str>, text: &str) {
        if self.overflowed || text.is_empty() {
            return;
        }

        let message_id = normalized_message_id(message_id);
        let continues_last = self
            .messages
            .last()
            .is_some_and(|message| message.message_id.as_deref() == message_id);
        let boundary_bytes = if continues_last {
            0
        } else {
            message_id.map_or(0, str::len)
                + usize::from(!self.messages.is_empty()) * MESSAGE_SEPARATOR.len()
        };
        if !continues_last && self.messages.len() >= MAX_CAPTURED_MESSAGES {
            self.overflowed = true;
            return;
        }

        let available = MAX_CAPTURED_TURN_BYTES
            .saturating_sub(self.retained_bytes)
            .saturating_sub(boundary_bytes);
        let retained_text = utf8_prefix(text, available);

        if continues_last {
            if let Some(last) = self.messages.last_mut() {
                last.text.push_str(retained_text);
            }
        } else if !retained_text.is_empty() {
            self.messages.push(CapturedMessage {
                message_id: message_id.map(ToOwned::to_owned),
                text: retained_text.to_owned(),
            });
        }
        if !retained_text.is_empty() {
            self.retained_bytes = self
                .retained_bytes
                .saturating_add(boundary_bytes)
                .saturating_add(retained_text.len());
        }
        self.overflowed = retained_text.len() < text.len();
    }

    pub(super) fn take(&mut self) -> CapturedTurnOutput {
        let capture = std::mem::take(self);
        let terminal_candidate = (!capture.overflowed)
            .then(|| capture.messages.last().map(|message| message.text.clone()))
            .flatten();
        let substantive_text = (!capture.messages.is_empty()).then(|| {
            capture
                .messages
                .into_iter()
                .map(|message| message.text)
                .collect::<Vec<_>>()
                .join(MESSAGE_SEPARATOR)
        });
        CapturedTurnOutput {
            terminal_candidate,
            substantive_text,
        }
    }
}

fn normalized_message_id(message_id: Option<&str>) -> Option<&str> {
    message_id.filter(|value| {
        !value.is_empty()
            && value.len() <= MAX_MESSAGE_ID_BYTES
            && !value.chars().any(char::is_control)
    })
}

fn utf8_prefix(value: &str, max_bytes: usize) -> &str {
    let mut boundary = value.len().min(max_bytes);
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &value[..boundary]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_message_is_terminal_and_all_messages_remain_substantive() {
        let mut capture = TurnTextCapture::default();
        capture.push(Some("progress-1"), "Progress ");
        capture.push(Some("progress-1"), "update");
        capture.push(Some("final-2"), "{\"outcome\":\"success\"}");

        assert_eq!(
            capture.take(),
            CapturedTurnOutput {
                terminal_candidate: Some("{\"outcome\":\"success\"}".into()),
                substantive_text: Some("Progress update\n\n{\"outcome\":\"success\"}".into()),
            }
        );
    }

    #[test]
    fn idless_legacy_chunks_stay_one_message() {
        let mut capture = TurnTextCapture::default();
        capture.push(None, "progress");
        capture.push(None, "{\"outcome\":\"success\"}");

        let output = capture.take();
        assert_eq!(
            output.terminal_candidate.as_deref(),
            Some("progress{\"outcome\":\"success\"}")
        );
        assert_eq!(output.substantive_text, output.terminal_candidate);
    }

    #[test]
    fn overflow_discards_terminal_authority_but_keeps_bounded_substantive_capture() {
        let mut capture = TurnTextCapture::default();
        capture.push(Some("oversized"), &"x".repeat(MAX_CAPTURED_TURN_BYTES + 1));

        let output = capture.take();
        assert!(output.terminal_candidate.is_none());
        let substantive = output.substantive_text.expect("bounded report text");
        assert!(substantive.len() <= MAX_CAPTURED_TURN_BYTES);
        assert!(!substantive.is_empty());
    }
}
