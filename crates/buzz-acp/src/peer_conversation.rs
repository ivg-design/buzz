//! Addressed peer conversations share chat threads without creating another job.

use nostr::Event;

use crate::queue::parse_thread_tags;
use crate::scope::SessionScope;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerMessage {
    Question,
    Reply,
}

/// Recognize the exact typed peer marker. Verification of the enrolled author
/// is performed separately by the harness before admitting a question.
pub(crate) fn peer_message(event: &Event, recipient: &str) -> Option<PeerMessage> {
    if event.kind.as_u16() != 9 || !addressed_to(event, recipient) {
        return None;
    }
    let mut markers = event.tags.iter().filter(|tag| {
        tag.as_slice()
            .first()
            .is_some_and(|name| name == "buzz-peer")
    });
    let marker = markers.next()?.as_slice();
    if markers.next().is_some() || marker.len() != 3 {
        return None;
    }
    let id = uuid::Uuid::parse_str(&marker[2]).ok()?;
    if id.is_nil() || id.to_string() != marker[2] {
        return None;
    }
    match marker[1].as_str() {
        "question" => Some(PeerMessage::Question),
        "reply" => Some(PeerMessage::Reply),
        _ => None,
    }
}

pub(crate) fn addressed_to(event: &Event, recipient: &str) -> bool {
    event.tags.iter().any(|tag| {
        let tag = tag.as_slice();
        tag.first().is_some_and(|name| name == "p")
            && tag.get(1).is_some_and(|key| key == recipient)
    })
}

/// Presentation records never instruct another agent. Only an explicitly
/// addressed scheduled occurrence is an instruction among task markers.
pub(crate) fn is_passive_record(event: &Event) -> bool {
    if event.kind.as_u16() == buzz_core::kind::KIND_CONVERSATION_ORGANIZATION as u16 {
        return true;
    }
    event.kind.as_u16() == 9
        && event.tags.iter().any(|tag| {
            let tag = tag.as_slice();
            tag.first().is_some_and(|name| name == "buzz-task")
                && tag.get(1).is_some_and(|kind| {
                    matches!(
                        kind.as_str(),
                        "root" | "assignment" | "schedule" | "lifecycle" | "report"
                    )
                })
        })
}

/// An explicit agent address starts or continues a visible task thread.
/// Unmentioned direct messages retain their ordinary inline behavior.
pub(crate) fn requires_task_thread(event: &Event, recipient: &str) -> bool {
    event.kind.as_u16() == 9 && addressed_to(event, recipient)
}

/// Scheduled instructions and peer questions always use their visible thread,
/// including in DMs and when ordinary replies are configured inline.
pub(crate) fn conversation_scope(channel_id: uuid::Uuid, event: &Event) -> SessionScope {
    conversation_scope_for_root(
        channel_id,
        &parse_thread_tags(event)
            .root_event_id
            .unwrap_or_else(|| event.id.to_hex()),
    )
}

/// Use the organization projection's effective destination for grouped
/// messages while retaining the same visible-thread session behavior.
pub(crate) fn conversation_scope_for_root(
    channel_id: uuid::Uuid,
    effective_root: &str,
) -> SessionScope {
    SessionScope::Thread {
        channel_id,
        root_event_id: effective_root.to_ascii_lowercase(),
    }
}

pub(crate) fn is_scheduled_instruction(event: &Event) -> bool {
    event.kind.as_u16() == 9
        && event.tags.iter().any(|tag| {
            let tag = tag.as_slice();
            tag.len() == 4
                && tag[0] == "buzz-task"
                && tag[1] == "scheduled"
                && tag[2..].iter().all(|value| {
                    uuid::Uuid::parse_str(value)
                        .is_ok_and(|id| !id.is_nil() && id.to_string() == *value)
                })
        })
}

pub(crate) fn question_instruction(event: &Event) -> Option<String> {
    let recipient = event.tags.iter().find_map(|tag| {
        let tag = tag.as_slice();
        (tag.first().map(String::as_str) == Some("p"))
            .then(|| tag.get(1))
            .flatten()
    })?;
    if peer_message(event, recipient) != Some(PeerMessage::Question) {
        return None;
    }
    Some(format!(
        "This is a question from an enrolled teammate, not a new work assignment. \
         Answer it using buzz_peer_reply with request_event_id=\"{}\" and your answer. \
         That sends the visible reply to this same task thread and returns it to \
         the teammate waiting for it. Preserve your current task. Do not dispatch \
         another job, ask a human to relay the answer, or start an acknowledgement loop.",
        event.id.to_hex()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, Tag};

    fn message(mode: &str, root: Option<&str>) -> (Event, String) {
        let keys = Keys::generate();
        let recipient = Keys::generate().public_key().to_hex();
        let mut tags = vec![
            Tag::parse(["p", &recipient]).unwrap(),
            Tag::parse(["buzz-peer", mode, "12345678-1234-4234-8234-123456789012"]).unwrap(),
        ];
        if let Some(root) = root {
            tags.push(Tag::parse(["e", root, "", "root"]).unwrap());
            tags.push(Tag::parse(["e", root, "", "reply"]).unwrap());
        }
        (
            EventBuilder::new(Kind::Custom(9), "What is your status?")
                .tags(tags)
                .sign_with_keys(&keys)
                .unwrap(),
            recipient,
        )
    }

    #[test]
    fn addressed_question_keeps_task_root_and_reply_contract() {
        let root = "a".repeat(64);
        let (event, recipient) = message("question", Some(&root));
        assert_eq!(
            peer_message(&event, &recipient),
            Some(PeerMessage::Question)
        );
        assert_eq!(peer_message(&event, &"b".repeat(64)), None);
        assert_eq!(
            conversation_scope(uuid::Uuid::new_v4(), &event).root_event_id(),
            Some(root.as_str())
        );
        let instruction = question_instruction(&event).unwrap();
        assert!(instruction.contains("buzz_peer_reply"));
        assert!(instruction.contains(&event.id.to_hex()));
    }

    #[test]
    fn reply_does_not_create_another_question_or_job() {
        let (event, recipient) = message("reply", Some(&"a".repeat(64)));
        assert_eq!(peer_message(&event, &recipient), Some(PeerMessage::Reply));
        assert!(question_instruction(&event).is_none());
    }

    #[test]
    fn schedule_delivers_exact_user_text_and_only_occurrences_invoke() {
        let keys = Keys::generate();
        let recipient = Keys::generate().public_key().to_hex();
        let instruction = "Continue the current task. If blocked, ask the responsible teammate.\nReport only useful progress.";
        let schedule = uuid::Uuid::new_v4().to_string();
        let root = EventBuilder::new(Kind::Custom(9), "Scheduled reminder")
            .tags([Tag::parse(["buzz-task", "schedule", &schedule]).unwrap()])
            .sign_with_keys(&keys)
            .unwrap();
        assert!(is_passive_record(&root));
        assert!(!requires_task_thread(&root, &recipient));
        let occurrence = EventBuilder::new(Kind::Custom(9), instruction)
            .tags([
                Tag::parse([
                    "buzz-task",
                    "scheduled",
                    &schedule,
                    &uuid::Uuid::new_v4().to_string(),
                ])
                .unwrap(),
                Tag::parse(["p", &recipient]).unwrap(),
                Tag::parse(["e", &root.id.to_hex(), "", "root"]).unwrap(),
                Tag::parse(["e", &root.id.to_hex(), "", "reply"]).unwrap(),
            ])
            .sign_with_keys(&keys)
            .unwrap();
        assert_eq!(occurrence.content, instruction);
        assert!(!is_passive_record(&occurrence));
        assert!(is_scheduled_instruction(&occurrence));
        assert!(requires_task_thread(&occurrence, &recipient));
        assert_eq!(
            conversation_scope(uuid::Uuid::new_v4(), &occurrence).root_event_id(),
            Some(root.id.to_hex().as_str())
        );
        assert!(question_instruction(&occurrence).is_none());
    }

    #[test]
    fn presentation_records_never_start_an_agent_turn() {
        let keys = Keys::generate();
        for marker in ["root", "assignment", "lifecycle", "report"] {
            let event = EventBuilder::new(Kind::Custom(9), "Task update")
                .tags([Tag::parse(["buzz-task", marker, "id"]).unwrap()])
                .sign_with_keys(&keys)
                .unwrap();
            assert!(is_passive_record(&event));
        }
        let dm = EventBuilder::new(Kind::Custom(9), "Hello")
            .sign_with_keys(&keys)
            .unwrap();
        assert!(!requires_task_thread(&dm, &keys.public_key().to_hex()));
        assert!(!is_passive_record(&dm));
    }
}
