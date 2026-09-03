use super::*;
use nostr::{EventBuilder, Keys, Kind};

fn event_with_tags(content: &str, tags: Vec<Vec<String>>) -> Event {
    let tags = tags
        .into_iter()
        .map(|tag| nostr::Tag::parse(tag).expect("valid tag"))
        .collect::<Vec<_>>();
    EventBuilder::new(Kind::Custom(9), content)
        .tags(tags)
        .sign_with_keys(&Keys::generate())
        .expect("signed event")
}

fn batch(channel_id: Uuid, scope: SessionScope, event: Event) -> FlushBatch {
    FlushBatch {
        channel_id,
        scope,
        events: vec![BatchEvent {
            event,
            prompt_tag: "@mention".into(),
            received_at: Instant::now(),
        }],
        cancelled_events: vec![],
        cancel_reason: None,
    }
}

fn stream_channel() -> PromptChannelInfo {
    PromptChannelInfo {
        name: "general".into(),
        channel_type: "stream".into(),
        description: None,
        project: None,
    }
}

fn dm_channel() -> PromptChannelInfo {
    PromptChannelInfo {
        name: "DM".into(),
        channel_type: "dm".into(),
        description: None,
        project: None,
    }
}

#[test]
fn timeline_places_top_level_channel_response_in_main_timeline() {
    let channel_id = Uuid::new_v4();
    let event = event_with_tags("@agent hello", vec![]);
    let event_id = event.id.to_hex();
    let prompt = format_prompt(
        &batch(channel_id, SessionScope::Conversation { channel_id }, event),
        &FormatPromptArgs {
            channel_info: Some(&stream_channel()),
            reply_placement: ReplyPlacement::Timeline,
            ..Default::default()
        },
    )
    .join("\n\n");

    assert!(prompt.contains("response appears directly in the current timeline"));
    assert!(prompt.contains("without `--reply-to`"));
    assert!(!prompt.contains(&format!("--reply-to {event_id}")));
}

#[test]
fn timeline_top_level_response_remains_direct_with_thread_scoped_session() {
    let channel_id = Uuid::new_v4();
    let event = event_with_tags("@agent hello", vec![]);
    let event_id = event.id.to_hex();
    let prompt = format_prompt(
        &batch(
            channel_id,
            SessionScope::Thread {
                channel_id,
                root_event_id: event_id.clone(),
            },
            event,
        ),
        &FormatPromptArgs {
            channel_info: Some(&stream_channel()),
            reply_placement: ReplyPlacement::Timeline,
            ..Default::default()
        },
    )
    .join("\n\n");

    assert!(prompt.contains("response appears directly in the current timeline"));
    assert!(!prompt.contains(&format!("--reply-to {event_id}")));
}

#[test]
fn timeline_keeps_existing_thread_response_at_canonical_root() {
    let channel_id = Uuid::new_v4();
    let root_id = "a".repeat(64);
    let parent_id = "b".repeat(64);
    let event = event_with_tags(
        "@agent follow-up",
        vec![
            vec!["e".into(), root_id.clone(), String::new(), "root".into()],
            vec!["e".into(), parent_id.clone(), String::new(), "reply".into()],
        ],
    );
    let event_id = event.id.to_hex();
    let prompt = format_prompt(
        &batch(channel_id, SessionScope::Conversation { channel_id }, event),
        &FormatPromptArgs {
            channel_info: Some(&stream_channel()),
            reply_placement: ReplyPlacement::Timeline,
            ..Default::default()
        },
    )
    .join("\n\n");

    assert!(prompt.contains(&format!("--reply-to {root_id}")));
    assert!(!prompt.contains(&format!("--reply-to {parent_id}")));
    assert!(!prompt.contains(&format!("--reply-to {event_id}")));
}

#[test]
fn top_level_dm_honors_reply_placement_matrix() {
    for placement in [ReplyPlacement::Thread, ReplyPlacement::Timeline] {
        let channel_id = Uuid::new_v4();
        let event = event_with_tags("hello", vec![]);
        let event_id = event.id.to_hex();
        let prompt = format_prompt(
            &batch(channel_id, SessionScope::Conversation { channel_id }, event),
            &FormatPromptArgs {
                channel_info: Some(&dm_channel()),
                reply_placement: placement,
                ..Default::default()
            },
        )
        .join("\n\n");

        assert!(prompt.contains("Scope: dm"));
        match placement {
            ReplyPlacement::Thread => {
                assert!(prompt.contains(&format!("--reply-to {event_id}")));
                assert!(!prompt.contains("response appears directly in the current timeline"));
            }
            ReplyPlacement::Timeline => {
                assert!(prompt.contains("without `--reply-to`"));
                assert!(prompt.contains("response appears directly in the current timeline"));
                assert!(!prompt.contains(&format!("--reply-to {event_id}")));
            }
        }
    }
}

#[test]
fn dm_event_already_in_thread_stays_at_canonical_root_under_both_policies() {
    let root_id = "c".repeat(64);
    let parent_id = "d".repeat(64);

    for placement in [ReplyPlacement::Thread, ReplyPlacement::Timeline] {
        let channel_id = Uuid::new_v4();
        let event = event_with_tags(
            "follow-up",
            vec![
                vec!["e".into(), root_id.clone(), String::new(), "root".into()],
                vec!["e".into(), parent_id.clone(), String::new(), "reply".into()],
            ],
        );
        let event_id = event.id.to_hex();
        let prompt = format_prompt(
            &batch(channel_id, SessionScope::Conversation { channel_id }, event),
            &FormatPromptArgs {
                channel_info: Some(&dm_channel()),
                reply_placement: placement,
                ..Default::default()
            },
        )
        .join("\n\n");

        assert!(prompt.contains(&format!("--reply-to {root_id}")));
        assert!(!prompt.contains(&format!("--reply-to {parent_id}")));
        assert!(!prompt.contains(&format!("--reply-to {event_id}")));
        assert!(!prompt.contains("response appears directly in the current timeline"));
    }
}
