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

    assert!(prompt.contains("Trusted reply destination: current timeline"));
    assert!(!prompt.contains(&format!(
        "Trusted reply destination: thread root {event_id}"
    )));
    assert_eq!(
        trusted_chat_thread_root(
            &batch(
                channel_id,
                SessionScope::Conversation { channel_id },
                event_with_tags("@agent hello", vec![]),
            ),
            &FormatPromptArgs {
                channel_info: Some(&stream_channel()),
                reply_placement: ReplyPlacement::Timeline,
                ..Default::default()
            },
        ),
        None,
        "a later top-level turn must clear any prior thread binding"
    );
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

    assert!(prompt.contains("Trusted reply destination: current timeline"));
    assert!(!prompt.contains(&format!(
        "Trusted reply destination: thread root {event_id}"
    )));
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

    assert!(prompt.contains(&format!("Trusted reply destination: thread root {root_id}")));
    assert!(!prompt.contains(&format!(
        "Trusted reply destination: thread root {parent_id}"
    )));
    assert!(!prompt.contains(&format!(
        "Trusted reply destination: thread root {event_id}"
    )));
    let route_event = event_with_tags(
        "@agent follow-up",
        vec![
            vec!["e".into(), root_id.clone(), String::new(), "root".into()],
            vec!["e".into(), parent_id.clone(), String::new(), "reply".into()],
        ],
    );
    assert_eq!(
        trusted_chat_thread_root(
            &batch(
                channel_id,
                SessionScope::Conversation { channel_id },
                route_event,
            ),
            &FormatPromptArgs {
                channel_info: Some(&stream_channel()),
                reply_placement: ReplyPlacement::Timeline,
                ..Default::default()
            },
        )
        .as_deref(),
        Some(root_id.as_str()),
        "an explicit thread follow-up must bind the trusted tool to the canonical root"
    );
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
                assert!(prompt.contains(&format!(
                    "Trusted reply destination: new thread root {event_id}"
                )));
                assert!(!prompt.contains("Trusted reply destination: current timeline"));
            }
            ReplyPlacement::Timeline => {
                assert!(prompt.contains("Trusted reply destination: current timeline"));
                assert!(!prompt.contains(&format!(
                    "Trusted reply destination: new thread root {event_id}"
                )));
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

        assert!(prompt.contains(&format!("Trusted reply destination: thread root {root_id}")));
        assert!(!prompt.contains(&format!(
            "Trusted reply destination: thread root {parent_id}"
        )));
        assert!(!prompt.contains(&format!(
            "Trusted reply destination: thread root {event_id}"
        )));
        assert!(!prompt.contains("Trusted reply destination: current timeline"));
    }
}

#[test]
fn timeline_trusted_route_tracks_sequential_threads_then_clears_for_top_level() {
    let channel_id = Uuid::new_v4();
    let channel = stream_channel();
    let args = FormatPromptArgs {
        channel_info: Some(&channel),
        reply_placement: ReplyPlacement::Timeline,
        ..Default::default()
    };
    let threaded = |root: &str, parent: &str| {
        batch(
            channel_id,
            SessionScope::Conversation { channel_id },
            event_with_tags(
                "follow-up",
                vec![
                    vec!["e".into(), root.into(), String::new(), "root".into()],
                    vec!["e".into(), parent.into(), String::new(), "reply".into()],
                ],
            ),
        )
    };
    let root_a = "a".repeat(64);
    let root_b = "b".repeat(64);

    assert_eq!(
        trusted_chat_thread_root(&threaded(&root_a, &"c".repeat(64)), &args).as_deref(),
        Some(root_a.as_str())
    );
    assert_eq!(
        trusted_chat_thread_root(&threaded(&root_b, &"d".repeat(64)), &args).as_deref(),
        Some(root_b.as_str()),
        "the same channel-scoped session must follow a different explicit thread"
    );
    let mut batched = threaded(&root_a, &"e".repeat(64));
    let newest = threaded(&root_b, &"f".repeat(64))
        .events
        .pop()
        .expect("newest event");
    batched.events.push(newest);
    assert_eq!(
        trusted_chat_thread_root(&batched, &args).as_deref(),
        Some(root_b.as_str()),
        "a multi-event flush must route its one response to the latest user turn"
    );
    let mut merged = threaded(&root_b, &"1".repeat(64));
    merged.cancelled_events = threaded(&root_a, &"2".repeat(64)).events;
    merged.cancel_reason = Some(CancelReason::Steer);
    assert_eq!(
        trusted_chat_thread_root(&merged, &args).as_deref(),
        Some(root_b.as_str()),
        "cancelled prior work remains context and must not steal the new response route"
    );
    assert_eq!(
        trusted_chat_thread_root(
            &batch(
                channel_id,
                SessionScope::Conversation { channel_id },
                event_with_tags("back to main", vec![]),
            ),
            &args,
        ),
        None,
        "a subsequent top-level turn must not inherit the prior thread root"
    );
}
