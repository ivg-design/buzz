use super::*;
use nostr::{EventBuilder, JsonUtil, Keys, Kind, Tag, Timestamp};

fn message(channel: Uuid, parent: Option<&Event>) -> Event {
    let mut tags = vec![
        Tag::parse(["h", &channel.to_string()]).unwrap(),
        Tag::parse([
            "imeta",
            "url https://media.example/original.png",
            "m image/png",
        ])
        .unwrap(),
    ];
    if let Some(parent) = parent {
        tags.push(Tag::parse(["e", &parent.id.to_hex(), "", "reply"]).unwrap());
    }
    EventBuilder::new(Kind::Custom(9), "Original https://example.com/source")
        .tags(tags)
        .sign_with_keys(&Keys::generate())
        .unwrap()
}

fn change(channel: Uuid, action: OrganizationAction) -> Event {
    EventBuilder::new(
        Kind::Custom(KIND_CONVERSATION_ORGANIZATION as u16),
        serde_json::to_string(&OrganizationChange { version: 1, action }).unwrap(),
    )
    .tags([Tag::parse(["h", &channel.to_string()]).unwrap()])
    .sign_with_keys(&Keys::generate())
    .unwrap()
}

fn grouping(channel: Uuid, source: &Event, root: &Event) -> Event {
    change(
        channel,
        OrganizationAction::Group {
            message_ids: vec![source.id.to_hex()],
            thread_root_id: root.id.to_hex(),
            title: Some("Build discussion".into()),
            summary: Some("Build findings and their original links.".into()),
        },
    )
}

#[test]
fn grouping_preserves_signed_sources_links_attachments_and_reply_relationships() {
    let channel = Uuid::new_v4();
    let source = message(channel, None);
    let reply = message(channel, Some(&source));
    let root = message(channel, None);
    let originals = vec![source.clone(), reply.clone(), root.clone()];
    let bytes: Vec<_> = originals.iter().map(Event::as_json).collect();
    let operation = grouping(channel, &source, &root);
    assert!(validate_references(&operation, &originals).is_ok());
    // Later replies need no rewrite or re-publication and are not accidentally
    // added to the stored operation's bounded explicit selection.
    let late = message(channel, Some(&source));
    assert_eq!(
        crate::nip10::parse_thread_markers(&late.tags)
            .resolve()
            .unwrap()
            .1,
        source.id.to_hex()
    );
    assert_eq!(
        originals.iter().map(Event::as_json).collect::<Vec<_>>(),
        bytes
    );
    for event in &originals {
        crate::verify_event(event).unwrap();
    }
    assert_eq!(parse_change(&operation).unwrap().1.references().len(), 2);
}

#[test]
fn cross_channel_and_missing_references_are_rejected() {
    let channel = Uuid::new_v4();
    let source = message(Uuid::new_v4(), None);
    let root = message(channel, None);
    let operation = grouping(channel, &source, &root);
    assert!(validate_references(&operation, &[source, root.clone()]).is_err());
    assert!(validate_references(&operation, &[root]).is_err());
}

#[test]
fn nested_destination_and_source_self_group_are_rejected() {
    let channel = Uuid::new_v4();
    let source = message(channel, None);
    let reply = message(channel, Some(&source));
    assert!(validate_references(
        &grouping(channel, &source, &reply),
        &[source.clone(), reply]
    )
    .is_err());
    assert!(parse_change(&grouping(channel, &source, &source)).is_err());
}

#[test]
fn ordinary_agent_signer_can_hide_other_authors_messages_and_restore_with_undo() {
    let channel = Uuid::new_v4();
    let source = message(channel, None);
    let hidden = change(
        channel,
        OrganizationAction::Hide {
            message_ids: vec![source.id.to_hex()],
            hidden: true,
        },
    );
    assert_ne!(hidden.pubkey, source.pubkey);
    assert!(validate_references(&hidden, &[source.clone()]).is_ok());
    let undo = change(
        channel,
        OrganizationAction::Undo {
            change_event_id: hidden.id.to_hex(),
        },
    );
    assert!(validate_references(&undo, &[hidden]).is_ok());
    let undo_undo = change(
        channel,
        OrganizationAction::Undo {
            change_event_id: undo.id.to_hex(),
        },
    );
    assert!(validate_references(&undo_undo, &[undo]).is_err());
}

#[test]
fn undo_cannot_reference_future_or_original_message() {
    let channel = Uuid::new_v4();
    let source = message(channel, None);
    let undo = change(
        channel,
        OrganizationAction::Undo {
            change_event_id: source.id.to_hex(),
        },
    );
    assert!(validate_references(&undo, &[source.clone()]).is_err());
    let hidden = change(
        channel,
        OrganizationAction::Hide {
            message_ids: vec![source.id.to_hex()],
            hidden: true,
        },
    );
    let future = EventBuilder::new(
        Kind::Custom(KIND_CONVERSATION_ORGANIZATION as u16),
        &hidden.content,
    )
    .tags(hidden.tags.to_vec())
    .custom_created_at(Timestamp::from(hidden.created_at.as_secs() + 60))
    .sign_with_keys(&Keys::generate())
    .unwrap();
    let undo = change(
        channel,
        OrganizationAction::Undo {
            change_event_id: future.id.to_hex(),
        },
    );
    assert!(validate_references(&undo, &[future]).is_err());
}

#[test]
fn ambiguous_channel_routing_expiration_and_unknown_fields_are_rejected() {
    let channel = Uuid::new_v4();
    let source = message(channel, None);
    let valid = change(
        channel,
        OrganizationAction::Hide {
            message_ids: vec![source.id.to_hex()],
            hidden: true,
        },
    );
    for extra in [
        vec!["h", &channel.to_string()],
        vec!["e", &source.id.to_hex(), "", "reply"],
        vec!["expiration", "9"],
    ] {
        let invalid = EventBuilder::new(valid.kind, &valid.content)
            .tags(valid.tags.clone().to_vec())
            .tags([Tag::parse(extra).unwrap()])
            .sign_with_keys(&Keys::generate())
            .unwrap();
        assert!(parse_change(&invalid).is_err());
    }
    let invalid = EventBuilder::new(
        valid.kind,
        valid
            .content
            .replace("\"version\":1", "\"version\":1,\"discard_original\":true"),
    )
    .tags(valid.tags.to_vec())
    .sign_with_keys(&Keys::generate())
    .unwrap();
    assert!(parse_change(&invalid).is_err());
}

#[test]
fn change_bounds_and_metadata_roundtrip_are_enforced() {
    let ids = vec!["a".repeat(64); 101];
    assert!(OrganizationChange {
        version: 1,
        action: OrganizationAction::Hide {
            message_ids: ids,
            hidden: true
        }
    }
    .validate()
    .is_err());
    let operation = OrganizationChange {
        version: 1,
        action: OrganizationAction::ThreadMetadata {
            thread_root_id: "a".repeat(64),
            title: Some("Release".into()),
            summary: Some(String::new()),
        },
    };
    operation.validate().unwrap();
    assert_eq!(
        serde_json::from_str::<OrganizationChange>(&serde_json::to_string(&operation).unwrap())
            .unwrap(),
        operation
    );
}

#[test]
fn same_second_signed_restore_and_rename_follow_the_observed_change() {
    let channel = Uuid::new_v4();
    let now = 1_789_200_000;
    let keys = Keys::generate();
    let action = |hidden| OrganizationChange {
        version: 1,
        action: OrganizationAction::Hide {
            message_ids: vec!["a".repeat(64)],
            hidden,
        },
    };
    let hidden = build_change_event(channel, &action(true), &keys, now, &[]).unwrap();
    // Both user actions happen at the exact same wall-clock second. Real signed
    // event IDs are hashes, not artificially ascending fixture IDs.
    let restored =
        build_change_event(channel, &action(false), &keys, now, &[hidden.clone()]).unwrap();
    assert_eq!(hidden.created_at.as_secs(), now);
    assert_eq!(restored.created_at.as_secs(), now + 1);
    let mut replay = [restored, hidden];
    replay.sort_by_key(|event| (event.created_at, event.id));
    assert!(matches!(
        parse_change(&replay[1]).unwrap().1.action,
        OrganizationAction::Hide { hidden: false, .. }
    ));
    for event in &replay {
        crate::verify_event(event).unwrap();
    }

    let metadata = |title: &str| OrganizationChange {
        version: 1,
        action: OrganizationAction::ThreadMetadata {
            thread_root_id: "a".repeat(64),
            title: Some(title.into()),
            summary: None,
        },
    };
    let first = build_change_event(channel, &metadata("Draft"), &keys, now, &[]).unwrap();
    let second =
        build_change_event(channel, &metadata("Final"), &keys, now, &[first.clone()]).unwrap();
    assert!(second.created_at > first.created_at);
    assert!(
        matches!(parse_change(&second).unwrap().1.action, OrganizationAction::ThreadMetadata { title: Some(title), .. } if title == "Final")
    );
}

#[test]
fn truly_concurrent_changes_retain_timestamp_and_id_tie_breaking() {
    let channel = Uuid::new_v4();
    let now = 1_789_200_000;
    let change = OrganizationChange {
        version: 1,
        action: OrganizationAction::Hide {
            message_ids: vec!["a".repeat(64)],
            hidden: true,
        },
    };
    let first = build_change_event(channel, &change, &Keys::generate(), now, &[]).unwrap();
    let other = build_change_event(channel, &change, &Keys::generate(), now, &[]).unwrap();
    assert_eq!(first.created_at, other.created_at);
    assert_ne!(first.id, other.id);
    let mut forward = [first.clone(), other.clone()];
    let mut reversed = [other, first];
    forward.sort_by_key(|event| (event.created_at, event.id));
    reversed.sort_by_key(|event| (event.created_at, event.id));
    assert_eq!(forward, reversed);
}

#[test]
fn sequential_builder_rejects_misrouted_and_unbounded_future_history() {
    let channel = Uuid::new_v4();
    let now = 1_789_200_000;
    let keys = Keys::generate();
    let change = OrganizationChange {
        version: 1,
        action: OrganizationAction::Hide {
            message_ids: vec!["a".repeat(64)],
            hidden: true,
        },
    };
    let wrong = build_change_event(Uuid::new_v4(), &change, &keys, now, &[]).unwrap();
    assert!(build_change_event(channel, &change, &keys, now, &[wrong]).is_err());
    let future = build_change_event(channel, &change, &keys, now + 300, &[]).unwrap();
    assert!(build_change_event(channel, &change, &keys, now, &[future]).is_err());
}
