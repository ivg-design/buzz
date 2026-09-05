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

fn participants(root: &Event, agents: Vec<String>) -> OrganizationAction {
    OrganizationAction::Participants {
        thread_root_id: root.id.to_hex(),
        agent_pubkeys: agents,
    }
}

fn ordered_change(channel: Uuid, action: OrganizationAction, history: &[Event]) -> Event {
    build_change_event(
        channel,
        &OrganizationChange { version: 1, action },
        &Keys::generate(),
        1_789_200_000,
        history,
    )
    .unwrap()
}

fn group_action(source: &Event, destination: &Event) -> OrganizationAction {
    OrganizationAction::Group {
        message_ids: vec![source.id.to_hex()],
        thread_root_id: destination.id.to_hex(),
        title: None,
        summary: None,
    }
}

fn nested_message(channel: Uuid, root: &Event, parent: &Event) -> Event {
    EventBuilder::new(Kind::Custom(9), "Reply https://example.com/attachment")
        .tags([
            Tag::parse(["h", &channel.to_string()]).unwrap(),
            Tag::parse(["e", &root.id.to_hex(), "", "root"]).unwrap(),
            Tag::parse(["e", &parent.id.to_hex(), "", "reply"]).unwrap(),
        ])
        .sign_with_keys(&Keys::generate())
        .unwrap()
}

#[test]
fn participant_wire_requires_complete_unique_lowercase_keys_and_accepts_empty() {
    let channel = Uuid::new_v4();
    let root = message(channel, None);
    let key = Keys::generate().public_key().to_hex();
    let operation = ordered_change(channel, participants(&root, vec![key.clone()]), &[]);
    let parsed = validate_references(&operation, &[root.clone()]).unwrap();
    assert_eq!(parsed.references(), vec![root.id.to_hex()]);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&operation.content).unwrap()["action"],
        serde_json::json!({"type":"participants", "thread_root_id":root.id.to_hex(), "agent_pubkeys":[key]})
    );
    let empty = ordered_change(channel, participants(&root, vec![]), &[operation]);
    assert!(validate_references(&empty, &[root.clone()]).is_ok());
    for keys in [
        vec![key.to_uppercase()],
        vec!["a".repeat(63)],
        vec!["g".repeat(64)],
        vec![key.clone(), key],
        (0..101).map(|value| format!("{value:064x}")).collect(),
    ] {
        assert!(OrganizationChange {
            version: 1,
            action: participants(&root, keys),
        }
        .validate()
        .is_err());
    }
    assert!(
        serde_json::from_value::<OrganizationChange>(serde_json::json!({
            "version":1,"action":{"type":"participants","thread_root_id":root.id.to_hex()}
        }))
        .is_err()
    );
}

#[test]
fn participants_require_an_original_top_level_root_in_the_same_channel() {
    let channel = Uuid::new_v4();
    let root = message(channel, None);
    let reply = message(channel, Some(&root));
    let foreign = message(Uuid::new_v4(), None);
    for target in [reply, foreign] {
        let operation = ordered_change(channel, participants(&target, vec![]), &[]);
        assert!(validate_references(&operation, &[target]).is_err());
    }
    let operation = ordered_change(channel, participants(&root, vec![]), &[]);
    assert!(validate_references(&operation, &[]).is_err());
}

#[test]
fn participant_removal_and_undo_distinguish_empty_policy_from_no_policy() {
    let channel = Uuid::new_v4();
    let root = message(channel, None);
    let root_id = root.id.to_hex();
    let sources = [root.clone()];
    let a = Keys::generate().public_key().to_hex();
    let b = Keys::generate().public_key().to_hex();
    let projection = |changes: &[Event]| {
        OrganizationProjection::from_events(channel, changes, &sources).unwrap()
    };
    assert_eq!(projection(&[]).participants(&root_id), None);
    let joined = ordered_change(channel, participants(&root, vec![a, b.clone()]), &[]);
    // Full-list replacement removes a; it does not append to the previous list.
    let replaced = ordered_change(
        channel,
        participants(&root, vec![b.clone()]),
        &[joined.clone()],
    );
    assert!(replaced.created_at > joined.created_at);
    let removed = ordered_change(channel, participants(&root, vec![]), &[replaced.clone()]);
    let mut history = vec![joined.clone(), replaced.clone(), removed.clone()];
    assert_eq!(projection(&history).participants(&root_id), Some(&[][..]));
    let undo = ordered_change(
        channel,
        OrganizationAction::Undo {
            change_event_id: removed.id.to_hex(),
        },
        &history,
    );
    history.push(undo);
    assert_eq!(projection(&history).participants(&root_id), Some(&[b][..]));
    for target in [replaced, joined] {
        history.push(ordered_change(
            channel,
            OrganizationAction::Undo {
                change_event_id: target.id.to_hex(),
            },
            &history,
        ));
    }
    assert_eq!(projection(&history).participants(&root_id), None);
}

#[test]
fn concurrent_signed_participant_policies_use_event_id_order_and_deduplicate_history() {
    let channel = Uuid::new_v4();
    let root = message(channel, None);
    let first = ordered_change(
        channel,
        participants(&root, vec![Keys::generate().public_key().to_hex()]),
        &[],
    );
    let second = ordered_change(channel, participants(&root, vec![]), &[]);
    assert_eq!(first.created_at, second.created_at);
    let winner = if first.id > second.id {
        &first
    } else {
        &second
    };
    let OrganizationAction::Participants { agent_pubkeys, .. } =
        parse_change(winner).unwrap().1.action
    else {
        panic!("participant action expected");
    };
    for history in [
        vec![first.clone(), second.clone()],
        vec![second.clone(), first.clone(), second.clone(), first.clone()],
    ] {
        let projection =
            OrganizationProjection::from_events(channel, &history, &[root.clone()]).unwrap();
        assert_eq!(
            projection.participants(&root.id.to_hex()),
            Some(agent_pubkeys.as_slice())
        );
    }
    // Undo at the same signed second is valid regardless of its random ID rank.
    let undo = ordered_change(
        channel,
        OrganizationAction::Undo {
            change_event_id: winner.id.to_hex(),
        },
        &[],
    );
    let loser = if first.id > second.id {
        &second
    } else {
        &first
    };
    let OrganizationAction::Participants { agent_pubkeys, .. } =
        parse_change(loser).unwrap().1.action
    else {
        panic!("participant action expected");
    };
    let projection =
        OrganizationProjection::from_events(channel, &[undo, first, second], &[root.clone()])
            .unwrap();
    assert_eq!(
        projection.participants(&root.id.to_hex()),
        Some(agent_pubkeys.as_slice())
    );
}

#[test]
fn grouped_subtrees_follow_destination_policy_and_undo_without_rewriting_sources() {
    let channel = Uuid::new_v4();
    let a = message(channel, None);
    let b = message(channel, None);
    let c = message(channel, None);
    let reply = message(channel, Some(&a));
    let late_reply = nested_message(channel, &a, &reply);
    let originals = vec![
        a.clone(),
        b.clone(),
        c.clone(),
        reply.clone(),
        late_reply.clone(),
    ];
    let bytes: Vec<_> = originals.iter().map(Event::as_json).collect();
    let joined = ordered_change(
        channel,
        participants(&b, vec![Keys::generate().public_key().to_hex()]),
        &[],
    );
    let empty_c = ordered_change(channel, participants(&c, vec![]), &[joined.clone()]);
    let move_a = ordered_change(channel, group_action(&a, &b), &[empty_c.clone()]);
    let move_b = ordered_change(channel, group_action(&b, &c), &[move_a.clone()]);
    let mut history = vec![joined, empty_c, move_a, move_b.clone()];
    let projection = OrganizationProjection::from_events(channel, &history, &originals).unwrap();
    for event in [&a, &b, &reply, &late_reply] {
        let root = projection.effective_root(&event.id.to_hex());
        assert_eq!(root, c.id.to_hex());
        assert_eq!(projection.participants(&root), Some(&[][..]));
    }
    history.push(ordered_change(
        channel,
        OrganizationAction::Undo {
            change_event_id: move_b.id.to_hex(),
        },
        &history,
    ));
    let projection = OrganizationProjection::from_events(channel, &history, &originals).unwrap();
    let root = projection.effective_root(&late_reply.id.to_hex());
    assert_eq!(root, b.id.to_hex());
    assert_eq!(projection.participants(&root).unwrap().len(), 1);
    assert_eq!(
        originals.iter().map(Event::as_json).collect::<Vec<_>>(),
        bytes
    );
}

#[test]
fn latest_ancestor_move_wins_with_missing_parent_and_destination_cycles() {
    let channel = Uuid::new_v4();
    let a = message(channel, None);
    let b = message(channel, None);
    let c = message(channel, None);
    let reply = message(channel, Some(&a));
    let leaf = nested_message(channel, &a, &reply);
    let child_move = ordered_change(channel, group_action(&reply, &b), &[]);
    let ancestor_move = ordered_change(channel, group_action(&a, &c), &[child_move.clone()]);
    let history = [child_move, ancestor_move];
    for sources in [
        vec![a.clone(), b.clone(), c.clone(), reply, leaf.clone()],
        vec![a.clone(), b.clone(), c.clone(), leaf.clone()],
    ] {
        let projection = OrganizationProjection::from_events(channel, &history, &sources).unwrap();
        assert_eq!(projection.effective_root(&leaf.id.to_hex()), c.id.to_hex());
    }
    let move_a = ordered_change(channel, group_action(&a, &b), &[]);
    let move_b = ordered_change(channel, group_action(&b, &a), &[move_a.clone()]);
    let projection =
        OrganizationProjection::from_events(channel, &[move_a, move_b], &[a.clone(), b.clone()])
            .unwrap();
    assert_eq!(projection.effective_root(&a.id.to_hex()), a.id.to_hex());
    assert_eq!(projection.effective_root(&b.id.to_hex()), a.id.to_hex());
}

#[test]
fn organization_projection_rejects_foreign_or_tampered_history_and_invalid_undo() {
    let channel = Uuid::new_v4();
    let root = message(channel, None);
    let valid = ordered_change(channel, participants(&root, vec![]), &[]);
    let mut tampered = valid.clone();
    tampered.content.push(' ');
    let foreign = ordered_change(Uuid::new_v4(), participants(&root, vec![]), &[]);
    let missing_undo = ordered_change(
        channel,
        OrganizationAction::Undo {
            change_event_id: "a".repeat(64),
        },
        &[],
    );
    for invalid in [tampered, foreign, missing_undo] {
        assert!(OrganizationProjection::from_events(channel, &[invalid], &[root.clone()]).is_err());
    }
    assert!(OrganizationProjection::from_events(
        channel,
        &[valid],
        &[message(Uuid::new_v4(), None)]
    )
    .is_err());
}

#[test]
fn incremental_ancestry_is_atomic_and_new_replies_inherit_moved_subtree_policy() {
    let channel = Uuid::new_v4();
    let a = message(channel, None);
    let b = message(channel, None);
    let reply = message(channel, Some(&a));
    let leaf = nested_message(channel, &a, &reply);
    let moved = ordered_change(channel, group_action(&reply, &b), &[]);
    let empty = ordered_change(channel, participants(&b, vec![]), &[moved.clone()]);
    let mut projection = OrganizationProjection::from_events(
        channel,
        &[moved, empty.clone()],
        &[a.clone(), b.clone(), reply.clone()],
    )
    .unwrap();
    let leaf_id = leaf.id.to_hex();
    let mut tampered = nested_message(channel, &a, &reply);
    tampered.content.push(' ');
    for invalid in [tampered, message(Uuid::new_v4(), None), empty] {
        assert!(projection
            .add_messages(channel, &[leaf.clone(), invalid])
            .is_err());
        // Even the valid first item was not inserted when a later item failed.
        assert_eq!(projection.effective_root(&leaf_id), leaf_id);
        assert_eq!(projection.effective_root(&reply.id.to_hex()), b.id.to_hex());
        assert_eq!(projection.participants(&b.id.to_hex()), Some(&[][..]));
    }
    // A caller cannot change the channel fence by supplying a different ID.
    assert!(projection
        .add_messages(Uuid::new_v4(), std::slice::from_ref(&leaf))
        .is_err());
    assert_eq!(projection.effective_root(&leaf_id), leaf_id);
    projection
        .add_messages(channel, &[reply, leaf.clone(), leaf])
        .unwrap();
    assert_eq!(projection.effective_root(&leaf_id), b.id.to_hex());
    assert_eq!(projection.effective_root(&a.id.to_hex()), a.id.to_hex());
    assert_eq!(
        projection.participants(&projection.effective_root(&leaf_id)),
        Some(&[][..])
    );
}
