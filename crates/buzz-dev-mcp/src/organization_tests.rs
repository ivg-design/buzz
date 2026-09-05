use super::*;

fn params() -> OrganizationReadParams {
    serde_json::from_value(serde_json::json!({"limit": 20})).unwrap()
}

#[test]
fn scoped_history_uses_composite_cursor_without_losing_same_second_events() {
    let channel = uuid::Uuid::new_v4();
    let mut input = params();
    input.before_created_at = Some(123);
    input.before_event_id = Some("a".repeat(64));
    let filter = history_filter(channel, &input).unwrap();
    assert_eq!(filter["#h"], serde_json::json!([channel]));
    assert_eq!(filter["before_id"], "a".repeat(64));
    assert_eq!(filter["until"], 123);
    assert_eq!(filter["limit"], 21);
    input.before_event_id = None;
    assert!(history_filter(channel, &input).is_err());
}

#[test]
fn change_history_is_separate_from_searchable_messages() {
    let mut input = params();
    input.source = OrganizationHistorySource::Changes;
    let filter = history_filter(uuid::Uuid::new_v4(), &input).unwrap();
    assert_eq!(filter["kinds"], serde_json::json!([40009]));
    input.search = Some("build".into());
    assert!(history_filter(uuid::Uuid::new_v4(), &input).is_err());
}

#[test]
fn search_uses_relevance_pages_without_dropping_a_sentinel_hit() {
    let mut input = params();
    input.search = Some("build".into());
    input.search_page = Some(2);
    let filter = history_filter(uuid::Uuid::new_v4(), &input).unwrap();
    assert_eq!(filter["page"], 2);
    assert_eq!(filter["limit"], 20);
    assert_eq!(filter["search"], "build");
    input.thread_root_id = Some("a".repeat(64));
    assert!(history_filter(uuid::Uuid::new_v4(), &input).is_err());
}

#[test]
fn every_typed_action_decodes_to_shared_wire_and_rejects_unrecognized_fields() {
    for action in [
        serde_json::json!({"type":"group","message_ids":["a".repeat(64)],"thread_root_id":"b".repeat(64),"title":"Build"}),
        serde_json::json!({"type":"thread_metadata","thread_root_id":"b".repeat(64),"summary":"Build findings"}),
        serde_json::json!({"type":"participants","thread_root_id":"b".repeat(64),"agent_pubkeys":["c".repeat(64)]}),
        serde_json::json!({"type":"participants","thread_root_id":"b".repeat(64),"agent_pubkeys":[]}),
        serde_json::json!({"type":"hide","message_ids":["a".repeat(64)],"hidden":true}),
        serde_json::json!({"type":"undo","change_event_id":"a".repeat(64)}),
    ] {
        let input: OrganizationActionInput = serde_json::from_value(action.clone()).unwrap();
        let decoded = OrganizationChange {
            version: 1,
            action: serde_json::from_value(serde_json::to_value(input).unwrap()).unwrap(),
        };
        decoded.validate().unwrap();
        let mut invalid = action;
        invalid["delete_original"] = serde_json::json!(true);
        assert!(serde_json::from_value::<OrganizationActionInput>(invalid).is_err());
    }
}

#[test]
fn participant_input_requires_verified_enrolled_agents() {
    let peer = super::super::peers::VerifiedPeer {
        name: "Clauditron".into(),
        pubkey: "c".repeat(64),
        owner_pubkey: "d".repeat(64),
    };
    assert!(validate_participant_roster(&[peer.pubkey.clone()], &[peer]).is_ok());
    assert!(validate_participant_roster(&[], &[]).is_ok());
    let error = validate_participant_roster(&["e".repeat(64)], &[]).unwrap_err();
    assert!(error.contains("not in the verified Nemo roster"));
}

#[test]
fn effective_participant_result_distinguishes_unset_empty_and_undo() {
    use buzz_core::organization::OrganizationAction;
    use nostr::{EventBuilder, Keys, Kind, Tag};

    let channel = uuid::Uuid::new_v4();
    let root = EventBuilder::new(Kind::Custom(9), "Shared task")
        .tags([Tag::parse(["h", &channel.to_string()]).unwrap()])
        .sign_with_keys(&Keys::generate())
        .unwrap();
    let thread_root_id = root.id.to_hex();
    let unset =
        effective_participants(channel, &thread_root_id, &[], std::slice::from_ref(&root)).unwrap();
    assert_eq!(unset["configured"], false);
    assert_eq!(unset["agent_pubkeys"], serde_json::json!([]));

    let empty = organization::build_change_event(
        channel,
        &OrganizationChange {
            version: 1,
            action: OrganizationAction::Participants {
                thread_root_id: thread_root_id.clone(),
                agent_pubkeys: vec![],
            },
        },
        &Keys::generate(),
        1_789_200_000,
        &[],
    )
    .unwrap();
    let explicitly_empty = effective_participants(
        channel,
        &thread_root_id,
        std::slice::from_ref(&empty),
        std::slice::from_ref(&root),
    )
    .unwrap();
    assert_eq!(explicitly_empty["configured"], true);
    assert_eq!(explicitly_empty["agent_pubkeys"], serde_json::json!([]));

    let undo = organization::build_change_event(
        channel,
        &OrganizationChange {
            version: 1,
            action: OrganizationAction::Undo {
                change_event_id: empty.id.to_hex(),
            },
        },
        &Keys::generate(),
        1_789_200_000,
        std::slice::from_ref(&empty),
    )
    .unwrap();
    let undone = effective_participants(channel, &thread_root_id, &[empty, undo], &[root]).unwrap();
    assert_eq!(undone["configured"], false);
    assert_eq!(undone["agent_pubkeys"], serde_json::json!([]));
}
