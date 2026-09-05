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
