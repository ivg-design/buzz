use super::{
    engine::{self, Decision},
    events,
    store::Store,
    types::*,
};
use nostr::{JsonUtil, Keys};

const START: i64 = 1_788_638_400_000;

pub(super) fn fixture() -> (TimedTask, Keys, Keys) {
    let owner = Keys::generate();
    let agent = Keys::generate();
    let input = TaskInput {
        recipient_pubkey: agent.public_key().to_hex(),
        channel_id: uuid::Uuid::new_v4().to_string(),
        origin_event_id: None,
        instruction: "  Exact instruction\nwith newlines.  ".into(),
        interval: Interval {
            value: 1,
            unit: IntervalUnit::Minutes,
        },
        repetition: Repetition::Forever,
    };
    let id = uuid::Uuid::new_v4().to_string();
    let root = events::root(&id, &input, &owner, "http://localhost:1").unwrap();
    (
        TimedTask {
            id,
            input,
            owner_pubkey: owner.public_key().to_hex(),
            relay_url: "http://localhost:1".into(),
            thread_id: root.id.to_hex(),
            root_event: root,
            root_published: false,
            status: TaskStatus::Active,
            created_at: START,
            updated_at: START,
            next_run_at: Some(START + 60_000),
            last_delivered_at: None,
            last_delivered_event_id: None,
            delivered_count: 0,
            missed_count: 0,
            last_error: None,
            delivery_state: DeliveryState::Idle,
            in_flight: None,
            retry_at: 0,
            consecutive_failures: 0,
        },
        owner,
        agent,
    )
}

fn prepare(task: &mut TimedTask, owner: &Keys, now: i64) -> String {
    let Decision::Deliver { due_at } = engine::plan(task, now, true).unwrap() else {
        panic!("expected delivery");
    };
    let id = uuid::Uuid::new_v4().to_string();
    let event = events::occurrence(task, &id, owner).unwrap();
    task.in_flight = Some(Occurrence {
        id: id.clone(),
        event,
        due_at,
        attempts: 1,
    });
    id
}

#[test]
fn intervals_are_positive_elapsed_durations_and_first_run_is_delayed() {
    let (mut task, _, _) = fixture();
    assert_eq!(
        engine::plan(&mut task, START + 59_999, true).unwrap(),
        Decision::Wait
    );
    for (unit, millis) in [
        (IntervalUnit::Minutes, 120_000),
        (IntervalUnit::Hours, 7_200_000),
        (IntervalUnit::Days, 172_800_000),
    ] {
        assert_eq!(Interval { value: 2, unit }.millis().unwrap(), millis);
    }
    task.input.interval.value = 0;
    assert!(task.input.validate(START).is_err());
}

#[test]
fn local_cutoff_captures_the_selected_dst_offset() {
    let cutoff = |offset| Repetition::Until {
        local_date_time: "2026-11-01T01:30".into(),
        time_zone: "America/New_York".into(),
        utc_offset_minutes: offset,
    };
    let early = cutoff(-240).end_at().unwrap().unwrap();
    let late = cutoff(-300).end_at().unwrap().unwrap();
    assert_eq!(
        late - early,
        3_600_000,
        "repeated wall time must retain its selected offset"
    );
    assert_eq!(
        chrono::DateTime::from_timestamp_millis(early)
            .unwrap()
            .to_rfc3339(),
        "2026-11-01T05:30:00+00:00"
    );
    assert!(Repetition::Until {
        local_date_time: "2026-02-30T10:00".into(),
        time_zone: "UTC".into(),
        utc_offset_minutes: 0
    }
    .end_at()
    .is_err());
}

#[test]
fn offline_intervals_keep_one_tick_and_never_burst_replay() {
    let (mut task, _, _) = fixture();
    assert_eq!(
        engine::plan(&mut task, START + 600_000, false).unwrap(),
        Decision::Wait
    );
    assert_eq!(task.missed_count, 9);
    assert_eq!(task.next_run_at, Some(START + 600_000));
    engine::plan(&mut task, START + 600_000, false).unwrap();
    assert_eq!(
        task.missed_count, 9,
        "same poll cannot double-count missed ticks"
    );
    assert_eq!(
        engine::plan(&mut task, START + 600_001, true).unwrap(),
        Decision::Deliver {
            due_at: START + 600_000
        }
    );
    assert_eq!(task.next_run_at, Some(START + 660_000));
}

#[test]
fn unconfirmed_delivery_coalesces_ticks_and_blocks_duplicate_send() {
    let (mut task, owner, _) = fixture();
    let occurrence = prepare(&mut task, &owner, START + 60_000);
    assert_eq!(
        engine::plan(&mut task, START + 360_000, true).unwrap(),
        Decision::Reconcile
    );
    assert_eq!(task.missed_count, 5);
    assert_eq!(task.in_flight.as_ref().unwrap().id, occurrence);
    assert_eq!(task.next_run_at, Some(START + 420_000));
}

#[test]
fn failed_transport_does_not_consume_count_and_duplicate_ack_is_idempotent() {
    let (mut task, owner, _) = fixture();
    task.input.repetition = Repetition::Count { count: 1 };
    prepare(&mut task, &owner, START + 60_000);
    let event_id = task.in_flight.as_ref().unwrap().event.id.to_hex();
    engine::delivery_error(&mut task, "relay unavailable".into(), START + 60_000);
    assert_eq!(task.delivered_count, 0);
    engine::acknowledge(&mut task, &event_id, START + 90_000).unwrap();
    engine::acknowledge(&mut task, &event_id, START + 91_000).unwrap();
    assert_eq!(task.delivered_count, 1);
    assert!(engine::acknowledge(&mut task, &"e".repeat(64), START).is_err());
    assert_eq!(task.status, TaskStatus::Completed);
    assert_eq!(
        engine::plan(&mut task, START + 200_000, true).unwrap(),
        Decision::Wait
    );
}

#[test]
fn sqlite_restart_retains_exact_signed_event_and_ack_accounting() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("schedules.sqlite3");
    let (mut task, owner, _) = fixture();
    prepare(&mut task, &owner, START + 60_000);
    let signed = task.in_flight.as_ref().unwrap().event.as_json();
    let event_id = task.in_flight.as_ref().unwrap().event.id.to_hex();
    Store::open(&path).unwrap().save(&task).unwrap();
    let store = Store::open(&path).unwrap();
    let mut restored = store.get(&task.id).unwrap();
    assert_eq!(restored.in_flight.as_ref().unwrap().event.as_json(), signed);
    assert_eq!(
        engine::plan(&mut restored, START + 3_600_000, true).unwrap(),
        Decision::Reconcile
    );
    engine::acknowledge(&mut restored, &event_id, START + 3_600_000).unwrap();
    store.save(&restored).unwrap();
    drop(store);
    let mut twice = Store::open(&path).unwrap().get(&task.id).unwrap();
    engine::acknowledge(&mut twice, &event_id, START + 3_660_000).unwrap();
    assert_eq!(twice.delivered_count, 1);
}

#[test]
fn pause_resume_and_cancel_preserve_unconfirmed_transport_without_replay() {
    let (mut task, owner, _) = fixture();
    let occurrence = prepare(&mut task, &owner, START + 60_000);
    engine::set_status(&mut task, TaskStatus::Paused, START + 70_000).unwrap();
    engine::delivery_error(
        &mut task,
        "Network temporarily unavailable".into(),
        START + 80_000,
    );
    assert_eq!(task.next_run_at, None);
    assert_eq!(
        engine::plan(&mut task, START + 1_000_000, true).unwrap(),
        Decision::Reconcile
    );
    engine::set_status(&mut task, TaskStatus::Active, START + 1_000_000).unwrap();
    assert_eq!(task.next_run_at, Some(START + 1_060_000));
    assert_eq!(task.retry_at, 0, "manual resume clears transport backoff");
    engine::set_status(&mut task, TaskStatus::Cancelled, START + 1_000_001).unwrap();
    assert_eq!(task.in_flight.as_ref().unwrap().id, occurrence);
    assert!(engine::set_status(&mut task, TaskStatus::Active, START + 1_000_002).is_err());
}

#[test]
fn cutoff_is_inclusive_and_stops_future_delivery() {
    let (mut task, _, _) = fixture();
    let end = START + 60_000;
    task.input.repetition = Repetition::Until {
        local_date_time: chrono::DateTime::from_timestamp_millis(end)
            .unwrap()
            .format("%Y-%m-%dT%H:%M")
            .to_string(),
        time_zone: "UTC".into(),
        utc_offset_minutes: 0,
    };
    task.input.validate(START).unwrap();
    assert!(!task.exhausted(end).unwrap());
    assert_eq!(
        engine::plan(&mut task, end + 1, true).unwrap(),
        Decision::Wait
    );
    assert_eq!(task.status, TaskStatus::Completed);
}

#[test]
fn exact_instruction_routes_into_one_thread_and_setup_root_cannot_trigger_agent() {
    let (mut task, owner, _) = fixture();
    prepare(&mut task, &owner, START + 60_000);
    let event = &task.in_flight.as_ref().unwrap().event;
    assert_eq!(event.content, task.input.instruction);
    assert!(!task
        .root_event
        .tags
        .iter()
        .any(|tag| tag.as_slice()[0] == "p"));
    assert!(event
        .tags
        .iter()
        .any(|tag| tag.as_slice() == ["e", &task.thread_id, "", "reply"]));
    assert!(event
        .tags
        .iter()
        .any(|tag| tag.as_slice() == ["p", &task.input.recipient_pubkey]));
    assert!(event.verify().is_ok());
}

#[test]
fn confirmed_delivery_allows_next_interval_without_any_agent_response() {
    let (mut task, owner, _) = fixture();
    prepare(&mut task, &owner, START + 60_000);
    let first = task.in_flight.as_ref().unwrap().event.id.to_hex();
    engine::acknowledge(&mut task, &first, START + 60_001).unwrap();
    assert!(task.in_flight.is_none());
    assert_eq!(task.delivered_count, 1);
    assert_eq!(
        engine::plan(&mut task, START + 119_999, true).unwrap(),
        Decision::Wait
    );
    assert!(matches!(
        engine::plan(&mut task, START + 120_000, true).unwrap(),
        Decision::Deliver { .. }
    ));
}

#[test]
fn tenant_identity_fences_and_edit_preserve_signed_instruction() {
    let (mut task, owner, _) = fixture();
    assert!(task.authorize(&task.owner_pubkey, &task.relay_url).is_ok());
    assert!(task.authorize(&"a".repeat(64), &task.relay_url).is_err());
    assert!(task
        .authorize(&task.owner_pubkey, "http://another-community")
        .is_err());
    prepare(&mut task, &owner, START + 60_000);
    let original = task.in_flight.as_ref().unwrap().event.content.clone();
    let mut input = task.input.clone();
    input.instruction = "New future instruction".into();
    engine::update(&mut task, input, START + 61_000).unwrap();
    assert_eq!(task.in_flight.as_ref().unwrap().event.content, original);
    let wire = serde_json::to_value(&task).unwrap();
    assert!(wire.get("input").is_none());
    assert_eq!(wire["instruction"], "New future instruction");
}

#[test]
fn repeated_delivery_failure_has_bounded_backoff_and_remains_visible() {
    let (mut task, _, _) = fixture();
    for attempt in 1..=8 {
        engine::delivery_error(&mut task, "Cannot reach relay".into(), START);
        assert!(task.retry_at > START);
        if attempt < 8 {
            assert_eq!(task.status, TaskStatus::Active);
        }
    }
    assert_eq!(task.status, TaskStatus::Active);
    assert!(task.retry_at <= START + 900_000);
    assert_eq!(task.delivered_count, 0);
    assert!(task.last_error.is_some());
}
