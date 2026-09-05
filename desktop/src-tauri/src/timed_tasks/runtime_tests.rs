//! Exercise the production publisher against disposable authenticated HTTP conversations.
use super::{runtime::tick_task, store::Store, tests::fixture, types::*, TimedTasksState};
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use nostr::{Event, EventBuilder, Keys, Kind, Tag};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct RelayFixture {
    owner: Keys,
    agent: Keys,
    channel: String,
    events: Arc<Mutex<Vec<Event>>>,
    // Simulates accepted relay storage followed by a failed HTTP acknowledgement.
    lose_ack: Arc<Mutex<bool>>,
    member: Arc<Mutex<bool>>,
}

async fn query(
    State(fixture): State<RelayFixture>,
    Json(filters): Json<Vec<Value>>,
) -> Json<Vec<Event>> {
    let mut results = Vec::new();
    for filter in filters {
        let kinds = filter["kinds"]
            .as_array()
            .expect("production query must explicitly scope kinds");
        if kinds.contains(&json!(39002)) {
            if *fixture.member.lock().unwrap() {
                results.push(
                    EventBuilder::new(Kind::Custom(39002), "")
                        .allow_self_tagging()
                        .tags([
                            Tag::parse(["d", &fixture.channel]).unwrap(),
                            Tag::parse(["p", &fixture.owner.public_key().to_hex()]).unwrap(),
                            Tag::parse(["p", &fixture.agent.public_key().to_hex()]).unwrap(),
                        ])
                        .sign_with_keys(&fixture.owner)
                        .unwrap(),
                );
            }
        } else if kinds.contains(&json!(20001)) {
            results.push(
                EventBuilder::new(Kind::Custom(20001), "online")
                    .sign_with_keys(&fixture.agent)
                    .unwrap(),
            );
        } else {
            assert_eq!(filter["#h"], json!([fixture.channel]));
            for event in fixture.events.lock().unwrap().iter() {
                let id_matches = filter
                    .get("ids")
                    .is_none_or(|ids| ids.as_array().unwrap().contains(&json!(event.id.to_hex())));
                let author_matches = filter.get("authors").is_none_or(|authors| {
                    authors
                        .as_array()
                        .unwrap()
                        .contains(&json!(event.pubkey.to_hex()))
                });
                if id_matches && author_matches {
                    results.push(event.clone());
                }
            }
        }
    }
    Json(results)
}

async fn publish(
    State(fixture): State<RelayFixture>,
    Json(event): Json<Event>,
) -> (StatusCode, Json<Value>) {
    event.verify().unwrap();
    let occurrence = event.tags.iter().any(|tag| {
        let t = tag.as_slice();
        t.len() > 1 && t[0] == "buzz-task" && t[1] == "scheduled"
    });
    let mut saved = fixture.events.lock().unwrap();
    if occurrence {
        let parent = event
            .tags
            .iter()
            .find(|tag| tag.as_slice()[0] == "e")
            .unwrap();
        assert!(
            saved
                .iter()
                .any(|item| item.id.to_hex() == parent.as_slice()[1]),
            "visible root must exist before the instruction"
        );
    }
    assert!(
        !saved.iter().any(|item| item.id == event.id),
        "already accepted signed occurrence should be found, not republished"
    );
    saved.push(event.clone());
    if occurrence && *fixture.lose_ack.lock().unwrap() {
        *fixture.lose_ack.lock().unwrap() = false;
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "lost acknowledgement"})),
        );
    }
    (
        StatusCode::OK,
        Json(json!({"event_id": event.id.to_hex(), "accepted": true, "message": "stored"})),
    )
}

#[tokio::test]
async fn real_publisher_persists_before_send_and_recovers_lost_ack_without_duplicate_work() {
    let _serial = crate::relay_admission::TEST_SERIAL.lock().await;
    crate::relay_admission::reset_rate_limit_gate();
    let (mut task, owner, agent) = fixture();
    let fixture = RelayFixture {
        owner: owner.clone(),
        agent: agent.clone(),
        channel: task.input.channel_id.clone(),
        events: Arc::default(),
        lose_ack: Arc::new(Mutex::new(true)),
        member: Arc::new(Mutex::new(true)),
    };
    let router = Router::new()
        .route("/query", post(query))
        .route("/events", post(publish))
        .with_state(fixture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    task.relay_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let state = crate::app_state::build_app_state();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("test.sqlite3");
    let scheduler = TimedTasksState::default();
    *scheduler.store.lock().unwrap() = Some(Store::open(&path).unwrap());
    scheduler.save(&task).unwrap();
    let due = task.next_run_at.unwrap();

    // Setup is visible before the first interval, and cannot mention/trigger the agent.
    tick_task(&state, &scheduler, &owner, &mut task, due - 1)
        .await
        .unwrap();
    assert!(task.root_published);
    assert_eq!(fixture.events.lock().unwrap().len(), 1);
    assert!(!fixture.events.lock().unwrap()[0]
        .tags
        .iter()
        .any(|tag| tag.as_slice()[0] == "p"));
    assert!(tick_task(&state, &scheduler, &owner, &mut task, due)
        .await
        .is_err());
    let persisted = scheduler.with_store(|store| store.get(&task.id)).unwrap();
    assert_eq!(persisted.in_flight.as_ref().unwrap().attempts, 1);
    assert_eq!(
        persisted.delivered_count, 0,
        "failed ACK cannot count as delivery"
    );
    assert_eq!(fixture.events.lock().unwrap().len(), 2);

    // Reopen the actual journal as a restart; reconcile the exact stored event without another POST.
    *scheduler.store.lock().unwrap() = Some(Store::open(&path).unwrap());
    task = persisted;
    tick_task(&state, &scheduler, &owner, &mut task, due + 600_000)
        .await
        .unwrap();
    assert_eq!(task.delivered_count, 1);
    assert_eq!(fixture.events.lock().unwrap().len(), 2);
    assert!(task.missed_count >= 9);
    assert_eq!(task.delivery_state, DeliveryState::Delivered);
    assert!(task.in_flight.is_none());
    // No recipient result/receipt is needed. The next ordinary interval delivers again.
    let next = task.next_run_at.unwrap();
    tick_task(&state, &scheduler, &owner, &mut task, next)
        .await
        .unwrap();
    assert_eq!(task.delivered_count, 2);
    assert_eq!(fixture.events.lock().unwrap().len(), 3);
    assert!(task.in_flight.is_none());
    server.abort();
    crate::relay_admission::reset_rate_limit_gate();
}

#[tokio::test]
async fn destination_failure_does_not_consume_due_slot() {
    let _serial = crate::relay_admission::TEST_SERIAL.lock().await;
    crate::relay_admission::reset_rate_limit_gate();
    let (mut task, owner, agent) = fixture();
    let fixture = RelayFixture {
        owner: owner.clone(),
        agent,
        channel: task.input.channel_id.clone(),
        events: Arc::default(),
        lose_ack: Arc::new(Mutex::new(false)),
        member: Arc::new(Mutex::new(false)),
    };
    let router = Router::new()
        .route("/query", post(query))
        .with_state(fixture);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    task.relay_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let state = crate::app_state::build_app_state();
    let due = task.next_run_at;
    assert!(tick_task(
        &state,
        &TimedTasksState::default(),
        &owner,
        &mut task,
        due.unwrap()
    )
    .await
    .is_err());
    assert_eq!(task.next_run_at, due);
    assert!(task.in_flight.is_none());
    assert_eq!(task.delivered_count, 0);
    server.abort();
    crate::relay_admission::reset_rate_limit_gate();
}
