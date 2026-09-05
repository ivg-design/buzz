use super::{
    engine::{self, Decision},
    events,
    store::Store,
    types::*,
    TimedTasksState,
};
use crate::{app_state::AppState, relay};
use nostr::{Event, Keys};
use std::{sync::atomic::Ordering, time::Duration};
use tauri::{Emitter, Manager};

/// Initialize disk storage and one bounded publisher loop; setup itself does not send instructions.
pub fn start(app: &tauri::AppHandle) -> Result<(), String> {
    let directory = app.path().app_data_dir().map_err(|e| e.to_string())?;
    std::fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
    let store = Store::open(&directory.join("timed-tasks.sqlite3"))?;
    *app.state::<TimedTasksState>()
        .store
        .lock()
        .map_err(|e| e.to_string())? = Some(store);
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if app
                .state::<AppState>()
                .shutdown_started
                .load(Ordering::Acquire)
            {
                break;
            }
            // Keep storage available through identity recovery, but never sign as an
            // ephemeral recovery identity. Importing/unlocking resumes ordinary polls.
            if app.state::<AppState>().signing_keys().is_err() {
                continue;
            }
            if let Err(error) = tick(&app).await {
                tracing::warn!(%error, "timed task scheduler could not poll");
            }
        }
    });
    Ok(())
}

pub(super) async fn authorize_destination(
    state: &AppState,
    keys: &Keys,
    relay: &str,
    input: &TaskInput,
) -> Result<(), String> {
    let members = relay::query_relay_at_with_keys(
        state,
        relay,
        &[serde_json::json!({
            "kinds": [39002], "#d": [&input.channel_id], "limit": 1,
        })],
        keys,
        None,
    )
    .await?;
    let event = members.first().ok_or("channel membership is unavailable")?;
    for key in [&keys.public_key().to_hex(), &input.recipient_pubkey] {
        if !event.tags.iter().any(|tag| {
            let parts = tag.as_slice();
            parts.len() >= 2 && parts[0] == "p" && &parts[1] == key
        }) {
            return Err(
                "both the scheduling identity and recipient must be channel members".into(),
            );
        }
    }
    for origin in input
        .origin_event_id
        .iter()
        .chain(input.thread_root_id.iter())
    {
        let events = relay::query_relay_at_with_keys(
            state,
            relay,
            &[serde_json::json!({
                "ids": [origin], "kinds": [9, 1, 11, 40002, 45001, 45002], "#h": [&input.channel_id], "limit": 1,
            })],
            keys,
            None,
        )
        .await?;
        if !events.iter().any(|event| {
            event.id.to_hex() == *origin
                && event.tags.iter().any(|tag| {
                    let parts = tag.as_slice();
                    parts.len() >= 2 && parts[0] == "h" && parts[1] == input.channel_id
                })
        }) {
            return Err("initiating message is not accessible in this channel".into());
        }
    }
    Ok(())
}

async fn online(state: &AppState, keys: &Keys, task: &TimedTask) -> Result<bool, String> {
    let events = relay::query_relay_at_with_keys(
        state,
        &task.relay_url,
        &[serde_json::json!({
            "kinds": [20001], "authors": [&task.input.recipient_pubkey],
        })],
        keys,
        None,
    )
    .await?;
    Ok(events
        .iter()
        .filter(|event| {
            event.pubkey.to_hex() == task.input.recipient_pubkey
                || event.tags.iter().any(|tag| {
                    let p = tag.as_slice();
                    p.len() >= 2 && p[0] == "p" && p[1] == task.input.recipient_pubkey
                })
        })
        .max_by_key(|event| event.created_at)
        .is_some_and(|event| matches!(event.content.trim(), "online" | "away")))
}

async fn publish(
    state: &AppState,
    keys: &Keys,
    task: &TimedTask,
    event: &Event,
) -> Result<(), String> {
    // A timeout can mean the relay accepted the event. Its immutable signed id is retained
    // and queried on the next poll before any retry, never replaced by a new signature.
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        relay::submit_signed_event_at_with_keys(event, state, &task.relay_url, keys),
    )
    .await
    .map_err(|_| "relay delivery timed out; checking the same event before retry")??;
    if result.event_id != event.id.to_hex() {
        return Err("relay did not acknowledge the exact scheduled event".into());
    }
    Ok(())
}

async fn tick(app: &tauri::AppHandle) -> Result<(), String> {
    let state = app.state::<AppState>();
    let scheduler = app.state::<TimedTasksState>();
    let keys = state.signing_keys()?;
    let relay_url = relay::relay_api_base_url_with_override(&state);
    let tasks =
        scheduler.with_store(|store| store.list(&keys.public_key().to_hex(), &relay_url))?;
    for snapshot in tasks {
        if snapshot.status != TaskStatus::Active && snapshot.in_flight.is_none() {
            continue;
        }
        let _operation = scheduler.operation.lock().await;
        // Fence queued async polls against identity/workspace swaps, just as the commands do.
        let current_keys = state.signing_keys()?;
        snapshot.authorize(
            &current_keys.public_key().to_hex(),
            &relay::relay_api_base_url_with_override(&state),
        )?;
        let mut task = scheduler.with_store(|store| store.get(&snapshot.id))?;
        let now = chrono::Utc::now().timestamp_millis();
        if now < task.retry_at {
            continue;
        }
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            tick_task(&state, &scheduler, &keys, &mut task, now),
        )
        .await
        .map_err(|_| "scheduler poll timed out; durable delivery will be reconciled".to_string())
        .and_then(|result| result);
        if let Err(error) = result {
            engine::delivery_error(&mut task, error, now);
        }
        scheduler.save(&task)?;
        app.emit("timed-tasks-changed", &task.id)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub(super) async fn tick_task(
    state: &AppState,
    scheduler: &TimedTasksState,
    keys: &Keys,
    task: &mut TimedTask,
    now: i64,
) -> Result<(), String> {
    let started = std::time::Instant::now();
    if task.in_flight.is_some() {
        engine::plan(task, now, false)?;
        return reconcile(state, scheduler, keys, task, now).await;
    }
    if task.status != TaskStatus::Active {
        return Ok(());
    }
    if !task.root_published {
        authorize_destination(state, keys, &task.relay_url, &task.input).await?;
        ensure_root(state, scheduler, keys, task).await?;
    }
    if task.next_run_at.is_none_or(|next| next > now) {
        return Ok(());
    }
    let is_online = online(state, keys, task).await?;
    // A failed authority/query check must not consume the due slot.
    if is_online {
        authorize_destination(state, keys, &task.relay_url, &task.input).await?;
    }
    let now = after_io(now, started);
    let Decision::Deliver { due_at } = engine::plan(task, now, is_online)? else {
        return Ok(());
    };
    let id = uuid::Uuid::new_v4().to_string();
    let event = events::occurrence(task, &id, keys)?;
    task.in_flight = Some(Occurrence {
        id,
        event,
        due_at,
        attempts: 0,
    });
    scheduler.save(task)?;
    publish_pending(state, scheduler, keys, task, now).await
}

async fn publish_pending(
    state: &AppState,
    scheduler: &TimedTasksState,
    keys: &Keys,
    task: &mut TimedTask,
    now: i64,
) -> Result<(), String> {
    let started = std::time::Instant::now();
    ensure_root(state, scheduler, keys, task).await?;
    let now = after_io(now, started);
    if task.exhausted(now)? {
        task.status = TaskStatus::Completed;
        task.next_run_at = None;
        if task
            .in_flight
            .as_ref()
            .is_some_and(|item| item.attempts == 0)
        {
            task.in_flight = None;
            task.delivery_state = DeliveryState::Idle;
        }
        return Ok(());
    }
    let pending = task.in_flight.as_mut().ok_or("missing pending delivery")?;
    pending.attempts = pending.attempts.saturating_add(1);
    let event = pending.event.clone();
    scheduler.save(task)?; // Crash after POST remains distinguishable from an unsent event.
    publish(state, keys, task, &event).await?;
    engine::acknowledge(task, &event.id.to_hex(), now)
}

async fn ensure_root(
    state: &AppState,
    scheduler: &TimedTasksState,
    keys: &Keys,
    task: &mut TimedTask,
) -> Result<(), String> {
    if !task.root_published {
        // A lost root ACK must not wedge the schedule on a duplicate-event response.
        let found = relay::query_relay_at_with_keys(
            state,
            &task.relay_url,
            &[serde_json::json!({
                "ids": [task.root_event.id.to_hex()], "kinds": [9], "#h": [&task.input.channel_id], "limit": 1,
            })],
            keys,
            None,
        )
        .await?;
        if !found
            .iter()
            .any(|event| event.id == task.root_event.id && event.verify().is_ok())
        {
            publish(state, keys, task, &task.root_event).await?;
        }
        task.root_published = true;
        scheduler.save(task)?;
    }
    Ok(())
}

async fn reconcile(
    state: &AppState,
    scheduler: &TimedTasksState,
    keys: &Keys,
    task: &mut TimedTask,
    now: i64,
) -> Result<(), String> {
    let started = std::time::Instant::now();
    let pending = task
        .in_flight
        .as_ref()
        .ok_or("missing pending delivery")?
        .clone();
    let events = relay::query_relay_at_with_keys(state, &task.relay_url, &[
        serde_json::json!({"ids": [pending.event.id.to_hex()], "kinds": [9], "#h": [&task.input.channel_id], "limit": 1}),
    ], keys, None).await?;
    if events
        .iter()
        .any(|event| event.id == pending.event.id && event.verify().is_ok())
    {
        return engine::acknowledge(task, &pending.event.id.to_hex(), after_io(now, started));
    }
    let now = after_io(now, started);
    if task.exhausted(now)? && task.status == TaskStatus::Active {
        task.status = TaskStatus::Completed;
        task.next_run_at = None;
        task.last_error = Some(
            "Schedule ended; an unconfirmed delivery will be checked without resending.".into(),
        );
    }
    if task.status != TaskStatus::Active {
        return Ok(());
    }
    if !online(state, keys, task).await? {
        task.delivery_state = DeliveryState::WaitingOffline;
        return Ok(());
    }
    authorize_destination(state, keys, &task.relay_url, &task.input).await?;
    publish_pending(state, scheduler, keys, task, now).await
}

fn after_io(now: i64, started: std::time::Instant) -> i64 {
    now.saturating_add(i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX))
}
