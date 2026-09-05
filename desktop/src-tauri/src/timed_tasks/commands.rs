use super::{engine, events, types::*, TimedTasksState};
use crate::{app_state::AppState, relay};
use tauri::State;

fn scope(
    state: &AppState,
    relay_url: Option<&str>,
    signer: Option<&str>,
) -> Result<(nostr::Keys, String), String> {
    let relay = relay::relay_api_base_url_with_override(state);
    relay::assert_expected_relay_scope(relay_url, &relay)?;
    let keys = state.signing_keys()?;
    relay::assert_expected_signer(signer, &keys.public_key().to_hex())?;
    Ok((keys, relay))
}

/// List only schedules owned by the current authenticated identity in this community.
#[tauri::command]
pub async fn timed_tasks_list(
    recipient_pubkey: Option<String>,
    channel_id: Option<String>,
    state: State<'_, AppState>,
    scheduler: State<'_, TimedTasksState>,
) -> Result<Vec<TimedTask>, String> {
    let (keys, relay) = scope(&state, None, None)?;
    let mut tasks =
        scheduler.with_store(|store| store.list(&keys.public_key().to_hex(), &relay))?;
    tasks.retain(|task| {
        recipient_pubkey
            .as_ref()
            .is_none_or(|key| key == &task.input.recipient_pubkey)
            && channel_id
                .as_ref()
                .is_none_or(|id| id == &task.input.channel_id)
    });
    Ok(tasks)
}

/// Save a schedule without executing its instruction. The visible root is published before delivery.
#[tauri::command]
pub async fn timed_tasks_create(
    input: TaskInput,
    expected_relay_url: Option<String>,
    expected_signer_pubkey: Option<String>,
    state: State<'_, AppState>,
    scheduler: State<'_, TimedTasksState>,
) -> Result<TimedTask, String> {
    let _operation = scheduler.operation.lock().await;
    let now = chrono::Utc::now().timestamp_millis();
    input.validate(now)?;
    let (keys, relay) = scope(
        &state,
        expected_relay_url.as_deref(),
        expected_signer_pubkey.as_deref(),
    )?;
    super::runtime::authorize_destination(&state, &keys, &relay, &input).await?;
    if scheduler
        .with_store(|store| store.list(&keys.public_key().to_hex(), &relay))?
        .len()
        >= 1000
    {
        return Err("maximum of 1000 saved timed tasks reached".into());
    }
    let id = uuid::Uuid::new_v4().to_string();
    let root_event = events::root(&id, &input, &keys, &relay)?;
    let thread_id = input
        .thread_root_id
        .clone()
        .unwrap_or_else(|| root_event.id.to_hex());
    let task = TimedTask {
        next_run_at: Some(now + input.interval.millis()?),
        id,
        input,
        owner_pubkey: keys.public_key().to_hex(),
        relay_url: relay,
        thread_id,
        root_event,
        root_published: false,
        status: TaskStatus::Active,
        created_at: now,
        updated_at: now,
        last_delivered_at: None,
        last_delivered_event_id: None,
        delivered_count: 0,
        missed_count: 0,
        last_error: None,
        delivery_state: DeliveryState::Idle,
        in_flight: None,
        retry_at: 0,
        consecutive_failures: 0,
    };
    scheduler.save(&task)?;
    Ok(task)
}

/// Edit future occurrences; any already-signed occurrence retains its exact original instruction.
#[tauri::command]
pub async fn timed_tasks_update(
    id: String,
    input: TaskInput,
    expected_relay_url: Option<String>,
    expected_signer_pubkey: Option<String>,
    state: State<'_, AppState>,
    scheduler: State<'_, TimedTasksState>,
) -> Result<TimedTask, String> {
    let _operation = scheduler.operation.lock().await;
    let (keys, relay) = scope(
        &state,
        expected_relay_url.as_deref(),
        expected_signer_pubkey.as_deref(),
    )?;
    let mut task = scheduler.with_store(|store| store.get(&id))?;
    task.authorize(&keys.public_key().to_hex(), &relay)?;
    super::runtime::authorize_destination(&state, &keys, &relay, &input).await?;
    let destination_changed = input.channel_id != task.input.channel_id
        || input.thread_root_id != task.input.thread_root_id
        || input.post_to_channel != task.input.post_to_channel;
    if destination_changed && task.in_flight.is_some() {
        return Err("Wait for the pending delivery before changing its destination".into());
    }
    if destination_changed {
        task.root_event = events::root(&task.id, &input, &keys, &relay)?;
        task.thread_id = input
            .thread_root_id
            .clone()
            .unwrap_or_else(|| task.root_event.id.to_hex());
        task.root_published = false;
    }
    engine::update(&mut task, input, chrono::Utc::now().timestamp_millis())?;
    scheduler.save(&task)?;
    Ok(task)
}

/// Pause/resume/cancel future delivery. Cancellation does not stop a recipient's already-delivered work.
#[tauri::command]
pub async fn timed_tasks_set_status(
    id: String,
    status: TaskStatus,
    expected_relay_url: Option<String>,
    expected_signer_pubkey: Option<String>,
    state: State<'_, AppState>,
    scheduler: State<'_, TimedTasksState>,
) -> Result<TimedTask, String> {
    let _operation = scheduler.operation.lock().await;
    let (keys, relay) = scope(
        &state,
        expected_relay_url.as_deref(),
        expected_signer_pubkey.as_deref(),
    )?;
    let mut task = scheduler.with_store(|store| store.get(&id))?;
    task.authorize(&keys.public_key().to_hex(), &relay)?;
    engine::set_status(&mut task, status, chrono::Utc::now().timestamp_millis())?;
    scheduler.save(&task)?;
    Ok(task)
}
