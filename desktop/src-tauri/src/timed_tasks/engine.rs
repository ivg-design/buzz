//! Clock-driven transitions shared by production polling and deterministic tests.

use super::types::*;

#[derive(Debug, PartialEq)]
pub enum Decision {
    Wait,
    Reconcile,
    Deliver { due_at: i64 },
}

/// Advance elapsed intervals without replaying a burst after sleep or a busy recipient.
pub fn plan(task: &mut TimedTask, now: i64, online: bool) -> Result<Decision, String> {
    if task.in_flight.is_some() {
        if task.status == TaskStatus::Active {
            let count_reached = matches!(&task.input.repetition, Repetition::Count { count } if task.delivered_count >= *count);
            if !count_reached {
                let through = task
                    .input
                    .repetition
                    .end_at()?
                    .map_or(now, |end| now.min(end));
                coalesce(task, through, false)?;
            }
            if task.exhausted(now)? {
                task.next_run_at = None;
            }
        }
        return Ok(Decision::Reconcile);
    }
    if task.status != TaskStatus::Active {
        return Ok(Decision::Wait);
    }
    if task.exhausted(now)? {
        task.status = TaskStatus::Completed;
        task.next_run_at = None;
        return Ok(Decision::Wait);
    }
    if task.next_run_at.is_none_or(|next| next > now) {
        return Ok(Decision::Wait);
    }
    if !online {
        task.delivery_state = DeliveryState::WaitingOffline;
        // Keep one overdue slot, and account for older slots exactly once.
        coalesce(task, now, true)?;
        return Ok(Decision::Wait);
    }
    let due_at = task.next_run_at.ok_or("missing next delivery time")?;
    coalesce(task, now, false)?;
    task.delivery_state = DeliveryState::Pending;
    Ok(Decision::Deliver { due_at })
}

fn coalesce(task: &mut TimedTask, now: i64, keep_one: bool) -> Result<(), String> {
    let Some(next) = task.next_run_at.filter(|next| *next <= now) else {
        return Ok(());
    };
    let interval = task.input.interval.millis()?;
    let ticks = (now - next) / interval + 1;
    let held = i64::from(keep_one);
    // A newly delivered slot accounts for one tick; all ticks while an occurrence
    // is outstanding are missed. Offline retains the most recent overdue tick.
    let skipped = if keep_one || task.in_flight.is_none() {
        ticks - 1
    } else {
        ticks
    };
    task.missed_count = task.missed_count.saturating_add(skipped as u64);
    task.next_run_at = Some(
        next.checked_add((ticks - held) * interval)
            .ok_or("schedule overflow")?,
    );
    Ok(())
}

/// Count exact relay delivery once, then release the occurrence to the ordinary recipient queue.
/// A recipient response is never required before a later interval can deliver another instruction.
pub fn acknowledge(task: &mut TimedTask, event_id: &str, now: i64) -> Result<(), String> {
    if task.last_delivered_event_id.as_deref() == Some(event_id) {
        return Ok(());
    }
    let pending = task.in_flight.as_ref().ok_or("no pending occurrence")?;
    if pending.event.id.to_hex() != event_id {
        return Err("stale delivery acknowledgement".into());
    }
    task.delivered_count = task.delivered_count.saturating_add(1);
    task.last_delivered_at = Some(now);
    task.last_delivered_event_id = Some(event_id.into());
    task.in_flight = None;
    task.delivery_state = DeliveryState::Delivered;
    task.last_error = None;
    task.consecutive_failures = 0;
    task.retry_at = 0;
    task.updated_at = now;
    if task.status == TaskStatus::Active && task.exhausted(now)? {
        task.status = TaskStatus::Completed;
        task.next_run_at = None;
    }
    Ok(())
}

pub fn set_status(task: &mut TimedTask, status: TaskStatus, now: i64) -> Result<(), String> {
    if status == TaskStatus::Completed {
        return Err("completion is managed by the scheduler".into());
    }
    if task.status == TaskStatus::Cancelled {
        return Err("cancelled task cannot be changed".into());
    }
    if status == TaskStatus::Active {
        if task.exhausted(now)? {
            return Err(
                "schedule count or end time has already been reached; edit it first".into(),
            );
        }
        task.next_run_at = Some(
            now.checked_add(task.input.interval.millis()?)
                .ok_or("schedule overflow")?,
        );
        task.retry_at = 0;
        task.consecutive_failures = 0;
    } else {
        task.next_run_at = None;
        if task
            .in_flight
            .as_ref()
            .is_some_and(|item| item.attempts == 0)
        {
            task.in_flight = None;
            task.delivery_state = DeliveryState::Idle;
        }
    }
    task.status = status;
    task.updated_at = now;
    Ok(())
}

pub fn update(task: &mut TimedTask, input: TaskInput, now: i64) -> Result<(), String> {
    input.validate(now)?;
    if task.status == TaskStatus::Cancelled {
        return Err("cancelled task cannot be edited".into());
    }
    if input.recipient_pubkey != task.input.recipient_pubkey {
        return Err("recipient cannot be changed in this agent's schedule".into());
    }
    let timing_changed =
        input.interval != task.input.interval || input.repetition != task.input.repetition;
    task.input = input;
    task.updated_at = now;
    task.retry_at = 0;
    task.consecutive_failures = 0;
    if task.status == TaskStatus::Active && timing_changed {
        task.next_run_at = Some(
            now.checked_add(task.input.interval.millis()?)
                .ok_or("schedule overflow")?,
        );
    }
    Ok(())
}

/// Persist bounded backoff and the latest failure without silently disabling the user's schedule.
pub fn delivery_error(task: &mut TimedTask, error: String, now: i64) {
    task.consecutive_failures = task.consecutive_failures.saturating_add(1);
    let delay = 30_000 * (1_i64 << task.consecutive_failures.min(5));
    task.retry_at = now.saturating_add(delay.min(900_000));
    task.last_error = Some(error.chars().take(500).collect());
    task.updated_at = now;
}
