//! Durable schedule model. All timestamps crossing the UI bridge are epoch milliseconds.

use chrono::{FixedOffset, NaiveDateTime, TimeZone};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Interval {
    pub value: u32,
    pub unit: IntervalUnit,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum IntervalUnit {
    Minutes,
    Hours,
    Days,
}

impl Interval {
    pub fn millis(&self) -> Result<i64, String> {
        if self.value == 0 || self.value > 525_600 {
            return Err("interval must be between 1 and 525600".into());
        }
        let unit = match self.unit {
            IntervalUnit::Minutes => 60_000,
            IntervalUnit::Hours => 3_600_000,
            IntervalUnit::Days => 86_400_000,
        };
        Ok(i64::from(self.value) * unit)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(
    tag = "mode",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum Repetition {
    Forever,
    Count {
        count: u32,
    },
    Until {
        local_date_time: String,
        time_zone: String,
        utc_offset_minutes: i32,
    },
}

impl Repetition {
    /// Resolve the selected wall time with its explicitly captured UTC offset.
    /// The zone is a display label; subsequent DST/system-zone changes do not move the cutoff.
    pub fn end_at(&self) -> Result<Option<i64>, String> {
        match self {
            Self::Forever => Ok(None),
            Self::Count { count } if *count > 0 => Ok(None),
            Self::Count { .. } => Err("repeat count must be positive".into()),
            Self::Until {
                local_date_time,
                time_zone,
                utc_offset_minutes,
            } => {
                if time_zone.is_empty()
                    || time_zone.len() > 100
                    || !(-840..=840).contains(utc_offset_minutes)
                {
                    return Err("valid local timezone and UTC offset are required".into());
                }
                let local = NaiveDateTime::parse_from_str(local_date_time, "%Y-%m-%dT%H:%M")
                    .map_err(|_| "end date and time must be YYYY-MM-DDTHH:mm")?;
                let offset =
                    FixedOffset::east_opt(utc_offset_minutes * 60).ok_or("invalid UTC offset")?;
                let instant = offset
                    .from_local_datetime(&local)
                    .single()
                    .ok_or("invalid local end time")?;
                Ok(Some(instant.timestamp_millis()))
            }
        }
    }
}

/// Editable form fields. Prompt bytes are retained exactly, including surrounding whitespace.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskInput {
    pub recipient_pubkey: String,
    pub channel_id: String,
    pub origin_event_id: Option<String>,
    pub instruction: String,
    pub interval: Interval,
    pub repetition: Repetition,
}

impl TaskInput {
    pub fn validate(&self, now: i64) -> Result<(), String> {
        nostr::PublicKey::from_hex(&self.recipient_pubkey).map_err(|_| "invalid agent identity")?;
        uuid::Uuid::parse_str(&self.channel_id).map_err(|_| "invalid channel identity")?;
        if let Some(origin) = &self.origin_event_id {
            nostr::EventId::from_hex(origin).map_err(|_| "invalid origin event")?;
        }
        if self.instruction.trim().is_empty() || self.instruction.len() > 32_000 {
            return Err("instruction must contain text and be at most 32000 bytes".into());
        }
        let first = now
            .checked_add(self.interval.millis()?)
            .ok_or("interval overflow")?;
        if self.repetition.end_at()?.is_some_and(|end| end < first) {
            return Err("end time must allow at least one interval after saving".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Active,
    Paused,
    Cancelled,
    Completed,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Idle,
    WaitingOffline,
    Pending,
    Delivered,
}

/// One immutable signed delivery. A failed publish retries these exact bytes and event id.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Occurrence {
    pub id: String,
    pub event: nostr::Event,
    pub due_at: i64,
    pub attempts: u32,
}

/// Persisted schedule plus user-visible operational state.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TimedTask {
    pub id: String,
    #[serde(flatten)]
    pub input: TaskInput,
    pub owner_pubkey: String,
    pub relay_url: String,
    pub thread_id: String,
    pub status: TaskStatus,
    pub created_at: i64,
    pub updated_at: i64,
    pub next_run_at: Option<i64>,
    pub last_delivered_at: Option<i64>,
    pub last_delivered_event_id: Option<String>,
    pub delivered_count: u32,
    pub missed_count: u64,
    pub last_error: Option<String>,
    pub delivery_state: DeliveryState,
    pub root_event: nostr::Event,
    pub root_published: bool,
    pub in_flight: Option<Occurrence>,
    pub retry_at: i64,
    pub consecutive_failures: u32,
}

impl TimedTask {
    /// Captured owner and community fence every read and mutation.
    pub fn authorize(&self, owner: &str, relay: &str) -> Result<(), String> {
        if self.owner_pubkey != owner || self.relay_url != relay {
            return Err("timed task belongs to another identity or community".into());
        }
        Ok(())
    }

    pub fn exhausted(&self, now: i64) -> Result<bool, String> {
        Ok(match &self.input.repetition {
            Repetition::Count { count } => self.delivered_count >= *count,
            _ => self.input.repetition.end_at()?.is_some_and(|end| now > end),
        })
    }
}
