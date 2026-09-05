//! Persistent desktop scheduling. The host must be running and connected to deliver work.
//! First delivery/resume waits one interval; days are elapsed 24-hour periods. Local end
//! dates retain their selected UTC offset, including through DST or system-zone changes.

mod commands;
mod engine;
mod events;
mod runtime;
mod store;
mod types;

pub use commands::*;
pub use runtime::start;

use std::sync::Mutex;
use store::Store;
use types::TimedTask;

/// Tauri-managed scheduler state, separate from identity and core job storage.
#[derive(Default)]
pub struct TimedTasksState {
    store: Mutex<Option<Store>>,
    operation: tokio::sync::Mutex<()>,
}

impl TimedTasksState {
    fn with_store<T>(
        &self,
        operation: impl FnOnce(&Store) -> Result<T, String>,
    ) -> Result<T, String> {
        let guard = self.store.lock().map_err(|e| e.to_string())?;
        operation(guard.as_ref().ok_or("timed task storage is unavailable")?)
    }

    fn save(&self, task: &TimedTask) -> Result<(), String> {
        self.with_store(|store| store.save(task))
    }
}

#[cfg(test)]
mod runtime_tests;
#[cfg(test)]
mod tests;
