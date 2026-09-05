//! Private local SQLite journal; the relay remains the authority for message permissions.

use super::types::TimedTask;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

pub struct Store {
    connection: Connection,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self, String> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .mode(0o600)
                .open(path)
                .map_err(|e| e.to_string())?;
        }
        let connection = Connection::open(path).map_err(|e| e.to_string())?;
        connection
            .busy_timeout(std::time::Duration::from_secs(3))
            .map_err(|e| e.to_string())?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS timed_tasks_v1 (
                id TEXT PRIMARY KEY, owner TEXT NOT NULL, relay TEXT NOT NULL, body TEXT NOT NULL
            );",
            )
            .map_err(|e| e.to_string())?;
        Ok(Self { connection })
    }

    pub fn save(&self, task: &TimedTask) -> Result<(), String> {
        let body = serde_json::to_string(task).map_err(|e| e.to_string())?;
        self.connection
            .execute(
                "INSERT INTO timed_tasks_v1 (id,owner,relay,body) VALUES (?1,?2,?3,?4)
             ON CONFLICT(id) DO UPDATE SET body=excluded.body",
                params![task.id, task.owner_pubkey, task.relay_url, body],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn get(&self, id: &str) -> Result<TimedTask, String> {
        let body: Option<String> = self
            .connection
            .query_row("SELECT body FROM timed_tasks_v1 WHERE id=?1", [id], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|e| e.to_string())?;
        serde_json::from_str(&body.ok_or("timed task not found")?).map_err(|e| e.to_string())
    }

    pub fn list(&self, owner: &str, relay: &str) -> Result<Vec<TimedTask>, String> {
        let mut statement = self.connection.prepare(
            "SELECT body FROM timed_tasks_v1 WHERE owner=?1 AND relay=?2 ORDER BY rowid DESC LIMIT 1000",
        ).map_err(|e| e.to_string())?;
        let rows = statement
            .query_map(params![owner, relay], |row| row.get::<_, String>(0))
            .map_err(|e| e.to_string())?;
        rows.map(|row| {
            serde_json::from_str(&row.map_err(|e| e.to_string())?).map_err(|e| e.to_string())
        })
        .collect()
    }
}
