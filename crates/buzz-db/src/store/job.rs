//! Cross-pod serialization fence for agent-job validation and event insert.

use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use buzz_core::{CommunityId, StoredEvent};
use nostr::Event;

use crate::{Db, EventQuery, Result};

/// Open transaction holding a community/operation-scoped advisory lock.
///
/// Dropping the value rolls back the otherwise-empty transaction and releases
/// the lock. Call [`Self::commit`] after the corresponding event insert.
pub struct JobOperationLock {
    transaction: Transaction<'static, Postgres>,
}

impl JobOperationLock {
    /// Query authoritative job history on the locked transaction.
    pub async fn query_events(&mut self, query: &EventQuery) -> Result<Vec<StoredEvent>> {
        crate::event::query_events_on(self.transaction.as_mut(), query).await
    }

    /// Lock and return the current user ownership record for an authorization decision.
    pub async fn user_owner_for_share(
        &mut self,
        community: CommunityId,
        pubkey: &[u8],
    ) -> Result<Option<Option<Vec<u8>>>> {
        let row = sqlx::query(
            "SELECT agent_owner_pubkey FROM users \
             WHERE community_id = $1 AND pubkey = $2 FOR SHARE",
        )
        .bind(community.as_uuid())
        .bind(pubkey)
        .fetch_optional(self.transaction.as_mut())
        .await?;
        row.map(|row| {
            row.try_get("agent_owner_pubkey")
                .map_err(crate::DbError::from)
        })
        .transpose()
    }

    /// Lock an active relay-membership row for the duration of the job insert.
    pub async fn relay_member_for_share(
        &mut self,
        community: CommunityId,
        pubkey: &str,
    ) -> Result<bool> {
        let present = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM relay_members \
             WHERE community_id = $1 AND pubkey = $2 FOR SHARE)",
        )
        .bind(community.as_uuid())
        .bind(pubkey)
        .fetch_one(self.transaction.as_mut())
        .await?;
        Ok(present)
    }

    /// Lock an active direct channel-membership row for the job write.
    pub async fn channel_member_for_share(
        &mut self,
        community: CommunityId,
        channel_id: Uuid,
        pubkey: &[u8],
    ) -> Result<bool> {
        let present = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM channel_members \
             WHERE community_id = $1 AND channel_id = $2 AND pubkey = $3 \
               AND removed_at IS NULL FOR SHARE)",
        )
        .bind(community.as_uuid())
        .bind(channel_id)
        .bind(pubkey)
        .fetch_one(self.transaction.as_mut())
        .await?;
        Ok(present)
    }

    /// Lock the current live NIP-33 row so replacement/deletion cannot race validation.
    pub async fn lock_parameterized_head(
        &mut self,
        community: CommunityId,
        kind: i32,
        pubkey: &[u8],
        d_tag: &str,
    ) -> Result<Option<Vec<u8>>> {
        let rows = sqlx::query(
            "SELECT id FROM events WHERE community_id = $1 AND kind = $2 \
             AND pubkey = $3 AND d_tag = $4 AND deleted_at IS NULL FOR SHARE",
        )
        .bind(community.as_uuid())
        .bind(kind)
        .bind(pubkey)
        .bind(d_tag)
        .fetch_all(self.transaction.as_mut())
        .await?;
        if rows.len() > 1 {
            return Err(crate::DbError::InvalidData(
                "parameterized coordinate resolved to multiple live heads".into(),
            ));
        }
        rows.into_iter()
            .next()
            .map(|row| row.try_get("id").map_err(crate::DbError::from))
            .transpose()
    }

    /// Insert the protected job event on the same locked transaction.
    pub async fn insert_event(
        &mut self,
        community: CommunityId,
        event: &Event,
        channel_id: Uuid,
    ) -> Result<(StoredEvent, bool)> {
        crate::event::insert_event_with_thread_metadata_tx(
            &mut self.transaction,
            community,
            event,
            Some(channel_id),
            None,
        )
        .await
    }

    /// Release the lock after the protected insert becomes visible.
    pub async fn commit(self) -> Result<()> {
        self.transaction.commit().await?;
        Ok(())
    }
}

async fn acquire(
    pool: &PgPool,
    community: CommunityId,
    lock_domains: &[String],
) -> Result<JobOperationLock> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SET LOCAL lock_timeout = '5s'")
        .execute(&mut *transaction)
        .await?;
    let mut keys: Vec<String> = lock_domains
        .iter()
        .map(|domain| format!("{}:{domain}", community.as_uuid()))
        .collect();
    keys.sort();
    keys.dedup();
    for key in keys {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(key)
            .execute(&mut *transaction)
            .await?;
    }
    Ok(JobOperationLock { transaction })
}

impl Db {
    /// Serialize job history validation plus insert for one community operation.
    pub async fn acquire_job_operation_locks(
        &self,
        community: CommunityId,
        lock_domains: &[String],
    ) -> Result<JobOperationLock> {
        if self.max_connections < 1 {
            return Err(crate::DbError::InvalidData(
                "job operation fencing requires a writer connection".into(),
            ));
        }
        if lock_domains.is_empty() {
            return Err(crate::DbError::InvalidData(
                "job operation fencing requires at least one lock domain".into(),
            ));
        }
        acquire(&self.pool, community, lock_domains).await
    }
}

#[cfg(test)]
mod postgres_tests;
