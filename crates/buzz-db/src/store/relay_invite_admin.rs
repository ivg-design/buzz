//! Metadata-only administration for durable relay invites.
//!
//! The reusable bearer code never enters this module: migration 0025 stores
//! only its SHA-256 digest, and the management view intentionally omits that
//! digest as well. Revocation deletes the scoped row, making every later
//! presentation of the code resolve to `Invalid` in the atomic claim path.

use buzz_datastore_tracing::datastore_span;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row as _};

use crate::error::Result;
use crate::{observability, CommunityId, Db};

/// Bound an operator read even if a legacy tenant accumulated many invites.
const MAX_PENDING_INVITES: i64 = 1_000;

/// Safe metadata for one live, not-yet-exhausted invite.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingRelayInvite {
    /// Server-generated invite identifier. This is not the bearer code.
    pub id: uuid::Uuid,
    /// Maximum successful claims, or `None` for a legacy unlimited invite.
    pub max_uses: Option<i32>,
    /// Successful claims committed so far.
    pub use_count: i32,
    /// Remaining claims, or `None` for a legacy unlimited invite.
    pub uses_remaining: Option<i32>,
    /// Time after which the claim path rejects the invite.
    pub expires_at: DateTime<Utc>,
    /// Pubkey that created the invite.
    pub created_by: String,
    /// Time at which the invite was created.
    pub created_at: DateTime<Utc>,
}

/// List live, not-yet-exhausted invites for one community.
pub async fn list_pending_relay_invites(
    pool: &PgPool,
    community: CommunityId,
) -> Result<Vec<PendingRelayInvite>> {
    let mut connection =
        observability::acquire_writer(pool, observability::WriterOperation::Authorization).await?;
    let rows = sqlx::query(
        "SELECT id, max_uses, use_count, expires_at, created_by, created_at \
         FROM relay_invites \
         WHERE community_id = $1 \
           AND expires_at > now() \
           AND (max_uses IS NULL OR use_count < max_uses) \
         ORDER BY created_at DESC, id DESC \
         LIMIT $2",
    )
    .bind(community.as_uuid())
    .bind(MAX_PENDING_INVITES)
    .fetch_all(&mut *connection)
    .await?;

    rows.into_iter()
        .map(
            |row| -> std::result::Result<PendingRelayInvite, sqlx::Error> {
                let max_uses: Option<i32> = row.try_get("max_uses")?;
                let use_count: i32 = row.try_get("use_count")?;
                Ok(PendingRelayInvite {
                    id: row.try_get("id")?,
                    max_uses,
                    use_count,
                    uses_remaining: max_uses.map(|limit| limit - use_count),
                    expires_at: row.try_get("expires_at")?,
                    created_by: row.try_get("created_by")?,
                    created_at: row.try_get("created_at")?,
                })
            },
        )
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(crate::error::DbError::from)
}

/// Revoke one invite scoped to a community.
///
/// The delete shares the community lifecycle write fence. PostgreSQL row
/// locking linearizes it with an in-flight claim: after this function commits,
/// no later claim can find the invite row.
pub async fn revoke_relay_invite(
    pool: &PgPool,
    community: CommunityId,
    invite_id: uuid::Uuid,
) -> Result<bool> {
    let connection =
        observability::acquire_writer(pool, observability::WriterOperation::Authorization).await?;
    let mut tx = sqlx::Transaction::begin(connection, None).await?;
    crate::deletion::DeletionStore::new(pool.clone())
        .guard_transaction(&mut tx, community)
        .await?;
    let removed = sqlx::query("DELETE FROM relay_invites WHERE community_id = $1 AND id = $2")
        .bind(community.as_uuid())
        .bind(invite_id)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    tx.commit().await?;
    Ok(removed)
}

impl Db {
    /// Lists safe metadata for pending invites in `community`.
    #[datastore_span(name = "list_pending_relay_invites", system = "postgresql")]
    pub async fn list_pending_relay_invites(
        &self,
        community: CommunityId,
    ) -> Result<Vec<PendingRelayInvite>> {
        list_pending_relay_invites(&self.pool, community).await
    }

    /// Revokes an invite by its non-secret identifier.
    #[datastore_span(name = "revoke_relay_invite", system = "postgresql")]
    pub async fn revoke_relay_invite(
        &self,
        community: CommunityId,
        invite_id: uuid::Uuid,
    ) -> Result<bool> {
        revoke_relay_invite(&self.pool, community, invite_id).await
    }
}

#[cfg(test)]
#[path = "relay_invite_admin_tests.rs"]
mod postgres_tests;
