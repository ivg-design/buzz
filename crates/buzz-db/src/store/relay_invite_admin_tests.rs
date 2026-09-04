use super::*;
use crate::relay_invite::{claim_relay_invite, mint_relay_invite, ClaimOutcome};
use buzz_core::invite::hash_v2_code;
use sqlx::PgPool;

async fn setup_pool() -> PgPool {
    PgPool::connect(&crate::test_support::database_url())
        .await
        .expect("connect to test DB")
}

async fn make_test_community(pool: &PgPool) -> CommunityId {
    let id = uuid::Uuid::new_v4();
    sqlx::query("INSERT INTO communities (id, host) VALUES ($1, $2)")
        .bind(id)
        .bind(format!("relay-invite-admin-{}.example", id.simple()))
        .execute(pool)
        .await
        .expect("insert test community");
    CommunityId::from_uuid(id)
}

async fn delete_test_community(pool: &PgPool, community: CommunityId) {
    let mut tx = pool.begin().await.expect("begin test cleanup");
    sqlx::query("DELETE FROM relay_invites WHERE community_id = $1")
        .bind(community.as_uuid())
        .execute(&mut *tx)
        .await
        .expect("delete test invites");
    sqlx::query("DELETE FROM relay_members WHERE community_id = $1")
        .bind(community.as_uuid())
        .execute(&mut *tx)
        .await
        .expect("delete test members");
    sqlx::query("DELETE FROM communities WHERE id = $1")
        .bind(community.as_uuid())
        .execute(&mut *tx)
        .await
        .expect("delete test community");
    tx.commit().await.expect("commit test cleanup");
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn list_returns_only_live_unexhausted_metadata() {
    let pool = setup_pool().await;
    let community = make_test_community(&pool).await;
    let pending = mint_relay_invite(&pool, community, "owner", 3600, Some(1))
        .await
        .expect("mint pending invite");
    let expired = mint_relay_invite(&pool, community, "owner", 3600, Some(1))
        .await
        .expect("mint expired invite");
    let exhausted = mint_relay_invite(&pool, community, "owner", 3600, Some(1))
        .await
        .expect("mint exhausted invite");
    sqlx::query(
        "UPDATE relay_invites SET expires_at = now() - interval '1 second' \
         WHERE community_id = $1 AND id = $2",
    )
    .bind(community.as_uuid())
    .bind(expired.invite_id)
    .execute(&pool)
    .await
    .expect("expire invite");
    let outcome = claim_relay_invite(
        &pool,
        community,
        &hash_v2_code(&exhausted.code),
        &"ab".repeat(32),
        None,
    )
    .await
    .expect("exhaust invite");
    assert!(matches!(outcome, ClaimOutcome::Joined { .. }));

    let listed = list_pending_relay_invites(&pool, community)
        .await
        .expect("list pending invites");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, pending.invite_id);
    assert_eq!(listed[0].max_uses, Some(1));
    assert_eq!(listed[0].uses_remaining, Some(1));

    delete_test_community(&pool, community).await;
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn revoke_is_tenant_scoped_and_invalidates_the_bearer() {
    let pool = setup_pool().await;
    let community_a = make_test_community(&pool).await;
    let community_b = make_test_community(&pool).await;
    let invite = mint_relay_invite(&pool, community_a, "owner", 3600, Some(1))
        .await
        .expect("mint invite");

    assert!(!revoke_relay_invite(&pool, community_b, invite.invite_id)
        .await
        .expect("cross-tenant revoke"));
    assert!(revoke_relay_invite(&pool, community_a, invite.invite_id)
        .await
        .expect("revoke invite"));
    assert!(!revoke_relay_invite(&pool, community_a, invite.invite_id)
        .await
        .expect("idempotent missing revoke"));
    assert_eq!(
        claim_relay_invite(
            &pool,
            community_a,
            &hash_v2_code(&invite.code),
            &"cd".repeat(32),
            None,
        )
        .await
        .expect("claim revoked invite"),
        ClaimOutcome::Invalid,
    );

    delete_test_community(&pool, community_a).await;
    delete_test_community(&pool, community_b).await;
}
