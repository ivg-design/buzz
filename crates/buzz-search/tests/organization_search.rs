//! Exercise the production query against a disposable legacy-style FTS schema.

use buzz_core::{kind::KIND_CONVERSATION_ORGANIZATION, CommunityId};
use buzz_search::{ChannelScope, SearchMode, SearchQuery, SearchService};
use sqlx::{postgres::PgPoolOptions, Executor};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires local Postgres"]
async fn organization_json_is_excluded_even_when_legacy_storage_indexes_every_kind() {
    let url = std::env::var("BUZZ_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://buzz:buzz_dev@localhost:5432/buzz".into());
    let schema = format!("organization_fts_test_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let scoped_url = format!("{url}?options=-c%20search_path%3D{schema}");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&scoped_url)
        .await
        .unwrap();
    // Deliberately use the legacy broad index. A fresh-install-only allowlist
    // would make this pass without the production query exclusion.
    pool.execute("CREATE TABLE events (
        community_id uuid, id bytea, kind integer, pubkey bytea,
        channel_id uuid, created_at timestamptz, deleted_at timestamptz,
        content text, search_tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED
    )").await.unwrap();
    let community = CommunityId::from_uuid(Uuid::new_v4());
    let message_id = vec![1_u8; 32];
    let change_id = vec![2_u8; 32];
    for (id, kind) in [
        (&message_id, 9_i32),
        (&change_id, KIND_CONVERSATION_ORGANIZATION as i32),
    ] {
        sqlx::query("INSERT INTO events (community_id,id,kind,pubkey,created_at,content) VALUES ($1,$2,$3,$4,NOW(),'release')")
            .bind(community.as_uuid()).bind(id).bind(kind).bind(vec![3_u8; 32])
            .execute(&pool).await.unwrap();
    }
    let result = SearchService::new(pool.clone())
        .search(&SearchQuery {
            community,
            q: "release".into(),
            channel_scope: ChannelScope::Any,
            kinds: None,
            authors: None,
            since: None,
            until: None,
            page: 1,
            per_page: 20,
            mode: SearchMode::FullText,
        })
        .await;
    pool.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    let hits = result.unwrap().hits;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].event_id.as_slice(), message_id);
}
