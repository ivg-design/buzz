use buzz_core::CommunityId;
use nostr::{Event, EventBuilder, Keys, Kind};
use sqlx::PgPool;
use uuid::Uuid;

use crate::relay_admin_actions::{self, ClaimResult, LeaseResult};
use crate::Db;

async fn setup() -> (Db, CommunityId) {
    let pool = PgPool::connect(&crate::test_support::database_url())
        .await
        .expect("connect to A2A test database");
    let db = Db::from_pool(pool);
    let community_id = Uuid::new_v4();
    sqlx::query("INSERT INTO communities (id, host) VALUES ($1, $2)")
        .bind(community_id)
        .bind(format!("job-delete-{}.example", community_id.simple()))
        .execute(&db.pool)
        .await
        .expect("insert deletion test community");
    (db, CommunityId::from_uuid(community_id))
}

fn event(kind: u16, label: &str) -> Event {
    EventBuilder::new(Kind::Custom(kind), label)
        .sign_with_keys(&Keys::generate())
        .expect("sign deletion fixture")
}

async fn store(db: &Db, community: CommunityId, event: &Event) {
    assert!(
        db.insert_event(community, event, None)
            .await
            .expect("insert deletion fixture")
            .1
    );
}

async fn assert_live(db: &Db, community: CommunityId, event: &Event) {
    assert!(
        db.get_event_by_id(community, event.id.as_bytes())
            .await
            .expect("load deletion fixture")
            .is_some(),
        "kind {} must remain live",
        event.kind.as_u16()
    );
}

async fn claimed_delete_action(db: &Db, community: CommunityId, target: &Event) -> (Uuid, Uuid) {
    let report_event_id = Keys::generate().public_key().to_bytes().to_vec();
    let reporter = Keys::generate().public_key().to_bytes().to_vec();
    let report_id: Uuid = sqlx::query_scalar(
        "INSERT INTO moderation_reports \
         (community_id, report_event_id, reporter_pubkey, target_kind, target_event_id, report_type) \
         VALUES ($1,$2,$3,'event',$4,'other') RETURNING id",
    )
    .bind(community.as_uuid())
    .bind(report_event_id)
    .bind(reporter)
    .bind(target.id.as_bytes().as_slice())
    .fetch_one(&db.pool)
    .await
    .expect("insert deletion report");
    let actor = Keys::generate().public_key().to_bytes().to_vec();
    let action_id = match relay_admin_actions::claim_report(
        &db.pool,
        community,
        report_id,
        Uuid::new_v4(),
        &actor,
        "operator",
        "delete",
        Some("immutability test"),
        None,
        "resolve:delete",
        "relay_operator",
        None,
        Some(target.id.as_bytes()),
        None,
    )
    .await
    .expect("claim deletion action")
    {
        ClaimResult::Claimed(action) => action.id,
        other => panic!("expected claimed delete action, got {other:?}"),
    };
    assert!(relay_admin_actions::begin_enforcing(&db.pool, action_id)
        .await
        .expect("begin deletion enforcement"));
    let lease = match relay_admin_actions::acquire_action_lease(
        &db.pool,
        action_id,
        chrono::Utc::now() + chrono::Duration::seconds(60),
    )
    .await
    .expect("acquire deletion lease")
    {
        LeaseResult::Acquired(token) => token,
        other => panic!("expected acquired lease, got {other:?}"),
    };
    (action_id, lease)
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn generic_soft_delete_paths_preserve_every_job_protocol_kind() {
    let (db, community) = setup().await;
    for kind in 43_001..=43_006 {
        let direct = event(kind, "direct immutable job");
        store(&db, community, &direct).await;
        assert!(!db
            .soft_delete_event(community, direct.id.as_bytes())
            .await
            .expect("direct soft delete"));
        assert_live(&db, community, &direct).await;

        let threaded = event(kind, "threaded immutable job");
        store(&db, community, &threaded).await;
        assert!(!db
            .soft_delete_event_and_update_thread(community, threaded.id.as_bytes(), None, None,)
            .await
            .expect("thread-aware soft delete"));
        assert_live(&db, community, &threaded).await;
    }

    let ordinary = event(1, "ordinary event remains deletable");
    store(&db, community, &ordinary).await;
    assert!(db
        .soft_delete_event(community, ordinary.id.as_bytes())
        .await
        .expect("ordinary event delete"));
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn admin_delete_path_rejects_every_job_protocol_kind_atomically() {
    let (db, community) = setup().await;
    for kind in 43_001..=43_006 {
        let target = event(kind, "admin immutable job");
        store(&db, community, &target).await;
        let (action_id, lease) = claimed_delete_action(&db, community, &target).await;
        let error = db
            .execute_delete_with_marker(
                action_id,
                lease,
                community,
                target.id.as_bytes(),
                None,
                None,
            )
            .await
            .expect_err("admin path must reject job deletion");
        assert!(error
            .to_string()
            .contains("job protocol events are immutable"));
        assert_live(&db, community, &target).await;
        let action = relay_admin_actions::get_action(&db.pool, action_id)
            .await
            .expect("read rejected action")
            .expect("rejected action exists");
        assert_eq!(action.step_marker, None, "rejected delete must be atomic");
    }

    let ordinary = event(1, "ordinary admin target");
    store(&db, community, &ordinary).await;
    let (action_id, lease) = claimed_delete_action(&db, community, &ordinary).await;
    assert!(db
        .execute_delete_with_marker(
            action_id,
            lease,
            community,
            ordinary.id.as_bytes(),
            None,
            None,
        )
        .await
        .expect("ordinary admin delete"));
}
