use buzz_core::organization::{build_change_event, OrganizationAction, OrganizationChange};
use nostr::{Event, EventBuilder, Keys, Kind, Tag, Timestamp};
use sqlx::postgres::PgPoolOptions;

use crate::test_support::job::JobFixture;

async fn root_message(fixture: &JobFixture) -> Event {
    let event = EventBuilder::new(Kind::Custom(9), "Topic discussion")
        .tags([Tag::parse(["h", &fixture.channel_id.to_string()]).unwrap()])
        .sign_with_keys(&fixture.requester)
        .unwrap();
    fixture
        .state
        .db
        .insert_event(fixture.tenant.community(), &event, Some(fixture.channel_id))
        .await
        .unwrap();
    event
}

fn participants(fixture: &JobFixture, root: &Event, agents: &[&Keys]) -> Event {
    build_change_event(
        fixture.channel_id,
        &OrganizationChange {
            version: 1,
            action: OrganizationAction::Participants {
                thread_root_id: root.id.to_hex(),
                agent_pubkeys: agents
                    .iter()
                    .map(|agent| agent.public_key().to_hex())
                    .collect(),
            },
        },
        &fixture.requester,
        Timestamp::now().as_secs(),
        &[],
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated BUZZ_TEST_DATABASE_URL and BUZZ_TEST_REDIS_URL"]
async fn participants_validate_current_enrollment_and_existing_channel_access() {
    let database_url = std::env::var("BUZZ_TEST_DATABASE_URL").expect("dedicated test database");
    std::env::var("BUZZ_TEST_REDIS_URL").expect("dedicated test Redis");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    let db = buzz_db::Db::from_pool(pool.clone());
    db.migrate().await.unwrap();
    pool.close().await;

    let fixture = JobFixture::new(4).await;
    let root = root_message(&fixture).await;
    let validate = |event: Event| {
        let fixture = &fixture;
        async move { super::validate(&fixture.tenant, &event, &fixture.state).await }
    };
    assert!(validate(participants(&fixture, &root, &[&fixture.worker]))
        .await
        .is_ok());
    assert!(validate(participants(&fixture, &root, &[])).await.is_ok());
    assert!(
        validate(participants(&fixture, &root, &[&fixture.requester]))
            .await
            .is_err(),
        "humans are not agent participants"
    );
    assert!(
        validate(participants(&fixture, &root, &[&Keys::generate()]))
            .await
            .is_err(),
        "unknown identities cannot be added"
    );

    let other = JobFixture::new(4).await;
    assert!(
        validate(participants(&fixture, &root, &[&other.worker]))
            .await
            .is_err(),
        "another community's agent is not enrolled here"
    );

    let owner = Keys::generate();
    let agent = Keys::generate();
    let community = fixture.tenant.community();
    let db = &fixture.state.db;
    for keys in [&owner, &agent] {
        db.ensure_user(community, &keys.public_key().to_bytes())
            .await
            .unwrap();
    }
    db.add_relay_member(community, &owner.public_key().to_hex(), "member", None)
        .await
        .unwrap();
    db.set_agent_owner(
        community,
        &agent.public_key().to_bytes(),
        &owner.public_key().to_bytes(),
    )
    .await
    .unwrap();
    assert!(
        validate(participants(&fixture, &root, &[&agent]))
            .await
            .is_err(),
        "a private thread cannot add channel access"
    );

    db.update_channel(
        community,
        fixture.channel_id,
        buzz_db::channel::ChannelUpdate {
            name: None,
            description: None,
            visibility: Some("open".into()),
            ttl_seconds: None,
        },
    )
    .await
    .unwrap();
    assert!(
        validate(participants(&fixture, &root, &[&agent]))
            .await
            .is_ok(),
        "open-channel participation needs no membership mutation"
    );
    assert!(!db
        .is_member(
            community,
            fixture.channel_id,
            &agent.public_key().to_bytes()
        )
        .await
        .unwrap());

    db.remove_relay_member(community, &owner.public_key().to_hex())
        .await
        .unwrap();
    assert!(
        validate(participants(&fixture, &root, &[&agent]))
            .await
            .is_err(),
        "owner removal revokes enrollment immediately"
    );
    assert!(
        validate(participants(&fixture, &root, &[])).await.is_ok(),
        "removing everyone remains possible after revocation"
    );
}
