use std::sync::Arc;
use std::time::Duration;

use buzz_core::job::JobErrorOutcome;
use nostr::Event;
use uuid::Uuid;

use crate::test_support::job::{persist, JobFixture};

use super::{validate_job_event, JobAuthError};

fn assert_one_insert_one_error(
    left: Result<bool, JobAuthError>,
    right: Result<bool, JobAuthError>,
) {
    match (left, right) {
        (Ok(true), Err(JobAuthError::Invalid(_))) | (Err(JobAuthError::Invalid(_)), Ok(true)) => {}
        results => panic!("expected one insert and one validation conflict, got {results:?}"),
    }
}

async fn store_accepted_chain(fixture: &JobFixture, key: &str) -> (Event, Event) {
    let request = fixture.request(Uuid::new_v4(), key);
    assert!(persist(fixture, &request).await.expect("store request"));
    let processed = fixture.processed(&request);
    assert!(persist(fixture, &processed)
        .await
        .expect("store processed receipt"));
    let accepted = fixture.accepted(&request, &processed);
    assert!(persist(fixture, &accepted)
        .await
        .expect("store accepted receipt"));
    (request, accepted)
}

async fn assert_restricted_and_absent(fixture: &JobFixture, event: &Event) {
    assert!(matches!(
        persist(fixture, event).await,
        Err(JobAuthError::Restricted(_))
    ));
    assert!(fixture
        .state
        .db
        .get_event_by_id_for_event_write(fixture.tenant.community(), &event.id.to_bytes(),)
        .await
        .expect("query denied event")
        .is_none());
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn concurrent_identical_request_is_inserted_once_and_replays_successfully() {
    let fixture = JobFixture::new(4).await;
    let event = fixture.request(Uuid::new_v4(), "identical-request");

    let (left, right) = tokio::join!(persist(&fixture, &event), persist(&fixture, &event));
    let mut inserted = [
        left.expect("first delivery"),
        right.expect("second delivery"),
    ];
    inserted.sort_unstable();
    assert_eq!(inserted, [false, true]);

    let count = fixture
        .state
        .db
        .count_events(&buzz_db::EventQuery {
            ids: Some(vec![event.id.to_bytes().to_vec()]),
            ..buzz_db::EventQuery::for_community(fixture.tenant.community())
        })
        .await
        .expect("count exact replay rows");
    assert_eq!(count, 1);
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn concurrent_requests_with_one_idempotency_key_admit_one_operation() {
    let fixture = JobFixture::new(4).await;
    let left = fixture.request(Uuid::new_v4(), "shared-idempotency");
    let right = fixture.request(Uuid::new_v4(), "shared-idempotency");

    let results = tokio::join!(persist(&fixture, &left), persist(&fixture, &right));
    assert_one_insert_one_error(results.0, results.1);
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn concurrent_requests_with_one_operation_id_admit_one_request() {
    let fixture = JobFixture::new(4).await;
    let operation = Uuid::new_v4();
    let left = fixture.request(operation, "operation-left");
    let right = fixture.request(operation, "operation-right");

    let results = tokio::join!(persist(&fixture, &left), persist(&fixture, &right));
    assert_one_insert_one_error(results.0, results.1);
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn concurrent_successors_of_one_prior_event_admit_exactly_one_child() {
    let fixture = JobFixture::new(4).await;
    let request = fixture.request(Uuid::new_v4(), "single-successor");
    assert!(persist(&fixture, &request).await.expect("store request"));
    let processed = fixture.processed(&request);
    assert!(persist(&fixture, &processed)
        .await
        .expect("store processed receipt"));
    let accepted = fixture.accepted(&request, &processed);
    assert!(persist(&fixture, &accepted)
        .await
        .expect("store accepted receipt"));
    let left = fixture.progress(&request, &accepted, "left child");
    let right = fixture.progress(&request, &accepted, "right child");

    let results = tokio::join!(persist(&fixture, &left), persist(&fixture, &right));
    assert_one_insert_one_error(results.0, results.1);
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn job_validation_and_insert_complete_with_one_writer_connection() {
    let fixture = JobFixture::new(1).await;
    let request = fixture.request(Uuid::new_v4(), "single-connection");

    let inserted = tokio::time::timeout(Duration::from_secs(5), persist(&fixture, &request))
        .await
        .expect("job write must not self-deadlock with max_connections=1")
        .expect("single-connection job write");
    assert!(inserted);
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn locked_authority_row_serializes_channel_membership_revocation() {
    let fixture = Arc::new(JobFixture::new(4).await);
    let request = fixture.request(Uuid::new_v4(), "authority-race");
    let mut validated = validate_job_event(&fixture.tenant, &fixture.state, &request)
        .await
        .expect("validate while authority is current");

    let revocation_fixture = Arc::clone(&fixture);
    let revocation = tokio::spawn(async move {
        revocation_fixture
            .state
            .db
            .remove_member(
                revocation_fixture.tenant.community(),
                revocation_fixture.channel_id,
                &revocation_fixture.worker.public_key().to_bytes(),
                &revocation_fixture.requester.public_key().to_bytes(),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !revocation.is_finished(),
        "revocation must wait for the held FOR SHARE authority row lock"
    );

    let (_, inserted) = validated
        .insert_event(&fixture.tenant, &request, fixture.channel_id)
        .await
        .expect("insert while authority lock is held");
    assert!(inserted);
    validated.commit().await.expect("commit fenced request");
    revocation
        .await
        .expect("join revocation")
        .expect("revocation completes after commit");

    let later = fixture.request(Uuid::new_v4(), "after-revocation");
    assert!(matches!(
        persist(&fixture, &later).await,
        Err(JobAuthError::Restricted(_))
    ));
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn revoked_accepted_worker_can_store_exact_membership_terminal_once() {
    let fixture = JobFixture::new(4).await;
    let (request, accepted) = store_accepted_chain(&fixture, "revoked-terminal").await;
    let terminal = fixture.error(
        &request,
        &accepted,
        JobErrorOutcome::Indeterminate,
        "membership_revoked",
        "Project channel authorization was revoked while the worker was active",
    );
    assert_restricted_and_absent(&fixture, &terminal).await;

    fixture
        .state
        .db
        .remove_member(
            fixture.tenant.community(),
            fixture.channel_id,
            &fixture.worker.public_key().to_bytes(),
            &fixture.requester.public_key().to_bytes(),
        )
        .await
        .expect("remove accepted worker");

    assert!(persist(&fixture, &terminal)
        .await
        .expect("store exact membership terminal"));
    assert!(!persist(&fixture, &terminal)
        .await
        .expect("replay exact membership terminal"));

    let count = fixture
        .state
        .db
        .count_events(&buzz_db::EventQuery {
            ids: Some(vec![terminal.id.to_bytes().to_vec()]),
            ..buzz_db::EventQuery::for_community(fixture.tenant.community())
        })
        .await
        .expect("count terminal rows");
    assert_eq!(count, 1);
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn arbitrary_former_member_lifecycle_events_remain_denied() {
    let fixture = JobFixture::new(4).await;
    let (progress_request, progress_accepted) =
        store_accepted_chain(&fixture, "revoked-progress").await;
    let (result_request, result_accepted) = store_accepted_chain(&fixture, "revoked-result").await;
    let (error_request, error_accepted) = store_accepted_chain(&fixture, "revoked-error").await;
    let (altered_request, altered_accepted) =
        store_accepted_chain(&fixture, "revoked-altered-terminal").await;
    let (failed_request, failed_accepted) =
        store_accepted_chain(&fixture, "revoked-failed-terminal").await;
    let (cancelled_request, cancelled_accepted) =
        store_accepted_chain(&fixture, "revoked-cancelled").await;
    let cancel = fixture.cancel(&cancelled_request, &cancelled_accepted);
    assert!(persist(&fixture, &cancel)
        .await
        .expect("store requester cancel"));

    fixture
        .state
        .db
        .remove_member(
            fixture.tenant.community(),
            fixture.channel_id,
            &fixture.worker.public_key().to_bytes(),
            &fixture.requester.public_key().to_bytes(),
        )
        .await
        .expect("remove accepted worker");

    let denied = [
        fixture.progress(&progress_request, &progress_accepted, "still working"),
        fixture.result(&result_request, &result_accepted),
        fixture.error(
            &error_request,
            &error_accepted,
            JobErrorOutcome::Indeterminate,
            "side_effect_unknown",
            "arbitrary terminal",
        ),
        fixture.error(
            &altered_request,
            &altered_accepted,
            JobErrorOutcome::Indeterminate,
            "membership_revoked",
            "arbitrary former-member payload",
        ),
        fixture.error(
            &failed_request,
            &failed_accepted,
            JobErrorOutcome::Failed,
            "membership_revoked",
            "Project channel authorization was revoked while the worker was active",
        ),
        fixture.cancelled(&cancelled_request, &cancel),
    ];
    for event in denied {
        assert_restricted_and_absent(&fixture, &event).await;
    }
}
