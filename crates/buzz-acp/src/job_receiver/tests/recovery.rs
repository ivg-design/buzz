use super::*;

#[tokio::test]
async fn membership_revocation_freezes_exactly_one_terminal() {
    let sender = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let (request, request_event) = fixture(
        &sender,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "revoked",
        "work",
    );
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
    let receiver = JobReceiver::for_test(
        context(&worker),
        worker.clone(),
        rest,
        sponsor(&worker),
        grants(&request),
        ledger_root.clone(),
    );
    assert!(matches!(
        receiver
            .handle_request(channel, request_event, Some(&project(&request)))
            .await
            .expect("claim"),
        HandleOutcome::Dispatch(_)
    ));
    let _ = published.recv().await.expect("processed");
    let _ = published.recv().await.expect("accepted");
    assert_eq!(
        receiver
            .terminate_channel(channel)
            .await
            .expect("terminate revoked channel"),
        1
    );
    let terminal = published.recv().await.expect("terminal");
    assert!(matches!(
        JobEvent::parse(&terminal).expect("valid terminal"),
        JobEvent::Error(error)
            if error.outcome == JobErrorOutcome::Indeterminate
                && error.code == "membership_revoked"
                && !error.retryable
    ));
    assert_eq!(
        receiver
            .terminate_channel(channel)
            .await
            .expect("terminal replay"),
        0
    );
    assert!(
        published.try_recv().is_err(),
        "must not fork terminal state"
    );
    server.abort();
    std::fs::remove_dir_all(ledger_root).ok();
}

#[tokio::test]
async fn panic_after_prompt_start_never_redispatches_and_freezes_one_terminal() {
    let sender = Keys::generate();
    let worker = Keys::generate();
    let channel = Uuid::new_v4();
    let ledger_root = root();
    let (request, request_event) = fixture(
        &sender,
        &worker,
        channel,
        "https://github.com/mysteropodes/nemo",
        "panic-after-start",
        "work",
    );
    let (rest, mut published, server) = RestClient::accepting_test_pair(worker.clone()).await;
    let receiver = JobReceiver::for_test(
        context(&worker),
        worker.clone(),
        rest,
        sponsor(&worker),
        grants(&request),
        ledger_root.clone(),
    );
    let HandleOutcome::Dispatch(dispatch) = receiver
        .handle_request(channel, request_event, Some(&project(&request)))
        .await
        .expect("claim request")
    else {
        panic!("request should dispatch")
    };
    receiver
        .mark_prompt_started(&dispatch.claim)
        .await
        .expect("durable prompt-start marker");
    let _ = published.recv().await.expect("processed");
    let _ = published.recv().await.expect("accepted");

    let replay_probe = dispatch.emitter.clone();
    dispatch
        .emitter
        .indeterminate(
            "worker_panicked".into(),
            "Worker process stopped before recording a terminal outcome".into(),
        )
        .await
        .expect("freeze panic terminal");
    let terminal = published.recv().await.expect("panic terminal");
    assert!(matches!(
        JobEvent::parse(&terminal).expect("valid terminal"),
        JobEvent::Error(error)
            if error.outcome == JobErrorOutcome::Indeterminate
                && error.code == "worker_panicked"
                && !error.retryable
    ));
    assert!(
        receiver
            .pending_events()
            .await
            .expect("scan pending requests")
            .is_empty(),
        "a prompt-started job must never be redispatched"
    );
    assert!(
        replay_probe
            .indeterminate("worker_panicked".into(), "duplicate".into())
            .await
            .is_err(),
        "a terminal lifecycle must reject a sibling terminal"
    );
    receiver
        .recover_lifecycle()
        .await
        .expect("terminal restart replay");
    assert!(
        published.try_recv().is_err(),
        "panic handling must emit exactly one terminal"
    );
    server.abort();
    std::fs::remove_dir_all(ledger_root).ok();
}
