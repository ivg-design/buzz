use super::*;
use buzz_sdk::ThreadRef;
use nostr::{Event, EventId, Keys};

fn trigger_event(channel_id: Uuid, thread_ref: Option<&ThreadRef>) -> Event {
    buzz_sdk::build_message(
        channel_id,
        "@Codexitron check readiness",
        thread_ref,
        &[],
        false,
        &[],
        &[],
    )
    .expect("triggering message")
    .sign_with_keys(&Keys::generate())
    .expect("signed triggering message")
}

async fn published_nudge(
    triggering_event: &Event,
    channel_id: Uuid,
    placement: ReplyPlacement,
) -> Event {
    let agent_keys = Keys::generate();
    let recipient = Keys::generate().public_key().to_hex();
    let payload = SetupPayload {
        agent_name: "Codexitron".into(),
        agent_pubkey: agent_keys.public_key().to_hex(),
        requirements: vec![],
    };
    let (publisher, mut published) = RelayEventPublisher::test_pair();
    let nudge_emitter = SetupNudgeEmitter {
        publisher: &publisher,
        keys: &agent_keys,
        payload: &payload,
        reply_placement: placement,
    };

    nudge_emitter
        .publish(channel_id, triggering_event, &recipient)
        .await
        .expect("setup nudge published");

    published.recv().await.expect("published setup nudge")
}

#[tokio::test]
async fn setup_nudge_reply_placement_matrix_matches_normal_reply_contract() {
    #[derive(Clone, Copy, Debug)]
    enum TriggerShape {
        TopLevel,
        ExistingThread,
    }

    let root_id = EventId::from_byte_array([0xA1; 32]);
    let parent_id = EventId::from_byte_array([0xB2; 32]);
    let cases = [
        (TriggerShape::TopLevel, ReplyPlacement::Thread),
        (TriggerShape::TopLevel, ReplyPlacement::Timeline),
        (TriggerShape::ExistingThread, ReplyPlacement::Thread),
        (TriggerShape::ExistingThread, ReplyPlacement::Timeline),
    ];

    for (shape, placement) in cases {
        let channel_id = Uuid::new_v4();
        let thread_ref = matches!(shape, TriggerShape::ExistingThread).then_some(ThreadRef {
            root_event_id: root_id,
            parent_event_id: parent_id,
        });
        let triggering_event = trigger_event(channel_id, thread_ref.as_ref());
        let expected_root = match shape {
            TriggerShape::ExistingThread => Some(root_id.to_hex()),
            TriggerShape::TopLevel if placement == ReplyPlacement::Thread => {
                Some(triggering_event.id.to_hex())
            }
            TriggerShape::TopLevel => None,
        };

        let nudge = published_nudge(&triggering_event, channel_id, placement).await;
        let actual = crate::queue::parse_thread_tags(&nudge);
        assert_eq!(
            actual.root_event_id, expected_root,
            "wrong setup nudge root for shape={shape:?}, placement={placement}"
        );
        assert_eq!(
            actual.parent_event_id, expected_root,
            "setup nudges must stay flat for shape={shape:?}, placement={placement}"
        );
    }
}
