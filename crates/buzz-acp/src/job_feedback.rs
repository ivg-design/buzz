//! Deliver a human's task-thread correction to the exact active worker.
use crate::{
    job_receiver::JobEmitter,
    pool::{AgentPool, SteerAck, SteerRequest},
    scope::SessionScope,
};
use nostr::Event;
use std::collections::HashMap;

pub(crate) fn target<'a>(
    jobs: &'a HashMap<SessionScope, JobEmitter>,
    channel: uuid::Uuid,
    event: &Event,
) -> Option<(&'a SessionScope, &'a JobEmitter)> {
    let (root, _) = buzz_core::nip10::parse_thread_markers(&event.tags).resolve()?;
    let mut matching = jobs.iter().filter(|(_, emitter)| {
        emitter
            .conversation()
            .is_some_and(|c| c.channel_id == channel.to_string() && c.thread_root_id == root)
    });
    let first = matching.next()?;
    // Legacy shared parent threads can contain several jobs; never pick one arbitrarily.
    matching.next().is_none().then_some(first)
}

pub(crate) fn deliver(
    pool: &mut AgentPool,
    scope: &SessionScope,
    emitter: &JobEmitter,
    event: &Event,
) {
    let event_id = event.id.to_hex();
    let prompt = format!("Human course correction in your current task thread. Preserve this job's operation, request and scope. Apply this clarification to your active work. Reply readably in the same task thread.\nAuthor: {}\nTimestamp: {}\nEvent: {}\n\n{}", event.pubkey.to_hex(), event.created_at.as_secs(), event_id, event.content);
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    let sent = pool.send_steer(
        scope,
        SteerRequest {
            prompt_blocks: vec![prompt],
            ack_tx,
        },
    );
    let emitter = emitter.clone();
    tokio::spawn(async move {
        let received = sent.is_ok() && matches!(ack_rx.await, Ok(SteerAck::Success { .. }));
        let message = if received {
            "Received your course correction in the active task."
        } else {
            "Your course correction is preserved in this thread, but the provider could not accept it during the active turn. It has not been applied yet."
        };
        if let Err(error) = emitter
            .progress(
                buzz_core::job::JobProgressStatus::Progress,
                message.into(),
                vec![format!("buzz:event:{event_id}")],
            )
            .await
        {
            tracing::warn!("course-correction delivery receipt pending: {error}");
        }
    });
}
