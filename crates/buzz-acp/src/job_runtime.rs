//! Runtime fencing shared by the agent-job event loop and focused tests.

use crate::config::DedupMode;
use crate::pool::{AgentPool, ControlSignal};
use crate::queue::{EventQueue, FlushBatch};
use crate::scope::SessionScope;

/// Remove queued work for one cancelled job and signal only its exact active
/// worker. Returns true when the caller must wait for that worker to quiesce
/// before publishing `cancelled`.
pub(crate) fn quiesce_for_cancel(
    pool: &mut AgentPool,
    queue: &mut EventQueue,
    scope: &SessionScope,
    request_event_id: &str,
) -> bool {
    queue.remove_event(scope, request_event_id);
    let entry = pool
        .task_map_mut()
        .values_mut()
        .find(|meta| meta.scope.as_ref() == Some(scope));
    let Some(meta) = entry else {
        queue.mark_complete(scope.clone());
        return false;
    };

    if let Some(tx) = meta.control_tx.take() {
        let _ = tx.send(ControlSignal::Cancel);
    }
    true
}

/// Chat turns may be retried after a task panic in Queue mode. A durable job
/// has crossed its prompt-start boundary and must never run side effects twice.
pub(crate) fn recoverable_batch_for(
    dedup_mode: DedupMode,
    scope: &SessionScope,
    batch: &FlushBatch,
) -> Option<FlushBatch> {
    match dedup_mode {
        DedupMode::Queue if !scope.is_job() => Some(batch.clone()),
        DedupMode::Queue | DedupMode::Drop => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::TaskMeta;
    use crate::queue::QueuedEvent;
    use nostr::{EventBuilder, Keys, Kind};
    use std::collections::HashSet;
    use std::time::Instant;
    use uuid::Uuid;

    fn job_scope(channel_id: Uuid, marker: char) -> SessionScope {
        SessionScope::Job {
            channel_id,
            operation_id: Uuid::new_v4().to_string(),
            request_event_id: marker.to_string().repeat(64),
        }
    }

    fn queued_event(scope: SessionScope) -> QueuedEvent {
        let event = EventBuilder::new(Kind::Custom(43001), "job")
            .tags([])
            .sign_with_keys(&Keys::generate())
            .expect("sign test event");
        QueuedEvent {
            channel_id: scope.channel_id(),
            scope,
            event,
            received_at: Instant::now(),
            prompt_tag: "agent-job".into(),
        }
    }

    #[test]
    fn queued_cancel_removes_job_without_starting_a_prompt() {
        let scope = job_scope(Uuid::new_v4(), 'a');
        let event = queued_event(scope.clone());
        let event_id = event.event.id.to_hex();
        let mut queue = EventQueue::new(DedupMode::Queue);
        assert!(queue.push_job(event));
        let mut pool = AgentPool::from_slots(vec![]);

        assert!(!quiesce_for_cancel(
            &mut pool, &mut queue, &scope, &event_id
        ));
        assert!(!queue.has_undispatched_work());
        assert!(!queue.is_scope_in_flight(&scope));
    }

    #[tokio::test]
    async fn running_cancel_signals_only_the_exact_job_and_waits_for_quiescence() {
        let channel_id = Uuid::new_v4();
        let scope = job_scope(channel_id, 'b');
        let sibling = job_scope(channel_id, 'c');
        let event = queued_event(scope.clone());
        let event_id = event.event.id.to_hex();
        let mut queue = EventQueue::new(DedupMode::Queue);
        assert!(queue.push_job(event));
        let _batch = queue.flush_next().expect("dispatch job batch");
        assert!(queue.is_scope_in_flight(&scope));

        let mut pool = AgentPool::from_slots(vec![]);
        let exact_task = pool.join_set.spawn(std::future::pending::<()>());
        let sibling_task = pool.join_set.spawn(std::future::pending::<()>());
        let (exact_tx, exact_rx) = tokio::sync::oneshot::channel();
        let (sibling_tx, mut sibling_rx) = tokio::sync::oneshot::channel();
        pool.task_map_mut().insert(
            exact_task.id(),
            TaskMeta {
                agent_index: 0,
                channel_id: Some(channel_id),
                scope: Some(scope.clone()),
                turn_id: "exact-job".into(),
                recoverable_batch: None,
                control_tx: Some(exact_tx),
                steer_tx: None,
                successful_steer_deliveries: HashSet::new(),
            },
        );
        pool.task_map_mut().insert(
            sibling_task.id(),
            TaskMeta {
                agent_index: 1,
                channel_id: Some(channel_id),
                scope: Some(sibling),
                turn_id: "sibling-job".into(),
                recoverable_batch: None,
                control_tx: Some(sibling_tx),
                steer_tx: None,
                successful_steer_deliveries: HashSet::new(),
            },
        );

        assert!(quiesce_for_cancel(&mut pool, &mut queue, &scope, &event_id));
        assert_eq!(
            exact_rx.await.expect("exact cancel signal"),
            ControlSignal::Cancel
        );
        assert!(matches!(
            sibling_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(queue.is_scope_in_flight(&scope));

        pool.join_set.abort_all();
        while pool.join_set.join_next().await.is_some() {}
    }

    #[test]
    fn job_panic_policy_never_requeues_a_side_effect_batch() {
        let channel_id = Uuid::new_v4();
        let scope = job_scope(channel_id, 'd');
        let mut queue = EventQueue::new(DedupMode::Queue);
        assert!(queue.push_job(queued_event(scope.clone())));
        let batch = queue.flush_next().expect("dispatch job batch");

        assert!(recoverable_batch_for(DedupMode::Queue, &scope, &batch).is_none());

        let chat_scope = SessionScope::Conversation { channel_id };
        assert!(recoverable_batch_for(DedupMode::Queue, &chat_scope, &batch).is_some());
        assert!(recoverable_batch_for(DedupMode::Drop, &chat_scope, &batch).is_none());
    }
}
