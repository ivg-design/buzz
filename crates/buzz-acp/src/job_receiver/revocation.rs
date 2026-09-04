use buzz_core::job::{JobControlAction, JobEvent};
use uuid::Uuid;

use super::{JobEmitter, JobReceiver, ReceiverError};

/// Freeze one deterministic terminal for every nonterminal claim whose
/// Project channel authorization has just been revoked.
pub(super) async fn terminate_channel(
    receiver: &JobReceiver,
    channel_id: Uuid,
) -> Result<usize, ReceiverError> {
    let mut terminated = 0;
    let mut first_error = None;
    for claim in receiver.ledger.claims().await? {
        if claim.community != receiver.tenant.community_id {
            continue;
        }
        let JobEvent::Request(request) = JobEvent::parse(&claim.request_event)
            .map_err(|error| ReceiverError::Receipt(error.to_string()))?
        else {
            continue;
        };
        if request.common.project.home_channel != channel_id.to_string() {
            continue;
        }
        let lifecycle = receiver.ledger.lifecycle_store(&claim);
        lifecycle.initialize(claim.accepted.id.to_hex()).await?;
        let (_, pending, terminal) = lifecycle.snapshot().await?;
        if terminal {
            continue;
        }
        if let Some(event) = pending {
            if let Err(error) = receiver.rest.submit_event_confirmed(&event).await {
                first_error.get_or_insert_with(|| error.to_string());
                continue;
            }
            lifecycle.confirm(event.id.to_hex()).await?;
            if lifecycle.snapshot().await?.2 {
                continue;
            }
        }
        let pending_cancel = lifecycle.pending_cancel().await?.is_some();
        let emitter = JobEmitter::new(
            &request,
            claim.request_event_id,
            receiver.keys.clone(),
            receiver.rest.clone(),
            lifecycle,
            receiver
                .grants
                .capabilities_for(&request)
                .unwrap_or_default(),
            claim.digest,
            receiver.sponsor.clone(),
        );
        let result = if pending_cancel {
            emitter
                .control(
                    JobControlAction::Cancelled,
                    "requester_cancelled".into(),
                    None,
                )
                .await
        } else {
            emitter
                .indeterminate(
                    "membership_revoked".into(),
                    "Project channel authorization was revoked while the worker was active".into(),
                )
                .await
        };
        match result {
            Ok(_) => terminated += 1,
            Err(error) => {
                // publish_followup freezes before REST submission, so the live
                // retry worker can converge the exact event later.
                first_error.get_or_insert_with(|| error.to_string());
            }
        }
    }
    match first_error {
        Some(error) => Err(ReceiverError::Receipt(error)),
        None => Ok(terminated),
    }
}
