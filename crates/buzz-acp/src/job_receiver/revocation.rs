use buzz_core::job::JobEvent;
use uuid::Uuid;

use super::ledger::ReceiptKind;
use super::{JobEmitter, JobReceiver, ReceiverError};

/// Freeze one deterministic terminal for every accepted, nonterminal claim
/// whose Project channel authorization has just been revoked. A claim revoked
/// before Accepted is durably suppressed at Processed instead.
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
        if !lifecycle.exists() {
            let accepted_acked = receiver
                .ledger
                .receipt_acked(&claim, ReceiptKind::Accepted)
                .await?;
            if !accepted_acked {
                // There is no protocol-valid worker terminal before Accepted:
                // Error/Result require Accepted or Progress, and Cancelled
                // requires a requester Cancel. Persist Processed as the local
                // lifecycle root so receipt retry can never manufacture the
                // frozen Accepted after channel authority has been revoked.
                lifecycle.initialize(claim.processed.id.to_hex()).await?;
                terminated += 1;
                continue;
            }
            lifecycle.initialize(claim.accepted.id.to_hex()).await?;
        }
        let snapshot = lifecycle.privilege_snapshot().await?;
        if snapshot.terminal {
            continue;
        }
        if snapshot.accepted_event_id == claim.processed.id.to_hex()
            && snapshot.cancel_event_id.is_none()
        {
            // A prior pre-Accept revocation already suppressed this claim.
            continue;
        }
        let prompt_started = receiver.ledger.prompt_started(&claim).await?;
        if !prompt_started {
            super::git_receipt_journal::initialize_for_unstarted_lifecycle(
                &lifecycle,
                &receiver.tenant.community_id,
                &receiver.agent_pubkey,
                &claim,
            )
            .map_err(|error| ReceiverError::Privilege(error.to_string()))?;
        }
        let emitter = JobEmitter::new(
            &request,
            claim.request_event_id.clone(),
            receiver.keys.clone(),
            receiver.rest.clone(),
            lifecycle.clone(),
            receiver
                .grants
                .capabilities_for(&request)
                .unwrap_or_default(),
            claim.digest.clone(),
            receiver.sponsor.clone(),
        );
        let pending = snapshot.pending_outbox;
        if let Some(event) = pending.as_ref() {
            super::validate_pending_terminal_git_effect(
                &lifecycle,
                event,
                &snapshot.head_event_id,
                &claim,
                &receiver.agent_pubkey,
                &receiver.sponsor,
            )?;
            if let Err(error) = emitter.retry_lifecycle_outbox().await {
                first_error.get_or_insert_with(|| error.to_string());
                continue;
            }
            if lifecycle.snapshot().await?.2 {
                continue;
            }
        }
        let pending_cancel = lifecycle.pending_cancel().await?.is_some();
        let result = if pending_cancel {
            (if prompt_started {
                super::cancel::CancellationTerminal::interrupted_full_host_turn()
            } else {
                super::cancel::terminal_for_lifecycle(
                    &lifecycle,
                    &receiver.tenant.community_id,
                    &receiver.agent_pubkey,
                    &claim,
                )
            })
            .publish(&emitter)
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
