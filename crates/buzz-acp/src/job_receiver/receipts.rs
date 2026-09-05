use super::emitter::publish_lifecycle_mirror;
use super::ledger::{ReceiptKind, StoredClaim, StoredDecline};
use super::{JobReceiver, ReceiverError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PublishOutcome {
    Accepted,
    CancelledBeforeAccept,
}

/// Publish the immutable claim receipts in causal order.
///
/// A relay acknowledgement is persisted before moving to the next receipt so
/// the background outbox retry only submits receipts that may still be
/// missing. An explicit duplicate request forces an exact-ID replay of both
/// receipts to give the requester a deterministic response.
pub(super) async fn publish(
    receiver: &JobReceiver,
    claim: &StoredClaim,
    force_replay: bool,
) -> Result<PublishOutcome, ReceiverError> {
    submit(receiver, claim, ReceiptKind::Processed, force_replay).await?;
    if cancelled_before_accept(receiver, claim).await? {
        return Ok(PublishOutcome::CancelledBeforeAccept);
    }
    submit(receiver, claim, ReceiptKind::Accepted, force_replay).await?;
    Ok(PublishOutcome::Accepted)
}

pub(super) async fn publish_decline(
    receiver: &JobReceiver,
    decline: &StoredDecline,
    force_replay: bool,
) -> Result<(), ReceiverError> {
    if !force_replay && receiver.ledger.decline_acked(decline).await? {
        return Ok(());
    }
    super::verified_durable_decline(decline, &receiver.agent_pubkey, &receiver.sponsor.pubkey)?;
    receiver
        .rest
        .submit_event_confirmed(&decline.declined)
        .await?;
    publish_lifecycle_mirror(&receiver.rest, &decline.declined, &receiver.keys)
        .await
        .map_err(|error| ReceiverError::Receipt(error.to_string()))?;
    receiver.ledger.mark_decline_acked(decline).await?;
    Ok(())
}

async fn cancelled_before_accept(
    receiver: &JobReceiver,
    claim: &StoredClaim,
) -> Result<bool, ReceiverError> {
    if receiver
        .ledger
        .receipt_acked(claim, ReceiptKind::Accepted)
        .await?
    {
        return Ok(false);
    }
    let lifecycle = receiver.ledger.lifecycle_store(claim);
    if !lifecycle.exists() {
        return Ok(false);
    }
    let snapshot = lifecycle.privilege_snapshot().await?;
    Ok(snapshot.accepted_event_id == claim.processed.id.to_hex())
}

async fn submit(
    receiver: &JobReceiver,
    claim: &StoredClaim,
    kind: ReceiptKind,
    force_replay: bool,
) -> Result<(), ReceiverError> {
    if !force_replay && receiver.ledger.receipt_acked(claim, kind).await? {
        return Ok(());
    }
    let event = match kind {
        ReceiptKind::Processed => &claim.processed,
        ReceiptKind::Accepted => &claim.accepted,
    };
    receiver.rest.submit_event_confirmed(event).await?;
    publish_lifecycle_mirror(&receiver.rest, event, &receiver.keys)
        .await
        .map_err(|error| ReceiverError::Receipt(error.to_string()))?;
    receiver.ledger.mark_receipt_acked(claim, kind).await?;
    Ok(())
}
