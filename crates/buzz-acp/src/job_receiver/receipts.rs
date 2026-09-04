use super::ledger::{ReceiptKind, StoredClaim};
use super::{JobReceiver, ReceiverError};

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
) -> Result<(), ReceiverError> {
    submit(receiver, claim, ReceiptKind::Processed, force_replay).await?;
    submit(receiver, claim, ReceiptKind::Accepted, force_replay).await
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
    receiver.ledger.mark_receipt_acked(claim, kind).await?;
    Ok(())
}
