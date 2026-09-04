use buzz_core::job::{JobControlAction, JobEvent};
use nostr::Event;
use uuid::Uuid;

use super::lifecycle::CancelDecision;
use super::{JobEmitter, JobReceiver, ReceiverError};
use crate::scope::SessionScope;

pub struct JobCancel {
    pub scope: SessionScope,
    pub request_event_id: String,
    pub emitter: JobEmitter,
}

pub enum CancelOutcome {
    Consumed,
    Cancel(Box<JobCancel>),
}

pub(super) async fn handle(
    receiver: &JobReceiver,
    channel_id: Uuid,
    event: Event,
) -> Result<CancelOutcome, ReceiverError> {
    event
        .verify()
        .map_err(|error| ReceiverError::Receipt(format!("cancel signature: {error}")))?;
    let JobEvent::Control(control) =
        JobEvent::parse(&event).map_err(|error| ReceiverError::Receipt(error.to_string()))?
    else {
        return Ok(CancelOutcome::Consumed);
    };
    if control.action != JobControlAction::Cancel
        || control.followup.common.recipient_pubkey != receiver.agent_pubkey
        || control.followup.common.project.home_channel != channel_id.to_string()
    {
        return Ok(CancelOutcome::Consumed);
    }
    let Some(claim) = receiver
        .ledger
        .claim_for_request(&control.followup.request_event_id)
        .await?
    else {
        return Ok(CancelOutcome::Consumed);
    };
    if claim.community != receiver.tenant.community_id {
        return Ok(CancelOutcome::Consumed);
    }
    let JobEvent::Request(request) = JobEvent::parse(&claim.request_event)
        .map_err(|error| ReceiverError::Receipt(error.to_string()))?
    else {
        return Err(ReceiverError::Receipt(
            "stored claim is not a request".into(),
        ));
    };
    if control.followup.request_event_id != claim.request_event_id
        || control.followup.common != request.common
        || control.followup.common.sender_pubkey != claim.requester
    {
        return Ok(CancelOutcome::Consumed);
    }
    let Some(prior_event_id) = control.followup.prior_event_id.as_deref() else {
        return Ok(CancelOutcome::Consumed);
    };
    let lifecycle = receiver.ledger.lifecycle_store(&claim);
    lifecycle.initialize(claim.accepted.id.to_hex()).await?;
    match lifecycle
        .observe_cancel(event.id.to_hex(), prior_event_id.to_owned())
        .await?
    {
        CancelDecision::AlreadyTerminal => return Ok(CancelOutcome::Consumed),
        CancelDecision::Observed | CancelDecision::Replay => {}
    }
    let scope = SessionScope::Job {
        channel_id,
        operation_id: request.common.operation_id.clone(),
        request_event_id: claim.request_event_id.clone(),
    };
    let emitter = JobEmitter::new(
        &request,
        claim.request_event_id.clone(),
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
    Ok(CancelOutcome::Cancel(Box::new(JobCancel {
        scope,
        request_event_id: claim.request_event_id,
        emitter,
    })))
}
