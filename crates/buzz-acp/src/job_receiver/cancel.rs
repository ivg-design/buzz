use buzz_core::job::{JobControlAction, JobEvent};
use nostr::Event;
use uuid::Uuid;

use super::ledger::ReceiptKind;
use super::lifecycle::CancelDecision;
use super::{JobEmitter, JobPrivilegeRegistry, JobReceiver, ReceiverError, TerminalDisposition};
use crate::scope::SessionScope;

pub struct JobCancel {
    pub scope: SessionScope,
    pub request_event_id: String,
    pub emitter: JobEmitter,
    pub terminal: CancellationTerminal,
}

#[derive(Clone, Debug)]
pub(crate) enum CancellationTerminal {
    Deferred {
        lifecycle: super::lifecycle::LifecycleStore,
        community_id: String,
        worker_pubkey: String,
        claim: Box<super::ledger::StoredClaim>,
        prompt_started: bool,
    },
    Cancelled,
    Indeterminate {
        code: String,
        message: String,
    },
}

impl CancellationTerminal {
    pub(super) fn interrupted_full_host_turn() -> Self {
        Self::Indeterminate {
            code: "cancelled_full_host_turn".into(),
            message: "Cancellation interrupted a full-host turn; native host side effects cannot be proven absent and require reconciliation".into(),
        }
    }

    pub(crate) fn resolve(self) -> Self {
        match self {
            Self::Deferred {
                lifecycle,
                community_id,
                worker_pubkey,
                claim,
                prompt_started,
            } => {
                if prompt_started {
                    Self::interrupted_full_host_turn()
                } else {
                    terminal_for_lifecycle(&lifecycle, &community_id, &worker_pubkey, &claim)
                }
            }
            terminal => terminal,
        }
    }

    pub(crate) async fn publish(
        self,
        emitter: &JobEmitter,
    ) -> Result<String, super::emitter::EmitError> {
        match self.resolve() {
            Self::Deferred { .. } => unreachable!("deferred cancellation terminal must resolve"),
            Self::Indeterminate { code, message } => {
                emitter
                    .terminal(TerminalDisposition::Indeterminate { code, message })
                    .await
            }
            Self::Cancelled => {
                emitter
                    .control(
                        JobControlAction::Cancelled,
                        "requester_cancelled".into(),
                        None,
                    )
                    .await
            }
        }
    }
}

pub enum CancelOutcome {
    Consumed,
    Cancel(Box<JobCancel>),
}

pub(super) async fn handle(
    receiver: &JobReceiver,
    privileges: &JobPrivilegeRegistry,
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
    let scope = SessionScope::Job {
        channel_id,
        operation_id: request.common.operation_id.clone(),
        request_event_id: claim.request_event_id.clone(),
    };
    let lifecycle = receiver.ledger.lifecycle_store(&claim);
    let processed_id = claim.processed.id.to_hex();
    let accepted_id = claim.accepted.id.to_hex();
    let accepted_acked = receiver
        .ledger
        .receipt_acked(&claim, ReceiptKind::Accepted)
        .await?;
    let anchor = if accepted_acked {
        accepted_id
    } else if prior_event_id == processed_id {
        // A valid Cancel delivered by the relay with Processed as its exact
        // predecessor proves Processed was stored and won the pre-Accept slot.
        receiver
            .ledger
            .mark_receipt_acked(&claim, ReceiptKind::Processed)
            .await?;
        processed_id
    } else if prior_event_id == accepted_id {
        // The successor proves an Accepted whose HTTP acknowledgement was
        // lost. Persist both inferred receipt acknowledgements before use.
        receiver
            .ledger
            .mark_receipt_acked(&claim, ReceiptKind::Processed)
            .await?;
        receiver
            .ledger
            .mark_receipt_acked(&claim, ReceiptKind::Accepted)
            .await?;
        accepted_id
    } else {
        return Ok(CancelOutcome::Consumed);
    };
    lifecycle.initialize(anchor).await?;
    let snapshot = lifecycle.privilege_snapshot().await?;
    if snapshot.terminal {
        return Ok(CancelOutcome::Consumed);
    }
    let event_id = event.id.to_hex();
    let is_replay = snapshot.cancel_event_id.as_deref() == Some(event_id.as_str());
    let pending_is_predecessor = snapshot
        .pending_outbox
        .as_ref()
        .is_some_and(|pending| pending.id.to_hex() == prior_event_id);
    if !is_replay && snapshot.head_event_id != prior_event_id && !pending_is_predecessor {
        return Ok(CancelOutcome::Consumed);
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

    // The relay has already durably stored this verified Cancel. Revoke all
    // active leases before advancing the local chain, but do not wait here:
    // a bounded drain timeout must never consume the Cancel without recording
    // it. The terminal is resolved from the receipt journal only after the
    // caller has proven every privileged child reaped.
    privileges.revoke(&scope);
    match lifecycle
        .observe_cancel(event_id, prior_event_id.to_owned())
        .await?
    {
        CancelDecision::AlreadyTerminal => return Ok(CancelOutcome::Consumed),
        CancelDecision::Observed | CancelDecision::Replay => {}
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
    Ok(CancelOutcome::Cancel(Box::new(JobCancel {
        scope,
        request_event_id: claim.request_event_id.clone(),
        emitter,
        terminal: CancellationTerminal::Deferred {
            lifecycle,
            community_id: receiver.tenant.community_id.clone(),
            worker_pubkey: receiver.agent_pubkey.clone(),
            claim: Box::new(claim),
            prompt_started,
        },
    })))
}

pub(super) fn terminal_for_lifecycle(
    lifecycle: &super::lifecycle::LifecycleStore,
    community_id: &str,
    worker_pubkey: &str,
    claim: &super::ledger::StoredClaim,
) -> CancellationTerminal {
    use super::git_receipt_journal::{summary_for_lifecycle, GitEffect};

    match summary_for_lifecycle(lifecycle, community_id, worker_pubkey, claim) {
        Ok(summary) if summary.effect == GitEffect::NotApplied => CancellationTerminal::Cancelled,
        Ok(summary) if summary.effect == GitEffect::Applied => {
            CancellationTerminal::Indeterminate {
                code: "cancel_after_applied_git_operation".into(),
                message: format!(
                    "Cancellation arrived after {} of {} privileged Git operations were durably observed as applied; repository state requires reconciliation",
                    summary.applied_count, summary.operation_count
                ),
            }
        }
        Ok(summary) => CancellationTerminal::Indeterminate {
            code: "cancel_during_ambiguous_git_operation".into(),
            message: format!(
                "Cancellation arrived while {} of {} privileged Git operations had an ambiguous effect; repository state requires reconciliation",
                summary.ambiguous_count, summary.operation_count
            ),
        },
        Err(_) => CancellationTerminal::Indeterminate {
            code: "cancel_during_ambiguous_git_operation".into(),
            message: "Cancellation arrived but the durable Git receipt journal was missing or invalid; repository state requires reconciliation".into(),
        },
    }
}
