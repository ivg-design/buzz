use buzz_core::job::{JobClaimStatus, JobControlAction, JobEvent};

use super::gate::JobAuthError;

pub(super) fn validate_transition(
    current: &JobEvent,
    request: &JobEvent,
) -> Result<(), JobAuthError> {
    let root = request.common();
    let next = current.common();
    if root.operation_id != next.operation_id
        || root.idempotency_key != next.idempotency_key
        || root.coordinator_epoch != next.coordinator_epoch
        || root.project != next.project
        || root.repository != next.repository
        || root.expires_at != next.expires_at
    {
        return Err(JobAuthError::Invalid(
            "job transition does not match its request scope".into(),
        ));
    }
    let requester_to_worker =
        next.sender_pubkey == root.sender_pubkey && next.recipient_pubkey == root.recipient_pubkey;
    let worker_to_requester =
        next.sender_pubkey == root.recipient_pubkey && next.recipient_pubkey == root.sender_pubkey;
    match current {
        JobEvent::Accepted(_)
        | JobEvent::Progress(_)
        | JobEvent::Result(_)
        | JobEvent::Error(_)
            if !worker_to_requester =>
        {
            Err(JobAuthError::Restricted(
                "only the addressed worker may author this job transition".into(),
            ))
        }
        JobEvent::Control(control) => match control.action {
            JobControlAction::Cancel if requester_to_worker => Ok(()),
            JobControlAction::Cancelled | JobControlAction::Release | JobControlAction::Handoff
                if worker_to_requester =>
            {
                Ok(())
            }
            _ => Err(JobAuthError::Restricted(
                "job control action is not authorized for this signer/addressee".into(),
            )),
        },
        JobEvent::Request(_) => Err(JobAuthError::Invalid(
            "request cannot be used as its own transition".into(),
        )),
        _ => Ok(()),
    }
}

pub(super) fn requires_predecessor(job: &JobEvent) -> bool {
    matches!(job, JobEvent::Accepted(body) if body.claim.status == JobClaimStatus::Accepted)
        || matches!(
            job,
            JobEvent::Progress(_) | JobEvent::Result(_) | JobEvent::Error(_)
        )
        || matches!(
            job,
            JobEvent::Control(control)
                if matches!(
                    control.action,
                    JobControlAction::Cancelled
                        | JobControlAction::Release
                        | JobControlAction::Handoff
                )
        )
}

pub(super) fn validate_predecessor(
    current: &JobEvent,
    prior: &JobEvent,
    request_id: &str,
) -> Result<(), JobAuthError> {
    if prior.common().operation_id != current.common().operation_id
        || prior.common().idempotency_key != current.common().idempotency_key
        || prior.request_event_id() != Some(request_id)
    {
        return Err(JobAuthError::Invalid(
            "prior_event_id belongs to a different operation".into(),
        ));
    }
    match current {
        JobEvent::Accepted(body) if body.claim.status == JobClaimStatus::Processed => Err(
            JobAuthError::Invalid("processed receipt must not carry prior_event_id".into()),
        ),
        JobEvent::Accepted(body) if body.claim.status == JobClaimStatus::Accepted => match prior {
            JobEvent::Accepted(prior_body)
                if prior_body.claim.status == JobClaimStatus::Processed =>
            {
                Ok(())
            }
            _ => Err(JobAuthError::Invalid(
                "accepted receipt must follow a processed receipt".into(),
            )),
        },
        JobEvent::Accepted(body) if body.claim.status == JobClaimStatus::Declined => Err(
            JobAuthError::Invalid("declined receipt must not carry prior_event_id".into()),
        ),
        JobEvent::Progress(_) | JobEvent::Result(_) | JobEvent::Error(_) => match prior {
            JobEvent::Accepted(prior_body)
                if prior_body.claim.status == JobClaimStatus::Accepted =>
            {
                Ok(())
            }
            JobEvent::Progress(_) => Ok(()),
            _ => Err(JobAuthError::Invalid(
                "job lifecycle event must follow an accepted receipt or progress event".into(),
            )),
        },
        JobEvent::Control(control) => match (control.action, prior) {
            (JobControlAction::Cancelled, JobEvent::Control(prior_control))
                if prior_control.action == JobControlAction::Cancel =>
            {
                Ok(())
            }
            (JobControlAction::Cancel, JobEvent::Accepted(_))
            | (JobControlAction::Cancel, JobEvent::Progress(_)) => Ok(()),
            (
                JobControlAction::Release | JobControlAction::Handoff,
                JobEvent::Accepted(prior_body),
            ) if prior_body.claim.status == JobClaimStatus::Accepted => Ok(()),
            (JobControlAction::Release | JobControlAction::Handoff, JobEvent::Progress(_)) => {
                Ok(())
            }
            (JobControlAction::Cancel, _) => Err(JobAuthError::Invalid(
                "cancel must follow a processed, accepted, or progress event".into(),
            )),
            (JobControlAction::Cancelled, _) => Err(JobAuthError::Invalid(
                "cancelled must follow a requester cancel event".into(),
            )),
            _ => Err(JobAuthError::Invalid(
                "release/handoff must follow an accepted receipt or progress event".into(),
            )),
        },
        JobEvent::Accepted(_) => Err(JobAuthError::Invalid(
            "invalid processed/accepted predecessor relation".into(),
        )),
        JobEvent::Request(_) => Err(JobAuthError::Invalid(
            "request cannot carry prior_event_id".into(),
        )),
    }
}
