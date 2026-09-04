use nostr::Event;

use buzz_core::job::{JobClaimStatus, JobControlAction, JobEvent};
use buzz_core::tenant::TenantContext;
use buzz_db::EventQuery;

use super::gate::JobAuthError;

pub(super) async fn validate_operation_history(
    tenant: &TenantContext,
    lock: &mut buzz_db::JobOperationLock,
    event: &Event,
    current: &JobEvent,
    channel_id: uuid::Uuid,
) -> Result<(), JobAuthError> {
    const MAX_HISTORY: usize = 1_000;
    let mut queries = Vec::new();
    match current {
        JobEvent::Request(request) => {
            if request.supersedes_event_id.is_none() && request.common.coordinator_epoch != 1 {
                return Err(JobAuthError::Invalid(
                    "initial job request coordinator_epoch must be 1".into(),
                ));
            }
            // Idempotency is community-wide for one authenticated requester.
            let mut by_key = EventQuery::for_community(tenant.community());
            by_key.kinds = Some(vec![43001]);
            by_key.pubkey = Some(event.pubkey.to_bytes().to_vec());
            by_key.custom_tag = Some(("k".into(), current.common().idempotency_key.clone()));
            queries.push(by_key);

            // Operation UUIDs are unique for a requester inside a Project.
            let mut by_operation = EventQuery::for_community(tenant.community());
            by_operation.kinds = Some(vec![43001]);
            by_operation.custom_tag = Some(("i".into(), current.common().operation_id.clone()));
            queries.push(by_operation);
        }
        _ => {
            let root = current
                .request_event_id()
                .ok_or_else(|| JobAuthError::Invalid("job follow-up is rootless".into()))?;
            let mut by_root = EventQuery::for_community(tenant.community());
            by_root.kinds = Some((43001..=43006).collect());
            by_root.channel_id = Some(channel_id);
            by_root.e_tags = Some(vec![root.to_owned()]);
            queries.push(by_root);
        }
    }
    let mut history = Vec::new();
    for mut query in queries {
        query.limit = Some((MAX_HISTORY + 1) as i64);
        query.max_limit = Some((MAX_HISTORY + 1) as i64);
        let stored = lock.query_events(&query).await.map_err(|error| {
            JobAuthError::Internal(format!("loading job operation history: {error}"))
        })?;
        if stored.len() > MAX_HISTORY {
            return Err(JobAuthError::Invalid(
                "job operation history exceeds the validation bound".into(),
            ));
        }
        for stored_event in stored {
            if stored_event.event.id == event.id
                || history
                    .iter()
                    .any(|(id, _)| id == &stored_event.event.id.to_hex())
            {
                continue;
            }
            let parsed = JobEvent::parse(&stored_event.event).map_err(|error| {
                JobAuthError::Internal(format!("stored job event failed validation: {error}"))
            })?;
            if parsed.common().coordinator_epoch == current.common().coordinator_epoch {
                history.push((stored_event.event.id.to_hex(), parsed));
            }
        }
    }

    match current {
        JobEvent::Request(_request) => {
            let duplicate = history
                .iter()
                .any(|(_, prior)| matches!(prior, JobEvent::Request(_)));
            if duplicate {
                return Err(JobAuthError::Invalid(
                    "coordinator epoch already has a request event".into(),
                ));
            }
        }
        _ => {
            let root = current
                .request_event_id()
                .ok_or_else(|| JobAuthError::Invalid("job follow-up is rootless".into()))?;
            let same_root: Vec<&(String, JobEvent)> = history
                .iter()
                .filter(|(_, prior)| prior.request_event_id() == Some(root))
                .collect();
            if same_root.iter().any(|(_, prior)| is_terminal(prior)) {
                return Err(JobAuthError::Invalid(
                    "job operation already has a terminal event".into(),
                ));
            }
            if matches!(current, JobEvent::Control(control) if control.action == JobControlAction::Cancel)
                && current.prior_event_id().is_none()
                && !same_root.is_empty()
            {
                return Err(JobAuthError::Invalid(
                    "root cancel is only valid before the first lifecycle receipt".into(),
                ));
            }
            if let JobEvent::Accepted(receipt) = current {
                match receipt.claim.status {
                    JobClaimStatus::Processed | JobClaimStatus::Declined
                        if !same_root.is_empty() =>
                    {
                        return Err(JobAuthError::Invalid(
                            "job request root already has a lifecycle successor".into(),
                        ));
                    }
                    JobClaimStatus::Accepted if same_root.iter().any(|(_, prior)| {
                        matches!(prior, JobEvent::Accepted(body) if body.claim.status == JobClaimStatus::Accepted)
                    }) => {
                        return Err(JobAuthError::Invalid(
                            "job request already has an accepted receipt".into(),
                        ));
                    }
                    _ => {}
                }
            }
            if let Some(prior_id) = current.prior_event_id() {
                let child_exists = same_root
                    .iter()
                    .any(|(_, prior)| prior.prior_event_id() == Some(prior_id));
                if child_exists {
                    return Err(JobAuthError::Invalid(
                        "prior_event_id already has a successor (fork rejected)".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn is_terminal(job: &JobEvent) -> bool {
    matches!(job, JobEvent::Result(_) | JobEvent::Error(_))
        || matches!(job, JobEvent::Accepted(body) if body.claim.status == JobClaimStatus::Declined)
        || matches!(
            job,
            JobEvent::Control(control)
                if matches!(
                    control.action,
                    JobControlAction::Cancelled
                        | JobControlAction::Release
                        | JobControlAction::Handoff
                ) || (control.action == JobControlAction::Cancel
                    && control.followup.prior_event_id.is_none())
        )
}
