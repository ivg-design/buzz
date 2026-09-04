use buzz_core::job::{
    JobAccepted, JobControlAction, JobError, JobEvent, JobProgress, JobRequest, JobResult,
};
use buzz_core::kind::{
    KIND_JOB_ACCEPTED, KIND_JOB_ERROR, KIND_JOB_PROGRESS, KIND_JOB_REQUEST, KIND_JOB_RESULT,
};

use crate::client::BuzzClient;
use crate::error::CliError;
use crate::JobsCmd;

use super::publish::{
    parse_input, publish, publish_control, request_scope_digest, require_operation,
};
use super::query::{get, list};
use super::ParsedInput;

pub async fn dispatch(command: JobsCmd, client: &BuzzClient) -> Result<(), CliError> {
    match command {
        JobsCmd::Submit { input } => {
            let parsed: ParsedInput<JobRequest> = parse_input(&input, KIND_JOB_REQUEST)?;
            publish(client, JobEvent::Request(parsed.body), &parsed.raw, None).await
        }
        JobsCmd::List {
            project_address,
            recipient,
            state,
            cursor,
        } => list(client, project_address, recipient, state, cursor).await,
        JobsCmd::Get { operation_id } => get(client, &operation_id).await,
        JobsCmd::Accept {
            operation_id,
            input,
        } => {
            let parsed: ParsedInput<JobAccepted> = parse_input(&input, KIND_JOB_ACCEPTED)?;
            let mut body = parsed.body;
            require_operation(&operation_id, &body.followup.common.operation_id)?;
            body.claim.scope_digest =
                request_scope_digest(client, &body.followup.request_event_id).await?;
            publish(
                client,
                JobEvent::Accepted(body),
                &parsed.raw,
                Some(&operation_id),
            )
            .await
        }
        JobsCmd::Progress {
            operation_id,
            input,
        } => {
            let parsed: ParsedInput<JobProgress> = parse_input(&input, KIND_JOB_PROGRESS)?;
            let body = parsed.body;
            require_operation(&operation_id, &body.followup.common.operation_id)?;
            publish(
                client,
                JobEvent::Progress(body),
                &parsed.raw,
                Some(&operation_id),
            )
            .await
        }
        JobsCmd::Complete {
            operation_id,
            input,
        } => {
            let parsed: ParsedInput<JobResult> = parse_input(&input, KIND_JOB_RESULT)?;
            let body = parsed.body;
            require_operation(&operation_id, &body.followup.common.operation_id)?;
            publish(
                client,
                JobEvent::Result(body),
                &parsed.raw,
                Some(&operation_id),
            )
            .await
        }
        JobsCmd::Fail {
            operation_id,
            input,
        } => {
            let parsed: ParsedInput<JobError> = parse_input(&input, KIND_JOB_ERROR)?;
            let body = parsed.body;
            require_operation(&operation_id, &body.followup.common.operation_id)?;
            publish(
                client,
                JobEvent::Error(body),
                &parsed.raw,
                Some(&operation_id),
            )
            .await
        }
        JobsCmd::Cancel {
            operation_id,
            input,
        } => publish_control(client, &operation_id, &input, JobControlAction::Cancel).await,
        JobsCmd::AcknowledgeCancel {
            operation_id,
            input,
        } => publish_control(client, &operation_id, &input, JobControlAction::Cancelled).await,
        JobsCmd::Release {
            operation_id,
            input,
        } => publish_control(client, &operation_id, &input, JobControlAction::Release).await,
        JobsCmd::Handoff {
            operation_id,
            input,
        } => publish_control(client, &operation_id, &input, JobControlAction::Handoff).await,
    }
}
