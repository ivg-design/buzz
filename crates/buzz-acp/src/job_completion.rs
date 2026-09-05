//! Publish worker output and settle its lifecycle without blocking other agents.

use crate::{job_receiver, scope, PromptOutcome};
use std::time::Duration;

#[derive(Clone)]
pub(crate) enum DeferredJobTerminal {
    Cancellation(Box<job_receiver::CancellationTerminal>),
    Outcome(job_receiver::TerminalDisposition),
    WorkerOutput {
        capture: crate::acp::CapturedTurnOutput,
        completed: bool,
        fallback: job_receiver::TerminalDisposition,
    },
}

pub(crate) fn job_terminal_disposition(
    outcome: &PromptOutcome,
    prompt_not_attempted: bool,
    captured_text: Option<String>,
    operation_id: &str,
    request_event_id: &str,
    scope_digest: &str,
) -> job_receiver::TerminalDisposition {
    match outcome {
        PromptOutcome::Ok(_) => {
            let disposition = job_receiver::parse_terminal_outcome(
                captured_text,
                operation_id,
                request_event_id,
                scope_digest,
            );
            match disposition {
                job_receiver::TerminalDisposition::Failed {
                    retryable: true, ..
                } => job_receiver::TerminalDisposition::Indeterminate {
                    code: "retryable_failure_after_full_host_turn".into(),
                    message: "Worker requested a retry after a full-host turn; native host side effects cannot be proven absent, so automatic replay is unsafe".into(),
                },
                disposition => disposition,
            }
        }
        // Only an exact pre-prompt boundary proves a setup failure. A generic
        // ACP error after delivery cannot rule out non-Git side effects. The
        // finisher also reconciles any applied/ambiguous durable Git effects.
        PromptOutcome::Error(_) if prompt_not_attempted => {
            job_receiver::TerminalDisposition::Failed {
                code: "worker_startup_failed".into(),
                message: "Worker setup failed before the requested prompt was sent".into(),
                retryable: true,
            }
        }
        _ => job_receiver::TerminalDisposition::Indeterminate {
            code: "worker_turn_interrupted".into(),
            message: "Worker turn ended without a proven terminal outcome".into(),
        },
    }
}

#[cfg(test)]
mod job_terminal_disposition_tests {
    use super::*;
    use crate::acp;

    #[test]
    fn only_proven_preprompt_acp_error_is_retryable_failure() {
        let outcome = PromptOutcome::Error(acp::AcpError::Protocol(
            "session setup rejected permission mode".into(),
        ));
        let disposition = job_terminal_disposition(
            &outcome,
            true,
            None,
            "9064f66a-a18a-4f04-b85e-5c39b2b2a1ea",
            &"4".repeat(64),
            &"5".repeat(64),
        );
        assert!(matches!(
            disposition,
            job_receiver::TerminalDisposition::Failed {
                ref code,
                retryable: true,
                ..
            } if code == "worker_startup_failed"
        ));
        assert!(matches!(
            job_terminal_disposition(
                &outcome,
                false,
                None,
                "9064f66a-a18a-4f04-b85e-5c39b2b2a1ea",
                &"4".repeat(64),
                &"5".repeat(64),
            ),
            job_receiver::TerminalDisposition::Indeterminate { ref code, .. }
                if code == "worker_turn_interrupted"
        ));
    }

    #[test]
    fn retryable_worker_failure_after_full_host_prompt_is_indeterminate() {
        let operation_id = "9064f66a-a18a-4f04-b85e-5c39b2b2a1ea";
        let request_event_id = "4".repeat(64);
        let scope_digest = "5".repeat(64);
        let terminal = serde_json::json!({
            "schema_version": "buzz.job-outcome.v1",
            "operation_id": operation_id,
            "request_event_id": request_event_id,
            "scope_digest": scope_digest,
            "outcome": "failed",
            "code": "tool_unavailable",
            "reason": "required host tool was unavailable",
            "retryable": true
        });
        assert!(matches!(
            job_terminal_disposition(
                &PromptOutcome::Ok(acp::StopReason::EndTurn),
                false,
                Some(terminal.to_string()),
                operation_id,
                &request_event_id,
                &scope_digest,
            ),
            job_receiver::TerminalDisposition::Indeterminate { ref code, .. }
                if code == "retryable_failure_after_full_host_turn"
        ));
    }
}

pub(crate) fn spawn_job_terminal_finisher(
    privileges: job_receiver::JobPrivilegeRegistry,
    scope: scope::SessionScope,
    terminal: Option<(job_receiver::JobEmitter, DeferredJobTerminal)>,
) {
    tokio::spawn(async move {
        let mut retry_delay = Duration::from_millis(250);
        loop {
            match privileges.revoke_and_wait(&scope).await {
                Ok(()) => break,
                Err(error) => {
                    tracing::warn!(
                        scope = %scope.telemetry_label(),
                        "agent-job cancellation terminal remains deferred until privileged operations drain: {error}"
                    );
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
                }
            }
        }

        // Resolve every registry-backed fact while the drained capability is
        // still addressable, then revoke/remove it before any terminal event
        // becomes externally observable. The relay can expose a publish to a
        // subscriber before the HTTP acknowledgement returns, so removal
        // after `publish().await` leaves a real post-terminal privilege window.
        let effect = privileges.git_effect_summary(&scope);
        privileges.remove(&scope);
        let terminal = match terminal {
            Some((emitter, terminal)) => {
                let terminal = resolve_worker_output(&emitter, &scope, terminal).await;
                let terminal = match terminal {
                    DeferredJobTerminal::Cancellation(terminal) => {
                        DeferredJobTerminal::Cancellation(Box::new((*terminal).resolve()))
                    }
                    DeferredJobTerminal::Outcome(disposition) => DeferredJobTerminal::Outcome(
                        job_receiver::guard_terminal_with_git_effect(disposition, effect),
                    ),
                    DeferredJobTerminal::WorkerOutput { .. } => unreachable_worker_output(),
                };
                Some((emitter, terminal))
            }
            None => None,
        };

        if let Some((emitter, terminal)) = terminal {
            match emitter.is_terminal().await {
                Ok(true) => {}
                Ok(false) => {
                    let publish = match terminal {
                        DeferredJobTerminal::Cancellation(terminal) => {
                            (*terminal).publish(&emitter).await
                        }
                        DeferredJobTerminal::Outcome(disposition) => {
                            emitter.terminal(disposition).await
                        }
                        DeferredJobTerminal::WorkerOutput { .. } => {
                            emitter.terminal(worker_output_unavailable()).await
                        }
                    };
                    if let Err(error) = publish {
                        tracing::warn!(
                            scope = %scope.telemetry_label(),
                            "publishing drained agent-job terminal failed: {error}"
                        );
                    }
                }
                Err(error) => tracing::warn!(
                    scope = %scope.telemetry_label(),
                    "reading drained agent-job terminal state failed: {error}"
                ),
            }
        }
    });
}

fn worker_output_unavailable() -> job_receiver::TerminalDisposition {
    job_receiver::TerminalDisposition::Indeterminate {
        code: "worker_report_unavailable".into(),
        message: "The worker returned, but its report has not been confirmed in the task thread. Do not repeat actions before checking the work.".into(),
    }
}

fn unreachable_worker_output() -> DeferredJobTerminal {
    DeferredJobTerminal::Outcome(worker_output_unavailable())
}

async fn resolve_worker_output(
    emitter: &job_receiver::JobEmitter,
    scope: &scope::SessionScope,
    terminal: DeferredJobTerminal,
) -> DeferredJobTerminal {
    let DeferredJobTerminal::WorkerOutput {
        capture,
        completed,
        fallback,
    } = terminal
    else {
        return terminal;
    };
    let report = job_receiver::HumanJobReport::from_turn_output(
        capture.terminal_candidate.as_deref(),
        capture.substantive_text.as_deref(),
    );
    let report_id = match report {
        Some(report) => {
            let mut delay = Duration::from_millis(500);
            loop {
                match emitter.publish_human_report(report.clone()).await {
                    Ok(id) => break Some(id),
                    Err(error) => {
                        tracing::warn!(scope = %scope.telemetry_label(), "worker report delivery remains pending: {error}");
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_secs(30));
                    }
                }
            }
        }
        None => None,
    };
    if !completed {
        return DeferredJobTerminal::Outcome(fallback);
    }
    let scope::SessionScope::Job {
        operation_id,
        request_event_id,
        ..
    } = scope
    else {
        return DeferredJobTerminal::Outcome(worker_output_unavailable());
    };
    let disposition = job_receiver::parse_terminal_outcome_with_report(
        capture.terminal_candidate,
        operation_id,
        request_event_id,
        emitter.scope_digest(),
        report_id.as_deref(),
    );
    let disposition = match disposition {
        job_receiver::TerminalDisposition::Failed { retryable: true, .. } =>
            job_receiver::TerminalDisposition::Indeterminate {
                code: "retryable_failure_after_full_host_turn".into(),
                message: "The worker reported a failure after execution. Check existing effects before retrying.".into(),
            },
        disposition => disposition,
    };
    DeferredJobTerminal::Outcome(disposition)
}
