//! Start-time bootstrap for Buzz's checksum-pinned ACP adapters.
//!
//! Provider CLIs and authentication remain user-owned readiness surfaces. This
//! module installs only Buzz's embedded Codex/Claude adapter plus the verified
//! Node runtime needed to execute it, then clears discovery caches before the
//! real agent launch proceeds.

use crate::managed_agents::KnownAcpRuntime;
use std::collections::BTreeMap;

use super::install_report::InstallReporter;
use super::managed_node::{
    managed_node_runtime_ready, managed_node_runtime_supported, resolve_adapter_path,
};
use super::{acquire_install_guard, install_adapter_if_needed, plan_adapter_install};

const BUNDLED_RUNTIME_IDS: &[&str] = &["codex", "claude"];

pub(crate) async fn ensure_record_bundled_adapter_for_start(
    app: &tauri::AppHandle,
    record: &crate::managed_agents::ManagedAgentRecord,
) -> Result<(), String> {
    let failures =
        ensure_records_bundled_adapters_for_start(app, std::slice::from_ref(record)).await;
    match failures.into_iter().next() {
        Some((_, error)) => Err(error),
        None => Ok(()),
    }
}

/// Prepare each distinct pinned adapter needed by `records` exactly once. The
/// returned failures are per agent so restore/reconcile can persist or display
/// one failed row without suppressing unrelated runtimes.
pub(crate) async fn ensure_records_bundled_adapters_for_start(
    app: &tauri::AppHandle,
    records: &[crate::managed_agents::ManagedAgentRecord],
) -> Vec<(String, String)> {
    let personas = crate::managed_agents::load_personas(app).unwrap_or_default();
    let global = crate::managed_agents::load_global_agent_config(app).unwrap_or_default();
    let mut commands = Vec::new();
    let mut failures = Vec::new();
    for record in records {
        match crate::managed_agents::resolve_effective_harness_descriptor(
            record, &personas, &global,
        ) {
            Ok(descriptor) => commands.push((record.pubkey.clone(), descriptor.command)),
            Err(error) => failures.push((
                record.pubkey.clone(),
                format!(
                    "cannot start agent {}: {}",
                    record.pubkey,
                    crate::managed_agents::user_facing_harness_error(&error)
                ),
            )),
        }
    }

    for (_, (command, pubkeys)) in group_bundled_bootstrap_targets(commands) {
        if let Err(error) = ensure_bundled_adapter_for_command(app, &command).await {
            failures.extend(pubkeys.into_iter().map(|pubkey| (pubkey, error.clone())));
        }
    }
    failures
}

fn group_bundled_bootstrap_targets(
    commands: impl IntoIterator<Item = (String, String)>,
) -> BTreeMap<&'static str, (String, Vec<String>)> {
    let mut targets: BTreeMap<&'static str, (String, Vec<String>)> = BTreeMap::new();
    for (pubkey, command) in commands {
        let Some(runtime_id) = bundled_adapter_runtime_id(&command) else {
            continue;
        };
        let entry = targets
            .entry(runtime_id)
            .or_insert_with(|| (command, Vec::new()));
        entry.1.push(pubkey);
    }
    targets
}

async fn ensure_bundled_adapter_for_command(
    app: &tauri::AppHandle,
    effective_command: &str,
) -> Result<(), String> {
    let Some(runtime_id) = bundled_adapter_runtime_id(effective_command) else {
        return Ok(());
    };

    let app = app.clone();
    tokio::task::spawn_blocking(move || ensure_bundled_adapter_blocking(&app, runtime_id))
        .await
        .map_err(|error| format!("adapter bootstrap task panicked: {error}"))?
}

fn bundled_adapter_runtime_id(effective_command: &str) -> Option<&'static str> {
    crate::managed_agents::known_acp_runtime(effective_command)
        .map(|runtime| runtime.id)
        .filter(|runtime_id| BUNDLED_RUNTIME_IDS.contains(runtime_id))
}

fn ensure_bundled_adapter_blocking(
    app: &tauri::AppHandle,
    runtime_id: &'static str,
) -> Result<(), String> {
    let runtime = crate::managed_agents::known_acp_runtime_exact(runtime_id)
        .ok_or_else(|| format!("unknown bundled ACP runtime: {runtime_id}"))?;

    crate::managed_agents::refresh_login_shell_path();
    crate::managed_agents::clear_resolve_cache();
    if bundled_adapter_is_ready(runtime) {
        return Ok(());
    }
    if !managed_node_runtime_supported() {
        return Err(format!(
            "Buzz does not ship a verified Node runtime for this platform; cannot prepare the pinned {runtime_id} ACP adapter"
        ));
    }

    // An explicit Settings install may already be preparing this runtime. Wait
    // for it, then re-check before materializing or invoking npm a second time.
    let _guard = acquire_install_guard(runtime_id, true)?;
    crate::managed_agents::clear_resolve_cache();
    if bundled_adapter_is_ready(runtime) {
        return Ok(());
    }

    let reporter = InstallReporter::for_run(app, runtime_id);
    let mut steps = Vec::new();
    if !install_adapter_if_needed(runtime, &reporter, &mut steps) {
        return Err(bootstrap_failure(runtime_id, &steps, reporter.log_path()));
    }

    // Both positive and negative command resolutions are cached. Invalidate
    // after npm's atomic prefix update, then verify the complete pinned tree.
    crate::managed_agents::clear_resolve_cache();
    if !bundled_adapter_is_ready(runtime) {
        return Err(bootstrap_failure(runtime_id, &steps, reporter.log_path()));
    }
    Ok(())
}

fn bundled_adapter_is_ready(runtime: &'static KnownAcpRuntime) -> bool {
    if !managed_node_runtime_ready() {
        return false;
    }
    let adapter = resolve_adapter_path(runtime.commands, runtime.adapter_install_commands);
    plan_adapter_install(
        runtime.id,
        adapter.as_deref(),
        runtime.adapter_install_commands,
        None,
    )
    .is_none()
}

fn bootstrap_failure(
    runtime_id: &str,
    steps: &[crate::managed_agents::InstallStepResult],
    log_path: Option<String>,
) -> String {
    let detail = steps
        .iter()
        .rev()
        .find(|step| !step.success)
        .map(|step| step.stderr.trim())
        .filter(|detail| !detail.is_empty())
        .unwrap_or("the installed adapter did not pass checksum verification");
    match log_path {
        Some(path) => format!(
            "failed to prepare the pinned {runtime_id} ACP adapter: {detail} (install log: {path})"
        ),
        None => format!("failed to prepare the pinned {runtime_id} ACP adapter: {detail}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_checksum_pinned_runtimes_bootstrap_on_start() {
        assert_eq!(bundled_adapter_runtime_id("codex-acp"), Some("codex"));
        assert_eq!(
            bundled_adapter_runtime_id("claude-agent-acp"),
            Some("claude")
        );
        assert_eq!(
            bundled_adapter_runtime_id("claude-code-acp"),
            Some("claude")
        );
        assert_eq!(bundled_adapter_runtime_id("goose"), None);
        assert_eq!(bundled_adapter_runtime_id("custom-acp"), None);
    }

    #[test]
    fn cold_restore_bootstraps_each_pinned_runtime_once() {
        let targets = group_bundled_bootstrap_targets([
            ("codex-one".into(), "codex-acp".into()),
            ("codex-two".into(), "codex-acp".into()),
            ("claude-one".into(), "claude-agent-acp".into()),
            ("goose-one".into(), "goose".into()),
        ]);

        assert_eq!(targets.len(), 2);
        assert_eq!(targets["codex"].1, ["codex-one", "codex-two"]);
        assert_eq!(targets["claude"].1, ["claude-one"]);
    }

    #[test]
    fn cold_auto_start_plans_adapter_only_for_missing_or_unverified_bundles() {
        let unverified = std::path::Path::new("/tmp/unverified-acp-adapter");
        for runtime_id in BUNDLED_RUNTIME_IDS {
            let runtime = crate::managed_agents::known_acp_runtime_exact(runtime_id).unwrap();
            for adapter in [None, Some(unverified)] {
                let plan = plan_adapter_install(
                    runtime_id,
                    adapter,
                    runtime.adapter_install_commands,
                    None,
                )
                .expect("a cold upgrade must prepare a missing or unverified bundle");
                assert!(plan.iter().any(|command| command.contains("acp")));
                assert!(
                    plan.iter()
                        .all(|command| !runtime.cli_install_commands.contains(command)),
                    "start bootstrap must never install the provider CLI"
                );
            }
        }
    }
}
