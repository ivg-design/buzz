//! Start-time bootstrap for Buzz's checksum-pinned ACP adapters.
//!
//! Provider CLIs and authentication remain user-owned readiness surfaces. This
//! module installs only Buzz's embedded Codex/Claude adapter plus the verified
//! Node runtime needed to execute it, then clears discovery caches before the
//! real agent launch proceeds.

use crate::managed_agents::{is_npm_global_install, KnownAcpRuntime};
use std::collections::BTreeMap;

use super::install_report::InstallReporter;
use super::managed_node::{
    ensure_managed_node_runtime_blocking, managed_claude_acp_install_command,
    managed_codex_acp_install_command, managed_node_runtime_ready, managed_node_runtime_supported,
    managed_npm_command, npm_eacces_hint, resolve_adapter_path,
};
use super::{plan_adapter_install, run_install_command_with_retry};

const BUNDLED_RUNTIME_IDS: &[&str] = &["codex", "claude"];

struct ActiveInstalls {
    runtimes: std::sync::Mutex<std::collections::HashSet<String>>,
    available: std::sync::Condvar,
}

fn active_installs() -> &'static ActiveInstalls {
    use std::collections::HashSet;
    use std::sync::{Condvar, Mutex, OnceLock};
    static ACTIVE: OnceLock<ActiveInstalls> = OnceLock::new();
    ACTIVE.get_or_init(|| ActiveInstalls {
        runtimes: Mutex::new(HashSet::new()),
        available: Condvar::new(),
    })
}

pub(super) struct ActiveInstallGuard(String);

impl Drop for ActiveInstallGuard {
    fn drop(&mut self) {
        let installs = active_installs();
        if let Ok(mut runtimes) = installs.runtimes.lock() {
            runtimes.remove(&self.0);
            installs.available.notify_all();
        }
    }
}

/// Reserve one runtime's installer. Interactive install requests fail fast so
/// the UI can report an already-running install; start-time bootstrap waits for
/// that same install and then re-checks the verified adapter before doing work.
pub(super) fn acquire_install_guard(
    runtime_id: &str,
    wait: bool,
) -> Result<ActiveInstallGuard, String> {
    let installs = active_installs();
    let mut runtimes = installs
        .runtimes
        .lock()
        .map_err(|_| "install lock poisoned".to_string())?;
    while runtimes.contains(runtime_id) {
        if !wait {
            return Err(format!(
                "an install is already in progress for {runtime_id}"
            ));
        }
        runtimes = installs
            .available
            .wait(runtimes)
            .map_err(|_| "install lock poisoned".to_string())?;
    }
    runtimes.insert(runtime_id.to_string());
    Ok(ActiveInstallGuard(runtime_id.to_string()))
}

/// Install a known runtime's ACP adapter without touching its provider CLI or
/// authentication state. Failures are appended to `steps` and reported as
/// `false`, preserving the explicit installer and automatic-start behavior at
/// one implementation seam.
pub(super) fn install_adapter_if_needed(
    runtime: &'static crate::managed_agents::KnownAcpRuntime,
    reporter: &InstallReporter,
    steps: &mut Vec<crate::managed_agents::InstallStepResult>,
) -> bool {
    let adapter_path = resolve_adapter_path(runtime.commands, runtime.adapter_install_commands);
    let adapter_probe_path = crate::managed_agents::readiness::cli_probe::augmented_path();
    let plan = plan_adapter_install(
        runtime.id,
        adapter_path.as_deref(),
        runtime.adapter_install_commands,
        adapter_probe_path.as_deref(),
    );
    let bundled_runtime = matches!(runtime.id, "codex" | "claude");
    if bundled_runtime && managed_node_runtime_supported() && !managed_node_runtime_ready() {
        if let Err(step) = ensure_managed_node_runtime_blocking() {
            reporter.record_step(steps, *step);
            return false;
        }
    }

    if let Some(cmds) = plan {
        let use_managed_npm =
            cmds.iter().any(|cmd| is_npm_global_install(cmd)) && managed_node_runtime_supported();

        for cmd in cmds {
            let planned = match if use_managed_npm
                && runtime.id == "codex"
                && cmd
                    .trim_start()
                    .starts_with("npm install -g @agentclientprotocol/codex-acp")
            {
                managed_codex_acp_install_command()
            } else if use_managed_npm
                && runtime.id == "claude"
                && cmd
                    .trim_start()
                    .starts_with("npm install -g @agentclientprotocol/claude-agent-acp")
            {
                managed_claude_acp_install_command()
            } else if use_managed_npm {
                managed_npm_command(cmd)
            } else {
                Ok(None)
            } {
                Ok(Some(command)) => command,
                Ok(None) => cmd.to_string(),
                Err(step) => {
                    reporter.record_step(steps, *step);
                    return false;
                }
            };

            let mut result = run_install_command_with_retry("adapter", &planned, reporter);
            if !result.success && result.hint.is_none() && is_npm_global_install(cmd) {
                result.hint = npm_eacces_hint(&result.stderr, cmd);
            }
            let success = result.success;
            steps.push(result);
            if !success {
                return false;
            }
        }
    }
    true
}

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
