//! Typed Desktop commands for the owner-selected Workspace Project.

use reqwest::Method;
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Manager, State};

use crate::{
    app_state::AppState,
    managed_agents::{
        load_workspace_project_for_relay, restart_managed_agent_runtime,
        save_workspace_project_for_relay, ManagedAgentRuntimeKey, WorkspaceProject,
    },
    relay::{
        assert_expected_relay_scope, assert_expected_signer, build_nip98_auth_header,
        classify_request_error, parse_json_response, relay_api_base_url_with_override,
        relay_error_message, relay_ws_url_with_override,
    },
};

const NEMO_WORKSPACE_INSTRUCTIONS: &str =
    include_str!("../../../../docs/NEMO_WORKSPACE_INSTRUCTIONS.md");
const NEMO_STATUS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetWorkspaceProjectInput {
    project: Option<WorkspaceProject>,
    expected_relay_url: String,
    expected_signer_pubkey: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceProjectState {
    relay_url: String,
    project: Option<WorkspaceProject>,
    codex_instruction_status: &'static str,
    codex_instruction_error: Option<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceProjectSaveResult {
    relay_url: String,
    project: Option<WorkspaceProject>,
    changed: bool,
    restarted_count: u32,
    failed_restart_count: u32,
    codex_instruction_status: &'static str,
    codex_instruction_error: Option<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NemoWorkspaceStatus {
    mode: &'static str,
    project_name: &'static str,
    repository: &'static str,
    checkout_root: Option<String>,
    repository_access: NemoCapabilityStatus,
    a2a: NemoCapabilityStatus,
    instructions: NemoInstructionStatus,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NemoCapabilityStatus {
    status: &'static str,
    error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NemoInstructionStatus {
    status: &'static str,
    source: Option<&'static str>,
    revision: Option<&'static str>,
    content: Option<&'static str>,
    error: Option<String>,
}

const CODEX_INSTRUCTION_UNAVAILABLE: &str =
    "Install Buzz's reviewed Codex adapter before starting managed Codex agents.";

fn codex_instruction_delivery_state(verified: bool) -> (&'static str, Option<&'static str>) {
    if verified {
        ("verified", None)
    } else {
        ("unavailable", Some(CODEX_INSTRUCTION_UNAVAILABLE))
    }
}

fn current_codex_instruction_delivery_state() -> (&'static str, Option<&'static str>) {
    codex_instruction_delivery_state(crate::managed_agents::resolve_command("codex-acp").is_some())
}

fn unavailable_nemo_status(error: impl Into<String>) -> NemoWorkspaceStatus {
    let error = error.into();
    NemoWorkspaceStatus {
        mode: "nemo",
        project_name: buzz_core_pkg::nemo::DISPLAY_NAME,
        repository: buzz_core_pkg::nemo::REPOSITORY,
        checkout_root: None,
        repository_access: NemoCapabilityStatus {
            status: "unavailable",
            error: Some(error.clone()),
        },
        a2a: NemoCapabilityStatus {
            status: "unavailable",
            error: Some(error.clone()),
        },
        instructions: NemoInstructionStatus {
            status: "unavailable",
            source: None,
            revision: None,
            content: None,
            error: Some(error),
        },
    }
}

async fn authenticated_community_context(
    state: &AppState,
    api_base: &str,
) -> Result<buzz_core_pkg::CommunityContext, String> {
    let url = format!("{}/api/context", api_base.trim_end_matches('/'));
    let auth = build_nip98_auth_header(&Method::GET, &url, &[], state)?;
    let response = state
        .http_client
        .get(&url)
        .header("Authorization", auth)
        .timeout(NEMO_STATUS_TIMEOUT)
        .send()
        .await
        .map_err(|error| classify_request_error(&error))?;
    if !response.status().is_success() {
        return Err(relay_error_message(response).await);
    }
    let context: buzz_core_pkg::CommunityContext = parse_json_response(response).await?;
    let expected_host = url::Url::parse(api_base)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .ok_or_else(|| "active community URL has no canonical host".to_owned())?;
    context.validate_binding(&expected_host, &state.signing_keys()?.public_key())?;
    Ok(context)
}

#[tauri::command]
pub async fn get_nemo_workspace_status(
    community_id: String,
    relay_url: String,
    state: State<'_, AppState>,
) -> Result<NemoWorkspaceStatus, String> {
    let requested_relay = match buzz_core_pkg::relay::normalize_relay_url(&relay_url) {
        Ok(relay) => relay,
        Err(_) => return Ok(unavailable_nemo_status("community relay is invalid")),
    };
    let active_relay =
        match buzz_core_pkg::relay::normalize_relay_url(&relay_ws_url_with_override(&state)) {
            Ok(relay) => relay,
            Err(_) => return Ok(unavailable_nemo_status("active community relay is invalid")),
        };
    if community_id.trim().is_empty() {
        return Ok(unavailable_nemo_status("community identity is missing"));
    }
    if requested_relay != active_relay || active_relay != buzz_core_pkg::nemo::RELAY_URL {
        return Ok(unavailable_nemo_status(
            "the active community is outside the managed Nemo workspace",
        ));
    }

    let api_base = relay_api_base_url_with_override(&state);
    let context = match authenticated_community_context(&state, &api_base).await {
        Ok(context) => context,
        Err(error) => {
            return Ok(unavailable_nemo_status(format!(
                "could not verify the active Nemo community: {error}"
            )))
        }
    };
    // `community_id` is the desktop's local saved-workspace identifier.  It
    // scopes the UI query/cache but is deliberately not compared with the
    // server-issued CommunityContext UUID: those identifiers live in
    // different namespaces.  Authority comes from the authenticated, signed
    // context bound to the configured relay and the active signing identity.
    if context.host != buzz_core_pkg::nemo::RELAY_HOST {
        return Ok(unavailable_nemo_status(
            "the active community does not match the managed Nemo community",
        ));
    }

    let checkout = tauri::async_runtime::spawn_blocking(|| {
        super::project_repo_paths::find_local_repo_dir(
            None,
            "nemo",
            Some(buzz_core_pkg::nemo::REPOSITORY),
        )
    })
    .await
    .map_err(|error| format!("Nemo checkout inspection failed: {error}"))??;
    let checkout_root = checkout.map(|path| path.to_string_lossy().into_owned());
    let repository_error = checkout_root
        .is_none()
        .then(|| "Clone the Nemo repository under the configured REPOS directory".to_owned());
    let repository_status = if repository_error.is_none() {
        "ready"
    } else {
        "unavailable"
    };

    Ok(NemoWorkspaceStatus {
        mode: "nemo",
        project_name: buzz_core_pkg::nemo::DISPLAY_NAME,
        repository: buzz_core_pkg::nemo::REPOSITORY,
        checkout_root,
        repository_access: NemoCapabilityStatus {
            status: repository_status,
            error: repository_error.clone(),
        },
        a2a: NemoCapabilityStatus {
            status: repository_status,
            error: repository_error,
        },
        instructions: NemoInstructionStatus {
            status: "verified",
            source: Some(buzz_core_pkg::nemo::INSTRUCTION_SOURCE),
            revision: None,
            content: Some(NEMO_WORKSPACE_INSTRUCTIONS.trim()),
            error: None,
        },
    })
}

#[tauri::command]
pub async fn get_workspace_project(
    state: State<'_, AppState>,
) -> Result<WorkspaceProjectState, String> {
    let _workspace_guard = state.workspace_apply_lock.clone().lock_owned().await;
    let relay_url = relay_ws_url_with_override(&state);
    let canonical_relay =
        buzz_core_pkg::relay::normalize_relay_url(&relay_url).map_err(|error| error.to_string())?;
    let relay_for_read = canonical_relay.clone();
    let project = tauri::async_runtime::spawn_blocking(move || {
        load_workspace_project_for_relay(&relay_for_read)
    })
    .await
    .map_err(|error| format!("Workspace Project read failed: {error}"))??;
    let (codex_instruction_status, codex_instruction_error) =
        current_codex_instruction_delivery_state();
    Ok(WorkspaceProjectState {
        relay_url: canonical_relay,
        project,
        codex_instruction_status,
        codex_instruction_error,
    })
}

#[tauri::command]
pub async fn set_workspace_project(
    input: SetWorkspaceProjectInput,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<WorkspaceProjectSaveResult, String> {
    // Serialize with workspace switching through persistence and every restart.
    // The pair relay and fallback owner identity therefore cannot change after
    // the caller's scope checks and before a replacement process is spawned.
    let _workspace_guard = state.workspace_apply_lock.clone().lock_owned().await;
    let relay_url = relay_ws_url_with_override(&state);
    let api_base = relay_api_base_url_with_override(&state);
    assert_expected_relay_scope(Some(&input.expected_relay_url), &api_base)?;
    let signer = state.signing_keys()?.public_key().to_hex();
    assert_expected_signer(Some(&input.expected_signer_pubkey), &signer)?;
    let canonical_relay =
        buzz_core_pkg::relay::normalize_relay_url(&relay_url).map_err(|error| error.to_string())?;

    let relay_for_write = canonical_relay.clone();
    let (project, changed) = tauri::async_runtime::spawn_blocking(move || {
        save_workspace_project_for_relay(&relay_for_write, input.project)
    })
    .await
    .map_err(|error| format!("Workspace Project write failed: {error}"))??;

    let (restarted_count, failed_restart_count) = if changed {
        restart_running_pairs(&app, &canonical_relay).await?
    } else {
        (0, 0)
    };
    let (codex_instruction_status, codex_instruction_error) =
        current_codex_instruction_delivery_state();
    Ok(WorkspaceProjectSaveResult {
        relay_url: canonical_relay,
        project,
        changed,
        restarted_count,
        failed_restart_count,
        codex_instruction_status,
        codex_instruction_error,
    })
}

async fn restart_running_pairs(app: &AppHandle, relay_url: &str) -> Result<(u32, u32), String> {
    let state = app.state::<AppState>();
    let live_keys = {
        let mut runtimes = state
            .managed_agent_processes
            .lock()
            .map_err(|error| error.to_string())?;
        runtimes
            .iter_mut()
            .filter_map(|(key, runtime)| {
                matches!(runtime.child.try_wait(), Ok(None)).then(|| key.clone())
            })
            .collect::<Vec<_>>()
    };
    let targets = restart_targets_for_relay(live_keys, relay_url)?;
    let mut restarted = 0u32;
    let mut failed = 0u32;
    for key in targets {
        match restart_managed_agent_runtime(key.pubkey, key.relay_url, app.clone()).await {
            Ok(_) => restarted += 1,
            Err(error) => {
                failed += 1;
                eprintln!(
                    "buzz-desktop: Workspace Project failed to restart managed agent: {error}"
                );
            }
        }
    }
    Ok((restarted, failed))
}

fn restart_targets_for_relay(
    keys: impl IntoIterator<Item = ManagedAgentRuntimeKey>,
    relay_url: &str,
) -> Result<Vec<ManagedAgentRuntimeKey>, String> {
    let canonical =
        buzz_core_pkg::relay::normalize_relay_url(relay_url).map_err(|error| error.to_string())?;
    let mut targets = keys
        .into_iter()
        .filter(|key| key.relay_url == canonical)
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| left.pubkey.cmp(&right.pubkey));
    targets.dedup();
    Ok(targets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_plan_applies_each_agent_once_and_never_crosses_relays() {
        let a = ManagedAgentRuntimeKey::new("a".repeat(64), "wss://a.example").unwrap();
        let b = ManagedAgentRuntimeKey::new("b".repeat(64), "wss://a.example").unwrap();
        let other = ManagedAgentRuntimeKey::new("c".repeat(64), "wss://b.example").unwrap();
        assert_eq!(
            restart_targets_for_relay(
                [b.clone(), a.clone(), other, a.clone()],
                "WSS://A.EXAMPLE/",
            )
            .unwrap(),
            vec![a, b]
        );
    }

    #[test]
    fn codex_policy_status_requires_verified_delivery() {
        assert_eq!(codex_instruction_delivery_state(true), ("verified", None));
        assert_eq!(
            codex_instruction_delivery_state(false),
            ("unavailable", Some(CODEX_INSTRUCTION_UNAVAILABLE))
        );
    }
}
