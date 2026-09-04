//! Setup-listener payload for a spawn whose agent is not ready to run.
//!
//! The desktop is the sole readiness source; buzz-acp only transports the
//! payload. Kept beside the spawn rather than inside it so the readiness →
//! JSON → env write path reads as one unit.

use crate::managed_agents::readiness::{EffectiveAgentEnv, EffectiveHarnessDescriptor};
use crate::managed_agents::{
    agent_readiness, AgentReadiness, KnownAcpRuntime, ManagedAgentRecord, Requirement,
};

/// Build the effective env the agent would have at start-time, run the
/// readiness predicate, and serialize any setup payload for the private
/// desktop-to-harness startup pipe.
///
/// Returns whether the payload was set — stamped on `ManagedAgentProcess` and
/// used by `install_acp_runtime` to target only stuck agents for auto-restart.
///
/// SECURITY: `BUZZ_ACP_SETUP_PAYLOAD` is in `RESERVED_ENV_KEYS` so user env
/// cannot set it, but we also explicitly remove it after writing user env to
/// guard against the parent-process environment. We then set it only when
/// desktop has computed `NotReady`.
///
/// The JSON format mirrors `setup_mode::SetupPayload` in buzz-acp:
///   `{ "agent_name": "...", "agent_pubkey": "...", "requirements": [{ "surface": "...", ... }] }`
pub(super) fn build_setup_payload(
    record: &ManagedAgentRecord,
    descriptor: &EffectiveHarnessDescriptor,
    runtime_meta: Option<&'static KnownAcpRuntime>,
) -> Option<String> {
    // Construct EffectiveAgentEnv from the descriptor the caller resolved — no
    // second resolver call; the descriptor's env is already the fully layered
    // result.
    let effective = EffectiveAgentEnv {
        env: descriptor.env.clone(),
        config_file_path: runtime_meta.and_then(|r| r.config_file_path),
        effective_command: descriptor.command.clone(),
    };
    // Compute the optional payload before touching the command.
    let setup_payload_json =
        if let AgentReadiness::NotReady { requirements } = agent_readiness(&effective) {
            let reqs: Vec<serde_json::Value> = requirements
                .into_iter()
                .map(|r| match r {
                    Requirement::NormalizedField { field } => serde_json::json!({
                        "surface": "normalized_field",
                        "field": field,
                    }),
                    Requirement::EnvKey { key } => serde_json::json!({
                        "surface": "env_key",
                        "key": key,
                    }),
                    Requirement::CliLogin {
                        probe_args,
                        setup_copy,
                        availability,
                    } => serde_json::json!({
                        "surface": "cli_login",
                        "probe_args": probe_args,
                        "setup_copy": setup_copy,
                        "availability": availability,
                    }),
                    Requirement::CliConfigInvalid {
                        probe_args,
                        setup_copy,
                        diagnostic,
                    } => serde_json::json!({
                        "surface": "cli_config_invalid",
                        "probe_args": probe_args,
                        "setup_copy": setup_copy,
                        "diagnostic": diagnostic,
                    }),
                    Requirement::GitBash => serde_json::json!({
                        "surface": "git_bash",
                    }),
                    Requirement::MissingBinary { command } => serde_json::json!({
                        "surface": "missing_binary",
                        "command": command,
                    }),
                })
                .collect();
            let payload = serde_json::json!({
                "agent_name": record.name,
                "agent_pubkey": record.pubkey,
                "requirements": reqs,
            });
            match serde_json::to_string(&payload) {
                Ok(json) => Some(json),
                Err(e) => {
                    eprintln!(
                        "buzz-desktop: failed to serialize setup payload for {}: {e}",
                        record.name
                    );
                    None
                }
            }
        } else {
            None
        };

    if setup_payload_json.is_some() {
        eprintln!(
            "buzz-desktop: agent {} not ready — spawning in setup-listener mode",
            record.name
        );
    }
    setup_payload_json
}
