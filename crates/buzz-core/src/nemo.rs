//! Exact public coordinates for the dedicated Nemo development workspace.
//!
//! This is intentionally specific rather than a generic implicit-authority
//! mechanism. A deployment must match every coordinate before the managed
//! workspace behavior applies.

/// Canonical relay used by the dedicated Nemo community.
pub const RELAY_URL: &str = "wss://buzz.mograph.life";
/// Host whose server-resolved tenant owns the dedicated Nemo workspace.
pub const RELAY_HOST: &str = "buzz.mograph.life";
/// Canonical NIP-MP Project address for Nemo.
pub const PROJECT_ADDRESS: &str =
    "30621:1c7b17a0f192078060df6a59865f3610919b161d6c4743478ddd62a7ba1cbedf:nemo";
/// Project home channel carrying signed A2A events.
pub const HOME_CHANNEL: &str = "40bdd8ad-8cf1-4757-bf43-9c7b301a9b50";
/// Canonical source repository.
pub const REPOSITORY: &str = "https://github.com/mysteropodes/nemo";
/// Human-readable project name.
pub const DISPLAY_NAME: &str = "Nemo";
/// Stable identity of the instructions compiled into a Buzz release.
pub const INSTRUCTION_SOURCE: &str = "builtin:docs/NEMO_WORKSPACE_INSTRUCTIONS.md";
/// Explicit metadata when the Buzz owner has not linked a GitHub login.
/// Relay authorization always uses `sponsor.pubkey`; this value is never an
/// identity or permission source.
pub const UNLINKED_GITHUB_LOGIN: &str = "unlinked";

/// True only for the exact dedicated Nemo Project and repository tuple.
pub fn matches(project_address: &str, home_channel: &str, repository: &str) -> bool {
    project_address == PROJECT_ADDRESS && home_channel == HOME_CHANNEL && repository == REPOSITORY
}

/// Portable directory component accepted by both Nemo A2A dispatch and
/// receiver worktree provisioning.
pub fn valid_worktree_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= crate::job::MAX_IDEMPOTENCY_KEY_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        && !value.starts_with('.')
        && !value.ends_with('.')
}

#[cfg(test)]
mod tests {
    #[test]
    fn worktree_components_are_portable_and_pathless() {
        for accepted in ["buzz-a2a", "worker_2", "release.3"] {
            assert!(super::valid_worktree_component(accepted), "{accepted}");
        }
        for rejected in ["", ".hidden", "trailing.", "team/worker", "team:worker"] {
            assert!(!super::valid_worktree_component(rejected), "{rejected}");
        }
    }
}
