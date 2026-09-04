//! Trusted, typed relay operations kept outside the model shell boundary.

mod credentials;
mod media;
mod relay;
mod scope;
mod tools;

pub use credentials::TrustedConfig;
pub use relay::{PublishedEvent, TrustedRelay};
pub use scope::{GrantMatch, GrantSet};
pub use tools::{
    cancel, dispatch, handoff, inbox, send_chat, status, A2aCancelParams, A2aDispatchParams,
    A2aHandoffParams, A2aInboxParams, A2aStatusParams, ChatSendParams,
};

/// Environment names that belong to the harness boundary and must never reach
/// model-controlled shell children.
pub(crate) const HARNESS_ONLY_ENV: &[&str] = &[
    "BUZZ_PRIVATE_KEY",
    "BUZZ_ACP_PRIVATE_KEY",
    "BUZZ_RELAY_PRIVATE_KEY",
    "BUZZ_MEMBER_PRIVATE_KEY",
    "BUZZ_PRIVATE_KEY_FILE",
    "BUZZ_ACP_PRIVATE_KEY_FILE",
    "BUZZ_NSEC",
    "BUZZ_AGENT_NSEC",
    "NOSTR_PRIVATE_KEY",
    "NOSTR_PRIVATE_KEY_FILE",
    "NOSTR_SECRET_KEY",
    "NOSTR_NSEC",
    "BUZZ_AUTH_TAG",
    "BUZZ_AGENT_AUTH_TAG",
    "BUZZ_AUTH_TAG_FILE",
    "BUZZ_AUTHORIZATION",
    "BUZZ_AUTHZ",
    "BUZZ_API_TOKEN",
    "BUZZ_ACP_API_TOKEN",
    "BUZZ_AUTH_TOKEN",
    "BUZZ_ACCESS_TOKEN",
    "BUZZ_ACP_JOB_GRANTS_JSON",
    "BUZZ_ACP_JOB_GRANTS_FILE",
    "BUZZ_ACP_JOB_LEDGER_DIR",
    "BUZZ_ACP_OWNER_GITHUB_LOGIN",
    "BUZZ_ACP_ALLOW_INSECURE_LOOPBACK_JOBS",
    "BUZZ_MCP_SESSION_CHANNEL_ID",
    "BUZZ_MCP_SESSION_THREAD_ROOT_ID",
    "BUZZ_MCP_JOB_OPERATION_ID",
    "BUZZ_MCP_JOB_REQUEST_EVENT_ID",
];

pub(crate) fn is_harness_only_env(name: &str) -> bool {
    if HARNESS_ONLY_ENV.contains(&name)
        || name.starts_with("BUZZ_MCP_")
        || name.starts_with("BUZZ_ACP_JOB_")
        || name.starts_with("GIT_CONFIG_")
    {
        return true;
    }
    (name.starts_with("BUZZ_") || name.starts_with("NOSTR_"))
        && [
            "PRIVATE_KEY",
            "SECRET",
            "NSEC",
            "AUTH",
            "TOKEN",
            "KEY_FILE",
            "KEYFILE",
        ]
        .iter()
        .any(|marker| name.contains(marker))
}
