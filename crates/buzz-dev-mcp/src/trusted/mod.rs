//! Trusted, typed relay operations kept outside the model shell boundary.

mod credentials;
mod git;
mod media;
mod peers;
mod privilege;
mod relay;
mod scope;
mod service;
mod tools;

pub(crate) use credentials::scrub_harness_environment;
pub use credentials::{HarnessTrustedIdentity, TrustedConfig, TrustedSessionScope};
pub use git::{ProjectGitCommitParams, ProjectGitParams};
pub use privilege::{
    JobPrivilegeGate, PrivilegeFuture, PrivilegedGitDisposition, PrivilegedGitOperationReceipt,
    PrivilegedOperationOutcome, ProjectGitOperation, TrustedGitOperationLease,
};
pub use relay::{PublishedEvent, TrustedRelay};
pub use scope::{GrantMatch, GrantSet};
pub use service::TrustedSessionMcp;
pub use tools::{
    cancel, dispatch, inbox, peers, send_chat, status, A2aCancelParams, A2aDispatchParams,
    A2aHandoffParams, A2aInboxParams, A2aPeersParams, A2aStatusParams, ChatSendParams,
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

/// Ambient GitHub credentials that must not cross into a model-controlled
/// process.  Typed Project Git captures only the credential record it needs
/// and passes that record over a one-shot descriptor.
const MODEL_SHELL_CREDENTIAL_ENV: &[&str] = &[
    "GH_TOKEN",
    "GITHUB_TOKEN",
    "GH_ENTERPRISE_TOKEN",
    "GITHUB_ENTERPRISE_TOKEN",
    "GIT_ASKPASS",
    "GIT_SSH",
    "GIT_SSH_COMMAND",
    "SSH_ASKPASS",
    "SSH_AUTH_SOCK",
    "GPG_AGENT_INFO",
    "GCM_CREDENTIAL_STORE",
    "GCM_GUI_PROMPT",
    "GCM_INTERACTIVE",
];

pub(crate) fn is_harness_only_env(name: &str) -> bool {
    let name = name.to_ascii_uppercase();
    if HARNESS_ONLY_ENV.contains(&name.as_str())
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

/// Remove every known or namespaced harness-only value from a model-controlled
/// async child process without mutating the harness process itself.
pub(crate) fn scrub_async_command_environment(command: &mut tokio::process::Command) {
    let names: Vec<std::ffi::OsString> = std::env::vars_os()
        .filter_map(|(name, _)| {
            name.to_str()
                .is_some_and(is_harness_only_env)
                .then_some(name)
        })
        .collect();
    for name in names {
        command.env_remove(name);
    }
    for name in MODEL_SHELL_CREDENTIAL_ENV {
        command.env_remove(name);
    }
}

#[cfg(all(test, unix))]
mod child_environment_tests {
    #[tokio::test]
    async fn real_child_cannot_read_future_harness_secret() {
        let name = format!("BUZZ_MCP_TEST_AUTH_{}", uuid::Uuid::new_v4());
        let secret = format!("sentinel-{}", uuid::Uuid::new_v4());
        std::env::set_var(&name, &secret);
        std::env::set_var("SSH_AUTH_SOCK", &secret);
        let mut command = tokio::process::Command::new("/usr/bin/env");
        super::scrub_async_command_environment(&mut command);
        let output = command.output().await.expect("spawn environment reader");
        std::env::remove_var(&name);
        std::env::remove_var("SSH_AUTH_SOCK");
        let child = String::from_utf8(output.stdout).expect("UTF-8 child environment");
        assert!(!child.contains(&name));
        assert!(!child.contains(&secret));
        assert!(!child.contains("SSH_AUTH_SOCK"));
    }
}
