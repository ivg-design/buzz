use std::path::{Path, PathBuf};

use nostr::Keys;
use zeroize::Zeroize;

#[cfg(test)]
use super::HARNESS_ONLY_ENV;
use super::{is_harness_only_env, GrantSet};

/// Validated configuration retained only by typed in-process tools.
///
/// This intentionally has no `Debug` implementation: it owns signing state.
pub struct TrustedConfig {
    pub(super) relay_url: String,
    pub(super) keys: Keys,
    pub(super) auth_tag: Option<nostr::Tag>,
    pub(super) auth_tag_json: Option<String>,
    pub(super) owner_pubkey: String,
    pub(super) owner_github_login: Option<String>,
    pub(super) grants: GrantSet,
    pub(super) session_channel_id: Option<String>,
    pub(super) session_thread_root_id: Option<String>,
    pub(super) job_operation_id: Option<String>,
    pub(super) job_request_event_id: Option<String>,
    pub(super) allow_insecure_loopback: bool,
}

impl TrustedConfig {
    /// Capture credentials exactly once and scrub all harness-only inputs before
    /// tracing, shim creation, or any child process can observe them.
    pub fn capture(cwd: &Path) -> Result<Option<Self>, String> {
        let relay_url = std::env::var("BUZZ_RELAY_URL").ok();
        let mut private_key = std::env::var("BUZZ_PRIVATE_KEY").ok();
        let mut auth_tag_raw = std::env::var("BUZZ_AUTH_TAG").ok();
        let grants_json = std::env::var("BUZZ_ACP_JOB_GRANTS_JSON").ok();
        let grants_file = std::env::var_os("BUZZ_ACP_JOB_GRANTS_FILE").map(PathBuf::from);
        let owner_github_login = nonempty_env("BUZZ_ACP_OWNER_GITHUB_LOGIN");
        let session_channel_id = nonempty_env("BUZZ_MCP_SESSION_CHANNEL_ID");
        let session_thread_root_id = nonempty_env("BUZZ_MCP_SESSION_THREAD_ROOT_ID");
        let job_operation_id = nonempty_env("BUZZ_MCP_JOB_OPERATION_ID");
        let job_request_event_id = nonempty_env("BUZZ_MCP_JOB_REQUEST_EVENT_ID");
        let allow_insecure_loopback = std::env::var("BUZZ_ACP_ALLOW_INSECURE_LOOPBACK_JOBS")
            .ok()
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes"));

        scrub_harness_environment();

        let Some(mut raw_key) = private_key.take() else {
            if let Some(ref mut raw) = auth_tag_raw {
                raw.zeroize();
            }
            return Ok(None);
        };
        let parsed = Keys::parse(raw_key.trim())
            .map_err(|_| "BUZZ_PRIVATE_KEY is not a valid Nostr signing key".to_owned());
        raw_key.zeroize();
        let keys = match parsed {
            Ok(keys) => keys,
            Err(error) => {
                zeroize_optional(&mut auth_tag_raw);
                return Err(error);
            }
        };
        let relay_url = match relay_url.filter(|value| !value.trim().is_empty()) {
            Some(relay_url) => relay_url,
            None => {
                zeroize_optional(&mut auth_tag_raw);
                return Err("BUZZ_RELAY_URL is required with BUZZ_PRIVATE_KEY".to_owned());
            }
        };

        let (auth_tag, auth_tag_json, owner_pubkey) = match auth_tag_raw.as_mut() {
            Some(raw) if !raw.trim().is_empty() => {
                let parsed = parse_auth(raw, &keys);
                raw.zeroize();
                let (tag, json, owner) = parsed?;
                (Some(tag), Some(json), owner)
            }
            Some(raw) => {
                raw.zeroize();
                (None, None, keys.public_key().to_hex())
            }
            None => (None, None, keys.public_key().to_hex()),
        };

        let grants = GrantSet::load(cwd, grants_json, grants_file)?;
        Ok(Some(Self {
            relay_url,
            keys,
            auth_tag,
            auth_tag_json,
            owner_pubkey,
            owner_github_login,
            grants,
            session_channel_id,
            session_thread_root_id,
            job_operation_id,
            job_request_event_id,
            allow_insecure_loopback,
        }))
    }
}

fn zeroize_optional(value: &mut Option<String>) {
    if let Some(value) = value {
        value.zeroize();
    }
}

fn parse_auth(raw: &str, keys: &Keys) -> Result<(nostr::Tag, String, String), String> {
    let tag =
        buzz_sdk::nip_oa::parse_auth_tag(raw).map_err(|_| "BUZZ_AUTH_TAG is invalid".to_owned())?;
    let owner = buzz_sdk::nip_oa::verify_auth_tag(raw, &keys.public_key())
        .map_err(|_| "BUZZ_AUTH_TAG does not authorize the configured signer".to_owned())?
        .to_hex();
    let json = serde_json::to_string(&tag)
        .map_err(|_| "BUZZ_AUTH_TAG could not be normalized".to_owned())?;
    Ok((tag, json, owner))
}

fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn scrub_harness_environment() {
    let names: Vec<std::ffi::OsString> = std::env::vars_os()
        .filter_map(|(name, _)| {
            name.to_str()
                .is_some_and(is_harness_only_env)
                .then_some(name)
        })
        .collect();
    for name in names {
        std::env::remove_var(name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn capture_without_key_still_scrubs_every_harness_variable() {
        let _guard = ENV_LOCK.lock().expect("lock");
        for name in HARNESS_ONLY_ENV {
            std::env::set_var(name, "sentinel-secret");
        }
        std::env::remove_var("BUZZ_PRIVATE_KEY");
        let result = TrustedConfig::capture(Path::new(".")).expect("capture");
        assert!(result.is_none());
        for name in HARNESS_ONLY_ENV {
            assert!(std::env::var_os(name).is_none(), "{name} survived scrub");
        }
    }

    #[test]
    fn capture_scrubs_future_namespaced_and_git_config_harness_values() {
        let _guard = ENV_LOCK.lock().expect("lock");
        let future = [
            "BUZZ_MCP_FUTURE_BINDING",
            "BUZZ_ACP_JOB_FUTURE_GRANT",
            "BUZZ_FUTURE_AUTH_PROOF",
            "NOSTR_FUTURE_SECRET",
            "GIT_CONFIG_KEY_19",
            "GIT_CONFIG_VALUE_19",
        ];
        for name in future {
            std::env::set_var(name, "sentinel-secret");
        }
        std::env::remove_var("BUZZ_PRIVATE_KEY");
        assert!(TrustedConfig::capture(Path::new("."))
            .expect("capture")
            .is_none());
        for name in future {
            assert!(std::env::var_os(name).is_none(), "{name} survived scrub");
        }
    }

    #[cfg(unix)]
    #[test]
    fn scrubbed_environment_is_absent_from_child_processes() {
        let _guard = ENV_LOCK.lock().expect("lock");
        for name in HARNESS_ONLY_ENV {
            std::env::set_var(name, format!("child-sentinel-{name}"));
        }
        std::env::remove_var("BUZZ_PRIVATE_KEY");
        assert!(
            TrustedConfig::capture(Path::new("."))
                .expect("capture")
                .is_none(),
            "no signer should be configured"
        );

        let output = std::process::Command::new("/usr/bin/env")
            .output()
            .expect("spawn environment reader");
        let child_environment = String::from_utf8(output.stdout).expect("UTF-8 environment");
        for name in HARNESS_ONLY_ENV {
            assert!(
                !child_environment
                    .lines()
                    .any(|line| line.starts_with(&format!("{name}="))),
                "{name} reached a child process"
            );
        }
    }
}
