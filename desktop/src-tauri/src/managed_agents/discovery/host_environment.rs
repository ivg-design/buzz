//! Bounded reconstruction of the host CLI environment for desktop launches.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

/// Delimits the environment payload from startup text emitted by interactive
/// shell configuration. Neither startup text nor captured values are logged.
const HOST_ENV_MARKER: &[u8] = b"\0__BUZZ_HOST_ENV_V1__\0";
const HOST_ENV_COMMAND: &str = "command printf '\\0__BUZZ_HOST_ENV_V1__\\0'; command env -0";

fn parse_host_shell_environment(stdout: &[u8]) -> Option<BTreeMap<String, String>> {
    let marker_start = stdout
        .windows(HOST_ENV_MARKER.len())
        .position(|window| window == HOST_ENV_MARKER)?;
    let payload = &stdout[marker_start + HOST_ENV_MARKER.len()..];
    let mut environment = BTreeMap::new();
    for entry in payload
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
    {
        let Some(separator) = entry.iter().position(|byte| *byte == b'=') else {
            continue;
        };
        let Ok(key) = std::str::from_utf8(&entry[..separator]) else {
            continue;
        };
        let Ok(value) = std::str::from_utf8(&entry[separator + 1..]) else {
            continue;
        };
        if key.is_empty() || key.contains('=') || key.contains('\0') {
            continue;
        }
        environment.insert(key.to_string(), value.to_string());
    }
    Some(environment)
}

#[derive(Clone)]
struct HostEnvironmentCache {
    generation: u64,
    /// `None` means unprobed; `Some(None)` means the bounded probe failed.
    probed: Option<Option<BTreeMap<String, String>>>,
}

fn cache() -> &'static Mutex<HostEnvironmentCache> {
    static CACHE: OnceLock<Mutex<HostEnvironmentCache>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Mutex::new(HostEnvironmentCache {
            generation: 0,
            probed: None,
        })
    })
}

#[cfg(not(windows))]
fn fetch() -> Option<BTreeMap<String, String>> {
    let stdout =
        super::login_shell::run_bytes_in_login_shell(&["-l", "-i", "-c", HOST_ENV_COMMAND])?;
    parse_host_shell_environment(&stdout)
}

#[cfg(windows)]
fn fetch() -> Option<BTreeMap<String, String>> {
    // Native Windows children already inherit the GUI process environment.
    // Git Bash emits a POSIX environment whose paths are invalid for them.
    None
}

/// Return the exported environment seen by an ordinary interactive login-shell
/// CLI. This restores host configuration, credential-agent sockets and tool
/// settings that a Finder/desktop launch does not inherit. The bounded result is
/// cached; explicit agent and Desktop policy variables are layered on top by the
/// launch caller.
pub(crate) fn login_shell_environment() -> Option<BTreeMap<String, String>> {
    loop {
        let generation = {
            let guard = cache().lock().unwrap_or_else(|error| error.into_inner());
            if let Some(result) = &guard.probed {
                return result.clone();
            }
            guard.generation
        };

        let result = fetch();
        let mut guard = cache().lock().unwrap_or_else(|error| error.into_inner());
        if guard.generation != generation {
            continue;
        }
        match (&guard.probed, &result) {
            // A concurrent successful probe is authoritative over this miss.
            (Some(Some(_)), None) => {}
            _ => guard.probed = Some(result),
        }
        return guard.probed.clone().flatten();
    }
}

/// Invalidate the host environment alongside the existing login-shell PATH
/// cache after installs, retries, and Doctor refreshes.
pub(super) fn refresh_host_environment() {
    let mut guard = cache().lock().unwrap_or_else(|error| error.into_inner());
    guard.generation = guard.generation.wrapping_add(1);
    guard.probed = None;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_nul_delimited_environment_after_private_marker() {
        let stdout = b"shell startup text\n\0__BUZZ_HOST_ENV_V1__\0\
CODEX_HOME=/sentinel/codex\0CLAUDE_CONFIG_DIR=/sentinel/claude\0\
SSH_AGENT_SOCK=/sentinel/agent\0VALUE_WITH_EQUALS=a=b=c\0MALFORMED\0";

        let environment = parse_host_shell_environment(stdout).expect("marker must parse");
        assert_eq!(environment.len(), 4);
        assert_eq!(
            environment.get("CODEX_HOME").map(String::as_str),
            Some("/sentinel/codex")
        );
        assert_eq!(
            environment.get("CLAUDE_CONFIG_DIR").map(String::as_str),
            Some("/sentinel/claude")
        );
        assert_eq!(
            environment.get("SSH_AGENT_SOCK").map(String::as_str),
            Some("/sentinel/agent")
        );
        assert_eq!(
            environment.get("VALUE_WITH_EQUALS").map(String::as_str),
            Some("a=b=c")
        );
    }

    #[test]
    fn missing_marker_does_not_treat_shell_startup_text_as_environment() {
        assert!(parse_host_shell_environment(b"CODEX_HOME=/must/not/parse\0").is_none());
    }
}
