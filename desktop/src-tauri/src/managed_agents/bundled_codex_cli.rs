//! Verified native Codex CLI shipped inside a release bundle.

use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
};

use super::bundled_codex_manifest::verify_bundle;

const EXPECTED_MANIFEST_SHA256: Option<&str> =
    option_env!("BUZZ_DESKTOP_BUNDLED_CODEX_CLI_MANIFEST_SHA256");
const EXPECTED_TARGET: Option<&str> =
    option_env!("BUZZ_DESKTOP_BUNDLED_CODEX_CLI_TARGET");

static RESOURCE_DIR: OnceLock<PathBuf> = OnceLock::new();
static VERIFIED_CLI: OnceLock<CliState> = OnceLock::new();

const CODEX_PATH_ENV: &str = "CODEX_PATH";

#[derive(Debug)]
enum CliState {
    NotConfigured,
    Ready(PathBuf),
    Invalid(String),
}

pub(crate) fn initialize_resource_dir(path: PathBuf) {
    let _ = RESOURCE_DIR.set(path);
}

fn cli_state() -> &'static CliState {
    VERIFIED_CLI.get_or_init(|| {
        let (Some(expected_sha), Some(expected_target)) =
            (EXPECTED_MANIFEST_SHA256, EXPECTED_TARGET)
        else {
            return CliState::NotConfigured;
        };
        let Some(resource_dir) = RESOURCE_DIR.get() else {
            return CliState::Invalid(
                "Tauri resource directory was not initialized before Codex launch".to_string(),
            );
        };
        match verify_bundle(
            &resource_dir.join("codex-cli"),
            Some(expected_sha),
            expected_target,
        ) {
            Ok((codex_path, _)) => CliState::Ready(codex_path),
            Err(error) => CliState::Invalid(error),
        }
    })
}

fn verified_cli_path_from_state(state: &CliState) -> Result<&Path, String> {
    match state {
        CliState::NotConfigured => {
            Err("verified bundled Codex CLI is unavailable in this build".to_string())
        }
        CliState::Ready(path) => Ok(path),
        CliState::Invalid(error) => Err(format!("invalid bundled Codex CLI: {error}")),
    }
}

fn configure_command_from_state(command: &mut Command, state: &CliState) -> Result<(), String> {
    // Record an explicit removal before consulting state. A missing or invalid
    // bundle must never fall back to an ambient executable selected by the
    // parent process.
    command.env_remove(CODEX_PATH_ENV);
    let path = verified_cli_path_from_state(state)?;
    command.env(CODEX_PATH_ENV, path);
    Ok(())
}

/// Return the verified native Codex executable for non-child launch surfaces.
pub(crate) fn verified_cli_path() -> Result<PathBuf, String> {
    verified_cli_path_from_state(cli_state()).map(Path::to_path_buf)
}

/// Bind an adapter process to the verified native Codex executable.
///
/// A debug build without staged resources remains buildable for unrelated
/// desktop work, but Codex surfaces report the runtime unavailable. Release
/// builds require a verified staged bundle at build time.
pub(crate) fn configure_command(command: &mut Command) -> Result<(), String> {
    configure_command_from_state(command, cli_state())
}

#[cfg(test)]
mod tests {
    use super::{configure_command_from_state, CliState, CODEX_PATH_ENV};
    use std::{ffi::OsString, process::Command};
    use std::path::Path;

    fn command_env(command: &Command, key: &str) -> Option<Option<OsString>> {
        command
            .get_envs()
            .find(|(candidate, _)| *candidate == key)
            .map(|(_, value)| value.map(OsString::from))
    }

    #[test]
    fn ready_state_replaces_ambient_codex_path_with_exact_verified_path() {
        let verified = Path::new("/Applications/Buzz App.app/Contents/Resources/codex");
        let mut command = Command::new("codex-acp");
        command.env(CODEX_PATH_ENV, "/tmp/ambient-codex");

        configure_command_from_state(&mut command, &CliState::Ready(verified.to_path_buf()))
            .unwrap();

        assert_eq!(
            command_env(&command, CODEX_PATH_ENV),
            Some(Some(verified.as_os_str().to_os_string()))
        );
    }

    #[test]
    fn unavailable_state_removes_ambient_codex_path_and_fails_closed() {
        let mut command = Command::new("codex-acp");
        command.env(CODEX_PATH_ENV, "/tmp/ambient-codex");

        let error = configure_command_from_state(&mut command, &CliState::NotConfigured)
            .unwrap_err();

        assert!(error.contains("unavailable"), "{error}");
        assert_eq!(command_env(&command, CODEX_PATH_ENV), Some(None));
    }

    #[test]
    fn invalid_state_removes_ambient_codex_path_and_reports_diagnostic() {
        let mut command = Command::new("codex-acp");
        command.env(CODEX_PATH_ENV, "/tmp/ambient-codex");

        let error = configure_command_from_state(
            &mut command,
            &CliState::Invalid("payload checksum mismatch".to_string()),
        )
        .unwrap_err();

        assert!(error.contains("payload checksum mismatch"), "{error}");
        assert_eq!(command_env(&command, CODEX_PATH_ENV), Some(None));
    }
}
