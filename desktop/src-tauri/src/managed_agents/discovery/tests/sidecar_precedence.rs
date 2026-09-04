use std::path::{Path, PathBuf};

use super::super::command_search_dirs_for;

#[cfg(unix)]
#[test]
fn codex_auth_probe_fails_before_running_without_verified_native_cli() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("probe-ran");
    let adapter = dir.path().join("codex-acp");
    std::fs::write(
        &adapter,
        format!("#!/bin/sh\ntouch '{}'\nexit 0\n", marker.display()),
    )
    .unwrap();
    std::fs::set_permissions(&adapter, std::fs::Permissions::from_mode(0o755)).unwrap();
    let runtime = crate::managed_agents::known_acp_runtime_exact("codex").unwrap();

    let status = super::super::probe_auth_status(
        &adapter,
        &["codex-acp", "cli", "login", "status"],
        runtime,
    );

    assert!(
        matches!(
            status,
            crate::managed_agents::AuthStatus::ConfigInvalid { ref diagnostic }
                if diagnostic.contains("bundled Codex CLI")
        ),
        "unexpected status: {status:?}"
    );
    assert!(!marker.exists(), "the unbound adapter must never execute");
}

#[test]
fn sidecar_search_order_matches_the_build_profile() {
    let workspace = Path::new("workspace");
    let current = Path::new("working-directory");
    let bundled = Path::new("bundle/Contents/MacOS");

    let cases = [
        (
            false,
            vec![
                PathBuf::from("bundle/Contents/MacOS"),
                PathBuf::from("workspace/target/release"),
                PathBuf::from("workspace/target/debug"),
                PathBuf::from("working-directory/target/release"),
                PathBuf::from("working-directory/target/debug"),
            ],
        ),
        (
            true,
            vec![
                PathBuf::from("workspace/target/debug"),
                PathBuf::from("workspace/target/release"),
                PathBuf::from("working-directory/target/debug"),
                PathBuf::from("working-directory/target/release"),
                PathBuf::from("bundle/Contents/MacOS"),
            ],
        ),
    ];

    for (debug_build, expected) in cases {
        assert_eq!(
            command_search_dirs_for(workspace, Some(current), Some(bundled), debug_build,),
            expected,
            "unexpected sidecar search order for debug_build={debug_build}"
        );
    }
}
