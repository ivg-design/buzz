use std::path::{Path, PathBuf};

use super::super::command_search_dirs_for;

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
            command_search_dirs_for(
                workspace,
                Some(current),
                Some(bundled),
                debug_build,
            ),
            expected,
            "unexpected sidecar search order for debug_build={debug_build}"
        );
    }
}
