use super::super::project_git_exec::build_test_git_auth_config;
use super::super::project_git_file_content::{read_local_preview_content, validate_repo_file_path};
use super::*;

#[test]
fn parse_ls_tree_keeps_paths_after_eager_preview_limit() {
    let repo_dir = tempfile::tempdir().expect("create temporary repository");
    std::fs::create_dir(repo_dir.path().join("src")).expect("create source directory");
    std::fs::write(repo_dir.path().join("README.md"), "# Deferred README")
        .expect("write deferred README");
    std::fs::write(
        repo_dir.path().join("src/application.rs"),
        "fn deferred() {}",
    )
    .expect("write deferred source file");
    let hidden_entries = (0..MAX_EAGER_FILE_PREVIEWS)
        .map(|index| {
            format!(
                "100644 blob {} 1\t.agents/generated-{index:03}.txt",
                "a".repeat(40)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let output = format!(
        "{hidden_entries}\n100644 blob {} 17\tREADME.md\n100644 blob {} 16\tsrc/application.rs",
        "b".repeat(40),
        "c".repeat(40)
    );

    let files = parse_ls_tree(&output, &std::collections::HashMap::new());

    assert_eq!(files.len(), MAX_EAGER_FILE_PREVIEWS + 2);
    let readme = files
        .iter()
        .find(|file| file.path == "README.md")
        .expect("README metadata remains visible");
    assert_eq!(readme.preview_content, None);
    assert_eq!(
        read_preview_content(repo_dir.path(), &readme.path, readme.size).as_deref(),
        Some("# Deferred README")
    );
    assert_eq!(
        files.last().map(|file| file.path.as_str()),
        Some("src/application.rs")
    );
    let source = files.last().expect("source metadata remains visible");
    assert_eq!(source.preview_content, None);
    assert_eq!(
        read_preview_content(repo_dir.path(), &source.path, source.size).as_deref(),
        Some("fn deferred() {}")
    );
}

#[test]
fn repo_file_paths_reject_traversal_and_absolute_paths() {
    assert!(validate_repo_file_path("src/application.rs").is_ok());
    assert!(validate_repo_file_path("../outside.txt").is_err());
    assert!(validate_repo_file_path("src/../outside.txt").is_err());
    assert!(validate_repo_file_path("/absolute.txt").is_err());
}

#[test]
fn parse_ls_tree_never_reads_blob_previews() {
    let non_blob_entries = (0..MAX_EAGER_FILE_PREVIEWS)
        .map(|index| {
            format!(
                "160000 commit {} -\tvendor/dependency-{index:03}",
                "a".repeat(40)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let output = format!(
        "{non_blob_entries}\n100644 blob {} 12\tapplication.rs",
        "b".repeat(40)
    );

    let files = parse_ls_tree(&output, &std::collections::HashMap::new());

    assert_eq!(
        files
            .last()
            .and_then(|file| file.preview_content.as_deref()),
        None
    );
    assert_eq!(files.last().and_then(|file| file.size), None);
}

#[test]
fn parse_worktree_files_counts_only_files_toward_eager_preview_limit() {
    let repo_dir = tempfile::tempdir().expect("create temporary repository");
    std::fs::create_dir(repo_dir.path().join("directory")).expect("create directory");
    let paths = (0..MAX_EAGER_FILE_PREVIEWS)
        .map(|index| {
            let path = format!("file-{index:03}.txt");
            std::fs::write(repo_dir.path().join(&path), "preview").expect("write preview file");
            path
        })
        .collect::<Vec<_>>();
    let output = std::iter::once("directory")
        .chain(paths.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join("\0");

    let files = parse_worktree_files(repo_dir.path(), &output, &std::collections::HashMap::new());

    assert_eq!(files.len(), MAX_EAGER_FILE_PREVIEWS);
    assert!(files.iter().all(|file| file.preview_content.is_some()));
}

#[test]
fn selected_local_branch_snapshot_and_content_do_not_move_the_worktree() {
    let auth = build_test_git_auth_config().expect("build test git config");
    let root = tempfile::tempdir().expect("create test directory");
    let checkout = root.path().join("checkout");
    let checkout_path = checkout.to_str().expect("checkout path");
    run_git(&["init", "--", checkout_path], None, &auth).expect("initialize checkout");

    std::fs::write(checkout.join("shared.txt"), "main committed\n").expect("write main file");
    std::fs::write(checkout.join("main-only.txt"), "main only\n").expect("write main-only file");
    run_git(
        &["add", "shared.txt", "main-only.txt"],
        Some(&checkout),
        &auth,
    )
    .expect("stage main files");
    run_git(
        &[
            "-c",
            "user.name=Buzz Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "Main content",
        ],
        Some(&checkout),
        &auth,
    )
    .expect("commit main content");
    run_git(&["branch", "-M", "main"], Some(&checkout), &auth).expect("name main branch");

    run_git(&["checkout", "-b", "feature"], Some(&checkout), &auth).expect("create feature branch");
    std::fs::write(checkout.join("shared.txt"), "feature committed\n").expect("write feature file");
    std::fs::remove_file(checkout.join("main-only.txt")).expect("remove main-only file");
    std::fs::write(checkout.join("feature-only.txt"), "feature only\n")
        .expect("write feature-only file");
    std::fs::write(checkout.join("feature notes.txt"), "feature notes\n")
        .expect("write spaced feature file");
    run_git(&["add", "-A"], Some(&checkout), &auth).expect("stage feature files");
    run_git(
        &[
            "-c",
            "user.name=Buzz Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "Feature content",
        ],
        Some(&checkout),
        &auth,
    )
    .expect("commit feature content");
    let feature_head = run_git(&["rev-parse", "HEAD"], Some(&checkout), &auth)
        .expect("read feature head")
        .trim()
        .to_string();

    run_git(&["checkout", "main"], Some(&checkout), &auth).expect("restore main branch");
    std::fs::write(checkout.join("shared.txt"), "main uncommitted\n")
        .expect("write dirty main file");
    let head_before = run_git(&["rev-parse", "HEAD"], Some(&checkout), &auth)
        .expect("read main head")
        .trim()
        .to_string();
    let status_before =
        run_git(&["status", "--porcelain"], Some(&checkout), &auth).expect("read main status");

    let feature = snapshot_from_worktree(&checkout, &auth, Some("feature"), Some("main"))
        .expect("snapshot feature branch");
    assert_eq!(
        feature
            .latest_commit
            .as_ref()
            .map(|commit| commit.hash.as_str()),
        Some(feature_head.as_str())
    );
    assert!(feature
        .files
        .iter()
        .any(|file| file.path == "feature-only.txt"));
    assert!(!feature
        .files
        .iter()
        .any(|file| file.path == "main-only.txt"));
    assert_eq!(
        read_local_preview_content(&checkout, "shared.txt", Some("feature"), &auth)
            .expect("read feature content")
            .as_deref(),
        Some("feature committed\n")
    );
    assert_eq!(
        read_local_preview_content(&checkout, "feature notes.txt", Some("feature"), &auth)
            .expect("read spaced feature path")
            .as_deref(),
        Some("feature notes\n")
    );

    let main = snapshot_from_worktree(&checkout, &auth, Some("main"), Some("main"))
        .expect("snapshot current main branch");
    assert!(main.files.iter().any(|file| file.path == "main-only.txt"));
    assert!(!main
        .files
        .iter()
        .any(|file| file.path == "feature-only.txt"));
    assert_eq!(
        main.files
            .iter()
            .find(|file| file.path == "shared.txt")
            .and_then(|file| file.preview_content.as_deref()),
        Some("main uncommitted\n")
    );
    assert_eq!(
        read_local_preview_content(&checkout, "shared.txt", Some("main"), &auth)
            .expect("read dirty main content")
            .as_deref(),
        Some("main uncommitted\n")
    );

    assert_eq!(
        run_git(&["branch", "--show-current"], Some(&checkout), &auth)
            .expect("read current branch")
            .trim(),
        "main"
    );
    assert_eq!(
        run_git(&["rev-parse", "HEAD"], Some(&checkout), &auth)
            .expect("read unchanged head")
            .trim(),
        head_before
    );
    assert_eq!(
        run_git(&["status", "--porcelain"], Some(&checkout), &auth).expect("read unchanged status"),
        status_before
    );
}

#[test]
fn sync_status_preserves_origin_and_surfaces_fetch_failures() {
    let auth = build_test_git_auth_config().expect("build test git config");
    let root = tempfile::tempdir().expect("create test directory");
    let remote = root.path().join("remote.git");
    let checkout = root.path().join("checkout");
    let remote_path = remote.to_str().expect("remote path");
    let checkout_path = checkout.to_str().expect("checkout path");

    run_git(&["init", "--bare", "--", remote_path], None, &auth).expect("initialize remote");
    run_git(&["init", "--", checkout_path], None, &auth).expect("initialize checkout");
    std::fs::write(checkout.join("README.md"), "tracked\n").expect("write fixture");
    run_git(&["add", "README.md"], Some(&checkout), &auth).expect("stage fixture");
    run_git(
        &[
            "-c",
            "user.name=Buzz Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "Initial commit",
        ],
        Some(&checkout),
        &auth,
    )
    .expect("commit fixture");
    run_git(&["branch", "-M", "main"], Some(&checkout), &auth).expect("rename branch");
    run_git(
        &["remote", "add", "origin", remote_path],
        Some(&checkout),
        &auth,
    )
    .expect("add remote");
    run_git(&["push", "origin", "main"], Some(&checkout), &auth).expect("seed remote");

    let upstream = root.path().join("upstream");
    let upstream_path = upstream.to_str().expect("upstream checkout path");
    run_git(
        &[
            "clone",
            "--branch",
            "main",
            "--",
            remote_path,
            upstream_path,
        ],
        None,
        &auth,
    )
    .expect("clone upstream fixture");
    std::fs::write(upstream.join("CHANGELOG.md"), "remote update\n").expect("write remote update");
    run_git(&["add", "CHANGELOG.md"], Some(&upstream), &auth).expect("stage remote update");
    run_git(
        &[
            "-c",
            "user.name=Buzz Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "Remote update",
        ],
        Some(&upstream),
        &auth,
    )
    .expect("commit remote update");
    run_git(&["push", "origin", "main"], Some(&upstream), &auth).expect("push remote update");
    let upstream_head = run_git(&["rev-parse", "HEAD"], Some(&upstream), &auth)
        .expect("read upstream head")
        .trim()
        .to_string();

    let status = compare_local_remote_status(&checkout, remote_path, Some("main"), None, &auth)
        .expect("refresh remote status");
    assert_eq!(status.remote_head.as_deref(), Some(upstream_head.as_str()));
    assert_eq!(status.behind_count, 1);
    assert_eq!(
        run_git(&["remote", "get-url", "origin"], Some(&checkout), &auth)
            .expect("read origin")
            .trim(),
        remote_path,
        "a status refresh must not rewrite origin",
    );

    std::fs::remove_dir_all(&remote).expect("remove remote fixture");
    let error = compare_local_remote_status(&checkout, remote_path, Some("main"), None, &auth)
        .err()
        .expect("a failed fetch must fail the status refresh");
    assert!(error.contains("query remote branches:"), "{error}");
}

#[test]
fn sync_and_pull_preserve_full_history_beyond_one_hundred_commits() {
    let auth = build_test_git_auth_config().expect("build test git config");
    let root = tempfile::tempdir().expect("create test directory");
    let remote = root.path().join("remote.git");
    let checkout = root.path().join("checkout");
    let upstream = root.path().join("upstream");
    let remote_path = remote.to_str().expect("remote path");
    let checkout_path = checkout.to_str().expect("checkout path");
    let upstream_path = upstream.to_str().expect("upstream path");
    run_git(&["init", "--bare", "--", remote_path], None, &auth).expect("init remote");
    run_git(&["init", "--", checkout_path], None, &auth).expect("init checkout");
    for index in 0..105 {
        run_git(
            &[
                "-c",
                "user.name=Buzz Test",
                "-c",
                "user.email=test@example.com",
                "commit",
                "--allow-empty",
                "-m",
                &format!("commit {index}"),
            ],
            Some(&checkout),
            &auth,
        )
        .expect("create history");
    }
    run_git(&["branch", "-M", "main"], Some(&checkout), &auth).expect("rename branch");
    run_git(
        &["remote", "add", "origin", remote_path],
        Some(&checkout),
        &auth,
    )
    .expect("add remote");
    run_git(&["push", "origin", "main"], Some(&checkout), &auth).expect("seed remote");
    run_git(
        &[
            "clone",
            "--branch",
            "main",
            "--",
            remote_path,
            upstream_path,
        ],
        None,
        &auth,
    )
    .expect("clone upstream");
    run_git(
        &[
            "-c",
            "user.name=Buzz Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "--allow-empty",
            "-m",
            "remote update",
        ],
        Some(&upstream),
        &auth,
    )
    .expect("commit upstream update");
    run_git(&["push", "origin", "main"], Some(&upstream), &auth).expect("push update");

    let status = compare_local_remote_status(&checkout, remote_path, Some("main"), None, &auth)
        .expect("refresh status");
    assert_eq!(status.behind_count, 1);
    assert!(!checkout.join(".git/shallow").exists());
    run_git(
        &[
            "pull",
            "--ff-only",
            "--no-tags",
            "--end-of-options",
            "origin",
            "main",
        ],
        Some(&checkout),
        &auth,
    )
    .expect("pull full history");
    assert!(!checkout.join(".git/shallow").exists());
    assert_eq!(
        run_git(&["rev-list", "--count", "HEAD"], Some(&checkout), &auth)
            .expect("count history")
            .trim(),
        "106"
    );
}

#[test]
fn sync_status_rejects_repository_local_origin_rewrites() {
    let auth = build_test_git_auth_config().expect("build test git config");
    let root = tempfile::tempdir().expect("create test directory");
    let remote = root.path().join("remote.git");
    let redirected = root.path().join("redirected.git");
    let checkout = root.path().join("checkout");
    let remote_path = remote.to_str().expect("remote path");
    let redirected_path = redirected.to_str().expect("redirected path");
    let checkout_path = checkout.to_str().expect("checkout path");

    run_git(&["init", "--bare", "--", remote_path], None, &auth).expect("initialize remote");
    run_git(&["init", "--bare", "--", redirected_path], None, &auth)
        .expect("initialize redirect target");
    run_git(&["init", "--", checkout_path], None, &auth).expect("initialize checkout");
    run_git(
        &["remote", "add", "origin", remote_path],
        Some(&checkout),
        &auth,
    )
    .expect("add expected origin");
    let rewrite_key = format!("url.{redirected_path}.insteadOf");
    run_git(
        &["config", "--local", rewrite_key.as_str(), remote_path],
        Some(&checkout),
        &auth,
    )
    .expect("install repository-local URL rewrite");
    assert_eq!(
        run_git(&["remote", "get-url", "origin"], Some(&checkout), &auth)
            .expect("resolve effective origin")
            .trim(),
        redirected_path,
    );

    let error = compare_local_remote_status(&checkout, remote_path, Some("main"), None, &auth)
        .err()
        .expect("rewritten origin must be rejected before network access");
    assert!(error.contains("disallowed network setting url."), "{error}");
}

#[test]
fn sync_status_rejects_repository_local_custom_remote_helpers() {
    let auth = build_test_git_auth_config().expect("build test git config");
    let root = tempfile::tempdir().expect("create test directory");
    let remote = root.path().join("remote.git");
    let checkout = root.path().join("checkout");
    let remote_path = remote.to_str().expect("remote path");
    let checkout_path = checkout.to_str().expect("checkout path");

    run_git(&["init", "--bare", "--", remote_path], None, &auth).expect("initialize remote");
    run_git(&["init", "--", checkout_path], None, &auth).expect("initialize checkout");
    run_git(
        &["remote", "add", "origin", remote_path],
        Some(&checkout),
        &auth,
    )
    .expect("add expected origin");
    run_git(
        &["config", "--local", "remote.origin.vcs", "ext"],
        Some(&checkout),
        &auth,
    )
    .expect("install repository-local custom helper");

    let error = compare_local_remote_status(&checkout, remote_path, Some("main"), None, &auth)
        .err()
        .expect("custom origin helpers must be rejected before network access");
    assert!(
        error.contains("disallowed network setting remote.origin.vcs"),
        "{error}"
    );
}
