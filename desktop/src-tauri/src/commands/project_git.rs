use super::project_git_exec::{
    build_git_auth_config, build_git_clone_auth_config, clean_branch, clean_target_ref,
    ensure_safe_local_network_config, run_git, run_git_in_request,
    validate_local_clone_url_for_workspace, validate_workspace_clone_url, GitAuthConfig,
    GitRequestBudget,
};
use super::project_git_file_content::{
    checkout_project_repo, read_preview_content, RemoteBlobFilter,
};
use super::project_git_push::push_project_local_repository_blocking;
pub use super::project_git_types::{
    GitIdentityInfo, ProjectLocalRepoInfo, ProjectLocalRepoSnapshotInfo, ProjectRepoCommitInfo,
    ProjectRepoContributorInfo, ProjectRepoFileInfo, ProjectRepoPullResult, ProjectRepoPushResult,
    ProjectRepoSnapshotInfo, ProjectRepoSyncStatusInfo,
};
use super::project_repo_paths::{canonical_repos_roots, find_local_repo_dir, normalized_clone_url};
use crate::app_state::AppState;
use std::time::UNIX_EPOCH;
use tauri::{AppHandle, State};
use tauri_plugin_opener::OpenerExt;

// Bound eager content without truncating the repository tree.
const MAX_EAGER_FILE_PREVIEWS: usize = 250;

#[cfg(test)]
#[path = "project_git_tests.rs"]
mod tests;
fn parse_latest_commit(output: &str) -> Option<ProjectRepoCommitInfo> {
    let line = output.lines().next()?;
    let mut parts = line.split('\0');
    let hash = parts.next()?.to_string();
    let short_hash = parts.next()?.to_string();
    let author_name = parts.next()?.to_string();
    let author_email = parts.next()?.to_string();
    let timestamp = parts.next()?.parse::<i64>().ok()?;
    let subject = parts.next().unwrap_or_default().to_string();

    Some(ProjectRepoCommitInfo {
        hash,
        short_hash,
        author_name,
        author_email,
        timestamp,
        subject,
    })
}
fn short_hash(hash: &str) -> String {
    hash.chars().take(7).collect()
}

pub(crate) fn first_output_line(output: &str) -> Option<String> {
    output
        .lines()
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn parse_count(output: &str) -> usize {
    output.trim().parse::<usize>().unwrap_or_default()
}

fn has_uncommitted_changes(output: &str) -> bool {
    output
        .lines()
        .any(|line| !line.starts_with("??") && !line.trim().is_empty())
}

fn has_untracked_files(output: &str) -> bool {
    output.lines().any(|line| line.starts_with("??"))
}

fn parse_commits(output: &str) -> Vec<ProjectRepoCommitInfo> {
    output
        .lines()
        .filter_map(parse_latest_commit)
        .take(50)
        .collect()
}

fn parse_contributors(output: &str) -> Vec<ProjectRepoContributorInfo> {
    let mut contributors: std::collections::HashMap<String, ProjectRepoContributorInfo> =
        std::collections::HashMap::new();

    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let mut parts = line.split('\0');
        let name = parts.next().unwrap_or_default().trim().to_string();
        let email = parts.next().unwrap_or_default().trim().to_string();
        let timestamp = parts
            .next()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .unwrap_or_default();
        if name.is_empty() && email.is_empty() {
            continue;
        }

        let key = if email.is_empty() {
            name.to_lowercase()
        } else {
            email.to_lowercase()
        };
        contributors
            .entry(key)
            .and_modify(|contributor| {
                contributor.commit_count += 1;
                contributor.last_commit_at = contributor.last_commit_at.max(timestamp);
            })
            .or_insert(ProjectRepoContributorInfo {
                name,
                email,
                commit_count: 1,
                last_commit_at: timestamp,
            });
    }

    let mut contributors = contributors.into_values().collect::<Vec<_>>();
    contributors.sort_by(|left, right| {
        right
            .commit_count
            .cmp(&left.commit_count)
            .then_with(|| right.last_commit_at.cmp(&left.last_commit_at))
            .then_with(|| left.name.cmp(&right.name))
    });
    contributors.truncate(50);
    contributors
}

fn parse_latest_commit_by_path(
    output: &str,
) -> std::collections::HashMap<String, ProjectRepoCommitInfo> {
    let mut current_commit = None;
    let mut result = std::collections::HashMap::new();

    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        if line.contains('\0') {
            current_commit = parse_latest_commit(line);
            continue;
        }

        if let Some(commit) = &current_commit {
            result
                .entry(line.to_string())
                .or_insert_with(|| commit.clone());
        }
    }

    result
}

fn path_modified_at(path: &std::path::Path) -> Option<i64> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    modified
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
}

fn parse_worktree_files(
    repo_dir: &std::path::Path,
    output: &str,
    latest_commit_by_path: &std::collections::HashMap<String, ProjectRepoCommitInfo>,
) -> Vec<ProjectRepoFileInfo> {
    output
        .split('\0')
        .filter(|path| !path.trim().is_empty())
        .filter_map(|path| {
            let full_path = repo_dir.join(path);
            let metadata = std::fs::metadata(&full_path).ok()?;
            metadata.is_file().then_some((path, full_path, metadata))
        })
        .enumerate()
        .map(|(index, (path, full_path, metadata))| {
            let size = Some(metadata.len());
            let latest_commit = latest_commit_by_path.get(path).cloned();
            ProjectRepoFileInfo {
                path: path.to_string(),
                kind: "blob".to_string(),
                size,
                preview_content: (index < MAX_EAGER_FILE_PREVIEWS)
                    .then(|| read_preview_content(repo_dir, path, size))
                    .flatten(),
                last_changed_at: latest_commit
                    .as_ref()
                    .map(|commit| commit.timestamp)
                    .or_else(|| path_modified_at(&full_path)),
                latest_commit,
            }
        })
        .collect()
}

fn normalize_branch_name(branch: &str) -> &str {
    branch
        .trim()
        .strip_prefix("refs/heads/")
        .unwrap_or_else(|| branch.trim())
}

fn branch_activity_range(
    repo_dir: &std::path::Path,
    auth: &GitAuthConfig,
    branch_name: Option<&str>,
    base_branch: Option<&str>,
) -> Option<String> {
    let branch_name = branch_name.map(normalize_branch_name)?;
    let base_branch = base_branch.map(normalize_branch_name)?;

    if branch_name.is_empty() || base_branch.is_empty() || branch_name == base_branch {
        return None;
    }

    let remote_base_ref = format!("refs/remotes/origin/{base_branch}");
    if run_git(
        &["rev-parse", "--verify", "--quiet", remote_base_ref.as_str()],
        Some(repo_dir),
        auth,
    )
    .is_err()
    {
        return None;
    }

    Some(format!("origin/{base_branch}..HEAD"))
}

fn parse_ls_tree(
    output: &str,
    latest_commit_by_path: &std::collections::HashMap<String, ProjectRepoCommitInfo>,
) -> Vec<ProjectRepoFileInfo> {
    output
        .lines()
        .filter_map(|line| {
            let (meta, path) = line.split_once('\t')?;
            let mut parts = meta.split_whitespace();
            let _mode = parts.next()?;
            let kind = parts.next()?.to_string();
            let _object = parts.next()?;
            Some(ProjectRepoFileInfo {
                path: path.to_string(),
                kind,
                // Asking `ls-tree --long` for blob sizes hydrates every missing
                // object in a partial clone. Remote snapshots deliberately
                // expose path metadata only; file content has its own bounded
                // request path.
                size: None,
                preview_content: None,
                last_changed_at: latest_commit_by_path
                    .get(path)
                    .map(|commit| commit.timestamp),
                latest_commit: latest_commit_by_path.get(path).cloned(),
            })
        })
        .collect()
}

fn snapshot_from_repo(
    repo_dir: &std::path::Path,
    auth: &GitAuthConfig,
    branch_name: Option<&str>,
    base_branch: Option<&str>,
    budget: &mut GitRequestBudget,
) -> Result<ProjectRepoSnapshotInfo, String> {
    if run_git_in_request(
        &["rev-parse", "--verify", "--quiet", "HEAD"],
        Some(repo_dir),
        auth,
        budget,
    )
    .is_err()
    {
        return Ok(ProjectRepoSnapshotInfo {
            latest_commit: None,
            commits: Vec::new(),
            files: Vec::new(),
            contributors: Vec::new(),
        });
    }

    let latest_commit_output = run_git_in_request(
        &["log", "-1", "--format=%H%x00%h%x00%an%x00%ae%x00%at%x00%s"],
        Some(repo_dir),
        auth,
        budget,
    )
    .map_err(|error| format!("read latest repository commit: {error}"))?;
    let latest_commit = parse_latest_commit(&latest_commit_output)
        .ok_or_else(|| "Latest repository commit metadata was malformed.".to_string())?;
    let branch_activity_range = if let (Some(branch_name), Some(base_branch)) =
        (branch_name.map(normalize_branch_name), base_branch.map(normalize_branch_name))
    {
        if !branch_name.is_empty() && !base_branch.is_empty() && branch_name != base_branch {
            let remote_base_ref = format!("refs/remotes/origin/{base_branch}");
            run_git_in_request(
                &["rev-parse", "--verify", "--quiet", remote_base_ref.as_str()],
                Some(repo_dir),
                auth,
                budget,
            )
            .ok()
            .map(|_| format!("origin/{base_branch}..HEAD"))
        } else {
            None
        }
    } else {
        None
    };
    let branch_activity_ref = branch_activity_range.as_deref().unwrap_or("HEAD");
    let commits = run_git_in_request(
        &[
            "log",
            "--max-count=50",
            "--format=%H%x00%h%x00%an%x00%ae%x00%at%x00%s",
            branch_activity_ref,
        ],
        Some(repo_dir),
        auth,
        budget,
    )
    .map(|output| parse_commits(&output))
    .map_err(|error| format!("read repository commits: {error}"))?;
    let contributors = run_git_in_request(
        &[
            "log",
            "--max-count=100",
            "--format=%an%x00%ae%x00%at",
            branch_activity_ref,
        ],
        Some(repo_dir),
        auth,
        budget,
    )
    .map(|output| parse_contributors(&output))
    .map_err(|error| format!("read repository contributors: {error}"))?;
    let latest_commit_by_path = run_git_in_request(
        &[
            "log",
            "--max-count=100",
            "--format=%H%x00%h%x00%an%x00%ae%x00%at%x00%s",
            "--name-only",
            "--diff-filter=ACMRT",
            "--",
        ],
        Some(repo_dir),
        auth,
        budget,
    )
    .map(|output| parse_latest_commit_by_path(&output))
    .map_err(|error| format!("read repository path history: {error}"))?;
    let files = run_git_in_request(
        &["ls-tree", "-r", "HEAD"],
        Some(repo_dir),
        auth,
        budget,
    )
    .map(|output| parse_ls_tree(&output, &latest_commit_by_path))
    .map_err(|error| format!("read repository file tree: {error}"))?;

    Ok(ProjectRepoSnapshotInfo {
        latest_commit: Some(latest_commit),
        commits,
        files,
        contributors,
    })
}

fn snapshot_from_worktree(
    repo_dir: &std::path::Path,
    auth: &GitAuthConfig,
    branch_name: Option<&str>,
    base_branch: Option<&str>,
) -> Result<ProjectRepoSnapshotInfo, String> {
    let has_head = run_git(
        &["rev-parse", "--verify", "--quiet", "HEAD"],
        Some(repo_dir),
        auth,
    )
    .is_ok();
    let latest_commit = if has_head {
        let output = run_git(
            &["log", "-1", "--format=%H%x00%h%x00%an%x00%ae%x00%at%x00%s"],
            Some(repo_dir),
            auth,
        )
        .map_err(|error| format!("read latest local repository commit: {error}"))?;
        Some(
            parse_latest_commit(&output)
                .ok_or_else(|| "Latest local repository commit metadata was malformed.".to_string())?,
        )
    } else {
        None
    };
    let branch_activity_range = branch_activity_range(repo_dir, auth, branch_name, base_branch);
    let branch_activity_ref = branch_activity_range.as_deref().unwrap_or("HEAD");
    let (commits, contributors, latest_commit_by_path) = if latest_commit.is_some() {
        let commits = run_git(
            &[
                "log",
                "--max-count=50",
                "--format=%H%x00%h%x00%an%x00%ae%x00%at%x00%s",
                branch_activity_ref,
            ],
            Some(repo_dir),
            auth,
        )
        .map(|output| parse_commits(&output))
        .map_err(|error| format!("read local repository commits: {error}"))?;
        let contributors = run_git(
            &[
                "log",
                "--max-count=100",
                "--format=%an%x00%ae%x00%at",
                branch_activity_ref,
            ],
            Some(repo_dir),
            auth,
        )
        .map(|output| parse_contributors(&output))
        .map_err(|error| format!("read local repository contributors: {error}"))?;
        let latest_commit_by_path = run_git(
            &[
                "log",
                "--max-count=100",
                "--format=%H%x00%h%x00%an%x00%ae%x00%at%x00%s",
                "--name-only",
                "--diff-filter=ACMRT",
                "--",
            ],
            Some(repo_dir),
            auth,
        )
        .map(|output| parse_latest_commit_by_path(&output))
        .map_err(|error| format!("read local repository path history: {error}"))?;
        (commits, contributors, latest_commit_by_path)
    } else {
        (Vec::new(), Vec::new(), std::collections::HashMap::new())
    };

    let files = run_git(
        &[
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ],
        Some(repo_dir),
        auth,
    )
    .map(|output| parse_worktree_files(repo_dir, &output, &latest_commit_by_path))
    .map_err(|error| format!("read local repository file list: {error}"))?;

    Ok(ProjectRepoSnapshotInfo {
        latest_commit,
        commits,
        files,
        contributors,
    })
}

/// Normalizes a relay-supplied branch option through the shared
/// [`clean_branch`] validation, so every command applies the same character
/// allowlist and flag-injection rejection before the value reaches git.
pub(crate) fn normalize_branch_option(branch: Option<&str>) -> Option<String> {
    clean_branch(branch.map(str::to_string))
}

pub(crate) fn compare_local_remote_status(
    repo_dir: &std::path::Path,
    clone_url: &str,
    branch_name: Option<&str>,
    base_branch: Option<&str>,
    auth: &GitAuthConfig,
) -> Result<ProjectRepoSyncStatusInfo, String> {
    let local_branch = run_git(&["branch", "--show-current"], Some(repo_dir), auth)
        .ok()
        .and_then(|output| first_output_line(&output));
    let local_branches = run_git(
        &[
            "for-each-ref",
            "--count=200",
            "--format=%(refname:short)",
            "refs/heads/",
        ],
        Some(repo_dir),
        auth,
    )
    .map(|output| {
        output
            .lines()
            .filter_map(|branch| normalize_branch_option(Some(branch)))
            .collect()
    })
    .unwrap_or_default();
    // The local checkout's branch name is attacker-influencable (a hostile
    // remote can point HEAD at a flag-shaped refname), so it must pass the
    // same `clean_branch` validation as relay-supplied names before it is
    // ever handed to git as an argument.
    let branch = normalize_branch_option(branch_name)
        .or_else(|| normalize_branch_option(local_branch.as_deref()))
        .unwrap_or_else(|| "main".to_string());

    // Local discovery already proves that origin matches the project URL.
    // Status and Fetch are read operations, so they must never rewrite the
    // user's repository configuration.
    let base_branch =
        normalize_branch_option(base_branch).filter(|base_branch| *base_branch != branch);
    ensure_safe_local_network_config(repo_dir, auth)?;
    let effective_origin = run_git(&["remote", "get-url", "origin"], Some(repo_dir), auth)
        .map_err(|error| format!("read local checkout origin: {error}"))?;
    if normalized_clone_url(&effective_origin) != normalized_clone_url(clone_url) {
        return Err("Local checkout origin resolves to a different repository URL.".to_string());
    }
    let mut remote_ref_queries = vec![format!("refs/heads/{branch}")];
    if let Some(base_branch) = base_branch.as_deref() {
        remote_ref_queries.push(format!("refs/heads/{base_branch}"));
    }
    let mut remote_query_args = vec!["ls-remote", "--heads", "--end-of-options", "origin"];
    remote_query_args.extend(remote_ref_queries.iter().map(String::as_str));
    let remote_heads = run_git(&remote_query_args, Some(repo_dir), auth)
        .map_err(|error| format!("query remote branches: {error}"))?;
    let remote_has_branch = |candidate: &str| {
        let expected = format!("refs/heads/{candidate}");
        remote_heads
            .lines()
            .any(|line| line.split_whitespace().nth(1) == Some(expected.as_str()))
    };
    let remote_has_branches = if remote_heads.trim().is_empty() {
        !run_git(
            &["ls-remote", "--heads", "--end-of-options", "origin"],
            Some(repo_dir),
            auth,
        )
        .map_err(|error| format!("query remote branch existence: {error}"))?
        .trim()
        .is_empty()
    } else {
        true
    };
    let mut fetch_refs = Vec::with_capacity(2);
    if remote_has_branch(&branch) {
        fetch_refs.push(branch.as_str());
    } else {
        let stale_ref = format!("refs/remotes/origin/{branch}");
        run_git(
            &["update-ref", "-d", stale_ref.as_str()],
            Some(repo_dir),
            auth,
        )?;
    }
    if let Some(base_branch) = base_branch.as_deref() {
        if remote_has_branch(base_branch) {
            fetch_refs.push(base_branch);
        } else {
            let stale_ref = format!("refs/remotes/origin/{base_branch}");
            run_git(
                &["update-ref", "-d", stale_ref.as_str()],
                Some(repo_dir),
                auth,
            )?;
        }
    }
    if !fetch_refs.is_empty() {
        let mut fetch_args = vec![
            "fetch",
            "--quiet",
            "--no-tags",
            "--end-of-options",
            "origin",
        ];
        fetch_args.extend(fetch_refs);
        run_git(&fetch_args, Some(repo_dir), auth)
            .map_err(|error| format!("fetch remote state: {error}"))?;
    }

    let local_head = run_git(&["rev-parse", "HEAD"], Some(repo_dir), auth)
        .ok()
        .and_then(|output| first_output_line(&output));
    let remote_ref = format!("refs/remotes/origin/{branch}");
    let remote_head = run_git(
        &["rev-parse", "--verify", "--quiet", remote_ref.as_str()],
        Some(repo_dir),
        auth,
    )
    .ok()
    .and_then(|output| first_output_line(&output));
    // A legacy empty clone may have an unborn local `master` while the project
    // declares `main`. Permit that mismatch only when the remote has no branch
    // refs at all.
    let is_first_publish = remote_head.is_none() && !remote_has_branches;
    let merge_base = base_branch.as_deref().and_then(|base_branch| {
        run_git(
            &[
                "merge-base",
                "HEAD",
                format!("origin/{base_branch}").as_str(),
            ],
            Some(repo_dir),
            auth,
        )
        .ok()
        .and_then(|output| first_output_line(&output))
    });
    let status = run_git(&["status", "--porcelain"], Some(repo_dir), auth).unwrap_or_default();
    let has_uncommitted_changes = has_uncommitted_changes(&status);
    let has_untracked_files = has_untracked_files(&status);
    let ahead_count = match remote_head.as_deref() {
        Some(_) => run_git(
            &[
                "rev-list",
                "--count",
                format!("origin/{branch}..HEAD").as_str(),
            ],
            Some(repo_dir),
            auth,
        )
        .map(|output| parse_count(&output))
        .unwrap_or_default(),
        None => usize::from(local_head.is_some()),
    };
    let behind_count = match remote_head.as_deref() {
        Some(_) => run_git(
            &[
                "rev-list",
                "--count",
                format!("HEAD..origin/{branch}").as_str(),
            ],
            Some(repo_dir),
            auth,
        )
        .map(|output| parse_count(&output))
        .unwrap_or_default(),
        None => 0,
    };

    let push_block_reason = if local_head.is_none() {
        Some("No local commits to push.".to_string())
    } else if local_branch.as_deref() != Some(branch.as_str()) && !is_first_publish {
        Some(format!(
            "Local checkout is on a different branch than {branch}."
        ))
    } else if has_uncommitted_changes || has_untracked_files {
        Some("Commit or discard local changes before pushing.".to_string())
    } else if behind_count > 0 {
        Some("Pull or reconcile remote commits before pushing.".to_string())
    } else if ahead_count == 0 {
        Some("Local branch is already pushed.".to_string())
    } else {
        None
    };

    // Pulling is a fast-forward only merge of origin/<branch> into the
    // current checkout, so it is blocked whenever that would not apply
    // cleanly (diverged history, dirty worktree, branch mismatch).
    let pull_block_reason = if local_head.is_none() {
        Some("No local commits yet — clone instead of pulling.".to_string())
    } else if remote_head.is_none() {
        Some("Remote branch not found.".to_string())
    } else if behind_count == 0 {
        Some("Local branch is up to date.".to_string())
    } else if local_branch.as_deref() != Some(branch.as_str()) {
        Some(format!(
            "Local checkout is on a different branch than {branch}."
        ))
    } else if has_uncommitted_changes {
        Some("Commit or stash local changes before pulling.".to_string())
    } else if ahead_count > 0 {
        Some("Local and remote have diverged — reconcile in a terminal.".to_string())
    } else {
        None
    };

    Ok(ProjectRepoSyncStatusInfo {
        local_path: Some(repo_dir.display().to_string()),
        local_branch,
        local_branches,
        local_head: local_head.clone(),
        local_short_head: local_head.as_deref().map(short_hash),
        remote_branch: Some(branch),
        remote_head: remote_head.clone(),
        remote_short_head: remote_head.as_deref().map(short_hash),
        merge_base,
        ahead_count,
        behind_count,
        has_uncommitted_changes,
        has_untracked_files,
        can_push: push_block_reason.is_none(),
        push_block_reason,
        can_pull: pull_block_reason.is_none(),
        pull_block_reason,
    })
}

/// The viewer's configured git identity (`user.name` / `user.email`), used by
/// the frontend to attribute their own commits to their Buzz profile when git
/// author strings don't match any relay profile fields.
#[tauri::command]
pub async fn get_git_identity(state: State<'_, AppState>) -> Result<GitIdentityInfo, String> {
    let auth = build_git_auth_config(&state)?;
    tauri::async_runtime::spawn_blocking(move || {
        let read = |key: &str| {
            run_git(&["config", "--get", key], None, &auth)
                .ok()
                .map(|output| output.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        Ok(GitIdentityInfo {
            name: read("user.name"),
            email: read("user.email"),
        })
    })
    .await
    .map_err(|error| format!("git identity task failed: {error}"))?
}

#[tauri::command]
pub async fn get_project_repo_snapshot(
    clone_url: String,
    default_branch: Option<String>,
    base_branch: Option<String>,
    target_ref: Option<String>,
    target_commit: Option<String>,
    state: State<'_, AppState>,
) -> Result<ProjectRepoSnapshotInfo, String> {
    validate_local_clone_url_for_workspace(&clone_url, &state)?;
    // Public GitHub reads stay anonymous, so no workspace Nostr credential
    // helper or signing key is exposed to an external host.
    let auth = build_git_clone_auth_config(&clone_url, &state)?;
    let branch = clean_branch(default_branch);
    let base_branch = clean_branch(base_branch);
    let target_ref = clean_target_ref(target_ref);
    let target_commit = target_commit
        .map(|value| value.to_ascii_lowercase())
        .filter(|value| matches!(value.len(), 40 | 64))
        .filter(|value| value.chars().all(|c| c.is_ascii_hexdigit()));

    tauri::async_runtime::spawn_blocking(move || {
        let temp_dir = tempfile::tempdir().map_err(|error| format!("create temp dir: {error}"))?;
        let repo_dir = temp_dir.path().join("repo");
        let mut budget = GitRequestBudget::remote(temp_dir.path());
        checkout_project_repo(
            &repo_dir,
            &clone_url,
            branch.as_deref(),
            target_ref.as_deref(),
            target_commit.as_deref(),
            &auth,
            &mut budget,
            RemoteBlobFilter::MetadataOnly,
        )?;

        if let Some(base_branch) = base_branch
            .as_deref()
            .filter(|base_branch| Some(*base_branch) != branch.as_deref())
        {
            let base_refspec =
                format!("refs/heads/{base_branch}:refs/remotes/origin/{base_branch}");
            run_git_in_request(
                &[
                    "fetch",
                    "--depth=100",
                    "--no-tags",
                    "--end-of-options",
                    "origin",
                    base_refspec.as_str(),
                ],
                Some(&repo_dir),
                &auth,
                &mut budget,
            )
            .map_err(|error| format!("fetch repository base branch: {error}"))?;
        }

        snapshot_from_repo(
            &repo_dir,
            &auth,
            branch.as_deref(),
            base_branch.as_deref(),
            &mut budget,
        )
    })
    .await
    .map_err(|error| format!("repo snapshot task failed: {error}"))?
}

#[tauri::command]
pub async fn get_project_local_repo_snapshot(
    repos_dir: Option<String>,
    project_dtag: String,
    clone_url: Option<String>,
    default_branch: Option<String>,
    base_branch: Option<String>,
    state: State<'_, AppState>,
) -> Result<Option<ProjectLocalRepoSnapshotInfo>, String> {
    let auth = build_git_auth_config(&state)?;
    let branch = clean_branch(default_branch);
    let base_branch = clean_branch(base_branch);

    tauri::async_runtime::spawn_blocking(move || {
        let Some(repo_dir) =
            find_local_repo_dir(repos_dir.as_deref(), &project_dtag, clone_url.as_deref())?
        else {
            return Ok(None);
        };
        let snapshot =
            snapshot_from_worktree(&repo_dir, &auth, branch.as_deref(), base_branch.as_deref())?;
        Ok(Some(ProjectLocalRepoSnapshotInfo {
            path: repo_dir.display().to_string(),
            snapshot,
        }))
    })
    .await
    .map_err(|error| format!("local repo snapshot task failed: {error}"))?
}

#[tauri::command]
pub async fn list_project_local_repositories(
    repos_dir: Option<String>,
) -> Result<Vec<ProjectLocalRepoInfo>, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let repos_roots = canonical_repos_roots(repos_dir.as_deref())?;
        let mut seen_paths = std::collections::HashSet::new();
        let mut repos = Vec::new();
        for repos_root in repos_roots {
            let entries = std::fs::read_dir(&repos_root)
                .map_err(|error| format!("read reposDir: {error}"))?;
            for entry in entries.filter_map(Result::ok) {
                let Some(file_type) = entry.file_type().ok() else {
                    continue;
                };
                if !file_type.is_dir() && !file_type.is_symlink() {
                    continue;
                }
                let Ok(path) = entry.path().canonicalize() else {
                    continue;
                };
                if !path.starts_with(&repos_root) || !path.is_dir() || !path.join(".git").exists() {
                    continue;
                }
                if !seen_paths.insert(path.clone()) {
                    continue;
                }
                repos.push(ProjectLocalRepoInfo {
                    name: entry.file_name().to_string_lossy().to_string(),
                    path: path.display().to_string(),
                });
            }
        }
        repos.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(repos)
    })
    .await
    .map_err(|error| format!("local repo list task failed: {error}"))?
}

#[tauri::command]
pub async fn open_project_repository_folder(
    repos_dir: Option<String>,
    project_dtag: String,
    clone_url: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<(), String> {
    validate_local_clone_url_for_workspace(&clone_url, &state)?;
    let repo_dir = tauri::async_runtime::spawn_blocking(move || {
        find_local_repo_dir(repos_dir.as_deref(), &project_dtag, Some(&clone_url))?
            .ok_or_else(|| "No local checkout found.".to_string())
    })
    .await
    .map_err(|error| format!("local repo lookup task failed: {error}"))??;
    app.opener()
        .open_path(repo_dir.to_string_lossy(), None::<&str>)
        .map_err(|error| format!("open local repository folder: {error}"))
}

#[tauri::command]
pub async fn get_project_repo_sync_status(
    repos_dir: Option<String>,
    project_dtag: String,
    clone_url: String,
    branch_name: Option<String>,
    base_branch: Option<String>,
    state: State<'_, AppState>,
) -> Result<ProjectRepoSyncStatusInfo, String> {
    validate_local_clone_url_for_workspace(&clone_url, &state)?;
    let auth = build_git_clone_auth_config(&clone_url, &state)?;

    tauri::async_runtime::spawn_blocking(move || {
        let Some(repo_dir) =
            find_local_repo_dir(repos_dir.as_deref(), &project_dtag, Some(&clone_url))?
        else {
            return Ok(ProjectRepoSyncStatusInfo {
                local_path: None,
                local_branch: None,
                local_branches: Vec::new(),
                local_head: None,
                local_short_head: None,
                remote_branch: branch_name
                    .as_deref()
                    .and_then(|branch| normalize_branch_option(Some(branch))),
                remote_head: None,
                remote_short_head: None,
                merge_base: None,
                ahead_count: 0,
                behind_count: 0,
                has_uncommitted_changes: false,
                has_untracked_files: false,
                can_push: false,
                push_block_reason: Some("No local checkout found.".to_string()),
                can_pull: false,
                pull_block_reason: Some("No local checkout found.".to_string()),
            });
        };

        compare_local_remote_status(
            &repo_dir,
            &clone_url,
            branch_name.as_deref(),
            base_branch.as_deref(),
            &auth,
        )
    })
    .await
    .map_err(|error| format!("repo sync status task failed: {error}"))?
}

#[tauri::command]
pub async fn push_project_local_repository(
    repos_dir: Option<String>,
    project_dtag: String,
    clone_url: String,
    branch_name: Option<String>,
    base_branch: Option<String>,
    state: State<'_, AppState>,
) -> Result<ProjectRepoPushResult, String> {
    validate_workspace_clone_url(&clone_url, &state)?;
    let auth = build_git_auth_config(&state)?;

    tauri::async_runtime::spawn_blocking(move || {
        let Some(repo_dir) =
            find_local_repo_dir(repos_dir.as_deref(), &project_dtag, Some(&clone_url))?
        else {
            return Err("No local checkout found.".to_string());
        };
        push_project_local_repository_blocking(
            &repo_dir,
            clone_url,
            branch_name,
            base_branch,
            &auth,
        )
    })
    .await
    .map_err(|error| format!("repo push task failed: {error}"))?
}

/// Fast-forwards the local checkout to the remote branch head. Refuses to
/// run whenever the sync status reports the pull would not apply cleanly.
#[tauri::command]
pub async fn pull_project_local_repository(
    repos_dir: Option<String>,
    project_dtag: String,
    clone_url: String,
    branch_name: Option<String>,
    state: State<'_, AppState>,
) -> Result<ProjectRepoPullResult, String> {
    validate_local_clone_url_for_workspace(&clone_url, &state)?;
    let auth = build_git_clone_auth_config(&clone_url, &state)?;

    tauri::async_runtime::spawn_blocking(move || {
        let Some(repo_dir) =
            find_local_repo_dir(repos_dir.as_deref(), &project_dtag, Some(&clone_url))?
        else {
            return Err("No local checkout found.".to_string());
        };
        let status = compare_local_remote_status(
            &repo_dir,
            &clone_url,
            branch_name.as_deref(),
            None,
            &auth,
        )?;
        if !status.can_pull {
            return Err(status
                .pull_block_reason
                .unwrap_or_else(|| "Local checkout cannot be pulled.".to_string()));
        }
        let branch = status
            .remote_branch
            .as_deref()
            .ok_or_else(|| "No branch selected for pull.".to_string())?;
        run_git(
            &[
                "pull",
                "--ff-only",
                "--no-tags",
                "--end-of-options",
                "origin",
                branch,
            ],
            Some(&repo_dir),
            &auth,
        )?;

        Ok(ProjectRepoPullResult {
            pulled: true,
            message: format!("Pulled {branch} from remote."),
        })
    })
    .await
    .map_err(|error| format!("repo pull task failed: {error}"))?
}
