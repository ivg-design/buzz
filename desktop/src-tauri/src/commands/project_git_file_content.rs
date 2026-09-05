use super::project_git::first_output_line;
use super::project_git_exec::{
    build_git_auth_config, build_git_clone_auth_config, clean_branch, clean_target_ref, run_git,
    run_git_bytes, run_git_in_request, validate_local_clone_url_for_workspace, GitAuthConfig,
    GitRequestBudget,
};
use super::project_repo_paths::find_local_repo_dir;
use crate::app_state::AppState;
use tauri::State;

const MAX_PREVIEW_BYTES: u64 = 64 * 1024;
const REMOTE_HISTORY_DEPTH_ARG: &str = "--depth=100";
const REMOTE_SEED_DEPTH_ARG: &str = "--depth=1";

#[derive(Clone, Copy)]
pub(crate) enum RemoteBlobFilter {
    MetadataOnly,
    PreviewContent,
    DiffContent,
}

impl RemoteBlobFilter {
    fn argument(self) -> &'static str {
        match self {
            Self::MetadataOnly => "--filter=blob:none",
            Self::PreviewContent => "--filter=blob:limit=65536",
            Self::DiffContent => "--filter=blob:limit=1048576",
        }
    }
}

/// Arguments shared by every temporary remote checkout used for repository
/// browsing. The clone is partial, shallow, single-branch, tag-free, and does
/// not materialize a worktree. Callers fetch an explicit target afterward when
/// the one-commit seed is sufficient.
pub(crate) fn bounded_remote_clone_args<'a>(
    clone_url: &'a str,
    repo_path: &'a str,
    branch: Option<&'a str>,
    seed_only: bool,
    blob_filter: RemoteBlobFilter,
) -> Vec<&'a str> {
    let mut args = vec![
        "clone",
        blob_filter.argument(),
        if seed_only {
            REMOTE_SEED_DEPTH_ARG
        } else {
            REMOTE_HISTORY_DEPTH_ARG
        },
        "--single-branch",
        "--no-tags",
        "--no-checkout",
    ];
    if let Some(branch) = branch {
        args.push("--branch");
        args.push(branch);
    }
    args.extend(["--", clone_url, repo_path]);
    args
}

pub(crate) fn read_preview_content(
    repo_dir: &std::path::Path,
    path: &str,
    size: Option<u64>,
) -> Option<String> {
    if size.is_some_and(|value| value > MAX_PREVIEW_BYTES) {
        return None;
    }

    let full_path = repo_dir.join(path);
    if std::fs::symlink_metadata(&full_path)
        .ok()?
        .file_type()
        .is_symlink()
    {
        return None;
    }
    let normalized = full_path.canonicalize().ok()?;
    let repo_root = repo_dir.canonicalize().ok()?;
    if !normalized.starts_with(repo_root) {
        return None;
    }

    let metadata = std::fs::metadata(&normalized).ok()?;
    if !metadata.is_file() || metadata.len() > MAX_PREVIEW_BYTES {
        return None;
    }
    let bytes = std::fs::read(normalized).ok()?;
    if bytes.contains(&0) {
        return None;
    }
    String::from_utf8(bytes).ok()
}

pub(crate) fn read_local_preview_content(
    repo_dir: &std::path::Path,
    path: &str,
    branch: Option<&str>,
    auth: &GitAuthConfig,
) -> Result<Option<String>, String> {
    let current_branch = run_git(&["branch", "--show-current"], Some(repo_dir), auth)
        .ok()
        .and_then(|output| first_output_line(&output));
    let Some(branch) = branch.filter(|branch| current_branch.as_deref() != Some(*branch)) else {
        return Ok(read_preview_content(repo_dir, path, None));
    };
    let branch_ref = format!("refs/heads/{branch}");
    let commit_ref = format!("{branch_ref}^{{commit}}");
    run_git(
        &["rev-parse", "--verify", "--quiet", commit_ref.as_str()],
        Some(repo_dir),
        auth,
    )
    .map_err(|_| "The selected local repository branch was not found.".to_string())?;

    let tree_entry = run_git(
        &["ls-tree", "-z", branch_ref.as_str(), "--", path],
        Some(repo_dir),
        auth,
    )?;
    let Some(tree_entry) = tree_entry
        .strip_suffix('\0')
        .filter(|entry| !entry.contains('\0'))
    else {
        return Ok(None);
    };
    let Some(object) = tree_entry
        .split_once('\t')
        .and_then(|(metadata, listed_path)| {
            if listed_path != path {
                return None;
            }
            let mut parts = metadata.split_whitespace();
            let mode = parts.next()?;
            let kind = parts.next()?;
            let object = parts.next()?;
            (matches!(mode, "100644" | "100755") && kind == "blob").then_some(object)
        })
    else {
        return Ok(None);
    };
    let size = run_git(&["cat-file", "-s", object], Some(repo_dir), auth)?
        .trim()
        .parse::<u64>()
        .map_err(|_| "Requested repository file size was malformed.".to_string())?;
    if size > MAX_PREVIEW_BYTES {
        return Ok(None);
    }
    let bytes = run_git_bytes(&["cat-file", "blob", object], Some(repo_dir), auth)?;
    if bytes.len() as u64 != size || bytes.contains(&0) {
        return Ok(None);
    }
    Ok(String::from_utf8(bytes).ok())
}

pub(crate) fn validate_repo_file_path(path: &str) -> Result<(), String> {
    if path.is_empty()
        || std::path::Path::new(path)
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err("Repository file path must be a relative file path.".to_string());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn checkout_project_repo(
    repo_dir: &std::path::Path,
    clone_url: &str,
    branch: Option<&str>,
    target_ref: Option<&str>,
    target_commit: Option<&str>,
    auth: &GitAuthConfig,
    budget: &mut GitRequestBudget,
    blob_filter: RemoteBlobFilter,
) -> Result<(), String> {
    let repo_path = repo_dir
        .to_str()
        .ok_or_else(|| "temporary repository path is not UTF-8".to_string())?;
    let explicit_target = target_ref.or(target_commit);

    if let Some(fetch_ref) = explicit_target {
        let clone_args = bounded_remote_clone_args(clone_url, repo_path, None, true, blob_filter);
        run_git_in_request(&clone_args, None, auth, budget)?;
        run_git_in_request(
            &[
                "fetch",
                "--depth=100",
                "--no-tags",
                "--end-of-options",
                "origin",
                fetch_ref,
            ],
            Some(repo_dir),
            auth,
            budget,
        )?;
        if let Some(expected_commit) = target_commit {
            let fetched_commit =
                run_git_in_request(&["rev-parse", "FETCH_HEAD"], Some(repo_dir), auth, budget)
                    .ok()
                    .and_then(|output| first_output_line(&output))
                    .map(|commit| commit.to_ascii_lowercase())
                    .ok_or_else(|| "Could not resolve the requested repository ref.".to_string())?;
            if fetched_commit != expected_commit {
                return Err(
                    "The requested repository ref changed. Refresh and try again.".to_string(),
                );
            }
        }
        run_git_in_request(
            &["update-ref", "--no-deref", "HEAD", "FETCH_HEAD"],
            Some(repo_dir),
            auth,
            budget,
        )?;
        return Ok(());
    }

    let clone_args = bounded_remote_clone_args(clone_url, repo_path, branch, false, blob_filter);
    if run_git_in_request(&clone_args, None, auth, budget).is_err() && branch.is_some() {
        if repo_dir.exists() {
            std::fs::remove_dir_all(repo_dir)
                .map_err(|error| format!("reset temporary repository: {error}"))?;
        }
        let fallback_args =
            bounded_remote_clone_args(clone_url, repo_path, None, false, blob_filter);
        run_git_in_request(&fallback_args, None, auth, budget)?;
    }
    Ok(())
}

#[tauri::command]
pub async fn get_project_repo_file_content(
    clone_url: String,
    default_branch: Option<String>,
    target_ref: Option<String>,
    target_commit: Option<String>,
    path: String,
    state: State<'_, AppState>,
) -> Result<Option<String>, String> {
    validate_local_clone_url_for_workspace(&clone_url, &state)?;
    validate_repo_file_path(&path)?;
    let auth = build_git_clone_auth_config(&clone_url, &state)?;
    let branch = clean_branch(default_branch);
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
            RemoteBlobFilter::PreviewContent,
        )?;
        let tree_entry = run_git_in_request(
            &["ls-tree", "HEAD", "--", path.as_str()],
            Some(&repo_dir),
            &auth,
            &mut budget,
        )?;
        let object = tree_entry
            .split_once('\t')
            .and_then(|(metadata, listed_path)| {
                (listed_path == path).then_some(metadata.split_whitespace().collect::<Vec<_>>())
            })
            .filter(|parts| parts.first().copied() == Some("100644"))
            .and_then(|parts| parts.get(2).copied())
            .ok_or_else(|| "Requested repository path is not a regular file.".to_string())?;
        if run_git_in_request(
            &["cat-file", "-e", object],
            Some(&repo_dir),
            &auth,
            &mut budget,
        )
        .is_err()
        {
            return Err("Requested repository file exceeds the remote preview limit.".to_string());
        }
        let size = run_git_in_request(
            &["cat-file", "-s", object],
            Some(&repo_dir),
            &auth,
            &mut budget,
        )?
        .trim()
        .parse::<u64>()
        .map_err(|_| "Requested repository file size was malformed.".to_string())?;
        if size > MAX_PREVIEW_BYTES {
            return Err("Requested repository file exceeds the remote preview limit.".to_string());
        }
        if run_git_in_request(
            &["checkout", "--quiet", "HEAD", "--", path.as_str()],
            Some(&repo_dir),
            &auth,
            &mut budget,
        )
        .is_err()
        {
            return Ok(None);
        }
        Ok(read_preview_content(&repo_dir, &path, Some(size)))
    })
    .await
    .map_err(|error| format!("repo file content task failed: {error}"))?
}

#[tauri::command]
pub async fn get_project_local_repo_file_content(
    repos_dir: Option<String>,
    project_dtag: String,
    clone_url: Option<String>,
    default_branch: Option<String>,
    path: String,
    state: State<'_, AppState>,
) -> Result<Option<String>, String> {
    validate_repo_file_path(&path)?;
    let auth = build_git_auth_config(&state)?;
    let branch = clean_branch(default_branch);
    tauri::async_runtime::spawn_blocking(move || {
        let Some(repo_dir) =
            find_local_repo_dir(repos_dir.as_deref(), &project_dtag, clone_url.as_deref())?
        else {
            return Ok(None);
        };
        read_local_preview_content(&repo_dir, &path, branch.as_deref(), &auth)
    })
    .await
    .map_err(|error| format!("local repo file content task failed: {error}"))?
}

#[cfg(test)]
mod tests {
    use super::{bounded_remote_clone_args, RemoteBlobFilter};

    #[test]
    fn remote_clone_arguments_are_shallow_filtered_and_do_not_checkout() {
        assert_eq!(
            bounded_remote_clone_args(
                "https://github.com/block/buzz.git",
                "/tmp/buzz-repo",
                Some("main"),
                false,
                RemoteBlobFilter::MetadataOnly,
            ),
            [
                "clone",
                "--filter=blob:none",
                "--depth=100",
                "--single-branch",
                "--no-tags",
                "--no-checkout",
                "--branch",
                "main",
                "--",
                "https://github.com/block/buzz.git",
                "/tmp/buzz-repo",
            ]
        );
        assert_eq!(
            bounded_remote_clone_args(
                "https://github.com/block/buzz.git",
                "/tmp/buzz-repo",
                None,
                true,
                RemoteBlobFilter::MetadataOnly,
            ),
            [
                "clone",
                "--filter=blob:none",
                "--depth=1",
                "--single-branch",
                "--no-tags",
                "--no-checkout",
                "--",
                "https://github.com/block/buzz.git",
                "/tmp/buzz-repo",
            ]
        );
    }
}
