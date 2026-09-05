use super::project_git_exec::{
    build_git_auth_config, build_git_clone_auth_config, clean_branch, run_git_in_request,
    validate_local_clone_url_for_workspace, GitAuthConfig, GitRequestBudget,
};
use super::project_git_file_content::{bounded_remote_clone_args, RemoteBlobFilter};
use super::project_repo_paths::find_local_repo_dir;
use crate::app_state::AppState;
use serde::Serialize;
use tauri::State;

/// Per-file cap on rendered patch lines. One regenerated lockfile or
/// minified bundle would otherwise produce tens of thousands of DOM nodes
/// in the diff view and freeze the webview.
const MAX_PATCH_LINES: usize = 2_000;
const MAX_PATCH_BYTES: usize = 256 * 1024;
const MAX_DIFF_BYTES: usize = 2 * 1024 * 1024;
const MAX_DIFF_FILES: usize = 50;

struct DiffGit<'a> {
    auth: &'a GitAuthConfig,
    budget: &'a mut GitRequestBudget,
}

impl DiffGit<'_> {
    fn run(&mut self, args: &[&str], cwd: Option<&std::path::Path>) -> Result<String, String> {
        run_git_in_request(args, cwd, self.auth, self.budget)
    }
}

#[derive(Serialize)]
pub struct ProjectRepoDiffFileInfo {
    pub path: String,
    pub additions: usize,
    pub deletions: usize,
    pub patch: String,
    pub truncated: bool,
}

#[derive(Serialize)]
pub struct ProjectRepoDiffInfo {
    pub files: Vec<ProjectRepoDiffFileInfo>,
    pub additions: usize,
    pub deletions: usize,
    pub commit_body: Option<String>,
}

fn clean_target_ref(value: Option<String>) -> Option<String> {
    value.filter(|value| {
        value.starts_with("refs/")
            && !value.contains("..")
            && value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '_' | '.' | '-'))
    })
}

pub(crate) fn clean_commit(value: Option<String>) -> Option<String> {
    value
        .filter(|value| matches!(value.len(), 40 | 64))
        .filter(|value| value.chars().all(|c| c.is_ascii_hexdigit()))
}

fn fetch_target(
    repo_dir: &std::path::Path,
    git: &mut DiffGit<'_>,
    branch: Option<&str>,
    target_ref: Option<&str>,
    target_commit: Option<&str>,
) -> Result<(), String> {
    if let Some(target_ref) = target_ref {
        if git
            .run(
                &[
                    "fetch",
                    "--depth=100",
                    "--no-tags",
                    "--end-of-options",
                    "origin",
                    target_ref,
                ],
                Some(repo_dir),
            )
            .is_ok()
        {
            git.run(
                &["update-ref", "--no-deref", "HEAD", "FETCH_HEAD"],
                Some(repo_dir),
            )?;
            return Ok(());
        }
    } else if let Some(target_commit) = target_commit {
        if git
            .run(
                &[
                    "fetch",
                    "--depth=100",
                    "--no-tags",
                    "--end-of-options",
                    "origin",
                    target_commit,
                ],
                Some(repo_dir),
            )
            .is_ok()
        {
            git.run(
                &["update-ref", "--no-deref", "HEAD", "FETCH_HEAD"],
                Some(repo_dir),
            )?;
            return Ok(());
        }
    }

    if let Some(target_commit) = target_commit {
        if git
            .run(
                &[
                    "fetch",
                    "--depth=100",
                    "--no-tags",
                    "--end-of-options",
                    "origin",
                    target_commit,
                ],
                Some(repo_dir),
            )
            .is_ok()
        {
            git.run(
                &["update-ref", "--no-deref", "HEAD", "FETCH_HEAD"],
                Some(repo_dir),
            )?;
            return Ok(());
        }
    }

    if let Some(branch) = branch {
        let refspec = format!("refs/heads/{branch}:refs/remotes/origin/{branch}");
        git.run(
            &[
                "fetch",
                "--depth=100",
                "--no-tags",
                "--end-of-options",
                "origin",
                &refspec,
            ],
            Some(repo_dir),
        )?;
        git.run(
            &[
                "update-ref",
                "--no-deref",
                "HEAD",
                &format!("origin/{branch}"),
            ],
            Some(repo_dir),
        )?;
        return Ok(());
    }

    git.run(
        &[
            "fetch",
            "--depth=100",
            "--no-tags",
            "--end-of-options",
            "origin",
            "HEAD",
        ],
        Some(repo_dir),
    )?;
    git.run(
        &["update-ref", "--no-deref", "HEAD", "FETCH_HEAD"],
        Some(repo_dir),
    )?;
    Ok(())
}

fn diff_base_ref(
    repo_dir: &std::path::Path,
    git: &mut DiffGit<'_>,
    base_branch: Option<&str>,
) -> Option<String> {
    let base_branch = base_branch?;
    let refspec = format!("refs/heads/{base_branch}:refs/remotes/origin/{base_branch}");
    git.run(
        &[
            "fetch",
            "--depth=100",
            "--no-tags",
            "--end-of-options",
            "origin",
            &refspec,
        ],
        Some(repo_dir),
    )
    .ok()?;
    Some(format!("origin/{base_branch}"))
}

fn parse_count(value: &str) -> usize {
    value.parse::<usize>().unwrap_or_default()
}

fn parse_numstat(output: &str) -> Vec<(String, usize, usize)> {
    output
        .lines()
        .filter_map(|line| {
            let mut parts = line.split('\t');
            let additions = parse_count(parts.next()?);
            let deletions = parse_count(parts.next()?);
            let path = parts.next()?.to_string();
            Some((path, additions, deletions))
        })
        .collect()
}

fn empty_tree_ref(repo_dir: &std::path::Path, git: &mut DiffGit<'_>) -> Result<String, String> {
    git.run(&["hash-object", "-t", "tree", "/dev/null"], Some(repo_dir))
        .map(|output| output.trim().to_string())
}

fn diff_range(
    repo_dir: &std::path::Path,
    git: &mut DiffGit<'_>,
    base_ref: Option<String>,
) -> String {
    if let Some(base_ref) = base_ref {
        return if git
            .run(&["merge-base", &base_ref, "HEAD"], Some(repo_dir))
            .is_ok()
        {
            format!("{base_ref}...HEAD")
        } else {
            format!("{base_ref}..HEAD")
        };
    }

    empty_tree_ref(repo_dir, git)
        .map(|empty_tree| format!("{empty_tree}..HEAD"))
        .unwrap_or_else(|_| "HEAD^..HEAD".to_string())
}

/// Range for a single commit against its parent, used by the commit detail
/// view. Root commits fall back to the empty tree so the whole initial tree
/// renders as additions. Errors when the commit is not reachable in the
/// available history — diffing an unrelated ref instead would be misleading.
fn commit_parent_range(
    repo_dir: &std::path::Path,
    git: &mut DiffGit<'_>,
    commit: &str,
) -> Result<String, String> {
    git.run(
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{commit}^{{commit}}"),
        ],
        Some(repo_dir),
    )
    .map_err(|_| format!("commit {commit} was not found in the repository history"))?;
    let parent = format!("{commit}^");
    if git
        .run(
            &["rev-parse", "--verify", "--quiet", &parent],
            Some(repo_dir),
        )
        .is_ok()
    {
        return Ok(format!("{parent}..{commit}"));
    }
    let empty_tree = empty_tree_ref(repo_dir, git)?;
    Ok(format!("{empty_tree}..{commit}"))
}

fn local_ref_exists(repo_dir: &std::path::Path, git: &mut DiffGit<'_>, ref_name: &str) -> bool {
    git.run(
        &["rev-parse", "--verify", "--quiet", ref_name],
        Some(repo_dir),
    )
    .is_ok()
}

fn local_target_ref(
    repo_dir: &std::path::Path,
    git: &mut DiffGit<'_>,
    branch: Option<&str>,
    target_commit: Option<&str>,
) -> String {
    if let Some(target_commit) = target_commit {
        if local_ref_exists(repo_dir, git, target_commit) {
            return target_commit.to_string();
        }
    }
    if let Some(branch) = branch {
        if local_ref_exists(repo_dir, git, branch) {
            return branch.to_string();
        }
        let origin_branch = format!("origin/{branch}");
        if local_ref_exists(repo_dir, git, &origin_branch) {
            return origin_branch;
        }
    }
    "HEAD".to_string()
}

fn local_base_ref(
    repo_dir: &std::path::Path,
    git: &mut DiffGit<'_>,
    branch: Option<&str>,
    target_branch: Option<&str>,
) -> Option<String> {
    let branch = branch?;
    let origin_branch = format!("origin/{branch}");
    if local_ref_exists(repo_dir, git, &origin_branch) {
        return Some(origin_branch);
    }
    if target_branch == Some(branch) {
        return None;
    }
    local_ref_exists(repo_dir, git, branch).then_some(branch.to_string())
}

fn local_diff_range(
    repo_dir: &std::path::Path,
    git: &mut DiffGit<'_>,
    base_branch: Option<&str>,
    target_branch: Option<&str>,
    base_commit: Option<&str>,
    target_commit: Option<&str>,
) -> String {
    let target_ref = local_target_ref(repo_dir, git, target_branch, target_commit);
    if let Some(base_commit) = base_commit {
        if base_commit != target_ref && local_ref_exists(repo_dir, git, base_commit) {
            return if git
                .run(&["merge-base", base_commit, &target_ref], Some(repo_dir))
                .is_ok()
            {
                format!("{base_commit}...{target_ref}")
            } else {
                format!("{base_commit}..{target_ref}")
            };
        }
    }
    if let Some(base_ref) = local_base_ref(repo_dir, git, base_branch, target_branch) {
        return if git
            .run(&["merge-base", &base_ref, &target_ref], Some(repo_dir))
            .is_ok()
        {
            format!("{base_ref}...{target_ref}")
        } else {
            format!("{base_ref}..{target_ref}")
        };
    }
    // With no base at all, a bare commit means "diff against its parent"
    // (commit detail view) rather than against the whole tree.
    if base_commit.is_none() && base_branch.is_none() {
        if let Some(target_commit) = target_commit {
            if local_ref_exists(repo_dir, git, target_commit) {
                if let Ok(range) = commit_parent_range(repo_dir, git, target_commit) {
                    return range;
                }
            }
        }
    }
    empty_tree_ref(repo_dir, git)
        .map(|empty_tree| format!("{empty_tree}..{target_ref}"))
        .unwrap_or_else(|_| format!("{target_ref}^..{target_ref}"))
}

/// Caps text at a byte and line boundary without slicing through UTF-8.
fn truncate_patch(patch: String, max_bytes: usize) -> (String, bool) {
    let mut cut_at = patch.len().min(max_bytes);
    while !patch.is_char_boundary(cut_at) {
        cut_at = cut_at.saturating_sub(1);
    }
    if let Some(line_cut) = patch[..cut_at]
        .char_indices()
        .filter(|(_, character)| *character == '\n')
        .map(|(index, _)| index)
        .nth(MAX_PATCH_LINES.saturating_sub(1))
    {
        cut_at = cut_at.min(line_cut);
    }
    let truncated = cut_at < patch.len();
    (patch[..cut_at].to_string(), truncated)
}

fn diff_from_repo(
    repo_dir: &std::path::Path,
    git: &mut DiffGit<'_>,
    range: &str,
    target_commit: Option<&str>,
) -> Result<ProjectRepoDiffInfo, String> {
    let mut remaining_bytes = MAX_DIFF_BYTES;
    let commit_body = target_commit
        .map(|commit| {
            git.run(
                &[
                    "show",
                    "--no-patch",
                    "--format=%b",
                    "--end-of-options",
                    commit,
                ],
                Some(repo_dir),
            )
            .map(|body| {
                truncate_patch(
                    body.trim_end().to_string(),
                    remaining_bytes.min(MAX_PATCH_BYTES),
                )
                .0
            })
        })
        .transpose()?
        .filter(|body| !body.is_empty());
    remaining_bytes =
        remaining_bytes.saturating_sub(commit_body.as_deref().map(str::len).unwrap_or_default());
    let numstat = git.run(
        &["diff", "--no-ext-diff", "--no-textconv", "--numstat", range],
        Some(repo_dir),
    )?;
    let stats = parse_numstat(&numstat);
    if stats.len() > MAX_DIFF_FILES {
        return Err(format!(
            "Repository diff exceeds the {MAX_DIFF_FILES} file limit."
        ));
    }
    let mut files = Vec::with_capacity(stats.len());
    for (path, additions, deletions) in stats {
        let (patch, truncated) = if remaining_bytes == 0 {
            (String::new(), true)
        } else {
            let patch = git.run(
                &[
                    "diff",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--find-renames",
                    "--find-copies",
                    "--unified=80",
                    "--src-prefix=a/",
                    "--dst-prefix=b/",
                    range,
                    "--",
                    &path,
                ],
                Some(repo_dir),
            )?;
            truncate_patch(patch, remaining_bytes.min(MAX_PATCH_BYTES))
        };
        remaining_bytes = remaining_bytes.saturating_sub(patch.len());
        files.push(ProjectRepoDiffFileInfo {
            path,
            additions,
            deletions,
            patch,
            truncated,
        });
    }
    Ok(ProjectRepoDiffInfo {
        additions: files
            .iter()
            .fold(0_usize, |sum, file| sum.saturating_add(file.additions)),
        deletions: files
            .iter()
            .fold(0_usize, |sum, file| sum.saturating_add(file.deletions)),
        commit_body,
        files,
    })
}

#[tauri::command]
pub async fn get_project_repo_diff(
    clone_url: String,
    default_branch: Option<String>,
    base_branch: Option<String>,
    target_ref: Option<String>,
    target_commit: Option<String>,
    state: State<'_, AppState>,
) -> Result<ProjectRepoDiffInfo, String> {
    validate_local_clone_url_for_workspace(&clone_url, &state)?;
    let auth = build_git_clone_auth_config(&clone_url, &state)?;
    let branch = clean_branch(default_branch);
    let base_branch = clean_branch(base_branch);
    let target_ref = clean_target_ref(target_ref);
    let target_commit = clean_commit(target_commit);

    tauri::async_runtime::spawn_blocking(move || {
        let temp_dir = tempfile::tempdir().map_err(|error| format!("create temp dir: {error}"))?;
        let repo_dir = temp_dir.path().join("repo");
        let repo_path = repo_dir
            .to_str()
            .ok_or_else(|| "temporary repository path is not UTF-8".to_string())?;
        let mut budget = GitRequestBudget::remote(temp_dir.path());
        let mut git = DiffGit {
            auth: &auth,
            budget: &mut budget,
        };
        let clone_args = bounded_remote_clone_args(
            &clone_url,
            repo_path,
            branch.as_deref(),
            false,
            RemoteBlobFilter::DiffContent,
        );
        git.run(&clone_args, None)?;
        fetch_target(
            &repo_dir,
            &mut git,
            branch.as_deref(),
            target_ref.as_deref(),
            target_commit.as_deref(),
        )?;
        // A commit with no base branch or target ref means "diff this commit
        // against its parent" (commit detail view), not "diff HEAD against a
        // base".
        let range = match (&target_ref, &base_branch, &target_commit) {
            (None, None, Some(commit)) => commit_parent_range(&repo_dir, &mut git, commit)?,
            _ => {
                let base_ref = diff_base_ref(&repo_dir, &mut git, base_branch.as_deref());
                diff_range(&repo_dir, &mut git, base_ref)
            }
        };
        let commit_body_ref = if target_ref.is_none() && base_branch.is_none() {
            target_commit.as_deref()
        } else {
            None
        };
        diff_from_repo(&repo_dir, &mut git, &range, commit_body_ref)
    })
    .await
    .map_err(|error| format!("repo diff task failed: {error}"))?
}

#[tauri::command]
#[allow(clippy::too_many_arguments)]
pub async fn get_project_local_repo_diff(
    repos_dir: Option<String>,
    project_dtag: String,
    clone_url: Option<String>,
    default_branch: Option<String>,
    base_branch: Option<String>,
    base_commit: Option<String>,
    target_commit: Option<String>,
    state: State<'_, AppState>,
) -> Result<Option<ProjectRepoDiffInfo>, String> {
    let auth = build_git_auth_config(&state)?;
    let branch = clean_branch(default_branch);
    let base_branch = clean_branch(base_branch);
    let base_commit = clean_commit(base_commit);
    let target_commit = clean_commit(target_commit);

    tauri::async_runtime::spawn_blocking(move || {
        let Some(repo_dir) =
            find_local_repo_dir(repos_dir.as_deref(), &project_dtag, clone_url.as_deref())?
        else {
            return Ok(None);
        };
        let mut budget = GitRequestBudget::local();
        let mut git = DiffGit {
            auth: &auth,
            budget: &mut budget,
        };
        let range = local_diff_range(
            &repo_dir,
            &mut git,
            base_branch.as_deref(),
            branch.as_deref(),
            base_commit.as_deref(),
            target_commit.as_deref(),
        );
        let commit_body_ref = if base_commit.is_none() && base_branch.is_none() {
            target_commit.as_deref()
        } else {
            None
        };
        diff_from_repo(&repo_dir, &mut git, &range, commit_body_ref).map(Some)
    })
    .await
    .map_err(|error| format!("local repo diff task failed: {error}"))?
}

#[cfg(test)]
mod tests {
    use super::{parse_numstat, truncate_patch, MAX_DIFF_FILES, MAX_PATCH_LINES};

    #[test]
    fn patch_truncation_respects_utf8_bytes_and_lines() {
        let (bytes, byte_truncated) = truncate_patch("ééé".to_string(), 5);
        assert_eq!(bytes, "éé");
        assert!(byte_truncated);

        let input = (0..=MAX_PATCH_LINES)
            .map(|index| format!("line {index}\n"))
            .collect::<String>();
        let (lines, line_truncated) = truncate_patch(input, usize::MAX);
        assert_eq!(lines.lines().count(), MAX_PATCH_LINES);
        assert!(line_truncated);
    }

    #[test]
    fn numstat_parser_exposes_over_limit_file_counts_to_caller() {
        let input = (0..=MAX_DIFF_FILES)
            .map(|index| format!("1\t0\tfile-{index}.txt"))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(parse_numstat(&input).len(), MAX_DIFF_FILES + 1);
    }
}
