//! Project-scoped repository instruction preloading for managed ACP sessions.
//!
//! A project home is remote metadata; it must never select a local directory by
//! name alone. This resolver admits a checkout only when its real git `origin`
//! matches a clone URL from the authoritative repository announcement. A
//! repository can then opt in through one bounded manifest that names canonical
//! `.agents/skills/<name>/SKILL.md` files. No credentials or arbitrary files are
//! read into the prompt.

use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use serde::Deserialize;
use url::Url;

use crate::prompt_project::PromptProjectInfo;

const MANIFEST_PATH: &str = ".agents/buzz-preload.json";
const SCHEMA_VERSION: &str = "buzz.project-preload.v1";
const MAX_MANIFEST_BYTES: u64 = 8 * 1024;
const MAX_SKILLS: usize = 8;
const MAX_SKILL_BYTES: u64 = 24 * 1024;
const MAX_TOTAL_BYTES: usize = 64 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreloadManifest {
    schema_version: String,
    repository: String,
    skills: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectPreload {
    pub working_directory: PathBuf,
    pub instructions: Option<String>,
}

/// Resolve the exact checkout and any repository-owned skills for one project.
///
/// `preferred_checkout` is the already-authorized checkout of a one-shot Job.
/// Ordinary project conversations discover candidates only below
/// `<harness-cwd>/REPOS`. An absent checkout or manifest is a normal opt-out;
/// a present but invalid manifest fails closed.
pub fn resolve(
    harness_cwd: &Path,
    project: &PromptProjectInfo,
    preferred_checkout: Option<&Path>,
) -> Result<Option<ProjectPreload>, String> {
    let expected_origins = project
        .default_repo_clone_urls
        .iter()
        .filter_map(|value| canonical_github_repository(value))
        .collect::<HashSet<_>>();
    if expected_origins.is_empty() {
        return Ok(None);
    }

    let checkout = match preferred_checkout {
        Some(path) => validated_checkout(path, &expected_origins)?,
        None => discover_checkout(harness_cwd, project, &expected_origins)?,
    };
    let Some(checkout) = checkout else {
        return Ok(None);
    };
    let instructions = load_manifest(&checkout, &expected_origins)?;
    Ok(Some(ProjectPreload {
        working_directory: checkout,
        instructions,
    }))
}

fn discover_checkout(
    harness_cwd: &Path,
    project: &PromptProjectInfo,
    expected_origins: &HashSet<String>,
) -> Result<Option<PathBuf>, String> {
    let repos = harness_cwd.join("REPOS");
    let Ok(repos) = repos.canonicalize() else {
        return Ok(None);
    };
    if !repos.is_dir() {
        return Ok(None);
    }

    let mut names = Vec::new();
    for origin in expected_origins {
        if let Some((owner, repo)) = github_owner_repo(origin) {
            push_unique(&mut names, format!("{owner}--{repo}"));
            push_unique(&mut names, repo.to_string());
        }
    }
    if valid_component(&project.slug) {
        push_unique(&mut names, project.slug.clone());
    }
    if let Some(repo_id) = project
        .default_repo_id
        .as_deref()
        .filter(|value| valid_component(value))
    {
        push_unique(&mut names, repo_id.to_string());
    }

    let mut matches = Vec::new();
    for name in names {
        let path = repos.join(name);
        let Ok(path) = path.canonicalize() else {
            continue;
        };
        if !path.starts_with(&repos) || !path.is_dir() {
            continue;
        }
        if validated_checkout(&path, expected_origins)?.is_some()
            && !matches.iter().any(|existing| existing == &path)
        {
            matches.push(path);
        }
    }
    match matches.len() {
        0 => Ok(None),
        1 => Ok(matches.pop()),
        count => Err(format!(
            "project repository resolves to {count} local checkouts under REPOS; refusing an ambiguous instruction source"
        )),
    }
}

fn validated_checkout(
    path: &Path,
    expected_origins: &HashSet<String>,
) -> Result<Option<PathBuf>, String> {
    let Ok(root) = path.canonicalize() else {
        return Ok(None);
    };
    if !root.is_dir() || !root.join(".git").exists() {
        return Ok(None);
    }
    let toplevel = git_output(&root, &["rev-parse", "--show-toplevel"])?;
    if !toplevel.status.success() {
        return Ok(None);
    }
    let Some(toplevel) = std::str::from_utf8(&toplevel.stdout)
        .ok()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .and_then(|path| path.canonicalize().ok())
    else {
        return Ok(None);
    };
    if toplevel != root {
        return Ok(None);
    }
    let output = git_output(&root, &["config", "--local", "--get", "remote.origin.url"])?;
    if !output.status.success() {
        return Ok(None);
    }
    let origin = std::str::from_utf8(&output.stdout)
        .ok()
        .and_then(canonical_github_repository);
    Ok(origin
        .filter(|origin| expected_origins.contains(origin))
        .map(|_| root))
}

fn git_output(root: &Path, args: &[&str]) -> Result<std::process::Output, String> {
    Command::new("git")
        .arg("--no-optional-locks")
        .arg("-C")
        .arg(root)
        .args(args)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_CONFIG")
        .env_remove("GIT_CONFIG_COUNT")
        .output()
        .map_err(|error| format!("could not inspect project checkout: {error}"))
}

fn load_manifest(
    checkout: &Path,
    expected_origins: &HashSet<String>,
) -> Result<Option<String>, String> {
    let manifest_path = checkout.join(MANIFEST_PATH);
    let metadata = match fs::symlink_metadata(&manifest_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("could not inspect {MANIFEST_PATH}: {error}")),
    };
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(format!(
            "{MANIFEST_PATH} must be a regular non-symlink file"
        ));
    }
    if metadata.len() > MAX_MANIFEST_BYTES {
        return Err(format!(
            "{MANIFEST_PATH} exceeds {MAX_MANIFEST_BYTES} bytes"
        ));
    }
    let bytes = fs::read(&manifest_path)
        .map_err(|error| format!("could not read {MANIFEST_PATH}: {error}"))?;
    let manifest: PreloadManifest = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid {MANIFEST_PATH}: {error}"))?;
    if manifest.schema_version != SCHEMA_VERSION {
        return Err(format!(
            "unsupported {MANIFEST_PATH} schema_version {:?}",
            manifest.schema_version
        ));
    }
    let repository = canonical_github_repository(&manifest.repository)
        .ok_or_else(|| format!("{MANIFEST_PATH} repository is not canonical GitHub HTTPS"))?;
    if !expected_origins.contains(&repository) {
        return Err(format!(
            "{MANIFEST_PATH} repository does not match the authoritative Project repository"
        ));
    }
    if manifest.skills.is_empty() || manifest.skills.len() > MAX_SKILLS {
        return Err(format!(
            "{MANIFEST_PATH} must list between 1 and {MAX_SKILLS} skills"
        ));
    }

    let mut seen = HashSet::new();
    let mut rendered = Vec::with_capacity(manifest.skills.len());
    let mut total = 0usize;
    for name in manifest.skills {
        if !valid_skill_name(&name) || !seen.insert(name.clone()) {
            return Err(format!(
                "{MANIFEST_PATH} contains an invalid or duplicate skill name"
            ));
        }
        let relative = format!(".agents/skills/{name}/SKILL.md");
        let path = checkout.join(&relative);
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("could not inspect {relative}: {error}"))?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            return Err(format!("{relative} must be a regular non-symlink file"));
        }
        if metadata.len() > MAX_SKILL_BYTES {
            return Err(format!("{relative} exceeds {MAX_SKILL_BYTES} bytes"));
        }
        let canonical = path
            .canonicalize()
            .map_err(|error| format!("could not resolve {relative}: {error}"))?;
        if !canonical.starts_with(checkout) {
            return Err(format!("{relative} escapes the project checkout"));
        }
        let content = fs::read_to_string(&canonical)
            .map_err(|error| format!("could not read UTF-8 {relative}: {error}"))?;
        if skill_frontmatter_name(&content) != Some(name.as_str()) {
            return Err(format!("{relative} frontmatter name does not match {name}"));
        }
        total = total.saturating_add(content.len());
        if total > MAX_TOTAL_BYTES {
            return Err(format!(
                "project preloaded instructions exceed {MAX_TOTAL_BYTES} bytes"
            ));
        }
        rendered.push(format!(
            "## Repository skill: {name}\nSource: {relative}\n\n{}",
            content.trim()
        ));
    }

    Ok(Some(format!(
        "These repository-owned skills are mandatory for this Project session. Their scope is only {repository}. Resolve linked references relative to the project checkout.\n\n{}",
        rendered.join("\n\n")
    )))
}

fn canonical_github_repository(value: &str) -> Option<String> {
    let value = value.trim();
    let parsed = Url::parse(value).ok()?;
    if parsed.scheme() != "https"
        || parsed.host_str()? != "github.com"
        || parsed.port().is_some()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return None;
    }
    let parts = parsed
        .path_segments()?
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let [owner, raw_repo] = parts.as_slice() else {
        return None;
    };
    let repo = raw_repo.strip_suffix(".git").unwrap_or(raw_repo);
    if !valid_github_name(owner) || !valid_github_name(repo) {
        return None;
    }
    Some(format!(
        "https://github.com/{}/{}",
        owner.to_ascii_lowercase(),
        repo.to_ascii_lowercase()
    ))
}

fn github_owner_repo(repository: &str) -> Option<(&str, &str)> {
    let suffix = repository.strip_prefix("https://github.com/")?;
    suffix.split_once('/')
}

fn valid_github_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
}

fn valid_skill_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
}

fn valid_component(value: &str) -> bool {
    let path = Path::new(value);
    value.len() <= 128
        && path.components().count() == 1
        && matches!(path.components().next(), Some(Component::Normal(_)))
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|candidate| candidate == &value) {
        values.push(value);
    }
}

fn skill_frontmatter_name(content: &str) -> Option<&str> {
    let mut lines = content.lines();
    if lines.next()? != "---" {
        return None;
    }
    let mut name = None;
    for line in lines {
        if line == "---" {
            return name;
        }
        if let Some(value) = line.strip_prefix("name:") {
            if name.replace(value.trim()).is_some() {
                return None;
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::TempDir;

    fn git(cwd: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(cwd)
            .args(args)
            .status()
            .expect("run git fixture command");
        assert!(status.success(), "git {:?} failed", args);
    }

    fn project() -> PromptProjectInfo {
        PromptProjectInfo {
            name: "Nemo".into(),
            slug: "nemo".into(),
            owner: "a".repeat(64),
            coordinate: format!("30621:{}:nemo", "a".repeat(64)),
            default_repo_owner: Some("b".repeat(64)),
            default_repo_id: Some("nemo".into()),
            default_repo_clone_urls: vec!["https://github.com/mysteropodes/nemo.git".into()],
        }
    }

    fn fixture() -> (TempDir, PathBuf) {
        let temp = TempDir::new().expect("tempdir");
        let checkout = temp.path().join("REPOS/nemo");
        fs::create_dir_all(&checkout).expect("checkout dir");
        git(&checkout, &["init", "--quiet"]);
        git(
            &checkout,
            &[
                "remote",
                "add",
                "origin",
                "https://github.com/mysteropodes/nemo.git",
            ],
        );
        (temp, checkout)
    }

    const SKILL_BODY: &str = "---\nname: nemo-a2a\ndescription: Coordinate Nemo work.\n---\n\n# Nemo A2A\n\nNEMO-A2A-1\n";

    fn write_skill(checkout: &Path) {
        let skill = checkout.join(".agents/skills/nemo-a2a/SKILL.md");
        fs::create_dir_all(skill.parent().expect("skill parent")).expect("skill dir");
        fs::write(skill, SKILL_BODY).expect("skill");
        fs::write(
            checkout.join(MANIFEST_PATH),
            r#"{"schema_version":"buzz.project-preload.v1","repository":"https://github.com/mysteropodes/nemo","skills":["nemo-a2a"]}"#,
        )
        .expect("manifest");
    }

    #[test]
    fn authoritative_origin_preloads_skill_and_selects_checkout() {
        let (temp, checkout) = fixture();
        write_skill(&checkout);

        let preload = resolve(temp.path(), &project(), None)
            .expect("valid preload")
            .expect("checkout found");
        assert_eq!(preload.working_directory, checkout.canonicalize().unwrap());
        let instructions = preload.instructions.expect("instructions");
        assert!(instructions.contains("NEMO-A2A-1"));
        assert!(instructions.contains("Source: .agents/skills/nemo-a2a/SKILL.md"));
        assert!(
            instructions.ends_with(SKILL_BODY.trim()),
            "the complete declared skill must enter the preload without truncation"
        );
        assert!(!instructions.contains(temp.path().to_string_lossy().as_ref()));
    }

    #[test]
    fn wrong_origin_never_selects_same_named_checkout() {
        let (temp, checkout) = fixture();
        git(
            &checkout,
            &[
                "remote",
                "set-url",
                "origin",
                "https://github.com/evil/nemo",
            ],
        );
        write_skill(&checkout);
        assert_eq!(resolve(temp.path(), &project(), None).unwrap(), None);
    }

    #[test]
    fn non_nemo_project_never_receives_nemo_instructions() {
        let (temp, checkout) = fixture();
        write_skill(&checkout);
        let mut other_project = project();
        other_project.slug = "nemo".into();
        other_project.default_repo_id = Some("nemo".into());
        other_project.default_repo_clone_urls =
            vec!["https://github.com/example/different-project.git".into()];

        assert_eq!(resolve(temp.path(), &other_project, None).unwrap(), None);
    }

    #[test]
    fn absent_manifest_uses_verified_project_checkout_without_prompt_copy() {
        let (temp, checkout) = fixture();
        let preload = resolve(temp.path(), &project(), None)
            .expect("resolution")
            .expect("checkout found");
        assert_eq!(preload.working_directory, checkout.canonicalize().unwrap());
        assert_eq!(preload.instructions, None);
    }

    #[test]
    fn malformed_or_cross_repo_manifest_fails_closed() {
        let (temp, checkout) = fixture();
        write_skill(&checkout);
        fs::write(
            checkout.join(MANIFEST_PATH),
            r#"{"schema_version":"buzz.project-preload.v1","repository":"https://github.com/other/nemo","skills":["nemo-a2a"]}"#,
        )
        .unwrap();
        let error = resolve(temp.path(), &project(), None).unwrap_err();
        assert!(error.contains("does not match"));

        fs::write(
            checkout.join(MANIFEST_PATH),
            r#"{"schema_version":"buzz.project-preload.v1","repository":"https://github.com/mysteropodes/nemo","skills":["nemo-a2a"]}"#,
        )
        .unwrap();
        fs::write(
            checkout.join(".agents/skills/nemo-a2a/SKILL.md"),
            "---\nname: nemo-a2a\nmissing closing frontmatter",
        )
        .unwrap();
        let error = resolve(temp.path(), &project(), None).unwrap_err();
        assert!(error.contains("frontmatter name does not match"));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_skill_file_fails_closed() {
        use std::os::unix::fs::symlink;

        let (temp, checkout) = fixture();
        write_skill(&checkout);
        let skill = checkout.join(".agents/skills/nemo-a2a/SKILL.md");
        fs::remove_file(&skill).unwrap();
        let outside = temp.path().join("outside.md");
        fs::write(&outside, "---\nname: nemo-a2a\n---\nsecret").unwrap();
        symlink(outside, skill).unwrap();
        let error = resolve(temp.path(), &project(), None).unwrap_err();
        assert!(error.contains("regular non-symlink"));
    }
}
